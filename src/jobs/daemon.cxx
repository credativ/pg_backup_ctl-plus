extern "C" {
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/shm.h> /* for IPC_STAT */
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <syslog.h>
#include <string.h>
#include <signal.h>
}

/* logging */
#include <boost/log/trivial.hpp>
#include <istream>
#include <stack>

#include <bgrndroletype.hxx>
#include <daemon.hxx>
#include <parser.hxx>
#include <commands.hxx>
#include <reaper.hxx>
#include <server.hxx>

#define MSG_QUEUE_MAX_TOKEN_SZ 255

using namespace credativ;

volatile sig_atomic_t _pgbckctl_shutdown_mode = DAEMON_RUN;
extern int errno;

/**
 * Background launcher child reaper.
 */
background_reaper *launcher_reaper = nullptr;

/*
 * Forwarded declarations.
 */
static pid_t daemonize(job_info &info);

/*
 * Type of background process. Either launcher or worker
 * background processes currently exists. In case this isn't
 * a background process, NO_BACKGROUND is set.
 */
BackgroundJobType _pgbckctl_job_type = NO_BACKGROUND;

/*
 * Signal handler objects
 */
AtomicSignalHandler *termHandler = new AtomicSignalHandler(&_pgbckctl_shutdown_mode,
                                                           DAEMON_TERM_NORMAL);
AtomicSignalHandler *emergencyHandler = new AtomicSignalHandler(&_pgbckctl_shutdown_mode,
                                                                DAEMON_TERM_EMERGENCY);

BackgroundWorker::BackgroundWorker(job_info info) {

  this->launcher_status = LAUNCHER_STARTUP;
  this->ji = info;
  this->catalog = this->ji.cmdHandle->getCatalog();
  this->procInfo = std::make_shared<CatalogProc>();

  /*
   * XXX: Probably we should use getCatalog()->fullname() here, since
   *      we risk clashes with sqlite files equally named but residing
   *      in different locations.
   */
  this->my_shm.attach(this->ji.cmdHandle->getCatalog()->fullname(), false);

  /*
   * Worker shared memory area.
   */
  this->worker_shm = new WorkerSHM();
  this->worker_shm->attach(this->ji.cmdHandle->getCatalog()->fullname(), false);

}

BackgroundWorker::~BackgroundWorker() {

  this->my_shm.detach();
  this->worker_shm->detach();

}

void BackgroundWorker::release_launcher_role() {

  this->my_shm.detach();

}

LauncherStatus BackgroundWorker::status() {

  return this->launcher_status;

}

WorkerSHM *BackgroundWorker::workerSHM() {

  return this->worker_shm;

}

job_info BackgroundWorker::jobInfo() {

  return this->ji;

}

bool BackgroundWorker::checkPID(pid_t pid) {

  bool result = true;

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "checkPID(): checking PID " << pid;
#endif

  /*
   * Send SIGNAL 0 to the specified PID
   */
  if (kill(pid, 0) < 0) {
    /* check errno */
    int rc = errno;

    if (rc == ESRCH) {
      /* PID does not exist */
#ifdef __DEBUG__
      BOOST_LOG_TRIVIAL(debug) << "checkPID(): " << pid << " does not exist";
#endif
      result = false;
    }

    if (rc == EPERM) {
      /* No permissions for this PID ... */
#ifdef __DEBUG__
      BOOST_LOG_TRIVIAL(debug) << "checkPID(): " << pid << " permission denied";
#endif
      result = false;
    }

    return result;
  }

  return result;
}

void BackgroundWorker::prepareShutdown() {

  this->launcher_status = LAUNCHER_SHUTDOWN;

  /*
   * NOTE: We don't catch any exceptions here,
   * this is done by the initialize() caller, since
   * we remap those errors into a WorkerFailure exception.
   */

  /*
   * Mark us as shutdown.
   */
  catalog->startTransaction();

  try {
    catalog->unregisterProc(procInfo->pid, procInfo->archive_id);
  } catch (std::exception& e) {
    catalog->rollbackTransaction();
    /* don't hide exception from caller */
    throw e;
  }

  catalog->commitTransaction();

  /* close database */
  catalog->close();
}

void BackgroundWorker::assign_reaper(background_reaper *reaper) {

  if (reaper != nullptr)
    this->reaper = reaper;

}

void BackgroundWorker::execute_reaper() {

  if (this->reaper != nullptr) {
    this->reaper->reap();
  }

}

void BackgroundWorker::registerMe() {

  std::shared_ptr<CatalogProc> tempProc;

  /*
   * NOTE: We don't catch any exceptions here,
   * this is done by the initialize() caller, since
   * we remap those errors into a WorkerFailure exception.
   */

  /*
   * Registering a worker needs to recognize the
   * following possible preconditions:
   *
   * - If a former worker was already launched on an archive,
   *   we just update its process state in the catalog. Thus, we
   *   need to check if an entry for this worker already exists.
   *   If TRUE, we update its PID and set its state to RUNNING.
   *
   * - If a former worker crashed or did an emergency exit,
   *   we'll find an orphaned running state of this worker.
   *   We need to try hard to check, whether this process is
   *   still alive. The current procedure for this is as
   *   follows:
   *
   *   signal(0) the stored PID to check if the PID still
   *   is alive and belongs to us.
   *
   * - If no entry is found, just create one with the
   *   proper attributes.
   */

  procInfo->pid = getpid();
  procInfo->archive_id = -1;
  procInfo->type = CatalogProc::PROC_TYPE_LAUNCHER;
  procInfo->state = CatalogProc::PROC_STATUS_RUNNING;
  procInfo->started = CPGBackupCtlBase::current_timestamp();

  /*
   * It's time to remember our PID
   * into our control memory segment.
   */
  this->my_shm.setPID(procInfo->pid);
  procInfo->shm_key = this->my_shm.get_shmkey();
  procInfo->shm_id  = this->my_shm.get_shmid();

  /*
   * Check if there is a catalog entry for this kind
   * of worker.
   */
  catalog->startTransaction();

  try {

#ifdef __DEBUG__
    BOOST_LOG_TRIVIAL(debug) << "checking for worker for archive " << procInfo->archive_id;
#endif

    tempProc = catalog->getProc(procInfo->archive_id,
                                procInfo->type);

    if (tempProc != nullptr
        && tempProc->pid > 0) {

      /*
       * There is an existing entry for this kind of worker.
       * Check, if the PID is still alive and belongs to us.
       *
       * XXX: This is not really safe yet, since the PID
       *      could have been reused in the meantime. We
       *      need a further check here, to verify that
       *      the existing PID is not really one of us.
       */
      if (!checkPID(tempProc->pid)) {

        /*
         * The retrieved PID is dead. Remove the old one.
         */
        catalog->unregisterProc(tempProc->pid, tempProc->archive_id);

      } else {
        /* oops, PID seems alive */
        std::ostringstream oss;
        oss << "worker with PID "
            << tempProc->pid
            << ", type "
            << tempProc->type
            << " already active";
        throw WorkerFailure(oss.str());
      }

    }

    /*
     * Tell the catalog which fields we need
     */
    procInfo->pushAffectedAttribute(SQL_PROCS_PID_ATTNO);
    procInfo->pushAffectedAttribute(SQL_PROCS_ARCHIVE_ID_ATTNO);
    procInfo->pushAffectedAttribute(SQL_PROCS_TYPE_ATTNO);
    procInfo->pushAffectedAttribute(SQL_PROCS_STARTED_ATTNO);
    procInfo->pushAffectedAttribute(SQL_PROCS_STATE_ATTNO);
    procInfo->pushAffectedAttribute(SQL_PROCS_SHM_KEY_ATTNO);
    procInfo->pushAffectedAttribute(SQL_PROCS_SHM_ID_ATTNO);

    catalog->registerProc(procInfo);

  } catch (CPGBackupCtlFailure& e) {

    catalog->rollbackTransaction();

    /* don't hide exception from caller */
    throw e;

  }

  /* should still be in progress */
  catalog->commitTransaction();

}

void BackgroundWorker::initialize() {

  /*
   * Shouldn't be a nullptr...
   */
  if (this->catalog == nullptr)
    throw WorkerFailure("catalog handle for worker not initialized");

  if (this->procInfo == nullptr)
    throw WorkerFailure("background worker information not initialized");

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "worker PID " << getpid()
                           << " command tag " << this->ji.cmdHandle->tag;
  BOOST_LOG_TRIVIAL(debug) << "worker catalog " << this->ji.cmdHandle->getCatalog()->name();
#endif

  /*
   * Initialization phase should run within an
   * try...catch block to make sure we can react
   * on e.g. database failures. We remap those errors
   * into a WorkerFailure exception since we don't expect
   * the launcher to operate on all kind of exceptions
   * thrown by the pg_backup_ctl++ API...
   */
  try {

    /*
     * Re-open backup catalog database. The parent
     * closed it previously to make sure we get our
     * own child handle here.
     */
    this->catalog->open_rw();

    /*
     * Sanity check, are we still alone?
     */
    if (this->my_shm.getNumberOfAttached() > 1) {
      std::ostringstream errstr;

      errstr << "launcher for catalog instance "
             << this->ji.cmdHandle->getCatalog()->name()
             << " already running";
      throw WorkerFailure(errstr.str());
    }

    this->registerMe();

  } catch(CPGBackupCtlFailure& e) {

#ifdef __DEBUG__
    BOOST_LOG_TRIVIAL(debug) << "error registering launcher process, forcing shutdown: "
                             << e.what();
#endif

    /*
     * In case we've opened the catalog database
     * already, close it again
     */
    if (this->catalog->available())
      this->catalog->close();

    _pgbckctl_shutdown_mode = DAEMON_TERM_EMERGENCY;
  }

}

void BackgroundWorker::run() {

  this->launcher_status = LAUNCHER_RUN;

}

/******************************************************************************
 * ProcessSHM & objects implementation start
 ******************************************************************************/

ProcessSHM::ProcessSHM() {

}

ProcessSHM::~ProcessSHM() {

}

shmatt_t ProcessSHM::getNumberOfAttached(int check_shmid) {

  shmatt_t nattach = 0;
  shmid_ds shmstat;

  /*
   * We need to call shmctl() directly, since there seems to be
   * no boost::interprocess API for it.
   */

  /* perform the IPC_STAT operation */
  if (shmctl(check_shmid, IPC_STAT, &shmstat) >= 0) {

    /* shmstat contains all necessary information */
    nattach = shmstat.shm_nattch;

  } else {

    /*
     * Oops, something went wrong here, notify
     * the caller via SHMFailure exception.
     */
    std::ostringstream oss;
    oss << "could not stat launcher shared segment: "
        << strerror(errno);
    throw SHMFailure(oss.str());
  }

  return nattach;

}

boost::interprocess::interprocess_mutex *ProcessSHM::check_and_get_mutex() {

  /*
   * Die immediately if the SHM area wasn't initialized
   *
   * This means here that we just check whether the mutex was initialized.
   * If not, we will find a nullptr, thus we throw a SHMFailure exception
   * telling the caller something went terribly wrong.
   */
  if (mtx == nullptr)
    throw SHMFailure("shared memory not initialized yet, probably pg_backup_ctl++ launcher was not started yet");

  return mtx;

}

shmatt_t ProcessSHM::getNumberOfAttached() {

  shmatt_t nattach = 0;
  int shmid;

  if (this->shm == nullptr) {
    throw SHMFailure("can't get nattach from shared memory segment: not initialized");
  }

  shmid = this->shm->get_shmid();
  nattach = LauncherSHM::getNumberOfAttached(shmid);

  return nattach;

}

key_t ProcessSHM::get_shmkey() {

  if (this->shm == nullptr) {
    return (key_t) -1;
  }

  return this->shm_key;
}

int ProcessSHM::get_shmid() {

  if (this->shm == nullptr) {
    return -1;
  }

  return this->shm->get_shmid();
}

std::string ProcessSHM::getIdent() {
  return this->shm_ident;
}

/******************************************************************************
 * WorkerSHM & objects implementation start
 ******************************************************************************/

WorkerSHM::WorkerSHM() : ProcessSHM() {

  this->shm = nullptr;
  this->shm_mem_ptr = nullptr;

}

WorkerSHM::~WorkerSHM() {

  if (this->shm != nullptr) {
    delete this->shm;
  }

}

size_t WorkerSHM::calculateSHMsize() {

  /*
   * Calculate the shared memory size.
   *
   * Make sure we have a segment at least 4K in size.
   *
   */
  return 2 * (sizeof(shm_worker_area) * this->max_workers)
    + sizeof(boost::interprocess::interprocess_mutex)
    + ( 4096 - ( (sizeof(shm_worker_area) * this->max_workers)
                 + sizeof(boost::interprocess::interprocess_mutex) ) );

}

bool WorkerSHM::attach(std::string catalog, bool attach_only) {

  using namespace boost::interprocess;

  if (this->shm != nullptr) {
    /* already initialized, consider this as no-op */
    return true;
  }

  /*
   * Generate the worker shared memory key.
   */
  std::ostringstream keystr;
  bool result = false;

  keystr << catalog;

  xsi_key key = xsi_key(keystr.str().c_str(), 2);
  std::ostringstream shm_ctl_name;
  std::ostringstream mtx_ctl_name;

  /*
   * Calculate requested shared memory size.
   */
  this->size = this->calculateSHMsize();

  assert(this->size > 0 && this->max_workers > 0);

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "size/max_workers "
                           << this->size
                           << "/"
                           << this->max_workers;
#endif

  /*
   * Internal handle not yet connected. Either create one or,
   * if attach_only is set to true, just attach.
   */
  if (attach_only) {

    try {
      this->shm = new managed_xsi_shared_memory(open_only, key);
    } catch(interprocess_exception &ie) {
      if (ie.get_error_code() == not_found_error) {
        result = false;
        return result;
      } else {
        throw ie;
      }
    }
  } else {

    this->shm = new managed_xsi_shared_memory(open_or_create,
                                              key,
                                              this->size);
  }

  /*
   * Stick mutex into shared memory.
   */
  mtx_ctl_name << catalog << "_mtx";
  this->mtx
    = this->shm->find_or_construct<interprocess_mutex>(mtx_ctl_name.str().c_str())();

  /*
   * Create worker control area in shared memory. This is basically an
   * array with up to max_worker entries. Also initialize
   * the upper index and make sure the last is set to 0.
   */
  shm_ctl_name << catalog << "_worker_area";
  this->shm_mem_ptr
    = this->shm->find_or_construct<shm_worker_area>(shm_ctl_name.str().c_str())[this->max_workers]();

  this->upper = this->max_workers - 1;

  /*
   * Don't forget identifiers...
   */
  this->shm_ident = shm_ctl_name.str();
  this->shm_key   = key.get_key();

  /*
   * Looks good so far ...
   */
  result = true;
  return result;
}

void WorkerSHM::unlock() {

  if (this->shm == nullptr) {
    throw SHMFailure("failed to unlock shared memory: not attached");
  }

  this->mtx->unlock();
}

void WorkerSHM::lock() {

  if (this->shm == nullptr) {
    throw SHMFailure("failed to lock shared memory: not attached");
  }

  this->mtx->lock();
}

void WorkerSHM::write(unsigned int slot_index,
                      shm_worker_area &item) {

  shm_worker_area *ptr = NULL;

  if ( (this->shm == nullptr)
       || (this->shm_mem_ptr == nullptr)) {
    throw SHMFailure("attempt to write worker slot from uninitialized shared memory");
  }

  /*
   * Reference the worker slot at the
   * specified slot_index. If slot_index is larger
   * than our upper index, throw.
   */
  if (slot_index > this->upper) {
    ostringstream oss;

    oss << "requested slot index " << slot_index
        << " exceeds shared memory upper limit";
    throw SHMFailure(oss.str());
  }

  ptr = (shm_worker_area *)(this->shm_mem_ptr + slot_index);

  if (ptr != NULL) {

    ptr->pid = item.pid;
    ptr->cmdType = item.cmdType;
    ptr->archive_id = item.archive_id;
    ptr->started = item.started;

  }

}

bool WorkerSHM::detach_basebackup(unsigned int slot_index,
                                  int child_index) {

  sub_worker_info child_info;
  shm_worker_area *ptr;
  bool shortcut_still_valid = false;

  child_info = this->read(slot_index, child_index);
  child_info.backup_id = -1;
  this->write(slot_index, child_index, child_info);

  /*
   * We have to check whether the shortcut basebackup_in_use
   * is still valid.
   *
   * This means we need to loop through our child slots, checking whether
   * there are still basebackups attached. We modify the
   * basebackup_is_use flag in place.
   */
  ptr = (shm_worker_area *) (this->shm_mem_ptr + slot_index);

  for(unsigned int idx = 0; idx < MAX_WORKER_CHILDS; idx++) {

    /* As soon as we found an active basebackup, we're done */
    if (ptr->child_info[idx].backup_id > 0) {

      shortcut_still_valid = true;
      break;

    }

  }

  ptr->basebackup_in_use = shortcut_still_valid;
  return ptr->basebackup_in_use;

}

void WorkerSHM::write(unsigned int slot_index,
                      int &child_index,
                      sub_worker_info &child_info) {

  shm_worker_area *ptr = NULL;

  if ( (this->shm == nullptr)
       || (this->shm_mem_ptr == nullptr)) {
    throw SHMFailure("attempt to write worker slot from uninitialized shared memory");
  }

  /*
   * Reference the worker slot at the
   * specified slot_index. If slot_index is larger
   * than our upper index, throw.
   */
  if (slot_index > this->upper) {
    ostringstream oss;

    oss << "requested slot index " << slot_index
        << " exceeds shared memory upper limit";
    throw SHMFailure(oss.str());
  }

  ptr = (shm_worker_area *)(this->shm_mem_ptr + slot_index);

  /*
   * Check if child_index is a valid new index. If set to -1, we treat
   * it as a new entry.
   */
  if (child_index < 0) {

    /*
     * Search free child worker slot. We do this
     * in a small plain loop, MAX_WORKER_CHILDS isn't
     * so large....
     */
    for (unsigned int idx = 0; idx < MAX_WORKER_CHILDS; idx++) {

      if (ptr->child_info[idx].pid <= 0) {

        child_index = idx;
        break;

      }

    }

    /*
     * Check whether we've found one.
     */
    if (child_index < 0) {

      std::ostringstream oss;

      oss << "could not register sub worker child: out of slots";
      throw SHMFailure(oss.str());

    }

    /* Store child information into slot */
    ptr->child_info[child_index].pid = child_info.pid;
    ptr->child_info[child_index].backup_id = child_info.backup_id;

    /* Adjust parent slot information, and we're done */
    if (ptr->child_info[child_index].backup_id != -1)
      ptr->basebackup_in_use = true;

  } else {

    /* We want to modify an existing one, check the index */
    if ( (child_index < 0)
         || (child_index >= MAX_WORKER_CHILDS) ) {

      std::ostringstream oss;

      oss << "child slot index "
          << child_index
          << " exceeds allowed number of MAX_WORKER_CHILDS="
          << MAX_WORKER_CHILDS;
      throw SHMFailure(oss.str());

    }

    /* Everything looks sane, modify the entry. */
    ptr->child_info[child_index].pid = child_info.pid;
    ptr->child_info[child_index].backup_id = child_info.backup_id;

    if (child_info.backup_id != -1)
      ptr->basebackup_in_use = true;

    BOOST_LOG_TRIVIAL(debug) << "write child info done";

  }

}

bool WorkerSHM::isEmpty(unsigned int slot_index) {

  shm_worker_area *ptr;

  if ( (this->shm == nullptr)
       || (this->shm_mem_ptr == nullptr)) {
    throw SHMFailure("attempt to read worker slot from uninitialized shared memory");
  }

  if (slot_index > this->upper) {
    ostringstream oss;

    oss << "requested slot index "
        << slot_index
        << " exceeds shared memory upper limit";
    throw SHMFailure(oss.str());
  }

  ptr = (shm_worker_area *)(this->shm_mem_ptr + slot_index);
  return (ptr->pid == 0);

}

void WorkerSHM::reset() {

  shm_worker_area *ptr;

  if ( (this->shm == nullptr)
       || (this->shm_mem_ptr == nullptr)) {
    throw SHMFailure("attempt to read worker slot from uninitialized shared memory");
  }

  for (unsigned int i = 0; i <= this->upper; i++) {

    ptr = (shm_worker_area *)(this->shm_mem_ptr + i);

    if (ptr != NULL) {

      ptr->pid = 0;
      ptr->cmdType = EMPTY_DESCR;
      ptr->archive_id = -1;
      ptr->started = boost::posix_time::ptime();
      ptr->basebackup_in_use = false;

      for (int child_index = 0; child_index < MAX_WORKER_CHILDS; child_index++) {

        ptr->child_info[child_index].pid = -1;
        ptr->child_info[child_index].backup_id = -1;

      }

    }

  }

}

void WorkerSHM::free_child_by_pid(unsigned int slot_index,
                                  pid_t child_pid) {

  shm_worker_area *ptr = nullptr;
  unsigned int free_idx;

  if ( (this->shm == nullptr)
       || (this->shm_mem_ptr == nullptr)) {
    throw SHMFailure("attempt to read worker slot from uninitialized shared memory");
  }

  /* PID <= 0 means no-op */
  if (child_pid <= 0)
    return;

  if (slot_index > this->upper) {
    ostringstream oss;

    oss << "requested slot index "
        << slot_index
        << " exceeds shared memory upper limit";
    throw SHMFailure(oss.str());
  }

  /* Get the worker slot */
  ptr = (shm_worker_area *)(this->shm_mem_ptr + slot_index);

  /*
   * We need to search the child_pid
   * within our child_info array. Since the array
   * isn't considered to grow very large, just use
   * plain stupid for loop.
   *
   * We don't free the child slot directly here, since
   * we are going to call free_child(), which would mean
   * to have to indirectly iterate over the child_info
   * array again. So just remember the pid and do the
   * call afterwards.
   */
  for (unsigned int idx = 0; idx < MAX_WORKER_CHILDS; idx++) {

    if (ptr->child_info[idx].pid == child_pid) {

      free_idx = idx;
      break;

    }

  }

  if (free_idx >= 0)
    free_child(slot_index, free_idx);

}

void WorkerSHM::free_child(unsigned int slot_index,
                           unsigned int child_index) {

  unsigned int free_child_index = child_index;
  shm_worker_area *ptr = nullptr;
  bool basebackup_in_use = false;

  if ( (this->shm == nullptr)
       || (this->shm_mem_ptr == nullptr)) {
    throw SHMFailure("attempt to read worker slot from uninitialized shared memory");
  }

  if (slot_index > this->upper) {
    ostringstream oss;

    oss << "requested slot index "
        << slot_index
        << " exceeds shared memory upper limit";
    throw SHMFailure(oss.str());
  }

  if (free_child_index >= MAX_WORKER_CHILDS) {

    std::ostringstream oss;

    oss << "cannot free child index "
        << child_index
        << ", exceeding MAX_WORKER_CHILDS="
        << MAX_WORKER_CHILDS;

    throw SHMFailure(oss.str());

  }

  /* Get the worker slot */
  ptr = (shm_worker_area *)(this->shm_mem_ptr + slot_index);

  /*
   * Since we might call this method lockless,
   * make sure the pid stays the first modification here.
   */
  ptr->child_info[free_child_index].pid = 0;

  ptr->child_info[free_child_index].backup_id = -1;

  /*
   * We need to check whether any backup ID is still registered
   * within the current child slots. Loop through the list (which
   * is small, so this isn't a caveat here).
   */
  for (unsigned int idx = 0; idx < MAX_WORKER_CHILDS; idx++) {

    if (ptr->child_info[idx].backup_id >= 0) {
      ptr->basebackup_in_use = true;

      /* exit, since we have the info we need */
      break;
    }

  }

  ptr->basebackup_in_use = basebackup_in_use;

}

void WorkerSHM::free(unsigned int slot_index) {

  shm_worker_area *ptr;

  if ( (this->shm == nullptr)
       || (this->shm_mem_ptr == nullptr)) {
    throw SHMFailure("attempt to read worker slot from uninitialized shared memory");
  }

  if (slot_index > this->upper) {
    ostringstream oss;

    oss << "requested slot index "
        << slot_index
        << " exceeds shared memory upper limit";
    throw SHMFailure(oss.str());
  }

  ptr = (shm_worker_area *)(this->shm_mem_ptr + slot_index);

  ptr->pid = 0;
  ptr->cmdType = EMPTY_DESCR;
  ptr->archive_id = -1;
  ptr->started = boost::posix_time::ptime();
  ptr->basebackup_in_use = false;

  for (int child_index = 0; child_index < MAX_WORKER_CHILDS; child_index++) {

    ptr->child_info[child_index].pid = -1;
    ptr->child_info[child_index].backup_id = -1;

  }

  this->allocated--;

}

unsigned int WorkerSHM::allocate(shm_worker_area &item) {

  unsigned int result = 0;

  if ( (this->shm == nullptr)
       || (this->shm_mem_ptr == nullptr)) {
    throw SHMFailure("attempt to read worker slot from uninitialized shared memory");
  }

  result = this->getFreeIndex();
  this->write(result, item);
  this->allocated++;

  return result;

}

unsigned int WorkerSHM::getFreeIndex() {

  shm_worker_area *ptr = nullptr;

  if ( (this->shm == nullptr)
       || (this->shm_mem_ptr == nullptr)) {
    throw SHMFailure("attempt to read worker slot from uninitialized shared memory");
  }

  for (unsigned int i = 0; i <= this->upper; i++) {

    ptr = (shm_worker_area *)(this->shm_mem_ptr + i);

    if (ptr->pid == 0) {
      return i;
    }
  }

  throw SHMFailure("no worker slot available");

}

sub_worker_info WorkerSHM::read(unsigned int slot_index,
                                unsigned int child_index) {
  shm_worker_area *ptr = NULL;
  sub_worker_info result;

  if ( (this->shm == nullptr)
       || (this->shm_mem_ptr == nullptr)) {
    throw SHMFailure("attempt to write worker slot from uninitialized shared memory");
  }

  /*
   * Reference the worker slot at the
   * specified slot_index. If slot_index is larger
   * than our upper index, throw.
   */
  if (slot_index > this->upper) {
    ostringstream oss;

    oss << "requested slot index " << slot_index
        << " exceeds shared memory upper limit";
    throw SHMFailure(oss.str());
  }

  ptr = (shm_worker_area *)(this->shm_mem_ptr + slot_index);

  /**
   * Extract child info slot information, if valid
   * child_index present.
   */
  if (ptr != NULL) {

    if (child_index >= 0 || child_index < MAX_WORKER_CHILDS) {

      /* Copy information and we're done */
      result = ptr->child_info[child_index];

    } else {

      std::ostringstream oss;

      oss << "child slot index("
          << child_index
          << ") out of bounds";
      SHMFailure(oss.str());

    }

  }

  return result;

}

shm_worker_area WorkerSHM::read(unsigned int slot_index) {

  shm_worker_area result;
  shm_worker_area *ptr;

  if ( (this->shm == nullptr)
       || (this->shm_mem_ptr == nullptr)) {
    throw SHMFailure("attempt to read worker slot from uninitialized shared memory");
  }

  if (slot_index > this->upper) {
    ostringstream oss;

    oss << "requested slot index "
        << slot_index
        << " exceeds shared memory upper limit";
    throw SHMFailure(oss.str());
  }

  ptr = (shm_worker_area *)(this->shm_mem_ptr + slot_index);
  result = (*ptr);

  return result;

}

void WorkerSHM::detach() {

  if (this->shm != nullptr) {

    /*
     * Local pointers are NULL now.
     */
    this->upper = 0;
    this->mtx = nullptr;
    this->shm_mem_ptr = nullptr;

    /*
     * Now detach. We don't remove it
     * physically from the kernel here.
     */
    delete this->shm;
    this->shm = nullptr;
  }

}

void WorkerSHM::setMaxWorkers(unsigned int max_workers) {

  this->max_workers = max_workers;

}

unsigned int WorkerSHM::getMaxWorkers() {

  return this->max_workers;

}

size_t WorkerSHM::getSize() {

  /*
   * If not attached, just calculate the shared memory
   * size as it would be if we are attached.
   */
  if (this->shm == nullptr) {
    return this->calculateSHMsize();
  } else {
    return this->size;
  }

}

/******************************************************************************
 * LauncherSHM & objects implementation start
 ******************************************************************************/

LauncherSHM::LauncherSHM() : ProcessSHM() {

  this->shm = nullptr;

}

LauncherSHM::~LauncherSHM() {

  if (this->shm != nullptr) {
    delete this->shm;
  }

}

size_t LauncherSHM::getSize() {
  return this->size;
}

void LauncherSHM::detach() {

  if (this->shm != nullptr) {

    /*
     * Local pointers into shared memory
     * are now undefined.
     */
    this->mtx = nullptr;
    this->shm_mem_ptr = nullptr;

    /*
     * detach the shared memory segment.
     *
     * NOTE: This will *NOT* remove the shared memory
     *       physically from the system. The caller must
     *       call managed_xsi_shared_memory::remove()
     *       explictly.
     */
    delete this->shm;
    this->shm = nullptr;
  }

}

bool LauncherSHM::attach(std::string catalog, bool attach_only) {

  using namespace boost::interprocess;

  /*
   * Key referencing the shared memory segment.
   */
  xsi_key key(catalog.c_str(), 1);
  std::ostringstream shm_ctl_name;
  std::ostringstream mtx_ctl_name;
  bool result = false;

  if (this->shm != nullptr) {
    /* already initialized, nothing to do */
    return true;
  }

  /*
   * Internal handle not yet initialized, trying
   * to either a) attach or b) create, attach
   */
  if (attach_only) {

    try {
      this->shm = new managed_xsi_shared_memory(open_only, key);
    } catch (interprocess_exception &ie) {

      if (ie.get_error_code() == not_found_error) {
        result = false;
        return result;
      }
      else {
        throw ie;
      }
    }

  } else {
    this->shm = new managed_xsi_shared_memory(open_or_create,
                                              key,
                                              this->size);
  }

  /*
   * Stick mutex for access control into
   * shared memory.
   */
  mtx_ctl_name << catalog << "_mtx";
  this->mtx = this->shm->find_or_construct<interprocess_mutex>(mtx_ctl_name.str().c_str())();

  /*
   * Create control area in shared memory.
   */
  shm_ctl_name << catalog << "_shm_area";
  this->shm_mem_ptr = this->shm->find_or_construct<shm_launcher_area>(shm_ctl_name.str().c_str())();

  /*
   * Don't forget to remember the generated
   * SHM area identifier.
   */
  this->shm_ident = shm_ctl_name.str();
  this->shm_key = key.get_key();

  /* everything looks good ... */
  result = true;

  return result;
}

pid_t LauncherSHM::setPID(pid_t pid) {

  if (this->shm == nullptr) {
    throw SHMFailure("attemp to write PID into uninitialized shared memory");
  }

  this->mtx->lock();
  this->shm_mem_ptr->pid = pid;
  this->mtx->unlock();

  return this->shm_mem_ptr->pid;
}

/******************************************************************************
 * LauncherSHM implementation end
 ******************************************************************************/

static void _pgbckctl_sigchld_handler(int sig) {

  pid_t pid;
  int   wait_status;

  /* Only react on SIGCHLD */
  if (sig != SIGCHLD)
    return;

  while ((pid = waitpid(-1, &wait_status, WNOHANG)) != -1) {

    if (WIFSIGNALED(wait_status)) {

      /*
       * child terminated by signal
       *
       * Handle SIGKILL, since this usually means the
       * worker didn't clean its shared memory state.
       *
       * Be aware that we only react in case this is
       *
       * a) a background launcher process
       * b) a valid runnable_workers pidset was allocated
       * c) the launcher is in LAUNCHER_RUN state.
       *
       * The latter, c) is also recognized by _pgbckctl_job_type
       * when set to BACKGROUND_LAUNCHER.
       */
      if ( (_pgbckctl_job_type == BACKGROUND_LAUNCHER)
           && ( launcher_reaper != nullptr) ) {

        if ( (WTERMSIG(wait_status) == SIGKILL)
             || ( WTERMSIG(wait_status) == SIGABRT) ) {

          /*
           * This child PID died an horrible death, we need
           * to reap it out from the shared memory segment.
           *
           * We can't do this here, since the signal handlers
           * don't have access to the internal worker handlers. Instead,
           * we use a stack to remember dead pids and reap them out
           * during command processing in the launcher itself. The
           * launcher is then responsible to clear the corresponding
           * PID entry.
           */
          launcher_reaper->dead_pids.push(pid);

        }

      }

    }

    /*
     * Check status of exited child.
     */
    if (WIFEXITED(wait_status)) {

      /* child terminated via exit() */
      break;
    }

  }

}

/*
 * pg_backup_ctl launcher signal handler
 */
static void _pgbckctl_sighandler(int sig) {

  /*
   * Check whether we are called recursively. Reraise
   * the signal again, if true.
   */
  if (_pgbckctl_shutdown_mode != DAEMON_RUN) {
    /* raise(sig); */
  }

  if (sig == SIGTERM) {
    _pgbckctl_shutdown_mode = DAEMON_TERM_NORMAL;
  }

  if (sig == SIGINT) {
    _pgbckctl_shutdown_mode = DAEMON_TERM_EMERGENCY;
  }

  if (sig == SIGQUIT) {
    _pgbckctl_shutdown_mode = DAEMON_TERM_EMERGENCY;
  }

  if (sig == SIGHUP) {
    /* not yet */
  }

  if (sig == SIGUSR1) {
    _pgbckctl_shutdown_mode = DAEMON_STATUS_UPDATE;
  }

}

static pid_t daemonize(job_info &info) {

  /*
   * Holds PID of parent and child process
   */
  pid_t pid;
  pid_t sid;
  int forkerrno;

  /*
   * First at all, fork off the launcher. This
   * parent will fork again to launch the actual
   * background process and will exit.
   */

  if (info.detach) {

    BOOST_LOG_TRIVIAL(info) << "parent launcher detaching";

    pid = fork();
    forkerrno = errno;

    if (pid < 0) {
      std::ostringstream oss;
      oss << "fork error " << strerror(forkerrno);
      throw LauncherFailure(oss.str());
    }

    if (pid > 0) {
      BOOST_LOG_TRIVIAL(info) << "parent launcher forked with pid " << pid << ", detaching";
      waitpid(pid, NULL, WNOHANG);

      /*
       * This will force to return to the caller, child
       * will resume to fork() specific worker process...
       */
      info.pid = pid;
      return pid;
    }

  }

  /*
   * The background process will have the following
   * conditions set:
   *
   * - Change into the archive directory
   * - Close all STDIN and STDOUT file descriptors,
   *   if requested
   */
  if (info.close_std_fd) {
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
  }

  /*
   * Install signal handlers.
   */
  signal(SIGTERM, _pgbckctl_sighandler);
  signal(SIGINT, _pgbckctl_sighandler);
  signal(SIGQUIT, _pgbckctl_sighandler);
  signal(SIGHUP, _pgbckctl_sighandler);
  signal(SIGUSR1, _pgbckctl_sighandler);
  signal(SIGCHLD, _pgbckctl_sigchld_handler);

  /*
   * Close the catalog handler here. It will
   * be opened again in the final launcher process.
   */
  info.cmdHandle->getCatalog()->close();

  /*
   * Now launch the background job.
   */
  pid = fork();
  forkerrno = errno;

  if (pid < 0) {
    std::ostringstream oss;
    oss << "fork error " << strerror(forkerrno);
    throw LauncherFailure(oss.str());
  }

  if (pid > 0) {

    /*
     * This is the launcher's parent process.
     *
     * Set the session  leader.
     */
    if (info.detach) {
      sid = setsid();
      if (sid < 0) {
        std::ostringstream oss;
        oss << "could not set session leader: " << strerror(errno);
        throw LauncherFailure(oss.str());
      }
    }

    /*
     * Launcher processing loop.
     */
    do {

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_NORMAL) {
        BOOST_LOG_TRIVIAL(info) << "launcher shutdown request received";
        break;
      }

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_EMERGENCY) {
        BOOST_LOG_TRIVIAL(info) << "launcher emergency shutdown request received";
        break;
      }

      /*
       * Detach, after that, the child run's
       * as a daemon.
       */
      if (info.detach)
        break;
      usleep(10000);

    } while(true);

    exit(_pgbckctl_shutdown_mode);
  }

  if (pid == 0) {

    /*
     * This is the initial background worker (aka launcher) process
     */
    BackgroundWorker worker(info);
    WorkerSHM *worker_shm = worker.workerSHM();

    /*
     * Flag, indicating that we got a valid command string
     * from the msg queue.
     */
    bool cmd_ok;

    /*
     * If initialization got exit signal, exit. Do this here before
     * doing any initialization stuff.
     */
    if ( (_pgbckctl_shutdown_mode == DAEMON_TERM_EMERGENCY)
         || (_pgbckctl_shutdown_mode == DAEMON_TERM_NORMAL) ) {

         exit(_pgbckctl_shutdown_mode);

    }

    /*
     * Register launcher in the database.
     */
    worker.initialize();

    /* mark this as a background worker */
    info.background_exec = true;

#ifdef __DEBUG__
    BOOST_LOG_TRIVIAL(debug) << "initialize child reaper handler";
#endif

    /*
     * Create and initialize the dead man's reaper...
     */
    launcher_reaper = new background_worker_shm_reaper();
    dynamic_cast<background_worker_shm_reaper *>(launcher_reaper)->set_shm_handle(worker_shm);
    worker.assign_reaper(launcher_reaper);

    BOOST_LOG_TRIVIAL(warning) << "reset worker shared memory area" << endl;

    /*
     * Reset shared memory area, but only if no
     * workers are currently attached. This is a sanity check
     * in case the launcher has crashed somehow, but its workers
     * survived somehow. We don't consider this an error, but
     * give the caller a warning.
     *
     * We also need to be careful here with shared memory
     * interlocking!
     */
    worker_shm->lock();

    if (worker_shm->getNumberOfAttached() > 1) {

      BOOST_LOG_TRIVIAL(warning) << "ERROR: cannot re-initialize worker shared memory:" << endl
                                 << "       there are still workers attached, you need to terminate them first";

    } else {

      /* re-initialize all worker slots to be empy */
      try {

        worker_shm->reset();

      } catch(exception &e) {

        /*
         * Whoops, this failed somehow, this is considered
         * a hard error.
         */
        BOOST_LOG_TRIVIAL(fatal) << "failure resetting worker SHM: " << e.what();
        worker_shm->unlock();
        exit(DAEMON_FAILURE);

      }

    }

    worker_shm->unlock();

    BOOST_LOG_TRIVIAL(info) << "reset worker shared memory area done";

    /* Mark launcher process.
     *
     * This will instruct the signal handlers (e.g. SIGCHLD) to keep
     * track of child PIDs forked from this process.
     *
     * We do this after having done all necessary setup work
     * to make sure everything required is in place. Since signal
     * handlers don't have access to the internal state
     * of the worker handles, this must occure *AFTER* everything
     * required is allocated, especially the workers_running
     * PID set.
     */
    _pgbckctl_job_type = BACKGROUND_LAUNCHER;

    /*
     * Setup message queue.
     */
    establish_launcher_cmd_queue(info);

    /*
     * Mark background worker running.
     */
    worker.run();

    /*
     * Enter processing loop
     */
    while(true) {

      /*
       * Reap dead workers, if any.
       */
      worker.execute_reaper();

      usleep(1000);

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_NORMAL) {
        BOOST_LOG_TRIVIAL(info) << "shutdown request received";

        /*
         * This is considered a smart shutdown, where we
         * clean up all stuff which seems to be necessary for
         * clean startup again.
         */
        try {
          worker.prepareShutdown();
        } catch (std::exception& e) {
          BOOST_LOG_TRIVIAL(error) << "smart shutdown catched error: " << e.what();
        }
        break;
      }

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_EMERGENCY) {
        BOOST_LOG_TRIVIAL(info) << "emergency shutdown request received";
        break;
      }

      /*
       * Check message queue whether there is something to do.
       */
      std::string command = recv_launcher_cmd(info, cmd_ok);

      if (cmd_ok) {

        /*
         * We got a command string. Establish a command
         * handler and proceed. We don't do this ourselves, but
         * fork off a new command process to actually execute the
         * command.
         */
        try {

          pid_t worker_pid;

          BOOST_LOG_TRIVIAL(debug) << "BACKGROUND COMMAND: " << command;

          /*
           * Execute the command.
           *
           * worker_command() forks a new child process to
           * execute the command, so make sure we exit after
           * worker_command() returns.
           */
          if ((worker_pid = worker_command(worker, command)) == (pid_t) 0) {

            /* child should exit after having done its duty */
            exit(0);
          } else if (worker_pid < 0) {

            /*
             * A negative value of worker_pid indicates that the initial setup
             * of worker_command() and the specified job_handle had failed.
             */
            BOOST_LOG_TRIVIAL(fatal) << "launcher cannot fork worker process: worker setup failed";

          } else {

            BOOST_LOG_TRIVIAL(info) << "launcher forked worker process at PID " << worker_pid;

          }

        } catch (CParserIssue &pe) {

          BOOST_LOG_TRIVIAL(error) << "parser failed: " << pe.what();
          exit(DAEMON_FAILURE);

        } catch (WorkerFailure &e) {

          /* something shitty has happend ... */
          BOOST_LOG_TRIVIAL(fatal) << "fork() failed: " << e.what();
          exit(DAEMON_FAILURE);

        } catch (TCPServerFailure &tsrve) {

          BOOST_LOG_TRIVIAL(fatal) << "could not instantiate TCP streaming server: " << tsrve.what();
          exit(DAEMON_FAILURE);

        } catch (std::exception &e) {

          BOOST_LOG_TRIVIAL(fatal) << "background worker failure: " << e.what();
          exit(DAEMON_FAILURE);

        }

      }

    }

    exit(_pgbckctl_shutdown_mode);
  } /* child execution code */

  info.pid = pid;
  return pid;

}

bool credativ::launcher_is_running(std::shared_ptr<CatalogProc> procInfo) {

  shmatt_t nattach;

  try {
    if (procInfo != nullptr && procInfo->pid > 0) {

      /*
       * If we get a SHMFailure exception here, then the shmid
       * stored in the catalog doesn't exist anymore. In this case
       * just go further and start the launcher.
       */
      nattach = LauncherSHM::getNumberOfAttached(procInfo->shm_id);

    }
  } catch(SHMFailure &e) {

    /*
     * A SHMFailure here usually means the request shmid
     * doesn't exist or isn't readable anymore. This
     * indicates a crashed launcher instance, since the
     * catalog entry is orphaned.
     */
    nattach = 0;

    BOOST_LOG_TRIVIAL(warning) << "WARNING: catalog shm id "
                               << procInfo->shm_id
                               << " is orphaned";
  }

  return ((nattach > 0) ? true : false);

}

void credativ::establish_launcher_cmd_queue(job_info& info) {

  std::string message_queue_name;

  using namespace boost::interprocess;

  try {

    if (info.command_queue == nullptr) {

      /*
       * Create or open/attach the specified message queue.
       * Be aware that we need to choose the correct one, since
       * every launcher and workers are catalog related.
       */
      message_queue_name = "pg_backup_ctl::command_queue::"
        + info.cmdHandle->getCatalog()->name();

      info.command_queue = new message_queue(open_or_create,
                                             message_queue_name.c_str(),
                                             255,
                                             MSG_QUEUE_MAX_TOKEN_SZ);
    }

  } catch(interprocess_exception &e) {

    /*
     * Caller shouldn't deal with interprocess_exception directly,
     * so map it to LauncherFailure.
     */
    throw LauncherFailure(e.what());
  }

}

void credativ::send_launcher_cmd(job_info& info, std::string command) {

  using namespace boost::interprocess;

  /*
   * Having a uninitialized message queue forces
   * a generic failure.
   */
  if (info.command_queue == nullptr) {
    throw CPGBackupCtlFailure("could not send command with uninitialized message queue");
  }

  /*
   * If command string has zero length, omit the command.
   */
  if (command.length() <= 0)
    return;

  /*
   * Command can't be larger than MSG_QUEUE_MAX_TOKEN_SZ.
   */
  if (command.length() > MSG_QUEUE_MAX_TOKEN_SZ) {
    throw CPGBackupCtlFailure("token size exceeds max message queue token size");
  }

  try {

    /*
     * interprocess_exception is not recognized
     * by the pg_backup_ctl++ API, map it to a CPGBackupCtlFailure
     * instead.
     */

    if (!info.command_queue->try_send(command.data(), command.length(), 0)) {
      throw CPGBackupCtlFailure("timeout while sending message into message queue");
    }

  } catch(interprocess_exception &e) {
    throw CPGBackupCtlFailure(e.what());
  }
}

std::string credativ::recv_launcher_cmd(job_info &info, bool &cmd_received) {

  using namespace boost::interprocess;

  std::string command = "";

  /*
   * Having a uninitialized message queue forces
   * a generic failure.
   */
  if (info.command_queue == nullptr) {
    throw CPGBackupCtlFailure("could not send command with uninitialized message queue");
  }

  try {

    /*
     * Recv buffer. Holds up to MSG_QUEUE_MAX_TOKEN_SZ bytes.
     */
    char recvbuffer[MSG_QUEUE_MAX_TOKEN_SZ];
    message_queue::size_type recv_size;
    unsigned int prio;

    memset(recvbuffer, 0, MSG_QUEUE_MAX_TOKEN_SZ);
    if (info.command_queue->try_receive(&recvbuffer, MSG_QUEUE_MAX_TOKEN_SZ, recv_size, prio)) {
      cmd_received = true;
    } else {
      cmd_received = false;
    }

    command = recvbuffer;

  } catch(interprocess_exception &e) {
    throw CPGBackupCtlFailure(e.what());
  }

  return command;
}

/**
 * Launches a fully subprocess suitable to
 * execute background processes. This shouldn't be called
 * multiple times per catalog instance, since they are going
 * to share the same message queue otherwise.
 */
pid_t credativ::launch(job_info& info) {

  daemonize(info);
  return info.pid;

}

/**
 * worker_command() runs from the launcher and
 * forks a new process executing the
 * the command passed to the job_handle hold by the specified worker instance.
 *
 * If info doesn't hold a proper command handle, nothing will
 * be forked or executed (so this is effectively
 * a no-op then).
 *
 * The command passed to worker_command() should be
 * a command understood by the PGBackupCtlParser. Thus, the
 * caller should be prepared to handle parser errors.
 *
 * We don't care if the job_handle hold by the worker process
 * is set to background_exec, as we do in run_process. The specified
 * command handle will always be executed in a separate process.
 */
pid_t credativ::worker_command(BackgroundWorker &worker, std::string command) {

  job_info info = worker.jobInfo();
  pid_t pid;

  if (info.cmdHandle == nullptr)
    return (pid_t) -1;

  if ((pid = fork()) == (pid_t) 0) {

    /* Worker child */

    PGBackupCtlParser parser;
    std::shared_ptr<PGBackupCtlCommand> bgrnd_cmd_handler;
    JobSignalHandler *cmdSignalHandler;
    WorkerSHM *worker_shm;
    unsigned int worker_slot_index = 0;
    shm_worker_area worker_info;

    /*
     * Make sure we have the right background job context.
     */
    _pgbckctl_job_type = BACKGROUND_WORKER;

    /*
     * Reset SIGCHLD signal handler. Not required
     * in a background worker process.
     */
    signal(SIGCHLD, SIG_DFL);

    /*
     * Tell our background worker handle that we
     * aren't longer a launcher instance.
     */
    worker.release_launcher_role();

    BOOST_LOG_TRIVIAL(info) << "background job executing command " << command;

    worker_info.pid = ::getpid();
    worker_info.started = CPGBackupCtlBase::ISO8601_strTo_ptime(CPGBackupCtlBase::current_timestamp());

    /*
     * Parse command.
     */
    parser.parseLine(command);

    /*
     * Parser has instantiated a command handler
     * iff success.
     */
    bgrnd_cmd_handler = parser.getCommand();

    /*
     * Remember parsed command tag.
     */
    worker_info.cmdType = bgrnd_cmd_handler->getCommandTag();

    /*
     * If the PGBackupCtlCommand handler encapsulates a
     * command attached to an archive, we record the archive id
     * in the shared memory, too.
     */
    if (bgrnd_cmd_handler->archive_name().length() > 0) {

      /*
       * catalog access can throw here, don't suppress errors
       * at this point but remap that to a WorkerFailure exception.
       */
      try {

        std::shared_ptr<CatalogDescr> temp_descr
          = info.cmdHandle->getCatalog()->existsByName(bgrnd_cmd_handler->archive_name());

        if (temp_descr->id >= 0) {
          worker_info.archive_id = temp_descr->id;
        }

      } catch(CPGBackupCtlFailure &e) {

        throw WorkerFailure(e.what());

      }
    }

    /*
     * Set signal handlers.
     */
    cmdSignalHandler = dynamic_cast<JobSignalHandler *>(termHandler);
    bgrnd_cmd_handler->assignSigStopHandler(cmdSignalHandler);

    cmdSignalHandler = dynamic_cast<JobSignalHandler *>(emergencyHandler);
    bgrnd_cmd_handler->assignSigIntHandler(cmdSignalHandler);

    /*
     * Now it's time to execute the command. Since everything is setup now,
     * it's overdue to register the forked process into the
     * worker shared memory area. This needs to be done in a critical
     * section, to protect us against concurrent workers doing the same.
     */
    worker_shm = worker.workerSHM();
    if (!worker_shm->attach(info.cmdHandle->getCatalog()->fullname(), true)) {
      /* could not attach to shared memory segment */
      throw WorkerFailure("could not attach to worker shared memory area");
    }

    /*
     * WorkerSHM::allocate() can throw, but since
     * the real memory allocation is done before we're
     * probably safe here.
     */
    worker_shm->lock();
    worker_slot_index = worker_shm->allocate(worker_info);
    worker_shm->unlock();

#ifdef __DEBUG__
    BOOST_LOG_TRIVIAL(debug) << "WORKER SLOT " << worker_slot_index;
#endif

    /* Save worker slot index as its worker ID to command handler */
    bgrnd_cmd_handler->setWorkerID(worker_slot_index);

    try {
      bgrnd_cmd_handler->execute(info.cmdHandle->getCatalog()->fullname());
    } catch(exception &e) {

      /*
       * In any case, detach from the shared memory but clear
       * our slot before, but only if this is *NOT* a BACKGROUND_WORKER_CHILD.
       *
       * Background: BaseCatalogCommand derived classes might fork within
       * their execute() methods, to employ additional child processes. To
       * make sure they don't clear the Worker SHM, they set the
       * background job type flag to BACKGROUND_WORKER_CHILD. It's okay
       * to just test for that flag, since others are unlikely to occur here.
       *
       * WorkerSHM::free() can throw itself if there's no valid shared
       * memory handle here. But that seems unlikely, since the
       * actions before should have failed before.
       */
      if (_pgbckctl_job_type != BACKGROUND_WORKER_CHILD) {
        worker_shm->lock();
        worker_shm->free(worker_slot_index);
        worker_shm->unlock();
      }

      /* re-throw */
      throw WorkerFailure(e.what());

    }

    /* only reached if everything went okay */
    if (_pgbckctl_job_type != BACKGROUND_WORKER_CHILD) {

      worker_shm->lock();
      worker_shm->free(worker_slot_index);
      worker_shm->unlock();

    }

#ifdef __DEBUG__
    BOOST_LOG_TRIVIAL(debug) << "WORKER EXIT";
#endif

    /* Exit, if done */
    exit(0);

  } else if (pid < (pid_t) 0) {

    /*
     * fork() error, this is severe, so report
     * that by throwing a worker exception. This
     * affects the launcher process directly!
     */
    std::ostringstream oss;

    oss << "could not fork new worker: " << strerror(errno);
    throw WorkerFailure(oss.str());

  } else {

    /*
     * Launcher process, here's actually
     * nothing to do.
     */

  }

  return pid;
}

/**
 * run_pipelined_command() is some kind special
 * compared to run_process in that it employs
 * popen() to establish a unidirectional pipe.
 *
 * This also allows to pass down complex commands
 * with shell interactions, e.g. redirect et al.
 */
FILE * credativ::run_pipelined_command(job_info &info) {

  ostringstream cmd;

  if (!info.background_exec)
    throw WorkerFailure("running a pipelined command requires background execution");

  cmd << info.executable.string();

  /*
   * The exec arguments must be iterated to create
   * the command string.
   */
  for(unsigned int i = 0; i < info.execArgs.size(); i++) {
    if (i <= info.execArgs.size() - 1) {
      cmd << " ";
    }

    cmd << info.execArgs[i];
  }

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "DEBUG: executing " << cmd.str();
#endif

  info.fpipe_handle = ::popen(cmd.str().c_str(), info.po_mode.c_str());

  if (info.fpipe_handle == NULL) {

    ostringstream oss;

    oss << "could not execute popen(): " << strerror(errno);
    throw WorkerFailure(oss.str());

  }

  return info.fpipe_handle;
}

/**
 * run_process is supposed to run an executable
 * in a background process via execv() and
 * a bidirectional pipe where the caller
 * can write to and read from.
 */
pid_t credativ::run_process(job_info& info) {

  pid_t pid;

  /*
   * run_process() requires background job preparation.
   */
  if (!info.background_exec)
    throw WorkerFailure("running a process needs background execution");

  /*
   * Prepare pipe if requested. This must be happening
   * in the caller before we fork our background job.
   */
  if (info.use_pipe) {

    /*
     * Output pipe, used to read FROM STDIN in the child
     */
    if (::pipe(info.pipe_in) < 0) {
      std::ostringstream oss;
      oss << "failed to initialize parent output pipe: " << strerror(errno);
      throw WorkerFailure(oss.str());
    }

    if (::pipe(info.pipe_out) < 0) {
      std::ostringstream oss;
      oss << "failed to initialize parent input pipe: " << strerror(errno);
      throw WorkerFailure(oss.str());
    }

  }

  /*
   * Do the fork() ...
   */
  if ((pid = fork()) == (pid_t) 0) {

    /*
     * This is the child process, actually executing the executable.
     */
    string execstr = info.executable.string();

    /*
     * Create array for arguments. reserve two extra
     * pointers, since we store the executable name *and*
     * the finalizing NULL there, too
     */
    char *args[info.execArgs.size() + 2];

    /*
     * This is a background worker job.
     */
    _pgbckctl_job_type = BACKGROUND_WORKER;

    args[0] = new char[execstr.length() + 1];
    memset(args[0], '\0', execstr.length() + 1);
    strncpy(args[0], execstr.c_str(), execstr.length());

    for(unsigned int i = 1; i <= info.execArgs.size(); i++) {
      std::string item = info.execArgs[i - 1];

      args[i] = new char[item.length() + 1];
      memset(args[i], '\0', item.length() + 1);
      strncpy(args[i], item.c_str(), item.length());
    }

    args[info.execArgs.size() + 1] = NULL;

    if (info.use_pipe) {

      /*
       * pipe_in is STDIN, pipe_out is STDOUT
       */
      close(info.pipe_in[1]);
      close(info.pipe_out[0]);

      if (info.pipe_in[0] != STDIN_FILENO) {

        if (dup2(info.pipe_in[0], STDIN_FILENO) != STDIN_FILENO)
          throw WorkerFailure("could not bind pipe to STDIN");

        close(info.pipe_in[0]);
      }

      if (info.pipe_out[1] != STDOUT_FILENO) {

        if (dup2(info.pipe_out[1], STDOUT_FILENO) != STDOUT_FILENO)
          throw WorkerFailure("could not bind pipe to STDOUT");

        close(info.pipe_out[1]);
      }

    }

    if (info.close_std_fd) {

      close(STDIN_FILENO);
      close(STDOUT_FILENO);
      close(STDERR_FILENO);

    }

    if (::execv(execstr.c_str(), args) < 0) {
      std::ostringstream oss;
      oss << "error executing "
          << info.executable.string()
          << ": "
          << strerror(errno);
      BOOST_LOG_TRIVIAL(error) << oss.str();
      exit(-1);
    }

  } else if (pid < (pid_t) 0) {

    /*
     * Whoops, something went wrong here.
     */
    std::ostringstream oss;
    oss << "error forking process for executable "
        << info.executable << ": "
        << strerror(errno);
    throw WorkerFailure(oss.str());
  } else {

    /* Parent process */

    /*
     * Caller closes it's pipe fd's
     */
    close(info.pipe_in[0]);
    close(info.pipe_out[1]);

  }

  return pid;
}
