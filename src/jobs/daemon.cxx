#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <syslog.h>
#include <string.h>
#include <signal.h>
#include <istream>

#ifdef __DEBUG__
#include <iostream>
#endif

#include <commands.hxx>
#include <daemon.hxx>

using namespace credativ;

volatile sig_atomic_t _pgbckctl_shutdown_mode = DAEMON_RUN;
extern int errno;

/*
 * Forwarded declarations.
 */
static pid_t daemonize(job_info &info);

BackgroundWorker::BackgroundWorker(job_info info) {

  this->ji = info;
  this->catalog = this->ji.cmdHandle->getCatalog();
  this->procInfo = std::make_shared<CatalogProc>();

}

BackgroundWorker::~BackgroundWorker() {

}

bool BackgroundWorker::checkPID(pid_t pid) {

  bool result = true;

#ifdef __DEBUG__
  cerr << "checkPID(): checking PID " << pid << endl;
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
      cerr << "checkPID(): " << pid << " does not exist" << endl;
#endif
      result = false;
    }

    if (rc == EPERM) {
      /* No permissions for this PID ... */
#ifdef __DEBUG__
      cerr << "checkPID(): " << pid << " permission denied" << endl;
#endif
      result = false;
    }

    return result;
  }

  return result;
}

void BackgroundWorker::prepareShutdown() {

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
}

void BackgroundWorker::registerMe() {

  std::shared_ptr<CatalogProc> tempProc;

  /*
   * NOTE: We don't catch any exceptions here,
   * this is done by the initialize() caller, since
   * we remap those errors into a WorkerFailure exception.
   */

  /*
   * Register a dummy worker for now. This means
   * this worker is *not* associated with a specific
   * archive.
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
   *   We need to try hard to check, wether this process is
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
  procInfo->type = CatalogProc::PROC_TYPE_WORKER;
  procInfo->state = CatalogProc::PROC_STATUS_RUNNING;
  procInfo->started = CPGBackupCtlBase::current_timestamp();

  /*
   * Check if there is a catalog entry for this kind
   * of worker.
   */
  catalog->startTransaction();

  try {
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
        catalog->commitTransaction();
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

    catalog->registerProc(procInfo);
  } catch (std::exception& e) {

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
  cerr << "worker PID " << getpid()
       << " command tag " << this->ji.cmdHandle->tag << endl;
  cerr << "worker catalog " << this->ji.cmdHandle->getCatalog()->name() << endl;
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

    this->registerMe();

  } catch(std::exception& e) {

#ifdef __DEBUG__
    cerr << "error forcing shutdown: "
         << e.what()
         << endl;
#endif

    _pgbckctl_shutdown_mode = DAEMON_TERM_EMERGENCY;
  }

}

/*
 * pg_backup_ctl launcher signal handler
 */
static void _pgbckctl_sighandler(int sig) {

  /*
   * Check wether we are called recursively. Reraise
   * the signal again, if true.
   */
  if (_pgbckctl_shutdown_mode != DAEMON_RUN) {
    raise(sig);
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

    cout << "parent launcher detaching" << endl;

    pid = fork();
    forkerrno = errno;

    if (pid < 0) {
      std::ostringstream oss;
      oss << "fork error " << strerror(forkerrno);
      throw LauncherFailure(oss.str());
    }

    if (pid > 0) {
      cout << "parent launcher forked with pid " << pid << ", detaching" << endl;
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

  /*
   * Now launch the background job.
   */
  pid = fork();

  if (pid < 0) {
    std::ostringstream oss;
    oss << "fork error " << strerror(forkerrno);
    throw LauncherFailure(oss.str());
  }

  if (pid > 0) {

    int wstatus;

    /*
     * This is the launcher process.
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

    do {
      if (waitpid(pid, &wstatus, WUNTRACED | WCONTINUED) == -1) {
        std::cerr << "waitpid() error " << endl;
        exit(DAEMON_FAILURE);
      }

      /* If in detach mode, exit */
      if (info.detach) {
        exit(0);
      }

      cout << "launcher process loop" << endl;

      if (WIFEXITED(wstatus)) {
        std::cerr << "launcher exited, status " << WEXITSTATUS(wstatus) << endl;
      }

      if (WIFEXITED(wstatus)) {
        std::cerr << "launcher exited by signal " << WTERMSIG(wstatus) << endl;
      }

      if (WIFSTOPPED(wstatus)) {
        std::cerr << "launcher stopped by signal " << WSTOPSIG(wstatus) << endl;
      }

      if (WIFCONTINUED(wstatus)) {
        std::cerr << "launcher continued" << endl;
      }

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_NORMAL) {
        std::cout << "shutdown request received" << std::endl;
        break;
      }

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_EMERGENCY) {
        std::cout << "emergency shutdown request received" << std::endl;
        break;
      }

      usleep(10);
    } while (!WIFEXITED(wstatus) || !WIFSIGNALED(wstatus));

    cerr << "launcher exit" << endl;
    exit(_pgbckctl_shutdown_mode);
  }

  if (pid == 0) {
    /*
     * This is the background worker process
     */
    BackgroundWorker worker(info);

    /*
     * Register worker in the database.
     */
    worker.initialize();

    /*
     * Enter processing loop
     */
    while(true) {

      usleep(10);

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_NORMAL) {
        std::cout << "shutdown request received" << std::endl;

        /*
         * This is considered a smart shutdown, where we
         * clean up all stuff which seems to be necessary for
         * clean startup again.
         */
        try {
          worker.prepareShutdown();
        } catch (std::exception& e) {
          cerr << "smart shutdown catched error, resuming: " << endl;
        }
        break;
      }

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_EMERGENCY) {
        std::cout << "emergency shutdown request received" << std::endl;
        break;
      }

    }

    cerr << "child exit" << endl;
    exit(_pgbckctl_shutdown_mode);
  } /* child execution code */

  info.pid = pid;
  return pid;

}

pid_t launch(job_info& info) {

  daemonize(info);
  return info.pid;

}
