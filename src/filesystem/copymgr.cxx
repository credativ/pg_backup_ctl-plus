#include <fs-copy.hxx>

using namespace pgbckctl;

/* **************************************************************************
 * TargetDirectory
 * **************************************************************************/

TargetDirectory::TargetDirectory(path directory)
  : RootDirectory(directory){

}

TargetDirectory::~TargetDirectory() {

}

/* **************************************************************************
 * BaseCopyManager
 * **************************************************************************/

BaseCopyManager::BaseCopyManager(std::shared_ptr<BackupDirectory> in,
                                 std::shared_ptr<TargetDirectory> out) {
  this->setSourceDirectory(in);
  this->setTargetDirectory(out);
}

void BaseCopyManager::setSourceDirectory(std::shared_ptr<BackupDirectory> in) {
  if (in == nullptr) {
    throw CArchiveIssue("source file is undefined");
  }

  if (! in->exists()) {
    std::ostringstream oss;
    oss << "source directory \""
    << in->basedir()
    << "\" does not exist";
    throw CArchiveIssue("source directory \"\"");
  }

  source = in;
}

void BaseCopyManager::setTargetDirectory(std::shared_ptr<TargetDirectory> out) {
  if (out == nullptr) {
    throw CArchiveIssue("target file is undefined");
  }

  target = out;
}

std::shared_ptr<BackupCopyManager>
BaseCopyManager::get(std::shared_ptr<StreamingBaseBackupDirectory> in,
                     std::shared_ptr<TargetDirectory> out) {

  return std::make_shared<BackupCopyManager>(in, out);

}

BaseCopyManager::~BaseCopyManager() {

  /* target file was just written, make sure to sync them */
  //out->fsync();

  /* Make sure we close files properly */

}

void BaseCopyManager::assignSigStopHandler(JobSignalHandler *handler) {

  if (handler == nullptr) {
    throw CPGBackupCtlFailure("attempt to assign uninitialized stop signal handler");
  }

  stopHandler = handler;

}

void BaseCopyManager::assignSigIntHandler(JobSignalHandler *handler) {

  if (handler == nullptr) {
    throw CPGBackupCtlFailure("attempt to assign uninitialized int signal handler");
  }

  intHandler = handler;

}

#ifdef PG_BACKUP_CTL_HAS_LIBURING

/* **************************************************************************
 * IOUringCopyManager
 * **************************************************************************/

void IOUringCopyManager::_copyItem::work(IOUringCopyManager::_copyOperations &ops_handler,
                                         std::shared_ptr<ArchiveFile> in,
                                         std::shared_ptr<ArchiveFile> out) const {

  std::shared_ptr<vectored_buffer> rbuf = nullptr;

  /** io_uring instance belonging to this copy item, read queue */
  IOUringInstance ring;

  ssize_t recv_bytes = 0;
  struct io_uring_cqe *cqe = NULL;

  if ( (in == nullptr) || (out == nullptr) ) {
    throw CArchiveIssue("undefined input/output file in copy thread");
  }

  in->setOpenMode("rb");
  in->open();

  out->setOpenMode("wb+");
  out->open();

  BOOST_LOG_TRIVIAL(debug) << "work(), thread slot id = " << slot;

  ring.setup(in);

  BOOST_LOG_TRIVIAL(debug) << "work(), thread slot id = " << slot << " io_uring is setup";

  /* allocate input buffer according to current settings */
  ring.alloc_buffer(rbuf);

  /* start reading */
  ring.read(in, rbuf, (off_t) 0);

  /* wait till first read attempt is completed */
  BOOST_LOG_TRIVIAL(debug) << "work(), thread slot id = " << slot << " waiting for I/O completion";
  recv_bytes = ring.handle_current_io(rbuf);
  BOOST_LOG_TRIVIAL(debug) << "work(), thread slot id = " << slot << " waiting for I/O completion DONE";


  while (recv_bytes > 0) {

    ssize_t write_bytes = 0;

    BOOST_LOG_TRIVIAL(debug) << "work(), thread slot id = " << slot << " reading " << recv_bytes;

    rbuf->setOffset((off_t) 0);

    /* issue write request to new file */
    ring.write(out, rbuf, SEEK_END);
    write_bytes = ring.handle_current_io(rbuf);

    BOOST_LOG_TRIVIAL(debug) << "written " << write_bytes;

    while (write_bytes < rbuf->getEffectiveSize()) {
      io_uring_cqe *wcqe = NULL;
      rbuf->setOffset((off_t) write_bytes);
      ring.write(out, rbuf, SEEK_END);
      ring.wait(&wcqe);
      ring.seen(&wcqe);
      write_bytes += wcqe->res;
    }

    /* schedule next read */
    rbuf->setOffset((off_t) 0);
    rbuf->clear();

    if (::feof(in->getFileHandle()) != 0) {
      /* end of file reached */
      BOOST_LOG_TRIVIAL(debug) << "end of file after " << recv_bytes << " reached.";
      break;
    }

    in->lseek(SEEK_CUR, recv_bytes);
    ring.read(in, rbuf, SEEK_CUR);
    ring.wait(&cqe);
    recv_bytes = cqe->res;

    /*
     * Check whether we are forced to exit.
     *
     * XXX: Checking just for the exit flag should be safe
     *      without a critical section here.
     */
    if (ops_handler.exit)
      break;

  }

  BOOST_LOG_TRIVIAL(debug) << "work(), thread slot id = " << slot << " I/O finished";

  /*
   * Sync the out file ...
   *
   * We do this here so that the sync overhead is not located
   * in the main process but delegated to its specific copy thread.
   *
   * But keep the file open, this is the finally done by the caller whether
   * he needs to file for something else further.
   */
  out->fsync();

  /* Tear down uring ... */
  ring.exit();

  /* There doesn't seem to be more work to do,
   * so finalize this thread.
   *
   * We have to put our slot_id back onto the stack of available
   * threads for our operations scheduler, this should happen
   * in a critical section.
   *
   * We shouldn't forget to notify operations scheduler (our main thread)
   * there is something to do for him...
   */
  {
    unique_lock<std::mutex> lock(ops_handler.active_ops_mutex);
    ops_handler.ops_free.push(slot);
    ops_handler.needs_work = true;
    ops_handler.notify_cv.notify_one();
  }

  BOOST_LOG_TRIVIAL(debug) << "copy thread with slot ID " << slot << " hash finished";
}

void IOUringCopyManager::_copyItem::go(IOUringCopyManager::_copyOperations &ops_handler,
                                       std::shared_ptr<ArchiveFile> in,
                                       std::shared_ptr<ArchiveFile> out) {

  /* Make a callable function handler */
  //auto callable = std::bind(&_copyItem::work, this, ops_handler);

  BOOST_LOG_TRIVIAL(debug) << "setup copy thread with slot ID " << slot;

  /*
   * Initialize thead handle
   *
   * NOTE: Since operation handler ops_handler is passed by reference, we *have* to
   *       make sure it can be passed as a rvalue to the threads' method. This is
   *       done by using an instance of std::ref(), so the library can safely use them
   *       accordingly. See
   *
   *       https://en.cppreference.com/w/cpp/thread/thread/thread#Notes
   *
   *       for an explanation.
   */
  this->io_thread = std::make_shared<std::thread> (&IOUringCopyManager::_copyItem::work,
                                                   this, std::ref(ops_handler),
                                                   in,
                                                   out);
  this->io_thread->detach();

}

void IOUringCopyManager::_copyItem::exitForced() {

  this->exit_forced = true;

}

IOUringCopyManager::_copyItem::~_copyItem() {}

IOUringCopyManager::_copyItem::_copyItem(int slot) {

  /* slot id isn't allowed to be larger than MAX_PARALLEL_COPY INSTANCES */
  if (slot > MAX_PARALLEL_COPY_INSTANCES) {
    ostringstream err;
    err << "requested slot id for copy worker exceeds MAX_PARALLEL_COPY_INSTANCES("
        << MAX_PARALLEL_COPY_INSTANCES
        << ")";
    throw CPGBackupCtlFailure(err.str());
  }

  if (slot < 0) {
    throw CPGBackupCtlFailure("requested slot id is invalid (< 0)");
  }

  this->slot       = slot;

}

IOUringCopyManager::IOUringCopyManager(std::shared_ptr<BackupDirectory> in,
                                       std::shared_ptr<TargetDirectory> out) : BaseCopyManager(std::move(in), out) {}

IOUringCopyManager::IOUringCopyManager(std::shared_ptr<BackupDirectory> in,
                                       std::shared_ptr<TargetDirectory> out,
                                       unsigned short instances) : BaseCopyManager(in, out) {
  this->max_copy_instances = instances;
}

void IOUringCopyManager::performCopy() {
}

void IOUringCopyManager::setNumberOfCopyInstances(unsigned short instances) {

  if (instances > MAX_PARALLEL_COPY_INSTANCES) {
    std::ostringstream oss;
    oss << "number of copy instances("
        << instances
        << ") exceeds allowed("
        << MAX_PARALLEL_COPY_INSTANCES
        << ")";
    throw CArchiveIssue(oss.str());
  }

  this->max_copy_instances = instances;

}

unsigned short IOUringCopyManager::getNumberOfCopyInstances() {
  return this->max_copy_instances;
}

void IOUringCopyManager::makeCopyItem(const directory_entry &de,
                                      const unsigned int slot) {
  namespace bf = boost::filesystem;

  /*
   * Extract the relative path to the source file from
   * the absolute archive path.
   */
  path new_target = BackupDirectory::relative_path(de.path(), this->source->getPath());

  /* Sanity check: If new_target is empty or equals de.path(), throw an exception */
  if (( new_target.string() == "") || (new_target.string() == de.path().string())) {

    std::ostringstream err;
    err << "could not copy \""
        << de.path().string()
        << "\" to \""
        << this->target->getPath() / new_target
        << "\"";
    throw CArchiveIssue(err.str());

  }

  new_target = this->target->getPath() / new_target;

  BOOST_LOG_TRIVIAL(debug) << "copy \""
                           << de.path().string()
                           << "\" to \""
                           << new_target.string()
                           << "\"";

  /* Check whether this is a file or directory. The latter isn't handled by
   * a _copyItem instance, instead, we are creating the target directory
   * directly here.
   */
  if (bf::is_directory(de.path())) {

    BOOST_LOG_TRIVIAL(debug) << "copy item for directory \""
                             << de.path().string()
                             << "\", target \""
                             << new_target.string() << "\"";

    if (!bf::exists(new_target))
      bf::create_directories(new_target);
    else
      BOOST_LOG_TRIVIAL(warning) << "directory \"" << new_target << "\" already exists, ignoring";

    /* give back slot ID, since we haven't delegated a thread */
    ops.ops_free.push(slot);

    /* make sure we get notified that there is a new slot ID available */
    ops.needs_work = true;

  } else if (bf::is_regular_file(de.path())) {

    /* extract the filename from current directory_entry and build an absolute path to the new target
     * by creating a new ArchiveFile handle
     */
    std::shared_ptr<ArchiveFile> target_file
            = std::make_shared<ArchiveFile>(new_target);

    /* ... and finally the source file */
    std::shared_ptr<ArchiveFile> source_file = std::make_shared<ArchiveFile>(de.path());

    BOOST_LOG_TRIVIAL(debug) << "copy item for file \""
                             << source_file->getFileName()
                             << "\", target \""
                             << target_file->getFileName() << "\"";

                             /* Make a copy item */
    ops.ops[slot] = std::make_shared<_copyItem>(slot);
    ops.ops[slot]->go(ops, source_file, target_file);

  } else if (bf::is_symlink(de.path())) {

    /* give back slot ID, since we haven't delegated a thread */
    ops.ops_free.push(slot);

    /* make sure we get notified that there is a new slot ID available */
    ops.needs_work = true;

    BOOST_LOG_TRIVIAL(warning) << "\"" << new_target.string() << "\" is a symlink, currently ignored";

  }

}

void IOUringCopyManager::start() {

  namespace bf = boost::filesystem;

  /*
   * Initialize ops infrastructure.
   *
   * NOTE: At this point we don't need to protect against
   *       concurrent access to the ops infrastructure since
   *       at this point there shouldn't be running any
   *       parallel copy operations yet.
   */
  for (int i = 0; i < this->max_copy_instances; i++) {
    ops.ops_free.push(i);
  }

  /*
   * Check target directory. If it already exists and is non-empty, throw.
   * If it's not present yet, create it.
   */
  if (!bf::exists(target->getPath())) {
    bf::create_directories(target->getPath());
  } else {
    if (!bf::is_empty(target->getPath())) {
      throw CArchiveIssue("target directory \"" + target->getPath().string() + "\" is not empty");
    }
  }

  /*
   * Get directory entries from our source directory we need to copy.
   */
  DirectoryTreeWalker walker = source->walker();
  walker.open();

  /*
   * IMPORTANT:
   *
   * At this point we enter the processing loop where we
   * iterate through the contents of the source directory.
   * Make sure we protect operations against concurrent changes, since
   * we have to check, modify and probably terminate operations
   * concurrently by copy threads.
   */

  while (!walker.end()) {

    unsigned int slot_id = -1;

    /* Check if the list of free items has something for us */
    {
      std::unique_lock<std::mutex> lock(ops.active_ops_mutex);

      if (ops.ops_free.empty()) {
        /* wait until we are notified that thread(s) have finished. */
        BOOST_LOG_TRIVIAL(debug) << "copy dispatcher main thread waiting for new work";
        ops.notify_cv.wait(lock, [this] { return ops.needs_work; });
      }

      /*
       * Loop until no more free slots are available.
       */
      while (!ops.ops_free.empty() && !walker.end()) {

        BOOST_LOG_TRIVIAL(debug) << "copy dispatcher main thread woken up, new copy item";

        /* pop free copy item from stack */
        slot_id = ops.ops_free.top();
        ops.ops_free.pop();

        /* Make next copy operation */
        makeCopyItem(walker.next(), slot_id);

      }

      ops.needs_work = false;
    }

  }

  performCopy();

}

void IOUringCopyManager::stop() {

  /*
   * The main task here is to safely set the exit
   * attribute to copy operations. Every _copyItem checks
   * itself to abort its task, the corresponding thread
   * should exit safely then. We don't try to wait for them
   * here.
   */
  //const std::unique_lock<std::mutex> lock(ops.active_ops_mutex);
  ops.exit = true;

}

#else

/* **************************************************************************
 * LegacyCopyManager
 * **************************************************************************/

LegacyCopyManager::LegacyCopyManager(std::shared_ptr<BackupDirectory> in, std::shared_ptr<TargetDirectory> out)
        : BaseCopyManager(in, out) {

}

#endif
