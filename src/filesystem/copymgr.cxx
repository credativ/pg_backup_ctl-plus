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

BaseCopyManager::_copyItem::_copyItem(int slot) {

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

BaseCopyManager::_copyItem::~_copyItem() {}

void BaseCopyManager::_copyItem::exitForced() {

  this->exit_forced = true;

}

unsigned short BaseCopyManager::getNumberOfCopyInstances() {
  return this->max_copy_instances;
}

void BaseCopyManager::setNumberOfCopyInstances(unsigned short instances) {

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

#ifdef PG_BACKUP_CTL_HAS_LIBURING

/* **************************************************************************
 * IOUringCopyManager
 * **************************************************************************/

IOUringCopyManager::_iouring_copyItem::_iouring_copyItem(unsigned int slot) noexcept
  : BaseCopyManager::_copyItem::_copyItem(slot ){}

IOUringCopyManager::_iouring_copyItem::~_iouring_copyItem() noexcept{}

void IOUringCopyManager::_iouring_copyItem::work(BaseCopyManager::_copyOperations &ops_handler,
                                                 path inputFileName,
                                                 path outputFileName) const {

  std::shared_ptr<ArchiveFile> in  = std::make_shared<ArchiveFile>(inputFileName);
  std::shared_ptr<ArchiveFile> out = std::make_shared<ArchiveFile>(outputFileName);
  std::shared_ptr<vectored_buffer> rbuf = nullptr;

  /* io_uring instance belonging to this copy item, read queue */
  IOUringInstance ring;

  size_t total_bytes_read = 0;

  if ( (in == nullptr) || (out == nullptr) ) {
    throw CArchiveIssue("undefined input/output file in copy thread");
  }

  in->setOpenMode("rb");
  in->open();

  out->setOpenMode("wb+");
  out->open();

  ring.setup();

  /* allocate input buffer according to current settings */
  ring.alloc_buffer(rbuf, ring.getBlockSize() * ring.getQueueDepth());

  while(total_bytes_read < in->size()) {

    ssize_t recv_bytes = 0;

    /* start reading */
    ring.read(in, rbuf, (off_t) total_bytes_read);

    /* wait till first read attempt is completed */

    recv_bytes = ring.handle_current_io(rbuf);
    rbuf->setEffectiveSize(recv_bytes, true);

    if (recv_bytes > 0) {

      ssize_t write_bytes = 0;

      /* issue write request to new file */

      while (write_bytes < recv_bytes) {

        ring.write(out, rbuf, total_bytes_read);
        write_bytes += ring.handle_current_io(rbuf, true);

      }

      total_bytes_read += recv_bytes;

    }

    /* schedule next read */
    rbuf->clear();

    /*
     * Check whether we are forced to exit.
     *
     * XXX: Checking just for the exit flag should be safe
     *      without a critical section here.
     */
    if (ops_handler.exit)
      break;

  }

  /*
   * Sync the out file ...
   *
   * We do this here so that the sync overhead is not located
   * in the main process but delegated to its specific copy thread.
   *
   * But keep the file open, the close operation is finally done by the caller whether
   * he needs the file for something else further.
   */
  out->fsync();

  /* Sanity check */
  if (out->size() < in->size())
    throw CIOUringIssue("copied less bytes than file size");

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

}

void IOUringCopyManager::_iouring_copyItem::go(BaseCopyManager::_copyOperations &ops_handler,
                                       path inputFileName,
                                       path outputFileName) {

  BOOST_LOG_TRIVIAL(debug) << "setup copy thread with slot ID " << slot;

  /*
   * Initialize thread handle
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
  this->io_thread = std::make_shared<std::thread> (&IOUringCopyManager::_iouring_copyItem::work,
                                                   this, std::ref(ops_handler),
                                                   inputFileName,
                                                   outputFileName);
  this->io_thread->detach();

}

IOUringCopyManager::IOUringCopyManager(std::shared_ptr<BackupDirectory> in,
                                       std::shared_ptr<TargetDirectory> out) : BaseCopyManager(std::move(in), out) {}

IOUringCopyManager::IOUringCopyManager(std::shared_ptr<BackupDirectory> in,
                                       std::shared_ptr<TargetDirectory> out,
                                       unsigned short instances) : BaseCopyManager(in, out) {
  this->max_copy_instances = instances;
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
      /* XXX: Should we throw here instead ? */
      BOOST_LOG_TRIVIAL(warning) << "directory \"" << new_target << "\" already exists";

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
                             << de.path().string()
                             << "\", target \""
                             << new_target.string() << "\"";

                             /* Make a copy item */
    ops.ops[slot] = std::make_shared<_iouring_copyItem>(slot);
    ops.ops[slot]->go(ops, de.path(), new_target);

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
      throw CArchiveIssue("target directory \""
        + target->getPath().string() + "\" is not empty");
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

      /*
       * Check signal handlers whether we're requested to exit immediately.
       */
      if (stopHandler != nullptr) {
        ops.exit = stopHandler->check();
      }

      if (intHandler != nullptr) {
        ops.exit = intHandler->check();
      }

      /* if signal handlers are telling us to exit, do so. */
      if (ops.exit) {
        break;
      }

      /*
       * Main work of the processing loop:
       *
       * Check if there are any remaining thread slots available in our
       * operations handler.
       * If not, wait below until a thread finishes and frees
       * its slot. If more than one thread finishes, we try to consume all
       * of the free slots at once.
       */

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

  /* Make sure we mark operations finished in our handler. */
  ops.finalize = true;

}

void IOUringCopyManager::wait() {

  if (ops.finalize) {

    BOOST_LOG_TRIVIAL(debug) << "finalizing copy threads";

    while (ops.ops_free.size() < getNumberOfCopyInstances()) {
      unique_lock<std::mutex> lock(ops.active_ops_mutex);
      ops.notify_cv.wait(lock, [this] { return ops.needs_work; });
    }

  }

}

void IOUringCopyManager::stop() {

  /*
   * The main task here is to safely set the exit
   * attribute to copy operations. Every _copyItem checks
   * itself to abort its task, the corresponding thread
   * should exit safely then. We don't try to wait for them
   * here.
   */
  ops.exit = true;

}

#else

/* **************************************************************************
 * LegacyCopyManager
 * **************************************************************************/

LegacyCopyManager::LegacyCopyManager(std::shared_ptr<BackupDirectory> in,
                                     std::shared_ptr<TargetDirectory> out)
        : BaseCopyManager(in, out) {

}

void LegacyCopyManager::makeCopyItem(const boost::filesystem::directory_entry &de,
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
                             << de.path().string()
                             << "\", target \""
                             << new_target.string() << "\"";

    /* Make a copy item */
    ops.ops[slot] = std::make_shared<_legacy_copyItem>(slot);
    ops.ops[slot]->go(ops, de.path(), new_target);

  } else if (bf::is_symlink(de.path())) {

    /* give back slot ID, since we haven't delegated a thread */
    ops.ops_free.push(slot);

    /* make sure we get notified that there is a new slot ID available */
    ops.needs_work = true;

    BOOST_LOG_TRIVIAL(warning) << "\"" << new_target.string() << "\" is a symlink, currently ignored";

  }

}

void LegacyCopyManager::start() {

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

      /*
       * Check signal handlers whether we're requested to exit immediately.
       */
      if (stopHandler != nullptr) {
        ops.exit = stopHandler->check();
      }

      if (intHandler != nullptr) {
        ops.exit = intHandler->check();
      }

      /* if signal handlers are telling us to exit, do so. */
      if (ops.exit) {
        break;
      }

      /*
       * Main work of the processing loop:
       *
       * Check if there are any remaining thread slots available in our
       * operations handler.
       * If not, wait below until a thread finishes and frees
       * its slot. If more than one thread finishes, we try to consume all
       * of the free slots at once.
       */

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

  /* Make sure we mark any operations as done. This allows to safely wait
   * for any copy operations to shut everything down. */
  ops.finalize = true;

}

void LegacyCopyManager::wait() {

  if (ops.finalize) {

    BOOST_LOG_TRIVIAL(debug) << "finalizing copy threads";

    while (ops.ops_free.size() < getNumberOfCopyInstances()) {
      unique_lock<std::mutex> lock(ops.active_ops_mutex);
      ops.notify_cv.wait(lock, [this] { return ops.needs_work; });
    }

  }

}

void LegacyCopyManager::stop() {

  ops.exit = true;

}

/** I/O Thread legwork method */
void LegacyCopyManager::_legacy_copyItem::work(BaseCopyManager::_copyOperations &ops_handler,
                                               path inputFileName,
                                               path outFileName) const {

  std::shared_ptr<ArchiveFile> in  = std::make_shared<ArchiveFile>(inputFileName);
  std::shared_ptr<ArchiveFile> out = std::make_shared<ArchiveFile>(outFileName);
  std::shared_ptr<MemoryBuffer> buf = std::make_shared<MemoryBuffer>(8192);
  size_t read_bytes;
  size_t total_bytes;
  size_t write_bytes;

  /* Open source */
  in->setOpenMode("rb");
  in->open();

  /* Open target, this will create the file automatically */
  out->setOpenMode("wb+");
  out->open();

  /* We always try to read 8K sizes */
  total_bytes = in->size();
  read_bytes = 0;

  while (read_bytes < total_bytes) {

    read_bytes = in->read(buf->ptr(), buf->getSize());

    if (read_bytes > 0) {
      write_bytes = out->write(buf->ptr(), buf->getSize());
    } else {
      break;
    }

    /*
     * If we don't get read_bytes back from write(), we treat this as
     * a severe error
     */
    if (write_bytes < read_bytes) {
      std::ostringstream oss;
      oss << "short write: expected "
          << read_bytes
          << " got "
          << write_bytes;
      throw CArchiveIssue(oss.str());
    }

    buf->clear();

    /* Check if we're forced to exit */
    if (ops_handler.exit)
      break;

  }

  /* finish file, make sure everything hits storage */
  out->fsync();
  in->close();
  out->close();

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

}

void LegacyCopyManager::_legacy_copyItem::go(BaseCopyManager::_copyOperations &ops_handler, path inputFileName,
                                             path outputFileName) {

  BOOST_LOG_TRIVIAL(debug) << "setup copy thread with slot ID " << slot;

  /*
   * Initialize thread handle
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
  this->io_thread = std::make_shared<std::thread> (&LegacyCopyManager::_legacy_copyItem::work,
                                                   this,
                                                   std::ref(ops_handler),
                                                   inputFileName,
                                                   outputFileName);
  this->io_thread->detach();

}

LegacyCopyManager::_legacy_copyItem::_legacy_copyItem(unsigned int slot) noexcept
  : BaseCopyManager::_copyItem::_copyItem(slot) {}

LegacyCopyManager::_legacy_copyItem::~_legacy_copyItem() {}

#endif
