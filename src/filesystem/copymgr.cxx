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

void IOUringCopyManager::_copyItem::work() {



}

void IOUringCopyManager::_copyItem::go(IOUringCopyManager::_copyOperations &ops_handler) {

  /* Make a callable function handler */
  auto callable = std::bind(&_copyItem::work, this);

  /* Initialize thead handle */
  this->io_thread = std::make_shared<std::thread> (callable);

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
  this->read_ring  = std::make_shared<IOUringInstance>();
  this->write_ring = std::make_shared<IOUringInstance>();

}

IOUringCopyManager::_copyItem::_copyItem(std::shared_ptr<ArchiveFile> in,
                                         std::shared_ptr<ArchiveFile> out,
                                         int slot) {

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

  this->slot    = slot;
  this->inFile  = in;
  this->outFile = out;

  /* instantiate urings */
  this->read_ring = std::make_shared<IOUringInstance>();
  this->write_ring = std::make_shared<IOUringInstance>();

  /* setup urings with assigned in/out files */
  this->read_ring->setup(in);
  this->write_ring->setup(out);

}

IOUringCopyManager::IOUringCopyManager(std::shared_ptr<BackupDirectory> in,
                                       std::shared_ptr<TargetDirectory> out) : BaseCopyManager(in, out) {}

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

  /* Check target directory. If it already exists and is non-empty, throw.
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
   * Initially prepare our thread pool. We do this outside the main loop
   * below so that we can easily set up everything in front of.
   *
   * Everything is done within a single critical section to prepare everything
   * at once.
   */
  {
    std::lock_guard<std::mutex> lock(ops.active_ops_mutex);

    for (int slot = 0; slot < this->max_copy_instances; slot++) {

      if (!walker.end()) {

        /* directory handle */
        directory_entry de = walker.next();

        /*
         * Extract the relative path to the source file from
         * the absolute archive path.
         */
        path new_directory = BackupDirectory::relative_path(de.path(), target->getPath());

        /* Check wether this is a file or directory. The latter isn't handled by
         * a _copyItem instance, instead, we are creating the target directory
         * directly here.
         */
        if (bf::is_directory(de.path())) {

          bf::create_directories(de.path());

        } else if (bf::is_regular_file(de.path())) {

          /* extract the filename from current directory_entry and build an absolute path to the new target
           * by creating a new ArchiveFile handle
           */
          std::shared_ptr<ArchiveFile> target_file
            = std::make_shared<ArchiveFile>(target->getPath() / de.path().filename());

          /* ... and finally the source file */
          std::shared_ptr<ArchiveFile> source_file = std::make_shared<ArchiveFile>(de.path());

          /* Make a copy item */
          ops.ops[slot] = std::make_shared<_copyItem>(source_file, target_file, slot);

        } else if (bf::is_symlink(de.path())) {
            /* XXX: TO DO */
        }

      }

    }
  }

  /*
   * IMPORTANT:
   *
   * At this point we enter the processing loop where we
   * iterate through the contents of the source directory.
   * Make sure we protect operations against concurrent changes, since
   * we have to check, modify and probably terminate operations
   * concurrently to copy threads.
   */

  while (!walker.end()) {

    int slot_id = -1;

    /* Check if the list of free items has something for us */
    {
      std::lock_guard<std::mutex> lock(ops.active_ops_mutex);

    }


    if (ops.ops_free.empty()) {

    }

    slot_id = ops.ops_free.top();
    ops.ops_free.pop();

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
  const std::lock_guard<std::mutex>lock(ops.active_ops_mutex);
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
