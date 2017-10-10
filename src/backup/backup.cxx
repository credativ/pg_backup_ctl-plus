#include <common.hxx>
#include <backup.hxx>

using namespace credativ;

Backup::Backup(const std::shared_ptr<CatalogDescr>& descr) {}

Backup::~Backup() {};

TransactionLogBackup::TransactionLogBackup(const std::shared_ptr<CatalogDescr>& descr) : Backup(descr) {

  this->descr = descr;

}

TransactionLogBackup::~TransactionLogBackup() {

  if (this->isInitialized()) {
    this->finalize();
    delete this->directory;
  }

}

bool TransactionLogBackup::isInitialized() {
  return this->initialized;
}

void TransactionLogBackup::create() {

  if (this->isInitialized()) {
    /*
     * Transaction log segments are always stored
     * within the log/ subdirectory under the parent
     * archive directory. Usually these directory
     * already exists, but make sure ...
     */
    this->directory->create();
  } else {
    throw CArchiveIssue("cannot call create on uninitialized transaction log directory handle");
  }

}

void TransactionLogBackup::finalize() {

  /*
   * Sync all stacked file handles. Close all
   * files.
   *
   * NOTE: we call sync_pending() here, which already does all
   *       the necessary synchronisation here.
   */
  this->sync_pending();

  for (auto &item : this->fileList) {
    item->fileHandle->close();
  }

}

void TransactionLogBackup::initialize() {

  if (!this->isInitialized()) {
    this->directory = new ArchiveLogDirectory(path(this->descr->directory));
    this->initialized = true;
  }

}

std::string TransactionLogBackup::backupDirectoryString() {
  return ((ArchiveLogDirectory *)this->directory)->getPath().string();
}

void TransactionLogBackup::sync_pending() {

  for (auto &item : this->fileList) {
    if (item->sync_pending ||
        item->flush_pending) {
      item->fileHandle->fsync();
      item->sync_pending = item->flush_pending = true;
    }
  }

  /*
   * Make sure directory meta information is also
   * synced.
   */
  this->directory->fsync();
}

void TransactionLogBackup::flush_pending() {
  /* currently a no-op, call sync_pending instead */
  this->sync_pending();
}

std::shared_ptr<BackupFile> TransactionLogBackup::stackFile(std::string name) {

  std::shared_ptr<TransactionLogListItem> logref = std::make_shared<TransactionLogListItem>();

  /*
   * Stack a new transaction log segment file into
   * current pending file list.
   */
  if (!this->isInitialized()) {
    throw CArchiveIssue("cannot create transaction log backup file: not initialized");
  }

  /* Allocate new segment file handle */
  this->file = this->directory->walfile(name, this->compression);
  this->file->setOpenMode("wb");
  this->file->open();

  /*
   * Stack walfile reference into open file list.
   */
  logref->fileHandle = file;
  logref->filename = name;
  logref->sync_pending = true;
  logref->flush_pending = true;

  this->fileList.push_back(logref);
  return this->file;

}

StreamBaseBackup::StreamBaseBackup(const std::shared_ptr<CatalogDescr>& descr) : Backup(descr) {

  this->descr = descr;
  this->identifier = this->createMyIdentifier();

}

void StreamBaseBackup::setCompression(BackupProfileCompressType compression) {
  this->compression = compression;
}

BackupProfileCompressType StreamBaseBackup::getCompression() {
  return this->compression;
}

StreamBaseBackup::~StreamBaseBackup() {

  if (this->isInitialized()) {
    this->finalize();
    delete this->directory;
  }

};

std::string StreamBaseBackup::backupDirectoryString() {

  if (this->isInitialized()) {
    return ((StreamingBaseBackupDirectory *)this->directory)->getPath().string();
  } else {
    return std::string("");
  }

}

std::string StreamBaseBackup::createMyIdentifier() {

  return "streambackup-"
    + CPGBackupCtlBase::current_timestamp(true);

}

void StreamBaseBackup::create() {

  if (this->isInitialized()) {
    /*
     * A streamed base backup is hosted within a
     * subdirectory in <ARCHIVEDIR>/base. Create a new one but
     * create() will throw a CArchiveIssue exception in case
     * it already exists.
     */
    this->directory->create();
  } else {
    throw CArchiveIssue("cannot call create on uninitialized streaming directory handle");
  }

}

void StreamBaseBackup::finalize() {

  /*
   * Sync file handles and their contents.
   */
  for(auto& item : this->fileList) {
    item->fsync();
    item->close();
  }

  /*
   * Sync directory handle
   */
  this->directory->fsync();

  /*
   * Clear internal handles.
   */
  this->file = nullptr;
  this->fileList.clear();

}

bool StreamBaseBackup::isInitialized() {
  return this->initialized;
}

void StreamBaseBackup::initialize() {

  if (!this->isInitialized()) {
    this->directory = new StreamingBaseBackupDirectory(this->identifier,
                                                       path(this->descr->directory));
    this->initialized = true;
  }

}

std::shared_ptr<BackupFile> StreamBaseBackup::stackFile(std::string name) {

  if (!this->isInitialized()) {
    throw CArchiveIssue("cannot create stream backup files: not initialized");
  }

  /*
   * Allocate a new basebackup file. This will overwrite
   * the last used file reference.
   */
  this->file = this->directory->basebackup(name, this->compression);
  this->file->setOpenMode("wb");
  this->file->open();

  /*
   * Stack this reference into the private file stack.
   */
  this->fileList.push_back(this->file);
  return this->file;

}
