#include <common.hxx>
#include <backup.hxx>

using namespace credativ;

Backup::Backup(const std::shared_ptr<CatalogDescr>& descr) {}

Backup::~Backup() {};

TransactionLogBackup::TransactionLogBackup(const std::shared_ptr<CatalogDescr>& descr) : Backup(descr) {

  this->descr = descr;

}

TransactionLogBackup::~TransactionLogBackup() {}

bool TransactionLogBackup::isInitialized() {

}

void TransactionLogBackup::create() {

}

void TransactionLogBackup::finalize() {

}

void TransactionLogBackup::initialize() {

}

std::string TransactionLogBackup::backupDirectoryString() {

}

std::shared_ptr<BackupFile> stackFile(std::string name) {

}

void TransactionLogBackup::sync_pending() {

}

void TransactionLogBackup::flush_pending() {

}

std::shared_ptr<BackupFile> TransactionLogBackup::stackFile(std::string name) {

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
    throw CArchiveIssue("cannot call create on initialized streaming directory handle");
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

  this->directory = new StreamingBaseBackupDirectory(this->identifier,
                                                     path(this->descr->directory));
  this->initialized = true;

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
