#include <common.hxx>
#include <backup.hxx>

using namespace credativ;

Backup::Backup(const std::shared_ptr<CatalogDescr>& descr) {}

Backup::~Backup() {};

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

  this->finalize();
  delete this->directory;

};

std::string StreamBaseBackup::createMyIdentifier() {

  return "streambackup-"
    + CPGBackupCtlBase::current_timestamp(true);

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

  /*
   * A streamed base backup is hosted within a
   * subdirectory in <ARCHIVEDIR>/base. Create a new one but
   * create() will throw a CArchiveIssue exception in case
   * it already exists.
   */
  this->directory->create();

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

  /*
   * Stack this reference into the private file stack.
   */
  this->fileList.push_back(this->file);
  return this->file;

}
