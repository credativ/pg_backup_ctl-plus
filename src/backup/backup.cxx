#include <common.hxx>
#include <backup.hxx>

using namespace credativ;

Backup::Backup(const std::shared_ptr<CatalogDescr>& descr) {}

Backup::~Backup() {};

StreamBaseBackup::StreamBaseBackup(const std::shared_ptr<CatalogDescr>& descr) : Backup(descr) {

  this->descr = descr;
  this->directory = new BackupDirectory(path(this->descr->directory));

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

void StreamBaseBackup::create(std::string name) {

  /*
   * Allocate a new basebackup file. This will overwrite
   * the last used file reference.
   */
  this->file = this->directory->basebackup(name, this->compression);

  /*
   * Stack this reference into the private file stack.
   */
  this->fileList.push_back(this->file);

}
