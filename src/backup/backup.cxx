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

  delete this->directory;

};

void StreamBaseBackup::create() {

  this->file = this->directory->basebackup(this->compression);

}
