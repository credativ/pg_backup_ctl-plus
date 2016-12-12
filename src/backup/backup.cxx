#include <backup.hxx>

using namespace credativ;

Backup::Backup(const std::shared_ptr<CatalogDescr>& descr) {

}

Backup::~Backup() {};

StreamBaseBackup::StreamBaseBackup(const std::shared_ptr<CatalogDescr>& descr) : Backup(descr) {

}

StreamBaseBackup::~StreamBaseBackup() {};

void StreamBaseBackup::create() {

}
