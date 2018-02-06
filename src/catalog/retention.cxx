#include <retention.hxx>

using namespace credativ;

Retention::Retention(std::shared_ptr<BackupCatalog> catalog) {

  this->catalog = catalog;

}

Retention::~Retention() {}

void Retention::setCatalog(std::shared_ptr<BackupCatalog> catalog) {

  this->catalog = catalog;

}

std::shared_ptr<Retention> get(string retention_name,
                               std::shared_ptr<BackupCatalog> catalog) {

  return std::make_shared<Retention>(catalog);

}

GenericRetentionRule::GenericRetentionRule() {}

GenericRetentionRule::~GenericRetentionRule() {}
