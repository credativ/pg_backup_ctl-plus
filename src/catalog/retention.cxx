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

  return nullptr;

}

GenericRetentionRule::GenericRetentionRule() {}

GenericRetentionRule::GenericRetentionRule(shared_ptr<CatalogDescr> descr) {

}

GenericRetentionRule::~GenericRetentionRule() {}

/* *****************************************************************************
 * PinRetention implementation
 * ****************************************************************************/

PinRetention::PinRetention(BasicPinDescr *descr,
                           std::shared_ptr<BackupCatalog> catalog)
  : Retention(catalog) {

}

PinRetention::~PinRetention() {}

int PinRetention::apply(vector<shared_ptr<BaseBackupDescr>> list) {

  int result = 0;

}
