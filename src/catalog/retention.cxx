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

  /* throw in case the descr is invalid */
  if (descr == nullptr) {
    throw CArchiveIssue("pin descriptor is not initialized");
  }

  if (descr->getOperationType() == ACTION_UNDEFINED) {
    throw CArchiveIssue("cannot apply retention with an undefined pin operation");
  }

  this->pinDescr = descr;
}

PinRetention::~PinRetention() {}

void PinRetention::performPin(shared_ptr<BaseBackupDescr> bbdescr) {}

void PinRetention::performUnpin(shared_ptr<BaseBackupDescr> bbdescr) {}

int PinRetention::apply(vector<shared_ptr<BaseBackupDescr>> list) {

  int result = 0;

  PinOperationType policy = ACTION_UNDEFINED;

  if (list.size() == 0) {
    /* set return code and fall through */
    result = -1;
  }

  /*
   * Determine retention policy to perform. The rule we have
   * to use is encoded into our pinDescr instance.
   */
  policy = this->pinDescr->getOperationType();

  /*
   * Now dispatch the policy to its encoded action.
   */
  switch(policy) {
  case ACTION_ID:
    break;
  case ACTION_COUNT:
    break;
  case ACTION_NEWEST:
    break;
  case ACTION_OLDEST:
    break;
  case ACTION_CURRENT:
    break;
  default:
    throw CArchiveIssue("pin retention policy is undefined");
    break;
  }

  for ( auto & bbdescr : list ) {

    /*
     * Check the state of the basebackup, we ignore any basebackups
     * in aborted or in progress state.
     */
    if ( (bbdescr->status == BaseBackupDescr::BASEBACKUP_STATUS_ABORTED)
         || (bbdescr->status == BaseBackupDescr::BASEBACKUP_STATUS_IN_PROGRESS) ) {
      continue;
    }

    /*
     * Check out what to do, PIN or UNPIN.
     */
    switch(this->pinDescr->action()) {
    case PIN_BASEBACKUP:
      break;
    case UNPIN_BASEBACKUP:
      break;
    default:
      throw CArchiveIssue("unsupported command action for pin retention "
                          + CatalogDescr::commandTagName(this->pinDescr->action()));
    }
  }

  return result;
}
