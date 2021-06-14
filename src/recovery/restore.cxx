#include <recovery.hxx>
#include <fs-archive.hxx>

using namespace pgbckctl;

/* *************************************************************************** *
 * Implementation of class RestoreDescrID
 * *************************************************************************** */

void RestoreDescrID::getId(int &id) {

  if (this->type != RESTORE_BASEBACKUP_BY_ID)
    throw CArchiveIssue("invalid access to restore backup descriptor by ID");

  id = this->ident.id;

}

void RestoreDescrID::getId(std::string &name) {

  if (this->type != RESTORE_BASEBACKUP_BY_NAME)
    throw CArchiveIssue("invalid access to restore backup descriptor by name");

  name = this->ident.name;

}

void RestoreDescrID::setId(RestoreDescrIdentificationType type,
                           std::string const& name) {

  if (this->type != RESTORE_BASEBACKUP_BY_UNDEF)
    this->ident.~_restoredescr_ident();

  if (type != RESTORE_BASEBACKUP_BY_NAME)
    throw CArchiveIssue("invalid access to restore backup descriptor by name");

  this->ident.~_restoredescr_ident();
  new(&(this->ident.name)) std::string(name);

}

void RestoreDescrID::setId(RestoreDescrIdentificationType type,
                           int const& id) {

  if (this->type != RESTORE_BASEBACKUP_BY_UNDEF)
    this->ident.~_restoredescr_ident();

  if (type != RESTORE_BASEBACKUP_BY_ID)
    throw CArchiveIssue("invalid access to restore backup descriptor by ID");

  new(&(this->ident.id)) int;
  this->ident.id = id;

}

BaseBackupRetrieveMode RestoreDescrID::basebackupRetrieveMode(std::string const& name) {

  if ( (name == RestoreDescrID::DESCR_NAME_CURRENT)
       || (name == RestoreDescrID::DESCR_NAME_LATEST)
       || (name == RestoreDescrID::DESCR_NAME_NEWEST) ) {
    return BASEBACKUP_NEWEST;
  }

  if (name == RestoreDescrID::DESCR_NAME_OLDEST) {
    return BASEBACKUP_OLDEST;
  }

  throw CCatalogIssue("basebackup name must be either CURRENT, LATEST, NEWEST or OLDEST");

}

/* *************************************************************************** *
 * Implementation of class RestoreDescr
 * *************************************************************************** */

RestoreDescr::RestoreDescr(std::string bbname) {

  this->id.setId(RESTORE_BASEBACKUP_BY_NAME, bbname);

}

RestoreDescr::RestoreDescr(int id) {

  this->id.setId(RESTORE_BASEBACKUP_BY_ID, id);

}

RestoreDescr::~RestoreDescr() {}

void RestoreDescr::prepareTablespaceDescrForMap(unsigned int oid) {

  /*
   * Check if the specified tablespace OID is already registred.
   */
  std::map<unsigned int, std::shared_ptr<BackupTablespaceDescr>>::iterator it;

  it = this->tablespace_map.find(oid);

  if (it != this->tablespace_map.end()) {

    std::ostringstream oss;
    oss << "tablespace with OID \"" << oid << "\" already registered";
    throw CCatalogIssue(oss.str());

  }

  /* Not yet registered, prepare the descriptor */
  this->curr_tablespace_descr = std::make_shared<BackupTablespaceDescr>();
  this->curr_tablespace_descr->spcoid = oid;

}

std::shared_ptr<BackupTablespaceDescr> RestoreDescr::getPreparedTablespaceDescrForMap() {

  return this->curr_tablespace_descr;

}

void RestoreDescr::stackTablespaceDescrForMap(std::string location) {

  /*
   * Check if a tablespace descriptor was already prepared.
   * If not, throw.
   */
  if (this->curr_tablespace_descr == nullptr) {
    throw CCatalogIssue("no tablespace descriptor prepared for insertion");
  }

  /*
   * Else insert the new tablespace descriptor with its new location.
   */
  this->curr_tablespace_descr->spclocation = location;
  this->tablespace_map.insert(std::pair<unsigned int,
                              std::shared_ptr<BackupTablespaceDescr>>(curr_tablespace_descr->spcoid,
                                                                      curr_tablespace_descr));

}

/* *************************************************************************** *
 * Implementation of class Recovery
 * *************************************************************************** */

Recovery::Recovery() {}
Recovery::Recovery(std::shared_ptr<RestoreDescr> restoreDescr) {}

Recovery::~Recovery() {}

/* *************************************************************************** *
 * Implementation of class TarRecovery
 * *************************************************************************** */

TarRecovery::TarRecovery(std::shared_ptr<RestoreDescr> restoreDescr) {}

TarRecovery::~TarRecovery() {}

void TarRecovery::init() {}
