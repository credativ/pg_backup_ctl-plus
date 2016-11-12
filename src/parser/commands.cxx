#include <commands.hxx>

using namespace credativ;

void BaseCatalogCommand::pushAffectedAttribute(int colId) 
  throw (CArchiveIssue) {

  this->affectedAttributes.push_back(colId);
}

BaseCatalogCommand::~BaseCatalogCommand() {}

DropArchiveCatalogCommand::DropArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->catalog = catalog;
  this->tag     = DROP_ARCHIVE;
}

DropArchiveCatalogCommand::DropArchiveCatalogCommand() {}

void DropArchiveCatalogCommand::execute(bool existsOk) {

  shared_ptr<CatalogDescr> temp_descr(nullptr);

  /*
   * Die hard in case no catalog available.
   */
  if (this->catalog == NULL)
    throw CArchiveIssue("could not execute archive command: no catalog");

  /*
   * Open the catalog, if not yet done.
   */
  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  /*
   * existsOk means in this case we check wether
   * the archive exists. If false, raise an error, otherwise
   * just pass which turns this method into a no-op.
   */
  
  
}

CreateArchiveCatalogCommand::CreateArchiveCatalogCommand() {}

void BaseCatalogCommand::setCatalog(std::shared_ptr<BackupCatalog> catalog) {
  this->catalog = catalog;
}

CreateArchiveCatalogCommand::CreateArchiveCatalogCommand(shared_ptr<BackupCatalog> catalog) {

  this->catalog = catalog;
  this->tag = CREATE_ARCHIVE;

}

void CreateArchiveCatalogCommand::execute(bool existsOk) 
  throw(CArchiveIssue) {

  shared_ptr<CatalogDescr> temp_descr(nullptr);

  /*
   * Die hard in case no catalog available.
   */
  if (this->catalog == NULL)
    throw CArchiveIssue("could not execute archive command: no catalog");

  /*
   * Open the catalog, if not yet done
   */
  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  /*
   * If existsOk is TRUE and we have an
   * existing archive entry in the catalog, proceed.
   * Otherwise we die a hard and unpleasent immediate
   * death...
   */
  temp_descr =this->catalog->exists(this->directory);
  if (temp_descr->id < 0) {

    this->catalog->startTransaction();

    /*
     * This is a new archive entry.
     */
    this->catalog->createArchive(make_shared<CatalogDescr>(*this));

    this->catalog->commitTransaction();

  }
  else {

    if (!existsOk) {
      ostringstream oss;
      oss << "archive already exists: \"" << this->directory << "\"";
      throw CArchiveIssue("archive already exists: ");
    }

    /*
     * ID of the existing catalog entry needed.
     */
    this->id = temp_descr->id;

    this->catalog->startTransaction();

    /*
     * Update the existing archive catalog entry.
     */
    this->catalog->updateArchiveAttributes(make_shared<CatalogDescr>(*this),
                                           this->affectedAttributes);

    /*
     * ... and we're done.
     */
    this->catalog->commitTransaction();
  }



}
