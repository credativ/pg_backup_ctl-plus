#ifndef __HAVE_COMMANDS__
#define __HAVE_COMMANDS__

#include <BackupCatalog.hxx>
#include <commands.hxx>
#include <fs-archive.hxx>

#include <vector>

using namespace credativ;

namespace credativ {

  class BaseCatalogCommand : public CatalogDescr {
  protected:
    std::vector<int> affectedAttributes;
    std::shared_ptr<BackupCatalog> catalog = NULL;
  public:
    virtual void execute(bool existsOk) = 0;

    virtual void pushAffectedAttribute(int colId)
      throw(CArchiveIssue);

    virtual ~BaseCatalogCommand();

    virtual void setCatalog(std::shared_ptr<BackupCatalog> catalog);
  };

  class CreateArchiveCatalogCommand : public BaseCatalogCommand {
  public:
    /*
     * Default c'tor needs BackupCatalog handle.
     */
    CreateArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    CreateArchiveCatalogCommand();

    virtual void execute(bool existsOk) throw(CArchiveIssue);
  };

  class DropArchiveCatalogCommand : public BaseCatalogCommand {
  public:

    /*
     * Default c'tor needs BackupCatalog handle.
     */
    DropArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    DropArchiveCatalogCommand();

    virtual void execute(bool existsOk);

  };

}

#endif
