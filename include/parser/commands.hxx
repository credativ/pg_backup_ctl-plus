#ifndef __HAVE_COMMANDS__
#define __HAVE_COMMANDS__

#include <BackupCatalog.hxx>
#include <fs-archive.hxx>

#include <vector>

using namespace credativ;

namespace credativ {

  class BaseCatalogCommand : public CatalogDescr {
  protected:
    std::vector<int> affectedAttributes;
    std::shared_ptr<BackupCatalog> catalog = NULL;
  public:
    virtual void execute(bool existsOk)
      throw(CPGBackupCtlFailure) = 0;

    virtual void pushAffectedAttribute(int colId)
      throw(CArchiveIssue);

    virtual ~BaseCatalogCommand();

    virtual void setCatalog(std::shared_ptr<BackupCatalog> catalog);
  };

  class VerifyArchiveCatalogCommand : public BaseCatalogCommand {
  public:

    VerifyArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    VerifyArchiveCatalogCommand();

    virtual void execute(bool missingOk)
      throw(CPGBackupCtlFailure);
  };

  class CreateArchiveCatalogCommand : public BaseCatalogCommand {
  public:
    /*
     * Default c'tor needs BackupCatalog handle.
     */
    CreateArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    CreateArchiveCatalogCommand();

    virtual void execute(bool existsOk)
      throw(CPGBackupCtlFailure);
  };

  /*
   * Implements DROP ARCHIVE command
   */
  class DropArchiveCatalogCommand : public BaseCatalogCommand {
  public:

    /*
     * Default c'tor needs BackupCatalog handle.
     */
    DropArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    DropArchiveCatalogCommand();

    virtual void execute(bool existsOk)
      throw(CPGBackupCtlFailure);

  };

  /*
   * Implements ALTER ARCHIVE command
   */
  class AlterArchiveCatalogCommand : public BaseCatalogCommand {
  public:
    /*
     * Default c'tor needs BackupCatalog handle
     */

    AlterArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    AlterArchiveCatalogCommand();

    virtual void execute(bool ignoreMissing)
      throw (CPGBackupCtlFailure);
  };

  typedef enum {
    ARCHIVE_LIST,
    ARCHIVE_FILTERED_LIST,
    ARCHIVE_DETAIL_LIST
  } ListArchiveOutputMode;

  /*
   * Implements the LIST ARCHIVE command.
   *
   * NOTE:
   *    In contrast to other BaseCatalogCommand implementations, this
   *    command does directly write to /dev/stdout.
   */
  class ListArchiveCatalogCommand : public BaseCatalogCommand {
  private:
    ListArchiveOutputMode mode;
  public:
    /*
     * Default c'tor
     */
    ListArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    ListArchiveCatalogCommand();

    virtual void setOutputMode(ListArchiveOutputMode mode);

    virtual void execute (bool extendedOutput)
      throw (CPGBackupCtlFailure);
  };

}

#endif
