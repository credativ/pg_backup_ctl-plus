#ifndef __HAVE_COMMANDS__
#define __HAVE_COMMANDS__

#include <BackupCatalog.hxx>
#include <fs-archive.hxx>

#include <vector>

using namespace credativ;

namespace credativ {

  class BaseCatalogCommand : public CatalogDescr {
  protected:
    std::shared_ptr<BackupCatalog> catalog = NULL;
    virtual void copy(CatalogDescr& source);
  public:
    virtual void execute(bool existsOk) = 0;

    virtual ~BaseCatalogCommand();

    virtual void setCatalog(std::shared_ptr<BackupCatalog> catalog);
  };

  class VerifyArchiveCatalogCommand : public BaseCatalogCommand {
  public:

    VerifyArchiveCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    VerifyArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    VerifyArchiveCatalogCommand();

    virtual void execute(bool missingOk);
  };

  class CreateArchiveCatalogCommand : public BaseCatalogCommand {
  public:
    /*
     * Default c'tor needs BackupCatalog handle.
     */
    CreateArchiveCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    CreateArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    CreateArchiveCatalogCommand();

    virtual void execute(bool existsOk);
  };

  /*
   * Implements DROP ARCHIVE command
   */
  class DropArchiveCatalogCommand : public BaseCatalogCommand {
  public:

    /*
     * Default c'tor needs BackupCatalog handle.
     */
    DropArchiveCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    DropArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    DropArchiveCatalogCommand();

    virtual void execute(bool existsOk);

  };

  /*
   * Implements ALTER ARCHIVE command
   */
  class AlterArchiveCatalogCommand : public BaseCatalogCommand {
  public:
    /*
     * Default c'tor needs BackupCatalog handle
     */
    AlterArchiveCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    AlterArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    AlterArchiveCatalogCommand();

    virtual void execute(bool ignoreMissing);
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
    ListArchiveCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    ListArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    ListArchiveCatalogCommand();

    virtual void setOutputMode(ListArchiveOutputMode mode);

    virtual void execute (bool extendedOutput);
  };

  /*
   * Implements the START BASEBACKUP command.
   */
  class StartBaseBackupCatalogCommand : public BaseCatalogCommand {
  public:
    StartBaseBackupCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    StartBaseBackupCatalogCommand();

    virtual void execute(bool ignored);
  };

  /*
   * Implements the CREATE BACKUP PROFILE command.
   */
  class CreateBackupProfileCatalogCommand : public BaseCatalogCommand {
  private:
    std::shared_ptr<BackupProfileDescr> profileDescr;
  public:
    CreateBackupProfileCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    CreateBackupProfileCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    CreateBackupProfileCatalogCommand();

    virtual void execute(bool force_default);
    virtual void setProfile(std::shared_ptr<BackupProfileDescr> profileDescr);
  };

  /*
   * Implements the LIST BACKUP PROFILE command
   */
  class ListBackupProfileCatalogCommand : public BaseCatalogCommand {
  public:
    ListBackupProfileCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    ListBackupProfileCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    ListBackupProfileCatalogCommand();

    virtual void execute(bool extended);
  };

  /*
   * Implements the DROP BACKUP PROFILE command
   */
  class DropBackupProfileCatalogCommand : public BaseCatalogCommand {
  public:
    DropBackupProfileCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    DropBackupProfileCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    DropBackupProfileCatalogCommand();

    void execute(bool noop);
  };
}

#endif
