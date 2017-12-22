#ifndef __HAVE_COMMANDS__
#define __HAVE_COMMANDS__

#include <BackupCatalog.hxx>
#include <fs-archive.hxx>
#include <signalhandler.hxx>

#include <vector>

using namespace credativ;

namespace credativ {

  class BaseCatalogCommand : public CatalogDescr {
  protected:
    /**
     * Internal reference to sig stop signal handler.
     */
    JobSignalHandler *stopHandler = nullptr;

    std::shared_ptr<BackupCatalog> catalog = NULL;
    virtual void copy(CatalogDescr& source);
  public:
    virtual void execute(bool existsOk) = 0;

    virtual ~BaseCatalogCommand();

    virtual void setCatalog(std::shared_ptr<BackupCatalog> catalog);
    virtual std::shared_ptr<BackupCatalog> getCatalog();

    /**
     * Assigns specific signal handler objects to
     * a BaseCatalogCommand descendant instance. Currently
     * supported are: stop signal.
     */
    virtual void assignSigStopHandler(JobSignalHandler *handler);
  };

  class DropConnectionCatalogCommand : public BaseCatalogCommand {
  public:
    DropConnectionCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    DropConnectionCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    DropConnectionCatalogCommand();

    virtual void execute(bool flag);
  };

  class ListConnectionCatalogCommand : public BaseCatalogCommand {
  public:
    ListConnectionCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    ListConnectionCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    ListConnectionCatalogCommand();

    virtual void execute(bool flag);
  };

  class CreateConnectionCatalogCommand : public BaseCatalogCommand {
  public:
    CreateConnectionCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    CreateConnectionCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    CreateConnectionCatalogCommand();

    virtual void execute(bool flag);
  };

  class StartLauncherCatalogCommand : public BaseCatalogCommand {
  public:
    StartLauncherCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    StartLauncherCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    StartLauncherCatalogCommand();

    virtual void execute(bool flag);
  };

  class ListBackupCatalogCommand : public BaseCatalogCommand {
  public:
    ListBackupCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    ListBackupCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    ListBackupCatalogCommand();

    virtual void execute(bool flag);
  };

  class StartBasebackupCatalogCommand : public BaseCatalogCommand {
  public:
    StartBasebackupCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    StartBasebackupCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    StartBasebackupCatalogCommand();

    virtual void execute(bool background);
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

    virtual void execute(bool noop);
  };

  /*
   * Implements a START STREAMING FOR ARCHIVE command handler.
   */
  class StartStreamingForArchiveCommand : public BaseCatalogCommand {
  private:
    /**
     * PostgreSQL Streaming handle.
     */
    PGStream *pgstream = nullptr;

    /**
     * Internal handle for archive directory.
     */
    std::shared_ptr<BackupDirectory> archivedir = nullptr;

    /**
     * Internal ArchiveLogDirectory handle.
     */
    std::shared_ptr<ArchiveLogDirectory> logdir = nullptr;

    /**
     * Internal backup handler.
     */
    std::shared_ptr<TransactionLogBackup> backup = nullptr;

    /**
     * Catalog handle we operate on. Note that we
     * don't use the properties of our own instance here, since
     * we need the properties stored within the catalog. Since this Command
     * instance is required to retain its own identity, we maintain
     * a private copy of the CatalogDescr retrieved from the
     * catalog database.
     */
    std::shared_ptr<CatalogDescr> temp_descr = nullptr;

    /**
     * Helper function to update current status and XLOG position of stream.
     */
    virtual void updateStreamCatalogStatus(StreamIdentification &ident);

    /**
     * Prepare internal stream handle
     */
    virtual void prepareStream();

    /**
     * Finalize stream and shutdown.
     */
    virtual void finalizeStream();

  public:
    StartStreamingForArchiveCommand(std::shared_ptr<BackupCatalog> catalog);
    StartStreamingForArchiveCommand(std::shared_ptr<CatalogDescr> descr);
    StartStreamingForArchiveCommand();
    virtual ~StartStreamingForArchiveCommand();

    virtual void execute(bool noop);
  };

  /*
   * Implements a dummy command to be passed to background workers.
   *
   * This commmand is a wrapper around commands, that
   * are elected to be executed in a background worker.
   *
   * It doesn't do very much for the moment.
   */
  class BackgroundWorkerCommandHandle : public BaseCatalogCommand {
  protected:
    CatalogTag subTag = EMPTY_DESCR;
  public:
    BackgroundWorkerCommandHandle(std::shared_ptr<BackupCatalog> catalog);
    BackgroundWorkerCommandHandle(std::shared_ptr<CatalogDescr> descr);
    BackgroundWorkerCommandHandle();

    virtual void execute(bool noop);
  };

}
#endif
