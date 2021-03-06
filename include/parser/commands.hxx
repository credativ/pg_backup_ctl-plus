#ifndef __HAVE_COMMANDS__
#define __HAVE_COMMANDS__

#include <rtconfig.hxx>
#include <BackupCatalog.hxx>
#include <fs-archive.hxx>
#include <signalhandler.hxx>
#include <streamident.hxx>
#include <backupcleanupdescr.hxx>

#include <vector>

using namespace pgbckctl;

namespace pgbckctl {

  /* forwarded declarations */
  class BackupCatalog;
  class PGStream;
  class BackupDirectory;
  class ArchiveLogDirectory;
  class TransactionLogBackup;

  class BaseCatalogCommand : public CatalogDescr {
  protected:
    /**
     * Internal reference to sig stop signal handler.
     */
    JobSignalHandler *stopHandler = nullptr;

    /**
     * Internal reference to sig int handler.
     */
    JobSignalHandler *intHandler = nullptr;

    /**
     * Pointer to internal catalog handler.
     *
     * This is usually instantiated and initialized
     * by an external caller who maintaints our object instance
     * and assigned via setCatalog().
     */
    std::shared_ptr<BackupCatalog> catalog = NULL;

    /**
     * If attached to a shm_worker_area shared memory
     * segment, worker_id will identify the used slot.
     *
     * Set to -1 if no shared memory segment is in use (which
     * is true in case a catalog command is executed within
     * a background job.
     */
    int worker_id = -1;

    /**
     * Legwork method for deep copy operation
     * on this instance.
     */
    virtual void copy(CatalogDescr& source);
  public:
    virtual void execute(bool existsOk) = 0;

    virtual ~BaseCatalogCommand();

    virtual void setCatalog(std::shared_ptr<BackupCatalog> catalog);
    virtual std::shared_ptr<BackupCatalog> getCatalog();

    /**
     * Assign a SIGTERM signal handler object.
     */
    virtual void assignSigStopHandler(JobSignalHandler *handler);

    /**
     * Assign a SIGINT signal handler object.
     */
    virtual void assignSigIntHandler(JobSignalHandler *handler);

    /**
     * Assign worker ID to a catalog command instance. This is
     * done via background command instantiation.
     */
    virtual void setWorkerID(int worker_id);

  };

  class DropBasebackupCatalogCommand : public BaseCatalogCommand {
  public:

    DropBasebackupCatalogCommand();
    DropBasebackupCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    DropBasebackupCatalogCommand(std::shared_ptr<BackupCatalog> catalog);

    virtual ~DropBasebackupCatalogCommand();

    virtual void execute(bool flag);

  };

  class ShowVariableCatalogCommand : public BaseCatalogCommand {
  public:
    ShowVariableCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    ShowVariableCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    ShowVariableCatalogCommand();
    virtual ~ShowVariableCatalogCommand();

    virtual void execute(bool flag);
  };

  class ResetVariableCatalogCommand : public BaseCatalogCommand {
  public:
    ResetVariableCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    ResetVariableCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    ResetVariableCatalogCommand();
    virtual ~ResetVariableCatalogCommand();

    virtual void execute(bool flag);
  };

  class SetVariableCatalogCommand : public BaseCatalogCommand {
  public:
    SetVariableCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    SetVariableCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    SetVariableCatalogCommand();
    virtual ~SetVariableCatalogCommand();

    virtual void execute(bool flag);
  };

  class ShowVariablesCatalogCommand : public BaseCatalogCommand {
  public:
    ShowVariablesCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    ShowVariablesCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    ShowVariablesCatalogCommand();
    virtual ~ShowVariablesCatalogCommand();

    virtual void execute(bool flag);
  };

  class ExecCommandCatalogCommand : public BaseCatalogCommand {
  public:
    ExecCommandCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    ExecCommandCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    ExecCommandCatalogCommand();

    virtual void execute(bool flag);
  };

  class PinCatalogCommand : public BaseCatalogCommand {
  public:
    PinCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    PinCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    PinCatalogCommand();

    virtual ~PinCatalogCommand() {};

    virtual void execute(bool flag);
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

  class DropRetentionPolicyCommand : public BaseCatalogCommand {
  public:

    DropRetentionPolicyCommand();
    DropRetentionPolicyCommand(std::shared_ptr<CatalogDescr> descr);
    DropRetentionPolicyCommand(std::shared_ptr<BackupCatalog> catalog);

    virtual ~DropRetentionPolicyCommand();

    virtual void execute(bool flag);
  };

  class ApplyRetentionPolicyCommand : public BaseCatalogCommand {
  private:

    /*
     * List of basebackups where the retention policy should
     * be applied on. Populated by execute().
     */
    std::vector<std::shared_ptr<BaseBackupDescr>> bblist;

    /*
     * Executes the retrieved retention policy.
     */
    std::shared_ptr<BackupCleanupDescr> applyRulesAndRemoveBasebackups(std::shared_ptr<CatalogDescr> archiveDescr);

  public:

    ApplyRetentionPolicyCommand(std::shared_ptr<CatalogDescr> descr);
    ApplyRetentionPolicyCommand(std::shared_ptr<BackupCatalog> catalog);
    ApplyRetentionPolicyCommand();

    virtual ~ApplyRetentionPolicyCommand();

    virtual void execute(bool flag);

  };

  class CreateRetentionPolicyCommand : public BaseCatalogCommand {
  public:

    CreateRetentionPolicyCommand(std::shared_ptr<CatalogDescr> descr);
    CreateRetentionPolicyCommand(std::shared_ptr<BackupCatalog> catalog);
    CreateRetentionPolicyCommand();

    virtual ~CreateRetentionPolicyCommand() {};

    virtual void execute(bool flag);
  };

  class ListRetentionPoliciesCommand : public BaseCatalogCommand {
  public:
    ListRetentionPoliciesCommand(std::shared_ptr<CatalogDescr> descr);
    ListRetentionPoliciesCommand(std::shared_ptr<BackupCatalog> catalog);
    ListRetentionPoliciesCommand();
    virtual ~ListRetentionPoliciesCommand();

    virtual void execute(bool flag);
  };

  class ListRetentionPolicyCommand : public BaseCatalogCommand {
  public:

    ListRetentionPolicyCommand(std::shared_ptr<CatalogDescr> descr);
    ListRetentionPolicyCommand(std::shared_ptr<BackupCatalog> catalog);
    ListRetentionPolicyCommand();
    virtual ~ListRetentionPolicyCommand();

    virtual void execute(bool flag);
  };

  class CreateConnectionCatalogCommand : public BaseCatalogCommand {
  public:
    CreateConnectionCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    CreateConnectionCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    CreateConnectionCatalogCommand();

    virtual ~CreateConnectionCatalogCommand();

    virtual void execute(bool flag);
  };

  class StartLauncherCatalogCommand : public BaseCatalogCommand {
  protected:
    std::shared_ptr<CatalogProc> procInfo = nullptr;
  public:
    StartLauncherCatalogCommand(std::shared_ptr<CatalogDescr> descr);
    StartLauncherCatalogCommand(std::shared_ptr<BackupCatalog> catalog);
    StartLauncherCatalogCommand();

    virtual void execute(bool flag);
  };

  class ListBackupListCommand : public BaseCatalogCommand {
  public:
    ListBackupListCommand(std::shared_ptr<CatalogDescr> descr);
    ListBackupListCommand(std::shared_ptr<BackupCatalog> catalog);

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
  protected:

    /**
     * This is a helper method, performing various checks before starting
     * a basebackup. Returns a BackupCatalogErrorCode flag, telling the
     * current state whether START BASEBACKUP is allowed to proceed.
     *
     * check() expects a StreamIdentification structure initialized
     * by a PGStream::identify() call.
     *
     * If everything is in shape, check() returns a BASEBACKUP_CATALOG_OK
     * flag, telling us that we are allowed to proceed.
     */
    virtual BackupCatalogErrorCode check(int archive_id,
                                         StreamIdentification ident);

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

    /**
     * Internal check for various settings passed
     * to CREATE BACKUP PROFILE.
     */
    virtual void verify(bool print_version = false);

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
   * Implements a START RECOVERY STREAM FOR ARCHIVE command handler.
   */
  class StartRecoveryArchiveCommand : public BaseCatalogCommand {
  private:
  public:

    StartRecoveryArchiveCommand(std::shared_ptr<BackupCatalog> catalog);
    StartRecoveryArchiveCommand(std::shared_ptr<CatalogDescr> descr);
    StartRecoveryArchiveCommand();
    virtual ~StartRecoveryArchiveCommand();

    virtual void execute(bool flag);
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

  /**
   * Implement the SHOW WORKERS command.
   */
  class ShowWorkersCommandHandle : public BaseCatalogCommand {
  public:
    ShowWorkersCommandHandle(std::shared_ptr<BackupCatalog> catalog);
    ShowWorkersCommandHandle(std::shared_ptr<CatalogDescr> descr);
    ShowWorkersCommandHandle();

    virtual ~ShowWorkersCommandHandle();

    virtual void execute(bool noop);
  };

  /**
   * Implements the STOP STREAMING FOR ARCHIVE command.
   *
   */
  class StopStreamingForArchiveCommandHandle : public BaseCatalogCommand {
  private:
  public:
    StopStreamingForArchiveCommandHandle(std::shared_ptr<BackupCatalog> catalog);
    StopStreamingForArchiveCommandHandle(std::shared_ptr<CatalogDescr> descr);
    StopStreamingForArchiveCommandHandle();

    virtual ~StopStreamingForArchiveCommandHandle();

    virtual void execute(bool noop);
  };

  /**
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

  /**
   * Implements the RESTORE FROM ARCHIVE command.
   *
   */
  class RestoreFromArchiveCommandHandle : public BaseCatalogCommand {
  private:
  public:
    RestoreFromArchiveCommandHandle(std::shared_ptr<BackupCatalog> catalog);
    RestoreFromArchiveCommandHandle(std::shared_ptr<CatalogDescr> descr);
    RestoreFromArchiveCommandHandle();

    virtual ~RestoreFromArchiveCommandHandle();

    virtual void execute(bool noop);
  };

  /**
   * Implements the STAT ARCHIVE ... BASEBACKUP command.
   */
  class StatArchiveBaseBackupCommand : public BaseCatalogCommand {
  public:

    StatArchiveBaseBackupCommand(std::shared_ptr<CatalogDescr> descr);
    StatArchiveBaseBackupCommand(std::shared_ptr<BackupCatalog> catalog);
    StatArchiveBaseBackupCommand();

    virtual ~StatArchiveBaseBackupCommand();

    virtual void execute(bool noop);

  };
}

#endif
