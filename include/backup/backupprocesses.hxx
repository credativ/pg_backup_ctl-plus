#ifndef __BACKUP_PROCESSES__
#define __BACKUP_PROCESSES__

/* STL headers */
#include <queue>

/* pg_backup_ctl++ headers */
#include <descr.hxx>
#include <backup.hxx>
//#include <fs-archive.hxx>

/* PostgreSQL client API */
#include <libpq-fe.h>

#define MAXXLOGFNAMELEN MAXFNAMELEN

namespace credativ {

  /* forward declarations */
  class BackupDirectory;
  class StreamingBaseBackupDirectory;
  class StreamBaseBackup;
  class BackupFile;

  /*
   * State of base backup stream.
   */
  typedef enum {

    BASEBACKUP_STARTED,
    BASEBACKUP_START_POSITION,
    BASEBACKUP_END_POSITION,
    BASEBACKUP_TABLESPACE_META,
    BASEBACKUP_TABLESPACE_READY,
    BASEBACKUP_STEP_TABLESPACE,
    BASEBACKUP_STEP_TABLESPACE_BASE,
    BASEBACKUP_EOB,
    BASEBACKUP_INIT

  } BaseBackupState;

  /*
   * Backup tablespace step information structure.
   */
  struct BackupTablespaceStep {
    int current_step = -1;
    PGresult *handle = NULL;

    /*
     * Assigned file handle.
     */
    std::shared_ptr<BackupFile> file ;

    /*
     * Corresponding tablespace catalog handle
     */
    std::shared_ptr<BackupTablespaceDescr> descr;
  };

  /*
   * Implementation of class WALStreamerProcess
   */
  class WALStreamerProcess : public CPGBackupCtlBase {
  protected:
  public:
    WALStreamerProcess(PGconn *prepared_connection);
    ~WALStreamerProcess();
  };

  /*
   * Implements the base backup streaming
   * infrastructure.
   */
  class BaseBackupProcess : public CPGBackupCtlBase {

  protected:
    BaseBackupState current_state;
    PGconn *pgconn;
    std::shared_ptr<BackupProfileDescr> profile;
    std::shared_ptr<BaseBackupDescr> baseBackupDescr = nullptr;
    std::queue<std::shared_ptr<BackupTablespaceDescr>> tablespaces;

    int         timeline;
    std::string xlogpos;

    /*
     * Internal state of tablespace backup steps.
     */
    struct BackupTablespaceStep stepInfo;

  public:

    BaseBackupProcess(PGconn *prepared_connection,
                      std::shared_ptr<BackupProfileDescr> profile);
    ~BaseBackupProcess();

    /*
     * Returns a BaseBackupDescr describing the basebackup
     * started with a BaseBackupProcess instance. Only valid,
     * iff called after start(), otherwise you get a nullptr
     * instance.
     */
    std::shared_ptr<BaseBackupDescr> getBaseBackupDescr();

    /*
     * Start a BASE_BACKUP stream.
     *
     * Read the starting position from the initialized
     * basebackup stream.
     */
    virtual void start();

    /*
     * Request meta information of all tablespaces
     * to be included in the backup. If the internal state
     * machine is not properly synced with the streaming
     * protocol state, this will throw a StreamingExecution
     * exception.
     */
    virtual void readTablespaceInfo();

    /*
     * Backup the requested tablespace OID. Throws a
     * StreamingExecution exception in case the OID
     * is not in the internal tablespace meta info.
     */
    void backupTablespace(std::shared_ptr<BackupTablespaceDescr> descr);

    /*
     * Calls the final step in the streaming basebackup
     * protocol. Receives the WAL end position in the stream
     * and finalizes the basebackup stream.
     */
    virtual void end();

    /*
     * Step through the interal tablespace meta info
     * (initialized by calling readTablespaceInfo()), and
     * backup 'em into files into the catalog. The method
     * requires an correctly initialized archive handle
     * to write the files properly.
     *
     * Can be called multiple times as long as
     * the internal tablespace meta info has OIDs to
     * backup left and true is returned.
     * If no more tablespaces are there
     * to backup, this method returns false.
     *
     * The descr handle passed to stepTablespace() is assigned to
     * the current tablespace descriptor used for the last recent step.
     * The caller can just pass null and will get a valid pointer in case
     * the current step is operating on a valid tablespace meta entry.
     *
     * Please note that calling stepTablespace() multiple times without
     * a curse through backupTablespace() will throw a StreamingFailure
     * exception, since the tablespace data needs to be streamed to keep
     * the PostgreSQL protocol in sync.
     */
    virtual bool stepTablespace(std::shared_ptr<StreamBaseBackup> backupHandle,
                                std::shared_ptr<BackupTablespaceDescr> &descr);
  };

}

#endif
