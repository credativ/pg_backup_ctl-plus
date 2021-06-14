#ifndef __BACKUP_PROCESSES__
#define __BACKUP_PROCESSES__

/* STL headers */
#include <queue>

/* pg_backup_ctl++ headers */
#include <descr.hxx>
#include <streamident.hxx>
#include <shm.hxx>
#include <backup.hxx>
#include <signalhandler.hxx>
#include <xlogdefs.hxx>

#define MAXXLOGFNAMELEN MAXFNAMELEN

namespace pgbckctl {

  /* forward declarations */
  class BackupDirectory;
  class StreamingBaseBackupDirectory;
  class StreamBaseBackup;
  class BackupFile;
  class TransactionLogBackup;

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
    BASEBACKUP_STEP_TABLESPACE_INTERRUPTED,
    BASEBACKUP_MANIFEST_INTERRUPTED,
    BASEBACKUP_EOB,
    BASEBACKUP_INIT

  } BaseBackupState;

  /*
   * State of archiver (WAL Streamer).
   */
  typedef enum {
    ARCHIVER_STARTUP,
    ARCHIVER_START_POSITION,
    ARCHIVER_STREAMING,
    ARCHIVER_END_POSITION,
    ARCHIVER_TIMELINE_SWITCH,
    ARCHIVER_SHUTDOWN,
    ARCHIVER_STREAMING_TIMEOUT,
    ARCHIVER_STREAMING_INTR,
    ARCHIVER_STREAMING_ERROR,
    ARCHIVER_STREAMING_NO_DATA
  } ArchiverState;

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
  private:

    /**
     * Internal method to check whether a specified
     * stop handler was set.
     */
    virtual bool stopHandlerWantsExit();

    /**
     * Signal handler instances.
     */
    JobSignalHandler *stopHandler = nullptr;

  protected:

    ArchiverState current_state;
    PGconn *pgconn;
    StreamIdentification streamident;

    /**
     * Internal transaction log backup handler.
     */
    std::shared_ptr<TransactionLogBackup> backupHandler = nullptr;

    /**
     * Timeout for polling on WAL stream.
     *
     * Default is 10000ms
     *
     *
     * NOTE: This timeout value should never be
     * larger than the receiver_status_timeout below.
     */
    long timeout = 10000;

    /**
     * Timeout when to send status update to upstream, in milliseconds.
     * Default is 60s
     */
    long receiver_status_timeout = 60000;

    /**
     * Defines the interval since when
     * we are forced to send receiver status updates.
     */
    std::chrono::high_resolution_clock::time_point last_status_update;

    /**
     * Receive buffer
     */
    MemoryBuffer receiveBuffer;

    /**
     * Send buffer.
     */
    MemoryBuffer sendBuffer;

    /**
     * Poll on receiving WAL stream.
     *
     * No connection checks done here, caller is assumed
     * to have properly checked the PostgreSQL server connection
     * to be available.
     */
    virtual ArchiverState receivePoll();

    /**
     * Handle receiving a buffer from current stream.
     */
    virtual ArchiverState handleReceive(char **buffer, int *bufferlen);

    /**
     * Internal helper method to read a timeline switch
     * message after indicated a end-of-stream condition
     * on the current stream handle. See handleEndOfStream() for
     * details.
     */
    virtual void endOfStreamTimelineSwitch(PGresult *result,
                                           unsigned int& timeline,
                                           std::string& xlogpos);

    /*
     * Handle end-of-stream conditions. This could either
     * be a shutdown, or we've changed the timeline
     * on the server.
     */
    virtual PGresult *handleEndOfStream();

    /*
     * Calculates a timeval value suitable to
     * be passed to select().
     */
    void timeoutSelectValue(timeval *timeoutptr);

  public:

    WALStreamerProcess(PGconn *prepared_connection,
                       StreamIdentification streamident);
    ~WALStreamerProcess();

    /**
     * Assign a stop signal handler to a WALStreamerProcess
     * instance. This handler is used to check whether we
     * received an asynchronous stop signal somehow.
     */
    virtual void assignStopHandler(JobSignalHandler *handler);

    /**
     * Returns the current xlog position of the WAL Streamer.
     *
     * This is *not* the current position the WAL Streamer is streaming on,
     * but its write position, so you get the current position where
     * the WAL Streamer was last seen to write to.
     *
     * NOTE: when not in streaming mode, the reported location might
     *       be just garbage (that said, you can trust this information
     *       only when entered ARCHIVER_STREAMING and the first bytes
     *       has arrived). A WAL Streamer instance might initialize
     *       the current write location only if it starts to receive
     *       bytes.
     */
    XLogRecPtr getCurrentXLOGPos();

    /**
     * Returns a copy of the internal StreamIdentification state
     * object.
     */
    virtual StreamIdentification identification();

    /**
     * Returns the current timeline a WAL Streamer instance
     * is streaming from.
     */
    unsigned int getCurrentTimeline();

    /**
     * Start streaming of XLOG records.
     */
    virtual void start();

    /**
     * Receive XLOG data from stream.
     *
     * Requires a successful call to start() to begin
     * XLOG streaming. Returns FALSE if the WAL stream stopped,
     * otherwise will enter a receive loop.
     *
     * The caller can check by the method reason(), whether
     * the XLOG stream terminated somehow or we need to create a new log
     * segment or et al. See reason() for more information.
     */
    virtual bool receive();

    /**
     * Returns an identifier indicating the
     * current status of the XLOG stream.
     */
    ArchiverState reason();

    /**
     * finalizeSegment() finalizes the current XLOG segment file.
     *
     * If reason() returns ARCHIVER_END_POSITION we need to handle
     * the end of archive condition accordingly.
     */
    virtual void finalizeSegment();

    /**
     * Assigns a directory handle to a WALStreamerProcess instance.
     */
    virtual void setBackupHandler(std::shared_ptr<TransactionLogBackup> backupHandler);

    /**
     * Returns the current encoded XLOG position, if active.
     */
    virtual std::string xlogpos();

    /**
     * Handles XLOG data messages.
     *
     * Iff the WALStreamer has a BackupDirectory object
     * assigned, this will also safe the WAL stream to disk, according
     * to the object type specified here (see fs-archive.hxx for details).
     */
    virtual void handleMessage(XLOGStreamMessage *message);

    /**
     * Ends a copy stream in progress.
     *
     * This sends a end-of-copy message to the connected
     * streaming server.
     */
    virtual PGresult *end();

    /**
     * sendStatusUpdate() sends a ReceiverStatusUpdateMessage to the
     * connected stream.
     */
    virtual ArchiverState sendStatusUpdate();

    /**
     * Assign a timeout value for receiver status updates.
     * The default is 60s.
     *
     * NOTE: This will throw a StreamingFailure in case
     *       the value is lower than 10s.
     *
     *       10s currently is the internal fixed timeout value for
     *       polling on the PostgreSQL streaming socket.
     */
    virtual void setReceiverStatusTimeout(long value);

  };

  /*
   * Implements the base backup streaming
   * infrastructure.
   */
  class BaseBackupProcess : public CPGBackupCtlBase {
  private:

    /**
     * Internal method to check whether a specified
     * stop handler was set.
     */
    virtual bool stopHandlerWantsExit();

    /**
     * Signal handler instances.
     */
    JobSignalHandler *stopHandler = nullptr;

  protected:
    BaseBackupState current_state;
    PGconn *pgconn;
    std::shared_ptr<BackupProfileDescr> profile;
    std::shared_ptr<BaseBackupDescr> baseBackupDescr = nullptr;
    std::queue<std::shared_ptr<BackupTablespaceDescr>> tablespaces;

    int         timeline;
    std::string xlogpos;
    std::string systemid;
    unsigned long long wal_segment_size = 0;

    /*
     * Internal state of tablespace backup steps.
     */
    struct BackupTablespaceStep stepInfo;

  public:

    BaseBackupProcess(PGconn *prepared_connection,
                      std::shared_ptr<BackupProfileDescr> profile,
                      std::string systemid,
                      unsigned long long wal_segment_size);
    ~BaseBackupProcess();

    /*
     * Returns a BaseBackupDescr describing the basebackup
     * started with a BaseBackupProcess instance. Only valid,
     * iff called after start(), otherwise you get a nullptr
     * instance.
     */
    std::shared_ptr<BaseBackupDescr> getBaseBackupDescr();

    /*
     * Returns the system identifier of this basebackup. If not
     * yet performed, an empty string will be returned.
     */
    virtual std::string getSystemIdentifier();

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

    /**
     * Returns current status of a basebackup handle.
     */
    virtual BaseBackupState getState();

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

    /**
     * Assign a stop signal handler to a WALStreamerProcess
     * instance. This handler is used to check whether we
     * received an asynchronous stop signal somehow.
     */
    virtual void assignStopHandler(JobSignalHandler *handler);

    /**
     * Receives a backup manifest, if requested by the backup profile.
     */
    void receiveManifest(std::shared_ptr<StreamBaseBackup> backupHandle);

  };

}

#endif
