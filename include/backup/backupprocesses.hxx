#ifndef __BACKUP_PROCESSES__
#define __BACKUP_PROCESSES__

/* STL headers */
#include <queue>

/* pg_backup_ctl++ headers */
#include <BackupCatalog.hxx>
#include <descr.hxx>
#include <streamident.hxx>
#include <shm.hxx>
#include <backup.hxx>
#include <signalhandler.hxx>
#include <basebackupmsg.hxx>
#include <xlogdefs.hxx>

#define MAXXLOGFNAMELEN MAXFNAMELEN

namespace pgbckctl {

  /* forward declarations */
  class BackupDirectory;
  class StreamingBaseBackupDirectory;
  class StreamBaseBackup;
  class BackupFile;
  class TransactionLogBackup;
  class BackupCatalog;

  /**
   * @ref BaseBackupState State of base backup stream.
   */
  typedef enum {

    /**
     * Base backup stream was started and is fully initialized.
     */
    BASEBACKUP_STARTED,

    /**
     * Base backup stream was identified.
     */
    BASEBACKUP_START_POSITION,

    /**
     * Base backup stream has reached end and should
     * be terminated.
     */
    BASEBACKUP_END_POSITION,

    /**
     * We are about to request tablespace meta data from the
     * stream.
     */
    BASEBACKUP_TABLESPACE_META,

    /**
     * In tablespace data stream
     */
    BASEBACKUP_TABLESPACE_STREAM,

    /**
     * The tablespace meta data was fully read.
     */
    BASEBACKUP_TABLESPACE_READY,

    /**
     * In tablespace streaming mode, we are going  to stream tablespace binary data.
     */
    BASEBACKUP_STEP_TABLESPACE,

    /**
     * Base tablespace is streamed. This doesn't necessarily mean it's
     * the pg_default tablespace, but the very first from the tablespace queue to stream.
     */
    BASEBACKUP_STEP_TABLESPACE_BASE,

    /**
     * Tablespace stream was interrupted.
     */
    BASEBACKUP_STEP_TABLESPACE_INTERRUPTED,

    /**
     * Manifest stream was interrupted.
     */
    BASEBACKUP_MANIFEST_INTERRUPTED,

    /**
     * About to stream manifest data
     */
    BASEBACKUP_MANIFEST_STREAM,

    /**
     * End of basebackup stream reached.
     */
    BASEBACKUP_EOB,

    /**
     * Stream initialization state.
     */
    BASEBACKUP_INIT

  } BaseBackupState;

  /**
   * @ref BASEBACKUP_QUERY_TYPE defines type of query to construct by a BaseBackupStream class.
   */
  typedef enum {

    /** BASE_BACKUP command */
    BASEBACKUP_QUERY_TYPE_BASEBACKUP,

    /** Unknown */
    BASEBACKUP_QUERY_TYPE_UNKNOWN

  } BaseBackupQueryType;

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

  /**
   * tablespace queue definition.
   */
  typedef std::queue<std::shared_ptr<BackupTablespaceDescr>> *tablespace_queue_t;

  /**
   * @ref BackupTablespaceStep
   * Backup tablespace step information structure.
   */
  struct BackupTablespaceStep {
    int current_step = -1;
    PGresult *handle = nullptr;

    /*
     * Assigned file handle.
     */
    std::shared_ptr<BackupFile> file ;

    /*
     * Corresponding tablespace catalog handle
     */
    std::shared_ptr<BackupTablespaceDescr> descr;

    /**
     * Reset state backup to initial.
     */
    void reset() {
      current_step = -1;
      handle       = nullptr;
      file         = nullptr;
      descr        = nullptr;
    }

  };

  /*
   * Implementation of class WALStreamerProcess
   */
  class WALStreamerProcess : public CPGBackupCtlBase,
                             public StopSignalChecker {
  private:
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

  /**
   * @ref TablespaceQueue
   * Class TablespaceQueue holds a queue of tablespace descriptors initialized
   * by a basebackup stream.
   */
  class TablespaceQueue {
  private:
    /**
     * PostgreSQL connection handle.
     */
    PGconn *conn = nullptr;
  protected:

    /**
     * Holds a queue of #BackupTablespaceDescr.
     */
    std::queue<std::shared_ptr<BackupTablespaceDescr>> tablespaces;

    /**
     * Reads tablespace information from basebackup stream
     * @param current state of basebackup stream
     * @return basebackup stream state
     */
    virtual BaseBackupState getTablespaceInfo(BaseBackupState &state);

  public:

    explicit TablespaceQueue(PGconn *conn);
    ~TablespaceQueue() = default;

  };

  /**
   * @ref TablespaceIterator
   *
   * An implementation to help to iterate through basebackup tablespace streams
   */
  class TablespaceIterator {
  private:

    /** Byte counter of bytes streamed */
    size_t _consumed = 0;

  protected:

    /**
     * Internal state of tablespace backup steps.
     */
    struct BackupTablespaceStep stepInfo;

    /**
     * Stepper method. Needs to be implemented for any classes deriving
     * from a #TablespaceIterator.
     */
    virtual bool next(std::shared_ptr<BackupElemDescr> &next) = 0;

    /**
     * Set consumed bytes
     */
    virtual void setConsumed(size_t bytes_consumed);

    /**
     * Reset internal state
     */
    virtual void reset();

    /**
     * Increments the iterator one step.
     */
    virtual void incr();
  public:

    /**
     * Returns the number of bytes consumed by a stream
     */
    virtual size_t consumed();

  };

  /**
   * @ref TablespaceStreamer
   *
   * Based on a TablespaceIterator, saves a tablespace archive stream
   * step by step to the specified backup handler.
   *
   * This primarily implements the protocol behavior for PostgreSQL versions
   * earlier than PG15: there the streaming protocol sent individial COPY responses,
   * each for a specific tablespace.
   */
  class TablespaceStreamer : public TablespaceIterator,
                             public TablespaceQueue,
                             public StopSignalChecker {
  private:

    /**
     * State of backup stream
     *
     * Initial state is set by the constructor
     */
    BaseBackupState current_state = BASEBACKUP_STARTED;

    /** Internal PostgreSQL connection handle */
    PGconn *conn = nullptr;

    /** Internal stream backup target */
    std::shared_ptr<StreamBaseBackup> backupHandle = nullptr;

  protected:
  public:

    /** constructor */
    explicit TablespaceStreamer(std::shared_ptr<StreamBaseBackup> backupHandle,
                                PGconn *conn);

    /**
     * Save manifest from the protocol stream.
     */
    void manifest();

    /**
     * Performs backup/stream of the specific tablespace descriptor.
     */
    void tablespace(std::shared_ptr<BackupElemDescr> &descr);

    /**
     * @return Returns the current state of the base backup stream. See #BaseBackupState
     * for valid return codes.
     */
    BaseBackupState getState();

    /**
     * Returns the next tablespace descriptor to be streamed.
     */
    bool next(std::shared_ptr<BackupElemDescr> &next) override;

  };

  /**
   * A message streamer implements a basebackup streaming handler suitable for
   * the streaming protocol established with PostgreSQL 15 and later.
   * A message based streamer handles the different kind of COPY data messages, where
   * the payload of each data message transports the contents of a basebackup stream.
   */
  class MessageStreamer : public TablespaceIterator,
                          public TablespaceQueue,
                          public StopSignalChecker {
  private:

    /**
     * State of backup stream
     *
     * Initial state is set by the constructor
     */
    BaseBackupState current_state = BASEBACKUP_STARTED;

    /**
     * Pointer to PostgreSQL connection handle
     */
    PGconn *conn = nullptr;

    /**
     * Streaming backup target handle
     */
    std::shared_ptr<StreamBaseBackup> backupHandle = nullptr;

    /**
     * Receives a copy data stream.
     * @param msg BaseBackupMessage message kind
     */
    void data(std::shared_ptr<BaseBackupMessage> &msg);

  protected:

    /**
     * Starts a COPY data stream.
     */
    void startCopyStream();

  public:

    /**
     * Constructor
     * @param conn PostgreSQL connection handle
     * @param backupHandle Streaming backup target
     */
    explicit MessageStreamer(std::shared_ptr<StreamBaseBackup> backupHandle,
                             PGconn *conn);

    /**
     * Streams manifest data into the current archive file handle
     */
    void manifest();

    /**
     * Streams tablespace data into the current archive file handle.
     */
    void tablespace();

    /**
     * Returns the next tablespace descriptor to be streamed.
     */
    bool next(std::shared_ptr<BackupElemDescr> &next);

  };

  /**
   * Archive stream handler implementation.
   *
   * This class is responsible to handle messages sent by the
   * BASE_BACKUP command and implements various method to handle them.
   *
   * The protocol state is *not* managed within BaseBackupStream but by the
   * BaseBackupProcess implementation.
   *
   * The BASE_BACKUP commands sents after the first main ordinary result
   * set another one with tablespace information in the following layout:
   *
   * spcoid
   * spclocation
   * size
   *
   * The BaseBackupTablespaceInfo handler abstracts extracting those values.
   */
  class BaseBackupStream : public StopSignalChecker {
  private:
  protected:

    /** Internal PostgreSQL connection handle */
    PGconn *pgconn = nullptr;

    /**
     * Internal pointer to streaming backup target.
     */
    std::shared_ptr<StreamBaseBackup> backupHandle = nullptr;

    /**
     * Internal pointer to backup profile descriptor.
     */
     std::shared_ptr<BackupProfileDescr> profile = nullptr;

  public:
    explicit BaseBackupStream(PGconn *prepared_con,
                              std::shared_ptr<StreamBaseBackup> backupHandle,
                              std::shared_ptr<BackupProfileDescr> profileDescr);
    virtual ~BaseBackupStream() = default;

    virtual void getStartPosition(std::shared_ptr<BaseBackupDescr> &descr,
                                  BaseBackupState &current_state);
    virtual BaseBackupState getTablespaceInfo(BaseBackupState &state) = 0;
    virtual std::shared_ptr<BackupElemDescr> handleMessage(BaseBackupState &current_state) = 0;

    /**
     *
     * @param prepared_conn Prepared PostgreSQL database connection.
     * @return An initialized instance of BaseBackupStream.
     */
    static std::shared_ptr<BaseBackupStream>
    makeStreamInstance(PGconn *prepared_conn,
                       std::shared_ptr<StreamBaseBackup> backupHandle,
                       std::shared_ptr<BackupProfileDescr> profileDescr);

    virtual std::string query(std::shared_ptr<BackupProfileDescr> profile,
                             PGconn *prepared_conn,
                             BaseBackupQueryType type) = 0;

  };

  /**
   * Protocol implementation for BASE_BACKUP command, suitable for
   * PostgreSQL versions up to 12
   */
  class BaseBackupStream12 : public BaseBackupStream,
                                    TablespaceStreamer {
  public:
    explicit BaseBackupStream12(PGconn *prepared_conn,
                                std::shared_ptr<StreamBaseBackup> backupHandle,
                                std::shared_ptr<BackupProfileDescr> profileDescr);
    ~BaseBackupStream12() override;

    BaseBackupState getTablespaceInfo(BaseBackupState &state) override;
    std::shared_ptr<BackupElemDescr> handleMessage(BaseBackupState &current_state) override;

    std::string query(std::shared_ptr<BackupProfileDescr> profile,
                      PGconn *prepared_conn,
                      BaseBackupQueryType type) override;

  };

  /**
   * Protocol implementation for BASE_BACKUP command, suitable for
   * PostgreSQL versions from 13 up to 14.
   */
  class BaseBackupStream14 : public BaseBackupStream,
                                    TablespaceStreamer {
  public:

    explicit BaseBackupStream14(PGconn *prepared_conn,
                                std::shared_ptr<StreamBaseBackup> backupHandle,
                                std::shared_ptr<BackupProfileDescr> profileDescr);
    ~BaseBackupStream14() override;

    BaseBackupState getTablespaceInfo(BaseBackupState &state) override;
    std::shared_ptr<BackupElemDescr> handleMessage(BaseBackupState &current_state) override;

    std::string query(std::shared_ptr<BackupProfileDescr> profile,
                      PGconn *prepared_conn,
                      BaseBackupQueryType type) override;

  };

  /**
   * Protocol implementation for BASE_BACKUP command, suitable for
   * PostgreSQL versions 15 and above.
   */
  class BaseBackupStream15 : public BaseBackupStream,
                                    MessageStreamer {
  public:

    explicit BaseBackupStream15(PGconn *prepared_conn,
                                std::shared_ptr<StreamBaseBackup> backupHandle,
                                std::shared_ptr<BackupProfileDescr> profileDescr);
    ~BaseBackupStream15() override;

    BaseBackupState getTablespaceInfo(BaseBackupState &state) override;
    std::shared_ptr<BackupElemDescr> handleMessage(BaseBackupState &current_state) override;

    std::string query(std::shared_ptr<BackupProfileDescr> profile,
                      PGconn *prepared_conn,
                      BaseBackupQueryType type) override;

  };

  /*
   * Implements the base backup streaming
   * infrastructure.
   */
  class BaseBackupProcess : public CPGBackupCtlBase,
                            public StopSignalChecker {
  private:

    /**
     * Internal streaming protocol handler.
     */
    std::shared_ptr<BaseBackupStream> tinfo = nullptr;

  protected:
    BaseBackupState current_state;
    PGconn *pgconn;
    std::shared_ptr<BackupProfileDescr> profile;
    std::shared_ptr<BaseBackupDescr> baseBackupDescr = nullptr;

    int         timeline;
    std::string xlogpos;
    std::string systemid;
    unsigned long long wal_segment_size = 0;

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
     * Calls the final step in the streaming basebackup
     * protocol. Receives the WAL end position in the stream
     * and finalizes the basebackup stream.
     */
    virtual void end();

    /**
     * Returns current status of a basebackup handle.
     */
    virtual BaseBackupState getState();

    /**
     * @ref prepareDataStream
     * Prepare basebackup data streaming
     *
     * This prepares the current protocol state to retrieve archive
     * and, iff requested, manifest data from the stream.
     */
    virtual void prepareStream(std::shared_ptr<StreamBaseBackup> &backupHandle);

    /**
     * Step through the interal tablespace meta info
     * (initialized by calling readTablespaceInfo()), and
     * backup 'em into files into the catalog. The method
     * requires an correctly initialized archive handle
     * to write the files properly.
     */
    virtual bool stream(std::shared_ptr<BackupCatalog> catalog);

    /**
     * Receives a backup manifest, if requested by the backup profile.
     */
    void receiveManifest(std::shared_ptr<StreamBaseBackup> backupHandle);

    /**
     * Assigns a stop signal handler.
     */
    virtual void assignStopHandler(JobSignalHandler *stopHandler);
  };

}

#endif
