#ifndef __STREAM_HXX__
#define __STREAM_HXX__

#include <queue>

#include <common.hxx>
#include <descr.hxx>
#include <backupprocesses.hxx>

/* PostgreSQL client API */
#include <libpq-fe.h>

#define MAXXLOGFNAMELEN MAXFNAMELEN

using namespace credativ;

namespace credativ {

  /* forwarded declarations */
  class FeedbackMessage;
  class HotstandbyFeedbackMessage;
  class ReceiverStatusUpdateMessage;
  class PGStream;

  class StreamingFailure : public CPGBackupCtlFailure {
  private:
    ConnStatusType connStatus;
    ExecStatusType execStatus;
    PGTransactionStatusType transStatus;
  public:
    StreamingFailure(const char *errstr) : CPGBackupCtlFailure(errstr) {};
    StreamingFailure(std::string errstr) : CPGBackupCtlFailure(errstr) {};
    StreamingFailure(std::string errstr, ConnStatusType connStatus);
    StreamingFailure(std::string errstr, ExecStatusType execStatus);
    StreamingFailure(std::string errstr, PGTransactionStatusType transStatus);

    virtual ConnStatusType getConnStatus();
    virtual ExecStatusType getExecStatus();
    virtual PGTransactionStatusType getTransStatus();
  };

  class StreamingConnectionFailure : public StreamingFailure {
  public:
    StreamingConnectionFailure(std::string errstring, ConnStatusType connStatus)
      : StreamingFailure(errstring, connStatus) {};
  };

  class StreamingExecutionFailure : public StreamingFailure {
  private:
    std::string SQLSTATE = "00000";
  public:
    StreamingExecutionFailure(std::string errstring, ExecStatusType execStatus)
      : StreamingFailure(errstring, execStatus) {};
    StreamingExecutionFailure(std::string errstring,
                              ExecStatusType execStatus,
                              std::string& SQLSTATE);

    virtual std::string getSQLSTATE();
  };

  class StreamingTransactionFailure : public StreamingFailure {
  public:
    StreamingTransactionFailure(std::string errstring, PGTransactionStatusType transStatus)
      : StreamingFailure(errstring, transStatus) {};
  };

  class PGStream : CPGBackupCtlBase {
  private:
    /*
     * Reference to catalog handle
     */
    std::shared_ptr<CatalogDescr> descr;

    /*
     * Reference to libpq connection handle.
     */
    PGconn *pgconn;

    /*
     * Property that holds the XLOG segment size.
     *
     * Only valid when calling getStreamingProperties before.
     */
    uint32 walSegmentSize = 0;

    /*
     * Set to TRUE, if successfully identified.
     */
    bool identified = false;

    /*
     * Returns the XLOG segment size
     * used by the current streaming connection.
     *
     * Requires a valid streaming connection.
     */
    virtual uint32 walSegmentSizeInternal();
  public:

    PGStream(const std::shared_ptr<CatalogDescr>& descr);
    ~PGStream();

    /**
     * If identified, holds information
     * from IDENTIFY SYSTEM
     */
    StreamIdentification streamident;

    /**
     * Returns the WAL segment size for
     * an established streaming connection.
     */
    uint32 getWalSegmentSize();

    /**
     * Generate a slot name for an identified stream and
     * assigns the generated string to the internal streamident
     * handle. The slot name is formatted by an UUID string
     * with the specified prefix.
     *
     * Throws a StreamingExecutionFailure in case identification
     * not available.
     *
     * The generated identifier is assigned to the internal streamident
     * handle and its slot handle.
     *
     * This is just a helper method to assign a valid identifier.
     * Someone might use the streamident object directly.
     */
    std::string generateSlotNameUUID(std::string prefix);

    /**
     * Returns the server parameter value. Throws
     * a StreamingExecutionFailure exception in case
     * the SHOW query fails. A StreamingFailure is thrown,
     * if the parameter value doesn't return a value (which
     * shouldn't happen).
     */
    virtual std::string getServerSetting(std::string name);

    /**
     * Helper function to decode a XLOG position string.
     */
    static XLogRecPtr decodeXLOGPos(std::string pos) ;

    /**
     * Helper function to return XLOG position offset into
     * WAL stream.
     */
    virtual int XLOGOffset(XLogRecPtr pos);

    /**
     * Returns the starting XLogRecPtr position of a XLOG segment
     * relative to the specified XLOG position.
     */
    virtual XLogRecPtr XLOGSegmentStartPosition(XLogRecPtr pos);
    static  XLogRecPtr XLOGSegmentStartPosition(XLogRecPtr pos,
                                                uint32 wal_segment_size);

    /**
     * PostgreSQL version number this library
     * was linked against.
     */
    static int compiledPGVersionNum();

    /**
     * Helper function to return XLOG position offset
     * into WAL stream, static version.
     */
    static int XLOGOffset(XLogRecPtr pos,
                          uint32 wal_segment_size);

    /**
     * Helper function to encode a given XLogRecPtr value.
     *
     * Returns the string representation of the given position.
     */
    static std::string encodeXLOGPos(XLogRecPtr pos);

    /*
     * Get version from connected server
     */
    int getServerVersion();

    /*
     * Establish PostgreSQL streaming connection.
     */
    virtual void connect();

    /*
     * Disconnect from PostgreSQL instance.
     *
     * Also resets all internal state of PGStream.
     */
    virtual void disconnect();

    /*
     * Returns TRUE if PostgreSQL connection handle
     * is valid.
     */
    virtual bool connected();

    /*
     * The same as connected(), but also sets the
     * specified connection status argument to the
     * current ConnStatusType.
     */
    virtual bool connected(ConnStatusType& cs);

    /*
     * Set to true, if the system is already
     * identified
     */
    virtual bool isIdentified();

    /**
     * Request a timeline history file content. The content
     * is returned in buf, which is dynamically allocated by
     * PGStream.
     *
     * Requires a valid connected and identified streaming
     * replication connection, throws a StreamingFailure
     * exception otherwise.
     *
     * If a specific timeline is requested, use the overloaded version
     * of timelineHistoryFileContent(), which allows to specify a
     * specific timeline ID and doesn't insist on an identified
     * stream.
     *
     * If the timeline history file contents cannot be read from
     * the streaming connection, a StreamingExecutionFailure
     * exception will be generated.
     */
    virtual void timelineHistoryFileContent(MemoryBuffer &buffer,
                                            std::string &filename);
    virtual void timelineHistoryFileContent(MemoryBuffer &buffer,
                                            std::string &filename,
                                            int timeline);

    /*
     * Override internal PostgreSQL connection handle.
     */
    virtual void setPGConnection(PGconn *conn);

    /*
     * Identify the system. This is required before initiating the stream.
     */
    virtual void identify();

    /*
     * Create a replication slot for the identified stream.
     *
     * Throws a StreamingExecutionError if the creation fails or
     * the stream is not yet identified.
     *
     * XXX: This might also be called in case the specified
     *      replication slot already exists. The SQLSTATE
     *      of the StreamingExecutionError can be examined to get
     *      the error condition. Also, the status of the slot handle
     *      is set to REPLICATION_SLOT_EXISTS.
     *
     * The identifier of the replication slot must be specified before
     * calling createPhysicalReplicationSlot(). See generateSlotNameUUID for
     * a method to use. Otherwise you should pass a valid identifier
     * via slot->slot_name.
     */
    virtual void
    createPhysicalReplicationSlot(std::shared_ptr<PhysicalReplicationSlot> slot);

    /**
     * Starts streaming a basebackup. Stream should be
     * already connected and identified.
     * basebackup()
     */
    virtual std::shared_ptr<BaseBackupProcess> basebackup();
    virtual std::shared_ptr<BaseBackupProcess> basebackup(std::shared_ptr<BackupProfileDescr> profile);

    /**
     * Returns a WAL streaming handle. Stream should be already
     * connected and identified.
     */
    virtual std::shared_ptr<WALStreamerProcess> walstreamer();

    /**
     * Sets the internal PostgreSQL connection to non-blocking.
     */
    virtual void setNonBlocking();

    /**
     * Sets the internal PostgreSQL connection to blocking.
     */
    virtual void setBlocking();
  };

}

#endif
