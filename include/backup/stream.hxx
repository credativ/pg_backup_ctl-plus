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
     * Set to TRUE, if successfully identified.
     */
    bool identified = false;

    /*
     * Generate a slot name for an identified stream.
     * Throws a StreamingExecutionFailure in case identification
     * not available.
     */
    std::string generateSlotName();
  public:
    PGStream(const std::shared_ptr<CatalogDescr>& descr);
    ~PGStream();

    /*
     * If identified, holds information
     * from IDENTIFY SYSTEM
     */
    StreamIdentification streamident;

    /**
     * Returns the server parameter value. Throws
     * a StreamingExecutionFailure exception in case
     * the SHOW query fails. A StreamingFailure is thrown,
     * if the parameter value doesn't return a value (which
     * shouldn't happen).
     */
    virtual std::string getServerSetting(std::string name);

    /*
     * Helper function to decode a XLOG position string.
     */
    static XLogRecPtr decodeXLOGPos(std::string pos) ;

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
     *      the error condition.
     *
     * The identifier of the replication slot is auto-generated.
     */
    virtual std::shared_ptr<PhysicalReplicationSlot>
    createPhysicalReplicationSlot(bool reserve_wal,
                                  bool existing_ok,
                                  bool noident_ok);

    /**
     * Starts streaming a basebackup. Stream should be
     * already connected and identified.
     * basebackup()
     */
    virtual std::shared_ptr<BaseBackupProcess> basebackup();
    virtual std::shared_ptr<BaseBackupProcess> basebackup(std::shared_ptr<BackupProfileDescr> profile);

    /**
     * Returns information about the current timeline of the
     * PostgreSQL server.
     */
  };

}

#endif
