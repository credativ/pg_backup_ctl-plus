#ifndef __STREAM_HXX__
#define __STREAM_HXX__

#include <common.hxx>
#include <descr.hxx>

/* PostgreSQL client API */
#include <libpq-fe.h>

#define MAXXLOGFNAMELEN MAXFNAMELEN

using namespace credativ;

namespace credativ {

  /* forward declaration */
  class BackupDirectory;
  
  namespace streaming {

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

    /*
     * Represents a physical replication slot.
     * State of base backup stream.
     */
    struct PhysicalReplicationSlot {
      std::string slot_name;
      std::string consistent_point;
      std::string snapshot_name;
      std::string output_plugin;
    };

    /*
     * Represents an identified streaming connection.
     */
    class StreamIdentification {
    public:
      unsigned long long id = -1; /* internal catalog stream id */
      int archive_id = -1; /* used to reflect assigned archive */
      std::string stype;
      std::string slot_name;
      std::string systemid;
      int         timeline;
      std::string xlogpos;
      std::string dbname;
      std::string status;
      std::string create_date;

      StreamIdentification();
      ~StreamIdentification();

      /*
       * Physical replication slot, if any
       */
      std::shared_ptr<PhysicalReplicationSlot> slot = nullptr;

      /*
       * Set properties back to default.
       */
      void reset();

      /*
       * Returns the decoded XLogRecPtr from xlogpos
       */
      XLogRecPtr getXLOGStartPos();
    };
    
    /*
     * State of base backup stream.
     */
    typedef enum {

      BASEBACKUP_STARTED,
      BASEBACKUP_START_POSITION,
      BASEBACKUP_TABLESPACE_META,
      BASEBACKUP_STEP_TABLESPACE,
      BASEBACKUP_EOF,
      BASEBACKUP_INIT

    } BaseBackupState;

    class BaseBackupProcess : public CPGBackupCtlBase {

    protected:

      BaseBackupState current_state;
      PGconn *pgconn;
      StreamIdentification ident;

      int         timeline;
      std::string xlogpos;

      /*
       * Backup the requested tablespace OID. Throws a
       * StreamingExecution exception in case the OID
       * is not in the internal tablespace meta info.
       */
      void backupTablespace(int OID);

    public:

      BaseBackupProcess(StreamIdentification ident,
                        PGconn *prepared_connection);
      ~BaseBackupProcess();

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
       * machine is not properly synched with the streaming
       * protocol state, this will throw a StreamingExecution
       * exception.
       */
      virtual void readTablespaceInfo();

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
       */
      virtual bool stepTablespace(std::shared_ptr<credativ::BackupDirectory> archivedir);
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

      /*
       * Starts streaming a basebackup. Stream should be
       * already connected and identified.
       * basebackup() 
       */
      virtual std::shared_ptr<BaseBackupProcess> basebackup();
    };

  }

}

#endif
