#ifndef __STREAM_HXX__
#define __STREAM_HXX__

/* PostgreSQL API includes */
#include <postgres_fe.h>
#include <libpq-fe.h>
#include <access/xlog_internal.h>

#include <common.hxx>
#include <BackupCatalog.hxx>

namespace credativ {

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

    class StreamingConnectionFailure : StreamingFailure {
    public:
      StreamingConnectionFailure(std::string errstring, ConnStatusType connStatus)
        : StreamingFailure(errstring, connStatus) {};
    };

    class StreamingExecutionFailure : StreamingFailure {
    public:
      StreamingExecutionFailure(std::string errstring, ExecStatusType execStatus)
        : StreamingFailure(errstring, execStatus) {};
    };

    class StreamingTransactionFailure : StreamingFailure {
    public:
      StreamingTransactionFailure(std::string errstring, PGTransactionStatusType transStatus)
        : StreamingFailure(errstring, transStatus) {};
    };

    struct StreamIdentification {
      std::string systemid;
      int         timeline;
      std::string xlogpos;
      std::string dbname;

      /*
       * Returns the decoded XLogRecPtr from xlogpos
       */
      XLogRecPtr getXLOGStartPos()
        throw(StreamingFailure);
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
       * If identified, holds information
       * from IDENTIFY SYSTEM
       */
      StreamIdentification streamident;
    public:
      PGStream(const std::shared_ptr<CatalogDescr>& descr);
      ~PGStream();

      /*
       * Helper function to decode a XLOG position string.
       */
      static XLogRecPtr decodeXLOGPos(std::string pos)
        throw(StreamingFailure);

      /*
       * Get version from connected server
       */
      int getServerVersion()
        throw(StreamingConnectionFailure);

      /*
       * Establish PostgreSQL streaming connection.
       */
      virtual void connect()
        throw(StreamingConnectionFailure);

      /*
       * Returns TRUE if PostgreSQL connection handle
       * is valid.
       */
      virtual bool connected();

      /*
       * Override internal PostgreSQL connection handle.
       */
      virtual void setPGConnection(PGconn *conn);

      /*
       * Identify the system. This is required before initiating the stream.
       */
      virtual void identify()
        throw(StreamingExecutionFailure);
    };

  }

}

#endif
