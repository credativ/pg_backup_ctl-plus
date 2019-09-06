#ifndef __HAVE_PGSQL_PROTO_HXX__
#define __HAVE_PGSQL_PROTO_HXX__

#include <stack>
#include <vector>
#include <proto-buffer.hxx>

/*
 * PostgreSQL protocol v3 definitions, taken from
 *
 * src/include/libpq/pqcomm.h
 */
#define PG_PROTOCOL_MAJOR(v)    ((v) >> 16)
#define PG_PROTOCOL_MINOR(v)    ((v) & 0x0000ffff)
#define PG_PROTOCOL(m,n)        (((m) << 16) | (n))

namespace credativ {

  typedef enum {

                PGPROTO_STARTUP,
                PGPROTO_STARTUP_SSL_NO,
                PGPROTO_STARTUP_SSL_OK,
                PGPROTO_AUTH,
                PGPROTO_READ_STARTUP_GUC,
                PGPROTO_READY_FOR_QUERY,
                PGPROTO_READY_FOR_QUERY_WAIT,
                PGPROTO_SEND_BACKEND_KEY,
                PGPROTO_ERROR_CONDITION,
                PGPROTO_NOTICE_CONDITION,
                PGPROTO_ERROR_AFTER_QUERY, /* usually set back to PGPROTO_READY_FOR_QUERY_WAIT */
                PGPROTO_COMMAND_COMPLETE,
                PGPROTO_PROCESS_QUERY_START,
                PGPROTO_PROCESS_QUERY_RESULT,
                PGPROTO_PROCESS_QUERY_EXECUTE,
                PGPROTO_PROCESS_QUERY_IN_PROGRESS

  } PostgreSQLProtocolState;

  namespace pgprotocol {

    typedef char PGMessageType;
    typedef int PGAuthenticationType;

    /* ************************************************************************
     * PostgresSQL protocol message types
     * ***********************************************************************/

    /* FE messages */
    const PGMessageType ExecuteMessage         = 'E';
    const PGMessageType QueryMessage           = 'Q';
    const PGMessageType FlushMessage           = 'H';
    const PGMessageType FunctionCallMessage    = 'F';
    const PGMessageType CancelMessage          = 'X';
    const PGMessageType GSSResponseMessage     = 'p';
    const PGMessageType ParseMessageType       = 'P';
    const PGMessageType SASLInitialResponseMessage = 'p';
    const PGMessageType SASLResponse               = 'p';
    const PGMessageType SASLRequest                = '\0'; /* does not have a special type set */
    const PGMessageType SSLRequest                 = '\0'; /* same here */
    const PGMessageType StartupMessage             = '\0'; /* same here */
    const PGMessageType SyncMessage                = 'S';
    const PGMessageType TerminationMessage         = 'X';
    const PGMessageType CopyFailMessage            = 'f';

    /* BE messages */
    const PGMessageType ErrorMessage           = 'E';
    const PGMessageType PasswordMessage        = 'p';
    const PGMessageType AuthenticationMessage  = 'R';
    const PGMessageType AuthKerberosV5Message   = 'R';
    const PGMessageType AuthKerberosClearTextMessage = 'R';
    const PGMessageType AuthMD5PasswordMessage       = 'R';
    const PGMessageType AuthSCMCredentialMessage     = 'R';
    const PGMessageType AuthGSSMessage               = 'R';
    const PGMessageType AuthSSPIMessage              = 'R';
    const PGMessageType AuthGSSContinueMessage       = 'R';
    const PGMessageType AuthSASLMessage              = 'R';
    const PGMessageType AuthSASLContinueMessage      = 'R';
    const PGMessageType AuthSASLFinal                = 'R';
    const PGMessageType BackendKeyMessage            = 'K';
    const PGMessageType BindCompleteMessage          = '2';
    const PGMessageType CloseCompleteMessage         = '3';
    const PGMessageType CommandCompleteMessage       = 'C';
    const PGMessageType DescribeMessage              = 'D';
    const PGMessageType CopyOutResponseMessage       = 'H';
    const PGMessageType CopyBothResponseMessage      = 'W';
    const PGMessageType EmptyQueryMessage            = 'I';
    const PGMessageType FunctionCallResponseMessage  = 'V';
    const PGMessageType NegotiateProtocolMessage     = 'v';
    const PGMessageType NoDataMessage                = 'n';
    const PGMessageType NoticeMessage                = 'N';
    const PGMessageType NotificationMessage          = 'A';
    const PGMessageType ParameterStatusMessage       = 'S';
    const PGMessageType ParameterDescriptionMessage  = 't';
    const PGMessageType ParseCompleteMessage         = '1';
    const PGMessageType PortalSuspendedMessage       = 's';
    const PGMessageType ReadyForQueryMessage         = 'Z';
    const PGMessageType RowDescriptionMessage  = 'T';

    /* FE & BE messages */
    const PGMessageType CopyDataMessage              = 'd';
    const PGMessageType CopyDoneMessage              = 'c';
    const PGMessageType CopyInResponseMessage        = 'G';

    /* ********************************************************************** */

    /*
     * SSL message types, used during startup
     * and SSL negotiation.
     */
    const PGMessageType NoSSLMessage          = 'N';
    const PGMessageType SSLOkMessage          = 'S';

    /*
     * PostgreSQL authentication types
     */
    const PGAuthenticationType AuthRequest_OK        = 0;
    const PGAuthenticationType AuthRequest_KRB4      = 1;
    const PGAuthenticationType AuthRequest_KRB5      = 2;
    const PGAuthenticationType AuthRequest_PASSWD    = 3;
    const PGAuthenticationType AuthRequest_CRYPT     = 4;
    const PGAuthenticationType AuthRequest_MD5       = 5;
    const PGAuthenticationType AuthRequest_SCM_CREDS = 6;
    const PGAuthenticationType AuthRequest_GSS       = 7;
    const PGAuthenticationType AuthRequest_GSS_CONT  = 8;
    const PGAuthenticationType AuthRequest_SSPI      = 9;
    const PGAuthenticationType AuthRequest_SASL      = 10;
    const PGAuthenticationType AuthRequest_SASL_CONT = 11;
    const PGAuthenticationType AuthRequest_SASL_FIN  = 12;

    /* Fixed PostgreSQL command tags */
    typedef enum {
                  INSERT_CMD,
                  UPDATE_CMD,
                  DELETE_CMD,
                  SELECT_CMD,
                  MOVE_CMD,
                  FETCH_CMD,
                  COPY_CMD,
                  UNKNOWN_CMD
    } PGProtoCmdTag;

    struct pg_protocol_msg_header {

      PGMessageType type;
      unsigned int length;

    };

    struct pg_protocol_backendkey {
      struct pg_protocol_msg_header hdr = { BackendKeyMessage, 12 };
      unsigned int pid;
      unsigned int key;
    };

    struct pg_protocol_auth {
      struct pg_protocol_msg_header hdr = { AuthenticationMessage, 8 };
      int auth_type = 0;
    };

    struct pg_protocol_ready_for_query {
      struct pg_protocol_msg_header hdr;
      char tx_state;
    };

    struct pg_protocol_startup {

      unsigned int length;
      unsigned int protocolVersion;
      char *data_ptr;

    };

    struct pg_protocol_param_status {

      struct pg_protocol_msg_header hdr;
      char *data_ptr;

    };

    /*
     * Error response types.
     */
    typedef char PGErrorResponseType;

    const PGErrorResponseType PGProtoSqlState          = 'C';
    const PGErrorResponseType PGProtoSeverity          = 'S';
    const PGErrorResponseType PGProtoSeverityNonLocale = 'V';
    const PGErrorResponseType PGProtoMessage           = 'M';

    /***
     * Streaming protocol command tags.
     */
    typedef enum {

                  INVALID_COMMAND,
                  IDENTIFY_SYSTEM,
                  LIST_BASEBACKUPS

    } ProtocolCommandTag;

    /*************************************************************************
     * Handles and definitions to process
     * queries in the recovery instance.
     *************************************************************************/

    /**
     * Command descriptor populated by the streaming protocol parser.
     */
    class PGProtoCmdDescr {
    public:

      ProtocolCommandTag tag = INVALID_COMMAND;

      std::vector<std::string> cmd_arguments;
      std::string query;

      void setCommandTag(ProtocolCommandTag const& tag);

    };

    /**
     * Protocol Buffer Aggregator interface class.
     *
     * This should be implemented by a PGProtoStreamingCommand instances
     * if they create protocol level response messages.
     */
    class PGProtoBufferAggregator {
    protected:

      /**
       * The current_step identifies the current status of
       * protocol messages stacked via the buffer aggregator.
       *
       * Steps should always start by 1, never by 0 and never with
       * negative values. 0 always defines the buffer aggregator for never
       * being called before, whereas a negative value indicates
       * the end of the message flow.
       */
      int current_step = 0; /* not yet called */

    public:

      PGProtoBufferAggregator() {}
      virtual ~PGProtoBufferAggregator() {};

      virtual int step(ProtocolBuffer &buffer) = 0;
      virtual void reset() = 0;

    };

    /**
     * PGProtoRowDataDescr
     */
    class PGProtoColumnDataDescr {
    public:

      int length;
      std::string data;

    };

    class PGProtoColumns {
    public:

      int row_size = 0;
      std::vector<PGProtoColumnDataDescr> values;

      unsigned int fieldCount() {
        return values.size();
      }

    };

    /**
     * Formatted result set.
     *
     * Holds the column values.
     */
    class PGProtoDataDescr {
    public:

      pg_protocol_msg_header hdr = { DescribeMessage };

      /*
       * The number of field values is usually identical to
       * the column count in PGProtoRowDescr instances.
       */
      std::vector<PGProtoColumns> row_values;

    };

    class PGProtoColumnDescr {
    public:

      static const int PG_TYPELEN_VARLENA = -1;
      static const int PG_TYPEMOD_VARLENA = -1;
      static const int PG_TYPEOID_TEXT    = 25;
      static const int PG_TYPEOID_INT4    = 23;

      std::string name = "";
      int   tableoid   = 0;
      short attnum     = 0;
      int   typeoid    = 0;  /* XXX: text datatype OID? */
      short typelen    = -1; /* varlena */
      int   typemod    = -1; /* varlena */
      short format     = 0;  /* indicates TEXT format */

    };

    /**
     * PGProtoRowDescr
     *
     * Header for query result sets.
     */
    class PGProtoRowDescr {
    public:

      pg_protocol_msg_header hdr = { RowDescriptionMessage };

      /*
       * Number of column descriptors
       *
       * We don't use the vector count here, since
       * on the protocol level the column count is just
       * a short (16bit int) value.
       */
      short count = 0;

      /* List of PGProtoColumnDescr items */
      std::vector<PGProtoColumnDescr> column_list;

      unsigned int fieldCount() {
        return column_list.size();
      }
    };

    /**
     * PGProtoResult
     *
     * Encapsulates result sets sent over
     * the PostgreSQL wire protocol.
     */
    class PGProtoResultSet {
    private:

      int row_descr_size = 0;

      /*
       * PGProtoRowDescr is the "header" of each
       * data response.
       */
      PGProtoRowDescr row_descr;
      PGProtoDataDescr data_descr;

      virtual int calculateRowDescrSize();

      /**
       * Prepares a data or row descriptor message to be sent
       * over the wire.
       *
       * Returns the calculated message size includeing message
       * header length size without the message byte length.
       *
       * If a value < 0 is returned, the return code is as follows:
       *
       * -1: the column counts in row and data descriptor do not match.
       */
      virtual int prepareSend(ProtocolBuffer &buffer,
                              int type);

      /*
       * Internal iterator handle to loop through data rows.
       */
      std::vector<PGProtoColumns>::iterator row_iterator;

    public:

      static const int PGPROTO_ROW_DESCR_MESSAGE = 1;
      static const int PGPROTO_DATA_DESCR_MESSAGE = 2;

      PGProtoResultSet();
      virtual ~PGProtoResultSet();

      /*
       * Clear the result set. If an iteration to retrieve
       * rows into a ProtocolBuffer was in progress, this will
       * also be resettet.
       */
      virtual void clear();

      /**
       * Write the row descriptor message into the specified
       * protocol buffer. This also resets the internal
       * row data iterator to the very first row in the
       * result set.
       */
      virtual int descriptor(ProtocolBuffer &buffer);
      virtual int data(ProtocolBuffer &buffer);

      /**
       * Adds a new column definition to the result set
       * header.
       */
      virtual void addColumn(std::string colname,
                             int tableoid,
                             short attnum,
                             int typeoid,
                             short typelen,
                             int typemod,
                             short format = 0);

      /**
       * Adds a new row with the specified data. Should match a currently
       * present column header.
       */
      virtual void addRow(std::vector<PGProtoColumnDataDescr> column_values);

      /**
       * Current number of rows materialized in a PGProtoResultSet
       * instance.
       */
      virtual unsigned int rowCount();

    };

    /**
     * Encoded text representations
     * for PGProtoSeverity or PGProtoSeverityNonLocale.
     */
    typedef enum {
                  PG_ERR_ERROR,
                  PG_ERR_WARNING,
                  PG_ERR_NOTICE,
                  PG_ERR_LOG,
                  PG_ERR_INFO,
                  PG_ERR_DEBUG,
                  PG_ERR_HINT,
                  PG_ERR_DETAIL,
                  PG_ERR_FATAL,
                  PG_ERR_PANIC
    } PGErrorSeverity;

    /*
     * Error response descriptor.
     */
    typedef struct _pg_error_response {

      char type;
      std::string value;

    } PGErrorResponseField;

    /*
     * Stacked error response descriptor suitable
     * to be send over the wire.
     */
    class ProtocolErrorStack {
    private:

      /**
       * Stack for error response fields.
       */
      std::stack<PGErrorResponseField> es;

      /**
       * Size of the top element in the
       * stack. Saved to ease recalculation after
       * pop().
       */
      size_t top_element_size = 0;

      /**
       * Size in bytes occupied by each
       * instance of PGErrorResponseField in the stack.
       *
       * This is basically the size of each PGErrorResponseField
       * summed up (including sizeof(type) and value.length.
       *
       * This doesn't count trailing null bytes for value, so
       * be sure to add them when allocating a separate
       * memory buffer to hold all values using
       * ProtocolErrorStack::count().
       */
      size_t content_size = 0;

    public:

      /**
       * Transforms the error stack into a memory buffer,
       * suitable to be sent over the wire.
       *
       * Pops the elements from that stack, so after calling
       * toBuffer() the error stack is empty.
       *
       * The returned buffer forms a complete ErrorResponse
       * message on the postgresql protocol level, including
       * message header. This means the buffer contents
       * can be flushed to the protocol after returning immediately.
       *
       * The size returned in the second argument shows the
       * size of the complete ErrorResponse message, _without_
       * the protocol header.
       *
       * The default error argument value 'true' indicates
       * that ErrorResponse message is created, other a
       * NoticeResponse message will be created.
       */
      virtual void toBuffer(ProtocolBuffer &dest,
                            size_t &msg_size,
                            bool error = true);

      ProtocolErrorStack() {};
      virtual ~ProtocolErrorStack() {};

      /**
       * Push error response to the stack.
       */
      virtual void push(PGErrorResponseType type,
                        std::string value);

      /**
       * Push error response field to the stack.
       */
      virtual void push(PGErrorResponseField field);

      /**
       * Returns the last error response field on the stack.
       */
      virtual PGErrorResponseField top();

      /**
       * Pops the latest error response field from the error stack.
       */
      virtual void pop();

      /**
       * Returns the number of PGErrorResponseField
       * currently pushed onto the stack.
       */
      virtual size_t count();

      /**
       * Returns the size of the current top level
       * element in the stack. 0 if empty.
       */
      size_t getTopElementSize();

      /**
       * Returns the number of bytes currently allocated
       * by the PGErrorResponseField elements onto the stack.
       * 0 if empty.
       */
      virtual size_t getTotalElementSize();

      /**
       * Returns true in case the error stack is empty.
       */
      virtual bool empty();

    };

  }

}

#define MESSAGE_HDR_BYTE (sizeof(credativ::pgprotocol::PGMessageType))
#define MESSAGE_HDR_LENGTH_SIZE (sizeof(unsigned int))
#define MESSAGE_HDR_SIZE (MESSAGE_HDR_LENGTH_SIZE + MESSAGE_HDR_BYTE)
#define MESSAGE_HDR_DATA_LENGTH(hdr) ((hdr).length - MESSAGE_HDR_LENGTH_SIZE)
#define MESSAGE_DATA_LENGTH(msg) (((msg).hdr).length - MESSAGE_HDR_LENGTH_SIZE)
#define MESSAGE_LENGTH_OFFSET ((off_t) sizeof(credativ::pgprotocol::PGMessageType))
#define MESSAGE_DATA_OFFSET (MESSAGE_LENGTH_OFFSET + MESSAGE_HDR_LENGTH_SIZE)

/**
 * Currently we don't have any reason to allow arbitrary large
 * query lengths, so restrict the input buffer to up to 4096 bytes.
 */
#define PGPROTO_MAX_QUERY_SIZE 4096

#endif
