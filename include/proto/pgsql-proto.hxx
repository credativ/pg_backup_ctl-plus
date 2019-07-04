#ifndef __HAVE_PGSQL_PROTO_HXX__
#define __HAVE_PGSQL_PROTO_HXX__

#include <stack>
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
                PGPROTO_SEND_BACKEND_KEY

  } PostgreSQLProtocolState;

  namespace pgprotocol {

    typedef char PGMessageType;
    typedef int PGAuthenticationType;

    /*
     * PostgresSQL protocol message types
     */
    const PGMessageType ParameterStatusMessage = 'S';
    const PGMessageType AuthenticationMessage  = 'R';
    const PGMessageType ReadyForQueryMessage   = 'Z';
    const PGMessageType ErrorMessage           = 'E';
    const PGMessageType EmptyQueryMessage      = 'I';
    const PGMessageType DescribeMessage        = 'D';
    const PGMessageType RowDescriptionMessage  = 'T';
    const PGMessageType QueryMessage           = 'Q';
    const PGMessageType CommandCompleteMessage = 'C';
    const PGMessageType CancelMessage          = 'X';
    const PGMessageType NoticeMessage          = 'N';
    const PGMessageType PasswordMessage        = 'p';
    const PGMessageType BackendKeyMessage      = 'K';

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
       */
      virtual void toBuffer(ProtocolBuffer &dest,
                            size_t &msg_size);

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
#define MESSAGE_DATA_LENGTH(msg) (((msg).hdr).length - MESSAGE_HDR_LENGTH_SIZE)
#define MESSAGE_LENGTH_OFFSET ((off_t) sizeof(credativ::pgprotocol::PGMessageType))
#define MESSAGE_DATA_OFFSET (MESSAGE_LENGTH_OFFSET + MESSAGE_HDR_LENGTH_SIZE)

#endif
