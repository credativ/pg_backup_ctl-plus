#ifndef __HAVE_XLOGDEFS__

#include <common.hxx>

/* PostgreSQL client API */
#include <libpq-fe.h>

namespace credativ {

  /*
   * Forwarded class declarations.
   */
  class XLOGStreamMessage;
  class XLOGDataStreamMessage;
  class FeedbackMessage;
  class PrimaryFeedbackMessage;
  class ReceiverStatusUpdateMessage;
  class HotStandbyFeedBackMessage;

  /**
   * XLOG stream message exception.
   */
  class XLOGMessageFailure : public CPGBackupCtlFailure {
  public:
    XLOGMessageFailure(const char *errstr) throw() : CPGBackupCtlFailure(errstr) {};
    XLOGMessageFailure(std::string errstr) throw() : CPGBackupCtlFailure(errstr) {};
  };

  /*
   * Message handles for XLOG streaming.
   */
  class XLOGStreamMessage {
  protected:
    /*
     * Database connection handle, must be prepared
     * by PGstream
     */
    PGconn *connection = NULL;

    /*
     * Message type, implemented by derived message types.
     */
    unsigned char kind = 0;

    /*
     * Certain derived message classes wants
     * a response.
     */
    bool requestResponse;

    /**
     * Performs basic checks on the assigned byte buffer.
     */
    void basicCheckMemoryBuffer(MemoryBuffer &mybuffer);
  public:
    XLOGStreamMessage(PGconn *prepared_connection);
    ~XLOGStreamMessage();

    virtual void assign(MemoryBuffer &mybuffer) {};

    /**
     * Toggle request for server/client feedback message. Calling this before
     * send() requests the streaming endpoint to respond to this
     * message immediately. This will be resettet after each send()
     * call.
     */
    virtual void wantsResponse();

    /**
     * Returns the message type identifier byte.
     */
    virtual unsigned char what();

    /**
     * Operator to assign streamed byte buffer
     */
    virtual XLOGStreamMessage& operator<<(MemoryBuffer &srcbuffer) {};

    /**
     * A factory method, returning any message instance identified
     * by the specified XLOG data buffer.
     */
    static XLOGStreamMessage* message(PGconn *pg_connection,
                                      MemoryBuffer &srcbuffer);
  };

  /**
   * A WAL data stream message, contains binary WAL information.
   */
  class XLOGDataStreamMessage : public XLOGStreamMessage {
  protected:
    long long xlogstartpos = 0;
    long long xlogserverpos = 0;
    long long xlogstreamtime = 0;
    MemoryBuffer xlogdata;
  public:
    XLOGDataStreamMessage(PGconn *prepared_connection);
    ~XLOGDataStreamMessage();

    /**
     * Assign a byte buffer to this message, interpreting
     * the bytes and assigning to the current state of the message.
     *
     * If the incoming byte buffer doesn't hold a XLOGData message,
     * a XLOGMessageFailure exception is thrown.
     */
    virtual void assign(MemoryBuffer &mybuffer);

    /**
     * Overloaded operator to assign a memory buffer.
     */
    virtual XLOGStreamMessage& operator<<(MemoryBuffer &srcbuffer);
  };

  class FeedbackMessage : public XLOGStreamMessage {
  protected:

  public:
    FeedbackMessage(PGconn *prepared_connection);
    ~FeedbackMessage();

    virtual void send() {};
  };

  /*
   * A primary keep alive message.
   *
   * Note that this derives directly from XLOGStreamMessage, since
   * it's just a status update to be read (thus, no send() action
   * required).
   */
  class PrimaryFeedbackMessage : public XLOGStreamMessage {
  protected:
    long long xlogserverendpos = 0;
    long long xlogservertime = 0;
    unsigned char responseFlag;
  public:
    PrimaryFeedbackMessage(PGconn *prepared_connection);
    ~PrimaryFeedbackMessage();

    /**
     * Reads the buffer and assigns all properties
     * to this feedback message.
     */
    virtual void assign(MemoryBuffer &mybuffer);
  };

  class ReceiverStatusUpdateMessage : protected FeedbackMessage {
  public:
    ReceiverStatusUpdateMessage(PGconn *prepared_connection);
    ~ReceiverStatusUpdateMessage();

    /**
     * Sends a receiver status update to the inherited
     * database connection.
     */
    virtual void send();
  };

  class HotStandbyFeedbackMessage : public FeedbackMessage {
  public:
    HotStandbyFeedbackMessage(PGconn *prepared_connection);
    ~HotStandbyFeedbackMessage();
  };

}
#endif
