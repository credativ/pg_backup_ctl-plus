#ifndef __HAVE_XLOGDEFS__

#include <common.hxx>

/* PostgreSQL client API */
#include <libpq-fe.h>

namespace credativ {

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
  public:
    XLOGStreamMessage(PGconn *prepared_connection);
    ~XLOGStreamMessage();

    virtual void assign(MemoryBuffer &mybuffer) = 0;

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
  };

  /**
   * A WAL data stream message, contains binary WAL information.
   */
  class XLOGDataStreamMessage : protected XLOGStreamMessage {
  protected:
  public:
    XLOGDataStreamMessage(PGconn *prepared_connection);
    ~XLOGDataStreamMessage();
  };

  class FeedbackMessage : protected XLOGStreamMessage {
  protected:

  public:
    FeedbackMessage(PGconn *prepared_connection);
    ~FeedbackMessage();

    virtual void send() = 0;
  };

  /*
   * A primary keep alive message.
   *
   * Note that this derives directly from XLOGStreamMessage, since
   * it's just a status update to be read (thus, no send() action
   * required).
   */
  class PrimaryFeedbackMessage : protected XLOGStreamMessage {
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

  class HotStandbyFeedbackMessage : protected FeedbackMessage {
  public:
    HotStandbyFeedbackMessage(PGconn *prepared_connection);
    ~HotStandbyFeedbackMessage();
  };

}
#endif
