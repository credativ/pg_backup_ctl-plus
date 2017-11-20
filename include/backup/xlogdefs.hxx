#ifndef __HAVE_XLOGDEFS__
#define __HAVE_XLOGDEFS__

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
     * Size of WAL segments this message belongs to.
     */
    unsigned long long wal_segment_size = 0;

    /**
     * Performs basic checks on the assigned byte buffer.
     */
    void basicCheckMemoryBuffer(MemoryBuffer &mybuffer);
  public:
    XLOGStreamMessage(PGconn *prepared_connection);
    XLOGStreamMessage(PGconn *prepared_connection,
                      unsigned long long wal_segment_size);
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
     * Tells wether message has a response flag set.
     */
    virtual bool responseRequested();

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
                                      MemoryBuffer &srcbuffer,
                                      unsigned long long wal_segment_size);

    /**
     * Set internal WAL segment size. Needed to calculate
     * the XLOG position.
     */
    virtual void setWALSegmentSize(unsigned long long wal_segment_size);

    /**
     * Returns the current configured WAL segment size within
     * this message object.
     */
    virtual unsigned long long getWALSegmentSize();
  };

  /**
   * A WAL data stream message, contains binary WAL information.
   */
  class XLOGDataStreamMessage : public XLOGStreamMessage {
  protected:
    XLogRecPtr xlogstartpos = 0;
    XLogRecPtr xlogserverpos = 0;
    long long xlogstreamtime = 0;
    MemoryBuffer xlogdata;
  public:
    XLOGDataStreamMessage(PGconn *prepared_connection);
    XLOGDataStreamMessage(PGconn *prepared_connection,
                          unsigned long long wal_segment_size);
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

    /*
     * Getter method for xlogstartpos (current starting XLOG position
     * of the current data message in the stream.
     */
    virtual XLogRecPtr getXLOGStartPos();

    /**
     * Returns the source server XLOG position (reported by the
     * connected WAL sender).
     */
    virtual XLogRecPtr getXLOGServerPos();

  };

  class FeedbackMessage : public XLOGStreamMessage {
  protected:

  public:
    FeedbackMessage(PGconn *prepared_connection);
    FeedbackMessage(PGconn *prepared_connection,
                    unsigned long long wal_segment_size);
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
    XLogRecPtr xlogserverendpos = InvalidXLogRecPtr;
    uint64 xlogservertime = 0;
  public:
    PrimaryFeedbackMessage(PGconn *prepared_connection);
    PrimaryFeedbackMessage(PGconn *prepared_connection,
                           unsigned long long wal_segment_size);
    ~PrimaryFeedbackMessage();

    /**
     * Reads the buffer and assigns all properties
     * to this feedback message.
     */
    virtual void assign(MemoryBuffer &mybuffer);

    /**
     * Overloaded operator to assign a memory buffer.
     */
    virtual PrimaryFeedbackMessage& operator<<(MemoryBuffer &srcbuffer);

    /**
     * Returns the XLOG position the primary reported its
     * current WAL stream ends.
     */
    virtual XLogRecPtr getXLOGServerPos();
  };

  class ReceiverStatusUpdateMessage : protected FeedbackMessage {
  protected:
    XLogRecPtr xlogPos_written = InvalidXLogRecPtr;
    XLogRecPtr xlogPos_flushed = InvalidXLogRecPtr;
    XLogRecPtr xlogPos_applied = InvalidXLogRecPtr;
    uint64 current_time_us = 0;
    bool report_flush_position = false;
  public:
    ReceiverStatusUpdateMessage(PGconn *prepared_connection);
    ReceiverStatusUpdateMessage(PGconn *prepared_connection,
                                unsigned long long wal_segment_size);
    ~ReceiverStatusUpdateMessage();

    /**
     * Sends a receiver status update to the inherited
     * database connection.
     */
    virtual void send();

    /**
     * Apply current positional XLOG information.
     *
     * This updates the XLOG position information
     * to report the current flush, write and apply locations
     * to the primary.
     */
    virtual void setStatus(XLogRecPtr written,
                           XLogRecPtr flushed,
                           XLogRecPtr applied);

    /**
     * Force this message to also update XLOG flush position.
     * Default is FALSE and calling this method forces each
     * ReceiverStatusUpdateMessage object to set this internally to
     * true. After calling send(), this flag will be resetted to FALSE.
     */
    virtual void reportFlushPosition();

  };

  class HotStandbyFeedbackMessage : public FeedbackMessage {
  protected:
  public:
    HotStandbyFeedbackMessage(PGconn *prepared_connection);
    HotStandbyFeedbackMessage(PGconn *prepared_connection,
                              unsigned long long wal_segment_size);
    ~HotStandbyFeedbackMessage();

    /**
     * Sends a hot standby feedback message to
     * the inherited postgresql connection.
     */
    virtual void send();
  };

}
#endif
