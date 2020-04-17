#ifndef __HAVE_PG_PROTO_COPY__
#define __HAVE_PG_PROTO_COPY__

#include <string>
#include <pgsql-proto.hxx>
#include <pgbckctl_exception.hxx>

using namespace credativ;
using namespace credativ::pgprotocol;

namespace credativ {

  /* Forwarded declarations */
  class PGProtoCopyFormat;
  class PGProtoCopyFail;
  class PGProtoCopyDone;
  class PGProtoCopyIn;
  class PGProtoCopyOut;
  class PGProtoCopyBoth;

  /**
   * Copy subprotocol exception class.
   */
  class CopyProtocolFailure : public CPGBackupCtlFailure {
  public:

    CopyProtocolFailure(const char *errString) throw()
      : CPGBackupCtlFailure(errString) {}

    CopyProtocolFailure(std::string errString) throw()
      : CPGBackupCtlFailure(errString) {}

    virtual ~CopyProtocolFailure() throw() {}

    const char *what() const throw() {
      return errstr.c_str();
    }

  };

  /**
   * COPY format identifier.
   */
  typedef enum {
                COPY_TEXT = 0,
                COPY_BINARY = 1
  } PGProtoCopyFormatType;

  /**
   * Copy format instruction class.
   */
  class PGProtoCopyFormat {
  protected:

    short *formats = nullptr;
    unsigned short cols = 0;
    PGProtoCopyFormatType copy_format_type = COPY_BINARY;

  public:

    /*
     * Default c'tor, make a copy format
     * with the number of columns and either
     * all of them set to textual (all_binary=false)
     * or binary format (all_binary=true).
     */
    PGProtoCopyFormat(unsigned short num_cols = 1,
                      bool all_binary = true);

    virtual ~PGProtoCopyFormat();

    /**
     * Number of columns in COPY response.
     */
    virtual unsigned int count();

    /**
     * Pointer to the column format array. Don't modify
     * the pointer directly, since this is managed by the
     * format instance itself.
     */
    virtual short * ptr();


    /**
     * Get the format identifier for the specified column.
     */
    virtual short get(unsigned short idx);

    /**
     * Set the format specified for the specified column.
     */
    virtual void  set(unsigned short idx, short value);
    virtual short operator[](unsigned short idx);

    /**
     * Set the overall COPY mode, either COPY_TEXT or COPY_BINARY.
     */
    virtual void setFormat(PGProtoCopyFormatType format_type);

    /**
     * Returns the current overall format identifier.
     */
    virtual PGProtoCopyFormatType getFormat();
  };

  /**
   * Base class for PostgreSQL COPY sub-protocol
   * implementations.
   */
  class PGProtoCopy {
  protected:

    ProtocolBuffer *buf = nullptr;
    PGMessageType type = '\0';

    /**
     * Overall size of the copy message,
     * including 4 byte message size indicator.
     *
     * NOTE: derived copy format classes don't modify
     *       this attribute directly, they implement an
     *       own overwritten method calculateSize()
     *       to recalculate the message size.
     */
    std::size_t size = 0;

    /*
     * Prepare a copy message start
     */
    virtual void prepare();

    /**
     * Calculate total message size.
     */
    virtual std::size_t calculateSize() = 0;

  public:

    PGProtoCopy(ProtocolBuffer *buf);
    virtual ~PGProtoCopy();

    virtual void begin() = 0;
    virtual void end() = 0;

  };

  /**
   * Implements COPY FROM protocol support.
   */
  class PGProtoCopyIn : public PGProtoCopy {
  private:

    PGProtoCopyFormat format;

  protected:

    size_t calculateSize();

  public:

    PGProtoCopyIn(ProtocolBuffer *buf, const PGProtoCopyFormat &format);
    virtual ~PGProtoCopyIn();

    virtual void begin();
    virtual void data(ProtocolBuffer *buf);
    virtual void end();

  };

  /**
   * Implements COPY TO protocol support.
   */
  class PGProtoCopyOut : public PGProtoCopy {
  public:

    PGProtoCopyOut(ProtocolBuffer *buf);
    virtual ~PGProtoCopyOut();

    virtual void begin();
    virtual void data(ProtocolBuffer *buf);
    virtual void end();

  };

  /**
   * CopyBothResponse sub-protocol handler, used by START_REPLICATION
   * commands.
   */
  class PGProtoCopyBoth: public PGProtoCopy {
  public:

    PGProtoCopyBoth(ProtocolBuffer *buf);
    virtual ~PGProtoCopyBoth();

    virtual void begin();
    virtual void data(ProtocolBuffer *buf);
    virtual void end();
  };

  /**
   * Constructs a CopyFail Message
   */
  class PGProtoCopyFail : public PGProtoCopy {
  private:

    std::string error_message = "";

  protected:

    virtual std::size_t calculateSize();

  public:

    PGProtoCopyFail(ProtocolBuffer *buf);
    virtual ~PGProtoCopyFail();

    virtual void begin();
    virtual void message(std::string msg);
    virtual void end();

  };

  /**
   * Constructs a CopyDone message.
   */
  class PGProtoCopyDone : public PGProtoCopy {
  protected:

    virtual std::size_t calculateSize();

  public:

    PGProtoCopyDone(ProtocolBuffer *buf);
    virtual ~PGProtoCopyDone();

    virtual void begin();
    virtual void end();

  };

  /**
   * Constructs a CopyData message.
   */
  class PGProtoCopyData : public PGProtoCopy {
  public:


  };

}

#endif
