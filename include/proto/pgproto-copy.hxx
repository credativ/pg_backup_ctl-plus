#ifndef __HAVE_PG_PROTO_COPY__
#define __HAVE_PG_PROTO_COPY__

#include <string>
#include <mutex>
#include <memory>
#include <proto-buffer.hxx>
#include <pgsql-proto.hxx>
#include <pgbckctl_exception.hxx>

using namespace credativ;
using namespace credativ::pgprotocol;

namespace credativ {

  /* Forwarded declarations */
  class PGProtoCopy;
  class PGProtoState;
  struct PGProtoCopyContext;

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

    int16_t *formats = nullptr;
    uint16_t cols = 0;
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

  typedef enum {
        Init,
        Fail,
        Done,
        In,
        Out,
        Both
  } PGProtoCopyStateType;

  class PGProtoCopyState {
    public:
      PGProtoCopyState(){};
      virtual size_t read(PGProtoCopyContext &);
      virtual size_t write(PGProtoCopyContext &);
      virtual PGProtoCopyStateType state() = 0;
  };

  class PGProtoCopyResponseState : public PGProtoCopyState {
    protected:
      virtual std::shared_ptr<PGProtoCopyState> nextState() = 0;
      virtual PGMessageType type() = 0;
      size_t writeCopyResponse(PGProtoCopyContext &);
    public:
      PGProtoCopyStateType state();
      size_t write(PGProtoCopyContext &);
  };

  class PGProtoCopyOutResponseState : public PGProtoCopyResponseState {
    private:
      std::shared_ptr<PGProtoCopyState> nextState();
      PGMessageType type();
  };

  class PGProtoCopyInResponseState : public PGProtoCopyResponseState {
    private:
      std::shared_ptr<PGProtoCopyState> nextState();
      PGMessageType type();
  };

  class PGProtoCopyBothResponseState : public PGProtoCopyResponseState {
    private:
      std::shared_ptr<PGProtoCopyState> nextState();
      PGMessageType type();
  };

  class PGProtoCopyDataInState : public virtual PGProtoCopyState {
    protected:
      inline size_t readCopyData(PGProtoCopyContext &);
      inline size_t readCopyFail(PGProtoCopyContext &);
      inline size_t readCopyDone(PGProtoCopyContext &);
    public:
      virtual size_t read(PGProtoCopyContext &);
      virtual PGProtoCopyStateType state();
  };

  class PGProtoCopyDataOutState : public virtual PGProtoCopyState {
    protected:
      inline size_t writeCopyDone(PGProtoCopyContext &);
      inline size_t writeCopyData(PGProtoCopyContext &);
    public:
      virtual size_t write(PGProtoCopyContext &);
      virtual PGProtoCopyStateType state();
  };

  class PGProtoCopyDataBothState :
  public PGProtoCopyDataOutState, 
  public PGProtoCopyDataInState {

    public:
      size_t read(PGProtoCopyContext &);
      size_t write(PGProtoCopyContext &);
      PGProtoCopyStateType state();
  };

  class PGProtoCopyDoneState : public PGProtoCopyState {
    public:
      PGProtoCopyStateType state();
  };

  class PGProtoCopyFailState : public PGProtoCopyState {
    public:
      PGProtoCopyStateType state();
  };

  struct PGProtoCopyContext{
    std::shared_ptr<PGProtoCopyFormat> formats;
    std::shared_ptr<ProtocolBuffer> input_buffer;
    std::shared_ptr<ProtocolBuffer> input_data_buffer;
    std::shared_ptr<ProtocolBuffer> output_buffer;
    std::shared_ptr<ProtocolBuffer> output_data_buffer;
    std::shared_ptr<PGProtoCopyState> state;
  };

  class PGProtoCopy {
    protected:
      PGProtoCopyContext context_;
    public:
      PGProtoCopy (PGProtoCopyContext);
      ~PGProtoCopy();
      size_t write();
      size_t read();
      PGProtoCopyStateType state();
  };

}

#endif
