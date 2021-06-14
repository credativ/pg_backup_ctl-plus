
#ifndef __HAVE_PG_MESSAGES
#define __HAVE_PG_MESSAGES

#include <memory>
#include <string>
#include <pgsql-proto.hxx>
#include <pgproto-copy.hxx>

namespace pgbckctl {

  /**
   * PGMessage
   *
   * Base class for all MessageTypes or Formats.
   */

  class PGMessage{
  protected:

        PGMessageType message_identifier_;
        size_t message_size_; // in Byte + 4Bytes

  public:
    PGMessage();

    virtual size_t writeTo(std::shared_ptr<ProtocolBuffer>);
    virtual size_t readFrom(std::shared_ptr<ProtocolBuffer>);
    size_t getSize();

  };

  /*
   * PGMessageCopyResponse
   *
   */
  class PGMessageCopyResponse : public PGMessage {
  private:

    PGProtoCopyFormatType overall_format_;
    int16_t col_count_;
    int16_t * formats_;

  public:

    PGMessageCopyResponse() {
      message_identifier_ = UndefinedMessage;
      overall_format_ = COPY_TEXT;
      col_count_ = 0;
      message_size_ = 9;
      formats_ = nullptr;
    };

    size_t writeTo(std::shared_ptr<ProtocolBuffer>);
    size_t readFrom(std::shared_ptr<ProtocolBuffer>);
    size_t setFormats(PGProtoCopyFormat *);
  };

  /**
   * PGMessageCopyResponse
   *
   */
  class PGMessageCopyBothResponse : public PGMessageCopyResponse {
  public:
    PGMessageCopyBothResponse() : PGMessageCopyResponse() {
      message_identifier_ = CopyBothResponseMessage;
    }

  };

  /**
   * PGMessageCopyInResponse
   *
   */
  class PGMessageCopyInResponse : public PGMessageCopyResponse {
  public:

    PGMessageCopyInResponse() : PGMessageCopyResponse() {
            message_identifier_ = CopyInResponseMessage;
    }

  };


  /**
   * PGMessageCopyOut
   *
   */
  class PGMessageCopyOutResponse : public PGMessageCopyResponse {
  public:

    PGMessageCopyOutResponse() : PGMessageCopyResponse() {
            message_identifier_ = CopyOutResponseMessage;
    }

  };

  /**
   * PGMessageCopyData;
   *
   */
  class PGMessageCopyData : public PGMessage {
  private:

        std::string message_data_;

  public:

    PGMessageCopyData() : PGMessage() {
            message_identifier_ = CopyDataMessage;
    };

    size_t writeTo(std::shared_ptr<ProtocolBuffer>);
    size_t readFrom(std::shared_ptr<ProtocolBuffer>);
    size_t setData(std::string &);
    std::string getData();
  };

  /**
   * PGMessageCopyDone
   *
   */
  class PGMessageCopyDone: public PGMessage {
  public:

    PGMessageCopyDone() : PGMessage() {
      message_identifier_ = CopyDoneMessage;
    };

  };

  /**
   * PGMessageCopyFail
   *
   */
  class PGMessageCopyFail: public PGMessage {
  private:

    std::string error_message_;

  public:

    PGMessageCopyFail() : PGMessage(){
      message_identifier_ = CopyFailMessage;
      error_message_ = "";
    };

    size_t writeTo(std::shared_ptr<ProtocolBuffer>);
    size_t readFrom(std::shared_ptr<ProtocolBuffer>);
    void setMessage(std::string);
    std::string getMessage();
  };

}

#endif
