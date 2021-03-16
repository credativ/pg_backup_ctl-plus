
#include <iostream>
#include <pgmessage.hxx>

using namespace pgbckctl;
using namespace pgbckctl::pgprotocol;

/**
 *  PGMessageCopyResponse
 */
size_t PGMessageCopyResponse::setFormats(PGProtoCopyFormat * formats) {

  col_count_ = formats->count();
  overall_format_ = formats->getFormat();
  formats_ = formats->ptr();
  message_size_ += 2 * col_count_;
  return (size_t)col_count_;

}

size_t PGMessageCopyResponse::writeTo(std::shared_ptr<ProtocolBuffer> out_buffer) {

  PGMessage::writeTo(out_buffer);
  int16_t overall_format = overall_format_;

  out_buffer->write_short(overall_format);
  out_buffer->write_short(col_count_);

  for (int i = 0; i < col_count_; i++ ) {
    out_buffer->write_short(formats_[i]);
  }

  return message_size_;
}

size_t PGMessageCopyResponse::readFrom(std::shared_ptr<ProtocolBuffer> in_buffer) {

  PGMessage::readFrom(in_buffer);
  int16_t overall_format;

  in_buffer->read_short(overall_format);
  overall_format_ = (PGProtoCopyFormatType)overall_format;

  in_buffer->read_short(col_count_);

  for (int i = 0; i < col_count_; i++ ) {
    in_buffer->read_short(formats_[i]);
  }

  return message_size_;

}
