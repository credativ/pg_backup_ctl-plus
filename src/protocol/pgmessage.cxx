#include <iostream>
#include <pgmessage.hxx>

using namespace credativ;
using namespace credativ::pgprotocol;


/**
 *  Class implemenation for PGMessage
 */

PGMessage::PGMessage() {

  message_identifier_ = UndefinedMessage;
  message_size_ = 5;

}

size_t PGMessage::writeTo(std::shared_ptr<ProtocolBuffer> out_buffer) {

  if (out_buffer == nullptr ){
    throw CopyProtocolFailure("nullptr exception");
  }

  out_buffer->allocate(message_size_);
  out_buffer->first();
  out_buffer->write_byte(message_identifier_);
  out_buffer->write_int(message_size_-1);

  return message_size_;

}

size_t PGMessage::readFrom(std::shared_ptr<ProtocolBuffer> in_buffer) {

  int data_size;

  if (in_buffer == nullptr ){
    throw CopyProtocolFailure("nullptr exception");
  }

  in_buffer->read_byte(message_identifier_);
  in_buffer->read_int(data_size);

  message_size_ = data_size +1;
  return message_size_;

}

size_t PGMessage::getSize() {

  return message_size_;

}

