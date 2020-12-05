
#include <iostream>
#include <pgmessage.hxx>

using namespace credativ;
using namespace credativ::pgprotocol;

void PGMessageCopyFail::setMessage(std::string msg) {

  message_size_ = 5;
  error_message_ = msg;
  message_size_ += error_message_.size();

}

std::string PGMessageCopyFail::getMessage(){

  return error_message_;

}


size_t PGMessageCopyFail::writeTo(std::shared_ptr<ProtocolBuffer> out_buffer) {

  PGMessage::writeTo(out_buffer);

  if (error_message_.size() > 0) {
    out_buffer->write_buffer((void*)error_message_.c_str(), error_message_.size());
  }

  return message_size_;

}

size_t PGMessageCopyFail::readFrom(std::shared_ptr<ProtocolBuffer> in_buffer) {

  PGMessage::readFrom(in_buffer);

  if (message_size_ > 5){
    in_buffer->read_buffer((void*)error_message_.c_str(), error_message_.size());
  }

  return message_size_;

}

