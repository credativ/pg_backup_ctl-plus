#include <iostream>
#include <pgmessage.hxx>

using namespace pgbckctl;
using namespace pgbckctl::pgprotocol;

size_t PGMessageCopyData::setData(std::string &in_buffer){

  message_size_ = 5;
  message_data_ = in_buffer;
  message_size_ += message_data_.size();
  return message_size_;

}


std::string PGMessageCopyData::getData(){

  return message_data_;

}

size_t PGMessageCopyData::writeTo(std::shared_ptr<ProtocolBuffer> out_buffer) {

  PGMessage::writeTo(out_buffer);

  if (message_data_.size() > 0) {
    out_buffer->write_buffer((void*)message_data_.c_str(), message_data_.size());
  }

  return message_size_;

}

size_t PGMessageCopyData::readFrom(std::shared_ptr<ProtocolBuffer> in_buffer) {

  char * buffer = new char[in_buffer->getSize()];


  PGMessage::readFrom(in_buffer);
  if(message_size_ > 5){
    in_buffer->read_buffer((void*)buffer, in_buffer->getSize());
    message_data_ = std::string(buffer);
  }

  return message_size_;

}

