#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <boost/bind.hpp>
#include <boost/log/trivial.hpp>
#include <pgiosocketcontext.hxx>

using namespace credativ;
using namespace credativ::pgprotocol;

/* ****************************************************************************
 * PGSocketIOContextInterface
 * ****************************************************************************/

#define SOCKET_P(ptr) (*((ptr)->soc))

/* c'tor and d'tor without anything to do atm */
PGSocketIOContextInterface::PGSocketIOContextInterface() {}
PGSocketIOContextInterface::~PGSocketIOContextInterface() {}

PGSocketIOContextInterface::PGSocketIOContextInterface(boost::asio::ip::tcp::socket *soc)
  : ExecutableContext() {

  if(soc == nullptr) {
    throw ExecutableContextFailure("cannot instantiate socket I/O context without valid socket");
  }

  this->soc = soc;

}

boost::asio::ip::tcp::socket *PGSocketIOContextInterface::socket() {

  return this->soc;

}

void PGSocketIOContextInterface::start_read_msg() {

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO start_read_msg with " << read_body_buffer.getSize() << " bytes";

  boost::asio::async_read(SOCKET_P(this), boost::asio::buffer(this->read_body_buffer.ptr(),
                                                              this->read_body_buffer.getSize()),
                          boost::asio::transfer_exactly(read_body_buffer.getSize()),
                          boost::bind(&PGSocketIOContextInterface::pgproto_msg_in,
                                      this, _1, _2));


}

void PGSocketIOContextInterface::initial_read() {

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO initial_read with " << read_header_buffer.getSize() << " bytes";

  boost::asio::async_read(SOCKET_P(this), boost::asio::buffer(this->read_header_buffer.ptr(),
                                                              this->read_header_buffer.getSize()),
                          boost::asio::transfer_exactly(read_header_buffer.getSize()),
                          boost::bind(&PGSocketIOContextInterface::startup_msg_in,
                                      this, _1, _2));

}

void PGSocketIOContextInterface::initial_read_body() {

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO initial_read with " << read_body_buffer.getSize() << " bytes";

  boost::asio::async_read(SOCKET_P(this), boost::asio::buffer(this->read_body_buffer.ptr(),
                                                              this->read_body_buffer.getSize()),
                          boost::asio::transfer_exactly(read_body_buffer.getSize()),
                          boost::bind(&PGSocketIOContextInterface::startup_msg_body,
                                      this, _1, _2));

}


void PGSocketIOContextInterface::start_read_header() {

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO start_read_header with " << read_header_buffer.getSize() << " bytes";

  boost::asio::async_read(SOCKET_P(this), boost::asio::buffer(this->read_header_buffer.ptr(),
                                                              this->read_header_buffer.getSize()),
                          boost::asio::transfer_exactly(read_header_buffer.getSize()),
                          boost::bind(&PGSocketIOContextInterface::pgproto_header_in,
                                      this, _1, _2));

}

void PGSocketIOContextInterface::start_write() {

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO start_write with " << this->write_buffer.getSize() << " bytes";

  boost::asio::async_write(SOCKET_P(this), boost::asio::buffer(this->write_buffer.ptr(),
                                                               this->write_buffer.getSize()),
                           boost::asio::transfer_exactly(write_buffer.getSize()),
                           boost::bind(&PGSocketIOContextInterface::pgproto_msg_out,
                                       this, _1));

}
