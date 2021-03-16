#include <arpa/inet.h>

#include <cstddef>
#include <ostream>
#include <iostream>

#include <pgsql-proto.hxx>

using namespace pgbckctl;
using namespace pgbckctl::pgprotocol;


/* *****************************************************************************
 * ProtocolErrorStack Implementation
 * ****************************************************************************/

void ProtocolErrorStack::toBuffer(ProtocolBuffer &dest,
                                  size_t &msg_size,
                                  bool error) {

  /* ErrorResponse header */
  pgprotocol::pg_protocol_msg_header hdr;

  /*
   * Allocate enough space in the buffer. We must
   * add a byte for each message, because a null byte
   * indicates a new error field in the whole message.
   * Also, we need a finalizing NULL byte to indicate
   * the end of message.
   */
  msg_size = this->content_size + this->count() + 1;

  if (error) {

    hdr.type = ErrorMessage;

  } else {

    hdr.type = NoticeMessage;

  }

  hdr.length = MESSAGE_HDR_LENGTH_SIZE + msg_size;

  dest.allocate(MESSAGE_HDR_SIZE + msg_size);

  dest.write_byte(hdr.type);
  dest.write_int(hdr.length);

  /*
   * Pop error response fields from the stack,
   * write contents to the protocol buffer.
   */
  while (! empty()) {

    PGErrorResponseField ef = this->top();

    /* Message field type */
    dest.write_byte(ef.type);

    /* Write message, including null byte! */
    dest.write_buffer(ef.value.c_str(), ef.value.length());
    dest.write_byte('\0');

    /* Remove element from the stack and proceed */
    pop();

  }

  /* Final NULL byte, indicating end of message */
  dest.write_byte('\0');

}

void ProtocolErrorStack::push(PGErrorResponseType type,
                              std::string value) {

  PGErrorResponseField ef;

  ef.type = type;
  ef.value = value;

  this->es.push(ef);

  this->top_element_size = (sizeof(type) + value.length());
  this->content_size += this->top_element_size;

}

void ProtocolErrorStack::push(PGErrorResponseField field) {

  this->es.push(field);
  this->top_element_size = (sizeof(field.type) + field.value.length());
  this->content_size += this->top_element_size;

}

size_t ProtocolErrorStack::getTopElementSize() {

  return this->top_element_size;

}

size_t ProtocolErrorStack::getTotalElementSize() {

  return this->content_size;

}

size_t ProtocolErrorStack::count() {

  return this->es.size();
}

PGErrorResponseField ProtocolErrorStack::top() {

  return this->es.top();

}

void ProtocolErrorStack::pop() {

  /*
   * Before removing the top-level
   * element, we must decrease the total size
   * of the error stack
   */
  this->content_size -= this->top_element_size;

  /* Remove current element */
  this->es.pop();

  /*
   * Recalculate top element size, but only if anything is left.
   */
  if (!this->es.empty()) {

    PGErrorResponseField ef = this->top();
    this->top_element_size = (sizeof(ef.type) + ef.value.length());

  } else {

    this->top_element_size = 0;
    this->content_size = 0;

  }

}

bool ProtocolErrorStack::empty() {

  return this->es.empty();

}

/* *****************************************************************************
 * ProtocolBuffer Implementation
 * ****************************************************************************/

ProtocolBuffer::ProtocolBuffer() : MemoryBuffer() {

  this->curr_pos = (size_t) 0;

}

ProtocolBuffer::ProtocolBuffer(size_t size) : MemoryBuffer(size) {

  this->curr_pos = (size_t) 0;

}

ProtocolBuffer::~ProtocolBuffer() {}

size_t ProtocolBuffer::write_buffer(const void *buf, size_t bufsize) {

  size_t bw = 0;

  bw = MemoryBuffer::write(buf, bufsize, this->curr_pos);
  this->curr_pos += bw;

  return bw;

}

size_t ProtocolBuffer::write_int(const int value) {

  size_t bw = 0;
  int wv = htonl(value);

  bw += MemoryBuffer::write((void *) &wv, sizeof(wv), this->curr_pos);
  this->curr_pos += bw;

  return bw;

}

size_t ProtocolBuffer::write_short(const short value) {

  size_t bw = 0;
  short wv = htons(value);

  bw += MemoryBuffer::write((void *) &wv, sizeof(wv), this->curr_pos);
  this->curr_pos += bw;

  return bw;

}

size_t ProtocolBuffer::write_byte(const char c) {

  size_t bw = 0;

  bw = MemoryBuffer::write((void *) &c, sizeof(c), this->curr_pos);
  this->curr_pos += bw;

  return bw;

}

size_t ProtocolBuffer::write_byte(const unsigned char c) {

  size_t bw = 0;

  bw = MemoryBuffer::write((void *) &c, sizeof(c), this->curr_pos);
  this->curr_pos += bw;

  return bw;

}

size_t ProtocolBuffer::read_short(short &value) {

  size_t br = 0;
  short rv;

  br = MemoryBuffer::read((void *) &rv, sizeof(rv), this->curr_pos);
  this->curr_pos += br;
  value = ntohs(rv);

  return br;

}

size_t ProtocolBuffer::read_int(unsigned int &value) {

  size_t br = 0;
  unsigned int rv;

  br = MemoryBuffer::read((void *) &rv, sizeof(rv), this->curr_pos);
  this->curr_pos += br;
  value = ntohl(rv);

  return br;

}

size_t ProtocolBuffer::read_int(int &value) {

  size_t br = 0;
  int rv;

  br = MemoryBuffer::read((void *) &rv, sizeof(rv), this->curr_pos);
  this->curr_pos += br;
  value = ntohl(rv);

  return br;

}

size_t ProtocolBuffer::read_buffer(void *buf, size_t readsz) {

  size_t br = 0;

  br = MemoryBuffer::read(buf, readsz, this->curr_pos);
  this->curr_pos += br;

  return br;

}

size_t ProtocolBuffer::read_byte(char &c) {

  size_t br = 0;
  char rv;

  br = MemoryBuffer::read((void *) &rv, sizeof(rv), this->curr_pos);
  this->curr_pos += br;
  c = rv;

  return br;

}

size_t ProtocolBuffer::read_byte(unsigned char &c) {

  size_t br = 0;
  unsigned char rv;

  br = MemoryBuffer::read((void *) &rv, sizeof(rv), this->curr_pos);
  this->curr_pos += br;
  c = rv;

  return br;

}

void ProtocolBuffer::first() {

  this->curr_pos = 0;

}

void ProtocolBuffer::last() {

  this->curr_pos = MemoryBuffer::getSize() - 1;

}

size_t ProtocolBuffer::pos() {

  return this->curr_pos;

}

void ProtocolBuffer::allocate(size_t size) {

  MemoryBuffer::allocate(size);
  this->first();

}

void ProtocolBuffer::clear() {

  MemoryBuffer::clear();
  this->first();

}
