#include <arpa/inet.h>

#include <cstddef>
#include <ostream>
#include <proto-buffer.hxx>

using namespace credativ;

ProtocolBuffer::ProtocolBuffer() : MemoryBuffer() {

  this->curr_pos = (size_t) 0;

}

ProtocolBuffer::ProtocolBuffer(size_t size) : MemoryBuffer(size) {

  this->curr_pos = (size_t) 0;

}

ProtocolBuffer::~ProtocolBuffer() {}

size_t ProtocolBuffer::write(const void *buf, size_t bufsize) {

  size_t bw = 0;

  bw = MemoryBuffer::write(buf, bufsize, this->curr_pos);
  this->curr_pos += bw;

  return bw;

}

size_t ProtocolBuffer::write(const int value) {

  size_t bw = 0;
  int wv = htonl(value);

  bw += MemoryBuffer::write((void *) &wv, sizeof(wv), this->curr_pos);
  this->curr_pos += bw;

  return bw;

}

size_t ProtocolBuffer::write(const unsigned char c) {

  size_t bw = 0;

  bw = MemoryBuffer::write((void *) &c, sizeof(c), this->curr_pos);
  this->curr_pos += bw;

  return bw;

}

size_t ProtocolBuffer::read(int &value) {

  size_t br = 0;
  int rv;

  br = MemoryBuffer::read((void *) &rv, sizeof(rv), this->curr_pos);
  this->curr_pos += br;
  value = ntohl(rv);

  return br;

}

size_t ProtocolBuffer::read(void *buf, size_t readsz) {

  size_t br = 0;

  br = MemoryBuffer::read(buf, readsz, this->curr_pos);
  this->curr_pos += br;

  return br;

}

size_t ProtocolBuffer::read(unsigned char &c) {

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
