#include <iostream>
#include <sstream>
#include <cstring>
#include <pgbckctl_exception.hxx>
#include <memorybuffer.hxx>

using namespace pgbckctl;

MemoryBuffer::MemoryBuffer(size_t initialsz) {

  this->allocate(initialsz);

}

MemoryBuffer::MemoryBuffer() {

}

MemoryBuffer::~MemoryBuffer() {

  if (this->memory_buffer != NULL) {
    delete [] this->memory_buffer;
  }

}

size_t MemoryBuffer::getSize() {
  return this->size;
}

void MemoryBuffer::allocate(size_t size) {

  if (this->memory_buffer != NULL) {
    delete [] this->memory_buffer;
    this->memory_buffer = NULL;
    this->size          = 0;
  }

  this->memory_buffer = new char[size];
  this->size = size;

}

size_t MemoryBuffer::write(const void *buf, size_t bufsize, size_t off) {

  char *buf_ptr;

  if (this->memory_buffer == NULL)
    throw CPGBackupCtlFailure("could not write into uninitialized memory buffer");

  /*
   * Sanity check, off cannot be larger than size
   */
  if (off >= this->getSize()) {
    std::ostringstream oss;
    oss << "write offset into memory buffer("
        << off
        << ") exceeds size("
        << this->getSize()
        << ")";
    throw CPGBackupCtlFailure(oss.str());
  }

  /*
   * Also, off + bufsize must fit into the remaining buffer.
   */
  if ( (off + bufsize) > this->getSize() ) {
    std::ostringstream oss;
    oss << "writing " << bufsize << " into memory buffer exceeds size";
    throw CPGBackupCtlFailure(oss.str());
  }

  buf_ptr = this->memory_buffer + off;
  memcpy(buf_ptr, buf, bufsize);

  return bufsize;

}

size_t MemoryBuffer::read(void *buf, size_t readsz, size_t off) {

  if (this->memory_buffer == NULL)
    throw CPGBackupCtlFailure("could not write into uninitialized memory buffer");

  /*
   * Sanity check, off cannot be larger than size
   */
  if (off >= this->getSize()) {
    std::ostringstream oss;
    oss << "read offset into memory buffer("
        << off
        << ") exceeds size("
        << this->getSize()
        << ")";
    throw CPGBackupCtlFailure(oss.str());
  }

  /*
   * Also, off + bufsize must fit into the remaining buffer.
   */
  if ( (off + readsz) > this->getSize() ) {
    std::ostringstream oss;
    oss << "reading " << readsz << " from memory exhausts buffer size";
    throw CPGBackupCtlFailure(oss.str());
  }

  memcpy(buf, this->memory_buffer + off, readsz);

  return readsz;
}

void MemoryBuffer::assign(void *buf, size_t sz) {

  /*
   * Make a new internal buffer
   */
  this->allocate(sz);

  /* Copy over bytes from buf */
  this->write(buf, sz, 0);
}

void MemoryBuffer::clear() {

  if (this->memory_buffer == NULL)
    /* nothing to do */
    return;

  memset(this->memory_buffer, 0x0, this->size);
}

char& MemoryBuffer::operator[](unsigned int index) {

  /* Overflow checking */
  if (index >= this->getSize())
    throw CPGBackupCtlFailure("memory buffer index out of range");

  return this->memory_buffer[index];
}

MemoryBuffer& MemoryBuffer::operator=(MemoryBuffer& out) {

  /*
   * NOTE: allocate already frees the internal buffer
   *       and creates an new one with the appropiate size.
   */
  out.allocate(this->getSize());
  out.write(this->memory_buffer, this->getSize(), 0);
  return out;

}

char * MemoryBuffer::ptr() {

  if (this->memory_buffer == NULL)
    throw CPGBackupCtlFailure("attempt to access internal NULL pointer in memory buffer");

  return this->memory_buffer;

}

std::ostream& MemoryBuffer::operator<<(std::ostream& out) {

  out << std::string(this->memory_buffer);
  return out;

}
