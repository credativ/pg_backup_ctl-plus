#include <iostream>
#include <sstream>
#include <cstring>
#include <pgbckctl_exception.hxx>
#include <memorybuffer.hxx>

using namespace pgbckctl;

MemoryBuffer::MemoryBuffer(char *buf) {

  if (buf == nullptr) {
    throw CPGBackupCtlFailure("cannot instantiate memory buffer from undefined source");
  }

  size_t sz = sizeof(buf);

  /*
   * Make a new internal buffer
   */
  this->alloc_internal(sz);

  /* Copy over bytes from buf */
  _write(buf, sz, 0);

}

MemoryBuffer::MemoryBuffer(size_t initialsz) {

  this->alloc_internal(initialsz);

}

MemoryBuffer::MemoryBuffer() {}

MemoryBuffer::~MemoryBuffer() {

  if (this->memory_buffer != NULL) {
    delete [] this->memory_buffer;
  }

}

void MemoryBuffer::alloc_internal(size_t size) {

  if (this->memory_buffer != NULL) {
    delete [] this->memory_buffer;
    this->memory_buffer = NULL;
    this->size          = 0;
  }

  this->memory_buffer = new char[size];
  this->size = size;

}

size_t MemoryBuffer::getSize() {
  return this->size;
}

void MemoryBuffer::allocate(size_t size) {

  this->alloc_internal(size);

}

size_t MemoryBuffer::_write(const void *buf, size_t bufsize, size_t off) {

  char *buf_ptr = this->memory_buffer + off;
  memcpy(buf_ptr, buf, bufsize);

  return bufsize;

}

size_t MemoryBuffer::write(const void *buf, size_t bufsize, size_t off) {

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

  return _write(buf, bufsize, off);

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

void MemoryBuffer::own(char *buf, size_t sz) {

  if (buf == nullptr) {
    throw CPGBackupCtlFailure("memory buffer cannot own undefined pointer");
  }

  if (memory_buffer != nullptr) {
    delete [] memory_buffer;
  }

  this->memory_buffer = buf;
  this->size = sz;

}

void MemoryBuffer::_assign(void *buf, size_t sz) {

  alloc_internal(sz);
  _write(buf, sz, 0);

}

void MemoryBuffer::assign(void *buf, size_t sz) {

  _assign(buf, sz);

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

MemoryBuffer& MemoryBuffer::operator=(MemoryBuffer& src) {

  if (this == &src) {
    throw CPGBackupCtlFailure("request for memory buffer self assignment");
  }

  /*
   * NOTE: allocate already frees the internal buffer
   *       and creates an new one with the appropiate size.
   */
  this->allocate(src.getSize());
  this->write(src.memory_buffer, src.getSize(), 0);
  return *this;

}

MemoryBuffer& MemoryBuffer::operator=(std::shared_ptr<MemoryBuffer> &src) {

  if (this == src.get()) {
    throw CPGBackupCtlFailure("request for memory buffer self assignment");
  }

  /*
   * NOTE: allocate already frees the internal buffer
   *       and creates an new one with the appropiate size.
   */
  this->allocate(src->getSize());
  this->write(src->memory_buffer, src->getSize(), 0);
  return *this;

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
