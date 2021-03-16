#include <io_uring_instance.hxx>

#ifdef PG_BACKUP_CTL_HAS_LIBURING

using namespace credativ;

vectored_buffer::vectored_buffer(unsigned int bufsize,
                                 unsigned int count) {

  unsigned int i;

  this->iovecs = new struct iovec[count];

  for (i = 0; i < count; count++) {
    buffers.push_back(std::make_shared<MemoryBuffer>(bufsize));

    /* Prepare IO vectors suitable for preadv()/pwritev() */
    this->iovecs[i].iov_base = buffers[i]->ptr();
    this->iovecs[i].iov_len = bufsize;
  }

}

unsigned int vectored_buffer::getNumberOfBuffers() {
  return this->num_buffers;
}

unsigned int vectored_buffer::getBufferSize() {
  return this->buffer_size;
}

ssize_t vectored_buffer::getSize() {

  return (num_buffers * buffer_size);

}

off_t vectored_buffer::bufferOffset() {

  return ( (buffer_pos.index * buffer_size) + buffer_pos.offset );

}

off_t vectored_buffer::calculateOffset(off_t offset) {

  return ( (buffer_pos.index * buffer_size) + buffer_pos.offset ) + offset;

}

void vectored_buffer::setOffset(off_t offset) {

  if ( (offset + bufferOffset()) < calculateOffset(offset) )
    throw CIOUringIssue("invalid position in vectored buffer: new offset is lower than current offset");

  if ( calculateOffset(offset) > getSize() )
    throw CIOUringIssue("new vectored buffer position exceeds total size");

  buffer_pos.index = (unsigned int)(offset / (num_buffers * buffer_size));
  buffer_pos.offset += offset;

}

std::shared_ptr<MemoryBuffer> vectored_buffer::buffer() {

  return buffers[buffer_pos.index];

}

char * vectored_buffer::ptr() {

  return buffers[buffer_pos.index]->ptr();

}

off_t vectored_buffer::getOffset() {

  return bufferOffset();

}

vectored_buffer::~vectored_buffer() {

  delete[] iovecs;

}

IOUringInstance::IOUringInstance(unsigned int queue_depth,
                                 size_t block_size) {

  this->ring = NULL;
  this->queue_depth = queue_depth;
  this->block_size = block_size;

}

IOUringInstance::IOUringInstance() {

  this->ring = NULL;

}

IOUringInstance::~IOUringInstance() {}

void IOUringInstance::setup(std::shared_ptr<BackupFile> file) {

  int result;

  if (! file->isOpen() ) {
    throw CIOUringIssue("could not establish io_uring instance: file not opened");
  }

  result = io_uring_queue_init(this->queue_depth, this->ring, 0);

  if (result < 0) {
    throw CIOUringIssue(strerror(-result), result);
  }

  this->file = file;
  this->initialized = ( (this->ring != NULL) && result >= 0 );

}

struct io_uring *IOUringInstance::getRing() {

  if (!available())
    throw CIOUringIssue("could not return IO uring instance handle, you need to setup() before");

  return this->ring;

}

void IOUringInstance::read(std::shared_ptr<vectored_buffer> buf) {

  /*
   * Must match queue depth and blocksize
   */
  if ( (this->queue_depth != buf->getNumberOfBuffers())
       || (this->block_size != buf->getBufferSize()) ) {

    throw CIOUringIssue("queue depth and buffer sizes should match io_uring initialization");

  }

  

}

void IOUringInstance::write(std::shared_ptr<vectored_buffer> buf) {

}

void IOUringInstance::setBlockSize(size_t block_size) {

  this->block_size = block_size;

}

size_t IOUringInstance::getBlockSize() {

  if (!available())
    throw CIOUringIssue("could not get block size: io_uring instance not initialized");

  return this->block_size;

}

void IOUringInstance::alloc_buffer(std::shared_ptr<vectored_buffer> &vbuf) {

  if (!available())
    throw CIOUringIssue("cannot allocate buffer if IOUringInstance is not setup correctly");

  vbuf = std::make_shared<vectored_buffer>(this->block_size,
                                           this->queue_depth);

}

void IOUringInstance::setQueueDepth(unsigned int queue_depth) {

  if (available())
    throw CIOUringIssue("could not set queue depth: io_uring instance already setup");

  this->queue_depth = queue_depth;

}

bool IOUringInstance::available() {
  return initialized;
}

void IOUringInstance::wait() {

}

void IOUringInstance::exit() {

  io_uring_queue_exit(this->ring);
  initialized = false;
  file = nullptr;

}

#endif
