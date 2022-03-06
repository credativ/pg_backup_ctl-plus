#include <io_uring_instance.hxx>

#ifdef PG_BACKUP_CTL_HAS_LIBURING

using namespace pgbckctl;

vectored_buffer::vectored_buffer(unsigned int bufsize,
                                 unsigned int count) {

  unsigned int i;

  if (bufsize == 0)
    throw CIOUringIssue("vectored buffer must not be 0");

  if (count == 0)
    throw CIOUringIssue("number of buffers in vectored buffers must not be 0");

  BOOST_LOG_TRIVIAL(debug) << "allocated buffer bufsize = " << bufsize << " num blocks = " << count;

  num_buffers = count;
  buffer_size = bufsize;
  effective_size = 0;

  iovecs = new struct iovec[this->num_buffers];

  for (i = 0; i < this->num_buffers; i++) {
    buffers.push_back(std::make_shared<MemoryBuffer>(buffer_size));

    /* Prepare IO vectors suitable for preadv()/pwritev() */
    iovecs[i].iov_base = buffers[i]->ptr();
    iovecs[i].iov_len = buffer_size;
  }

}

void vectored_buffer::clear() {

  for(auto buf : buffers) {
    buf->clear();
  }

}

void vectored_buffer::setEffectiveSize(const ssize_t size, bool with_iovec) {

  if ( (size < 0) || (size > getSize()) ) {
    ostringstream oss;
    oss << "cannot set effective number of bytes("
        << size <<") "
        << "larger than maximum size("
        << getSize() << ")";
    throw CIOUringIssue(oss.str());
  }

  effective_size = size;

  /* If we are called to reset iovec, don't forget that */
  if (with_iovec) {
    iovecs->iov_len = size;
  }

}

ssize_t vectored_buffer::getEffectiveSize() {
  return effective_size;
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

IOUringInstance::IOUringInstance(unsigned int     queue_depth,
                                 size_t           block_size,
                                 struct io_uring  ring) {

  this->ring = ring;

  this->queue_depth = queue_depth;
  this->block_size = block_size;

  this->initialized = true;

}

IOUringInstance::IOUringInstance(unsigned int queue_depth,
                                 size_t       block_size) {

  this->queue_depth = queue_depth;
  this->block_size = block_size;

}

IOUringInstance::IOUringInstance() {}

IOUringInstance::~IOUringInstance() {

  if (initialized)
    io_uring_queue_exit(&this->ring);

}

void IOUringInstance::setup(std::shared_ptr<BackupFile> file) {

  int result;

  if (available()) {
    throw CIOUringIssue("io_uring already setup, call exit() before");
  }

  if (! file->isOpen() ) {
    throw CIOUringIssue("could not establish io_uring instance: file not opened");
  }

  result = io_uring_queue_init(this->queue_depth, &this->ring, 0);

  if (result < 0) {
    throw CIOUringIssue(strerror(-result), result);
  }

  this->file = file;
  this->initialized = ( result >= 0 );

}

void IOUringInstance::seen(struct io_uring_cqe **cqe) {
  io_uring_cqe_seen(&ring, *cqe);
  *cqe = NULL;
}

struct io_uring IOUringInstance::getRing() {

  if (!available())
    throw CIOUringIssue("could not return IO uring instance handle, you need to setup() before");

  return ring;

}

void IOUringInstance::read(std::shared_ptr<ArchiveFile> file,
                           std::shared_ptr<vectored_buffer> buf,
                           off_t pos) {

  struct io_uring_sqe *sqe = NULL;

  /*
   * Is this a valid archive file handle?
   */
  if (!file->isOpen()) {
    throw CIOUringIssue("file not opened");
  }

  /*
   * Must match queue depth and blocksize
   */
  if ( (this->queue_depth != buf->getNumberOfBuffers())
       || (this->block_size != buf->getBufferSize()) ) {

    throw CIOUringIssue("queue depth and buffer sizes must match io_uring initialization");

  }

  /* get a submission queue entry item */
  sqe = io_uring_get_sqe(&ring);

  if (!sqe) {
    throw CIOUringIssue("could not get a submission queue entry");
  }

  /* submit read request */
  io_uring_prep_readv(sqe,
                      file->getFileno(),
                      buf->iovecs,
                      buf->getNumberOfBuffers(),
                      pos);

  io_uring_submit(&ring);

}

ssize_t IOUringInstance::handle_current_io(std::shared_ptr<vectored_buffer> buffer) {

  io_uring_cqe *cqe = NULL;
  ssize_t result = (size_t) 0;

  if (!available())
    throw CIOUringIssue("could not handle I/O request, uring not available");

  /* wait for any completions */
  wait(&cqe);

  /* check return codes */
  if (cqe->res >= 0) {

    result = cqe->res;
    buffer->setEffectiveSize(cqe->res, true);
    BOOST_LOG_TRIVIAL(debug) << "handle_current_io(): effective size  " << cqe->res;
    seen(&cqe);

  } else {

    seen(&cqe);

    /* handle error condition */
    ostringstream oss;
    oss << "could not handle I/O request: " << strerror(cqe->res);
    throw CIOUringIssue(oss.str());

  }

  return result;

}

void IOUringInstance::write(std::shared_ptr<ArchiveFile> file,
                            std::shared_ptr<vectored_buffer> buf,
                            off_t pos) {

  struct io_uring_sqe *sqe = NULL;

  /*
   * File needs to be valid and opened.
   */
  if (!file->isOpen()) {
    throw CIOUringIssue("file not opened");
  }

  /*
   * Must match queue depth and block size.
   */
  if ( (this->queue_depth != buf->getNumberOfBuffers())
       || (this->block_size != buf->getBufferSize()) ) {

    throw CIOUringIssue("queue depth and buffer sizes must match io_uring initialization");

  }

  sqe = io_uring_get_sqe(&ring);

  if (!sqe) {
    throw CIOUringIssue("could not get a submission queue entry");
  }

  /* submit write request */
  io_uring_prep_writev(sqe,
                       file->getFileno(),
                       buf->iovecs,
                       buf->getNumberOfBuffers(),
                       pos);

  io_uring_submit(&ring);
}

void IOUringInstance::setBlockSize(size_t block_size) {

  this->block_size = block_size;

}

unsigned int IOUringInstance::getQueueDepth() {
  return this->queue_depth;
}

size_t IOUringInstance::getBlockSize() {

  if (!available())
    throw CIOUringIssue("could not get block size: io_uring instance not initialized");

  return this->block_size;

}

void IOUringInstance::alloc_buffer(std::shared_ptr<vectored_buffer> &vbuf) {

  if (!available())
    throw CIOUringIssue("cannot allocate buffer if IOUringInstance is not setup correctly");

  vbuf = std::make_shared<vectored_buffer>(block_size,
                                           queue_depth);

}

void IOUringInstance::setQueueDepth(unsigned int queue_depth) {

  if (available())
    throw CIOUringIssue("could not set queue depth: io_uring instance already setup");

  this->queue_depth = queue_depth;

}

bool IOUringInstance::available() {
  return initialized;
}

int IOUringInstance::wait(struct io_uring_cqe **cqe) {

  return io_uring_wait_cqe(&ring, cqe);

}

void IOUringInstance::exit() {

  if (!available())
    throw CArchiveIssue("attempt to tear down uninitialized io_uring");

  io_uring_queue_exit(&ring);
  initialized = false;
  file = nullptr;

}

#endif
