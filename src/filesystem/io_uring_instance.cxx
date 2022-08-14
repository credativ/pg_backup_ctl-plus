#include <io_uring_instance.hxx>

#ifdef PG_BACKUP_CTL_HAS_LIBURING

using namespace pgbckctl;

vectored_buffer::vectored_buffer(size_t total_size,
                                 unsigned int bufsize) {

  unsigned int i;
  unsigned int extra_bytes = 0;

  if (total_size == 0)
    throw CIOUringIssue("vectored buffer size must not be 0");

  if (bufsize == 0)
    throw CIOUringIssue("vectored block size must not be 0");

  num_buffers = (total_size / bufsize);
  buffer_size = bufsize;
  effective_size = total_size;
  mysize         = total_size;

  /* We need an additional buffer in case some remaining bytes are left */
  if (total_size % bufsize) {
    num_buffers++;
    extra_bytes = total_size % bufsize;
  }

  iovecs = new struct iovec[this->num_buffers];

  for (i = 0; i < this->num_buffers; i++) {

    /* Prepare IO vectors suitable for preadv()/pwritev() */

    if (i < (num_buffers)) {
      buffers.push_back(std::make_shared<MemoryBuffer>(buffer_size));
      iovecs[i].iov_base = buffers[i]->ptr();
      iovecs[i].iov_len = buffer_size;
    } else {
      buffers.push_back(std::make_shared<MemoryBuffer>(extra_bytes));
      iovecs[i].iov_base = buffers[i]->ptr();
      iovecs[i].iov_len = extra_bytes;
    }

  }

}

void vectored_buffer::setEffectiveSize(const ssize_t usable, bool adjust_buflen) {

  ssize_t extra_bytes = 0;
  int i;

  if (usable > this->mysize) {
    std::ostringstream oss;

    oss << "number of effective bytes("
        << usable << ") "
        << "in a vectored buffer cannot be larger than inital size("
        << mysize << ")";
    throw CIOUringIssue(oss.str());
  }

  if (num_buffers == 0) {
    throw CIOUringIssue("invalid number of buffers (0) to set effective size");
  }

  num_buffers = (usable / buffer_size);
  effective_size = usable;

  /* We need an additional buffer in case some remaining bytes are left */
  if (usable % buffer_size) {
    num_buffers++;
    extra_bytes = usable % buffer_size;
  }

  if (adjust_buflen) {

    for (i = 0; i < this->num_buffers; i++) {

      /* Prepare IO vectors suitable for preadv()/pwritev() */

      if (i < (num_buffers - 1)) {
        iovecs[i].iov_len = buffer_size;
      } else {
        iovecs[i].iov_len = extra_bytes;
      }
    }

  }

}

ssize_t vectored_buffer::getEffectiveSize(bool recalculate) {

  ssize_t result = 0;

  if (!recalculate) {

    if (effective_size == mysize)
      result = mysize;
    else
      result = effective_size;

  } else {

    for (int i = 0; i < num_buffers; i++) {
      result += iovecs->iov_len;
    }

  }

  return result;

}

void vectored_buffer::clear() {

  unsigned int cur = 0;
  buffer_pos.index = 0;
  buffer_pos.offset = 0;

  num_buffers = buffers.size();

  for(auto buf : buffers) {

    buf->clear();

    /* readjust pointers to buffer vector */
    iovecs[cur].iov_base = buf->ptr();
    iovecs[cur].iov_len = getBufferSize();
    cur++;

  }

}

unsigned int vectored_buffer::getNumberOfBuffers() {
  return this->num_buffers;
}

unsigned int vectored_buffer::getBufferSize() {
  return this->buffer_size;
}

ssize_t vectored_buffer::getSize(bool recalculate) {

  if (recalculate) {
    ssize_t size = 0;

    for (auto i: buffers) {
      size += i->getSize();
    }

    return size;

  } else {
    return this->mysize;
  }


}

off_t vectored_buffer::bufferOffset() {

    return ( (buffer_pos.index * buffer_size) + buffer_pos.offset );

}

unsigned int vectored_buffer::getEffectiveNumberOfBuffers() {
  return (this->getNumberOfBuffers() - buffer_pos.index);
}

struct iovec *vectored_buffer::iovec_ptr() {
  return this->iovecs + buffer_pos.index;
}

void vectored_buffer::calculateOffset(ssize_t offset) {

  ssize_t buff_offset = offset;

  do {

    buff_offset -= this->iovec_ptr()->iov_len;

  } while ( (buffer_pos.index < this->getNumberOfBuffers())
            && (buff_offset >= (ssize_t) iovec_ptr()->iov_len) );

  /*
   * Current buffer needs to be adjusted to the correct offset reflected by effective
   * bytes written.
   */
  this->iovecs[buffer_pos.index].iov_base = (char *)this->iovecs[buffer_pos.index].iov_base + buff_offset;
  this->iovecs[buffer_pos.index].iov_len -= buff_offset;

}

void vectored_buffer::setOffset(ssize_t offset) {

  if (offset > getSize() ) {
    std::ostringstream oss;
    oss << "new vectored buffer position exceeds total size (new pos="
        << offset << " > size=" << getSize() << ")";
    throw CIOUringIssue(oss.str());
  }

  /* num_buffers couldn't be zero here , but we are paranoid and recheck */
  if (num_buffers == 0) {
    throw CIOUringIssue("number of buffers in vectored buffer are invalid (=0)");
  }

  calculateOffset(offset);

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

void IOUringInstance::setup() {

  int result;

  if (available()) {
    throw CIOUringIssue("io_uring already setup, call exit() before");
  }

  result = io_uring_queue_init(this->queue_depth, &this->ring, 0);

  if (result < 0) {
    throw CIOUringIssue(strerror(-result), result);
  }

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
  if (this->block_size != buf->getBufferSize()) {

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
                      buf->iovec_ptr(),
                      buf->getEffectiveNumberOfBuffers(),
                      pos);

  io_uring_submit(&ring);

}

ssize_t IOUringInstance::handle_current_io(std::shared_ptr<vectored_buffer> rbuf, bool set_position) {

  io_uring_cqe *cqe = NULL;
  ssize_t result = (ssize_t) 0;

  if (!available())
    throw CIOUringIssue("could not handle I/O request, uring not available");

  if (rbuf == nullptr)
    throw CIOUringIssue("could not complete I/O request with undefined buffer");

  /* wait for any completions */
  wait(&cqe);

  /* check return codes */
  if (cqe->res >= 0) {

    result = (ssize_t) cqe->res;

    /* Reflect size of request in I/O buffers in case there were some bytes left to be read/written */
    if (cqe->res > 0 && set_position)
      rbuf->setOffset((off_t) cqe->res);

    seen(&cqe);

  } else {

    /* handle error condition */
    ostringstream oss;
    oss << "could not handle I/O request: " << strerror(-cqe->res);

    /* mark completed queue event as seen before throwing */
    seen(&cqe);
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
  if (this->block_size != buf->getBufferSize()) {
    throw CIOUringIssue("buffer sizes must match io_uring initialization");

  }

  sqe = io_uring_get_sqe(&ring);

  if (!sqe) {
    throw CIOUringIssue("could not get a submission queue entry");
  }

  /* submit write request */
  io_uring_prep_writev(sqe,
                       file->getFileno(),
                       buf->iovec_ptr(),
                       buf->getEffectiveNumberOfBuffers(),
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

void IOUringInstance::alloc_buffer(std::shared_ptr<vectored_buffer> &vbuf, size_t total_size) {

  if (!available())
    throw CIOUringIssue("cannot allocate buffer if IOUringInstance is not setup correctly");

  vbuf = std::make_shared<vectored_buffer>(total_size, block_size);

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

  int rc = io_uring_wait_cqe(&ring, cqe);

  if (rc < 0) {
    throw CIOUringIssue(strerror(rc));
  }

  return rc;

}

void IOUringInstance::exit() {

  if (!available())
    throw CArchiveIssue("attempt to tear down uninitialized io_uring");

  io_uring_queue_exit(&ring);
  initialized = false;

}

#endif
