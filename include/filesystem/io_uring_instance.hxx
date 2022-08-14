/* here for pg_backup_ctl.hxx */
#include <common.hxx>

#ifdef PG_BACKUP_CTL_HAS_LIBURING
#ifndef HAVE_IO_URING_INSTANCE
#define HAVE_IO_URING_INSTANCE

#include <fs-archive.hxx>
#include <memorybuffer.hxx>


#include <liburing.h>
extern "C" {
#include <sys/uio.h>
}

namespace pgbckctl {

  class CIOUringIssue : public CArchiveIssue {
  private:
    int reason = 0;
  public:
    CIOUringIssue(const char *errstr) throw() : CArchiveIssue(errstr) {};
    CIOUringIssue(std::string errstr) throw() : CArchiveIssue(errstr) {};
    CIOUringIssue(const char *errstr, int reason) throw() : CArchiveIssue(errstr)
    {
      this->reason = reason;
    };
    CIOUringIssue(std::string errstr, int reason) throw() : CArchiveIssue(errstr) {
      this->reason = reason;
    }

    /**
     * Returns the error code set by io_uring.
     */
    virtual int getReason() { return this->reason; };
  };

  class vectored_buffer {
  private:
    ssize_t buffer_size = 0;
    ssize_t num_buffers = 0;
    ssize_t mysize  = 0;

    struct _buffer_pos {
      off_t offset = 0;
      unsigned int index = 0;
      unsigned int effective_buffers = 0;
    } buffer_pos;

    /**
     * Effective usable size of data in buffer array.
     *
     * This is effectively the amount of usable bytes in the
     * complete buffer set. For example, after read operations, this
     * could be set to the bytes read into buffer which can be used.
     * Note that this could be fairly less than the total amount
     * of bytes a buffer array can handle. Larger settings aren't
     * supported. See also setter/getter methods
     * @pgbckctl::vectored_buffered::setEffectiveSize() and
     * @pgbckctl::vectored_buffer::getEffectiveSize().
     *
     * This allows to reuse the current buffer for repeated read/write
     * operations. After write or reading a buffer which for example is
     * not fully instrumented up to his maximum size, the caller can
     * reinitialize the effective_size to reflect its current number
     * of bytes in effect.
     */
    ssize_t effective_size = 0;

    /**
     * Returns the absolute offset into the vectorized
     * buffer array.
     */
    off_t bufferOffset();

    /**
     * Calculates the new absolute offset.
     */
    void calculateOffset(off_t offset);

  public:
    std::vector<std::shared_ptr<MemoryBuffer>> buffers;

    explicit vectored_buffer(size_t total_size, unsigned int bufsize);
    virtual ~vectored_buffer();

    /**
     * Public member iovecs holds vectorized buffers
     * suitable for preadv()/pwritev(). It is not necessary
     * to initialize them, this is done by the constructor.
     *
     * If the items of buffers are deallocated, all pointers in the
     * iovec are going to be undefined as well. The caller
     * should not need to derefence them directly and use
     * the MemoryBuffer objects in buffers instead.
     */
    struct iovec* iovecs;

    /*
     * Returns the overall size of allocated buffers
     * in an vectorized buffer instance.
     */
    ssize_t getSize(bool recalculate = false);

    /**
     * Clear buffer contents.
     *
     * The buffer remain fully allocated, but it's contents are cleared with NULL bytes.
     */
    void clear();

    /**
     * Returns the size of a single I/O buffer
     */
    unsigned int getBufferSize();

    /**
     * Returns the number of I/O vectors.
     */
    unsigned int getNumberOfBuffers();

    /**
     * During write operations, getEffectiveNumberOfBuffers() can be
     * used during short read/writes to determine the current number of
     * buffers left for read/write operations. E.g. after a short read/write operation
     * setOffset() can be called to set the number of bytes effectively worked on, which also
     * calculates the correct offset within the buffer array.
     *
     * @return number of buffers effectively left for further read/write operations.
     */
    unsigned int getEffectiveNumberOfBuffers();

    /**
     * Returns the current offset.
     *
     * This is the absolute offset into the
     * vectorized buffer array. The caller can divide
     * the offset through the currentBuffer() index to get
     * the current buffer.
     */
    off_t getOffset();

    /**
     * Returns a pointer into the current iovec array based on positional
     * information set during read/write operations.
     *
     * setOffset() adjusts this to the current offset read or written,
     * so this is suitable to be passed directly to [p]readv()/[p]writev(), e.g.
     * after short read/write.
     */
    struct iovec *iovec_ptr() ;

    /**
     * Advance the position into the vectorized buffer with
     * the given value. If the specified value is out of bounds,
     * this will throw.
     */
    void setOffset(ssize_t offset);

    /**
     * Returns a pointer to the current
     * buffer in the vector.
     */
    char * ptr();

    /**
     * Returns the current Buffer handle in the vector.
     */
    std::shared_ptr<MemoryBuffer> buffer();

    /**
     * Set the effective number of usable bytes within the I/O buffers. If _adjust_buflen_ is
     * set to _true_, _setEffectiveSize() will also recalculate the iovec's length. This is
     * useful if someone wants to reuse the buffer contents for read/write.
     *
     * The caller *must* call clear() after having used the buffer instance.
     */
     void setEffectiveSize(const ssize_t usable, bool adjust_buflen = false);

     /**
      * Returns the effective usable number of bytes in the current buffers. This
      * number reflect the bytes written/read by the caller and is *not* the total amount
      * of allocated bytes.
      */
     ssize_t getEffectiveSize(bool recalculate = false);

  };

  /**
   * A handler class for I/O uring.
   */
  class IOUringInstance {

  private:

    /**
     * Modified by setup(), reset back to false
     * by exit(). See available() for a public accessor.
     */
    bool initialized = false;

    /**
     * Internal queue depth, see setQueueDepth()
     * to modify.
     */
    unsigned int queue_depth = DEFAULT_QUEUE_DEPTH;

    /**
     * Internal block size, see setBlockSize()
     * to modify.
     */
    size_t block_size = DEFAULT_BLOCK_SIZE;

  protected:

    struct io_uring ring;

  public:

    /**
     * Default number of entries for SQE and CQE
     */
    const static unsigned int DEFAULT_QUEUE_DEPTH = 8;

    /**
     * Default block size for vectored buffers.
     */
    const static size_t DEFAULT_BLOCK_SIZE = 4096;

    /* C'tor */
    IOUringInstance();
    IOUringInstance(unsigned int queue_depth,
                    size_t       block_size);
    IOUringInstance(unsigned int     queue_depth,
                    size_t           block_size,
                    struct io_uring  ring);


    /* D'tor */
    virtual ~IOUringInstance();

    /**
     * Sets the internal block size.
     */
    virtual void setBlockSize(size_t block_size);

    /**
     * Returns the current configured block size
     * of an io_uring_instance. Throws if not initialized.
     */
    virtual size_t getBlockSize();

    /**
     * Set queue depth of an io_uring_instance.
     *
     * This should be called before setup(), otherwise
     * this will throw.
     */
    virtual void setQueueDepth(unsigned int queue_depth);

    /**
     * Returns the queue depth of a ring instance.
     */
    virtual unsigned int getQueueDepth();

    /**
     * Returns an allocated, aligned vectorized_buffer suitable
     * for use for an io_uring instance.
     */
    virtual void alloc_buffer(std::shared_ptr<vectored_buffer> &vbuf, size_t size);

    /**
     * Returns true if the ring is available. This is
     * usually set if the caller used setup() before. exit()
     * cleans up internal resources, closes the io_uring instance
     * again.
     */
    virtual bool available();

    /**
     * Setup an io_uring instance.
     */
    virtual void setup();

    /**
     * Vectored read requests.
     *
     * Emplaces the specified vector into the ring. The vector size
     * must be smaller or equal to queue depth and buffer size must match
     * block size.
     */
    virtual void read(std::shared_ptr<ArchiveFile> file,
                      std::shared_ptr<vectored_buffer> buf,
                      off_t pos);

    /**
     * Handles an I/O operation by waiting for completion and
     * sets internal buffer properties.
     */
    virtual ssize_t handle_current_io(std::shared_ptr<vectored_buffer> rbuf, bool set_position = false);

    /**
     * Vectored write requests.
     *
     * Emplaces the specified vector into the ring. The vector size
     * must be smaller or equal to queue depth and buffer size must match
     * block size.
     */
    virtual void write(std::shared_ptr<ArchiveFile> file,
                       std::shared_ptr<vectored_buffer> buf,
                       off_t pos);

    /**
     * Wait for consumer completion
     */
    virtual int wait(struct io_uring_cqe **cqe);

    /**
     * Tear down io_uring instance and free all internal
     * resources.
     */
    virtual void exit();

    /**
     * Must be called after wait() to finalize a CQE completion item.
     */
    void seen(struct io_uring_cqe **cqe);

    /**
     * Returns the io_uring handle used by this instance.
     *
     * Throws in case setup() wasn't called before.
     */
    virtual struct io_uring getRing();

  };

}

#endif
#endif
