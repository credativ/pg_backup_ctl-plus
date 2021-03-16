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

namespace credativ {

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
    unsigned int buffer_size = 0;
    unsigned int num_buffers = 0;

    struct _buffer_pos {
      off_t offset = 0;
      unsigned int index = 0;
    } buffer_pos;

    /**
     * Returns the absolute offset into the vectorized
     * buffer array.
     */
    off_t bufferOffset();

    /**
     * Calculates the new absolute offset, but does not
     * assign it.
     */
    off_t calculateOffset(off_t offset);

  public:
    std::vector<std::shared_ptr<MemoryBuffer>> buffers;

    vectored_buffer(unsigned int bufsize, unsigned int count);
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
    ssize_t getSize();

    /**
     * Returns the size of a single I/O buffer
     */
    unsigned int getBufferSize();

    /**
     * Returns the number of I/O vectors.
     */
    unsigned int getNumberOfBuffers();

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
     * Advance the position into the vectorized buffer with
     * the given value. If the specified value is out of bounds,
     * this will throw.
     */
    void setOffset(off_t offset);

    /**
     * Returns a pointer to the current
     * buffer in the vector.
     */
    char * ptr();

    /**
     * Returns the current Buffer handle in the vector.
     */
    std::shared_ptr<MemoryBuffer> buffer();

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
     * Backup file to read from or write to.
     */
    std::shared_ptr<BackupFile> file;

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

    struct io_uring *ring = NULL;

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
                    size_t block_size);

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
     * Returns an allocated, aligned vectorized_buffer suitable
     * for use for an io_uring instance.
     */
    virtual void alloc_buffer(std::shared_ptr<vectored_buffer> &vbuf);

    /**
     * Returns true if the ring is available. This is
     * usually set if the caller used setup() before. exit()
     * cleans up internal resources, closes the io_uring instance
     * again.
     */
    virtual bool available();

    /**
     * Setup an io_uring instance which uses the specified file
     * for read/write I/O. Throws on error.
     */
    virtual void setup(std::shared_ptr<BackupFile> file);

    /**
     * Vectored read requests.
     *
     * Emplaces the specified vector into the ring. The vector size
     * must be smaller or equal to queue depth and buffer size must match
     * block size.
     */
    virtual void read(std::shared_ptr<vectored_buffer> buf);

    /**
     * Vectored write requests.
     *
     * Emplaces the specified vector into the ring. The vector size
     * must be smaller or equal to queue depth and buffer size must match
     * block size.
     */
    virtual void write(std::shared_ptr<vectored_buffer> buf);

    /**
     * Wait for consumer submissions
     */
    virtual void wait();

    /**
     * Tear down io_uring instance and free all internal
     * resources.
     */
    virtual void exit();

    /**
     * Returns the io_uring handle used by this instance.
     *
     * Throws in case setup() wasn't called before.
     */
    virtual struct io_uring *getRing();

  };

}

#endif
#endif
