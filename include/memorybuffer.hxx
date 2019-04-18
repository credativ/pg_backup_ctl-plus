#ifndef __HAVE_MEMORY_BUFFER__
#define __HAVE_MEMORY_BUFFER__

namespace credativ {

  /**
   * A very lightweight in-memory buffer
   * class
   */
  class MemoryBuffer {
  protected:
    /**
     * Internal buffer array.
     */
    char *memory_buffer = NULL;

    /**
     * Internal memory buffer size.
     */
    size_t size;
  public:
    MemoryBuffer();
    MemoryBuffer(size_t initialsz);
    ~MemoryBuffer();

    /**
     * Allocate internal buffer. If an existing buffer exists,
     * it will be deallocated, its contents being thrown away.
     */
    virtual void allocate(size_t size);

    /**
     * Returns the internal allocated size.
     * A return value of 0 should be treated as
     * an unallocated buffer.
     */
    virtual size_t getSize();

    /**
     * Write into the buffer at the specified offset. Returns
     * the number of bytes written into the buffer. If the
     * end of the buffer is reached, 0 is returned.
     */
    virtual size_t write(const void *buf, size_t bufsize, size_t off);

    /**
     * Reads readsz from offset of the internal buffer.
     */
    virtual size_t read(void *buf, size_t readsz, size_t off);

    /**
     * Clears contents of the internal memory buffer.
     *
     * Effectively, this sets all bytes to NULL.
     */
    void clear();

    /**
     * Assigns contents of the specified buffer.
     *
     * NOTE: This allocates a new internal buffer, in opposite
     *       to a clear()/write() sequence.
     */
    virtual void assign(void *buf, size_t sz);

    std::ostream& operator<<(std::ostream& out);
    MemoryBuffer& operator=(MemoryBuffer &out);
    char& operator[](unsigned int index);

    /**
     * Returns a pointer of the internally maintained
     * byte buffer. The caller is responsible to maintain
     * it carefully, since its lifetime is bound to the
     * lifetime of its object instance. Any call to an uninitialized
     * pointer will cause a CPGBackupCtlFailure exception!
     */
    virtual char * ptr();
  };

}

#endif
