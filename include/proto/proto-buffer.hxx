#ifndef __HAVE_PROTO_BUFFER_HXX__
#define __HAVE_PROTO_BUFFER_HXX__

#include <memorybuffer.hxx>

namespace pgbckctl {

  class ProtocolBuffer;

  class ProtocolBuffer : public MemoryBuffer {

  protected:

    size_t curr_pos = 0;

  public:

    ProtocolBuffer();
    ProtocolBuffer(size_t size);
    virtual ~ProtocolBuffer();

    /**
     * Write to the current cursor position. If the protocol buffer is full,
     * this will throw a CPGBackupCtlFailure.
     */
    virtual size_t write_buffer(const void *buf, size_t bufsize);

    /**
     * Write a 16 bit short integer to buffer starting from
     * the current position
     */
    virtual size_t write_short(const short value);

    /**
     * Write a signed character value to buffer.
     */
    virtual size_t write_byte(const char c);

    /**
     * Write the specified unsigned character byte into the current
     * position of the buffer.
     */
    virtual size_t write_byte(const unsigned char c);

    /**
     * Read from the current cursor position. If the position is already located
     * at the end of the buffer, this will throw a CPGBackupCtlFailure.
     */
    virtual size_t read_buffer(void *buf, size_t readsz);

    /**
     * Write integer value to the buffer at its current position. The value
     * is automatically converted into network byte order. If there's no
     * more room (4 byte), an CPGBackupCtlFailure is thrown.
     */
    virtual size_t write_int(const int value);

    /**
     * Read a 16bit integer from the current position.
     */
    virtual size_t read_short(short &value);

    /**
     * Read integer value from the buffer at its current position. If the
     * cursor is already positioned at the end of the buffer, a CPGBackupCtlFailure
     * exception is thrown.
     */
    virtual size_t read_int(int &value);

    /**
     * Read an unsigned integer from the current position of the buffer.
     */
    virtual size_t read_int(unsigned int &value);

    /**
     * Read byte from the buffer at its current position. If the buffer
     * is already positioned at the end of the buffer, a CPGBackupCtlFailure
     * is thrown.
     */
    virtual size_t read_byte(unsigned char &c);

    /**
     * Read the signed character byte from the current position
     * of the buffer.
     */
    virtual size_t read_byte(char &c);

    /**
     * Move the cursor to the start position.
     */
    virtual void first();

    /**
     * Move the cursor to the end position.
     */
    virtual void last();

    /**
     * Returns the current position within the internal
     * byte buffer.
     */
    virtual size_t pos();

    /**
     * Overridden clear() method. Besides overwriting the current
     * buffer contents with null bytes, this also resets the current
     * byte position to the starting byte.
     */
    virtual void clear();

    /**
     * Overridden allocate() method. Also resets the current
     * position to the first byte of the buffer.
     */
    virtual void allocate(size_t size);
  };

}

#endif
