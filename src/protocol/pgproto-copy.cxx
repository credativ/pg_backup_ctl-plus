#include <sstream>
#include <ostream>
#include <pgproto-copy.hxx>

/* *****************************************************************************
 * Base class PGProtoCopyFormat
 * *****************************************************************************/

PGProtoCopyFormat::PGProtoCopyFormat(unsigned short num_cols,
                                     bool all_binary) {

  this->cols = num_cols;
  this->formats = new short[this->cols];

  /*
   * PostgreSQL COPY protocol requires all
   * format flags to be textual in case the overal
   * format flag is set to textual, too. We do the same
   * for the binary format, though this can be overwritten
   * in this case. See set() for details.
   */
  if (all_binary) {

    for(int i = 0;i < cols; i++)
      formats[i] = COPY_BINARY;

    copy_format_type = COPY_BINARY;

  } else {

    for(int i = 0;i < cols; i++)
      formats[i] = COPY_TEXT;

    copy_format_type = COPY_TEXT;

  }
}

PGProtoCopyFormat::~PGProtoCopyFormat() {

  delete formats;

}

PGProtoCopyFormatType PGProtoCopyFormat::getFormat() {

  return copy_format_type;

}

void PGProtoCopyFormat::setFormat(PGProtoCopyFormatType format_type) {

  copy_format_type = format_type;

  if (copy_format_type == 0) {

    /* Text format requires column format
     * to be textual as well */
    for (int i = 0; i < cols; i++)
      formats[i] = 0;

  } else if (copy_format_type == 1) {

    /* nothing special needs to be done here */

  } else {

    std::ostringstream err;

    err << "invalid copy format type: \"" << format_type << "\"";
    throw CopyProtocolFailure(err.str());
    
  }
}

unsigned int PGProtoCopyFormat::count() {

  return cols;

}

short * PGProtoCopyFormat::ptr() {

  return formats;

}

short PGProtoCopyFormat::get(unsigned short idx) {

  if (idx > (cols - 1))
    throw CopyProtocolFailure("invalid access to copy format header");

  return formats[idx];

}

void PGProtoCopyFormat::set(unsigned short idx,
                            short value) {

  if (idx > (cols - 1))
    throw CopyProtocolFailure("invalid access to copy format header");

  formats[idx] = value;

}

short PGProtoCopyFormat::operator[](unsigned short idx) {

  return get(idx);

}

/* *****************************************************************************
 * Base class PGProtoCopy
 * *****************************************************************************/

PGProtoCopy::PGProtoCopy(ProtocolBuffer *buf) {

  /* We need a valid reference to recv/send buffer
   * handler */
  if (buf == nullptr) {
    throw CopyProtocolFailure("undefined recv/send buffer handler");
  }

  this->buf = buf;

}


PGProtoCopy::~PGProtoCopy() {}

void PGProtoCopy::prepare() {

  /* Calculate overal message size */
  calculateSize();

  /* Adjust buffer for message, including type byte */
  buf->allocate(size + 1);

  /*
   * Prepare the message type byte.
   */
  buf->write_byte(type);

  /*
   * Set message size
   */
  buf->write_int(size);


}

/* *****************************************************************************
 * Implementation of class PGProtoCopyDone
 * *****************************************************************************/

PGProtoCopyDone::PGProtoCopyDone(ProtocolBuffer *buf)
  : PGProtoCopy(buf) {

  type = CopyDoneMessage;

}

PGProtoCopyDone::~PGProtoCopyDone() {}

void PGProtoCopyDone::begin() {

  /* A CopyDone message is  relatively simple,
   * just call prepare to materialize the message header
   * and we're done */
  prepare();

}

void PGProtoCopyDone::end() {}

std::size_t PGProtoCopyDone::calculateSize() {

  size = 0;
  size += sizeof(MESSAGE_HDR_LENGTH_SIZE);

  return size;

}

/* *****************************************************************************
 * Implementation of class PGProtoCopyFail
 * *****************************************************************************/

PGProtoCopyFail::PGProtoCopyFail(ProtocolBuffer *buf)
  : PGProtoCopy(buf) {

  type = CopyFailMessage;

}

PGProtoCopyFail::~PGProtoCopyFail() {}

void PGProtoCopyFail::begin() {

  /* Prepare message header */
  prepare();

  /*
   * Write error message, including null byte
   *
   * NOTE: calculateSize() already honored the trailing
   *       NULL byte.
   */
  buf->write_buffer(error_message.c_str(), error_message.length());
  buf->write_byte('\0');

}

void PGProtoCopyFail::message(std::string msg) {

  error_message = msg;

}

void PGProtoCopyFail::end() {
  /* no op here */
}

std::size_t PGProtoCopyFail::calculateSize() {

  size = 0;
  size += sizeof(MESSAGE_HDR_LENGTH_SIZE);
  size += (error_message.length() + 1); /* null byte */

  return size;

}

/* *****************************************************************************
 * PGProtoCopyIn, protocol handler for COPY FROM STDIN
 * *****************************************************************************/

PGProtoCopyIn::PGProtoCopyIn(ProtocolBuffer *buf,
                             const PGProtoCopyFormat &format)
  : PGProtoCopy(buf) {

  type = CopyInResponseMessage;
  this->format = format;

}

PGProtoCopyIn::~PGProtoCopyIn() {}

size_t PGProtoCopyIn::calculateSize() {

  /* size of message length property */
  size += sizeof(MESSAGE_HDR_LENGTH_SIZE);

  /* format identifier */
  size += sizeof(char);

  /* number of cols property */
  size += sizeof(short);

  /* size of format identifier, for <cols> columns */
  size += (sizeof(short) * format.count());

  return size;
  
}

void PGProtoCopyIn::begin() {

  /* Prepare message header */
  prepare();

  /* Write message format identifier */
  buf->write_byte((char)format.getFormat());
  
}

void PGProtoCopyIn::data(ProtocolBuffer *buf) {

}

void PGProtoCopyIn::end() {}

/* *****************************************************************************
 * PGProtoCopyOut, protocol handler for COPY TO STDOUT
 * *****************************************************************************/

PGProtoCopyOut::PGProtoCopyOut(ProtocolBuffer *buf)
  : PGProtoCopy(buf) {

  type = CopyOutResponseMessage;

}

PGProtoCopyOut::~PGProtoCopyOut() {}

void PGProtoCopyOut::begin() {

}

void PGProtoCopyOut::end() {}

void PGProtoCopyOut::data(ProtocolBuffer *buf) {}

/* *****************************************************************************
 * PGProtoCopyBoth, protocol handler for START_REPLICATION Copy sub-protocol
 * *****************************************************************************/

PGProtoCopyBoth::PGProtoCopyBoth(ProtocolBuffer *buf)
  : PGProtoCopy(buf) {

  type = CopyBothResponseMessage;

}

PGProtoCopyBoth::~PGProtoCopyBoth() {}

void PGProtoCopyBoth::begin() {}

void PGProtoCopyBoth::data(ProtocolBuffer *buf) {}

void PGProtoCopyBoth::end() {}
