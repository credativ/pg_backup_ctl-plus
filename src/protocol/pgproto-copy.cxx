#include <sstream>
#include <ostream>
#include <pgproto-copy.hxx>

using namespace pgbckctl;
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
 * class PGProtoCopy
 * *****************************************************************************/

PGProtoCopy::PGProtoCopy(PGProtoCopyContext context) {

  if (context.state == nullptr) {
    throw CopyProtocolFailure("No initial state choosen.");
  }

  context_.state = context.state;
  context_.formats = context.formats;
  context_.input_buffer = context.input_buffer;
  context_.input_data_buffer = context.input_data_buffer;
  context_.output_buffer = context.output_buffer;
  context_.output_data_buffer = context.output_data_buffer;

}

PGProtoCopy::~PGProtoCopy() {}

size_t PGProtoCopy::write() {
  return context_.state->write(context_);
}

size_t PGProtoCopy::read() {
  return context_.state->read(context_);
}

PGProtoCopyStateType PGProtoCopy::state() {
  return context_.state->state();
}
