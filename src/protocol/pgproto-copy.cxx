#include <ostream>
#include <pgproto-copy.hxx>

/* *****************************************************************************
 * Base class PGProtoCopy
 * *****************************************************************************/

PGProtoCopy::PGProtoCopy(ProtocolBuffer *buf) { }


PGProtoCopy::~PGProtoCopy() {}

/* *****************************************************************************
 * PGProtoCopyIn, protocol handler for COPY FROM STDIN
 * *****************************************************************************/

PGProtoCopyIn::PGProtoCopyIn(ProtocolBuffer *buf)
  : PGProtoCopy(buf) {}

PGProtoCopyIn::~PGProtoCopyIn() {}

void PGProtoCopyIn::begin() {

  /*
   * Create a CopyInResponse message to be sent to the client.
   */

}

void PGProtoCopyIn::end() {}
