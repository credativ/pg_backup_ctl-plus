#include <xlogdefs.hxx>

/* For PGconn */
#include <libpq-fe.h>

using namespace credativ;

/* ****************************************************************************
 * XLOGStreamMessage - Base class for all XLOG message classes.
 * ****************************************************************************/

XLOGStreamMessage::XLOGStreamMessage(PGconn *prepared_connection) {

  this->connection = prepared_connection;
  this->requestResponse = false;
  this->kind = '\0';

}

XLOGStreamMessage::~XLOGStreamMessage() {}

unsigned char XLOGStreamMessage::what() {

  return this->kind;

}

void XLOGStreamMessage::wantsResponse() {

  this->requestResponse = true;

}

void XLOGStreamMessage::basicCheckMemoryBuffer(MemoryBuffer &mybuffer) {

  if (mybuffer.getSize() <= 0)
    throw XLOGMessageFailure("attempt to interpret empty XLOG data buffer");

  if (this->what() != mybuffer[0])
    throw XLOGMessageFailure("buffer doesn't hold valid XLOGDataMessage data");

}

XLOGStreamMessage* XLOGStreamMessage::message(PGconn *pg_connection,
                                              MemoryBuffer &srcbuffer) {

  /*
   * Empty buffer not allowed, return a null pointer
   * in this case.
   */
  if (srcbuffer.getSize() <= 0)
    return nullptr;

  switch(srcbuffer[0]) {
  case 'w':
    {
      XLOGStreamMessage *message = new XLOGDataStreamMessage(pg_connection);
      *message << srcbuffer;
      return message;
    }
  default:
    /* unknown message type, bail out hard. */
    throw XLOGMessageFailure("unknown message type: " + srcbuffer[0]);
  }
}

/* ****************************************************************************
 * XLOGDataStreamMessage - XLOG Buffer data from stream
 * ****************************************************************************/

XLOGDataStreamMessage::XLOGDataStreamMessage(PGconn *prepared_connection)
  : XLOGStreamMessage(prepared_connection) {

  this->kind = 'w';

}

XLOGDataStreamMessage::~XLOGDataStreamMessage() {}

void XLOGDataStreamMessage::assign(MemoryBuffer &mybuffer) {

  char bytes[8];

  /*
   * Perform some basic checks.
   */
  this->basicCheckMemoryBuffer(mybuffer);

  /*
   * Enough room for a XLOG Data Message ?
   */
  if (mybuffer.getSize() < 25)
    throw XLOGMessageFailure("buffer doesn't look like a XLOG data message: invalid size");

  /*
   * Copy over bytes into internal buffer.
   */

  /*
   * NOTE: Skip the first byte, since this is the message type identifier.
   * The next 8 bytes corresponds to the XLOG start position.
   */
  mybuffer.read((char *)&bytes, 8, 1);
  this->xlogstartpos = (long long)(bytes);

  /* XLOG server position */
  mybuffer.read((char *)&bytes, 8, 9);
  this->xlogserverpos = (long long)(bytes);

  /* XLOG timestamp */
  mybuffer.read((char *)&bytes, 8, 17);
  this->xlogstreamtime = (long long)(bytes);

  /* Now copy over XLOG data blocks. This start at byte 25 */
  if (mybuffer.getSize() > 25) {

    char *xlogdatabytes = new char[mybuffer.getSize() - 25];

    this->xlogdata.allocate(mybuffer.getSize() - 25);
    mybuffer.read(xlogdatabytes, this->xlogdata.getSize(), 25);
    this->xlogdata.write(xlogdatabytes, this->xlogdata.getSize(), 0);

    delete xlogdatabytes;
  }
}

XLOGStreamMessage& XLOGDataStreamMessage::operator<<(MemoryBuffer &srcbuffer) {

  this->assign(srcbuffer);
  return *this;

}

/******************************************************************************
 * Helper classes for feedback messages.
 ******************************************************************************/

FeedbackMessage::FeedbackMessage(PGconn *prepared_connection)
  : XLOGStreamMessage(prepared_connection) {

  /* No special handling here, since abstract */
}

FeedbackMessage::~FeedbackMessage() {}

/*****************************************************************************
 * ReceiverUpdateStatusMessage
 *****************************************************************************/

ReceiverStatusUpdateMessage::ReceiverStatusUpdateMessage(PGconn *prepared_connection)
  : FeedbackMessage(prepared_connection) {

  this->kind = 'r';

}

ReceiverStatusUpdateMessage::~ReceiverStatusUpdateMessage() {}

void ReceiverStatusUpdateMessage::send() {

  /*
   * Finally reset primary status request flag.
   */
  this->requestResponse = false;
}

/*****************************************************************************
 * HotStandbyFeedbackMessage
 *****************************************************************************/

HotStandbyFeedbackMessage::HotStandbyFeedbackMessage(PGconn *prepared_connection)
  : FeedbackMessage(prepared_connection) {

  this->kind = 'h';

}

HotStandbyFeedbackMessage::~HotStandbyFeedbackMessage() {}

/*****************************************************************************
 * PrimaryFeedbackMessage
 *****************************************************************************/

PrimaryFeedbackMessage::PrimaryFeedbackMessage(PGconn *prepared_connection)
  : XLOGStreamMessage(prepared_connection) {

  this->requestResponse = false;
  this->kind = 'k';
}

PrimaryFeedbackMessage::~PrimaryFeedbackMessage() {}

void PrimaryFeedbackMessage::assign(MemoryBuffer &mybuffer) {

  char buffer[8];

  /*
   * Some basic checks on the input memory buffer.
   */
  this->basicCheckMemoryBuffer(mybuffer);

  /*
   * Enough room for a primary status message ?
   */
  if (mybuffer.getSize() < 18) {
    throw XLOGMessageFailure("input buffer doesn't look like a primary status message");
  }

  /*
   * First byte is the message byte, so skip this and read
   * in the xlogserverendpos provided by this status message.
   */
  mybuffer.read((char *)&buffer, 8, 1);
  this->xlogserverendpos = (long long)buffer;

  /*
   * Starting at offset byte 9 we find the server time
   * as of starting the transmission of this message.
   */
  mybuffer.read((char *)&buffer, 8, 9);
  this->xlogservertime = (long long) buffer;

  /*
   * If the server wants a response, the last byte indicates
   * this by setting it to '1'.
   */
  if (mybuffer[17] = '1')
    this->wantsResponse();

}
