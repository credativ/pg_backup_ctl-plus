#include <xlogdefs.hxx>

/* For PostgreSQL FE routines */
#include <postgres_fe.h>
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

XLOGStreamMessage::XLOGStreamMessage(PGconn *prepared_connection,
                                     unsigned long long wal_segment_size) {

  this->connection = prepared_connection;
  this->requestResponse = false;
  this->kind = '\0';

  this->setWALSegmentSize(wal_segment_size);
}

XLOGStreamMessage::~XLOGStreamMessage() {}

unsigned char XLOGStreamMessage::what() {

  return this->kind;

}

unsigned long long XLOGStreamMessage::getWALSegmentSize() {
  return this->wal_segment_size;
}

void XLOGStreamMessage::setWALSegmentSize(unsigned long long wal_segment_size) {

  if (wal_segment_size <= 0
      && (wal_segment_size % 2) != 0) {
    std::ostringstream oss;

    oss << "invalid WAL segment size in message: " << wal_segment_size;
    throw XLOGMessageFailure(oss.str());
  }

  this->wal_segment_size = wal_segment_size;

}

void XLOGStreamMessage::wantsResponse() {

  this->requestResponse = true;

}

bool XLOGStreamMessage::responseRequested() {

  return this->requestResponse;

}

void XLOGStreamMessage::basicCheckMemoryBuffer(MemoryBuffer &mybuffer) {

  if (mybuffer.getSize() <= 0)
    throw XLOGMessageFailure("attempt to interpret empty XLOG data buffer");

  if (this->what() != mybuffer[0])
    throw XLOGMessageFailure("buffer doesn't hold valid XLOGDataMessage data");

}

XLOGStreamMessage* XLOGStreamMessage::message(PGconn *pg_connection,
                                              MemoryBuffer &srcbuffer,
                                              unsigned long long wal_segment_size) {

  /*
   * Empty buffer not allowed, return a null pointer
   * in this case.
   */
  if (srcbuffer.getSize() <= 0) {
    return nullptr;
  }

  switch(srcbuffer[0]) {
  case 'w':
    {
      XLOGStreamMessage *message = new XLOGDataStreamMessage(pg_connection,
                                                             wal_segment_size);
      *message << srcbuffer;
      return message;
    }
  case 'k':
    {
      PrimaryFeedbackMessage *message = new PrimaryFeedbackMessage(pg_connection,
                                                                   wal_segment_size);
      *message << srcbuffer;
      return message;
    }
  default:
    /* unknown message type, bail out hard. */
    {
      std::ostringstream oss;

      oss << "unknown message type: " << srcbuffer[0];
      throw XLOGMessageFailure(oss.str());
    }
  }

  /* normally not reached */
  return nullptr;
}

/* ****************************************************************************
 * XLOGDataStreamMessage - XLOG Buffer data from stream
 * ****************************************************************************/

XLOGDataStreamMessage::XLOGDataStreamMessage(PGconn *prepared_connection)
  : XLOGStreamMessage(prepared_connection) {

  this->kind = 'w';

}

XLOGDataStreamMessage::XLOGDataStreamMessage(PGconn *prepared_connection,
                                             unsigned long long wal_segment_size)
  : XLOGStreamMessage(prepared_connection, wal_segment_size) {

  this->kind = 'w';

}

XLOGDataStreamMessage::~XLOGDataStreamMessage() {}

XLogRecPtr XLOGDataStreamMessage::getXLOGStartPos() {

  return this->xlogstartpos;

}

XLogRecPtr XLOGDataStreamMessage::getXLOGServerPos() {

  return this->xlogserverpos;

}

void XLOGDataStreamMessage::assign(MemoryBuffer &mybuffer) {

  char bytes[8];
  XLogRecPtr xlog_pos;

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
  mybuffer.read(bytes, 8, 1);
  memcpy(&xlog_pos, bytes, 8);
  this->xlogstartpos = SWAP_UINT64(xlog_pos);

  /* XLOG server position */
  mybuffer.read((char *)&bytes, 8, 9);
  memcpy(&xlog_pos, bytes, 8);
  this->xlogserverpos = SWAP_UINT64(xlog_pos);

  /* XLOG timestamp */
  mybuffer.read((char *)&bytes, 8, 17);
  this->xlogstreamtime = (long long)(bytes);

  /* Now copy over XLOG data blocks. This start at byte 25 */
  if (mybuffer.getSize() > 25) {

    char *xlogdatabytes = new char[mybuffer.getSize() - 25];

    this->xlogdata.allocate(mybuffer.getSize() - 25);
    mybuffer.read(xlogdatabytes, this->xlogdata.getSize(), 25);
    this->xlogdata.write(xlogdatabytes, this->xlogdata.getSize(), 0);

    delete [] xlogdatabytes;
  }
}

XLOGStreamMessage& XLOGDataStreamMessage::operator<<(MemoryBuffer &srcbuffer) {

  this->assign(srcbuffer);
  return *this;

}

char * XLOGDataStreamMessage::buffer() {
  return this->xlogdata.ptr();
}

size_t XLOGDataStreamMessage::dataBufferSize() {
  return this->xlogdata.getSize();
}

/******************************************************************************
 * Helper classes for feedback messages.
 ******************************************************************************/

FeedbackMessage::FeedbackMessage(PGconn *prepared_connection)
  : XLOGStreamMessage(prepared_connection) {

  /* No special handling here, since abstract */
}

FeedbackMessage::FeedbackMessage(PGconn *prepared_connection,
                                 unsigned long long wal_segment_size)
  : XLOGStreamMessage(prepared_connection, wal_segment_size) {

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

ReceiverStatusUpdateMessage::ReceiverStatusUpdateMessage(PGconn *prepared_connection,
                                                         unsigned long long wal_segment_size)
  : FeedbackMessage(prepared_connection, wal_segment_size) {

  this->kind = 'r';

}

ReceiverStatusUpdateMessage::~ReceiverStatusUpdateMessage() {}

void ReceiverStatusUpdateMessage::send() {

  char replydata[34];

  /* status message type byte */
  replydata[0] = this->kind;

  /* recv/write location */
  uint64_hton_sendbuf(&replydata[1], this->xlogPos_written);

  /* flush location, if requested */
  if (report_flush_position) {
    uint64_hton_sendbuf(&replydata[9], this->xlogPos_flushed);
  } else {
    uint64_hton_sendbuf(&replydata[9], InvalidXLogRecPtr);
  }

  /* apply location, no used here yet */
  uint64_hton_sendbuf(&replydata[17], InvalidXLogRecPtr);

  /* client local time, in microseconds */
  uint64_hton_sendbuf(&replydata[25], this->current_time_us);

  /* Request primary status keepalive, if required */
  if (this->responseRequested())
    replydata[33] = 1;
  else
    replydata[33] = 0;

  if (PQputCopyData(this->connection, replydata, sizeof(replydata)) <= 0
      || PQflush(this->connection)) {
    std::ostringstream oss;
    oss << "could not send status update message to primary: "
        << PQerrorMessage(this->connection);
    this->requestResponse = false;
    this->report_flush_position = false;
    throw XLOGMessageFailure(oss.str());
  }

  /*
   * Finally reset primary status request flag.
   */
  this->report_flush_position = false;
  this->requestResponse = false;
}

void ReceiverStatusUpdateMessage::reportFlushPosition() {

  this->report_flush_position = true;

}

void
ReceiverStatusUpdateMessage::setStatus(XLogRecPtr written,
                                       XLogRecPtr flushed,
                                       XLogRecPtr applied) {

  std::tm epoch_ts = {};
  time_t t = time(NULL);

  /* current time value */
  std::tm current_ts = *localtime(&t);

  /*
   * NOTE:
   *
   * can't use std::get_time() here, since this is not supported by
   * older g++ versions < 5 (CentOS 7, i'm looking at you)
   *
   * PostgreSQL's day 0 is 2000-01-01, and this is also
   * expected in the streaming protocol messages.
   */
  strptime("2000-01-01 00:00:00+02", "%Y-%m-%d %H:%M:%S%z", &epoch_ts);

  this->xlogPos_written = written;
  this->xlogPos_flushed = flushed;
  this->xlogPos_applied = applied;

  auto epoch_us
    = std::chrono::high_resolution_clock::from_time_t(std::mktime(&epoch_ts));
  auto current_us
    = std::chrono::high_resolution_clock::from_time_t(std::mktime(&current_ts));
  auto diff_us
    = std::chrono::duration_cast<std::chrono::microseconds>(current_us - epoch_us);
  this->current_time_us = diff_us.count();

}

/*****************************************************************************
 * HotStandbyFeedbackMessage
 *****************************************************************************/

HotStandbyFeedbackMessage::HotStandbyFeedbackMessage(PGconn *prepared_connection)
  : FeedbackMessage(prepared_connection) {

  this->kind = 'h';

}

HotStandbyFeedbackMessage::HotStandbyFeedbackMessage(PGconn *prepared_connection,
                                                     unsigned long long wal_segment_size)
  : FeedbackMessage(prepared_connection, wal_segment_size) {

  this->kind = 'h';

}

HotStandbyFeedbackMessage::~HotStandbyFeedbackMessage() {}

void HotStandbyFeedbackMessage::send() {

  throw XLOGMessageFailure("not implemend yet");

  if (this->connection == NULL
      && (PQstatus(this->connection) != CONNECTION_OK))
    throw XLOGMessageFailure("attempt to send status reply to disconnected server");

}

/*****************************************************************************
 * PrimaryFeedbackMessage
 *****************************************************************************/

PrimaryFeedbackMessage::PrimaryFeedbackMessage(PGconn *prepared_connection)
  : XLOGStreamMessage(prepared_connection) {

  this->requestResponse = false;
  this->kind = 'k';
}

PrimaryFeedbackMessage::PrimaryFeedbackMessage(PGconn *prepared_connection,
                                               unsigned long long wal_segment_size)
  : XLOGStreamMessage(prepared_connection, wal_segment_size) {

  this->requestResponse = false;
  this->kind = 'k';

}

PrimaryFeedbackMessage::~PrimaryFeedbackMessage() {}

PrimaryFeedbackMessage& PrimaryFeedbackMessage::operator<<(MemoryBuffer &srcbuffer) {

  this->assign(srcbuffer);
  return *this;

}

XLogRecPtr PrimaryFeedbackMessage::getXLOGServerPos() {

  return this->xlogserverendpos;

}

uint64 PrimaryFeedbackMessage::getServerTime() {

  return this->xlogservertime;

}

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
    throw XLOGMessageFailure("input buffer does not look like a primary status message");
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

  if (mybuffer[17] == '1') {
    this->wantsResponse();
  }

}
