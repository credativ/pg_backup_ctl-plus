#include <xlogdefs.hxx>

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

/* ****************************************************************************
 * XLOGDataStreamMessage - XLOG Buffer data from stream
 * ****************************************************************************/

XLOGDataStreamMessage::XLOGDataStreamMessage(PGconn *prepared_connection)
  : XLOGStreamMessage(prepared_connection) {

  this->kind = 'w';

}

XLOGDataStreamMessage::~XLOGDataStreamMessage() {}

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

}
