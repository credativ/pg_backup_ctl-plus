#include <stream.hxx>
#include <backup.hxx>
#include <backupprocesses.hxx>
#include <xlogdefs.hxx>
#include <proto-buffer.hxx>
#include <boost/log/trivial.hpp>

#include <stack>

/* Required for select() */
extern "C" {
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
}

using namespace pgbckctl;

/******************************************************************************
 * Implementation of WALStreamerProcess
 ******************************************************************************/

WALStreamerProcess::WALStreamerProcess(PGconn *prepared_connection,
                                       StreamIdentification streamident) {

  this->current_state = ARCHIVER_STARTUP;
  this->pgconn        = prepared_connection;
  this->streamident   = streamident;
  this->stopHandler   = nullptr;

  /*
   * prepared_connection needs to be a connected
   * libpq connection, otherwise we have to error out.
   */
  if (!((this->pgconn != NULL)
        && (PQstatus(this->pgconn) == CONNECTION_OK)))
    throw StreamingFailure("WAL stream not connected");
}

WALStreamerProcess::~WALStreamerProcess() {

  /*
   * Don't close the inherited PostgreSQL connection here.
   *
   * We are expecting to operate as a sub
   * on a calling PGStream handle, which
   * does all the legwork for us.
   */
}

ArchiverState WALStreamerProcess::reason() {
  return this->current_state;
}

ArchiverState WALStreamerProcess::receivePoll() {

  /*
   * Polling code like src/bin/pg_basebackup/receivelog.c
   * is implemented. See also
   *
   * https://www.postgresql.org/docs/10/static/libpq-example.html
   *
   * for an example.
   */

  int result;
  fd_set input_mask;
  int maxfd;
  timeval select_timeout;
  timeval *select_timeoutptr = NULL;

  /*
   * We need the PG socket to operate on.
   */
  int serversocket = PQsocket(this->pgconn);

  if (serversocket < 0) {
    std::ostringstream oss;
    oss << "error polling on server connection: "
        << PQerrorMessage(this->pgconn);
    throw StreamingFailure(oss.str());
  }

  FD_ZERO(&input_mask);
  FD_SET(serversocket, &input_mask);
  maxfd = serversocket;
  this->timeoutSelectValue(&select_timeout);

  /*
   * Calculate timeout value.
   */
  if (this->timeout > 0) {
    select_timeoutptr = &select_timeout;
  }

  result = ::select(maxfd + 1, &input_mask, NULL, NULL, select_timeoutptr);

  /*
   * Checkout what happened to select() after returning.
   */
  if (result < 0) {
    /*
     * A negative return code here can also be originated
     * from a EINTR, check. Otherwise we bail out hard.
     */
    if (errno == EINTR) {
      /* Archiver stream is interrupted by signal */
      return ARCHIVER_STREAMING_INTR;
    }

    /* ... else this is really an error. */
    return ARCHIVER_STREAMING_ERROR;
  }

  if ( (result > 0)
       && (FD_ISSET(serversocket, &input_mask)) ) {
    /* data is available */
    return ARCHIVER_STREAMING;
  }

  /* timeout on waiting for data */
  return ARCHIVER_STREAMING_TIMEOUT;
}

void WALStreamerProcess::timeoutSelectValue(timeval *timeoutptr) {

  if (timeoutptr == NULL) {
    return;
  } else {

    timeoutptr->tv_sec = this->timeout / 1000L;
    timeoutptr->tv_usec = (this->timeout % 1000L) / 1000L;

  }

}

ArchiverState WALStreamerProcess::sendStatusUpdate() {

  ReceiverStatusUpdateMessage rsum(this->pgconn);

#ifdef __DEBUG_XLOG__
  BOOST_LOG_TRIVIAL(debug) << " ... sending status update to primary ";
  BOOST_LOG_TRIVIAL(debug) << "     -> write position: "
                           << PGStream::encodeXLOGPos(this->streamident.write_position);
  BOOST_LOG_TRIVIAL(debug) << "     -> flush position: "
                           << PGStream::encodeXLOGPos(this->streamident.flush_position);
  BOOST_LOG_TRIVIAL(debug) << "     -> last reported flush position: "
                           << PGStream::encodeXLOGPos(this->streamident.last_reported_flush_position);
  BOOST_LOG_TRIVIAL(debug) << "     -> apply position: "
                           << PGStream::encodeXLOGPos(this->streamident.apply_position);
#endif

  if (this->streamident.flush_position == InvalidXLogRecPtr) {

    rsum.setStatus(this->streamident.write_position,
                   this->streamident.last_reported_flush_position,
                   this->streamident.apply_position);

  } else {

    rsum.setStatus(this->streamident.write_position,
                   this->streamident.flush_position,
                   this->streamident.apply_position);

  }

  /* Always report flush position back to upstream */
  rsum.reportFlushPosition();

  rsum.send();
  return this->current_state;

}

ArchiverState WALStreamerProcess::handleReceive(char **buffer, int *bufferlen) {

  /* Holds length of incoming buffer data */
  ArchiverState status = ARCHIVER_STREAMING_ERROR;

  /* bufferlen should be not null! */
  if (bufferlen == NULL) {
    throw StreamingFailure("buffer length attribute cannot be NULL");
  }

  if (*buffer != NULL) {
    PQfreemem(*buffer);
  }

  /* make sure buffer is NULL */
  *buffer = NULL;

  /* Get input data, if any */
  *bufferlen = PQgetCopyData(this->pgconn, buffer, 1);

  if (*bufferlen == 0) {

    /* no data yet, poll on input from WAL sender */
    if (buffer != NULL) {
      PQfreemem(*buffer);
      *buffer = NULL;
    }

    status = this->current_state = this->receivePoll();

    /* check for errors, timeout et al. */
    if ( (status != ARCHIVER_STREAMING)
         && (status != ARCHIVER_STREAMING_INTR) ) {
      return status;
    }

    /* Consume input */
    if (PQconsumeInput(this->pgconn) == 0) {

      /*
       * Okay, there's some trouble here, get the error
       * and bail out hard.
       */
      status = this->current_state = ARCHIVER_STREAMING_ERROR;

    }

    /* Read data into buffer, again */
    *bufferlen = PQgetCopyData(this->pgconn, buffer, 1);

    /*
     * If there's still zero bytes just return ARCHIVER_STREAMING_NO_DATA.
     */
    if (*bufferlen == 0) {
      status = this->current_state = ARCHIVER_STREAMING_NO_DATA;
    }

    /*
     * In case the connection was terminated, check
     * for a buffer length set to < -1.
     */
    if (*bufferlen <= -1) {
      status = this->current_state = ARCHIVER_END_POSITION;
    }

  } else if (*bufferlen == -1) {

    /*
     * Indicate end-of-stream, either shutdown
     * or end of current timeline
     */
    status = this->current_state = ARCHIVER_END_POSITION;

  } else if (*bufferlen == -2) {

    /* Oops, something went wrong here */
    status = this->current_state = ARCHIVER_STREAMING_ERROR;

  } else if (*bufferlen > 0) {

    status = this->current_state = ARCHIVER_STREAMING;

  }

  return status;
}

std::string WALStreamerProcess::xlogpos() {

  if (this->current_state != ARCHIVER_STREAMING_ERROR)
    return this->streamident.xlogpos;
  else
    throw StreamingFailure("attempt to query xlog position from wal streamer not in streaming state");

}

void WALStreamerProcess::setBackupHandler(std::shared_ptr<TransactionLogBackup> backupHandler) {

  this->backupHandler = backupHandler;

}

void WALStreamerProcess::handleMessage(XLOGStreamMessage *message) {

  char msgType;

  /*
   * message shouldn't be a nullptr...
   */
  if (message == nullptr)
    return;

  msgType = message->what();

  /*
   * WAL data (XLOGDataStreamMessage object) message should update xlogpos
   */
  switch(msgType) {
  case 'w':
    {
      XLOGDataStreamMessage *datamsg = dynamic_cast<XLOGDataStreamMessage *>(message);
      XLogRecPtr serverpos;

      this->streamident.xlogpos
        = PGStream::encodeXLOGPos(datamsg->getXLOGStartPos());

#ifdef __DEBUG_XLOG__
      BOOST_LOG_TRIVIAL(debug) << "NEW XLOG POS " << this->streamident.xlogpos;
#endif

      /* get server position, reported by the connected WAL sender */
      serverpos = datamsg->getXLOGServerPos();

#ifdef __DEBUG_XLOG__
      BOOST_LOG_TRIVIAL(debug) << "SERVER XLOG POS "
                               << PGStream::encodeXLOGPos(serverpos);
#endif
      this->streamident.server_position = serverpos;

      /*
       * Update XLOG write position and, if flushed, also
       * the flush position.
       *
       * NOTE: If no backup handler was configured,
       *       Flush is currently the same as the
       *       current write position.
       *
       * Our backup handler does all the necessary work
       * to spool the transaction logs into the archive.
       */
      this->streamident.write_position = datamsg->getXLOGServerPos();
      this->streamident.updateStartSegmentWriteOffset();

      if (this->backupHandler != nullptr) {

        /*
         * Everything should be in shape so far. A current
         * WAL segment file should be allocated and we can go
         * forward and spool the XLOG message data block into that
         * current WAL file. Though, we must take care for
         * a WAL segment boundary, in that case we have to switch to
         * new WAL segment file, again.
         *
         * We must remember the current flush position in
         * last_reported_flush_position, since we want to continously
         * report them back to upstream. In case no XLOG switch happened,
         * the backuphandler write() method will set an InvalidXLogRecPtr!
         */

        this->streamident.write_position
          = this->backupHandler->write(datamsg,
                                       this->streamident.flush_position,
                                       this->streamident.timeline);

        if (this->streamident.flush_position != InvalidXLogRecPtr) {

          this->streamident.last_reported_flush_position
            = this->streamident.flush_position;

        }

#ifdef __DEBUG_XLOG__
        BOOST_LOG_TRIVIAL(debug) << "DEBUG: flush position after write(): "
                                 << PGStream::encodeXLOGPos(this->streamident.flush_position);
#endif

      } else { /* if ... backupHandler != nullptr */

        this->streamident.flush_position = datamsg->getXLOGServerPos();
        this->streamident.last_reported_flush_position
          = this->streamident.flush_position;

      }

      break;
    }

  case 'k':
    {
      PrimaryFeedbackMessage *pm = dynamic_cast<PrimaryFeedbackMessage *>(message);

#ifdef __DEBUG_XLOG__
      BOOST_LOG_TRIVIAL(debug) << "primary feedback message";
#endif

      /*
       * Upstream wants an immediate status update?
       */
      if (pm->responseRequested()) {

#ifdef __DEBUG_XLOG__
        BOOST_LOG_TRIVIAL(debug) << "receiver status update forced";
#endif

        /* ...send it */
        this->sendStatusUpdate();

        /*
         * Update timeout interval.
         */
        this->last_status_update = CPGBackupCtlBase::current_hires_time_point();

      }

      break;

    }
  default:
    {
      /* unhandled message type */
#ifdef __DEBUG_XLOG__
      BOOST_LOG_TRIVIAL(debug) << "unhandled message type " << msgType;
#endif
      break;
    }

  } /* switch...msgType */

}

void WALStreamerProcess::setReceiverStatusTimeout(long value) {

  /*
   * We don't allow values lower than ower
   * fixed internal select() timeout.
   */
  if (value < this->timeout) {
    std::ostringstream oss;

    oss << "receiver status timeout cannot be lower than " << this->timeout;
    throw StreamingFailure(oss.str());
  }

  this->receiver_status_timeout = value;

}

bool WALStreamerProcess::receive() {

  bool can_continue = false;
  char *buffer = NULL; /* temporary recv buffer, handled by libpq */
  int bufferlen = 0;
  ArchiverState reason;

  /*
   * If we're not in streaming state, handle the state
   * accordingly.
   */
  if (this->current_state == ARCHIVER_STREAMING_ERROR) {
    /* error condition on streaming connection, abort */
    throw StreamingFailure("error on streaming connection");
  }

  if (this->current_state == ARCHIVER_STARTUP) {
    throw StreamingFailure("attempt to stream from uninitialized streaming handle, call start() before");
  }

  if (this->current_state == ARCHIVER_END_POSITION) {
    throw StreamingFailure("new log segment requested, use finalize() instead");
  }

  BOOST_LOG_TRIVIAL(info) << "entering WAL streaming receive() ";

  /*
   * Initialize status update start interval.
   */
  this->last_status_update = CPGBackupCtlBase::current_hires_time_point();

  this->current_state = reason = this->handleReceive(&buffer, &bufferlen);
  while (reason != ARCHIVER_END_POSITION
         && reason != ARCHIVER_SHUTDOWN
         && reason != ARCHIVER_STREAMING_ERROR
         && reason != ARCHIVER_TIMELINE_SWITCH
         && reason != ARCHIVER_STARTUP
         && reason != ARCHIVER_START_POSITION) {

    XLOGStreamMessage *message = nullptr;

    /**
     * Before doing anything, check whether our stop
     * handler wants to exit.
     */
    if (this->stopHandlerWantsExit()) {
      reason = this->current_state = ARCHIVER_SHUTDOWN;
      this->streamident.status = StreamIdentification::STREAM_PROGRESS_SHUTDOWN;
      break;
    }

    /*
     * Next we check whether we should send a status update message
     * to upstream. This is always the case if our internal
     * timeout value forces us to do.
     *
     * Since we also poll on the socket with a specific timeout, we
     * also recognize this timeout value here.
     */
    if (CPGBackupCtlBase::calculate_duration_ms(this->last_status_update
                                                - std::chrono::milliseconds(this->timeout),
                                                CPGBackupCtlBase::current_hires_time_point())
        >= std::chrono::milliseconds(this->receiver_status_timeout)) {

#ifdef __DEBUG_XLOG__
      BOOST_LOG_TRIVIAL(debug) << "standby status update overdue";
#endif

      this->sendStatusUpdate();
      this->last_status_update = CPGBackupCtlBase::current_hires_time_point();

    }

    /*
     * If not streaming, don't try to handle XLOG messages.
     */
    if (reason != ARCHIVER_STREAMING) {

      /*
       * Before trying next, copy local buffer ...
       */
      if (buffer != NULL) {
        PQfreemem(buffer);
        buffer = NULL;
      }

      /* ..next try */
      reason = this->handleReceive(&buffer, &bufferlen);
      continue;
    }

    /*
     * Interpret buffer, create a corresponding WAL message object.
     */
    this->receiveBuffer.allocate(bufferlen);
    this->receiveBuffer.write(buffer, bufferlen, 0);

    /*
     * Free copy buffers, they are copied over
     * into our local receive buffer, so we don't
     * need them anymore.
     */
    if (buffer != NULL) {
      PQfreemem(buffer);
      buffer = NULL;
    }

    message = XLOGStreamMessage::message(this->pgconn,
                                         this->receiveBuffer,
                                         this->streamident.wal_segment_size);

#ifdef __DEBUG_XLOG__
    BOOST_LOG_TRIVIAL(debug) << "write XLOG message ";
#endif

    try {
      if (message != nullptr) {

        this->handleMessage(message);

      }
    } catch(CPGBackupCtlFailure &e) {
      if (message != nullptr)
        delete message;

      throw e;
    }

    delete message;

    /* ..next try */
    this->current_state = reason = this->handleReceive(&buffer, &bufferlen);

  }

  /*
   * Internal handleReceive() loop exited, check
   * further actions depending on current state.
   */
  if (this->current_state == ARCHIVER_STREAMING_NO_DATA) {

    BOOST_LOG_TRIVIAL(warning) << "no data, continue ... ";
    can_continue = true;

  }

  if (this->current_state == ARCHIVER_STREAMING_ERROR) {

    BOOST_LOG_TRIVIAL(error) << "failure on PQgetCopyData(): "
                             << PQerrorMessage(this->pgconn);
    can_continue = false;

  }

  /*
   * Handle end-of-copy conditions.
   *
   * This could either a controlled shutdown
   * of the server or we reached the end of the current
   * timeline.
   */
  if (this->current_state == ARCHIVER_END_POSITION) {

    PGresult *pgres = this->handleEndOfStream();

    if (this->current_state == ARCHIVER_TIMELINE_SWITCH) {

      BOOST_LOG_TRIVIAL(info) << "end of archive stream because of timeline switch";

      /*
       * This is a timeline switch. Handle it accordingly.
       */
      this->endOfStreamTimelineSwitch(pgres,
                                      this->streamident.timeline,
                                      this->streamident.xlogpos);

      BOOST_LOG_TRIVIAL(debug) << "timeline switch to TLI="
                               << this->streamident.timeline
                               << ", XLOGPOS="
                               << this->streamident.xlogpos;

      PQclear(pgres);
      can_continue = true;

    } else if (this->current_state == ARCHIVER_SHUTDOWN) {

      BOOST_LOG_TRIVIAL(info) << "end of archive stream, shutting down";
      /*
       * Server side has terminated, schedule own shutdown accordingly.
       */
      can_continue = false;
    }

  } else if (this->current_state == ARCHIVER_SHUTDOWN) {

    PGresult *res = NULL;

    BOOST_LOG_TRIVIAL(info) << "forced archive stream shutdown";

    /*
     * Inner loop recognized a shutdown request and set the
     * current state to ARCHIVER_SHUTDOWN.
     */
    if ((res != NULL) && (PQresultStatus(res) == PGRES_COPY_IN))
        this->end();

    can_continue = false;

  }

  return can_continue;
}

XLogRecPtr WALStreamerProcess::getCurrentXLOGPos() {
  return this->streamident.write_position;
}

unsigned int WALStreamerProcess::getCurrentTimeline() {
  return this->streamident.timeline;
}

void WALStreamerProcess::endOfStreamTimelineSwitch(PGresult *result,
                                                   unsigned int& timeline,
                                                   std::string& xlogpos) {

  if (this->current_state != ARCHIVER_TIMELINE_SWITCH) {
    throw StreamingFailure("timeline switch requested on unsupported state in streaming connection");
  }

  if (result == NULL) {
    throw StreamingFailure("could not get timeline switch data from streaming connection: result is undefined");
  }

  /*
   * Get timeline and xlogpos from the next timeline.
   */
  if (PQnfields(result) < 2
      || PQntuples(result) != 1) {
    std::ostringstream oss;
    oss << "unexpected result after timeline switch: got "
        << "columns " << PQnfields(result) << " expected " << 2 << ", "
        << "tuples " << PQntuples(result) << "expected " << 1;
    throw StreamingFailure(oss.str());
  }

  /*
   * Read timeline
   */
  timeline = CPGBackupCtlBase::strToInt(PQgetvalue(result, 0, 0));

  /*
   * The XLOG position returned by the result set is encoded. We just
   * return the encoded position, the caller is responsible to encode it
   * into a proper representation.
   */
  xlogpos = PQgetvalue(result, 0, 1);

  /* and we're done */

}

PGresult *WALStreamerProcess::handleEndOfStream() {

  PGresult *result = NULL;

  if (this->current_state != ARCHIVER_END_POSITION) {
    throw CPGBackupCtlFailure("could not call handleEndOfStream() on unterminated WAL stream");
  }

  result = PQgetResult(this->pgconn);

  /*
   * Check whether we are still in COPY IN mode.
   */
  if (result != NULL && (PQresultStatus(result) == PGRES_COPY_IN)) {

    PQclear(result);

    /*
     * Finalize the COPY IN mode on our side. Iff the server is
     * still alive this will succeed.
     *
     * We need to ackknowledge this case in order to get the
     * timeline switch, if any in progress.
     *
     * end() will set the internal Archiver state to ARCHIVER_SHUTDOWN
     * in case of success, but we need to check if we really want
     * to have a shutdown or to continue. This is indicated by
     * a PGRES_TUPLES_OK or PGRES_COMMAND_OK return flag
     * by the returned PGresult object. Check that and set the
     * flag accordingly.
     *
     * PGGRES_TUPLES_OK means that the server had acknowledged our
     * end of stream message, but just want to switch its timeline
     *
     * PGRES_COMMAND_OK is set in case the server is commanded to
     * shutdown.
     *
     * Set the archiver state according to the examined results above.
     *
     * NOTE: end() may throw an exception in case the COPY stream
     *       couldn't be terminated properly.
     */
    try {

      BOOST_LOG_TRIVIAL(debug) << "end of COPY mode, telling upstream to quit";

      result = this->end();

    } catch(StreamingFailure &e) {

      /*
       * WALStreamer::end() might throw, make sure we don't leak
       * PQresult references
       */
      if (result != NULL)
        PQclear(result);

      result = NULL;

      this->current_state = ARCHIVER_STREAMING_ERROR;
      throw e;

    }

  } else {

    /*
     * Reaching this point usually means that the
     * server has disconnected, most likely without having established
     * a working streaming connection before.
     *
     * We now force a WAL Streamer shutdown without sending the
     * end-of-stream message.
     */
    this->current_state = ARCHIVER_SHUTDOWN;

  }

  return result;
}

StreamIdentification WALStreamerProcess::identification() {
  return this->streamident;
}

PGresult *WALStreamerProcess::end() {

  PGresult *result = NULL;

  if (PQputCopyEnd(this->pgconn, NULL) <= 0 || PQflush(this->pgconn)) {

    std::ostringstream oss;

    oss << "could not terminate COPY in progress: "
        << PQerrorMessage(this->pgconn);
    this->current_state = ARCHIVER_STREAMING_ERROR;
    throw StreamingFailure(oss.str());

  }

  /*
   * Make sure we indicate success
   *
   * We must tell the caller whether this was a graceful shutdown
   * or a timeline switch.
   */

  result = PQgetResult(this->pgconn);

  if (result != NULL && ((PQresultStatus(result) == PGRES_TUPLES_OK))) {

    this->current_state = ARCHIVER_TIMELINE_SWITCH;
    this->streamident.status = StreamIdentification::STREAM_PROGRESS_TIMELINE_SWITCH;

  } else if (result != NULL && ((PQresultStatus(result) == PGRES_COMMAND_OK))) {

    this->current_state = ARCHIVER_SHUTDOWN;
    this->streamident.status = StreamIdentification::STREAM_PROGRESS_SHUTDOWN;

  } else {

    std::ostringstream oss;

    oss << "could not terminate COPY in progress: "
        << PQerrorMessage(this->pgconn);
    this->current_state = ARCHIVER_STREAMING_ERROR;
    throw StreamingFailure(oss.str());

  }

  return result;

}

void WALStreamerProcess::finalizeSegment() {

  if (this->current_state != ARCHIVER_END_POSITION) {
    throw StreamingFailure("cannot finalize current transaction log segment");
  }

}

void WALStreamerProcess::start() {

  /*
   * NOTE: we expect a valid initialized Stream Identification handle,
   *       holding the starting XLOG position and a valid replication
   *       slot identifier.
   */
  std::ostringstream query;

  /*
   * buffer for escaped identifiers.
   */
  char escapedLabel[MAXPGPATH];
  char escapedXlogPos[MAXPGPATH];
  int escape_error;

  /* Result from START_REPLICATION command */
  PGresult *res;

  /*
   * If the ArchiveState is already set to ARCHIVER_STREAMING, calling
   * start() is supposed to be an error (we indeed already seem to
   * have issued a start() command, so there's no reason to stack them here.
   * Indicate the state of the WALStreamer to be not correct in this case
   * and throw an error.
   *
   * Also, if the current_state is set to ARCHIVE_END_POSITION we need to
   * handle a graceful streaming connection shutdown, so stop() is
   * required before calling start(). Other states are just handled by
   * receive().
   */
  if (this->current_state == ARCHIVER_STREAMING
      || this->current_state == ARCHIVER_END_POSITION) {
    throw StreamingFailure("invalid call to start() in archiver: already started");
  }

  /*
   * XXX: slot_name and xlogpos are incorporated as
   *      strings here, so we might have to deal with
   *      injection attempts here.
   *
   *      Normally they're comming from a PGStream
   *      object instance, but be paranoid here.
   */
  PQescapeStringConn(this->pgconn,
                     escapedLabel,
                     this->streamident.slot_name.c_str(),
                     this->streamident.slot_name.length(),
                     &escape_error);

  PQescapeStringConn(this->pgconn,
                     escapedXlogPos,
                     this->streamident.xlogpos.c_str(),
                     this->streamident.xlogpos.length(),
                     &escape_error);

  query << "START_REPLICATION SLOT "
        << escapedLabel
        << " PHYSICAL "
        << escapedXlogPos
        << " TIMELINE "
        << this->streamident.timeline
        << ";";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "WALStreamer start() query: " << query.str();
#endif

  /*
   * Fire the query ...
   */
  res = PQexec(this->pgconn, query.str().c_str());

  if (PQresultStatus(res) != PGRES_COPY_BOTH) {

    std::ostringstream oss;
    oss << "START_REPLICATION command failed: "
        << PQerrorMessage(this->pgconn);

    /* Don't leak result set */
    PQclear(res);
    throw StreamingFailure(oss.str());

  }

  PQclear(res);

  /*
   * If everything went smoothly, change internal state
   * to ARCHIVER_STREAMING, which enables the caller
   * to use receive()...
   */
  this->current_state = ARCHIVER_STREAMING;
  this->streamident.status = StreamIdentification::STREAM_PROGRESS_STREAMING;

}

/* ****************************************************************************
 * Implementation TablespaceQueue
 ******************************************************************************/

TablespaceQueue::TablespaceQueue(PGconn *conn) {

  this->conn = conn;

}

BaseBackupState TablespaceQueue::getTablespaceInfo(BaseBackupState &state) {

  PGresult *res;
  int i;
  ExecStatusType es;

  /*
   * Stack tablespace meta info only if
   * successfully started.
   */
  if (state != BASEBACKUP_STARTED)
    throw StreamingFailure("reading tablespace meta information requires a started base backup process");

  state = BASEBACKUP_TABLESPACE_META;

  /*
   * Read tablespace meta info from result set.
   */
  res = PQgetResult(this->conn);

  if ((es = PQresultStatus(res)) != PGRES_TUPLES_OK) {
    std::string sqlstate(PQresultErrorField(res, PG_DIAG_SQLSTATE));
    std::ostringstream oss;

    oss << "could not read tablespace meta info: "
        << PQresultErrorMessage(res);
    PQclear(res);
    throw StreamingExecutionFailure(oss.str(), es, sqlstate);
  }

  if (PQntuples(res) < 1) {
    PQclear(res);
    throw StreamingFailure("unexpected number of tuples for tablespace meta info");
  }

  if (PQnfields(res) != 3) {
    std::ostringstream oss;
    oss << "unexpected number of fields for tablespace meta info, expected 3, got "
        << PQnfields(res);
    PQclear(res);
    throw StreamingFailure(oss.str());
  }

  for (i = 0; i < PQntuples(res); i++) {

    std::shared_ptr<BackupTablespaceDescr> descr = std::make_shared<BackupTablespaceDescr>();

    /*
     * Tablespace OID
     */
    descr->spcoid = CPGBackupCtlBase::strToUInt(std::string(PQgetvalue(res, i, 0)));

    /*
     * Tablespace location (path, can be NULL in case of base directory)
     */
    if (!PQgetisnull(res, i, 1))
      descr->spclocation = std::string(PQgetvalue(res, i, 1));
    else
      descr->spclocation = "";

    /*
     * Tablespace size.
     *
     * Since we rely on PROGRESS in the BASE_BACKUP command, this is
     * assumed to be never NULL!
     */
    descr->spcsize = CPGBackupCtlBase::strToInt(std::string(PQgetvalue(res, i, 2)));

    /*
     * Push the new tablespace descriptor at the end of our internal
     * tablespace queue. Please note that the order of the tablespaces
     * retrieved via BASE_BACKUP does matter, so we use a FIFO concept here.
     */
    tablespaces.push(descr);
  }

  /*
   * Free result set
   */
  PQclear(res);

  /*
   * okay, looks like all meta information is there...
   */
  state = BASEBACKUP_TABLESPACE_READY;
  return state;

}

/* ****************************************************************************
 * Implementation TablespaceStreamer
 ******************************************************************************/

TablespaceStreamer::TablespaceStreamer(std::shared_ptr<StreamBaseBackup> backupHandle,
                                       PGconn *conn)
                                       : TablespaceQueue(conn) {

  this->conn = conn;
  this->backupHandle = backupHandle;

}

BaseBackupState TablespaceStreamer::getState() {

  return this->current_state;

}

void TablespaceStreamer::manifest() {

  std::shared_ptr<BackupFile> manifest_file = nullptr;
  bool interrupted = false;
  char *copybuf = NULL;
  PGresult *res;
  int rc;

  if (!backupHandle->isInitialized())
    throw StreamingFailure("could not receive backup manifest");

  if (current_state != BASEBACKUP_EOB)
    throw StreamingFailure("unexpected state while retrieving backup manifest");

  /* Should have a valid result set with copy data */
  res = PQgetResult(conn);

  if (PQresultStatus(res) != PGRES_COPY_OUT) {
    std::ostringstream oss;

    oss << "could not get COPY data for backup manifest: ";
    oss << PQerrorMessage(conn);
    PQclear(res);
    throw StreamingFailure(oss.str());

  }

  /*
   * Make the manifest file handle.
   *
   * The contents of a manifest are plain, so we have to
   * make sure we get the right compression type and reset
   * it to the former one to not confuse our callers.
   */
  BackupProfileCompressType former_compression = backupHandle->getCompression();
  backupHandle->setCompression(BACKUP_COMPRESS_TYPE_NONE);
  manifest_file = backupHandle->stackFile("backup_manifest");
  backupHandle->setCompression(former_compression);

  while(true) {

    if (copybuf != NULL) {
      PQfreemem(copybuf);
      copybuf = NULL;
    }

    rc = PQgetCopyData(conn, &copybuf, 0);

    if (rc == -1) {
      /* end of copy data */
      break;
    } else if (rc == -2) {
      std::ostringstream oss;

      oss << "could not read manifest COPY data: ";
      oss << PQerrorMessage(conn);
      throw StreamingFailure(oss.str());

    }

    manifest_file->write(copybuf, rc);

    /*
     * Check if we are requested to stop.
     *
     * If true, we remember that we were interrupted.
     */
    if (this->stopHandlerWantsExit()) {
      interrupted = true;
      break;
    }

  }

  /*
   * Receiving the manifest is the last action during base backups,
   * mark the internal start to finish backup.
   */
  if (!interrupted)
    current_state = BASEBACKUP_EOB;
  else
    current_state = BASEBACKUP_MANIFEST_INTERRUPTED;

}

void TablespaceStreamer::tablespace(std::shared_ptr<BackupElemDescr> &descr) {

  char *copybuf = nullptr;
  bool interrupted = false;
  int rc;

  while(true) {

    if (copybuf != nullptr) {
      PQfreemem(copybuf);
      copybuf = nullptr;
    }

    rc = PQgetCopyData(this->conn, &copybuf, 0);

    if (rc == -1) {
      /*
       * End of COPY data stream.
       *
       * PostgreSQL sends a tar format stream as defined in
       * POSIX 1003.1-2008 standard, but omits the last two trailing
       * 512-bytes zero chunks.
       */
      char zerochunk[1024];
      memset(zerochunk, 0, sizeof(zerochunk));

      this->stepInfo.file->write(zerochunk, sizeof(zerochunk));
      PQfreemem(copybuf);
      break;
    }

    /*
     * ... else write the chunk to the file.
     *
     * NOTE: We don't need to free the copy buffer here,
     *       since we go through the next loop where PQfreemem()
     *       will do this when necessary.
     */
    this->stepInfo.file->write(copybuf, rc);

    /*
     * Check if we are requested to stop.
     *
     * If true, we remember that we were interrupted.
     */
    if (this->stopHandlerWantsExit()) {
      interrupted = true;
      break;
    }
  }

  /*
   * Mark this tablespace as ready, but only in case we weren't
   * interrupted.
   */
  if (!interrupted)
    this->current_state = BASEBACKUP_STEP_TABLESPACE;
  else
    this->current_state = BASEBACKUP_STEP_TABLESPACE_INTERRUPTED;

}

bool TablespaceStreamer::next(std::shared_ptr<BackupElemDescr> &next) {

  std::shared_ptr<BackupTablespaceDescr> descr = nullptr;

  if (tablespaces.empty()) {
    this->current_state = BASEBACKUP_EOB;
    next = nullptr;
    return false;
  }

  /*
   * backupHandle must be initialized.
   */
  if (!backupHandle->isInitialized())
    throw StreamingFailure("cannot write into uninitialized streaming backup handle");

  /*
   * Check, if we are starting to step into a fresh
   * tablespace backup. This is indicated by an internal
   * state of -1 stored in the stepInfo struct property.
   *
   * In case of -1, we need to initialize the PGresult
   * handle of the current stepping.
   */
  this->incr();

  /*
   * Dequeue the current tablespace from the
   * internal queue, if any.
   */
  this->stepInfo.descr = tablespaces.front();
  tablespaces.pop();
  this->stepInfo.handle = PQgetResult(this->conn);

  if (PQresultStatus(this->stepInfo.handle) != PGRES_COPY_OUT) {

    std::ostringstream oss;
    oss << "could not get COPY data stream: " << PQerrorMessage(this->conn);
    throw StreamingFailure(oss.str());

  }

  next = descr = this->stepInfo.descr;

  /*
   * Make the file access handle.
   */
  this->stepInfo.file = backupHandle->stackFile(CPGBackupCtlBase::intToStr(descr->spcoid)
                                                + ".tar");

  if (descr->spclocation == "")
    this->current_state = BASEBACKUP_STEP_TABLESPACE_BASE;
  else
    this->current_state = BASEBACKUP_STEP_TABLESPACE;

  return true;

}

/******************************************************************************
 * Implementation of TablespaceIterator
 ******************************************************************************/

size_t TablespaceIterator::consumed() {
  return _consumed;
}

void TablespaceIterator::setConsumed(size_t bytes_consumed) {
  this->_consumed += bytes_consumed;
}

void TablespaceIterator::reset() {
  stepInfo.reset();
}

void TablespaceIterator::incr() {
  this->stepInfo.current_step++;
}

/******************************************************************************
 * Implementation of MessageStreamer
 ******************************************************************************/

MessageStreamer::MessageStreamer(std::shared_ptr<StreamBaseBackup> sb,
                                 PGconn *pgconn) : TablespaceQueue(pgconn) {

  backupHandle = sb;
  conn = pgconn;

}

void MessageStreamer::data(std::shared_ptr<BaseBackupMessage> &msg) {

  /*
   * Protocol state must be either BASEBACKUP_TABLESPACE_STREAM or
   * BASEBACKUP_MANIFEST_STREAM
   */
  if ( (current_state != BASEBACKUP_TABLESPACE_STREAM)
       && (current_state != BASEBACKUP_MANIFEST_STREAM) ) {

    throw StreamingFailure("invalid basebackup streaming state");

  }

  /*
   * This message should contain data for either manifest
   * or archive. We don't care here about its type at the moment, so
   * just write out its contents
   */
  stepInfo.file->write(msg->data(), msg->dataSize());

}

void MessageStreamer::startCopyStream() {

  PGresult *res = PQgetResult(conn);

  if (PQresultStatus(res) != PGRES_COPY_OUT) {
    std::ostringstream errormsg;

    errormsg << "not in COPY mode: " << PQerrorMessage(conn);
    throw StreamingFailure(errormsg.str());
  }

  PQclear(res);

}

bool MessageStreamer::next(std::shared_ptr<BackupElemDescr> &next) {

  bool result      = false;

  int rc; /* return code for PQgetCopyData() */

  /**
   * Entering streaming loop. We process the whole message according to
   * its kind. If done, we handle the content and return a proper BackupElemDescr.
   */
  while (true) {

    std::shared_ptr<BaseBackupMessage> msg = nullptr;
    char *copy_buffer                      = nullptr;

    if (stopHandlerWantsExit()) {
      current_state = BASEBACKUP_STEP_TABLESPACE_INTERRUPTED;
      result = false;
      break;
    }

    rc = PQgetCopyData(this->conn, &copy_buffer, 0);

    /*
     * PQgetCopyData() either returns -1 in case COPY has finished. There is no 0
     * return expected, since that would mean we're in async mode waiting for additional
     * data/start of data stream, which we're effectively aren't. -2 indicates an error, so
     * throw accordingly.
     */

    if (rc == -2) {
      std::ostringstream errormsg;

      errormsg << "error streaming basebackup: " << PQerrorMessage(conn);
      throw StreamingFailure(errormsg.str());
    }

    if (rc == -1) {
      PQfreemem(copy_buffer);

      /* We're done here, nothing more expected */
      current_state = BASEBACKUP_EOB;
      result = false;
      break;
    }

    /* Create a message object representing the current contents. Since this copies
     * the content of the current receive buffer, we deallocate it afterwards. The msg reference
     * will hold a copy. */
    msg = BaseBackupMessage::message(copy_buffer, rc);
    PQfreemem(copy_buffer);

    /*
     * Depending on message type, prepare further actions or, if a data stream
     * was received, save it to the already prepared output sink.
     */
    switch(msg->msgType()) {

      case BBMSG_TYPE_PROGRESS:
      {

        /*
         * A process update message. Update the consumed byte counter but don't
         * exit here after processing that message. We get a next turn to try...
         *
         * Don't bother with the streaming state here either.
         */
        this->setConsumed(dynamic_pointer_cast<BaseBackupProgressMsg>(msg)->getProgressBytes());
        continue;

      }

      case BBMSG_TYPE_DATA:
      {

        /*
         * Update streaming status
         */
        current_state = BASEBACKUP_TABLESPACE_STREAM;
        data(msg);

        continue;
      }

      case BBMSG_TYPE_ARCHIVE_START:
      {
        string archive_name = dynamic_pointer_cast<BaseBackupArchiveStartMsg>(msg)->getArchiveName();

        current_state = BASEBACKUP_STEP_TABLESPACE;

        BOOST_LOG_TRIVIAL(debug) << "processing archive " << archive_name;

        /*
         * If there is already a handle, finalize it.
         */
        if (stepInfo.file != nullptr) {

          stepInfo.file->fsync();
          stepInfo.file->close();
          stepInfo.reset();

        }

        /*
         * We are starting to step into a fresh
         * tablespace archive.
         */
        this->incr();

        /*
         * Prepare next descriptor
         */
        this->stepInfo.descr = tablespaces.front();
        tablespaces.pop();


        /* not required here, since we are in a COPY stream, still */
        stepInfo.handle = nullptr;

        /*
         * Save this new descr handle. This is returned after having received the
         * data messages for this specific archive.
         */
        next = stepInfo.descr;

        /*
         * Iterator and all other things are properly setup, stack the
         * next file for streaming data.
         */
        stepInfo.file = backupHandle->stackFile(archive_name);

        /*
         * After having BBMSG_TYPE_ARCHIVE_START received, the next expected
         * message is an BBSMSG_TYPE_DATA. At this point we return to the caller the current
         * tablespace handle and tell him to proceed.
         */
        result = true;
        break;

      }
      case BBMSG_TYPE_MANIFEST_START:
      {
        string archive_name = "backup.manifest";

        BOOST_LOG_TRIVIAL(debug) << "processing backup manifest";

        /*
         * NOTE:
         * There is no need to handle the iterator here, since we just need it to make
         * sure we have all tablespaces consumed. Also we don't want to have
         * the manifest data being compressed, so we need to take care that the backup
         * stream target prepares a proper handle for us.
         */
        current_state = BASEBACKUP_MANIFEST_STREAM;

        /* Save compression used for the archive data */
        BackupProfileCompressType former_compression = backupHandle->getCompression();

        /* Get a new uncompressed handle for manifest data */
        stepInfo.file = backupHandle->stackFile(archive_name);

        /* Make sure we set compression level back, whatever it was before */
        backupHandle->setCompression(former_compression);

        continue;

      }
      case BBMSG_TYPE_UNKNOWN:
        throw StreamingFailure("cannot process unknown message type in basebackup stream");

    }

  }

  return result;
}

/******************************************************************************
 * Implementation of BaseBackupStream
 ******************************************************************************/

BaseBackupStream::BaseBackupStream(PGconn *prepared_con,
                                   std::shared_ptr<StreamBaseBackup> sb,
                                   std::shared_ptr<BackupProfileDescr> profileDescr) {

  if (sb == nullptr) {
    throw StreamingFailure("cannot use basebackup stream with undefined basebackup stream target");
  }

  if (profileDescr == nullptr) {
    throw StreamingFailure("cannot use basebackup stream with undefined profile");
  }

  pgconn = prepared_con;
  backupHandle = sb;
  profile      = profileDescr;

}

void BaseBackupStream::getStartPosition(std::shared_ptr<BaseBackupDescr> &descr,
                                        BaseBackupState &current_state) {

  /* Initial state for reading tablespace info from result set */
  PGresult *result   = nullptr;
  ExecStatusType es;

  current_state = BASEBACKUP_START_POSITION;
  result = PQgetResult(this->pgconn);

  /* Error checking */
  if ((es = PQresultStatus(result)) != PGRES_TUPLES_OK) {
    std::string sqlstate(PQresultErrorField(result, PG_DIAG_SQLSTATE));
    std::ostringstream oss;

    oss << "basebackup streaming failed: " << PQresultErrorMessage(result);
    PQclear(result);
    throw StreamingExecutionFailure(oss.str(), es, sqlstate);
  }

  if (PQntuples(result) != 1) {
    std::ostringstream oss;
    oss << "unexpected result for BASE_BACKUP command, expected(rows/fields) 1/2"
        << " but got " << PQntuples(result) << "/" << PQnfields(result);
    PQclear(result);
    throw StreamingFailure(oss.str());
  }

  /* Initialize basebackup descriptor */
  if (descr == nullptr) {
    descr = std::make_shared<BaseBackupDescr>();
  }

  /*
   * First result set is the starting position of
   * the basebackup stream, with two columns:
   * 1 - XLogRecPtr
   * 2 - TimelineID
   *
   * This is also the time we record the start time and
   * other properties into our internal basebackup descriptor.
   */
  descr->xlogpos = PQgetvalue(result, 0, 0);

  /*
   * We always expect the timeline from the server here. Older
   * PostgreSQL instances than 9.3 don't send the timeline via
   * the BASE_BACKUP result set, but those aren't supported anyways.
   */
  descr->timeline = CPGBackupCtlBase::strToInt(std::string(PQgetvalue(result, 0, 1)));
  PQclear(result);

  current_state = BASEBACKUP_STARTED;

}

std::shared_ptr<BaseBackupStream>
BaseBackupStream::makeStreamInstance(PGconn *prepared_conn,
                                     std::shared_ptr<StreamBaseBackup> backupHandle,
                                     std::shared_ptr<BackupProfileDescr> profileDescr) {

  if (PQserverVersion(prepared_conn) < 130000) {
    return std::make_shared<BaseBackupStream12>(prepared_conn, backupHandle, profileDescr);
  } else if ((PQserverVersion(prepared_conn) >= 130000)
      && (PQserverVersion(prepared_conn) < 150000)) {
    return std::make_shared<BaseBackupStream14>(prepared_conn, backupHandle, profileDescr);
  } else if (PQserverVersion(prepared_conn) >= 150000) {
    return std::make_shared<BaseBackupStream15>(prepared_conn, backupHandle, profileDescr);
  } else {
    std::ostringstream oss;

    oss << "unsupported PostgreSQL version "
        << PQserverVersion(prepared_conn);
    throw StreamingFailure(oss.str());
  }

}

/******************************************************************************
 * Implementation of BaseBackupStream12
 ******************************************************************************/

BaseBackupStream12::BaseBackupStream12(PGconn *prepared_conn,
                                       std::shared_ptr<StreamBaseBackup> backupHandle,
                                       std::shared_ptr<BackupProfileDescr> profileDescr)
        : BaseBackupStream(prepared_conn, backupHandle, profileDescr),
          TablespaceStreamer(backupHandle, prepared_conn) {}

BaseBackupStream12::~BaseBackupStream12() = default;

BaseBackupState BaseBackupStream12::getTablespaceInfo(pgbckctl::BaseBackupState &state) {

  return TablespaceQueue::getTablespaceInfo(state);

}

std::string BaseBackupStream12::query(std::shared_ptr<BackupProfileDescr> profile,
                                      PGconn *prepared_conn,
                                      BaseBackupQueryType type) {

  std::ostringstream query;
  char escapedlabel[MAXPGPATH];
  int  escape_error;

  switch(type) {
    case BASEBACKUP_QUERY_TYPE_BASEBACKUP:
    {

      /* We need a profile in this case */
      if (profile == nullptr) {
        throw StreamingFailure("backup profile required for BASEBACKUP_QUERY_TYPE_BASEBACKUP");
      }

      /*
       * Sanity check: profile is not a nullptr.
       */
      query << "BASE_BACKUP";

      if (this->profile == nullptr)
        throw StreamingFailure("backup profile not initialized for streaming basebackup");

      /*
       * Special LABEL requested?
       */
      if (this->profile->label != "") {
        PQescapeStringConn(this->pgconn,
                           escapedlabel,
                           this->profile->label.c_str(),
                           this->profile->label.length(),
                           &escape_error);
        query << " LABEL '" << escapedlabel << "'";
      }

      /*
       * We always request PROGRESS
       */
      query << " PROGRESS ";

      /*
       * immediate CHECKPOINT requested?
       */
      if (this->profile->fast_checkpoint)
        query << " FAST ";

      /*
       * WAL stream request for base backup?
       */
      if (this->profile->include_wal)
        query << " WAL ";

      /*
       * We usually wait for WAL segments to be archived.
       */
      if (!this->profile->wait_for_wal)
        query << " NOWAIT ";

      /*
       * MAX_RATE limits the used bandwidth of the stream.
       * Check if this is requested...
       */
      if (this->profile->max_rate > 0)
        query << " MAX_RATE " << this->profile->max_rate;

      /*
       * Starting with PostgreSQL 11, we have the NOVERFIY_CHECKSUMS
       * available.
       */
      if ( (this->profile->noverify_checksums)
           && (PQserverVersion(this->pgconn) >= 110000) )
        query << " NOVERIFY_CHECKSUMS ";

      /*
       * Backup manifests are supported starting with PostgreSQL 13 only.
       * We don't throw an error here, since backup profiles are interchangeable, so
       * give the caller just a hint via logging.
       */
      if (this->profile->manifest) {
        BOOST_LOG_TRIVIAL(warning) << "manifests are support with PostgreSQL >= 13, ignoring";
      }

      /*
       * We always request the tablespace map from the stream.
       */
      query << " TABLESPACE_MAP;";

      break;
    }
    default:
    {
      throw StreamingFailure("unknown basebackup command type");
    }
  }

  return query.str();

}

std::shared_ptr<BackupElemDescr>
BaseBackupStream12::handleMessage(BaseBackupState &current_state) {

  std::shared_ptr<BackupElemDescr> elem = nullptr;

  if (current_state != BASEBACKUP_STEP_TABLESPACE) {
    throw StreamingFailure("could not stream list of tablespaces");
  }

  /* There is nothing else to expect here than a tablespace archive.
   * So just call next() until we have reached the end of stream. */
  if (next(elem)) {

    /* Check whether we are forced to abort */
    if (current_state == BASEBACKUP_STEP_TABLESPACE_INTERRUPTED) {
      throw StreamingFailure("streaming tablespace archive aborted");
    }

    current_state = BASEBACKUP_TABLESPACE_STREAM;

    /*
     * Store tablespace archive data
     * There can't be anything else as a BackupTablespaceDescr carried
     * by the backup element descriptor.
     */

    if (elem->getType() != BASEBACKUP_ELEM_TBLSPC) {
      throw StreamingFailure("unexpected backup element from stream");
    }

    /* consume tablespace data from stream */
    tablespace(elem);

    current_state = BASEBACKUP_STEP_TABLESPACE;

  } else {
    current_state = BASEBACKUP_EOB;
  }

  return elem;

}

/******************************************************************************
 * Implementation of BaseBackupStream14
 ******************************************************************************/

BaseBackupStream14::BaseBackupStream14(PGconn *prepared_conn,
                                       std::shared_ptr<StreamBaseBackup> backupHandle,
                                       std::shared_ptr<BackupProfileDescr> profileDescr)
  : BaseBackupStream(prepared_conn, backupHandle, profileDescr),
    TablespaceStreamer(backupHandle, prepared_conn) {}

BaseBackupStream14::~BaseBackupStream14() noexcept {}

BaseBackupState BaseBackupStream14::getTablespaceInfo(pgbckctl::BaseBackupState &state) {

  return TablespaceQueue::getTablespaceInfo(state);

}

std::string BaseBackupStream14::query(std::shared_ptr<BackupProfileDescr> profile,
                                      PGconn *prepared_conn, pgbckctl::BaseBackupQueryType type) {

  std::ostringstream query;
  char escapedlabel[MAXPGPATH];
  int  escape_error;

  switch(type) {

    case BASEBACKUP_QUERY_TYPE_BASEBACKUP:
    {

      /* We need a profile in this case */
      if (profile == nullptr) {
        throw StreamingFailure("backup profile required for BASEBACKUP_QUERY_TYPE_BASEBACKUP");
      }

      query << "BASE_BACKUP";

      if (this->profile == nullptr)
        throw StreamingFailure("backup profile not initialized for streaming basebackup");

      /*
       * Special LABEL requested?
       */
      if (this->profile->label != "") {
        PQescapeStringConn(this->pgconn,
                           escapedlabel,
                           this->profile->label.c_str(),
                           this->profile->label.length(),
                           &escape_error);
        query << " LABEL '" << escapedlabel << "'";
      }

      /*
       * We always request PROGRESS
       */
      query << " PROGRESS ";

      /*
       * immediate CHECKPOINT requested?
       */
      if (this->profile->fast_checkpoint)
        query << " FAST ";

      /*
       * WAL stream request for base backup?
       */
      if (this->profile->include_wal)
        query << " WAL ";

      /*
       * We usually wait for WAL segments to be archived.
       */
      if (!this->profile->wait_for_wal)
        query << " NOWAIT ";

      /*
       * MAX_RATE limits the used bandwidth of the stream.
       * Check if this is requested...
       */
      if (this->profile->max_rate > 0)
        query << " MAX_RATE " << this->profile->max_rate;

      /*
       * Starting with PostgreSQL 11, we have the NOVERFIY_CHECKSUMS
       * available.
       */
      if ( (this->profile->noverify_checksums)
           && (PQserverVersion(this->pgconn) >= 110000) )
        query << " NOVERIFY_CHECKSUMS ";

      /*
       * PostgreSQL 13 introduces backup manifests, check
       * if the profile requested that feature and make sure
       * we have a server version >= 13.
       */
      if ( (this->profile->manifest)
           && (PQserverVersion(this->pgconn) >= 130000) ) {

        query << " MANIFEST 'yes' ";
        query << " MANIFEST_CHECKSUMS "
              << "'" << this->profile->manifest_checksums << "'";

      }

      /*
       * We always request the tablespace map from the stream.
       */
      query << " TABLESPACE_MAP;";

      break;
    }

    default:
    {
      throw StreamingFailure("unknown basebackup command type");
    }

  }

  return query.str();

}

std::shared_ptr<BackupElemDescr>
BaseBackupStream14::handleMessage(BaseBackupState &current_state) {

  std::shared_ptr<BackupElemDescr> result = nullptr;

  if (current_state != BASEBACKUP_STEP_TABLESPACE) {
    throw StreamingFailure("could not stream list of tablespaces");
  }

  /* Check when we reach the end of the tablespace archive stream. If a manifest
   * is requested, return that instead of terminating the stream. Manifest streams after
   * the very last tablespace, so if a manifest ist requested we expect after next() has
   * done its job. */
  if (next(result)) {

    /* Get tablespace archive data */
    tablespace(result);

  } else {

    if (profile->manifest) {
      manifest();
      result = std::make_shared<BackupManifestDescr>();
    }

  }

  /* Return proper state to the caller */
  current_state = this->getState();
  return result;

}

/******************************************************************************
 * Implementation of BaseBackupStream15
 ******************************************************************************/

BaseBackupStream15::BaseBackupStream15(PGconn *prepared_conn,
                                       std::shared_ptr<StreamBaseBackup> backupHandle,
                                       std::shared_ptr<BackupProfileDescr> profileDescr)
        : BaseBackupStream(prepared_conn, backupHandle, profileDescr),
          MessageStreamer(backupHandle, prepared_conn){}

BaseBackupStream15::~BaseBackupStream15() noexcept {}

BaseBackupState BaseBackupStream15::getTablespaceInfo(pgbckctl::BaseBackupState &state) {

  state = TablespaceQueue::getTablespaceInfo(state);

  /*
   * When arriving here we either already have processed archive or manifest
   * start message.
   */
  startCopyStream();
  return state;

}

std::string BaseBackupStream15::query(std::shared_ptr<BackupProfileDescr> profile,
                                      PGconn *prepared_conn, pgbckctl::BaseBackupQueryType type) {

  std::ostringstream query;
  char escapedlabel[MAXPGPATH];
  int  escape_error;
  std::stack<string> options;

  /* We need a profile in this case */
  if (profile == nullptr) {
    throw StreamingFailure("backup profile required for BASEBACKUP_QUERY_TYPE_BASEBACKUP");
  }

  switch(type) {
    case BASEBACKUP_QUERY_TYPE_BASEBACKUP:
    {
      query << "BASE_BACKUP (";

      if (this->profile == nullptr)
        throw StreamingFailure("backup profile not initialized for streaming basebackup");

      /*
       * Special LABEL requested?
       */
      if (this->profile->label != "") {
        PQescapeStringConn(this->pgconn,
                           escapedlabel,
                           this->profile->label.c_str(),
                           this->profile->label.length(),
                           &escape_error);
        options.push("LABEL " + string("'") + string(escapedlabel) + string("'"));
      }

      /*
       * We always request PROGRESS
       */
      options.push("PROGRESS on");

      /*
       * immediate CHECKPOINT requested?
       */
      if (this->profile->fast_checkpoint)
        options.push("CHECKPOINT fast");

      /*
       * WAL stream request for base backup?
       */
      if (this->profile->include_wal)
        options.push(" WAL on");

      /*
       * We usually wait for WAL segments to be archived.
       */
      if (!this->profile->wait_for_wal)
        options.push("WAIT off");

      /*
       * MAX_RATE limits the used bandwidth of the stream.
       * Check if this is requested...
       */
      if (this->profile->max_rate > 0) {
        std::ostringstream conv;
        conv << "MAX_RATE " << this->profile->max_rate;
        options.push(conv.str());
      }


      /*
       * Starting with PostgreSQL 11, we have the NOVERFIY_CHECKSUMS
       * available.
       */
      if (this->profile->noverify_checksums)
        options.push("VERIFY_CHECKSUMS off");

      /*
       * PostgreSQL 13 introduces backup manifests, check
       * if the profile requested that feature and make sure
       * we have a server version >= 13.
       */
      if (this->profile->manifest) {

        std::ostringstream oss;

        oss << " MANIFEST on, ";
        oss << " MANIFEST_CHECKSUMS "
            << "'" << this->profile->manifest_checksums << "'";
        options.push(oss.str());

      }

      /*
       * We always request the tablespace map from the stream.
       */
      options.push("TABLESPACE_MAP on");

      /* Make the list of options */
      while (!options.empty()) {
        query << " " << options.top();
        options.pop();
        query << (!options.empty() ? "," : "");
      }

      /* Close the options list */
      query << ")";

      break;
    }

    default:
    {
      throw StreamingFailure("unknown basebackup command type");
    }

  }

  return query.str();

}

std::shared_ptr<BackupElemDescr>
BaseBackupStream15::handleMessage(BaseBackupState &current_state) {

  std::shared_ptr<BackupElemDescr> result = nullptr;

  /* Check for correct state */
  if (current_state != BASEBACKUP_STEP_TABLESPACE) {
    throw StreamingFailure("invalid protocol handler state");
  }

  /*
   * We are in COPY mode now and need to dispatch the specific CopyOutResponse payloads.
   * The first message bytes indicate the kind of message.
   */
  if (next(result)) {

    /* Checkt if we are interrupted by signal handler */
    if (current_state == BASEBACKUP_STEP_TABLESPACE_INTERRUPTED) {
      throw StreamingFailure("streaming tablespace archive aborted");
    }

  } else {
    current_state = BASEBACKUP_EOB;
  }

  return result;

}

/******************************************************************************
 * Implementation of BaseBackupProcess
 ******************************************************************************/

BaseBackupProcess::BaseBackupProcess(PGconn *prepared_connection,
                                     std::shared_ptr<BackupProfileDescr> profile,
                                     std::string systemid,
                                     unsigned long long wal_segment_size) {

  this->current_state = BASEBACKUP_INIT;

  this->pgconn = prepared_connection;
  this->profile = profile;
  this->systemid = systemid;
  this->wal_segment_size = wal_segment_size;

  /*
   * prepared_connection needs to be a connected
   * libpq connection, otherwise we have to error out.
   */
  if (!((this->pgconn != NULL)
        && (PQstatus(this->pgconn) == CONNECTION_OK)))
    throw StreamingFailure("basebackup stream not connected");

}

BaseBackupProcess::~BaseBackupProcess() {

  /*
   * Don't close the associated PostgreSQL
   * connection handle here!
   *
   * We are expected to operate as a sub
   * on a calling PGStream handle, which does
   * all the legwork for us.
   */

}

void BaseBackupProcess::assignStopHandler(pgbckctl::JobSignalHandler *stopHandler) {
  this->stopHandler = stopHandler;
}

void BaseBackupProcess::start() {

  std::string query;

  /*
   * Check if the stream was properly prepared. This can be easily
   * recognized if we already had properly called prepareStream() which should
   * have initialized the PostgreSQL version aware stream handler
   */
  if (tinfo == nullptr) {
    throw StreamingFailure("attempt to start an unprepared basebackup stream");
  }

  query = tinfo->query(profile, pgconn,
                       BASEBACKUP_QUERY_TYPE_BASEBACKUP);

  BOOST_LOG_TRIVIAL(debug) << "replication command: " << query;

  /*
   * Fire the query...
   */
  if (PQsendQuery(this->pgconn, query.c_str()) == 0) {
    std::ostringstream oss;
    oss << "BASE_BACKUP command failed: " << PQerrorMessage(this->pgconn);
    throw StreamingFailure(oss.str());
  }

  this->tinfo->getStartPosition(this->baseBackupDescr,
                                this->current_state);

  /* Save system identifier and WAL segment size to descriptor */

  this->baseBackupDescr->systemid         = this->systemid;
  this->baseBackupDescr->wal_segment_size = this->wal_segment_size;

  /*
   * Also the backup profile used for this basebackup.
   *
   * NOTE: This will be saved into the catalog later, which will
   *       protect the used backup profile from being deleted,
   *       as long as this basebackup exists.
   */
  this->baseBackupDescr->used_profile = this->profile->profile_id;

  if (!this->profile->label.empty())
    this->baseBackupDescr->label = this->profile->label;

  /*
   * And the start timestamp...
   */
  this->baseBackupDescr->started = CPGBackupCtlBase::current_timestamp();

}

std::shared_ptr<BaseBackupDescr> BaseBackupProcess::getBaseBackupDescr() {

  /*
   * NOTE. We don't bother which state the process handle has,
   * just return the descriptor.
   */
  return this->baseBackupDescr;
}

std::string BaseBackupProcess::getSystemIdentifier() {

  if (this->baseBackupDescr != nullptr)
    return this->baseBackupDescr->systemid;

  return "";

}

void BaseBackupProcess::readTablespaceInfo() {

  current_state = this->tinfo->getTablespaceInfo(current_state);

}

void BaseBackupProcess::prepareStream(std::shared_ptr<StreamBaseBackup> &backupHandle) {

  /* This method requires a valid backup target handler */
  if (backupHandle == nullptr) {
    throw StreamingFailure("undefined basebackup stream target");
  }

  /* Backup stream target should be properly initialized */
  if (!backupHandle->isInitialized()) {
    throw StreamingFailure("basebackup stream target not yet initialized");
  }

  /* Initialize the internal streaming protocol handler */
  this->tinfo = BaseBackupStream::makeStreamInstance(this->pgconn, backupHandle, profile);

  /* We want to have a stop handler for the basebackup stream */
  this->tinfo->assignStopHandler(this->stopHandler);

}

bool BaseBackupProcess::stream(std::shared_ptr<BackupCatalog> catalog) {

  /* Make sure everything is prepared properly */
  if (this->tinfo == nullptr) {
    throw StreamingFailure("cannot start data streaming without proper protocol handle");
  }

  if (current_state != BASEBACKUP_TABLESPACE_READY) {
    throw StreamingFailure("cannot start data streaming from improper state");
  }

  /* Prepare state to iterate through tablespaces */
  current_state = BASEBACKUP_STEP_TABLESPACE;

  while(true) {

    auto descr = tinfo->handleMessage(current_state);

    /* Check state */
    if (current_state == BASEBACKUP_STEP_TABLESPACE_INTERRUPTED) {
      throw StreamingFailure("basebackup stream interrupted");
    }

    if (current_state == BASEBACKUP_EOB) {
      BOOST_LOG_TRIVIAL(debug) << "end of backup stream reached";
      break;
    }

    if (descr->getType() == BASEBACKUP_ELEM_TBLSPC) {

      /*
       * The backup id is retrieved by the basebackup descriptor and not (obviously) not
       * provided directly within the basebackup stream. So we need to reference
       * it explictely here before saving the descriptor to disc.
       */
      dynamic_pointer_cast<BackupTablespaceDescr>(descr)->backup_id = baseBackupDescr->id;
      catalog->registerTablespaceForBackup(dynamic_pointer_cast<BackupTablespaceDescr>(descr));

    }

    /* Should we get another state than BASEBACKUP_STEP_TABLESPACE, error out */
    if (current_state != BASEBACKUP_STEP_TABLESPACE) {
      throw StreamingFailure("unexpected state in basebackup stream");
    }
  }

  /* success */
  return true;

}

void BaseBackupProcess::end() {

  PGresult *res;
  ExecStatusType es;

  /*
   * We must have reached internal state of BASEBACKUP_EOB, otherwise
   * we cannot receive the WAL end position from the stream.
   */
  if (this->current_state != BASEBACKUP_EOB) {
    throw StreamingFailure("cannot finalize basebackup stream, not in end position");
  }

  /*
   * Get stop location.
   */
  res = PQgetResult(this->pgconn);

  if ((es = PQresultStatus(res)) != PGRES_TUPLES_OK) {
    std::string sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
    std::ostringstream oss;

    oss << "could not read WAL end position from stream: "
        << PQresultErrorMessage(res);
    PQclear(res);
    throw StreamingExecutionFailure(oss.str(), es, sqlstate);
  }

  if (PQntuples(res) != 1) {
    throw StreamingFailure("unexpected number of tuples for stream end position");
  }

  if (PQnfields(res) != 2) {
    std::ostringstream oss;
    oss << "unexpected number of fields in WAL end position result set (got "
        << PQnfields(res) << " expected 2)";
    throw StreamingFailure(oss.str());
  }

  /*
   * Store the XLOG end position in the BaseBackupDescr.
   */
  this->baseBackupDescr->xlogposend = PQgetvalue(res, 0, 0);
  PQclear(res);

  /*
   * Update internal state that we have reached XLOG end position.
   */
  this->current_state = BASEBACKUP_END_POSITION;
}

BaseBackupState BaseBackupProcess::getState() {
  return this->current_state;
}
