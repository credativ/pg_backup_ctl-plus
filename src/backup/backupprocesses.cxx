#include <stream.hxx>
#include <backup.hxx>
#include <backupprocesses.hxx>
#include <xlogdefs.hxx>

/* Required for select() */
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

using namespace credativ;

/******************************************************************************
 * Implementation of WALStreamerProcess
 ******************************************************************************/

WALStreamerProcess::WALStreamerProcess(PGconn *prepared_connection,
                                       StreamIdentification streamident) {

  this->current_state = ARCHIVER_STARTUP;
  this->pgconn        = prepared_connection;
  this->streamident   = streamident;

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

  rsum.setStatus(this->streamident.write_position,
                 this->streamident.flush_position,
                 this->streamident.apply_position);
  rsum.send();
  return this->current_state;

}

ArchiverState WALStreamerProcess::handleReceive(char **buffer, int *bufferlen) {

  /* Holds length of incoming buffer data */
  int currlen = 0;
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
      char *datablock;

      this->streamident.xlogpos
        = PGStream::encodeXLOGPos(datamsg->getXLOGStartPos());

#ifdef __DEBUG_XLOG__
      std::cerr << "NEW XLOG POS " << this->streamident.xlogpos << std::endl;
#endif

      /* get server position, reported by the connected WAL sender */
      serverpos = datamsg->getXLOGServerPos();

#ifdef __DEBUG_XLOG__
      std::cerr << "SERVER XLOG POS "
                << PGStream::encodeXLOGPos(serverpos)
                << std::endl;
#endif
      this->streamident.server_position = serverpos;

      /*
       * Update XLOG write position and, if flushed, also
       * the flush position.
       *
       * NOTE: If no backup handler was configured,
       *       Flush is currently the same as the
       *       current write position.
       */
      this->streamident.write_position = datamsg->getXLOGServerPos();
      this->streamident.updateStartSegmentWriteOffset();

      /*
       * Our backup handler does all the necessary work
       * to spool the transaction logs into the archive.
       */
      if (this->backupHandler != nullptr) {

        /*
         * Everything should be in shape so far. A current
         * WAL segment file should be allocated and we can go
         * forward and spool the XLOG message data block into that
         * current WAL file. Though, we must take care for
         * a WAL segment boundary, in that case we have to switch to
         * new WAL segment file, again.
         */
        this->streamident.flush_position
          = this->backupHandler->write(datamsg,
                                       this->streamident.timeline);

      } else { /* if ... backupHandler != nullptr */

        this->streamident.flush_position = datamsg->getXLOGServerPos();

      }

      break;
    }

  case 'k':
    {
      PrimaryFeedbackMessage *pm = dynamic_cast<PrimaryFeedbackMessage *>(message);

#ifdef __DEBUG_XLOG__
      std::cerr << "primary feedback message" << std::endl;
#endif

      if (pm->responseRequested()) {
        /*
         * Create a receiver status update message.
         */
        ReceiverStatusUpdateMessage *rm = new ReceiverStatusUpdateMessage(this->pgconn);

        /*
         * Set properties to report required properties to primary.
         */
        rm->setStatus(this->streamident.write_position,
                      this->streamident.flush_position,
                      this->streamident.apply_position);

        /* ... and send 'em over */
        try {

#ifdef __DEBUG_XLOG__
          std::cerr << " ... sending status update to primary " << std::endl;
#endif

          rm->send();

          /*
           * Update timeout interval.
           */
          this->last_status_update = CPGBackupCtlBase::current_hires_time_point();

        } catch (XLOGMessageFailure &e) {
          delete rm;
          throw e;
        }

      }

      break;
    }
  default:
    {
      /* unhandled message type */
#ifdef __DEBUG_XLOG__
      std::cerr << "unhandled message type " << msgType << std::endl;
#endif
      break;
    }

  } /* switch...msgType */

}

bool WALStreamerProcess::stopHandlerWantsExit() {

  if (this->stopHandler != nullptr) {
    cerr << "checking STOP handler" << endl;
    return this->stopHandler->check();
  } else
    return false;

}

void WALStreamerProcess::assignStopHandler(JobSignalHandler *handler) {

  /* if handler is NULL, this is a no-op */
  if (handler == nullptr) {
    return;
  }

  this->stopHandler = handler;
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

  cerr << "entering WAL streaming receive() " << endl;

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
     * Before doing anything, check wether our stop
     * handler wants to exit.
     */
    if (this->stopHandlerWantsExit()) {
      reason = this->current_state = ARCHIVER_SHUTDOWN;
      this->streamident.status = StreamIdentification::STREAM_PROGRESS_SHUTDOWN;
      break;
    }

    /*
     * Next we check wether we should send a status update message
     * to upstream. This is always the case if our internal
     * timeout value forces us to do.
     */
    if (CPGBackupCtlBase::calculate_duration_ms(this->last_status_update,
                                                CPGBackupCtlBase::current_hires_time_point())
        >= std::chrono::milliseconds(this->receiver_status_timeout)) {

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

    std::cerr << "write XLOG message " << std::endl;

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
    cerr << "no data, continue ... " << endl;
    can_continue = true;
  }
  if (this->current_state == ARCHIVER_STREAMING_ERROR) {
    cerr << "failure on PQgetCopyData(): " << PQerrorMessage(this->pgconn) << endl;
    // std::ostringstream oss;
    // oss << "could not shutdown streaming connection properly: "
    //     << PQerrorMessage(this->pgconn);
    // throw StreamingFailure(oss.str());
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

      /*
       * This is a timeline switch. Handle it accordingly.
       */
      this->endOfStreamTimelineSwitch(pgres,
                                      this->streamident.timeline,
                                      this->streamident.xlogpos);
      PQclear(pgres);
      can_continue = true;

    } else if (this->current_state == ARCHIVER_SHUTDOWN) {

      /*
       * Server side has terminated, schedule own shutdown accordingly.
       */
      can_continue = false;
    }

  } else if (this->current_state == ARCHIVER_SHUTDOWN) {

    /*
     * Inner loop recognized a shutdown request and set the
     * current state to ARCHIVER_SHUTDOWN.
     */
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
    throw CPGBackupCtlFailure("could not call endOfStream() on unterminated WAL stream");
  }

  result = PQgetResult(this->pgconn);

  /*
   * Check wether we are still in COPY IN mode.
   */
  if (result != NULL && (PQresultStatus(result) == PGRES_COPY_IN)) {
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

      this->end();

      if (this->current_state != ARCHIVER_SHUTDOWN) {

        /*
         * end() should have send a successful end of stream
         * message to the connected server. If not succeeded, we
         * will be in an undetermined state, so die the hard way
         */
        throw StreamingFailure("unrecognized status after shutdown attempt, giving up");

      }
      if (PQresultStatus(result) == PGRES_TUPLES_OK) {
        this->current_state = ARCHIVER_TIMELINE_SWITCH;
        return result;
      }
      else if (PQresultStatus(result) == PGRES_COMMAND_OK) {
        this->current_state = ARCHIVER_SHUTDOWN;
        return result;
      }
      else {
        /* This means we have on error, so terminate
         * the stream nevertheless.
         */
        std::ostringstream oss;
        oss << "error during end of stream condition: "
            << PQresultErrorMessage(result);
        PQclear(result);
        result = NULL;
        this->current_state = ARCHIVER_STREAMING_ERROR;
        throw StreamingFailure(oss.str());
      }

    } catch(StreamingFailure &e) {
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
}

StreamIdentification WALStreamerProcess::identification() {
  return this->streamident;
}

void WALStreamerProcess::end() {

   if (PQputCopyEnd(this->pgconn, NULL) <= 0 || PQflush(this->pgconn)) {

    std::ostringstream oss;

    oss << "could not terminate COPY in progress: "
        << PQerrorMessage(this->pgconn);
    this->current_state = ARCHIVER_STREAMING_ERROR;
    throw StreamingFailure(oss.str());

  }

  this->current_state = ARCHIVER_SHUTDOWN;
  this->streamident.status = StreamIdentification::STREAM_PROGRESS_SHUTDOWN;

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
        << this->streamident.xlogpos
        << " TIMELINE "
        << this->streamident.timeline
        << ";";

#ifdef __DEBUG__
  std::cerr << "WALStreamer start() query: " << query.str() << std::endl;
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

/******************************************************************************
 * Implementation of BaseBackupProcess
 ******************************************************************************/

BaseBackupProcess::BaseBackupProcess(PGconn *prepared_connection,
                                     std::shared_ptr<BackupProfileDescr> profile,
                                     std::string systemid) {

  this->current_state = BASEBACKUP_INIT;

  this->pgconn = prepared_connection;
  this->profile = profile;
  this->systemid = systemid;

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

void BaseBackupProcess::start() {

  PGresult *result;
  ExecStatusType es;
  std::ostringstream query;
  char escapedlabel[MAXPGPATH];
  int  escape_error;

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
   * We always request the tablespace map from the stream.
   */
  query << " TABLESPACE_MAP;";

  /*
   * Fire the query...
   */
  if (PQsendQuery(this->pgconn, query.str().c_str()) == 0) {
    std::ostringstream oss;
    oss << "BASE_BACKUP command failed: " << PQerrorMessage(this->pgconn);
    throw StreamingFailure(oss.str());
  }

  result = PQgetResult(this->pgconn);

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

  this->current_state = BASEBACKUP_START_POSITION;

  /*
   * First result set is the starting position of
   * the basebackup stream, with two columns:
   * 1 - XLogRecPtr
   * 2 - TimelineID
   *
   * This is also the time we record the start time and
   * other properties into our internal basebackup descriptor.
   */
  this->baseBackupDescr = std::make_shared<BaseBackupDescr>();
  this->baseBackupDescr->xlogpos = PQgetvalue(result, 0, 0);

  /* Save system identifier to descriptor */

  this->baseBackupDescr->systemid = this->systemid;

  /*
   * We always expect the timeline from the server here. Older
   * PostgreSQL instances than 9.3 don't send the timeline via
   * the BASE_BACKUP result set!
   */
  this->baseBackupDescr->timeline
    = CPGBackupCtlBase::strToInt(std::string(PQgetvalue(result, 0, 1)));

  if (this->profile->label != "")
    this->baseBackupDescr->label = this->profile->label;

  /*
   * And the start timestamp...
   */
  this->baseBackupDescr->started = CPGBackupCtlBase::current_timestamp();

  /*
   * Mark BaseBackupProcess as successfully started
   */
  this->current_state = BASEBACKUP_STARTED;
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

  PGresult *res;
  int i;
  ExecStatusType es;

  /*
   * Stack tablespace meta info only if
   * successfully started.
   */
  if (this->current_state != BASEBACKUP_STARTED)
    throw StreamingFailure("reading tablespace meta information requires a started base backup process");

  this->current_state = BASEBACKUP_TABLESPACE_META;

  /*
   * Read tablespace meta info from result set.
   */
  res = PQgetResult(this->pgconn);

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
    descr->spcoid = CPGBackupCtlBase::strToInt(std::string(PQgetvalue(res, i, 0)));

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
    this->tablespaces.push(descr);
  }

  /*
   * Free result set
   */
  PQclear(res);

  /*
   * okay, looks like all meta information is there...
   */
  this->current_state = BASEBACKUP_TABLESPACE_READY;
}

bool BaseBackupProcess::stepTablespace(std::shared_ptr<StreamBaseBackup> backupHandle,
                                       std::shared_ptr<BackupTablespaceDescr> &current_tablespace) {

  std::shared_ptr<BackupTablespaceDescr> descr = nullptr;

  if (this->tablespaces.size() < 1) {
    this->current_state = BASEBACKUP_EOB;
    current_tablespace = nullptr;
    return false;
  }

  /*
   * backupHandle must be initialized.
   */
  if (!backupHandle->isInitialized())
    throw StreamingFailure("cannot write into uninitialized streaming backup handle");

  if (this->current_state != BASEBACKUP_TABLESPACE_READY)
    throw StreamingFailure("unexpected state in base backup stream, could not start tablespace backup");

  /*
   * Check, if we are starting to step into a fresh
   * tablespace backup. This is indicated by an internal
   * state of -1 stored in the stepInfo struct property.
   *
   * In case of -1, we need to initialize the PGresult
   * handle of the current stepping.
   */
  if (this->stepInfo.current_step == -1) {

    this->stepInfo.current_step = 0;

  }
  else {

    this->stepInfo.current_step++;

  }

  /*
   * Dequeue the current tablespace from the
   * internal queue, if any.
   */
  this->stepInfo.descr = this->tablespaces.front();
  this->tablespaces.pop();
  this->stepInfo.handle = PQgetResult(this->pgconn);

  if (PQresultStatus(this->stepInfo.handle) != PGRES_COPY_OUT) {

    std::ostringstream oss;
    oss << "could not get COPY data stream: " << PQerrorMessage(this->pgconn);
    throw StreamingFailure(oss.str());
  }

  current_tablespace = descr = this->stepInfo.descr;

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

void BaseBackupProcess::backupTablespace(std::shared_ptr<BackupTablespaceDescr> descr) {

  char *copybuf = NULL;
  int rc;

  while(true) {

    if (copybuf != NULL) {
      PQfreemem(copybuf);
      copybuf = NULL;
    }

    rc = PQgetCopyData(this->pgconn, &copybuf, 0);

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
  }

  this->current_state = BASEBACKUP_TABLESPACE_READY;
}
