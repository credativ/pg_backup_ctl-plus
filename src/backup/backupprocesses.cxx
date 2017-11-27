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

  } else if (*bufferlen == -1) {

    status = this->current_state = ARCHIVER_END_POSITION;

  } else if (*bufferlen == -2) {

    /* Oops, something went wrong here */
    status = this->current_state = ARCHIVER_STREAMING_ERROR;

  }

  return status;
}

std::string WALStreamerProcess::xlogpos() {

  if (this->current_state != ARCHIVER_STREAMING_ERROR)
    return this->streamident.xlogpos;
  else
    throw StreamingFailure("attempt to query xlog position from wal streamer not in streaming state");

}

void WALStreamerProcess::setArchiveLogDir(std::shared_ptr<BackupDirectory> archiveLogDir) {

  this->archiveLogDir = archiveLogDir;

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
      XLogRecPtr serverpos;

      this->streamident.xlogpos
        = PGStream::encodeXLOGPos(dynamic_cast<XLOGDataStreamMessage *>(message)->getXLOGStartPos());

#ifdef __DEBUG_XLOG__
      std::cerr << "NEW XLOG POS " << this->streamident.xlogpos << std::endl;
#endif

      /* get server position, reported by the connected WAL sender */
      serverpos = dynamic_cast<XLOGDataStreamMessage *>(message)->getXLOGServerPos();

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
       * XXX: Flush currenty the same as the current write position
       *      Need to implement WAL file handling to get that right.
       */
      this->streamident.write_position = dynamic_cast<XLOGDataStreamMessage *>(message)->getXLOGServerPos();
      this->streamident.flush_position = dynamic_cast<XLOGDataStreamMessage *>(message)->getXLOGServerPos();

#if PG_VERSION_NUM < 110000
      this->streamident.write_pos_start_offset += this->streamident.write_position % XLOG_SEG_SIZE;
#else
      this->streamident.write_pos_start_offset += XLogSegmentOffset(this->streamident.write_position,
                                                                    this->streamident.wal_segment_size);
#endif

      /*
       * archiveLogDir holds the handler which should
       * be applied to each WAL record.
       */
      if (this->archiveLogDir != nullptr) {

        XLogSegNo segment_number;
        char      xlogfilename[MAXPGPATH];

        /*
         * Write to the archive handle, but only in case
         * we have a valid byte chunk.
         */
#if PG_VERSION_NUM < 110000
        XLByteToSeg(this->streamident.write_pos_start_offset,
                    segment_number);
        XLogFileName(xlogfilename,
                     this->streamident.timeline,
                     segment_number);
#else
        XLByteToSeg(this->streamident.write_pos_start_offset,
                    segment_number,
                    this->streamident.wal_segment_size);
        XLogFileName(xlogfilename,
                     this->streamident.write_pos_start_offset,
                     segment_number,
                     this->streamident.wal_segment_size);
#endif
        std::cerr << "Write to XLOG segment " << xlogfilename << std::endl;
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
          std::cerr << " ... sending status update to primary " << std::endl;
          rm->send();
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

bool WALStreamerProcess::receive() {

  bool can_continue = false;
  char *buffer = NULL; /* temporary recv buffer, handle by libpq */
  int bufferlen = 0;

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

  while (this->handleReceive(&buffer, &bufferlen) == ARCHIVER_STREAMING) {

    XLOGStreamMessage *message = nullptr;

    /*
     * Interpret buffer, create a corresponding WAL message object.
     */
    this->receiveBuffer.allocate(bufferlen);
    this->receiveBuffer.write(buffer, bufferlen, 0);

    message = XLOGStreamMessage::message(this->pgconn,
                                         this->receiveBuffer,
                                         this->streamident.wal_segment_size);

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
    cerr << "end of WAL stream detected" << endl;
    can_continue = true;
  }

  return can_continue;
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
      break;
    }

    /*
     * ... else write the chunk to the file.
     */
    this->stepInfo.file->write(copybuf, rc);
  }

  this->current_state = BASEBACKUP_TABLESPACE_READY;
}
