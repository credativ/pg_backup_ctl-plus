#include <stream.hxx>
//#include <fs-archive.hxx>
#include <backup.hxx>
#include <backupprocesses.hxx>

using namespace credativ;

/******************************************************************************
 * Implementation of BaseBackupProcess
 ******************************************************************************/

BaseBackupProcess::BaseBackupProcess(PGconn *prepared_connection,
                                     std::shared_ptr<BackupProfileDescr> profile) {

  this->current_state = BASEBACKUP_INIT;

  this->pgconn = prepared_connection;
  this->profile = profile;

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
  if (this->profile->label != "")
    query << " LABEL '" << PQescapeStringConn(this->pgconn,
                                              escapedlabel,
                                              this->profile->label.c_str(),
                                              this->profile->label.length(),
                                              &escape_error) << "'";

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
  if (PQsendQuery(this->pgconn, query.str().c_str()) != 0) {
    std::ostringstream oss;
    oss << "BASE_BACKUP command failed: " << PQerrorMessage(this->pgconn);
    throw StreamingFailure(oss.str());
  }

  if ((es = PQresultStatus(result)) != PGRES_TUPLES_OK) {
    std::string sqlstate(PQresultErrorField(result, PG_DIAG_SQLSTATE));
    std::ostringstream oss;

    oss << "basebackup streaming failed: " << PQresultErrorMessage(result);
    PQclear(result);
    throw StreamingExecutionFailure(oss.str(), es, sqlstate);
  }

  result = PQgetResult(this->pgconn);

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

void BaseBackupProcess::readTablespaceInfo() {

  PGresult *res;
  int i;

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

  if (PQntuples(res) < 1) {
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
     * Since we rely in PROGRESS in the BASE_BACKUP command, this is
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
}