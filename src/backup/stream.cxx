#include <istream>

#include <stream.hxx>

using namespace credativ;

std::string ERRCODE_DUPLICATE_OBJECT("42710");

StreamingFailure::StreamingFailure(std::string errstr,
                                   ConnStatusType connStatus)
  : CPGBackupCtlFailure(errstr) {

  this->connStatus = connStatus;
}

StreamingFailure::StreamingFailure(std::string errstr, ExecStatusType execStatus)
  : CPGBackupCtlFailure(errstr) {

  this->execStatus = execStatus;
}

StreamingFailure::StreamingFailure(std::string errstr, PGTransactionStatusType transStatus)
  : CPGBackupCtlFailure(errstr) {

  this->transStatus = transStatus;
}

PGTransactionStatusType StreamingFailure::getTransStatus() {
  return this->transStatus;
}

ConnStatusType StreamingFailure::getConnStatus() {
  return this->connStatus;
}

ExecStatusType StreamingFailure::getExecStatus() {
  return this->execStatus;
}

std::string StreamingExecutionFailure::getSQLSTATE() {
  return this->SQLSTATE;
};

StreamingExecutionFailure::StreamingExecutionFailure(std::string errstring,
                                                     ExecStatusType execStatus,
                                                     std::string& SQLSTATE) : StreamingFailure(errstring, execStatus) {
  this->SQLSTATE = SQLSTATE;
};

XLogRecPtr StreamIdentification::getXLOGStartPos() {
  return PGStream::decodeXLOGPos(this->xlogpos);
}

StreamIdentification::StreamIdentification() {

 this->id = -1;
  this->archive_id = -1;
  this->stype = "";
  this->slot_name = "";
  this->systemid  = "";
  this->timeline  = -1;
  this->xlogpos   = "";
  this->dbname    = "";
  this->status    = "";
  this->create_date = "";
  this->slot = nullptr;
}

StreamIdentification::~StreamIdentification() {
  this->slot = nullptr;
}

void StreamIdentification::reset() {
  this->id = -1;
  this->archive_id = -1;
  this->stype = "";
  this->slot_name = "";
  this->systemid  = "";
  this->timeline  = -1;
  this->xlogpos   = "";
  this->dbname    = "";
  this->status    = "";
  this->create_date = "";
  this->slot = nullptr;
}


/******************************************************************************
 * Implementation of PGStream
 ******************************************************************************/

XLogRecPtr PGStream::decodeXLOGPos(std::string pos) {

  XLogRecPtr result;
  uint32     hi, lo;

  if (sscanf(pos.c_str(), "", &hi, &lo) != 2) {
    std::ostringstream oss;
    oss << "could not parse XLOG location string: " << pos;
    throw StreamingFailure(oss.str());
  }

  return ((uint64) hi) << 32 | lo;
}

int PGStream::getServerVersion() {

  if (!this->connected()) {
    throw StreamingConnectionFailure("could not get server version: not connected",
                                     CONNECTION_BAD);
  }

  return PQserverVersion(this->pgconn);
}

PGStream::PGStream(const std::shared_ptr<CatalogDescr>& descr) {
  this->descr = descr;
  this->pgconn = NULL;
}

bool PGStream::connected() {
  return ((this->pgconn != NULL)
          && (PQstatus(this->pgconn) == CONNECTION_OK));
}

bool PGStream::connected(ConnStatusType& cs) {

  if (!this->pgconn)
    return false;

  cs = PQstatus(this->pgconn);
  return (cs != CONNECTION_BAD);
}

PGStream::~PGStream() {

  if (this->connected()) {
    PQfinish(this->pgconn);
  }

}

void PGStream::setPGConnection(PGconn *conn) {
  this->pgconn = conn;
}

void PGStream::connect() {

  ConnStatusType cs;

  if (this->descr == nullptr) {
    throw StreamingFailure("could not establish streaming connection with undefined catalog");
  }

  /*
   * Build connection string.
   */
  std::ostringstream conninfo;

  /*
   * A specified DSN setting always overrides direct
   * host specifications and vice versa.
   *
   * XXX: maybe use libpq connection option parsing, looks safer.
   */
  cout << "DSN is " << this->descr->coninfo->dsn << endl;
  if (this->descr->coninfo->dsn.length() > 0) {
    cout << "using database DSN for connection" << endl;
    conninfo << this->descr->coninfo->dsn
             << " "
             << "replication=database";
  } else {
    conninfo << "host=" << this->descr->coninfo->pghost;
    conninfo << " " << "dbname=" << this->descr->coninfo->pgdatabase;
    conninfo << " " << "user=" << this->descr->coninfo->pguser;
    conninfo << " " << "port=" << this->descr->coninfo->pgport;
    conninfo << " " << "replication=database";
  }

  /*
   * Establish database connection
   */
  this->pgconn = PQconnectdb(conninfo.str().c_str());

  /*
   * Connection attempt successful?
   * Iff not, raise a StreamingFailure exception
   * which the reason attached.
   */
  if ((cs = PQstatus(this->pgconn)) == CONNECTION_BAD) {
    std::ostringstream oss;
    oss << "database connection failure: " << PQerrorMessage(this->pgconn);

    /* Don't forget to cleanup PQ connection */
    PQfinish(this->pgconn);
    throw StreamingConnectionFailure(oss.str(), cs);
  }
}

void PGStream::disconnect() {
  ConnStatusType cs;

  if (!this->connected())
    throw StreamingConnectionFailure("unable to disconnect stream: not connected",
                                     CONNECTION_BAD);

  PQfinish(this->pgconn);
  this->pgconn = NULL; /* to make sure */
  this->identified = false;

  /*
   * Reset Stream Identification.
   */
  this->streamident.reset();
}

bool PGStream::isIdentified() {
  return this->identified;
}

void PGStream::timelineHistoryFileContent(MemoryBuffer &buffer,
                                          std::string &filename,
                                          int timeline) {
  ExecStatusType es;
  PGresult *result;
  std::ostringstream query;

  if (!this->connected()) {
    throw StreamingFailure("stream is not connected");
  }

  query << "TIMELINE_HISTORY " << timeline << ";";

  result = PQexec(this->pgconn, query.str().c_str());

  /*
   * Valid result?
   */
  es = PQresultStatus(result);

  if (es != PGRES_TUPLES_OK) {
    std::string sqlstate(PQresultErrorField(result, PG_DIAG_SQLSTATE));
    std::ostringstream oss;
    oss << "TIMELINE_HISTORY command failed: " << PQresultErrorMessage(result);
    PQclear(result);
    throw StreamingExecutionFailure(oss.str(), es, sqlstate);
  }

  /*
   * We expect one row, two columns.
   */
  if (PQnfields(result) != 2) {
    PQclear(result);
    throw StreamingFailure("TIMELINE_HISTORY command error: expected 2 columns in result");
  }

  if (PQntuples(result) != 1) {
    PQclear(result);
    throw StreamingFailure("TIMELINE_HISTORY command error: expected single row in result");
  }

  filename = PQgetvalue(result, 0, 0);

  /*
   * Write the history file contents into referenced memory buffer.
   * Do this in a try..catch block to prevent result set leak.
   */
  try {

    buffer.allocate(PQgetlength(result, 0, 1));
    buffer.write(PQgetvalue(result, 0, 1), buffer.getSize(), 0);
    PQclear(result);

  } catch (CPGBackupCtlFailure &e) {

    PQclear(result);
    /* re-throw as StreamingFailure */
    throw StreamingFailure(e.what());

  }

  /* and we're done */

}

void PGStream::timelineHistoryFileContent(MemoryBuffer &buffer,
                                          std::string &filename) {

  if (!this->isIdentified()) {
    throw StreamingFailure("cannot get timeline history file content without timeline ID");
  }

  this->timelineHistoryFileContent(buffer,
                                   filename,
                                   this->streamident.timeline);

}

std::string PGStream::getServerSetting(std::string name) {

  ExecStatusType es;
  PGresult *result;
  std::string value;
  std::ostringstream query;

  if (!this->connected()) {
    throw StreamingFailure("stream is not connected");
  }

  query << "SHOW " << name << ";";

  /*
   * SHOW doesn't require to honour internal state of
   * the stream handle, so just go forward and get the parameter
   * value.
   */
  result = PQexec(this->pgconn, query.str().c_str());

  /*
   * Valid result?
   */
  es = PQresultStatus(result);

  if (es != PGRES_TUPLES_OK) {
    /* treat error as an exception */
    std::string sqlstate(PQresultErrorField(result, PG_DIAG_SQLSTATE));
    std::ostringstream oss;
    oss << "SHOW command failed: " << PQresultErrorMessage(result);
    PQclear(result);
    throw StreamingExecutionFailure(oss.str(), es, sqlstate);
  }

  /*
   * We expect at least one tuple...
   */
  if (PQntuples(result) >= 1) {
    value = std::string(PQgetvalue(result, 0, 0));
  } else {
    PQclear(result);
    throw StreamingFailure("unexpected result for parameter value: no rows");
  }

  PQclear(result);
  return value;
}

void PGStream::identify() {

  ExecStatusType es;
  PGresult      *result;
  const char *query = "IDENTIFY_SYSTEM;";

  /*
   * Clear internal state
   */
  this->identified = false;

  result = PQexec(this->pgconn, query);

  /*
   * If the query returned a valid result
   * handle, initialize the identification of
   * this PGStream stanza...
   */
  es = PQresultStatus(result);

  if (es != PGRES_TUPLES_OK) {
    /* whoops, command execution returned no rows... */
    std::string sqlstate(PQresultErrorField(result, PG_DIAG_SQLSTATE));
    std::ostringstream oss;
    oss << "IDENTIFY SYSTEM failed: " << PQresultErrorMessage(result);
    PQclear(result);
    throw StreamingExecutionFailure(oss.str(), es, sqlstate);
  }

  /* sanity check, expected number of rows/cols ? */
  if (PQnfields(result) < 4) {
    throw StreamingExecutionFailure("unexpected number(4) of system identification columns",
                                    es);
  }

  if (PQntuples(result) != 1) {
    throw StreamingExecutionFailure("unexpected number of rows(1) for system identification",
                                    es);
  }

  /*
   * Read properties into internal identification object.
   */
  this->streamident.systemid
    = std::string(PQgetvalue(result, 0, PQfnumber(result, "systemid")));
  this->streamident.timeline
    = CPGBackupCtlBase::strToInt(std::string(PQgetvalue(result, 0,
                                                        PQfnumber(result, "timeline"))));
  this->streamident.xlogpos
    = std::string(PQgetvalue(result, 0, PQfnumber(result, "xlogpos")));
  this->streamident.dbname
    = std::string(PQgetvalue(result, 0, PQfnumber(result, "dbname")));

  this->streamident.create_date
    = CPGBackupCtlBase::current_timestamp();

  this->identified = true;
}

std::shared_ptr<BaseBackupProcess> PGStream::basebackup() {

  /*
   * Instantiate a new backup profile object with
   * its default values.
   */
  std::shared_ptr<BackupProfileDescr> profile = std::make_shared<BackupProfileDescr>();
  return this->basebackup(profile);
}

std::shared_ptr<BaseBackupProcess> PGStream::basebackup(std::shared_ptr<BackupProfileDescr> profile) {
  return std::make_shared<BaseBackupProcess>(this->pgconn,
                                             profile);
}

std::string PGStream::generateSlotName() {

  if (!this->isIdentified()) {
    throw StreamingExecutionFailure("could not generate slot name: not identified",
                                    PGRES_FATAL_ERROR);
  }

  std::ostringstream slot_name;

  slot_name << "SLOT-pg_backup_ctl_stream_"
            << this->streamident.id;

  return slot_name.str();

}

std::shared_ptr<PhysicalReplicationSlot> PGStream::createPhysicalReplicationSlot(bool reserve_wal,
                                                                                 bool existing_ok,
                                                                                 bool noident_ok) {

  std::ostringstream query;
  ExecStatusType es;
  PGresult      *result;
  std::string slot_name;
  std::shared_ptr<PhysicalReplicationSlot> slot;

  /*
   * Connected?
   */
  if (!this->connected()) {
    throw StreamingConnectionFailure("could not create replication slot: not connected",
                                     CONNECTION_BAD);
  }

  /*
   * We need to be IDENTIFIED
   */
  if (!this-!isIdentified()
      && !noident_ok) {
    throw StreamingExecutionFailure("could not create replication slot: not identified",
                                    PGRES_FATAL_ERROR);
  }

  /*
   * Assign the slot name to current ident
   */
  this->streamident.slot_name = this->generateSlotName();

  query << "CREATE_REPLICATION_SLOT "
        << slot_name
        << " PHYSICAL";

  if (reserve_wal) {
    query << " RESERVE_WAL";
  }

  result = PQexec(this->pgconn, query.str().c_str());
  std::string sqlstate(PQresultErrorField(result, PG_DIAG_SQLSTATE));

  /*
   * Check state of the returned result. If the slot already exists
   * and existing_ok is set to TRUE, ignore the duplicate_object SQLSTATE.
   */
  if ((es = PQresultStatus(result)) != PGRES_TUPLES_OK) {
    if (sqlstate.compare(ERRCODE_DUPLICATE_OBJECT) == 0) {
      if (!existing_ok) {
        std::ostringstream oss;
        oss << "replication slot " << slot_name
            << " already exists";
        throw StreamingExecutionFailure(oss.str(), es,
                                        ERRCODE_DUPLICATE_OBJECT);
      }
    }
  }

  /*
   * If the query returned successfully, we get a single row result
   * set with 4 cols.
   */
  if(PQntuples(result) != 1) {
    throw StreamingExecutionFailure("cannot create replication slot: unexpected number of rows",
                                    es,
                                    sqlstate);
  }

  if (PQnfields(result) < 3) {
    throw StreamingExecutionFailure("connect create replication slot: unexpected number of columns",
                                    es,
                                    sqlstate);
  }

  slot = std::make_shared<PhysicalReplicationSlot>();
  slot->slot_name = std::string(PQgetvalue(result, 0,
                                           PQfnumber(result,
                                                     "slot_name")));

  if (this->isIdentified()) {
    this->streamident.slot = slot;
  }

  return slot;

}
