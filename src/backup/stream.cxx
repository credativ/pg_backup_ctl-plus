#include <istream>

#include <stream.hxx>

using namespace credativ::streaming;

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

XLogRecPtr StreamIdentification::getXLOGStartPos()
  throw (StreamingFailure) {
  return PGStream::decodeXLOGPos(this->xlogpos);
}

XLogRecPtr PGStream::decodeXLOGPos(std::string pos)
  throw(StreamingFailure) {

  XLogRecPtr result;
  uint32     hi, lo;

  if (sscanf(pos.c_str(), "", &hi, &lo) != 2) {
    std::ostringstream oss;
    oss << "could not parse XLOG location string: " << pos;
    throw StreamingFailure(oss.str());
  }

  return ((uint64) hi) << 32 | lo;
}

int PGStream::getServerVersion()
  throw(StreamingConnectionFailure) {

  if (!this->connected()) {
    throw StreamingConnectionFailure("could not get server version: not connected", CONNECTION_BAD);
  } else {

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

PGStream::~PGStream() {}

void PGStream::setPGConnection(PGconn *conn) {
  this->pgconn = conn;
}

void PGStream::connect()
  throw(StreamingConnectionFailure) {

  ConnStatusType cs;

  if (this->descr == nullptr) {
    throw StreamingFailure("could not establish streaming connection with undefined catalog");
  }

  /*
   * Build connection string.
   */
  std::ostringstream conninfo;

  conninfo << "host=" << this->descr->pghost;
  conninfo << " " << "dbname=" << this->descr->pgdatabase;
  conninfo << " " << "user=" << this->descr->pguser;
  conninfo << " " << "port=" << this->descr->pgport;
  conninfo << " " << "replication=database";

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

void PGStream::identify()
  throw(StreamingExecutionFailure) {

  ExecStatusType es;
  PGresult      *result;
  const char *query = "IDENTIFY SYSTEM";

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
    std::ostringstream oss;
    oss << "IDENTIFY SYSTEM failed: " << PQresultErrorMessage(result);
    PQclear(result);
    throw StreamingExecutionFailure(oss.str(), es);
  }

  /* sanity check, expected number of rows/cols ? */
  if (PQnfields(result) < 4) {
    throw StreamingExecutionFailure("unexpected number of system identification columns",
                                    es);
  }

  if (PQntuples(result) != 1) {
    throw StreamingExecutionFailure("expected number of rows for system identification",
                                    es);
  }

  /*
   * Read properties into internal identification object.
   */
  this->streamident.systemid
    = std::string(PQgetvalue(result, 0, PQfnumber(result, "systemid")));
  this->streamident.timeline = CPGBackupCtlBase::strToInt(std::string(PQgetvalue(result, 0, PQfnumber(result, "timeline"))));
  this->streamident.xlogpos
    = std::string(PQgetvalue(result, 0, PQfnumber(result, "xlogpos")));
  this->streamident.dbname
    = std::string(PQgetvalue(result, 0, PQfnumber(result, "dbname")));

  this->identified = true;
}
