#include <istream>
#include <boost/log/trivial.hpp>

/*
 * For PGStream::generateSlotNameUUID() ...
 */
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

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

XLogRecPtr StreamIdentification::xlogposDecoded() {
  return PGStream::decodeXLOGPos(this->xlogpos);
}

int StreamIdentification::updateStartSegmentWriteOffset() {

#if PG_VERSION_NUM < 110000
  this->write_pos_start_offset
    += this->write_position % XLOG_SEG_SIZE;
#else
  this->write_pos_start_offset
    += XLogSegmentOffset(this->write_position,
                         this->wal_segment_size);
#endif

  return this->write_pos_start_offset;

}

std::string StreamIdentification::xlogposEncoded() {
  return this->xlogpos;
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

int PGStream::compiledPGVersionNum() {
  return PG_VERSION_NUM;
}

int PGStream::XLOGOffset(XLogRecPtr pos) {

#if PG_VERSION_NUM >= 110000
  return XLogSegmentOffset(PGStream::decodeXLOGPos(this->streamident.xlogpos),
                           this->walSegmentSize);
#else
  return pos % this->walSegmentSize;
#endif

}

int PGStream::XLOGOffset(XLogRecPtr pos,
                         uint32_t wal_segment_size) {

#if PG_VERSION_NUM >= 110000
  return XLogSegmentOffset(pos, wal_segment_size);
#else
  return pos % wal_segment_size;
#endif

}

XLogRecPtr PGStream::XLOGSegmentStartPosition(XLogRecPtr pos) {

#if PG_VERSION_NUM < 110000
  return pos - (pos % XLOG_SEG_SIZE);
#else
  return pos - XLogSegmentOffset(pos, this->walSegmentSize);
#endif

}

XLogRecPtr PGStream::XLOGSegmentStartPosition(XLogRecPtr pos,
                                              uint32_t wal_segment_size) {

#if PG_VERSION_NUM < 110000
  return pos - (pos % wal_segment_size);
#else
  return pos - XLogSegmentOffset(pos, wal_segment_size);
#endif

}

XLogRecPtr PGStream::XLOGNextSegmentStartPosition(XLogRecPtr pos) {

#if PG_VERSION_NUM < 110000
  return (pos - (pos % XLOG_SEG_SIZE)) + XLOG_SEG_SIZE +1;
#else
  return (pos - XLogSegmentOffset(pos, this->walSegmentSize)) + this->walSegmentSize + 1;
#endif

}

XLogRecPtr PGStream::XLOGNextSegmentStartPosition(XLogRecPtr pos,
                                                  uint32_t wal_segment_size) {

#if PG_VERSION_NUM < 110000
  return (pos - (pos % wal_segment_size)) + wal_segment_size +1;
#else
  return (pos - XLogSegmentOffset(pos, wal_segment_size)) + wal_segment_size + 1;
#endif

}

XLogRecPtr PGStream::XLOGPrevSegmentStartPosition(XLogRecPtr pos) {

  XLogRecPtr prev_recptr = InvalidXLogRecPtr;

  /*
   * Get the starting position of the current segment
   * file, move the recptr then one byte backward and calculate
   * the offset again. This should give us the starting offset
   * of the previous XLOG segment the given XLogRecPtr belongs to.
   */

#if PG_VERSION_NUM < 110000

  prev_recptr = (pos - (pos % this->walSegmentSize)) - 1;
  prev_recptr = (prev_recptr - (prev_recptr % this->walSegmentSize));

#else

  prev_recptr = (pos - XLogSegmentOffset(pos, this->walSegmentSize)) - 1 ;
  prev_recptr = (prev_recptr - XLogSegmentOffset(prev_recptr, this->walSegmentSize));

#endif

  return prev_recptr;

}

XLogRecPtr PGStream::XLOGPrevSegmentStartPosition(XLogRecPtr pos,
                                                  uint32_t wal_segment_size) {

  XLogRecPtr prev_recptr = InvalidXLogRecPtr;

  /*
   * Get the starting position of the current segment
   * file, move the recptr then one byte backward and calculate
   * the offset again. This should give us the starting offset
   * of the previous XLOG segment the given XLogRecPtr belongs to.
   */

#if PG_VERSION_NUM < 110000

  prev_recptr = (pos - (pos % wal_segment_size)) - 1;
  prev_recptr = (prev_recptr - (prev_recptr % wal_segment_size));

#else

  prev_recptr = (pos - XLogSegmentOffset(pos, wal_segment_size)) - 1 ;
  prev_recptr = (prev_recptr - XLogSegmentOffset(prev_recptr, wal_segment_size));

#endif

  return prev_recptr;

}

std::string PGStream::encodeXLOGPos(XLogRecPtr pos) {

  std::string result = "";
  std::ostringstream oss;
  unsigned int hi, lo;

  hi = (uint32_t)(pos >> 32);
  lo = (uint32_t)(pos);

  oss << std::hex << hi << "/" << std::hex << lo;

  /* make sure we have a copy */
  result = oss.str();
  return result;
}

XLogRecPtr PGStream::decodeXLOGPos(std::string pos) {

  uint32_t     hi, lo;

  if (sscanf(pos.c_str(), "%X/%X", &hi, &lo) != 2) {
    std::ostringstream oss;
    oss << "could not parse XLOG location string: " << pos;
    throw StreamingFailure(oss.str());
  }

  return ((uint64_t) hi << 32) | lo;
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

void PGStream::setBlocking() {

  if (this->connected()) {
    PQsetnonblocking(this->pgconn, 0);
  } else {
    throw StreamingFailure("cannot set blocking mode: not connected to a PostgreSQL server");
  }

}

void PGStream::setNonBlocking() {

  if (this->connected()) {
    PQsetnonblocking(this->pgconn, 1);
  }
  else {
    throw StreamingFailure("cannot set non-blocking mode: not connected to a PostgreSQL server");
  }
}

PGPing PGStream::testConnection(bool no_throw) {

  PGPing result;

  /*
   * Check whether we need to connect via
   * DSN string or connection parameters. See connect()
   * for details
   */
  if (this->descr->coninfo->dsn.length()) {
    result = PQping(this->descr->coninfo->dsn.c_str());
  } else {

    ostringstream conninfo;

    conninfo << "host=" << this->descr->coninfo->pghost;
    conninfo << " " << "dbname=" << this->descr->coninfo->pgdatabase;
    conninfo << " " << "user=" << this->descr->coninfo->pguser;
    conninfo << " " << "port=" << this->descr->coninfo->pgport;
    conninfo << " " << "replication=true";

    result = PQping(conninfo.str().c_str());
  }

  if (!no_throw) {

    if (result == PQPING_REJECT)
      throw StreamingConnectionFailure("backup source currently doesn't accept connections");

    if (result == PQPING_NO_RESPONSE)
      throw StreamingConnectionFailure("backup source is not reachable");

    if (result == PQPING_NO_ATTEMPT)
      throw StreamingConnectionFailure("the connection to backup source cannot be established");

  }

  return result;
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

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "DSN is " << this->descr->coninfo->dsn;
#endif

  if (this->descr->coninfo->dsn.length() > 0) {

    BOOST_LOG_TRIVIAL(info) << "using database DSN for connection";
    conninfo << this->descr->coninfo->dsn
             << " "
             << "replication=true";

  } else {

    conninfo << "host=" << this->descr->coninfo->pghost;
    conninfo << " " << "dbname=" << this->descr->coninfo->pgdatabase;
    conninfo << " " << "user=" << this->descr->coninfo->pguser;
    conninfo << " " << "port=" << this->descr->coninfo->pgport;
    conninfo << " " << "replication=true";

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

  /*
   * Initialize streaming connection properties.
   */
  this->walSegmentSize = walSegmentSizeInternal();
}

void PGStream::disconnect() {

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

uint32_t PGStream::getWalSegmentSize() {
  return this->walSegmentSize;
}

uint32_t PGStream::walSegmentSizeInternal() {

  if (!this->connected()) {
    throw StreamingFailure("stream is not connected");
  }

  if (this->getServerVersion() < 100000) {

    /*
     * Check out if we build against an older version of
     * PostgreSQL (we support down to 9.6). 10 doesn't have
     * configurable XLOG segment sizes during initdb, so make
     * sure we use the right hardcoded setting here.
     *
     * Otherwise we use the hardcoded DEFAULT_XLOG_SEG_SIZE setting,
     * which should always return the right segment size on newer
     * PostgreSQL instances.
     *
     * Building against 9.6 isn't really recommened, but we
     * try to support this, nevertheless.
     */
#if PG_VERSION_NUM < 110000
    return XLOG_SEG_SIZE;
#else
    return DEFAULT_XLOG_SEG_SIZE;
#endif
  } else {

    /*
     * PostgreSQL 10 supports SHOW via the
     * streaming replication protocol.
     */
    string walsegsizestr = this->getServerSetting("wal_segment_size");

    /*
     * The unit string returned by SHOW.
     */
    string unit;

    /*
     * XLOG segment size in bytes.
     */
    uint32_t result = -1;

    /*
     * We need the segment size in bytes. SHOW returns
     * a unit indicator, so we need to calculate the bytes
     * according to that.
     */
    stringstream walsegsizestream(walsegsizestr);

    /*
     * Multiplier used to calculate correct segment size.
     */
    int multiplier = 1;

    /*
     * Use a stream to format input variables for calculation.
     */
    walsegsizestream >> result >> unit;

    if (walsegsizestream.fail())
      throw StreamingFailure("could not get XLOG segment size: conversion from SHOW command failed");

    if (unit == "MB")
      multiplier = 1024 * 1024;
    else if (unit == "GB")
      multiplier = 1024 * 1024 * 1024;

    result *= multiplier;

#if PG_VERSION_NUM >= 110000
    if (!IsValidWalSegSize(result))
      throw StreamingFailure("invalid XLOG segment size reported by server: " + result);
#else
    /*
     * PostgreSQL < 11 doesn't have IsValidWalSegSize(), so we do
     * a "not so smart" checking whether the reported segment size
     * is a power of two.
     */
    if (( result % 2) != 0)
      throw StreamingFailure("invalid XLOG segment size: needs to be a power of two");
#endif

    this->streamident.wal_segment_size = result;
    return result;
  }
}

std::string PGStream::getServerSetting(std::string name) {

  ExecStatusType es;
  PGresult *result;
  std::string value;
  std::ostringstream query;

  if (!this->connected()) {
    throw StreamingFailure("stream is not connected");
  }

  /*
   * SHOW is supported since PostgreSQL 10.
   */
  if (this->getServerVersion() < 100000) {
    throw StreamingFailure("SHOW requires a PostgreSQL server version >= 10.0");
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

void PGStream::identify(StreamIdentification &ident) {

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
  ident.systemid
    = std::string(PQgetvalue(result, 0, PQfnumber(result, "systemid")));
  ident.timeline
    = CPGBackupCtlBase::strToInt(std::string(PQgetvalue(result, 0,
                                                        PQfnumber(result, "timeline"))));
  ident.xlogpos
    = std::string(PQgetvalue(result, 0, PQfnumber(result, "xlogpos")));
  ident.dbname
    = std::string(PQgetvalue(result, 0, PQfnumber(result, "dbname")));

  ident.create_date
    = CPGBackupCtlBase::current_timestamp();

  this->identified = true;
  PQclear(result);

}

void PGStream::identify() {

  this->identify(this->streamident);

}

std::shared_ptr<WALStreamerProcess> PGStream::walstreamer() {

  return std::make_shared<WALStreamerProcess>(this->pgconn,
                                              this->streamident);

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
                                             profile,
                                             this->streamident.systemid,
                                             this->getWalSegmentSize());
}

std::string PGStream::generateSlotNameUUID(std::string prefix) {

  std::ostringstream slot_name;
  boost::uuids::uuid my_uuid = boost::uuids::random_generator()();
  std::string my_uuid_str;

  if (!this->isIdentified()) {
    throw StreamingExecutionFailure("could not generate slot name: not identified",
                                    PGRES_FATAL_ERROR);
  }

  slot_name << prefix << "_" << my_uuid;
  my_uuid_str = slot_name.str();

  CPGBackupCtlBase::strReplaceAll(my_uuid_str, "-", "_");

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "generated SLOT name " << my_uuid_str;
#endif

  this->streamident.slot_name = my_uuid_str;
  return my_uuid_str;

}

void
PGStream::createPhysicalReplicationSlot(std::shared_ptr<PhysicalReplicationSlot> slot) {

  std::ostringstream query;
  ExecStatusType es;
  PGresult      *result;

  /*
   * Have a valid slot handle?
   */
  if (slot == nullptr)
    throw StreamingFailure("cannot create physical replication slot: invalid slot handle");

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
  if (!this->isIdentified()
      && !slot->no_identok) {
    throw StreamingExecutionFailure("could not create replication slot: not identified",
                                    PGRES_FATAL_ERROR);
  }

  if (this->streamident.slot_name.length() <= 0) {
    throw StreamingFailure("replication slot name empty, use generateSlotNameUUID() to generate one");
  }

  slot->status = REPLICATION_SLOT_ERROR;

  query << "CREATE_REPLICATION_SLOT "
        << this->streamident.slot_name
        << " PHYSICAL";

  if (slot->reserve_wal) {
    query << " RESERVE_WAL";
  }

  result = PQexec(this->pgconn, query.str().c_str());

  /*
   * Check state of the returned result. If the slot already exists
   * and existing_ok is set to TRUE, ignore the duplicate_object SQLSTATE.
   */
  if ((es = PQresultStatus(result)) != PGRES_TUPLES_OK) {

    std::string sqlstate(PQresultErrorField(result, PG_DIAG_SQLSTATE));

    if (sqlstate.compare(ERRCODE_DUPLICATE_OBJECT) == 0) {
      if (!slot->existing_ok) {
        std::ostringstream oss;
        oss << "replication slot "
            << this->streamident.slot_name
            << " already exists";
        PQclear(result);
        throw StreamingExecutionFailure(oss.str(), es,
                                        ERRCODE_DUPLICATE_OBJECT);
      } else {
        /* Flag this slot that it already exists */
        slot->status = REPLICATION_SLOT_EXISTS;
      }
    } else {
      std::ostringstream oss;
      oss << "cannot create replication slot "
          << PQerrorMessage(this->pgconn);
      PQclear(result);
      throw StreamingExecutionFailure(oss.str(), es, sqlstate);
    }
  } else {
    /* everything seems ok, flag the slot descriptor accordingly. */
    slot->status = REPLICATION_SLOT_OK;
  }

  /*
   * Since we also claim to support PostgreSQL 9.6, we must
   * be careful here, since 9.6 servers don't reply to this command.
   */
  if (this->getServerVersion() < 90600) {
    PQclear(result);
    return;
  }

  /*
   * If the slot status was previously set to REPLICATION_SLOT_EXISTS,
   * we aren't going to bother here any longer. Reuse the existing slot.
   *
   * XXX: One might argue that we could possibly hijack another
   *      slot accidently having the same name. Though, i believe
   *      no one will use our internal identifier, but nevertheless, i agree,
   *      this possibility exists.
   */
  if (slot->status == REPLICATION_SLOT_OK) {
    /*
     * If the query returned successfully, we get a single row result
     * set with 4 cols.
     */
    if(PQntuples(result) != 1) {
      PQclear(result);
      throw StreamingFailure("cannot create replication slot: unexpected number of rows");
    }

    if (PQnfields(result) < 3) {
      PQclear(result);
      throw StreamingFailure("connect create replication slot: unexpected number of columns");
    }

    slot->slot_name = std::string(PQgetvalue(result, 0, 0));
    slot->consistent_point = std::string(PQgetvalue(result, 0, 1));

    if (this->isIdentified()) {
      this->streamident.slot = slot;
      this->streamident.slot->slot_name = this->streamident.slot_name;
    }
  }

  PQclear(result);

}
