#include <sstream>
/* required for string case insensitive comparison */
#include <boost/algorithm/string/predicate.hpp>

#include <catalog.hxx>
#include <BackupCatalog.hxx>
#include <stream.hxx>

using namespace credativ;
using namespace std;

/*
 * Map col id to string.
 */

/*
 * Keep indexes in sync with macros from include/catalog/catalog.hxx !!
 */

std::vector<std::string> BackupCatalog::archiveCatalogCols =
  {
    "id",
    "name",
    "directory",
    "compression"
  };

std::vector<std::string> BackupCatalog::connectionsCatalogCols =
  {
    "archive_id",
    "type",
    "dsn",
    "pghost",
    "pgport",
    "pguser",
    "pgdatabase"
  };

std::vector<std::string> BackupCatalog::backupCatalogCols =
  {
    "id",
    "archive_id",
    "xlogpos",
    "xlogposend",
    "timeline",
    "label",
    "fsentry",
    "started",
    "stopped",
    "pinned",
    "status"
  };

std::vector<std::string> BackupCatalog::streamCatalogCols =
  {
    "id",
    "archive_id",
    "stype",
    "slot_name",
    "systemid",
    "timeline",
    "xlogpos",
    "dbname",
    "status",
    "create_date"
  };

std::vector<std::string>BackupCatalog::backupProfilesCatalogCols =
  {
    "id",
    "name",
    "compress_type",
    "max_rate",
    "label",
    "fast_checkpoint",
    "include_wal",
    "wait_for_wal"
  };

std::vector<std::string>BackupCatalog::backupTablespacesCatalogCols =
  {
    "id",
    "backup_id",
    "spcoid",
    "spclocation",
    "spcsize"
  };

std::vector<std::string>BackupCatalog::procsCatalogCols =
  {
    "pid",
    "archive_id",
    "type",
    "started",
    "state"
  };

const char * STREAM_BASEBACKUP = "BASEBACKUP";
const char * STREAM_PROGRESS_IDENTIFIED = "IDENTIFIED";

PushableCols::PushableCols() {}
PushableCols::~PushableCols() {}

void PushableCols::pushAffectedAttribute(int colId) {

  this->affectedAttributes.push_back(colId);

}

std::vector<int> PushableCols::getAffectedAttributes() {
  return affectedAttributes;
}

void PushableCols::clearAffectedAttributes() {
  this->affectedAttributes.clear();
}

void PushableCols::setAffectedAttributes(std::vector<int> affectedAttributes) {
  this->affectedAttributes = affectedAttributes;
}

std::string StatCatalogArchive::gimmeFormattedString() {
  std::ostringstream formatted;

  formatted << CPGBackupCtlBase::makeHeader("Archive Catalog Overview",
                                            boost::format("%-15s\t%-30s\t%-20s")
                                            % "Name" % "Directory" % "Host", 80);
  formatted << boost::format("%-15s\t%-30s\t%-20s")
    % this->archive_name % this->archive_directory % this->archive_host;
  formatted << endl;

  /*
   * Catalog statistics data.
   */
  formatted << CPGBackupCtlBase::makeHeader("Backups",
                                            boost::format("%-9s\t%-9s\t%-9s\t%-16s")
                                            % "# total" % "# failed"
                                            % "# running" % "avg duration (s)", 80);
  formatted << boost::format("%-9s\t%-9s\t%-9s\t%-16s")
    % this->number_of_backups % this->backups_failed
    % this->backups_running
    % this->avg_backup_duration;
  formatted << endl;

  return formatted.str();
}

CatalogDescr& CatalogDescr::operator=(const CatalogDescr& source) {

  this->tag = source.tag;
  this->id = source.id;
  this->archive_name = source.archive_name;
  this->label = source.label;
  this->compression = source.compression;
  this->directory = source.directory;
  this->coninfo->pghost = source.coninfo->pghost;
  this->coninfo->pgport = source.coninfo->pgport;
  this->coninfo->pguser = source.coninfo->pguser;
  this->coninfo->pgdatabase = source.coninfo->pgdatabase;
  this->coninfo->dsn = source.coninfo->dsn;
  this->coninfo->type = source.coninfo->type;

}

void CatalogDescr::setJobDetachMode(bool const& detach) {
  this->detach = detach;
}

void CatalogDescr::setProfileAffectedAttribute(int const& colId) {
  this->backup_profile->pushAffectedAttribute(colId);
}

void CatalogDescr::setProfileWaitForWAL(bool const& wait) {
  this->backup_profile->wait_for_wal = wait;
  this->backup_profile->pushAffectedAttribute(SQL_BCK_PROF_WAIT_FOR_WAL_ATTNO);
}

void CatalogDescr::setProfileCheckpointMode(bool const& fastmode) {
  this->backup_profile->fast_checkpoint = fastmode;
  this->backup_profile->pushAffectedAttribute(SQL_BCK_PROF_FAST_CHKPT_ATTNO);
}

void CatalogDescr::setProfileWALIncluded(bool const& included) {
  this->backup_profile->include_wal = included;
  this->backup_profile->pushAffectedAttribute(SQL_BCK_PROF_INCL_WAL_ATTNO);
}

void CatalogDescr::setProfileBackupLabel(std::string const& label) {
  this->backup_profile->label = label;
  this->backup_profile->pushAffectedAttribute(SQL_BCK_PROF_LABEL_ATTNO);
}

void CatalogDescr::setProfileMaxRate(std::string const& max_rate) {
  this->backup_profile->max_rate = CPGBackupCtlBase::strToInt(max_rate);
  this->pushAffectedAttribute(SQL_BCK_PROF_MAX_RATE_ATTNO);
}

void CatalogDescr::setProfileCompressType(BackupProfileCompressType const& type) {
  this->backup_profile->compress_type = type;
  this->pushAffectedAttribute(SQL_BCK_PROF_COMPRESS_TYPE_ATTNO);
}

std::shared_ptr<BackupProfileDescr> CatalogDescr::getBackupProfileDescr() {
  return this->backup_profile;
}

void CatalogDescr::setProfileName(std::string const& profile_name) {
  this->backup_profile->name = profile_name;
  this->pushAffectedAttribute(SQL_BCK_PROF_NAME_ATTNO);
}

void CatalogDescr::setDbName(std::string const& db_name) {
  this->coninfo->pgdatabase = db_name;
  this->pushAffectedAttribute(SQL_ARCHIVE_PGDATABASE_ATTNO);
}

void CatalogDescr::setCommandTag(credativ::CatalogTag const& tag) {
  this->tag = tag;

  switch(tag) {
  case CREATE_ARCHIVE:
  case ALTER_ARCHIVE:
  case DROP_ARCHIVE:
    this->coninfo->type = ConnectionDescr::CONNECTION_TYPE_BASEBACKUP;
    break;
  default:
    this->coninfo->type = ""; /* force errors later */
  }

}

void CatalogDescr::setIdent(std::string const& ident) {
  this->archive_name = ident;
  this->pushAffectedAttribute(SQL_ARCHIVE_NAME_ATTNO);
}

void CatalogDescr::setConnectionType(std::string const& type) {
  this->coninfo->type = type;
  this->coninfo->pushAffectedAttribute(SQL_CON_TYPE_ATTNO);
}

void CatalogDescr::setHostname(std::string const& hostname) {
  this->coninfo->pghost = hostname;
  this->coninfo->pushAffectedAttribute(SQL_CON_PGHOST_ATTNO);
}

void CatalogDescr::setUsername(std::string const& username) {
  this->coninfo->pguser = username;
  this->coninfo->pushAffectedAttribute(SQL_CON_PGUSER_ATTNO);
}

void CatalogDescr::setPort(std::string const& portNumber) {
  this->coninfo->pgport = CPGBackupCtlBase::strToInt(portNumber);
  this->coninfo->pushAffectedAttribute(SQL_CON_PGPORT_ATTNO);
}

void CatalogDescr::setDirectory(std::string const& directory) {
  this->directory = directory;
}

void CatalogDescr::setArchiveId(int const& archive_id) {
  this->id = archive_id;
  this->coninfo->archive_id = this->id;

  this->pushAffectedAttribute(SQL_ARCHIVE_ID_ATTNO);
  this->coninfo->pushAffectedAttribute(SQL_CON_ARCHIVE_ID_ATTNO);
}

void CatalogDescr::setDSN(std::string const& dsn) {

  std::vector<int> attrs;

  this->coninfo->dsn = dsn;

  /*
   * DSN assignment causes to invalidate any other
   * connection parameter assigned directly.
   */
  this->coninfo->pghost = "";
  this->coninfo->pgport = -1;
  this->coninfo->pguser = "";
  this->coninfo->pgdatabase = "";

  /*
   * NOTE: we also need to remove them from the attributes list.
   */

  /*
   * Copy over any element not referencing direct
   * connection settings.
   */
  for(auto& colId : this->coninfo->getAffectedAttributes()) {

    switch(colId) {
    case SQL_CON_ARCHIVE_ID_ATTNO:
    case SQL_CON_TYPE_ATTNO:
      /* fall through until here */
      attrs.push_back(colId);
      break;
    default:
      break;
    }
  }

  attrs.push_back(SQL_CON_DSN_ATTNO);
  this->coninfo->setAffectedAttributes(attrs);

}

BackupCatalog::BackupCatalog() {
  this->isOpen = false;
  this->db_handle = NULL;
}

BackupCatalog::BackupCatalog(string sqliteDB) {
  this->isOpen = false;
  this->db_handle = NULL;

  /*
   * Identifiers
   */
  this->sqliteDB = sqliteDB;

  /*
   * Initialize/open catalog database.
   */
  this->open_rw();

  /*
   * Check catalog ...
   */
  this->checkCatalog();
}

std::string BackupCatalog::name() {
  return this->sqliteDB;
}

void BackupCatalog::startTransaction() {

  int rc;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  rc = sqlite3_exec(this->db_handle,
                    "BEGIN;",
                    NULL,
                    NULL,
                    NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "error starting catalog transaction: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }
}

void BackupCatalog::commitTransaction() {

  int rc;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  rc = sqlite3_exec(this->db_handle,
                    "COMMIT;",
                    NULL, NULL, NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "error committing catalog transaction: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

}

void BackupCatalog::rollbackTransaction() {

  int rc;

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  rc = sqlite3_exec(this->db_handle,
                    "ROLLBACK;",
                    NULL, NULL, NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "error rollback catalog transaction: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

}

std::shared_ptr<CatalogProc> BackupCatalog::fetchCatalogProcData(sqlite3_stmt *stmt,
                                                                 std::vector<int> affectedAttributes) {

  int currindex = 0; /* SQLite3 column index starts at zero! */
  std::shared_ptr<CatalogProc> procInfo = std::make_shared<CatalogProc>();

  if (stmt == NULL) {
    throw CCatalogIssue("cannot fetch proc information from catalog: invalid statement handle");
  }

  for (auto& colId : affectedAttributes) {

    switch (colId) {

    case SQL_PROCS_PID_ATTNO:
      procInfo->pid = sqlite3_column_int(stmt, currindex);
      break;

    case SQL_PROCS_ARCHIVE_ID_ATTNO:
      procInfo->archive_id = sqlite3_column_int(stmt, currindex);
      break;

    case SQL_PROCS_TYPE_ATTNO:
      /* can't be NULL, but for safety reason do a NULL check */
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        procInfo->type = (char *) sqlite3_column_text(stmt, currindex);
      break;

    case SQL_PROCS_STARTED_ATTNO:
      /* can't be NULL, but for safety reasons do a NULL check */
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        procInfo->started = (char *) sqlite3_column_text(stmt, currindex);
      break;

    case SQL_PROCS_STATE_ATTNO:
      /* can't be NULL, but for safety reasons do a NULL check */
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        procInfo->state = (char *) sqlite3_column_text(stmt, currindex);
      break;
    default:
      /* oops, should we throw a CCatalogIssue exception ? */
      break;
    }

    currindex++;
  }

  return procInfo;
}

void BackupCatalog::fetchConnectionData(sqlite3_stmt *stmt,
                                        std::shared_ptr<ConnectionDescr> conDescr) {
  int currindex = 0; /* SQLite3 column index starts at zero! */

  if (stmt == NULL)
    throw CCatalogIssue("cannot fetch connection information from catalog: invalid statement handle");

  if (conDescr == nullptr)
    throw CCatalogIssue("cannot fetch connection information from catalog: invalid connection handle");

  for (auto& colId : conDescr->getAffectedAttributes()) {
    switch(colId) {

    case SQL_CON_ARCHIVE_ID_ATTNO:
      conDescr->archive_id = sqlite3_column_int(stmt, currindex);
      break;

    case SQL_CON_TYPE_ATTNO:
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        conDescr->type = (char *) sqlite3_column_text(stmt, currindex);
      break;
    case SQL_CON_PGHOST_ATTNO:
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        conDescr->pghost = (char *) sqlite3_column_text(stmt, currindex);
      break;
    case SQL_CON_PGPORT_ATTNO:
      conDescr->pgport = sqlite3_column_int(stmt, currindex);
      break;
    case SQL_CON_PGUSER_ATTNO:
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        conDescr->pguser = (char *) sqlite3_column_text(stmt, currindex);
      break;
    case SQL_CON_PGDATABASE_ATTNO:
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        conDescr->pgdatabase = (char *) sqlite3_column_text(stmt, currindex);
      break;
    case SQL_CON_DSN_ATTNO:
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL) {
        conDescr->dsn = (char *) sqlite3_column_text(stmt, currindex);
        cout << "fetch DSN " << conDescr->dsn << endl;
      }
      break;
    default:
      /* oops, should we throw a CCatalogIssue exception ? */
      break;
    }

    currindex++;
  }

}

std::shared_ptr<StreamIdentification> BackupCatalog::fetchStreamData(sqlite3_stmt *stmt,
                                                                     std::vector<int> affectedRows) {

  int currindex = 0; /* column index starts at 0 */
  std::shared_ptr<StreamIdentification> ident(nullptr);

  if (stmt == NULL)
    throw("cannot fetch stream data: uninitialized statement handle");

  ident = make_shared<StreamIdentification>();

  for(auto& colId : affectedRows) {

    switch (colId) {
    case SQL_STREAM_ID_ATTNO:
      ident->id = sqlite3_column_int(stmt, currindex);
      break;
    case SQL_STREAM_ARCHIVE_ID_ATTNO:
      ident->archive_id = sqlite3_column_int(stmt, currindex);
      break;
    case SQL_STREAM_STYPE_ATTNO:
      ident->stype = (char *) sqlite3_column_text(stmt, currindex);
      break;
    case SQL_STREAM_SLOT_NAME_ATTNO:
      /*
       * NOTE: can be NULL!
       */
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        ident->slot_name = (char *) sqlite3_column_text(stmt, currindex);
      break;
    case SQL_STREAM_SYSTEMID_ATTNO:
      ident->systemid = sqlite3_column_int(stmt, currindex);
      break;
    case SQL_STREAM_TIMELINE_ATTNO:
      ident->timeline = sqlite3_column_int(stmt, currindex);
      break;
    case SQL_STREAM_XLOGPOS_ATTNO:
      ident->xlogpos = (char *) sqlite3_column_text(stmt, currindex);
      break;
    case SQL_STREAM_DBNAME_ATTNO:
      ident->dbname = (char *) sqlite3_column_text(stmt, currindex);
      break;
    case SQL_STREAM_STATUS_ATTNO:
      ident->status = (char *) sqlite3_column_text(stmt, currindex);
      break;
    case SQL_STREAM_REGISTER_DATE_ATTNO:
      ident->create_date = (char *) sqlite3_column_text(stmt, currindex);
      break;
    default:
      /* oops */
      break;
    }

    currindex++;

  }

  return ident;

}

std::shared_ptr<BackupProfileDescr> BackupCatalog::fetchBackupProfileIntoDescr(sqlite3_stmt *stmt,
                                                                               std::shared_ptr<BackupProfileDescr> descr,
                                                                               Range colIdRange) {
  if (stmt == NULL)
    throw("cannot fetch archive data: uninitialized statement handle");

  if (descr == nullptr)
    throw("cannot fetch archive data: invalid descriptor handle");

  std::vector<int> attr = descr->getAffectedAttributes();
  int current_stmt_col = colIdRange.start();

  for (int current = 0; current < attr.size(); current++) {

    /*
     * Sanity check, stop if range end is reached.
     */
    if (current_stmt_col > colIdRange.end())
      break;

    switch (attr[current]) {

    case SQL_BCK_PROF_ID_ATTNO:

      descr->profile_id = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_NAME_ATTNO:

      /*
       * name cannot be NULL, so no NULL check required.
       */
      descr->name = (char *)sqlite3_column_text(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_COMPRESS_TYPE_ATTNO:

      /*
       * BackupProfileCompressType is just stored as an integer in the catalog,
       * so we need to explicitely typecast.
       */
      descr->compress_type = (BackupProfileCompressType) sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_MAX_RATE_ATTNO:
      descr->max_rate = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_LABEL_ATTNO:

      /*
       * label cannot be NULL, so no NULL check required.
       */
      descr->label = (char *)sqlite3_column_text(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_FAST_CHKPT_ATTNO:

      descr->fast_checkpoint = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_INCL_WAL_ATTNO:

      descr->include_wal = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_WAIT_FOR_WAL_ATTNO:

      descr->wait_for_wal = sqlite3_column_int(stmt, current_stmt_col);
      break;

    default:
      break;
    }

    current_stmt_col++;
  }

  return descr;

}

shared_ptr<CatalogDescr> BackupCatalog::fetchArchiveDataIntoDescr(sqlite3_stmt *stmt,
                                                                  shared_ptr<CatalogDescr> descr) {

  if (stmt == NULL)
    throw("cannot fetch archive data: uninitialized statement handle");

  if (descr == nullptr)
    throw("cannot fetch archive data: invalid descriptor handle");

  /*
   * Save archive properties into catalog
   * descriptor
   */
  descr->id = sqlite3_column_int(stmt, SQL_ARCHIVE_ID_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_DIRECTORY_ATTNO) != SQLITE_NULL)
    descr->directory = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_DIRECTORY_ATTNO);

  descr->compression = sqlite3_column_int(stmt, SQL_ARCHIVE_COMPRESSION_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_PGHOST_ATTNO) != SQLITE_NULL)
    descr->coninfo->pghost = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_PGHOST_ATTNO);

  descr->coninfo->pgport = sqlite3_column_int(stmt, SQL_ARCHIVE_PGPORT_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_PGUSER_ATTNO) != SQLITE_NULL)
    descr->coninfo->pguser = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_PGUSER_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_PGDATABASE_ATTNO) != SQLITE_NULL)
    descr->coninfo->pgdatabase = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_PGDATABASE_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_NAME_ATTNO) != SQLITE_NULL)
    descr->archive_name = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_NAME_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_DSN_ATTNO) != SQLITE_NULL)
    descr->coninfo->dsn = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_DSN_ATTNO);

  return descr;

}

shared_ptr<CatalogDescr> BackupCatalog::existsByName(std::string name) {

  sqlite3_stmt *stmt;
  int rc;
  shared_ptr<CatalogDescr> result = make_shared<CatalogDescr>();

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /*
   * Check for the specified directory. Note that SQLite3 here
   * uses filesystem locking, so we can't just do row-level
   * locking on our own.
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          "SELECT * FROM archive WHERE name = ?1;",
                          -1,
                          &stmt,
                          NULL);

  sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_STATIC);

  /* ... perform SELECT */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  while(rc == SQLITE_ROW && rc != SQLITE_DONE) {

    this->fetchArchiveDataIntoDescr(stmt, result);
    rc = sqlite3_step(stmt);

  }

  sqlite3_finalize(stmt);

  /* result->id >= 0 means valid result */
  return result;

}

shared_ptr<CatalogDescr> BackupCatalog::exists(std::string directory) {

  sqlite3_stmt *stmt;
  int rc;
  shared_ptr<CatalogDescr> result = make_shared<CatalogDescr>();

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  /*
   * Check for the specified directory. Note that SQLite3 here
   * uses filesystem locking, so we can't just do row-level
   * locking on our own.
   */

  rc = sqlite3_prepare_v2(this->db_handle,
                          "SELECT * FROM archive WHERE directory = ?1;",
                          -1,
                          &stmt,
                          NULL);

  sqlite3_bind_text(stmt, 1, directory.c_str(), -1, SQLITE_STATIC);

  /* ... perform SELECT */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /* ... empty result set or single row expected */
  while (rc == SQLITE_ROW && rc != SQLITE_DONE) {

    this->fetchArchiveDataIntoDescr(stmt, result);
    rc = sqlite3_step(stmt);

  }

  sqlite3_finalize(stmt);

  /* result->id >= 0 means valid result */
  return result;

}

void BackupCatalog::dropBackupProfile(std::string name) {

  sqlite3_stmt *stmt;
  int rc;
  if (!this->available()) {
    throw CCatalogIssue("catalog database not openend");
  }

  rc = sqlite3_prepare_v2(this->db_handle,
                          "DELETE FROM backup_profiles WHERE name = ?1;",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << " cannot prepare query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /* Bind WHERE condition */
  sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_STATIC);

  /* ... and execute ... */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;

    oss << "error dropping backup profile: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::dropArchive(std::string name) {

  sqlite3_stmt *stmt;
  int rc;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  /*
   * Drop the archive by name.
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          "DELETE FROM archive WHERE name = ?1;",
                          -1,
                          &stmt,
                          NULL);

  /*
   * Bind WHERE condition ...
   */
  sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_STATIC);

  /*
   * ... and execute the DELETE statement.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result for DROP ARCHIVE in query: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);

}

std::shared_ptr<StatCatalogArchive> BackupCatalog::statCatalog(std::string archive_name) {

  sqlite3_stmt *stmt;
  std::string query;
  int rc;
  std::shared_ptr<StatCatalogArchive> result = std::make_shared<StatCatalogArchive>();

  /*
   * per default, set the stats result set to be empty.
   */
  result->archive_id = -1;

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  query =
"SELECT "
  "(SELECT COUNT(*) FROM backup WHERE archive_id = a.id) AS number_of_backups, "
  "(SELECT COUNT(*) FROM backup b "
                   "WHERE b.archive_id = a.id "
                         "AND b.status = 'aborted') AS backups_failed, "
  "(SELECT COUNT(*) FROM backup b "
                   "WHERE b.archive_id = a.id "
                         "AND b.status = 'in progress') AS backups_running, "
  "a.id, "
  "a.name, "
  "a.directory, "
  "CASE WHEN length(COALESCE(c.pghost, '')) > 0 THEN c.pghost ELSE c.dsn END AS pghost, "
  "(SELECT SUM(spcsize) FROM backup_tablespaces bt "
                            "JOIN backup b ON b.id = bt.backup_id "
                            "JOIN archive a2 ON a.id = b.archive_id "
                       "WHERE a2.id = a.id) AS approx_sz, "
  "(SELECT MAX(stopped) FROM backup b "
                       "WHERE b.archive_id = a.id) AS latest_finished, "
  "(SELECT CASE WHEN (started IS NOT NULL AND stopped IS NOT NULL) "
          "THEN AVG(CAST((julianday(stopped) - julianday(started)) * 24 * 60 * 60 AS integer)) "
          "ELSE 0 "
          "END AS val_avg_duration "
   "FROM "
   "backup b "
   "WHERE b.archive_id = a.id) AS avg_duration "
"FROM "
  "archive a JOIN connections c ON c.archive_id = a.id "
"WHERE "
  "a.name = ?1 AND c.type = 'basebackup';";

  /*
   * Prepare the query...
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          query.c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind parameters ...
   */
  sqlite3_bind_text(stmt, 1, archive_name.c_str(), -1, SQLITE_STATIC);

  /*
   * ... and execute the query.
   */
  rc = sqlite3_step(stmt);

  /*
   * Only a single line result set expected.
   */
  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    std::ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result in catalog query: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  result->number_of_backups = sqlite3_column_int(stmt, 0);
  result->backups_failed    = sqlite3_column_int(stmt, 1);
  result->backups_running   = sqlite3_column_int(stmt, 2);
  result->archive_id        = sqlite3_column_int(stmt, 3);

  if (sqlite3_column_type(stmt, 4) != SQLITE_NULL)
    result->archive_name      = (char *) sqlite3_column_text(stmt, 4);

  if (sqlite3_column_type(stmt, 5) != SQLITE_NULL)
    result->archive_directory = (char *) sqlite3_column_text(stmt, 5);

  if (sqlite3_column_type(stmt, 6) != SQLITE_NULL)
    result->archive_host      = (char *) sqlite3_column_text(stmt, 6);

  result->estimated_total_size = sqlite3_column_int(stmt, 7);

  if (sqlite3_column_type(stmt, 8) != SQLITE_NULL)
    result->latest_finished      = (char *) sqlite3_column_text(stmt, 8);

  result->avg_backup_duration  = sqlite3_column_int(stmt, 9);

  /*
   * We're done.
   */
  sqlite3_finalize(stmt);

  return result;
}

std::vector<std::shared_ptr<BaseBackupDescr>>
BackupCatalog::getBackupList(shared_ptr<CatalogDescr> descr) {

  sqlite3_stmt *stmt;
  std::ostringstream query;
  int rc;
  std::vector<int> backupAttrs;
  std::vector<int> tblspcAttrs;

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /*
   * Generate list of columns to retrieve...
   */
  backupAttrs.push_back(SQL_BACKUP_ID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_ARCHIVE_ID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_XLOGPOS_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_XLOGPOSEND_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_TIMELINE_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_LABEL_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_FSENTRY_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STARTED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STOPPED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STATUS);

  tblspcAttrs.push_back(SQL_BCK_TBLSPC_ID_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_BCK_ID_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCOID_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCLOC_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCSZ_ATTNO);

  std::string backupCols = BackupCatalog::SQLgetColumnList(SQL_BACKUP_ENTITY,
                                                           backupAttrs);
  std::string tblspcCols = BackupCatalog::SQLgetColumnList(SQL_BACKUP_TBLSPC_ENTITY,
                                                           tblspcAttrs);

#ifdef __DEBUG__
  cerr << backupCols << endl;
  cerr << tblspcCols << endl;
#endif
}

void BackupCatalog::updateArchiveAttributes(shared_ptr<CatalogDescr> descr,
                                            std::vector<int> affectedAttributes) {
  sqlite3_stmt *stmt;
  ostringstream updateSQL;
  int rc;
  int boundCols;

  if(!this->available())
    throw CCatalogIssue("catalog database not opened");

  /*
   * In case affectedAttributes is empty (or NULL), we throw an
   * error.
   */
  if (affectedAttributes.size() <= 0)
    throw CCatalogIssue("cannot update archive attributes with empty attribute list");

  /*
   * Loop through the affected columns list and build
   * a comma separated list of col=? pairs.
   */
  updateSQL << "UPDATE archive SET ";

  /*
   * Use an ordinary indexed loop ...
   */
  for(boundCols = 0; boundCols < affectedAttributes.size(); boundCols++) {

    /* boundCol must start at index 1 !! */
    updateSQL << BackupCatalog::SQLgetUpdateColumnTarget(SQL_ARCHIVE_ENTITY,
                                                         affectedAttributes[boundCols])
              << (boundCols + 1);

    if (boundCols < affectedAttributes.size() - 1) {
      updateSQL << ", ";
    }
  }

  /*
   * ...and attach the where clause
   */
  updateSQL << " WHERE id = ?" << (++boundCols) << ";";

#ifdef __DEBUG__
  cerr << "generate UPDATE SQL " << updateSQL.str() << endl;
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          updateSQL.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  /*
   * Assign bind variables. Please note that we rely
   * on the order of affectedAttributes to match the
   * previously formatted UPDATE SQL string.
   */
  this->SQLbindArchiveAttributes(descr,
                                 affectedAttributes,
                                 stmt,
                                 Range(1, boundCols));

  sqlite3_bind_int(stmt, boundCols,
                   descr->id);

  /*
   * Execute the statement.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "error updating archive in catalog database: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);

  /*
   * Also update connection info, but only
   * if affected attributes are set accordingly.
   */
  if (descr->coninfo->getAffectedAttributes().size() > 0) {
    this->updateCatalogConnection(descr->coninfo,
                                  descr->archive_name,
                                  ConnectionDescr::CONNECTION_TYPE_BASEBACKUP);
  }
}

std::string BackupCatalog::mapAttributeId(int catalogEntity,
                                          int colId) {

  std::string result = "";

  switch(catalogEntity) {

  case SQL_ARCHIVE_ENTITY:
    result = BackupCatalog::archiveCatalogCols[colId];
    break;
  case SQL_BACKUP_ENTITY:
    result = BackupCatalog::backupCatalogCols[colId];
    break;
  case SQL_STREAM_ENTITY:
    result = BackupCatalog::streamCatalogCols[colId];
    break;
  case SQL_BACKUP_PROFILES_ENTITY:
    result = BackupCatalog::backupProfilesCatalogCols[colId];
    break;
  case SQL_BACKUP_TBLSPC_ENTITY:
    result = BackupCatalog::backupTablespacesCatalogCols[colId];
    break;
  case SQL_PROCS_ENTITY:
    result = BackupCatalog::procsCatalogCols[colId];
    break;
  case SQL_CON_ENTITY:
    result = BackupCatalog::connectionsCatalogCols[colId];
    break;
  default:
    {
      std::ostringstream oss;
      oss << "unkown catalog entity: " << catalogEntity;
      throw CCatalogIssue(oss.str());
    }
  }

  return result;
}

string BackupCatalog::SQLgetColumnList(int catalogEntity, std::vector<int> attrs) {

  int i;
  std::ostringstream collist;

  for (i = 0; i < attrs.size(); i++) {
    collist << BackupCatalog::mapAttributeId(catalogEntity, attrs[i]);

    if (i < (attrs.size() - 1))
      collist << ", ";
  }

  return collist.str();

}

string BackupCatalog::SQLgetUpdateColumnTarget(int catalogEntity,
                                               int colId) {
  ostringstream oss;

  oss << BackupCatalog::mapAttributeId(catalogEntity, colId) << "=?";
  return oss.str();
}

std::shared_ptr<std::list<std::shared_ptr<BackupProfileDescr>>>
BackupCatalog::getBackupProfiles() {

  sqlite3_stmt *stmt;
  auto result = make_shared<std::list<std::shared_ptr<BackupProfileDescr>>>();

  /*
   * Build the query.
   */
  ostringstream query;
  Range range(0, 7);

  query << "SELECT id, name, compress_type, max_rate, label, fast_checkpoint, include_wal, wait_for_wal "
        << "FROM backup_profiles ORDER BY name;";

#ifdef __DEBUG__
  cerr << "QUERY: " << query.str() << endl;
#endif

  std::vector<int> attr;
  attr.push_back(SQL_BCK_PROF_ID_ATTNO);
  attr.push_back(SQL_BCK_PROF_NAME_ATTNO);
  attr.push_back(SQL_BCK_PROF_COMPRESS_TYPE_ATTNO);
  attr.push_back(SQL_BCK_PROF_MAX_RATE_ATTNO);
  attr.push_back(SQL_BCK_PROF_LABEL_ATTNO);
  attr.push_back(SQL_BCK_PROF_FAST_CHKPT_ATTNO);
  attr.push_back(SQL_BCK_PROF_INCL_WAL_ATTNO);
  attr.push_back(SQL_BCK_PROF_WAIT_FOR_WAL_ATTNO);

  int rc = sqlite3_prepare_v2(this->db_handle,
                              query.str().c_str(),
                              -1,
                              &stmt,
                              NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Execute the prepared statement
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc!= SQLITE_DONE) {
    ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  try {

    while(rc == SQLITE_ROW) {
      auto item = make_shared<BackupProfileDescr>();
      item->setAffectedAttributes(attr);
      this->fetchBackupProfileIntoDescr(stmt, item, range);
      result->push_back(item);

      rc = sqlite3_step(stmt);
    }

  } catch(exception& e) {
    /* re-throw exception, but don't leak the sqlite statement handle */
    sqlite3_finalize(stmt);
    throw e;
  }

  sqlite3_finalize(stmt);
  return result;
}

std::shared_ptr<BackupProfileDescr> BackupCatalog::getBackupProfile(std::string name) {

  std::shared_ptr<BackupProfileDescr> descr = std::make_shared<BackupProfileDescr>();

  sqlite3_stmt *stmt;
  int rc;
  std::ostringstream query;
  Range range(0, 7);

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /*
   * Build the query
   */
  query << "SELECT id, name, compress_type, max_rate, label, fast_checkpoint, include_wal, wait_for_wal "
        << "FROM backup_profiles WHERE name = ?1;";

#ifdef __DEBUG__
  cout << "QUERY : " << query.str() << endl;
#endif

  /*
   * Prepare the query
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  /* Should match column order of query */
  descr->pushAffectedAttribute(SQL_BCK_PROF_ID_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_NAME_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_COMPRESS_TYPE_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_MAX_RATE_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_LABEL_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_FAST_CHKPT_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_INCL_WAL_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_WAIT_FOR_WAL_ATTNO);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind the values.
   */
  sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_STATIC);

  /* ...and execute ... */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  try {
    /*
     * At this point only a single tuple expected...
     */
    if (rc == SQLITE_ROW) {
      this->fetchBackupProfileIntoDescr(stmt, descr, range);
    }

  } catch (exception &e) {
    /* don't leak sqlite3 statement handle */
    sqlite3_finalize(stmt);
    /* re-throw exception for caller */
    throw e;
  }

  sqlite3_finalize(stmt);
  return descr;
}

void BackupCatalog::createBackupProfile(std::shared_ptr<BackupProfileDescr> profileDescr) {

  sqlite3_stmt *stmt;
  std::ostringstream insert;
  int rc;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  insert << "INSERT INTO backup_profiles("
         << "name, compress_type, max_rate, label, fast_checkpoint, include_wal, wait_for_wal) "
         << "VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7);";

#ifdef __DEBUG__
  cerr << "createBackupProfile query: " << insert.str() << endl;
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          insert.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  /*
   * Bind new backup profile data.
   */
  Range range(1, 7);
  this->SQLbindBackupProfileAttributes(profileDescr,
                                       profileDescr->getAffectedAttributes(),
                                       stmt,
                                       range);
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;

    oss << "error creating backup profile in catalog database: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);

    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::createArchive(shared_ptr<CatalogDescr> descr) {

  sqlite3_stmt *stmt;
  int rc;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  if (descr->coninfo->type != ConnectionDescr::CONNECTION_TYPE_BASEBACKUP)
    throw CCatalogIssue("archives can create connections of type basebackup only");

  rc = sqlite3_prepare_v2(this->db_handle,
                          "INSERT INTO archive(name, directory, compression) "
                          "VALUES (?1, ?2, ?3);",
                          -1,
                          &stmt,
                          NULL);
  sqlite3_bind_text(stmt, 1,
                    descr->archive_name.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 2,
                    descr->directory.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 3,
                   descr->compression);

  // sqlite3_bind_text(stmt, 4,
  //                   descr->pghost.c_str(), -1 , SQLITE_STATIC);
  // sqlite3_bind_int(stmt, 5,
  //                  descr->pgport);
  // sqlite3_bind_text(stmt, 6,
  //                   descr->pguser.c_str(), -1, SQLITE_STATIC);
  // sqlite3_bind_text(stmt, 7,
  //                   descr->pgdatabase.c_str(), -1, SQLITE_STATIC);

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;

    oss << "error creating archive in catalog database: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);

    throw CCatalogIssue(oss.str());
  }

  /*
   * Initialize the CatalogDescr with its
   * new ID.
   */
  descr->setArchiveId(sqlite3_last_insert_rowid(this->db_handle));
  sqlite3_finalize(stmt);

}

int BackupCatalog::SQLbindBackupTablespaceAttributes(std::shared_ptr<BackupTablespaceDescr> tblspcDescr,
                                                     std::vector<int> affectedAttributes,
                                                     sqlite3_stmt *stmt,
                                                     Range range) {
  int result = range.start();

  if (stmt == NULL) {
    throw CCatalogIssue("cannot bind updated attributes: invalid statement handle");
  }

  for (auto& colId : affectedAttributes) {

    /*
     * Stop, if result has reached end of range.
     */
    if (result < range.end())
      break;

    switch(colId) {

    case SQL_BCK_TBLSPC_ID_ATTNO:
      sqlite3_bind_int(stmt, result, tblspcDescr->id);
      break;

    case SQL_BCK_TBLSPC_BCK_ID_ATTNO:
      sqlite3_bind_int(stmt, result, tblspcDescr->backup_id);
      break;

    case SQL_BCK_TBLSPC_SPCOID_ATTNO:
      sqlite3_bind_int(stmt, result, tblspcDescr->spcoid);
      break;

    case SQL_BCK_TBLSPC_SPCLOC_ATTNO:
      sqlite3_bind_text(stmt, result,
                        tblspcDescr->spclocation.c_str(),
                        -1,
                        SQLITE_STATIC);
      break;

    case SQL_BCK_TBLSPC_SPCSZ_ATTNO:
      sqlite3_bind_int(stmt, result, tblspcDescr->spcsize);
      break;

    default:
      {
        ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }

    result++;
  }

  return result;
}

int BackupCatalog::SQLbindConnectionAttributes(std::shared_ptr<ConnectionDescr> conDescr,
                                               std::vector<int> affectedAttributes,
                                               sqlite3_stmt *stmt,
                                               Range range) {
  int result = range.start();

  if (stmt == NULL)
    throw CCatalogIssue("cannot bind updated attributes: invalid statement handle");

  for (auto& colId : affectedAttributes) {
    /*
     * Stop, if result has reached end of range.
     */
    if (result > range.end())
      break;

    switch(colId) {

    case SQL_CON_ARCHIVE_ID_ATTNO:
      sqlite3_bind_int(stmt, result, conDescr->archive_id);
      break;

    case SQL_CON_TYPE_ATTNO:
      /* type cannot be null */
      sqlite3_bind_text(stmt, result,
                        conDescr->type.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_CON_PGHOST_ATTNO:
      sqlite3_bind_text(stmt, result,
                        conDescr->pghost.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_CON_PGPORT_ATTNO:
      sqlite3_bind_int(stmt,
                       result,
                       conDescr->pgport);
      break;

    case SQL_CON_PGUSER_ATTNO:
      sqlite3_bind_text(stmt, result,
                        conDescr->pguser.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_CON_PGDATABASE_ATTNO:
      sqlite3_bind_text(stmt, result,
                        conDescr->pgdatabase.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_CON_DSN_ATTNO:
      cout << "bound DSN TO id " << result << endl;
      sqlite3_bind_text(stmt, result,
                        conDescr->dsn.c_str(),
                        -1, SQLITE_STATIC);
      break;

    default:
      {
        std::ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }
    result ++;
  }

  return result;
}

int BackupCatalog::SQLbindProcsAttributes(std::shared_ptr<CatalogProc> procInfo,
                                          std::vector<int> affectedAttributes,
                                          sqlite3_stmt *stmt,
                                          Range range) {

  int result = range.start();

  if (stmt == NULL)
    throw CCatalogIssue("cannot bind updated attributes: invalid statement handle");

  for (auto& colId : affectedAttributes) {

    /*
     * Stop, if result has reached end of range.
     */
    if (result > range.end())
      break;

    switch(colId) {

    case SQL_PROCS_PID_ATTNO:
      sqlite3_bind_int(stmt, result, procInfo->pid);
      break;

    case SQL_PROCS_ARCHIVE_ID_ATTNO:
      sqlite3_bind_int(stmt, result, procInfo->archive_id);
      break;

    case SQL_PROCS_TYPE_ATTNO:
      sqlite3_bind_text(stmt, result,
                        procInfo->type.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_PROCS_STARTED_ATTNO:
      sqlite3_bind_text(stmt, result,
                        procInfo->started.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_PROCS_STATE_ATTNO:
      sqlite3_bind_text(stmt, result,
                        procInfo->state.c_str(),
                        -1, SQLITE_STATIC);
      break;

    default:
      {
        ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }

    result++;
  }

  return result;
}

int BackupCatalog::SQLbindBackupProfileAttributes(std::shared_ptr<BackupProfileDescr> profileDescr,
                                                  std::vector<int> affectedAttributes,
                                                  sqlite3_stmt *stmt,
                                                  Range range) {


  int result = range.start();

  if (stmt == NULL)
    throw CCatalogIssue("cannot bind updated attributes: invalid statement handle");

  for (auto& colId : affectedAttributes) {

    /*
     * Stop, if result has reached end of range.
     */
    if (result > range.end())
      break;

    switch(colId) {

    case SQL_BCK_PROF_ID_ATTNO:
      sqlite3_bind_int(stmt, result, profileDescr->profile_id);
      break;

    case SQL_BCK_PROF_NAME_ATTNO:
      sqlite3_bind_text(stmt, result,
                        profileDescr->name.c_str(), -1, SQLITE_STATIC);
      break;

    case SQL_BCK_PROF_COMPRESS_TYPE_ATTNO:
      sqlite3_bind_int(stmt, result, profileDescr->compress_type);
      break;

    case SQL_BCK_PROF_MAX_RATE_ATTNO:
      sqlite3_bind_int(stmt, result, profileDescr->max_rate);
      break;

    case SQL_BCK_PROF_LABEL_ATTNO:
      sqlite3_bind_text(stmt, result,
                        profileDescr->label.c_str(), -1, SQLITE_STATIC);
      break;

    case SQL_BCK_PROF_FAST_CHKPT_ATTNO:
      sqlite3_bind_int(stmt, result, profileDescr->fast_checkpoint);
      break;

    case SQL_BCK_PROF_INCL_WAL_ATTNO:
      sqlite3_bind_int(stmt, result, profileDescr->include_wal);
      break;

    case SQL_BCK_PROF_WAIT_FOR_WAL_ATTNO:
      sqlite3_bind_int(stmt, result, profileDescr->wait_for_wal);
      break;

    default:
      {
        ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }

    result++;
  }

  return result;
}

int BackupCatalog::SQLbindStreamAttributes(shared_ptr<StreamIdentification> ident,
                                           std::vector<int> affectedAttributes,
                                           sqlite3_stmt *stmt,
                                           Range range) {

  int result = range.start();

  if (stmt == NULL)
    throw CCatalogIssue("cannot bind updated attributes: invalid statement handle");

  for (auto& colId : affectedAttributes) {

    /*
     * Stop, if result has reached end of range.
     */
    if (result > range.end())
      break;

    switch(colId) {

    case SQL_STREAM_ID_ATTNO:
      sqlite3_bind_int(stmt, result, ident->id);
      break;

    case SQL_STREAM_ARCHIVE_ID_ATTNO:
      sqlite3_bind_int(stmt, result, ident->archive_id);
      break;

    case SQL_STREAM_STYPE_ATTNO:
      sqlite3_bind_text(stmt, result,
                        ident->stype.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_STREAM_SLOT_NAME_ATTNO:
      sqlite3_bind_text(stmt, result,
                        ident->slot_name.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_STREAM_SYSTEMID_ATTNO:
      sqlite3_bind_text(stmt, result,
                        ident->systemid.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_STREAM_TIMELINE_ATTNO:
      sqlite3_bind_int(stmt, result, ident->timeline);
      break;

    case SQL_STREAM_XLOGPOS_ATTNO:
      sqlite3_bind_text(stmt, result,
                        ident->xlogpos.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_STREAM_DBNAME_ATTNO:
      sqlite3_bind_text(stmt, result,
                        ident->dbname.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_STREAM_STATUS_ATTNO:
      sqlite3_bind_text(stmt, result,
                        ident->status.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_STREAM_REGISTER_DATE_ATTNO:
      sqlite3_bind_text(stmt, result,
                        ident->create_date.c_str(),
                        -1, SQLITE_STATIC);
      break;

    default:
      {
        ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }

    /* increment column index */
    result++;
  }

  return result;
}

int BackupCatalog::SQLbindArchiveAttributes(shared_ptr<CatalogDescr> descr,
                                            std::vector<int> affectedAttributes,
                                            sqlite3_stmt *stmt,
                                            Range range) {

  int result = range.start();

  if (stmt == NULL)
    throw CCatalogIssue("cannot bind updated attributes: invalid statement handle");

  for (auto& colId : affectedAttributes) {

    /*
     * Stop, if result has reached end of range.
     */
    if (result > range.end())
      break;

    switch(colId) {

    case SQL_ARCHIVE_ID_ATTNO:
      sqlite3_bind_int(stmt, result,
                      descr->id);
      break;

    case SQL_ARCHIVE_NAME_ATTNO:
      sqlite3_bind_text(stmt, result,
                        descr->archive_name.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_ARCHIVE_DIRECTORY_ATTNO:
      sqlite3_bind_text(stmt, result,
                        descr->directory.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_ARCHIVE_COMPRESSION_ATTNO:
      sqlite3_bind_int(stmt, result,
                       descr->compression);
      break;

    case SQL_ARCHIVE_PGHOST_ATTNO:
      sqlite3_bind_text(stmt, result,
                        descr->coninfo->pghost.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_ARCHIVE_PGPORT_ATTNO:
      sqlite3_bind_int(stmt, result,
                       descr->coninfo->pgport);
      break;

    case SQL_ARCHIVE_PGUSER_ATTNO:
      sqlite3_bind_text(stmt, result,
                        descr->coninfo->pguser.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_ARCHIVE_PGDATABASE_ATTNO:
      sqlite3_bind_text(stmt, result,
                        descr->coninfo->pgdatabase.c_str(),
                        -1, SQLITE_STATIC);
      break;

    default:
      {
        ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }

    result++;
  }

  return result;
}


void BackupCatalog::close() {
  if (available()) {
    sqlite3_close(this->db_handle);
  }
  else {
    throw CCatalogIssue("attempt to close uninitialized catalog");
  }
}

BackupCatalog::~BackupCatalog() {
  if (available())
    this->close();
}

std::string BackupCatalog::affectedColumnsToString(std::vector<int> affectedAttributes) {

  ostringstream result;

  for (int i = 0; i < affectedAttributes.size(); i++) {
    result << archiveCatalogCols[affectedAttributes[i]];

    if (i < (affectedAttributes.size() - 1))
      result << ", ";

  }

  return result.str();
}

std::string BackupCatalog::SQLmakePlaceholderList(std::vector<int> affectedAttributes) {

  ostringstream result;

  for(int i = 1; i <= affectedAttributes.size(); i++) {
    result << "?" << i;

    if (i < affectedAttributes.size())
      result << ", ";
  }

  return result.str();
}

std::string BackupCatalog::affectedColumnsToString(int entity,
                                                   std::vector<int> affectedAttributes) {

  ostringstream result;

  for (int colId = 0; colId < affectedAttributes.size(); colId++) {
    result << this->mapAttributeId(entity, affectedAttributes[colId]);

    if (colId < (affectedAttributes.size() -1))
      result << ", ";
  }
  return result.str();

}

std::string BackupCatalog::SQLgetFilterForArchive(std::shared_ptr<CatalogDescr> descr,
                                                  std::vector<int> affectedAttributes,
                                                  Range rangeBindID,
                                                  std::string op) {

  ostringstream result;
  int bindId = rangeBindID.start();

  for(auto colId : affectedAttributes) {
    result << archiveCatalogCols[colId] << "=?" << bindId;

    if (bindId < rangeBindID.end())
      result << " " << op << " ";

    bindId++;
  }

  return result.str();
}

std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> BackupCatalog::getArchiveList(std::shared_ptr<CatalogDescr> descr,
                                                                                        std::vector<int> affectedAttributes) {

  /*
   * Statement handle
   */
  sqlite3_stmt *stmt;

  /*
   * Result map for filtered list
   */
  auto result = make_shared<std::list<std::shared_ptr<CatalogDescr>>>();

  /*
   * Build the filtered query.
   */
  ostringstream query;
  Range range(1, affectedAttributes.size());

  query << "SELECT a.id, a.name, a.directory, a.compression, c.pghost, "
        << "c.pgport, c.pguser, c.pgdatabase, c.dsn "
        << " FROM archive a JOIN connections c ON c.archive_id = a.id"
        << " WHERE c.type = 'basebackup' AND "
        << this->SQLgetFilterForArchive(descr,
                                        affectedAttributes,
                                        range,
                                        " OR ")
        << " ORDER BY name;";

#ifdef __DEBUG__
  cerr << "QUERY: " << query.str() << endl;
#endif

  /*
   * Prepare the query.
   */
  int rc = sqlite3_prepare_v2(this->db_handle,
                              query.str().c_str(),
                              -1,
                              &stmt,
                              NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  this->SQLbindArchiveAttributes(descr,
                                 affectedAttributes,
                                 stmt,
                                 range);

  /*
   * ... and execute the statement.
   */

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc!= SQLITE_DONE) {
    ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  try {

    while(rc == SQLITE_ROW) {

      auto item = make_shared<CatalogDescr>();

      this->fetchArchiveDataIntoDescr(stmt, item);
      result->push_back(item);
      rc = sqlite3_step(stmt);

    }

  } catch(exception& e) {
    /* re-throw exception, but don't leak sqlite statement handle */
    sqlite3_finalize(stmt);
    throw e;
  }

  sqlite3_finalize(stmt);
  return result;
}

shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> BackupCatalog::getArchiveList() {

  /*
   * Statement handle for SQLite3 catalog db.
   */
  sqlite3_stmt *stmt;

  /*
   * Prepare the result map.
   */
  auto result = make_shared<std::list<std::shared_ptr<CatalogDescr>>>();

  /*
   * Build the query.
   */
  std::ostringstream query;
  query << "SELECT a.id, a.name, a.directory, a.compression, c.pghost, c.pgport, "
        << "c.pguser, c.pgdatabase, c.dsn "
        << "FROM archive a JOIN connections c ON c.archive_id = a.id "
        << "WHERE c.type = 'basebackup' ORDER BY name";

  int rc = sqlite3_prepare_v2(this->db_handle,
                              query.str().c_str(),
                              -1,
                              &stmt,
                              NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc!= SQLITE_DONE) {
    ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  try {

    /*
     * loop through the result, make a catalog descr
     * and push it into the result list;
     */
    while (rc == SQLITE_ROW) {

      auto item = make_shared<CatalogDescr>();

      this->fetchArchiveDataIntoDescr(stmt, item);
      result->push_back(item);
      rc = sqlite3_step(stmt);

    }

  } catch (exception &e) {
    /* re-throw exception, but don't leak sqlite statement handle */
    sqlite3_finalize(stmt);
    throw e;
  }

  sqlite3_finalize(stmt);
  return result;
}

void BackupCatalog::setStreamStatus(int streamid,
                                    std::string status) {

  sqlite3_stmt *stmt;

  int rc;

  if (!this->available())
    throw CCatalogIssue("could not update stream status: database not opened");

  rc = sqlite3_prepare_v2(this->db_handle,
                          "UPDATE stream SET status = ?1 WHERE id = ?2;",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "cannot prepare updating stream status for id " << streamid;
    throw CCatalogIssue(oss.str());
  }

  sqlite3_bind_text(stmt, 1, status.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 2, streamid);

  rc= sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "failed to update stream status for id " << streamid;
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /* and we're done */
  sqlite3_finalize(stmt);
}

void BackupCatalog::dropStream(int streamid) {

  int rc;
  sqlite3_stmt *stmt;
  std::ostringstream query;

  if (!this->available()) {
    throw CCatalogIssue("could not drop stream: database not opened");
  }

  rc = sqlite3_prepare_v2(this->db_handle,
                          "DELETE FROM stream WHERE id = ?1;",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "error preparing to register stream: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_bind_int(stmt, 1, streamid);

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error dropping stream: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);

}

void BackupCatalog::registerBasebackup(int archive_id,
                                       std::shared_ptr<BaseBackupDescr> backupDescr) {

  int rc;
  sqlite3_stmt *stmt;

  if (!this->available()) {
    throw CCatalogIssue("could not register basebackup: database not opened");
  }

  rc = sqlite3_prepare_v2(this->db_handle,
                          "INSERT INTO backup(archive_id, xlogpos, timeline, label, fsentry, started) "
                          "VALUES(?1, ?2, ?3, ?4, ?5, ?6);",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "error code "
        << rc
        << " when preparing to register basebackup: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_bind_int(stmt, 1, archive_id);
  sqlite3_bind_text(stmt, 2, backupDescr->xlogpos.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 3, backupDescr->timeline);
  sqlite3_bind_text(stmt, 4, backupDescr->label.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 5, backupDescr->fsentry.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 6, backupDescr->started.c_str(), -1, SQLITE_STATIC);

  /*
   * Execute the statement.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error registering basebackup: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Assign the generated ROW ID to this new basebackup entry.
   */
  backupDescr->id = sqlite3_last_insert_rowid(this->db_handle);

  /*
   * ... and we're done
   */
  sqlite3_finalize(stmt);
}

void BackupCatalog::finalizeBasebackup(std::shared_ptr<BaseBackupDescr> backupDescr) {

  int rc;
  sqlite3_stmt *stmt;

  if (!this->available()) {
    throw CCatalogIssue("could not finalize basebackup: database not opened");
  }

  /*
   * Descriptor should have a valid backup id
   */
  if (backupDescr->id < 0) {
    throw CCatalogIssue("could not finalize basebackup: invalid backup id");
  }

  /*
   * Check if this descriptor has an xlogposend value assigned.
   */
  if (backupDescr->xlogposend == "") {
    throw CCatalogIssue("could not finalize basebackup: expected xlog end position");
  }

  rc = sqlite3_prepare_v2(this->db_handle,
                          "UPDATE backup SET status = 'ready', stopped = ?1, xlogposend = ?2 "
                          "WHERE id = ?3 AND archive_id = ?4;",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "error finalizing basebackup: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  backupDescr->stopped = CPGBackupCtlBase::current_timestamp();

  /*
   * Bind values
   */
  sqlite3_bind_text(stmt, 1, backupDescr->stopped.c_str(),
                    -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 2, backupDescr->xlogposend.c_str(),
                    -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 3, backupDescr->id);
  sqlite3_bind_int(stmt, 4, backupDescr->archive_id);

  /*
   * Execute the statement
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error finalizing basebackup: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::abortBasebackup(std::shared_ptr<BaseBackupDescr> backupDescr) {

  int rc;
  sqlite3_stmt *stmt;
  std::ostringstream query;

  if (!this->available()) {
    throw CCatalogIssue("could not update basebackup status: catalog not available");
  }

  /*
   * Descriptor should have a valid backup id
   */
  if (backupDescr->id < 0) {
    throw CCatalogIssue("could not mark basebackup aborted: invalid backup id");
  }

  query << "UPDATE backup SET status = 'aborted' WHERE id = ?1 AND archive_id = ?2;";

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "error code "
        << rc
        << " when preparing to mark basebackup as aborted: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind parameters...
   */
  sqlite3_bind_int(stmt, 1, backupDescr->id);
  sqlite3_bind_int(stmt, 2, backupDescr->archive_id);

  rc = sqlite3_step(stmt);

  /*
   * Check command status.
   */
  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error registering stream: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::updateStream(int streamid,
                                 std::vector<int> affectedColumns,
                                 std::shared_ptr<StreamIdentification> streamident) {

  int rc;
  sqlite3_stmt *stmt;
  ostringstream updateSQL;
  int boundCols;

  if(!this->available()) {
    throw CCatalogIssue("could not update stream: database not opened");
  }

  /*
   * affectedColumns should specify at least one
   * catalog column being updated.
   */
  if (affectedColumns.size() <= 0)
    throw CCatalogIssue("cannot update stream with empty attribute list");

  /*
   * Build UPDATE SQL command...
   */
  updateSQL << "UPDATE stream SET ";

  /*
   * Loop through the affected columns list and
   * build a comma seprated list of col=? pairs.
   */

  for (boundCols = 0; boundCols < affectedColumns.size(); boundCols++) {

    updateSQL << BackupCatalog::SQLgetUpdateColumnTarget(SQL_STREAM_ENTITY,
                                                         affectedColumns[boundCols])
              << (boundCols + 1);

  }

  /*
   * WHERE clause identifes tuple per stream id
   */
  updateSQL << " WHERE id = ?" << (++boundCols) << ";";

#ifdef __DEBUG__
  cerr << "generate UPDATE SQL " << updateSQL.str() << endl;
#endif

  /*
   * Prepare the SQL statement.
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          updateSQL.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  /*
   * Bind UPDATE values. Please note that we rely
   * on the order of affectedAttributes to match
   * the previously formatted UPDATE SQL string.
   */
  this->SQLbindStreamAttributes(streamident,
                                affectedColumns,
                                stmt,
                                Range(1, boundCols));

  /*
   * Don't forget to bind values to the UPDATE WHERE clause...
   */
  sqlite3_bind_int(stmt, boundCols, streamident->id);

  /*
   * Execute the UPDATE
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "error updating stream in catalog database: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

std::shared_ptr<CatalogProc> BackupCatalog::getProc(int archive_id, std::string type) {

  int rc;
  sqlite3_stmt *stmt;
  std::shared_ptr<CatalogProc> procInfo(nullptr);
  std::vector<int> attrs;

  std::string query =
    "SELECT pid, archive_id, type, "
    "started, state FROM procs WHERE archive_id = ?1 "
    "AND type = ?2;";

  procInfo = std::make_shared<CatalogProc>();

  if (!this->available()) {
    throw CCatalogIssue("could not request proc information: database not opened");
  }

  /*
   * Prepare the query...
   */
#ifdef __DEBUG__
  cerr << "SELECT SQL: " << query << endl;
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "error querying procs catalog table for archive ID "
        << archive_id << ", type " << type
        << ": " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Record attributes we want to retrieve ...
   */
  attrs.push_back(SQL_PROCS_PID_ATTNO);
  attrs.push_back(SQL_PROCS_ARCHIVE_ID_ATTNO);
  attrs.push_back(SQL_PROCS_TYPE_ATTNO);
  attrs.push_back(SQL_PROCS_STARTED_ATTNO);
  attrs.push_back(SQL_PROCS_STATE_ATTNO);

  /*
   * Bind WHERE predicate values...
   */
  sqlite3_bind_int(stmt, 1, archive_id);
  sqlite3_bind_text(stmt, 2, type.c_str(), -1, SQLITE_STATIC);

  /*
   * Execute the statement.
   */
  rc = sqlite3_step(stmt);

  if (rc == SQLITE_ERROR) {
    std::ostringstream oss;
    oss << "error selecting proc information :"
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Only one tuple expected ...
   */
  if(rc == SQLITE_ROW) {

    /* Fetch proc data into new CatalogProc handle */
    procInfo = this->fetchCatalogProcData(stmt,
                                          attrs);

    if ((rc = sqlite3_step(stmt)) != SQLITE_DONE) {
      std::ostringstream oss;
      oss << "unexpected number for rows: getProc()";
      sqlite3_finalize(stmt);
      throw CCatalogIssue(oss.str());
    }

  }

  sqlite3_finalize(stmt);
  return procInfo;
}

void BackupCatalog::registerProc(std::shared_ptr<CatalogProc> procInfo) {

  int rc;
  sqlite3_stmt *stmt;

  if (!this->available())
    throw CCatalogIssue("could not register process handle in catalog: database not opened");

  /*
   * Prepare INSERT SQL command ...
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          "INSERT INTO procs("
                          "pid, archive_id, type, started, state)"
                          " VALUES(?, ?, ?, ?, ?);",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss
      << "error code "
      << rc
      << " when preparing to register proc handle (PID "
      << procInfo->pid
      << ": "
      << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind values.
   */
  this->SQLbindProcsAttributes(procInfo,
                               procInfo->getAffectedAttributes(),
                               stmt,
                               Range(1, 5));

  /*
   * ... and execute the INSERT command.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss
      << "error registering process handle (PID "
      << procInfo->pid << "): "
      << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::unregisterProc(int pid,
                                   int archive_id) {

  int rc;
  sqlite3_stmt *stmt;
  std::shared_ptr<CatalogProc> procInfo = std::make_shared<CatalogProc>();

  if (!this->available())
    throw CCatalogIssue("could not unregister process handle from catalog: databas not opened");

  /*
   * Prepare the DELETE SQL command...
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          "DELETE FROM procs WHERE pid = ?1 AND archive_id = ?2;",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss
      << "error code "
      << rc
      << " when unregistering process handle (PID "
      << pid << "): "
      << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind values...
   */
  procInfo->pid = pid;
  procInfo->archive_id = archive_id;

  procInfo->pushAffectedAttribute(SQL_PROCS_PID_ATTNO);
  procInfo->pushAffectedAttribute(SQL_PROCS_ARCHIVE_ID_ATTNO);
  this->SQLbindProcsAttributes(procInfo,
                               procInfo->getAffectedAttributes(),
                               stmt,
                               Range(1, 2));

  /*
   * ... and execute the DELETE statement.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss
      << "error unregistering process handle (PID "
      << pid << "): "
      << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::updateProc(std::shared_ptr<CatalogProc> procInfo,
                               std::vector<int> affectedAttributes,
                               int pid,
                               int archive_id) {

  int rc;
  sqlite3_stmt *stmt;
  std::ostringstream updateSQL;
  int boundCols;

  if (!this->available())
    throw CCatalogIssue("could not update process handle: database not opened");

  /*
   * Build UPDATE SQL command string.
   */
  updateSQL << "UPDATE procs SET ";

  /*
   * Prepare the update statement.
   */
  for (boundCols = 0; boundCols < affectedAttributes.size(); boundCols++) {

    /* boundCols must start at index 1 ! */
    updateSQL << BackupCatalog::SQLgetUpdateColumnTarget(SQL_PROCS_ENTITY,
                                                         affectedAttributes[boundCols])
              << (boundCols + 1);

    if (boundCols < affectedAttributes.size() -1 ) {
      updateSQL << ", ";
    }
  }

  /*
   * ... don't forget the WHERE clause ...
   */
  updateSQL << " WHERE pid = ?" << (++boundCols)
            << "AND archive_id = ?" << (++boundCols) << ";";

#ifdef __DEBUG__
  cerr << "generate UPDATE SQL " << updateSQL.str() << endl;
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          updateSQL.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  /*
   * Assign bind variables. Please note that we rely
   * on the order of affectedAttributes to match the
   * previously formatted UPDATE SQL string. Note that
   * we must not recognized the bind values for the
   * WHERE condition here, since we don't want to copy
   * around the affectedAttributes vector. Thus, we do
   * it manually afterwards.
   */
  this->SQLbindProcsAttributes(procInfo,
                               affectedAttributes,
                               stmt,
                               Range(1, (boundCols - 2)));

  sqlite3_bind_int(stmt, (boundCols - 1), pid);
  sqlite3_bind_int(stmt, boundCols, archive_id);

  /*
   * Execute the UPDATE statement.
   */
  sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error updating catalog proc handle: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::registerStream(int archive_id,
                                   std::string type,
                                   StreamIdentification& streamident) {

  int rc;
  sqlite3_stmt *stmt;

  if (!this->available()) {
    throw CCatalogIssue("could not register stream: database not opened");
  }

  rc = sqlite3_prepare_v2(this->db_handle,
                          "INSERT INTO stream("
                          "archive_id, stype, systemid, timeline, xlogpos, dbname, status, create_date)"
                          " VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "error code "
        << rc
        << " when preparing to register stream: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_bind_int(stmt, 1, archive_id);
  sqlite3_bind_text(stmt, 2, type.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 3, streamident.systemid.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 4, streamident.timeline);
  sqlite3_bind_text(stmt, 5, streamident.xlogpos.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 6, streamident.dbname.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 7, STREAM_PROGRESS_IDENTIFIED, -1, SQLITE_STATIC);

  /*
   * Set the creation date of this stream.
   */
  sqlite3_bind_text(stmt, 8, CPGBackupCtlBase::current_timestamp().c_str(), -1, SQLITE_STATIC);

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error registering stream: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Assign the new stream id to our current
   * stream identification object.
   */
  streamident.id = sqlite3_last_insert_rowid(this->db_handle);

  sqlite3_finalize(stmt);
}

void BackupCatalog::registerTablespaceForBackup(std::shared_ptr<BackupTablespaceDescr> tblspcDescr) {

  sqlite3_stmt *stmt;
  std::ostringstream query;
  std::vector<int> attrs;
  int rc;

  if (!this->available()) {
    throw CCatalogIssue("database not available");
  }

  /*
   * We expect the backup_id identifier to be set.
   */
  if (tblspcDescr->backup_id < 0) {
    throw CCatalogIssue("backup id required to register tablespace for backup");
  }

  /*
   * Attributes required to register the new tablespace.
   */
  attrs.push_back(SQL_BCK_TBLSPC_BCK_ID_ATTNO);
  attrs.push_back(SQL_BCK_TBLSPC_SPCOID_ATTNO);
  attrs.push_back(SQL_BCK_TBLSPC_SPCLOC_ATTNO);
  attrs.push_back(SQL_BCK_TBLSPC_SPCSZ_ATTNO);

  /*
   * Generate INSERT SQL.
   */
  query << "INSERT INTO backup_tablespaces("
        << BackupCatalog::SQLgetColumnList(SQL_BACKUP_TBLSPC_ENTITY,
                                           /* vector with col IDs */
                                           attrs)
        << ") VALUES(?1, ?2, ?3, ?4);";

#ifdef __DEBUG__
  cerr << "DEBUG: registerTablespaceForBackup() query: "
       << query.str()
       << endl;
#endif

  /*
   * Prepare the statement and bind values.
   */

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "could not prepare query to register tablespace: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_bind_int(stmt, 1, tblspcDescr->backup_id);
  sqlite3_bind_int(stmt, 2, tblspcDescr->spcoid);
  sqlite3_bind_text(stmt, 3, tblspcDescr->spclocation.c_str(),
                    -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 4, tblspcDescr->spcsize);

  /*
   * Execute query...
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error registering stream: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Remember new registered id of tablespace ...
   */
  tblspcDescr->id = sqlite3_last_insert_rowid(this->db_handle);

  /*
   * ... and we're done.
   */
  sqlite3_finalize(stmt);
}

void BackupCatalog::getCatalogConnection(std::shared_ptr<ConnectionDescr> conDescr,
                                         int archive_id,
                                         std::string type) {
  sqlite3_stmt *stmt;
  int rc;
  ostringstream query;

  /*
   * Since we scripple directly on the specified
   * pointer, make sure it's initialized.
   */
  if (conDescr == nullptr)
    throw CCatalogIssue("could not get connection info: result pointer is not initialized");

  query << "SELECT "
        << this->affectedColumnsToString(SQL_CON_ENTITY,
                                         conDescr->getAffectedAttributes())
        << " FROM connections WHERE archive_id = ?1 AND type = ?2;";

#ifdef __DEBUG__
  cout << "generated SQL: " << query.str() << endl;
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "could not prepare query to get connection: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind WHERE clause values...
   */
  sqlite3_bind_int(stmt, 1, archive_id);
  sqlite3_bind_text(stmt, 2, type.c_str(), -1, SQLITE_STATIC);

  /* ... execute SELECT */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "unexpected result in catalog query: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Mark result descriptor empty.
   */
  conDescr->archive_id = -1;
  conDescr->type = ConnectionDescr::CONNECTION_TYPE_UNKNOWN;

  /*
   * Empty result set or single row expected.
   */
  try {
    while (rc == SQLITE_ROW && rc != SQLITE_DONE) {
      this->fetchConnectionData(stmt, conDescr);
      break;
    }
  } catch(CCatalogIssue& e) {
    sqlite3_finalize(stmt);
    throw e;
  }

  sqlite3_finalize(stmt);
}

void
BackupCatalog::createCatalogConnection(std::shared_ptr<ConnectionDescr> conDescr) {

  sqlite3_stmt *stmt;
  std::ostringstream insert;
  int rc;

  if (!this-!available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  insert << "INSERT INTO connections("
         << this->affectedColumnsToString(SQL_CON_ENTITY,
                                          conDescr->getAffectedAttributes())
         << ") VALUES ("
         << this->SQLmakePlaceholderList(conDescr->getAffectedAttributes())
         << ");";

  rc = sqlite3_prepare_v2(this->db_handle,
                          insert.str().c_str(),
                          -1, &stmt, NULL);

#ifdef __DEBUG__
  cerr << "Generated SQL: " << insert.str() << endl;
#endif

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "could not prepare query for connection descr: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  this->SQLbindConnectionAttributes(conDescr,
                                    conDescr->getAffectedAttributes(),
                                    stmt,
                                    Range(1, conDescr->getAffectedAttributes().size()));

  /*
   * Execute the prepared statement.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error creating database connection entry: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);

}

void BackupCatalog::updateCatalogConnection(std::shared_ptr<ConnectionDescr> conDescr,
                                            std::string archive_name,
                                            std::string type) {
  sqlite3_stmt *stmt;
  ostringstream updateSQL;
  int rc;
  int boundCols;
  std::vector<int> attrs;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  attrs = conDescr->getAffectedAttributes();
  updateSQL << "UPDATE connections SET ";

  /*
   * generate column list being updated.
   */
  for (boundCols = 0; boundCols < attrs.size(); boundCols++) {

    /*
     * boundCols must start at index 1!
     */
    updateSQL << BackupCatalog::SQLgetUpdateColumnTarget(SQL_CON_ENTITY,
                                                         attrs[boundCols])
              << (boundCols + 1);

    if (boundCols < attrs.size() - 1)
      updateSQL << ", ";
  }

  /*
   * ...attach WHERE clause.
   */
  updateSQL << " WHERE archive_id = (SELECT id FROM archive WHERE name = ?"
            << (++boundCols)
            << ") AND type = ?" << (++boundCols)
            << ";";

  /*
   * ... prepare the query.
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          updateSQL.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "could not prepare query to update connection: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind attributes, take care for the additional
   * WHERE condition values!
   */
  this->SQLbindConnectionAttributes(conDescr,
                                    attrs,
                                    stmt,
                                    Range(1, boundCols));
  sqlite3_bind_text(stmt,
                    (boundCols - 1),
                    archive_name.c_str(),
                    -1,
                    SQLITE_STATIC);

  sqlite3_bind_text(stmt,
                    boundCols,
                    type.c_str(),
                    -1,
                    SQLITE_STATIC);

  /*
   * Execute the query.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "error updating connection in catalog database: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /* we're done */
  sqlite3_finalize(stmt);
}

void
BackupCatalog::dropCatalogConnection(std::string archive_name, std::string type) {

  sqlite3_stmt *stmt;
  ostringstream delete_query;
  int rc;

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  delete_query << "DELETE FROM connections "
    "WHERE archive_id = (SELECT id FROM archive WHERE name = ?1) "
    "AND type = ?2;";

  rc = sqlite3_prepare_v2(this->db_handle,
                          delete_query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "could not prepare query to drop connection: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind statement variables
   */
  sqlite3_bind_text(stmt, 1, archive_name.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 2, type.c_str(), -1, SQLITE_STATIC);

  /*
   * Execute the statement.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "could not delete connection: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);

}

std::vector<std::shared_ptr<StreamIdentification>>
BackupCatalog::getStreams(std::string archive_name) {

  std::vector<std::shared_ptr<StreamIdentification>> result;
  std::vector<int> streamRows;
  sqlite3_stmt *stmt;
  int rc;

  rc = sqlite3_prepare_v2(this->db_handle,
                          "SELECT * FROM stream WHERE archive_id = (SELECT id FROM archive WHERE name = ?1);",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "could not prepare query for stream/archive "
        << archive_name
        << ": "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_bind_text(stmt, 1, archive_name.c_str(), -1, SQLITE_STATIC);

  /* perform the SELECT */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  streamRows.push_back(SQL_STREAM_ID_ATTNO);
  streamRows.push_back(SQL_STREAM_ARCHIVE_ID_ATTNO);
  streamRows.push_back(SQL_STREAM_STYPE_ATTNO);
  streamRows.push_back(SQL_STREAM_SLOT_NAME_ATTNO);
  streamRows.push_back(SQL_STREAM_SYSTEMID_ATTNO);
  streamRows.push_back(SQL_STREAM_TIMELINE_ATTNO);
  streamRows.push_back(SQL_STREAM_XLOGPOS_ATTNO);
  streamRows.push_back(SQL_STREAM_DBNAME_ATTNO);
  streamRows.push_back(SQL_STREAM_STATUS_ATTNO);
  streamRows.push_back(SQL_STREAM_REGISTER_DATE_ATTNO);

  while (rc == SQLITE_ROW && rc != SQLITE_DONE) {

    /* fetch stream data into new StreamIdentification */
    std::shared_ptr<StreamIdentification> item
      = this->fetchStreamData(stmt,
                              streamRows);
    result.push_back(item);
    rc = sqlite3_step(stmt);

  }

  sqlite3_finalize(stmt);

}

bool BackupCatalog::tableExists(string tableName) {

  int table_exists = false;

  if (this->available()) {

    sqlite3_stmt *stmt;
    int rc;

    rc = sqlite3_prepare_v2(this->db_handle,
                            "SELECT 1 FROM sqlite_master WHERE name = ?1;",
                            -1,
                            &stmt,
                            NULL);
    sqlite3_bind_text(stmt, 1, tableName.c_str(), -1, SQLITE_STATIC);

    if (rc != SQLITE_OK) {
      ostringstream oss;
      oss << "could not prepare query for table "
          << tableName
          << ": "
          << sqlite3_errmsg(this->db_handle);
      throw CCatalogIssue(oss.str());
    }

    rc = sqlite3_step(stmt);

    if (rc != SQLITE_ROW) {
      ostringstream oss;
      oss << "could not execute query for table info: " << sqlite3_errmsg(this->db_handle);
      throw CCatalogIssue(oss.str());
    }

    table_exists = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);

  } else {
    throw CCatalogIssue("database not available");
  }

  return ((table_exists == 1) ? true : false);
}

void BackupCatalog::checkCatalog() {

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  if (!this->tableExists("version"))
    throw CCatalogIssue("catalog database doesn't have a \"version\" table");

  if (!this->tableExists("archive"))
    throw CCatalogIssue("catalog database doesn't have an \"archive\" table");

  if (!this->tableExists("backup"))
    throw CCatalogIssue("catalog database doesn't have a \"backup\" table");

  if (!this->tableExists("backup_tablespaces"))
    throw CCatalogIssue("catalog database doesn't have a \"backup_tablespaces\" table");

  if (!this->tableExists("stream"))
    throw CCatalogIssue("catalog database doesn't have a \"stream\" table");

  if (!this->tableExists("backup_profiles"))
    throw CCatalogIssue("catalog database doesn't have a \"backup_profiles\" table");

}

void BackupCatalog::open_rw() {
  int rc;
  char *errmsg;

  rc = sqlite3_open(this->sqliteDB.c_str(), &(this->db_handle));
  if(rc) {
    ostringstream oss;
    /*
     * Something went wrong...
     */
    oss << "cannot open catalog: " << sqlite3_errmsg(this->db_handle);
    sqlite3_close(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  rc = sqlite3_exec(this->db_handle,
                    "PRAGMA foreign_keys=ON;",
                    NULL,
                    NULL,
                    &errmsg);

  if (errmsg != NULL) {
    ostringstream oss;
    oss << "error setting SQLite Pragma: " << errmsg;
    sqlite3_free(errmsg);
    throw CCatalogIssue(oss.str());
  }

  this->isOpen = (this->db_handle != NULL);
}

void BackupCatalog::setCatalogDB(string sqliteDB) {
  /* we don't care wether the database exists already! */
  this->sqliteDB = sqliteDB;
}

bool BackupCatalog::available() {
  return ((this->isOpen) && (this->db_handle != NULL));
}
