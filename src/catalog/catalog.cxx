#include <sstream>

#include <catalog.hxx>
#include <BackupCatalog.hxx>

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
    "compression",
    "pghost",
    "pgport",
    "pguser",
    "pgdatabase"
  };


std::vector<std::string> BackupCatalog::backupCatalogCols =
  {
    "id",
    "archive_id",
    "history_filename",
    "label",
    "started",
    "stopped",
    "pinned"
  };

CatalogDescr& CatalogDescr::operator=(CatalogDescr source) {

  this->tag = source.tag;
  this->id = source.id;
  this->archive_name = source.archive_name;
  this->label = source.label;
  this->compression = source.compression;
  this->directory = source.directory;
  this->pghost = source.pghost;
  this->pgport = source.pgport;
  this->pguser = source.pguser;
  this->pgdatabase = source.pgdatabase;

}

BackupCatalog::BackupCatalog() {
  this->isOpen = false;
  this->db_handle = NULL;
}

BackupCatalog::BackupCatalog(string sqliteDB, string archiveDir) throw (CCatalogIssue) {
  this->isOpen = false;
  this->db_handle = NULL;

  /*
   * Identifiers
   */
  this->sqliteDB = sqliteDB;
  this->archiveDir = archiveDir;

  /*
   * Initialize/open catalog database.
   */
  this->open_rw();

  /*
   * Check catalog ...
   */
  this->checkCatalog();
}

void BackupCatalog::startTransaction()
  throw (CCatalogIssue) {

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

void BackupCatalog::commitTransaction() 
  throw (CCatalogIssue) {

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

void BackupCatalog::rollbackTransaction()
  throw(CCatalogIssue) {

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

shared_ptr<CatalogDescr> BackupCatalog::fetchArchiveDataIntoDescr(sqlite3_stmt *stmt,
                                                                  shared_ptr<CatalogDescr> descr) 
  throw (CCatalogIssue) {

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
    descr->pghost = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_PGHOST_ATTNO);

  descr->pgport = sqlite3_column_int(stmt, SQL_ARCHIVE_PGPORT_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_PGUSER_ATTNO) != SQLITE_NULL)
    descr->pguser = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_PGUSER_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_PGDATABASE_ATTNO) != SQLITE_NULL)
    descr->pgdatabase = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_PGDATABASE_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_NAME_ATTNO) != SQLITE_NULL)
    descr->archive_name = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_NAME_ATTNO);

  return descr;

}

shared_ptr<CatalogDescr> BackupCatalog::existsByName(std::string name)
  throw(CCatalogIssue) {

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

shared_ptr<CatalogDescr> BackupCatalog::exists(std::string directory)
  throw(CCatalogIssue) {

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
    sqlite3_finalize(stmt);
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
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

void BackupCatalog::dropArchive(std::string name)
  throw(CCatalogIssue) {

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
    oss << "unexpected result for DROP ARCHIVE in query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);

}

void BackupCatalog::updateArchiveAttributes(shared_ptr<CatalogDescr> descr,
                                            std::vector<int> affectedAttributes)
  throw (CCatalogIssue) {

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
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
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
  default:
    break; /* no op */
  }

  return result;
}

string BackupCatalog::SQLgetUpdateColumnTarget(int catalogEntity,
                                               int colId) {
  ostringstream oss;

  oss << BackupCatalog::mapAttributeId(catalogEntity, colId) << "=?";
  return oss.str();
}

void BackupCatalog::createArchive(shared_ptr<CatalogDescr> descr)
  throw (CCatalogIssue) {

  sqlite3_stmt *stmt;
  int rc;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  rc = sqlite3_prepare_v2(this->db_handle,
                          "INSERT INTO archive(name, directory, compression, pghost, pgport, pguser, pgdatabase) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7);",
                          -1,
                          &stmt,
                          NULL);
  sqlite3_bind_text(stmt, SQL_ARCHIVE_DIRECTORY_ATTNO,
                    descr->directory.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, SQL_ARCHIVE_COMPRESSION_ATTNO,
                   descr->compression);
  sqlite3_bind_text(stmt, SQL_ARCHIVE_PGHOST_ATTNO,
                    descr->pghost.c_str(), -1 , SQLITE_STATIC);
  sqlite3_bind_int(stmt, SQL_ARCHIVE_PGPORT_ATTNO,
                   descr->pgport);
  sqlite3_bind_text(stmt, SQL_ARCHIVE_PGUSER_ATTNO,
                    descr->pguser.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, SQL_ARCHIVE_PGDATABASE_ATTNO,
                    descr->pgdatabase.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, SQL_ARCHIVE_NAME_ATTNO,
                    descr->archive_name.c_str(), -1, SQLITE_STATIC);

  rc = sqlite3_step(stmt);

  sqlite3_finalize(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "error creating archive in catalog database: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

}

int BackupCatalog::SQLbindArchiveAttributes(shared_ptr<CatalogDescr> descr,
                                            std::vector<int> affectedAttributes,
                                            sqlite3_stmt *stmt,
                                            Range range) 
  throw (CCatalogIssue) {

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
                        descr->pghost.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_ARCHIVE_PGPORT_ATTNO:
      sqlite3_bind_int(stmt, result,
                       descr->pgport);
      break;

    case SQL_ARCHIVE_PGUSER_ATTNO:
      sqlite3_bind_text(stmt, result,
                        descr->pguser.c_str(),
                        -1, SQLITE_STATIC);
      break;
      
    case SQL_ARCHIVE_PGDATABASE_ATTNO:
      sqlite3_bind_text(stmt, result,
                        descr->pgdatabase.c_str(),
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
                                             

void BackupCatalog::close() throw(CCatalogIssue){
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
                                                                                        std::vector<int> affectedAttributes)
  throw (CCatalogIssue) {

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

  query << "SELECT id, name, directory, compression, pghost, pgport, pguser, pgdatabase "
        << " FROM archive WHERE " 
        << this->SQLgetFilterForArchive(descr,
                                        affectedAttributes,
                                        range,
                                        " OR ")
        << " ORDER BY name;";

#ifdef __DEBUG__
  cout << "QUERY: " << query.str() << endl;
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

shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> BackupCatalog::getArchiveList()
  throw(CCatalogIssue) {

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
  query << "SELECT id, name, directory, compression, pghost, pgport, pguser, pgdatabase "
        << "FROM archive ORDER BY name";

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

bool BackupCatalog::tableExists(string tableName) throw(CCatalogIssue) {

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

void BackupCatalog::checkCatalog() throw (CCatalogIssue) {

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  if (!this->tableExists("archive"))
    throw CCatalogIssue("catalog database doesn't have an \"archive\" table");

  if (!this->tableExists("backup"))
    throw CCatalogIssue("catalog database doesn't have a \"backup\" table");

}

void BackupCatalog::open_rw() throw (CCatalogIssue) {
  int rc;

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

  this->isOpen = (this->db_handle != NULL);
}

void BackupCatalog::setArchiveDir(string archiveDir) {
  this->archiveDir = archiveDir;
}

void BackupCatalog::setCatalogDB(string sqliteDB) {
  /* we don't care wether the database exists already! */
  this->sqliteDB = sqliteDB;
}

bool BackupCatalog::available() {
  return ((this->isOpen) && (this->db_handle != NULL));
}
