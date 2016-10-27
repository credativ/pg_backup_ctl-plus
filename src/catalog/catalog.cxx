#include <sstream>

#include <catalog.hxx>
#include <BackupCatalog.hxx>

using namespace credativ;

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
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /* ... empty result set or single row expected */
  while (rc == SQLITE_ROW && rc != SQLITE_DONE) {

    /*
     * Save archive properties into catalog
     * descriptor
     */
    result->id = sqlite3_column_int(stmt, SQL_ARCHIVE_ID_ATTNO);
    result->directory = directory;
    result->compression = sqlite3_column_int(stmt, SQL_ARCHIVE_COMPRESSION_ATTNO);

    if (sqlite3_column_type(stmt, SQL_ARCHIVE_PGHOST_ATTNO) != SQLITE_NULL)
      result->pghost = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_PGHOST_ATTNO);

    result->pgport = sqlite3_column_int(stmt, SQL_ARCHIVE_PGPORT_ATTNO);

    if (sqlite3_column_type(stmt, SQL_ARCHIVE_PGUSER_ATTNO) != SQLITE_NULL)
      result->pguser = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_PGUSER_ATTNO);

    if (sqlite3_column_type(stmt, SQL_ARCHIVE_PGDATABASE_ATTNO) != SQLITE_NULL)
      result->pgdatabase = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_PGDATABASE_ATTNO);


    rc = sqlite3_step(stmt);
  }

  sqlite3_finalize(stmt);

  /* Everything >= 0 means valid dataset */
  return result;

}

void BackupCatalog::createArchive(shared_ptr<CatalogDescr> descr)
  throw (CCatalogIssue) {

  sqlite3_stmt *stmt;
  int rc;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  rc = sqlite3_prepare_v2(this->db_handle,
                          "INSERT INTO archive(directory, compression, pghost, pgport, pguser) VALUES (?1, ?2, ?3, ?4, ?5);",
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

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "error creating archive in catalog database: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);

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
