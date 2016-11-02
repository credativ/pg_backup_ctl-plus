#ifndef __BACKUP_CATALOG__
#define __BACKUP_CATALOG__

#include <sqlite3.h>

#include <common.hxx>
using namespace std;

namespace credativ {

  /*
   * Base catalog exception.
   */
  class CCatalogIssue : public CPGBackupCtlFailure {
  public:
    CCatalogIssue(const char *errstr) throw() : CPGBackupCtlFailure(errstr) {};
    CCatalogIssue(string errstr) throw() : CPGBackupCtlFailure(errstr) {};
  };

  /*
   * A catalog descriptor is a reference
   * into the catalog database, representing an interface
   * between CPGBackupCtlFS and BackupCatalog objects.
   */
  class CatalogDescr : protected CPGBackupCtlBase {
  public:
    CatalogDescr() {};
    ~CatalogDescr() {};

    int id = -1;
    string archive_name = "";
    string label;
    bool compression = false;
    string directory;
    string pghost = "";
    int    pgport = -1;
    string pguser = "";
    string pgdatabase = "";
  };

  class BackupCatalog : protected CPGBackupCtlBase {
  private:
    sqlite3 *db_handle;
  protected:
    string sqliteDB;
    string archiveDir;
    bool   isOpen;
  public:
    BackupCatalog();
    BackupCatalog(string sqliteDB, string archiveDir) throw(CCatalogIssue);
    virtual ~BackupCatalog();

    /*
     * Rollback an existing catalog transaction.
     */
    virtual void rollbackTransaction() throw(CCatalogIssue);

    /*
     * Checks and locks an existing archive entry.
     */
    virtual shared_ptr<CatalogDescr> exists(std::string directory)
      throw (CCatalogIssue);

    /*
     * Commits the current catalog transaction.
     */
    virtual void commitTransaction()
      throw (CCatalogIssue);

    /*
     * Starts a transaction in the catalog database.
     */
    virtual void startTransaction()
      throw (CCatalogIssue);

    /*
     * Set sqlite database filename.
     */
    virtual void setCatalogDB(string sqliteDB);

    /*
     * Set the archive directory.
     */
    virtual void setArchiveDir(string archiveDir);

    /*
     * Returns true wether the catalog is available.
     */
    virtual bool available();

    /*
     * Check catalog tables.
     */
    virtual void checkCatalog() throw (CCatalogIssue);

    /*
     * Checks wether the specified table exists
     * in the catalog database.
     */
    virtual bool tableExists(string tableName) throw(CCatalogIssue);

    /*
     * Creates a new archive entry in the catalog database.
     */
    virtual void createArchive(shared_ptr<CatalogDescr> descr) throw (CCatalogIssue);

    /*
     * Open the sqlite database for read/write.
     */
    virtual void open_rw() throw(CCatalogIssue);

    /*
     * Close the sqlite catalog database.
     */
    virtual void close() throw(CCatalogIssue);

  };
}

#endif
