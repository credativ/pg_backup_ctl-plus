#ifndef __BACKUP_CATALOG__
#define __BACKUP_CATALOG__

#include <sqlite3.h>

#include <common.hxx>

using namespace std;

namespace credativ {

  /*
   * Defines flags to characterize the
   * action defined by a catalog descriptor.
   */
  typedef enum {
    EMPTY_DESCR = -1,
    CREATE_ARCHIVE,
    DROP_ARCHIVE,
    LIST_ARCHIVE
  } CatalogTag;

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
    CatalogDescr() { tag = EMPTY_DESCR; };
    ~CatalogDescr() {};

    CatalogTag tag;
    int id = -1;
    string archive_name = "";
    string label;
    bool compression = false;
    string directory;
    string pghost = "";
    int    pgport = -1;
    string pguser = "";
    string pgdatabase = "";

    CatalogDescr& operator=(CatalogDescr source);
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
     * Map col id to string.
     */

    /*
     * Keep indexes in sync with macros from include/catalog/catalog.hxx !!
     */

    /* archive table */
    static std::vector<std::string> archiveCatalogCols;

    /* backup table */
    static std::vector<std::string> backupCatalogCols;

    /*
     * This method maps col IDs from the specified
     * catalog entity to its string name.
     */
    static string mapAttributeId(int catalogEntity,
                                 int colId);

    /*
     * Returns a col=? string suitable to be used
     * in an dynamically generated UPDATE SQL command.
     */
    static string SQLgetUpdateColumnTarget(int catalogEntity, int colId);

    /*
     * Bind affected archive attribute values to the given SQLite3
     * stmt handle.
     */
    int SQLbindArchiveAttributes(std::shared_ptr<CatalogDescr> descr,
                                 std::vector<int> affectedAttributes,
                                 sqlite3_stmt *stmt)
      throw(CCatalogIssue);

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
     * Update archive attributes.
     */
    virtual void updateArchiveAttributes(shared_ptr<CatalogDescr> descr,
                                         std::vector<int> affectedAttributes)
      throw (CCatalogIssue);

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
