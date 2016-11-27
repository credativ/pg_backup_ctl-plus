#ifndef __BACKUP_CATALOG__
#define __BACKUP_CATALOG__

#include <sqlite3.h>
#include <list>

#include <common.hxx>
#include <catalog.hxx>

namespace credativ {

  /*
   * Defines flags to characterize the
   * action defined by a catalog descriptor.
   */
  typedef enum {
    EMPTY_DESCR = -1,
    CREATE_ARCHIVE,
    DROP_ARCHIVE,
    ALTER_ARCHIVE,
    VERIFY_ARCHIVE,
    LIST_ARCHIVE
  } CatalogTag;

  /*
   * Base catalog exception.
   */
  class CCatalogIssue : public CPGBackupCtlFailure {
  public:
    CCatalogIssue(const char *errstr) throw() : CPGBackupCtlFailure(errstr) {};
    CCatalogIssue(std::string errstr) throw() : CPGBackupCtlFailure(errstr) {};
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
    std::string archive_name = "";
    std::string label;
    bool compression = false;
    std::string directory;
    std::string pghost = "";
    int    pgport = -1;
    std::string pguser = "";
    std::string pgdatabase = "";

    CatalogDescr& operator=(CatalogDescr source);
  };

  class BackupCatalog : protected CPGBackupCtlBase {
  private:
    sqlite3 *db_handle;

    virtual std::shared_ptr<CatalogDescr> fetchArchiveDataIntoDescr(sqlite3_stmt *stmt,
                                                                    std::shared_ptr<CatalogDescr> descr)
      throw (CCatalogIssue);

    virtual std::string affectedColumnsToString(std::vector<int> affectedAttributes);

  protected:
    std::string sqliteDB;
    std::string archiveDir;
    bool   isOpen;
  public:
    BackupCatalog();
    BackupCatalog(std::string sqliteDB, std::string archiveDir) throw(CCatalogIssue);
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
    static std::string mapAttributeId(int catalogEntity,
                                      int colId);

    /*
     * Returns a col=? string suitable to be used
     * in an dynamically generated UPDATE SQL command.
     */
    static std::string SQLgetUpdateColumnTarget(int catalogEntity, int colId);

    /*
     * Returns the internal catalog magic number
     */
    static std::string magicNumber() {
      return CPGBackupCtlBase::intToStr(CATALOG_MAGIC);
    }

    /*
     * Bind affected archive attribute values to the given SQLite3
     * stmt handle.
     */
    int SQLbindArchiveAttributes(std::shared_ptr<CatalogDescr> descr,
                                 std::vector<int> affectedAttributes,
                                 sqlite3_stmt *stmt,
                                 Range range)
      throw(CCatalogIssue);

    /*
     * Rollback an existing catalog transaction.
     */
    virtual void rollbackTransaction() throw(CCatalogIssue);

    /*
     * Checks if the specified archive directory is already registered
     * somewhere in the archive.
     */
    virtual std::shared_ptr<CatalogDescr> exists(std::string directory)
      throw (CCatalogIssue);

    virtual std::shared_ptr<CatalogDescr> existsByName(std::string name)
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
    virtual void setCatalogDB(std::string sqliteDB);

    /*
     * Set the archive directory.
     */
    virtual void setArchiveDir(std::string archiveDir);

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
    virtual bool tableExists(std::string tableName) throw(CCatalogIssue);

    /*
     * Update archive attributes.
     */
    virtual void updateArchiveAttributes(std::shared_ptr<CatalogDescr> descr,
                                         std::vector<int> affectedAttributes)
      throw (CCatalogIssue);

    /*
     * Creates a new archive entry in the catalog database.
     */
    virtual void createArchive(std::shared_ptr<CatalogDescr> descr) throw (CCatalogIssue);

    /*
     * Delete the specified archive by name from the catalog.
     */
    virtual void dropArchive(std::string name)
      throw(CCatalogIssue);

    /*
     * Returns a SQL formatted WHERE condition with the
     * specified attributes attached.
     */
    std::string SQLgetFilterForArchive(std::shared_ptr<CatalogDescr> descr,
                                       std::vector<int> affectedAttributes,
                                       Range range,
                                       std::string op);

    /*
     * Returns a list of all registered archives in the catalog.
     */
    std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> getArchiveList()
      throw(CCatalogIssue);

    /*
     * Returns a filtered list according the specified attributes
     * defined by the CatalogDescr.affectedAttributes list.
     */
    std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> getArchiveList(std::shared_ptr<CatalogDescr> descr,
                                                                             std::vector<int> affectedAffected)
    throw (CCatalogIssue);

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
