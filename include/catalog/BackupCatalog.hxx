#ifndef __BACKUP_CATALOG__
#define __BACKUP_CATALOG__

#include <sqlite3.h>
#include <list>

#include <common.hxx>
#include <catalog.hxx>
#include <descr.hxx>
#include <stream.hxx>

namespace credativ {

  /*
   * Base catalog exception.
   */
  class CCatalogIssue : public CPGBackupCtlFailure {
  public:
    CCatalogIssue(const char *errstr) throw() : CPGBackupCtlFailure(errstr) {};
    CCatalogIssue(std::string errstr) throw() : CPGBackupCtlFailure(errstr) {};
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
     * Keep changes to catalog tables
     * in sync with macros from
     * include/catalog/catalog.hxx !!
     */

    /* archive table */
    static std::vector<std::string> archiveCatalogCols;

    /* backup table */
    static std::vector<std::string> backupCatalogCols;

    /* stream table */
    static std::vector<std::string> streamCatalogCols;

    /* backup profiles table */
    static std::vector<std::string> backupProfilesCatalogCols;

    /* backup tablespaces catalog table */
    static std::vector<std::string> backupTablespacesCatalogCols;
    
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
     * in the catalog. Returns a valid CatalogDescr handle if true, otherwise
     * the returned CatalogDescr handle is initialized with id = -1.
     */
    virtual std::shared_ptr<CatalogDescr> exists(std::string directory)
      throw (CCatalogIssue);

    /*
     * Checks if the specified archive name is already
     * registered in the catalog. Returns a valid CatalogDescr handle if
     * true, otherwise the returned CatalogDescr handle is initialized
     * with id = -1
     */
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
     * Creates a new backup profile.
     */
    virtual void createBackupProfile(std::string archive_name,
                                     std::shared_ptr<BackupProfileDescr> profileDescr);
    
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

    /*
     * Register a stream in the catalog.
     *
     * The streamident object reference is initialized with the
     * stream id.
     */
    virtual void registerStream(int archive_id,
                                credativ::streaming::StreamIdentification& streamident)
      throw (CCatalogIssue);

    /*
     * Drop the stream from the catalog database.
     */
    virtual void dropStream(int streamid)
      throw(CCatalogIssue);

    /*
     * Returns a StreamIdentification shared pointer
     * from a result set based on the stream catalog table.
     */
    std::shared_ptr<credativ::streaming::StreamIdentification>
       fetchStreamData(sqlite3_stmt *stmt,
                       std::string archive_name,
                       std::vector<int> affectedRows)
      throw(CCatalogIssue);

    /*
     * Get a list of streams for the specified archive.
     */
    std::vector<std::shared_ptr<credativ::streaming::StreamIdentification>>
       getStreams(std::string archive_name)
    throw(CCatalogIssue);

    /*
     * Update the status of the specified stream.
     */
    virtual void setStreamStatus(int streamid,
                                 std::string status)
      throw(CCatalogIssue);

  };
}

#endif
