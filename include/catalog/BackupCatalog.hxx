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

    virtual std::shared_ptr<BackupProfileDescr> fetchBackupProfileIntoDescr(sqlite3_stmt *stmt,
                                                                            std::shared_ptr<BackupProfileDescr> descr,
                                                                            Range colIdRange);
    virtual std::shared_ptr<CatalogDescr> fetchArchiveDataIntoDescr(sqlite3_stmt *stmt,
                                                                    std::shared_ptr<CatalogDescr> descr);

    /**
     * Maps column attribute numbers of the archive catalog to its names and returns
     * a comma separated string.
     */
    virtual std::string affectedColumnsToString(std::vector<int> affectedAttributes);

    /**
     * Maps attribute numbers of the given catalog entity to their names and returns
     * a comma-separated string.
     */
    virtual std::string affectedColumnsToString(int entity, std::vector<int> affectedAttributes);

    /**
     * Makes attribute nmubers of the given catalog entitity to their names
     * and returns a comma-separated string with each column identifier and
     * the specified prefix attached.
     */
    virtual std::string affectedColumnsToString(int entity,
                                                std::vector<int> affectedAttributes,
                                                std::string prefix);

    /**
     * Makes a comma-separated placeholder list from the given attribute numbers
     * given in affectedAttributes. Returns them as string.
     */
    virtual std::string SQLmakePlaceholderList(std::vector<int> affectedAttributes);

  protected:
    std::string sqliteDB;
    std::string archiveDir;
    bool   isOpen;
  public:
    BackupCatalog();
    BackupCatalog(std::string sqliteDB);
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

    /* procs catalog table */
    static std::vector<std::string> procsCatalogCols;

    /* connections catalog table */
    static std::vector<std::string> connectionsCatalogCols;

    /* retention catalog table */
    static std::vector<std::string> retentionCatalogCols;

    /* retention rules catalog table */
    static std::vector<std::string> retentionRulesCatalogCols;

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
     * Returns a comma separated list of columns specified by
     * colIds passed by the the attrs integer vector.
     */
    static std::string SQLgetColumnList(int catalogEntity, std::vector<int> attrs);

    /*
     * Returns the internal catalog magic number
     */
    static std::string magicNumber() {
      return CPGBackupCtlBase::intToStr(CATALOG_MAGIC);
    }

    /**
     * Bind affected retention attributes values
     * to the given SQLite3 statement.
     */
    int SQLbindRetentionPolicyAttributes(std::shared_ptr<RetentionDescr> retention,
                                         std::vector<int> &affectedAttributes,
                                         sqlite3_stmt *stmt,
                                         Range range);

    /**
     * Bind affected retention rules attribute values
     * to the given SQLite3 statement.
     */
    int SQLbindRetentionRuleAttributes(std::shared_ptr<RetentionRuleDescr> rule,
                                       std::vector<int> &affectedAttributes,
                                       sqlite3_stmt *stmt,
                                       Range range);

    /**
     * Bind affected backup attribute values
     * to the given SQLite3 statement.
     */
    int SQLbindBackupAttributes(std::shared_ptr<BaseBackupDescr> bbdescr,
                                sqlite3_stmt *stmt,
                                Range range);

    /*
     * Bind affected connection attribute values
     * to the given SQLite3 statement.
     */
    int SQLbindConnectionAttributes(std::shared_ptr<ConnectionDescr> conDescr,
                                    std::vector<int> affectedAttributes,
                                    sqlite3_stmt *stmt,
                                    Range range);

    /*
     * Bind affected backup profile attributes values
     * to the given SQLite3 statement handle.
     */
    int SQLbindBackupProfileAttributes(std::shared_ptr<BackupProfileDescr> profileDescr,
                                       std::vector<int> affectedAttributes,
                                       sqlite3_stmt *stmt,
                                       Range range);
    /*
     * Bind affected stream identification catalog attribute values
     * to the given SQLite3 statement handle.
     */
    int SQLbindStreamAttributes(StreamIdentification &ident,
                                std::vector<int> affectedAttributes,
                                sqlite3_stmt *stmt,
                                Range range);

    /*
     * Bind affected procs attribute values to the given
     * SQLite3 stmt handle.
     */
    int SQLbindProcsAttributes(std::shared_ptr<CatalogProc> procInfo,
                               std::vector<int> affectedAttributes,
                               sqlite3_stmt *stmt,
                               Range range);

    /*
     * Bind affected archive attribute values to the given SQLite3
     * stmt handle.
     */
    int SQLbindArchiveAttributes(std::shared_ptr<CatalogDescr> descr,
                                 std::vector<int> affectedAttributes,
                                 sqlite3_stmt *stmt,
                                 Range range);

    /**
     * Bind affected tablespace descriptor attributes to the
     * given sqlite3 statement handle.
     */
    int SQLbindBackupTablespaceAttributes(std::shared_ptr<BackupTablespaceDescr> tblspcDescr,
                                          std::vector<int> affectedAttributes,
                                          sqlite3_stmt *stmt,
                                          Range range);

    /**
     * Rollback an existing catalog transaction.
     */
    virtual void rollbackTransaction();

    /**
     * Checks if the specified archive directory is already registered
     * in the catalog. Returns a valid CatalogDescr handle if true, otherwise
     * the returned CatalogDescr handle is initialized with id = -1.
     */
    virtual std::shared_ptr<CatalogDescr> exists(std::string directory);

    /**
     * Checks if the specified archive name is already
     * registered in the catalog. Returns a valid CatalogDescr handle if
     * true, otherwise the returned CatalogDescr handle is initialized
     * with id = -1
     */
    virtual std::shared_ptr<CatalogDescr> existsByName(std::string name);

    /**
     * Checks if the specified archive id exists in the backup
     * catalog. Returns a valid CatalogDescr handle if true, otherwise the returned
     * CatalogDescr handle is initialized with -1.
     */
    virtual std::shared_ptr<CatalogDescr> existsById(int archive_id);

    /*
     * Commits the current catalog transaction.
     */
    virtual void commitTransaction();

    /*
     * Starts a transaction in the catalog database.
     */
    virtual void startTransaction();

    /*
     * Set sqlite database filename.
     */
    virtual void setCatalogDB(std::string sqliteDB);

    /*
     * Returns the name of the catalog database. In this
     * case the filename of the associated sqlite3 database.
     */
    virtual std::string name();

    /**
     * Returns the full path (including filename)
     * of the connected sqlite3 database file.
     */
    virtual std::string fullname();

    /*
     * Returns true wether the catalog is available.
     */
    virtual bool available();

    /*
     * Check catalog tables.
     */
    virtual void checkCatalog();

    /*
     * Checks wether the specified table exists
     * in the catalog database.
     */
    virtual bool tableExists(std::string tableName);

    /*
     * Update archive attributes.
     */
    virtual void updateArchiveAttributes(std::shared_ptr<CatalogDescr> descr,
                                         std::vector<int> affectedAttributes);

    /*
     * Creates a new archive entry in the catalog database.
     */
    virtual void createArchive(std::shared_ptr<CatalogDescr> descr);

    /*
     * Drop the specified backup profile.
     */
    virtual void dropBackupProfile(std::string profileName);

    /*
     * Creates a new backup profile.
     */
    virtual void createBackupProfile(std::shared_ptr<BackupProfileDescr> profileDescr);

    /*
     * Get a list of all registered backup profiles.
     */
    virtual std::shared_ptr<std::list<std::shared_ptr<BackupProfileDescr>>> getBackupProfiles();

    /*
     * Returns the specified backup profile.
     *
     * NOTE: the catalog descr will aways be initialized, but a
     *       non existing backup profile name will return a descriptor
     *       handle which id is set to -1!
     */
    virtual std::shared_ptr<BackupProfileDescr> getBackupProfile(std::string name);

    /*
     * Create a catalog database connection entry.
     */
    virtual void createCatalogConnection(std::shared_ptr<ConnectionDescr> conDescr);

    /*
     * Remove a catalog database connection
     */
    virtual void dropCatalogConnection(std::string archive_name, std::string type);

    /*
     * Updates the specified catalog connection entry.
     */
    virtual void updateCatalogConnection(std::shared_ptr<ConnectionDescr> conInfo,
                                         std::string archive_name,
                                         std::string type);

    /*
     * Initializes the specified conDescr ConnectionDescr object with
     * catalog information, if exists.
     *
     * If the specified archive_id has no connection of type specified, the
     * conDescr ConnectionDescr object will be initialized with id = -1 and
     * type = CONNECTION_TYPE_UNKNOWN.
     */
    virtual void getCatalogConnection(std::shared_ptr<ConnectionDescr> conDescr,
                                      int archive_id,
                                      std::string type);

    /*
     * Returns all connections defined for the specified archive_id.
     */
    virtual std::vector<std::shared_ptr<ConnectionDescr>> getCatalogConnection(int archive_id);

    /*
     * Delete the specified archive by name from the catalog.
     */
    virtual void dropArchive(std::string name);

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
    std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> getArchiveList();

    /*
     * Returns a filtered list according the specified attributes
     * defined by the CatalogDescr.affectedAttributes list.
     */
    std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> getArchiveList(std::shared_ptr<CatalogDescr> descr,
                                                                             std::vector<int> affectedAffected);

    /*
     * Open the sqlite database for read/write.
     */
    virtual void open_rw();

    /*
     * Close the sqlite catalog database.
     */
    virtual void close();

    /*
     * Register the specified process handle in the catalog database.
     */
    virtual void registerProc(std::shared_ptr<CatalogProc> procInfo);

    /*
     * Unregister a process handle from the catalog database.
     */
    virtual void unregisterProc(int pid, int archive_id);

    /*
     * Update a catalog process handle in the database.
     */
    virtual void updateProc(std::shared_ptr<CatalogProc> procInfo,
                            std::vector<int> affectedAttributes,
                            int pid,
                            int archive_id);

    /*
     * Register a stream in the catalog.
     *
     * The streamident object reference is initialized with the
     * stream id.
     */
    virtual void registerStream(int archive_id,
                                std::string type,
                                StreamIdentification& streamident);

    /*
     * Drop the stream from the catalog database.
     */
    virtual void dropStream(int streamid);

    /**
     * Returns a StreamIdentification shared pointer
     * from a result set based on the stream catalog table.
     */
    std::shared_ptr<StreamIdentification>
    fetchStreamData(sqlite3_stmt *stmt,
                    std::vector<int> affectedRows);

    /**
     * Reads the current row from the result set
     * identified by stmt. The returned RetnetionDescr
     * describes the stored retention policy afterwards.
     *
     * fetchRentionPolicy() doesn't fetch the policy rule(s)
     * attached by the current retention policy row.
     * Instead, it gets employed by getRetentionPolicy() which does
     * all the legwork to fetch a complete retention policy rule set.
     */
    std::shared_ptr<RetentionDescr> fetchRetentionPolicy(sqlite3_stmt *stmt,
                                                         std::shared_ptr<RetentionDescr> retention,
                                                         Range colIdRange);

    /**
     * Reads a retention rule definition from the current
     * row identified by stmt.
     */
    std::shared_ptr<RetentionRuleDescr> fetchRetentionRule(sqlite3_stmt *stmt,
                                                           std::shared_ptr<RetentionRuleDescr> retentionRule,
                                                           Range colIdRange);

    /**
     * Returns a retention policy descriptor with
     * all rule(s) attached.
     *
     * name is the identifier of the policy to be retrieved.
     * If the specified name cannot be found in the catalog,
     * a RetentionDescr pointer will be returned with a rule_id set to -1.
     *
     * A catalog database access error always throws a CCatalogIssue
     * exception.
     */
    virtual std::shared_ptr<RetentionDescr> getRetentionPolicy(std::string name);

    /**
     * Returns a list of all retention policies defined in the
     * backup catalog.
     *
     * attributesRetention must be a list of attribute identifiers
     * which should be retrieved for each policy from the catalog, the
     * same for attributesRules. If no rules information belonging to
     * a retention policy should be fetched, an empty vector in attributesRules
     * can be specified. attributesRetention must contain at least one
     * attribute.
     */
    virtual void getRetentionPolicies(std::vector<std::shared_ptr<RetentionDescr>> &list,
                                      std::vector<int> attributesRetention,
                                      std::vector<int> attributesRules);

    /**
     * Drops the specified retention policy and associated rules
     * from the backup catalog.
     */
    virtual void dropRetentionPolicy(std::string retention_name);

    /**
     * Creates a new retention policy described
     * by the specified RetentionDescr.
     */
    virtual void createRetentionPolicy(std::shared_ptr<RetentionDescr> retentionPolicy);

    /**
     * Creates or removes a pin on the specified basebackup ID(s).
     *
     * A pin is a lock placed on the basebackup catalog entry,
     * so that any retention policy applied will not delete
     * it. performPinAction() won't check if a pin is already placed
     * on the specified basebackup ID, this should be done
     * by the caller before.
     *
     * The same applies if a UNPIN action is specified, if
     * the specified backup id is not pinned, we will
     * execute the unpin, nevertheless.
     */
    virtual void performPinAction(BasicPinDescr *descr,
                                  std::vector<int> basebackupIds);

    /**
     * Fetch backup information from current stmt handle into
     * the specified BaseBackupDescr handle.
     */
    std::shared_ptr<BaseBackupDescr> fetchBackupIntoDescr(sqlite3_stmt *stmt,
                                                          std::shared_ptr<BaseBackupDescr> descr,
                                                          Range colIdRange);

    /**
     * Returns a list of all tablespaces belonging
     * to the specified base backup.
     */
    std::shared_ptr<BackupTablespaceDescr>
    fetchBackupTablespaceIntoDescr(sqlite3_stmt *stmt,
                                   std::shared_ptr<BackupTablespaceDescr> tablespace,
                                   Range range);

    /*
     * Fetch catalog process information from
     * statement handle.
     */
    std::shared_ptr<CatalogProc> fetchCatalogProcData(sqlite3_stmt *stmt,
                                                      std::vector<int> affectedAttributes);

    void fetchConnectionData(sqlite3_stmt *stmt,
                             std::shared_ptr<ConnectionDescr> conDescr);

    /*
     * Returns catalog process handle information, if any.
     * The returned CatalogProc handle is initialized with
     * PID -1 and archive id -1 in case no process handle
     * exists for the specified archive id.
     *
     * type must either be "launcher" or "archive streaming worker".
     */
    virtual std::shared_ptr<CatalogProc> getProc(int archive_id, std::string type);

    /*
     * Get a list of streams for the specified archive.
     */
    virtual void getStreams(std::string archive_name,
                            std::vector<std::shared_ptr<StreamIdentification>> &result);

    /*
     * Update the status of the specified stream.
     */
    virtual void setStreamStatus(int streamid,
                                 std::string status);

    /*
     * Updates the specified stream handle in the
     * catalog.
     *
     * affectedCols needs to specify the database columns
     * being updated.
     */
    virtual void updateStream(int streamid,
                              std::vector<int> affectedColumns,
                              StreamIdentification &streamident);

    /*
     * Register a started basebackup. This will create
     * a new entry in the backup table indicating a basebackup
     * was currently started. The basebackup is marked "in progress"
     * as long as basebackup_finalize() was called for the basebackup
     * handle.
     */
    virtual void registerBasebackup(int archive_id,
                                    std::shared_ptr<BaseBackupDescr> backupDescr);

    /**
     * Delete the specified basebackup from the archive.
     */
    virtual void deleteBaseBackup(int basebackupId);

    /*
     * Abort a registered basebackup. Marks the specified basebackup as failed.
     */
    virtual void abortBasebackup(std::shared_ptr<BaseBackupDescr> backupDescr);

    /*
     * Finalize a basebackup in progress. This marks the basebackup a successful
     * and usable in the catalog.
     */
    virtual void finalizeBasebackup(std::shared_ptr<BaseBackupDescr> backupDescr);

    /**
     * Returns a hash map with all backup tablespaces
     * belonging to the specified backup_id. The key
     * is the backup_id the tablespace belongs to.
     */
    virtual std::vector<std::shared_ptr<BackupTablespaceDescr>>
    getBackupTablespaces(int backup_id,
                         std::vector<int> attrs);

    /**
     * Register the given tablespace descriptor in the
     * backup catalog.
     *
     * Backup ID must be set and the descriptor should be
     * fully initialized.
     */
    virtual void registerTablespaceForBackup(std::shared_ptr<BackupTablespaceDescr> tblspcDescr);

    /**
     * Retrieve a complete list of backups stored in
     * the current catalog.
     *
     * This creates a list of backup handles with
     * alle referenced tablespaces.
     *
     * The returned list is pre-sorted by creation date,
     * the newest basebackup appears first in the list.
     */
    virtual std::vector<std::shared_ptr<BaseBackupDescr>>
    getBackupList(std::string archive_name);

    /**
     * Returns the latest backup in the catalog.
     *
     * valid_only specified wether the method should only
     * consider basebackups in state "ready". If set to false,
     * getLatestBaseBackup() will also consider "in progress" or
     * "aborted" basebackups.
     */
    virtual std::shared_ptr<BaseBackupDescr> getLatestBaseBackup(bool valid_only);

    /**
     * Returns a basebackup descriptor, describing the
     * specified basebackup referenced within the given archive_id
     * and by the basebackup ID.
     *
     * The returned pointer is always
     * initialized, if the specified basebackup cannot be
     * found the returned BaseBackupDescr has an ID set to -1.
     */
    virtual std::shared_ptr<BaseBackupDescr> getBaseBackup(int basebackupId,
                                                           int archive_Id);

    /**
     * Returns a catalog status view for the given archive.
     */
    virtual std::shared_ptr<StatCatalogArchive> statCatalog(std::string archive_name);

    /**
     * Returns the compiled in catalog magic number. Should
     * match at least the version returned from the catalog database
     * via getCatalogVersion();
     */
    static int getCatalogMagic();

    /**
     * Returns the catalog version number.
     *
     * This method returns the version number of the
     * database catalog, *not* the catalog version this module
     * needs. See catalogMagicNumber() for the compiled in
     * catalog version.
     */
    virtual int getCatalogVersion();
  };
}

#endif
