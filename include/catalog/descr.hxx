#ifndef __HAVE_CATALOGDESCR__
#define __HAVE_CATALOGDESCR__

#include <common.hxx>

namespace credativ {

  /*
   * Forwarded declarations
   */
  class BackupProfileDescr;
  class BackupTableSpaceDescr;
  class BaseBackupDescr;
  class PushableCols;

  /*
   * Defines flags to characterize the
   * action defined by a catalog descriptor.
   */
  typedef enum {
    EMPTY_DESCR = -1,
    CREATE_ARCHIVE,
    CREATE_BACKUP_PROFILE,
    CREATE_CONNECTION,
    DROP_ARCHIVE,
    DROP_BACKUP_PROFILE,
    ALTER_ARCHIVE,
    VERIFY_ARCHIVE,
    START_BASEBACKUP,
    LIST_ARCHIVE,
    LIST_BACKUP_PROFILE,
    LIST_BACKUP_PROFILE_DETAIL,
    LIST_BACKUP_CATALOG,
    LIST_CONNECTION,
    START_LAUNCHER,
    BACKGROUND_WORKER_COMMAND
  } CatalogTag;

  /*
   * Compression types supported for backup profiles.
   */
  typedef enum {

    BACKUP_COMPRESS_TYPE_NONE = 0,
    BACKUP_COMPRESS_TYPE_GZIP = 1,
    BACKUP_COMPRESS_TYPE_ZSTD

  } BackupProfileCompressType;

  /*
   * Represents a physical replication slot.
   * State of base backup stream.
   */
  struct PhysicalReplicationSlot {
    std::string slot_name;
    std::string consistent_point;
    std::string snapshot_name;
    std::string output_plugin;
  };

  /*
   * Base class for descriptors which wants
   * to have dynamic cols associated.
   */
  class PushableCols {
  protected:
    std::vector<int> affectedAttributes;
  public:
    PushableCols();
    ~PushableCols();

    virtual void pushAffectedAttribute(int colId);
    virtual std::vector<int> getAffectedAttributes();
    virtual void setAffectedAttributes(std::vector<int> affectedAttributes);
    virtual void clearAffectedAttributes();
  };

  /*
   * Represents a catalog database connection.
   */
  class ConnectionDescr : public PushableCols {
  public:
    static constexpr const char *CONNECTION_TYPE_BASEBACKUP = "basebackup";
    static constexpr const char *CONNECTION_TYPE_STREAMER = "streamer";
    static constexpr const char *CONNECTION_TYPE_UNKNOWN = "unknown";

    int archive_id = -1;
    std::string type = ConnectionDescr::CONNECTION_TYPE_UNKNOWN;
    std::string pghost = "";
    int    pgport = -1;
    std::string pguser = "";
    std::string pgdatabase = "";
    std::string dsn = "";
  };

  /*
   * Represents an identified streaming connection.
   */
  class StreamIdentification : public PushableCols {
  public:
    unsigned long long id = -1; /* internal catalog stream id */
    int archive_id = -1; /* used to reflect assigned archive */
    std::string stype;
    std::string slot_name;
    std::string systemid;
    int         timeline;
    std::string xlogpos;
    std::string dbname;
    std::string status;
    std::string create_date;

    StreamIdentification();
    ~StreamIdentification();

    /*
     * Physical replication slot, if any
     */
    std::shared_ptr<PhysicalReplicationSlot> slot = nullptr;

    /*
     * Set properties back to default.
     */
    void reset();

    /*
     * Returns the decoded XLogRecPtr from xlogpos
     */
    XLogRecPtr getXLOGStartPos();
  };

  /*
   * Catalog descriptor for background procs
   * entities in the backup catalog.
   */
  class CatalogProc : public PushableCols {
  public:
    /*
     * Static class members, identifying proc types.
     */
    static constexpr const char *PROC_TYPE_LAUNCHER = "launcher";
    static constexpr const char *PROC_TYPE_WORKER = "streamer";

    /*
     * Static class members, specifying proc status values.
     */
    static constexpr const char *PROC_STATUS_RUNNING = "running";
    static constexpr const char *PROC_STATUS_SHUTDOWN = "shutdown";

    int pid = -1;
    int archive_id = -1;
    std::string type;
    std::string started;
    std::string state;

  };

  /*
   * A catalog descriptor is a reference
   * into the catalog database, representing an interface
   * between CPGBackupCtlFS and BackupCatalog objects.
   *
   * IMPORTANT:
   *
   * If you make any changes to member variables here, be sure to
   * adjust the BaseCatalogCommand::copy() method to reference
   * your new members during copy as well!
   */
  class CatalogDescr : protected CPGBackupCtlBase, public PushableCols {
  protected:
    std::shared_ptr<BackupProfileDescr> backup_profile = std::make_shared<BackupProfileDescr>();
  public:
    CatalogDescr() { tag = EMPTY_DESCR; };
    ~CatalogDescr() {};

    CatalogTag tag;
    int id = -1;
    std::string archive_name = "";
    std::string label;
    bool compression = false;
    std::string directory;
    // std::string pghost = "";
    // int    pgport = -1;
    // std::string pguser = "";
    // std::string pgdatabase = "";
    // std::string dsn;
    std::shared_ptr<ConnectionDescr> coninfo = std::make_shared<ConnectionDescr>();

    /*
     * Properties for job control
     */
    bool detach = true;

    /*
     * Static class methods.
     */
    static std::string commandTagName(CatalogTag tag);

    /*
     * Returns command tag as string.
     */
    std::string getCommandTagAsStr();

    /*
     * The methods below are used by our spirit::parser
     * implementation.
     */
    void setDbName(std::string const& db_name);

    void setCommandTag(credativ::CatalogTag const& tag);

    void setIdent(std::string const& ident);

    void setHostname(std::string const& hostname);

    void setUsername(std::string const& username);

    void setPort(std::string const& portNumber);

    void setDirectory(std::string const& directory);

    void setProfileName(std::string const& profile_name);

    void setProfileCompressType(BackupProfileCompressType const& type);

    void setProfileMaxRate(std::string const& max_rate);

    std::shared_ptr<BackupProfileDescr> getBackupProfileDescr();

    void setProfileBackupLabel(std::string const& label);

    void setProfileWALIncluded(bool const& included);

    void setProfileCheckpointMode(bool const& fastmode);

    void setProfileWaitForWAL(bool const& wait);

    void setProfileAffectedAttribute(int const& colId);

    void setDSN(std::string const& dsn);

    void setArchiveId(int const& archive_id);

    void setConnectionType(std::string const& type);

    void setJobDetachMode(bool const& detach);

    CatalogDescr& operator=(const CatalogDescr& source);
  };

  /*
   * A BackupProfileDescr is a descriptor referencing
   * a backup profile entry for the specified archive in the
   * catalog.
   */
  class BackupProfileDescr : public PushableCols {
  public:
    int profile_id = -1;

    std::string name;
    BackupProfileCompressType compress_type = BACKUP_COMPRESS_TYPE_NONE;
    unsigned int max_rate = 0;
    std::string label = "PG_BCK_CTL BASEBACKUP";
    bool fast_checkpoint = false;
    bool include_wal     = false;
    bool wait_for_wal    = true;
  };

  /*
   * A BackupTablespaceDescr is a descriptor handle which
   * directly references tablespace meta information in the backup
   * catalog.
   */
  class BackupTablespaceDescr : public PushableCols {
  public:
    int id = -1;
    int backup_id = -1;
    int spcoid;
    std::string spclocation;
    unsigned long long spcsize;
  };

  /*
   * BaseBackupDescr represents a
   * catalog entry for either a running
   * or finalized basebackup.
   */
  class BaseBackupDescr : public PushableCols {
  public:
    int id = -1;
    int archive_id = -1;

    std::string xlogpos;
    std::string xlogposend;
    int timeline;
    std::string label;
    std::string fsentry;
    std::string started;
    std::string stopped;
    int pinned = 0;
    std::string status = "in progress";

    /* List of tablespaces descriptors in backup */
    std::vector<std::shared_ptr<BackupTablespaceDescr>> tablespaces;
  };

  /*
   * StatCatalog is a base class for stat commands
   * against the archive backup catalog. The idea is to
   * provide a generic interface to the commands to create
   * corresponding output for a specific stat*() call. Specific
   * descriptor should override the abstract method
   * gimmeFormattedString() to generate a string representing
   * the stat data.
   */
  class StatCatalog {
  public:
    virtual std::string gimmeFormattedString() = 0;
  };

  /*
   * Provides stat data for the archive itself.
   */
  class StatCatalogArchive : public StatCatalog {
  public:
    /* member values */
    int archive_id;
    int number_of_backups = 0;
    int backups_failed = 0;
    int backups_running = 0;

    std::string archive_name = "";
    std::string archive_directory = "";
    std::string archive_host = "";
    unsigned long long estimated_total_size = 0;
    unsigned long avg_backup_duration = 0;

    std::string latest_finished = "";

    virtual std::string gimmeFormattedString();
  };
}

#endif
