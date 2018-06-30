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
  class BasicPinDescr;
  class PinDesc;
  class UnpinDescr;
  class RetentionDescr;
  class RetentionRuleDescr;

  /*
   * Defines flags to characterize the
   * action defined by a catalog descriptor.
   *
   * IMPORTANT:
   *
   * Adding tags here requires CatalogDescr::setCommandTag()
   * being teached about the new tag, too (src/catalog/catalog.cxx).
   */
  typedef enum {
    EMPTY_DESCR = -1,
    CREATE_ARCHIVE,
    CREATE_BACKUP_PROFILE,
    CREATE_CONNECTION,
    DROP_ARCHIVE,
    DROP_BACKUP_PROFILE,
    DROP_CONNECTION,
    EXEC_COMMAND,
    ALTER_ARCHIVE,
    VERIFY_ARCHIVE,
    START_BASEBACKUP,
    LIST_ARCHIVE,
    LIST_BACKUP_PROFILE,
    LIST_BACKUP_PROFILE_DETAIL,
    LIST_BACKUP_CATALOG,
    LIST_BACKUP_LIST,
    LIST_CONNECTION,
    PIN_BASEBACKUP,
    UNPIN_BASEBACKUP,
    START_LAUNCHER,
    START_STREAMING_FOR_ARCHIVE,
    STOP_STREAMING_FOR_ARCHIVE,
    SHOW_WORKERS,
    BACKGROUND_WORKER_COMMAND,
    CREATE_RETENTION_POLICY,
    LIST_RETENTION_POLICIES,
    LIST_RETENTION_POLICY,
    DROP_RETENTION_POLICY,
    APPLY_RETENTION_POLICY
  } CatalogTag;

  /*
   * Compression types supported for backup profiles.
   */
  typedef enum {

    BACKUP_COMPRESS_TYPE_NONE = 0,
    BACKUP_COMPRESS_TYPE_GZIP = 1,
    BACKUP_COMPRESS_TYPE_ZSTD = 2,
    BACKUP_COMPRESS_TYPE_PBZIP = 3,
    BACKUP_COMPRESS_TYPE_PLAIN = 4

  } BackupProfileCompressType;

  typedef enum {

    REPLICATION_SLOT_OK,
    REPLICATION_SLOT_EXISTS,
    REPLICATION_SLOT_ERROR

  } ReplicationSlotStatus;

  /*
   * A retention rule id classifies the
   * various supported retention rules and their
   * actions.
   */
  typedef enum {

    RETENTION_NO_RULE = 100, /* unknown/undefined rule type */

    RETENTION_KEEP_WITH_LABEL = 200,
    RETENTION_DROP_WITH_LABEL = 201,

    RETENTION_KEEP_NUM = 300,
    RETENTION_DROP_NUM = 301,

    RETENTION_KEEP_BY_DATETIME = 400,
    RETENTION_DROP_BY_DATETIME = 401,

    /* PIN/UNPIN retention action */
    RETENTION_PIN = 500,
    RETENTION_UNPIN = 600

  } RetentionRuleId;

  /*
   * Represents a physical replication slot.
   * State of base backup stream.
   */
  struct PhysicalReplicationSlot {

    /*
     * Fields normally initialized by calling
     * PGStream::createPhysicalReplicationSlot(). Please note
     * that this is also version dependent!
     */
    std::string slot_name;
    std::string consistent_point;

    /* Unused fields atm */
    std::string snapshot_name;
    std::string output_plugin;

    /*
     * Settings for the replication slot.
     */
    bool reserve_wal = false;
    bool existing_ok = false;
    bool no_identok  = false;

    /*
     * Flag indicating that the slot
     * already existed and we have to ignore
     * it.
     *
     * This flag is only set by PGStream::createPhysicalReplicationSlot().
     */
    ReplicationSlotStatus status;
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

    static constexpr const char * STREAM_PROGRESS_IDENTIFIED = "IDENTIFIED";
    static constexpr const char * STREAM_PROGRESS_STREAMING = "STREAMING";
    static constexpr const char * STREAM_PROGRESS_SHUTDOWN = "SHUTDOWN";
    static constexpr const char * STREAM_PROGRESS_FAILED = "FAILED";

    unsigned long long id = -1; /* internal catalog stream id */
    int archive_id = -1; /* used to reflect assigned archive */
    std::string stype;
    std::string slot_name;
    std::string systemid;
    unsigned int timeline;
    std::string xlogpos;
    std::string dbname;
    std::string status;
    std::string create_date;

    /**
     * Runtime variable wal_segment_size, transports
     * the configured wal_segment_size during streaming
     * operation.
     *
     * Usually this gets initialized by instantiating
     * a PGStream object and establish a streaming connnection
     * (e.g. PGStream::connect()).
     */
    unsigned long long wal_segment_size = -1;

    /*
     * Tells the stream to restart from the server XLOG
     * position without consulting the catalog. Only used
     * during runtime.
     */
    bool force_xlogpos_restart = false;

    /*
     * Runtime streaming properties. Those usually
     * get instrumented for example by a WALStreamerProcess
     * instance.
     */
    int write_pos_start_offset = 0; /* starting offset into current XLogSegment */
    XLogRecPtr flush_position = InvalidXLogRecPtr;
    XLogRecPtr write_position = InvalidXLogRecPtr;
    XLogRecPtr apply_position = InvalidXLogRecPtr;
    XLogRecPtr server_position = InvalidXLogRecPtr;

    /*
     * Additional properties, those aren't necessarily
     * initialized. Use them with care.
     */
    std::string archive_name = "";

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
    XLogRecPtr xlogposDecoded();
    std::string xlogposEncoded();

    /**
     * Updates the internal write position segment
     * to XLOG segment start boundary.
     *
     * Please note that calling this method is only legit if you have
     * set the write_position and WAL segment size (which
     * might be hard coded if compiled against PostgreSQL < 11).
     */
    int updateStartSegmentWriteOffset();

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
    static constexpr const char *PROC_TYPE_STREAMER = "streamer";
    static constexpr const char *PROC_TYPE_WORKER = "worker";

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
    key_t shm_key;
    int   shm_id;

  };

  /**
   * PIN/UNPIN operation actions. Used to identify
   * the actions a PinDescr instance needs to perform.
   */
  typedef enum {

    /**
     * References a basebackup to PIN or UNPIN by its ID.
     */
    ACTION_ID = 100,

    /**
     * Gives the number of basebackups to PIN or UNPIN. The number is always
     * applied to the number of basebackups in ascending order, sorted by
     * their creation date.
     */
    ACTION_COUNT,

    /**
     * PIN/UNPIN newest basebackup
     */
    ACTION_NEWEST,

    /**
     * PIN/UNPIN oldest basebackup
     */
    ACTION_OLDEST,

    /**
     * For unpin only, ACTION_PINNED references
     * currently pinned basebackups.
     */
    ACTION_PINNED,

    /**
     * For uninitialized PinDescr instances
     */
    ACTION_UNDEFINED

  } PinOperationType;

  /**
   * A BasicPinDescr is the ancestor of either a PinDescr or UnpinDescr
   * instance. Both encapsulate a PIN or UNPIN action respectively.
   *
   * A BasicPinDescr isn't really usable, to identify a PIN or
   * UNPIN action you need an instance of either UnpinDescr
   * or PinDescr.
   */
  class BasicPinDescr {
  protected:

    /*
     * Catalog command tag.
     */
    CatalogTag tag;

    /*
     * Operation type for UNPIN/PIN action.
     */
    PinOperationType operation = ACTION_UNDEFINED;

    /**
     * If operation == ACTION_ID, then backupid is set to
     * the backup ID to operate on.
     *
     * If operation == ACTION_COUNT, we set the number of
     * basebackups to PIN or UNPIN in count.
     */
    union {
      int backupid = -1;
      unsigned int count;
    };

    /**
     * Converts the backup ID into an integer value
     * and assigns it to a PinDescr/UnpinDescr instance.
     */
    int bckIDStr(std::string backupid);

  public:

    BasicPinDescr();
    virtual ~BasicPinDescr();

    /**
     * Set backup ID
     */
    virtual void setBackupID(int backupid);
    virtual void setBackupID(std::string backupid);

    /**
     * Set number of PIN/UNPIN basebackups
     */
    virtual void setCount(std::string count);
    virtual void setCount(unsigned int count);

    /**
     * Returns the number of basebackups for
     * the PIN/UNPIN action.
     *
     * If the operation type of a BasicPinDescr instance
     * (specifically on if its descendants PinDescr/UnpinDescr)
     * doesn't reference a ACTION_COUNT flag, an CPGBackupCtlFailure
     * exception is thrown.
     */
    virtual unsigned int getCount();

    /**
     * Returns the backup ID associated with
     * PIN/UNPIN action. Throws a CPGBackupCtlFailure
     * exception in case the command type is not
     * referencing a backup ID.
     */
    virtual int getBackupID();

    /**
     * Returns the PIN/UNPIN operationt type.
     */
    virtual PinOperationType getOperationType();

    static BasicPinDescr *instance(CatalogTag action,
                                   PinOperationType type);

    virtual CatalogTag action();
  };

  class PinDescr : public BasicPinDescr {
  public:

    PinDescr(PinOperationType operation);

  };

  class UnpinDescr : public BasicPinDescr {
  public:

    UnpinDescr(PinOperationType operation);

  };

  /**
   * Option flags for the VERIFY ARCHIVE command.
   */
  typedef enum {

    VERIFY_DATABASE_CONNECTION

  } VerifyOption;

  /**
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

    /**
     * A retention rule descriptor for retention policies created during
     * parsing a CREATE RETENTION POLICY command.
     *
     * For correct handling, you have to call
     * makeRetentionDescr() before! Otherwise this instance
     * is always a nullptr.
     */
    std::shared_ptr<RetentionDescr> retention = nullptr;

  public:
    CatalogDescr() { tag = EMPTY_DESCR; };
    ~CatalogDescr();

    CatalogTag tag;
    int id = -1;
    std::string archive_name = "";
    std::string retention_name = "";
    std::string label;
    bool compression = false;
    std::string directory;

    /**
     * A PinDescr instance is initialized by the parser when
     * handling a PIN command. By default, a caller can only
     * be sure if a pin descriptor instance is correctly
     * initialized when the catalog tag is set to
     * either PIN_BASEBACKUP or UNPIN_BASEBACKUP _and_
     * the makePinDescr() method was called!
     */
    BasicPinDescr pinDescr;

    /**
     * Connection identifier used by the descriptor instance.
     *
     * NOTE: We can have multiple connection definitions for an archive,
     *       but usually we use only one at a time.
     */
    std::shared_ptr<ConnectionDescr> coninfo = std::make_shared<ConnectionDescr>();

    /*
     * Properties for job control
     */
    bool detach = true;

    /**
     * Properties for streaming control
     */
    bool forceXLOGPosRestart = false;

    /*
     * VERIFY command options.
     */
    bool check_connection = false;

    /**
     * Used for executing shell commands.
     */
    std::string execString = "";

    /*
     * Static class methods.
     */
    static std::string commandTagName(CatalogTag tag);

    /*
     * Returns command tag as string.
     */
    std::string getCommandTagAsStr();

    /**
     * Initialize a PinDescr attached to a catalog
     * descr.
     */
    void makePinDescr(PinOperationType const &operation,
                      std::string const &argument);

    /**
     * Initialize a PinDescr attached to a catalog descr.
     * Overloaded version which doesn't require a action
     * argument, e.g. for ACTION_OLDEST or ACTION_NEWEST
     * pin operation actions.
     */
    void makePinDescr(PinOperationType const& operation);

    /**
     * Creates a retention descriptor attached
     * to a catalog descriptor instance. The retention rule
     * is created along with the retention descriptor and added to
     * its rule list. If a retention descriptor already exists for
     * a catalog descriptor, just the new retention rule will be added
     * to this existing instance.
     */
    void makeRetentionDescr(RetentionRuleId const &ruleid,
                            std::string const& value);

    /**
     * Returns an shared_ptr to the internal retention policy
     * structure, if allocated. Will return a nullptr if
     * makeRetentionDescr() wasn't called before.
     */
    std::shared_ptr<RetentionDescr> getRetentionPolicyP();

    /**
     * Returns the PIN/UNPIN operation type assigned
     * to this catalog descriptor. If not initialized,
     * ACTION_UNDEFINED is returned.
     */
    PinOperationType pinOperation();

    /**
     * Set VERIFY command options.
     */
    void setVerifyOption(VerifyOption const& option);

    /*
     * The methods below are used by our spirit::parser
     * implementation.
     */
    void setExecString(std::string const& execStr);

    void setDbName(std::string const& db_name);

    void setCommandTag(credativ::CatalogTag const& tag);

    void setIdent(std::string const& ident);

    void setRetentionName(std::string const &ident);

    void setHostname(std::string const& hostname);

    void setUsername(std::string const& username);

    void setPort(std::string const& portNumber);

    void setDirectory(std::string const& directory);

    void setProfileNoVerify(bool const& noverify);

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

    void setStreamingForceXLOGPositionRestart( bool const& restart );

    CatalogDescr& operator=(CatalogDescr& source);
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
    bool noverify_checksums = false;
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
    unsigned int spcoid;
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
    std::string systemid;

    /*
     * Static const specifiers for status flags.
     */
    static constexpr const char *BASEBACKUP_STATUS_IN_PROGRESS = "in progress";
    static constexpr const char *BASEBACKUP_STATUS_ABORTED = "aborted";
    static constexpr const char *BASEBACKUP_STATUS_READY   = "ready";

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

  /**
   * A descriptor describing the catalog
   * representation of a retention rule.
   */
  class RetentionDescr : public PushableCols {
  public:
    int id = -1;
    std::string name = "";

    /*
     * Creation date of this retention policy. Please note
     * that this property isn't always initialized, especially
     * if you want to reuse it after having called BackupCatalog::createRetentionPolicy(),
     * which returns the handle after storing the descriptor properties
     * in the backup catalog. Though id is fully initialized afterwards!
     */
    std::string created;

    /**
     * Rules recognized by this retention policy.
     */
    std::vector<std::shared_ptr<RetentionRuleDescr>> rules;
  };

  /**
   * A retention rule descriptor, usually encodes
   * a retention rule.
   */
  class RetentionRuleDescr : public PushableCols {
  public:

    int id = -1;
    RetentionRuleId type = RETENTION_NO_RULE;
    std::string value = "";

  };

  /**
   * Define WAL cleanup modes.
   */
  typedef enum {

    WAL_CLEANUP_RANGE,
    WAL_CLEANUP_OFFSET,
    NO_WAL_TO_DELETE

  } WALCleanupMode;

  /**
   * Cleanup basebackup list mode.
   *
   * Can be either BACKUP_KEEP or BACKUP_DELETE
   */
  typedef enum {

    BASEBACKUP_KEEP,
    BASEBACKUP_DELETE

  } BasebackupCleanupMode;

  /**
   * A BackupCleanupDescr descriptor instance describes
   * which basebackups and WAL segment ranges can be evicted
   * from the archive. It carries a list of basebackup descriptors
   * which is identifying the basebackups to delete or to keep.
   *
   * The newest basebackup is the first in the vector, the older one
   * is the last. The cleanup descriptor also maintains a XLogRecPtr
   * offset or range, depending on the deletion mode specified in
   * the property mode.
   *
   * This identifies the starting (or ending) location of WAL segments which are
   * safe to delete from the archive. Please note that this XLogRecPtr doesn't
   * necessarily belong to the list of basebackups currently elected
   * for eviction from the archive, but might have been influenced
   * by a basebackup to keep or which was pinned before.
   *
   */
  class BackupCleanupDescr {
  public:

    std::vector<std::shared_ptr<BaseBackupDescr>> basebackups;
    BasebackupCleanupMode basebackupMode = BASEBACKUP_KEEP;

    XLogRecPtr wal_cleanup_start_pos = InvalidXLogRecPtr;
    XLogRecPtr wal_cleanup_end_pos = InvalidXLogRecPtr;

    WALCleanupMode mode = NO_WAL_TO_DELETE;

  };
}

#endif
