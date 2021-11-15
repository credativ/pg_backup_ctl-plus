#ifndef __HAVE_CATALOGDESCR__
#define __HAVE_CATALOGDESCR__

#include <common.hxx>
#include <rtconfig.hxx>
#include <unordered_set>
#include <queue>
#include <boost/tokenizer.hpp>

/* special descriptors */
#include <recoverydescr.hxx>

namespace pgbckctl {

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
  class RestoreDescr;

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
    START_RECOVERY_STREAM_FOR_ARCHIVE,
    STOP_STREAMING_FOR_ARCHIVE,
    SHOW_WORKERS,
    BACKGROUND_WORKER_COMMAND,
    CREATE_RETENTION_POLICY,
    LIST_RETENTION_POLICIES,
    LIST_RETENTION_POLICY,
    DROP_RETENTION_POLICY,
    APPLY_RETENTION_POLICY,
    SHOW_VARIABLES,
    SHOW_VARIABLE,
    SET_VARIABLE,
    RESET_VARIABLE,
    DROP_BASEBACKUP,
    RESTORE_BACKUP,
    STAT_ARCHIVE_BASEBACKUP
  } CatalogTag;

  /**
   * Compression types supported for backup profiles.
   */
  typedef enum {

    BACKUP_COMPRESS_TYPE_NONE = 0,
    BACKUP_COMPRESS_TYPE_GZIP = 1,
    BACKUP_COMPRESS_TYPE_ZSTD = 2,
    BACKUP_COMPRESS_TYPE_XZ   = 3,
    BACKUP_COMPRESS_TYPE_PLAIN = 4

  } BackupProfileCompressType;

  /**
   * Replication slot states.
   */
  typedef enum {

    REPLICATION_SLOT_OK,
    REPLICATION_SLOT_EXISTS,
    REPLICATION_SLOT_ERROR

  } ReplicationSlotStatus;

  /**
   * Output format identifier.
   */
  typedef enum {
                OUTPUT_CONSOLE,
                OUTPUT_JSON
  } OutputFormatType;

  /**
   * Retention Parser States.
   *
   * When parsing various retention policy commands, we need
   * to remember the states during parsing the commands to
   * build the final RetentionRuleId.
   *
   * The following are specific retention types used during
   * parser states. They don't describe a materialized retention
   * policy with its rule(s), but describe whether a DROP or KEEP
   * action was specified in e.g. the CREATE RETENTION POLICY
   * command and more.
   *
   * They don't have any representation in the backup catalog and
   * must not be used there!
   */
  typedef enum {

    RETENTION_NO_ACTION,
    RETENTION_ACTION_DROP,
    RETENTION_ACTION_KEEP

  } RetentionParsedAction;

  typedef enum {

    RETENTION_NO_MODIFIER,
    RETENTION_MODIFIER_NEWER_DATETIME,
    RETENTION_MODIFIER_OLDER_DATETIME,
    RETENTION_MODIFIER_LABEL,
    RETENTION_MODIFIER_NUM,
    RETENTION_MODIFIER_CLEANUP

  } RetentionParsedModifier;

  typedef struct {

    RetentionParsedAction action = RETENTION_NO_ACTION;
    RetentionParsedModifier modifier = RETENTION_NO_MODIFIER;

    /*
     * Interval modifiers like DAYS, MINUTES already
     * parsed...
     *
     * Primarily used to lookup already parsed modifiers
     * for error handling.
     */
    std::unordered_set<std::string> parsed_intv_mods;

  } RetentionParserState;

  /**
   * A retention rule id classifies the
   * various supported retention rules and their
   * actions.
   */
  typedef enum {

    RETENTION_NO_RULE = 0, /* unknown/undefined rule type */

    RETENTION_KEEP_WITH_LABEL = 200,
    RETENTION_DROP_WITH_LABEL = 201,

    RETENTION_KEEP_NUM = 300,
    RETENTION_DROP_NUM = 301,

    RETENTION_KEEP_NEWER_BY_DATETIME = 400,
    RETENTION_KEEP_OLDER_BY_DATETIME = 401,
    RETENTION_DROP_NEWER_BY_DATETIME = 402,
    RETENTION_DROP_OLDER_BY_DATETIME = 403,

    /* PIN/UNPIN retention action */
    RETENTION_PIN = 500,
    RETENTION_UNPIN = 600,

    RETENTION_CLEANUP = 700

  } RetentionRuleId;

  struct RetentionIntervalOperand {

    RetentionParsedModifier modifier = RETENTION_NO_MODIFIER;
    std::string token;

    virtual std::string str();
  };

  /**
   * A representation of a retention policy interval expression.
   *
   * A RETENTION_KEEP_BY_DATETIME and RETENTION_DROP_BY_DATETIME
   * retention rule allows to operate with a interval expression in the
   * form of
   *
   * nnn years|nnn months|nnn days|nn hours|nn minutes
   *
   * This is transformed into an internal list, which allows to store
   * specific operands from this expression. For example
   * backup catalog operation need to translate them into catalog
   * specific datetime expressions. In the current format, the operands
   * are SQLite3 compatible and are directly used by BackupCatalog
   * and its retention policy methods.
   */
  class RetentionIntervalDescr {

  public:

    RetentionIntervalDescr();

    /**
     * Initializes the list of operands of a RetentionIntervalDescr
     * with the oprands from the given expression. opr_value will
     * point to the last found operand in expression.
     */
    RetentionIntervalDescr(std::string expression);

    std::vector<RetentionIntervalOperand> opr_list;
    std::string opr_value;

    RetentionIntervalDescr operator+(RetentionIntervalDescr source);
    RetentionIntervalDescr operator+(std::string operand);

    RetentionIntervalDescr operator-(RetentionIntervalDescr source);
    RetentionIntervalDescr operator-(std::string operand);

    void push_add(std::string operand);
    void push_sub(std::string operand);

    /**
     * Returns a string representing the interval expression.
     *
     * compile() returns the string in its catalog representation, which
     * can the be reparsed into a RetentionIntervalOperand later. If
     * you just want to have the plain operand as a string (without
     * modifiers et al), use getOperandsAsString() instead.
     */
    std::string compile();

    /**
     * sqlite3_datetime() formats an interval instance encoded
     * into a call to datetime(), suitable
     * to be passed directly to SQLite3.
     *
     * NOTE: the returned datetime() function call doesn't
     * not encode the final operand values, the caller need to bind
     * them separately!
     */
    virtual std::string sqlite3_datetime();

    /**
     * Gets a string representation for a interval
     * expression, extract its individual tokens and assigns
     * it to the internal operator list.
     *
     * The format is required to be
     *
     * nnn years|nnn months|nnn days|nn hours|nn minutes
     */
    void push(std::string value);

    /**
     * Returns the plain operand string. Suitable for displaying
     * interval values.
     */
    std::string getOperandsAsString();

  };

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
   * Type of ConfigVariable.
   */
  typedef enum {

    VAR_TYPE_BOOL,
    VAR_TYPE_STRING,
    VAR_TYPE_ENUM, /* always a vector of strings */
    VAR_TYPE_INTEGER,
    VAR_TYPE_UNKNOWN

  } ConfigVariableType;

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
  class CatalogDescr : protected CPGBackupCtlBase,
                       public PushableCols,
                       public RuntimeVariableEnvironment {

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

    /**
     * A retention interval expression instance used during parsing a
     * pg_backup_ctl++ retention command (e.g. CREATE RETENTION POLICY).
     */
    std::shared_ptr<RetentionIntervalDescr> interval = nullptr;

    /**
     * A pointer to a RecoveryStreamDescr descriptor instantiated
     * during parsing a START RECOVERY STREAM command.
     */
    std::shared_ptr<RecoveryStreamDescr> recoveryStream = nullptr;

    /**
     * A pointer to a RestoreDescr instantiated during
     * parsing a RESTORE command.
     */
    std::shared_ptr<RestoreDescr> restoreDescr = nullptr;

  public:
    CatalogDescr() { tag = EMPTY_DESCR; };
    virtual ~CatalogDescr();

    CatalogTag tag;
    int id = -1;
    std::string archive_name = "";
    std::string retention_name = "";
    std::string label;
    bool compression = false;
    std::string directory;

    /**
     * Used to parse SET variable = value commands.
     */
    ConfigVariableType var_type = VAR_TYPE_UNKNOWN;
    std::string var_name;
    std::string var_val_str;
    int         var_val_int;
    bool        var_val_bool;

    /**
     * Used during parsing certain commands.
     */
    int basebackup_id = -1;
    bool verbose_output = false;

    /**
     * Used to parse retention policy commands.
     */
    RetentionParserState rps;

    /**
     * Option flag, indicating a SYSTEMID catalog update.
     */
    bool force_systemid_update = false;

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

    /**
     * Set restore descriptor.
     */
    void setRestoreDescr(std::shared_ptr<RestoreDescr> restoreDescr);

    /**
     * Toggle print verbose mode issued to a CatalogDescr
     * object from the parser.
     */
    void setPrintVerbose(bool const& verbose);

    /**
     * Set the basebackup id during parse analysis.
     */
    void setBasebackupID(std::string const& bbid);

    /**
     * Set the FORCE_SYSTEMID_OPTION option.
     */
    void setForceSystemIDUpdate(bool const& force_sysid_update);

    /*
     * Returns command tag as string.
     */
    std::string getCommandTagAsStr();

    void retentionIntervalExprFromParserState(std::string const& expr_value,
                                              std::string const& intv_mod);

    /**
     * makeRecoveryStreamDescr
     *
     * Instantiate and setup an internal recovery stream descriptor.
     * Used within parsing the START RECOVERY STREAM command.
     */
    void makeRecoveryStreamDescr();

    /**
     * getRecoveryStreamDescr().
     *
     * Returns the internal instance of a recovery stream
     * descriptor, if any. Could be a nullptr in case makeRecoveryStreamDescr()
     * wasn't called before.
     */
    std::shared_ptr<RecoveryStreamDescr> getRecoveryStreamDescr();

    /**
     * Attach the portnumber to the internal recovery stream
     * descriptor.
     *
     * NOTE: Will throw in case no recovery stream descriptor
     *       was initialized yet (see makeRecoveryStreamDescr()).
     */
    void setRecoveryStreamPort(std::string const& portnumber);

    /**
     * Returns a shared pointer to the internal
     * recovery descriptor.
     *
     * NOTE: might return a nullptr reference in case
     *       no restore descriptor was already created.
     *
     * A restore descriptor is usually created by parsing
     * a RESTORE command (by calling createRestoreDescr()).
     */
    std::shared_ptr<RestoreDescr> getRestoreDescr();

    /**
     * Assign a new internal restore descriptor by identifier.
     * bbident is a string identifying the basebackup by name within
     * a specified archive (see setIdent() for the archive identifier).
     */
    void createRestoreDescrByBaseBackupName(std::string const& bbident);

    /**
     * Assign a new internal restore descriptor by basebackup ID.
     * bbid is the id in its string representation and is converted
     * accordingly into an integer internally. This API is suitable for
     * PGBackupCtlParser, which currently works with parsed string literals
     * only.
     */
    void createRestoreDescrByBaseBackupID(std::string const& bbid);

    /**
     * Prepares a tablespace oid for a formerly created restore
     * descriptor. This method is primarily intended for use by
     * the PGBackupCtlParser.
     *
     * The method creates and prepares a BackupTablespaceDescr
     * instance for adding it to a tablespace map to
     * the formerly created restore descriptor. It just
     * prepared, but not finally added. This happens after
     * calling restoreTablespaceLocationFromParserState(), which
     * finally adds the tablespace OID and location together by
     * saving them in the tablespace descriptor in the
     * restore descriptors hash map.
     *
     * Thus, the calling sequence is just as the parser
     * scans a RESTORE BASEBACKUP command:
     *
     * createRestoreDescrByBaseBackup{Name|ID}()
     * `- restoreTablespaceOidFromParserState()
     *   |    `- restoreTablespaceLocationFromParserState()
     *    - -´
     *
     * createRestoreDescrByBaseBackupName() or createRestoreDescrByBaseBackupID()
     * are called just once during parsing, after that multiple
     * calls to restoreTablespaceOidFromParserState() and
     * restoreTablespaceLocationFromParserState() are allowed.
     *
     * Throws in case no valid restore descriptor is
     * attached to a CatalogDescr instance.
     */
    void restoreTablespaceOidFromParserState(std::string const& oid);

    /**
     * Stacks the current tablespace descr stored in the context
     * of a valid restoreDescr into its tablespace_map after having
     * attached the location accordingly.
     *
     * If unique_check is set to true, then the specified location is checked
     * whether is already assigned to a present OID. The method will throw
     * a CCatalogIssue then.
     */
    void restoreTablespaceLocationFromParserState(std::string const& location,
                                                  bool const& unique_check);

    /**
     * Prepares the specified tablespace location as a restore target
     * directory for *all* tablespaces in the backup.
     */
    void restoreTablespaceAllFromParserState(std::string const& location);

    /**
     * addRecoveryStreamAddress() stores the specified
     * hostname or ip in the recovery descriptor currently
     * present in a CatalogDescr instance. If makeRecoveryStreamDescr()
     * wasn't called before, this will throw.
     */
    void setRecoveryStreamAddr(std::string const& address);

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
     * Sets the current retention action parser state.
     */
    void setRetentionAction(RetentionParsedAction const &action);

    /**
     * Sets the current retention action modifier parser state.
     */
    void setRetentionActionModifier(RetentionParsedModifier const &modifier);

    /**
     * Creates a retention policy rule, attached
     * to a catalog retention descriptor instance.
     * It's required that the caller has instantiated a
     * retention policy with makeRetentionDescr() before.
     */
    void makeRetentionRule(std::string const& value);

    /**
     * Create a new internal retention policy
     * descriptor, without a rule. If a retention
     * policy descriptor already exists, this is effectively
     * a noop. Use detachRetentionDescr() then before, if you
     * want to have a new clean retention descriptor (or
     * clean the instance manually).
     */
    void makeRetentionDescr(RetentionRuleId const &ruleid);

    /**
     * Create a new internal retention policy
     * descriptor, with a rule value attached. If a retention
     * policy descriptor already exists, this is effectively
     * a noop. The rule is attached to the end of the rule list
     * of the current retention policy descriptor.
     */
    void makeRetentionRule(RetentionRuleId const &ruleid,
                           std::string const &value);
    /**
     * Makes a retention rule based on the current
     * parser states in this->rps.
     *
     * value is explicitely *NOT* checked for an empty
     * string.
     *
     * If there's no retention policy descriptor created yet,
     * it will be instantiated. The rule is added at the
     * end of the rule list attached to the current descriptor.
     */
    void makeRuleFromParserState(std::string const &value);

    /**
     * Detaches the internal retention policy from
     * a catalog descriptor. Please note that the internal
     * shared pointer is just set to NULL, so external references
     * might hold the instance still valid.
     */
    void detachRetentionDescr();

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
    void setVariableName(std::string const& var_name);
    void setVariableValueString(std::string const& var_value);
    void setVariableValueInteger(std::string const& var_value);
    void setVariableValueBool(bool const& var_value);

    void setExecString(std::string const& execStr);

    void setDbName(std::string const& db_name);

    void setCommandTag(pgbckctl::CatalogTag const& tag);

    void setIdent(std::string const& ident);

    void setRetentionName(std::string const &ident);

    void setHostname(std::string const& hostname);

    void setUsername(std::string const& username);

    void setPort(std::string const& portNumber);

    void setDirectory(std::string const& directory);

    void setProfileManifest(bool const& manifest);

    void setProfileManifestChecksumsIdent(std::string const& manifest_checksum_ident);

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

    OutputFormatType getOutputFormat();

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
    bool manifest           = false;
    std::string manifest_checksums = "CRC32C";

    static BackupProfileCompressType compressionType(std::string type);
    static std::string compressionType(BackupProfileCompressType type);

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
    unsigned long long wal_segment_size = 0;
    int used_profile = -1;
    int pg_version_num;

    /**
     * Static const specifiers for status flags.
     */
    static constexpr const char *BASEBACKUP_STATUS_IN_PROGRESS = "in progress";
    static constexpr const char *BASEBACKUP_STATUS_ABORTED = "aborted";
    static constexpr const char *BASEBACKUP_STATUS_READY   = "ready";

    /**
     * Runtime settings without catalog representation.
     */
    bool elected_for_deletion = false;

    /**
     * Set to TRUE in case this basebackup
     * exceeds a retention policy (see
     * BackupCatalog::getBackupList() and friends for details).
     */
    bool exceeds_retention_rule = false;

    /* List of tablespaces descriptors in backup */
    std::vector<std::shared_ptr<BackupTablespaceDescr>> tablespaces;

    /************* computed columns by SQL *************/

    /** Duration of the basebackup*/
    std::string duration = "N/A";

  };

  /*
   * Provides stat data for the archive itself.
   */
  class StatCatalogArchive {
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
   * Basebackup retrieval modes for BackupCatalog::getBaseBackup().
   */
  typedef enum {
		BASEBACKUP_NEWEST,
		BASEBACKUP_OLDEST
  } BaseBackupRetrieveMode;

  /**
   * Backup process error flags during initialization.
   *
   * Used to indicate various error conditions.
   */
  typedef enum {

                BASEBACKUP_CATALOG_OK,
                BASEBACKUP_CATALOG_INVALID_SYSTEMID,
                BASEBACKUP_CATALOG_FORCE_SYSTEMID_UPDATE,
                ARCHIVE_OK,
                ARCHIVE_INVALID_XLOG_EXISTS

  } BackupCatalogErrorCode;

  /**
   * Restore descriptor basebackup identification
   * mode.
   */
  typedef enum {

    RESTORE_BASEBACKUP_BY_ID,    /* restore a basebackup identified by its ID */
    RESTORE_BASEBACKUP_BY_NAME,  /* restore a basebackup identified by its name */
    RESTORE_BASEBACKUP_BY_UNDEF  /* descriptor not defined yet */

  } RestoreDescrIdentificationType;


  union _restoredescr_ident {

    int id;
    std::string name;

    _restoredescr_ident() {};
    ~_restoredescr_ident() {};

  };

  /**
   * Restore Descr Basebackup Identifier.
   *
   * For implementation details, visit src/recovery/restore.cxx
   */
  class RestoreDescrID {
  private:
    _restoredescr_ident ident;
  public:

    static constexpr const char *DESCR_NAME_CURRENT = "CURRENT";
    static constexpr const char *DESCR_NAME_NEWEST = "NEWEST";
    static constexpr const char *DESCR_NAME_LATEST = "LATEST";
    static constexpr const char *DESCR_NAME_OLDEST = "OLDEST";

    RestoreDescrIdentificationType type = RESTORE_BASEBACKUP_BY_UNDEF;

    void getId(int &id);
    void getId(std::string &name);

    void setId(RestoreDescrIdentificationType type,
               std::string const& name);

    void setId(RestoreDescrIdentificationType type,
               int const& id);

    static BaseBackupRetrieveMode basebackupRetrieveMode(std::string const& name);

  };

  /**
   * Tablespace Mapping Modes.
   */
  typedef enum {

    TABLESPACE_MAP_ALL,
    TABLESPACE_MAP_OID

  } TablespaceMappingMode;

  /**
   * A Basebackup restore descriptor.
   *
   * Holds all necessary information to restore a
   * specific basebackup.
   *
   * For implementation details, visit src/recovery/restore.cxx
   */
  class RestoreDescr {
  private:

    /**
     * A tablespace descriptor, initialized by makeTablespaceDescr().
     *
     * This facility supports usage of RestoreDescr during
     * parsing, e.g. the RESTORE BASEBACKUP command. The parser
     * first calls prepareTablespaceDescrForMap(oid), and finalizes this phase
     * via stackTablespaceDescrIntoMap(location), which uses the last created
     * descriptor here to place it into the public tablespace_map.
     */
    std::shared_ptr<BackupTablespaceDescr> curr_tablespace_descr = nullptr;

  public:

    RestoreDescr();
    RestoreDescr(std::string bbname);
    RestoreDescr(int id);
    virtual ~RestoreDescr();

    /* Descriptor basebackup identification */
    RestoreDescrID id;

    std::shared_ptr<BaseBackupDescr> basebackup = nullptr;
    std::shared_ptr<BackupProfileDescr> profile = nullptr;

    /**
     * Tablespace Mapping Mode.
     *
     * Set to TABLESPACE_MAP_ALL in case all tablespaces
     * should be mapped onto an alternate directory.
     *
     * Set to TABLESPACE_MAP_OID (the default) in case
     * specific tablespace OIDs are remapped.
     *
     * The latter adds specific OID=DIRECTORY key/value
     * to the tablespace_map hash table.
     */
    TablespaceMappingMode tblspc_map_mode = TABLESPACE_MAP_OID;

    /**
     * maketablespaceDescr() prepares a pointer to a
     * BackupTablespaceDescr instance, but doesn't place it
     * into the tablespace_map yet. To finalize this, the caller
     * needs to call stackTablespaceDescrIntoMap with the intended location
     * of the tablespace prepared earlier.
     *
     * If the specified tablespace OID has already a correspoding member
     * in the internal tablespace_map or is already prepared with the same OID,
     * this method will throw.
     */
    void prepareTablespaceDescrForMap(unsigned int oid);

    /**
     * Returns a pointer to the currently prepared tablespace for
     * the tablespace map. Only valid if prepareTablespaceDescrForMap()
     * was called before!
     */
    std::shared_ptr<BackupTablespaceDescr> getPreparedTablespaceDescrForMap();

    /**
     * If prepareTablespaceDescrForMap() was called earlier, a caller
     * can emplace a formerly prepared tablespace descriptor with
     * the specified tablespace location into the public tablespace
     * map.
     *
     * If prepareTablespaceDescrForMap() wasn't called before, this
     * method will throw.
     */
    void stackTablespaceDescrForMap(std::string location);

    /**
     * List of tablespace locations to restore. This map
     * usually used to generate a tablespace_map file during
     * restore.
     */
    std::map<unsigned int, std::shared_ptr<BackupTablespaceDescr>> tablespace_map;

  };

}

#endif
