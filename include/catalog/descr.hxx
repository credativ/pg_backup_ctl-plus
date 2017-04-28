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

  /*
   * Defines flags to characterize the
   * action defined by a catalog descriptor.
   */
  typedef enum {
    EMPTY_DESCR = -1,
    CREATE_ARCHIVE,
    CREATE_BACKUP_PROFILE,
    DROP_ARCHIVE,
    DROP_BACKUP_PROFILE,
    ALTER_ARCHIVE,
    VERIFY_ARCHIVE,
    START_BASEBACKUP,
    LIST_ARCHIVE,
    LIST_BACKUP_PROFILE,
    LIST_BACKUP_PROFILE_DETAIL
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
   * Represents an identified streaming connection.
   */
  class StreamIdentification {
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
   * Base class for descriptors which wants
   * to have dynamic cols associated.
   */
  class PushableCols {
  protected:
    std::vector<int> affectedAttributes;
  public:
    virtual void pushAffectedAttribute(int colId);
    virtual std::vector<int> getAffectedAttributes();
    virtual void setAffectedAttributes(std::vector<int> affectedAttributes);
    virtual void clearAffectedAttributes();
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
    std::string pghost = "";
    int    pgport = -1;
    std::string pguser = "";
    std::string pgdatabase = "";

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
  };
}

#endif
