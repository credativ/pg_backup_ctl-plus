#ifndef __HAVE_CATALOGDESCR__
#define __HAVE_CATALOGDESCR__

#include <common.hxx>

namespace credativ {

  /*
   * Forwarded declarations
   */
  class BackupProfileDescr;
  class BackupTableSpaceDescr;

  /*
   * Defines flags to characterize the
   * action defined by a catalog descriptor.
   */
  typedef enum {
    EMPTY_DESCR = -1,
    CREATE_ARCHIVE,
    CREATE_BACKUP_PROFILE,
    DROP_ARCHIVE,
    ALTER_ARCHIVE,
    VERIFY_ARCHIVE,
    START_BASEBACKUP,
    LIST_ARCHIVE
  } CatalogTag;

  /*
   * Compression types supported for backup profiles.
   */
  typedef enum {

    BACKUP_COMPRESS_TYPE_NONE = 0,
    BACKUP_COMPRESS_TYPE_GZIP = 1

  } BackupProfileCompressType;

  /*
   * Base class for descriptors which wants
   * to have dynamic cols associated.
   */
  class PushableCols {
  protected:
    std::vector<int> affectedAttributes;
    virtual void pushAffectedAttribute(int colId);
  public:
    virtual std::vector<int> getAffectedAttributes();
    virtual void setAffectedAttributes(std::vector<int> affectedAttributes);
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
    int archive;
  };
}

#endif
