#ifndef __HAVE_CATALOGDESCR__
#define __HAVE_CATALOGDESCR__

#include <common.hxx>

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
    START_BASEBACKUP,
    LIST_ARCHIVE
  } CatalogTag;

  /*
   * A catalog descriptor is a reference
   * into the catalog database, representing an interface
   * between CPGBackupCtlFS and BackupCatalog objects.
   */
  class CatalogDescr : protected CPGBackupCtlBase {
  protected:
    std::vector<int> affectedAttributes;

    virtual void pushAffectedAttribute(int colId);
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

    void setAffectedAttributes(std::vector<int> affectedAttributes);
    std::vector<int> getAffectedAttributes();

    CatalogDescr& operator=(const CatalogDescr& source);
  };
}

#endif
