#ifndef __BACKUP_HXX__
#define __BACKUP_HXX__

#include <descr.hxx>
#include <BackupCatalog.hxx>
#include <fs-archive.hxx>

namespace credativ {

  /*
   * Generic base class to implement backup
   * files.
   */
  class Backup : public CPGBackupCtlBase {
  protected:
    /*
     * Catalog descriptor handle, initialized
     * during c'tor.
     */
    std::shared_ptr<CatalogDescr> descr = nullptr;

    /*
     * File handle object representing a physical
     * backup file
     */
    std::shared_ptr<BackupFile> file = nullptr;

    /*
     * File handle is compressed.
     */
    BackupProfileCompressType compression = BACKUP_COMPRESS_TYPE_NONE;

    /*
     * Backup Directory, instantiated during c'tor
     */
    BackupDirectory *directory = nullptr;
  public:
    Backup(const std::shared_ptr<CatalogDescr>& descr);
    virtual ~Backup();

    virtual void create() = 0;
  };

  class StreamBaseBackup: public Backup {
  public:
    StreamBaseBackup(const std::shared_ptr<CatalogDescr>& descr);
    ~StreamBaseBackup();

    virtual void create();
    virtual void setCompression(BackupProfileCompressType compression);
    virtual BackupProfileCompressType getCompression();
  };

}

#endif
