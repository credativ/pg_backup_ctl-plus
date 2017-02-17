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

    virtual void create(std::string name) = 0;
  };

  /*
   * Streamed basebackup object representation.
   *
   * This class shouldn't be instantiated directly, but within
   * BaseBackupProcess() objects to encapsulate filesystem access
   *
   * Thus, StreamBaseBackup is the connection between catalog
   * and filesystem basebackup representation during the streaming
   * process.
   *
   * The caller can call create() and stack filesystem representations
   * into the StreamBaseBackup object. The file handles are internally
   * handled and or of type ArchiveFile. Call finalize() afterwards
   * to sync all outstanding filesystem buffers.
   */
  class StreamBaseBackup: public Backup {
  private:
    /*
     * Stack of internal allocated file handles
     * representing this instance of StreamBaseBackup.
     *
     * NOTE: We use shared_ptr here, even though we don't
     *       allow external access to our handles. This is because
     *       the BackupDirectory API just always returns shared_ptr and
     *       we don't want something special here.
     *
     * NOTE: Our ancestor class Backup has an internal handle for
     *       an ArchiveFile which is always the *last* allocated
     *       file handle in a StreamBaseBackup (since each tablespace
     *       can have its own dump file).
     */
    std::vector<std::shared_ptr<BackupFile>> fileList;
  public:
    StreamBaseBackup(const std::shared_ptr<CatalogDescr>& descr);
    ~StreamBaseBackup();

    virtual void create(std::string name);
    virtual void finalize();
    virtual void setCompression(BackupProfileCompressType compression);
    virtual BackupProfileCompressType getCompression();
  };

}

#endif
