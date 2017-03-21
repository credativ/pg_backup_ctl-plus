#ifndef __BACKUP_HXX__
#define __BACKUP_HXX__

#include <descr.hxx>
#include <BackupCatalog.hxx>
#include <fs-archive.hxx>

namespace credativ {

  class BackupFile;
  class BackupDirectory;

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

    bool initialized = false;

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
     * Backup Directory, instantiated during initialize()...
     */
    BackupDirectory *directory = nullptr;
  public:
    Backup(const std::shared_ptr<CatalogDescr>& descr);
    virtual ~Backup();

    virtual bool isInitialized() = 0;
    virtual void initialize() = 0;
    virtual void create() = 0;
    virtual void finalize() = 0;
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
   *
   * StreamBaseBackup objects are not designed to be reused. For new streamed
   * base backups create a new object instance instead.
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

    /*
     * Internal stream backup identifier. This is also
     * the directory name where all files are stored. The identifer
     * is created during the c'tor via createMyIdentifier().
     */
    std::string identifier = "";

    /*
     * On instantiation, StreamBaseBackup creates an internal
     * name in the format streambackup-<TIMESTAMP>, which represents
     * the directory, where all tarballs from the stream are stored.
     */
    std::string createMyIdentifier();
  public:
    StreamBaseBackup(const std::shared_ptr<CatalogDescr>& descr);
    ~StreamBaseBackup();

    virtual bool isInitialized();
    virtual void initialize();
    virtual std::shared_ptr<BackupFile> stackFile(std::string name);
    virtual void finalize();
    virtual void setCompression(BackupProfileCompressType compression);
    virtual BackupProfileCompressType getCompression();
    virtual void create();
  };

}

#endif
