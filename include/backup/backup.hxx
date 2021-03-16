#ifndef __BACKUP_HXX__
#define __BACKUP_HXX__

#include <descr.hxx>
#include <BackupCatalog.hxx>
#include <fs-archive.hxx>
#include <xlogdefs.hxx>

namespace pgbckctl {

  class BackupFile;
  class BackupDirectory;
  class ArchiveLogDirectory;

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
    virtual std::string backupDirectoryString() = 0;
  };

  /*
   * Represents a list entry of pending
   * transaction log segments in TransactionLogBackup.
   */
  class TransactionLogListItem {
  public:
    std::shared_ptr<BackupFile> fileHandle;
    std::string filename;
    bool sync_pending = false;
    bool flush_pending = false;
  };

  /*
   * Transaction log segment backup object representation.
   *
   * TransactionLogBackup encapsulates access between backup
   * (WAL streaming) operation and, backup catalog and
   * filesystem access.
   */
  class TransactionLogBackup : public Backup {
  private:

    /**
     * Directory handler for wal backup directory.
     */
    std::shared_ptr<ArchiveLogDirectory> logDirectory = nullptr;

    /**
     * Internal stack of allocated and pending
     * transaction log segments. This allows to
     * stack actions on transaction log segments until
     * finalize() is called (see methods sync_pending() and
     * flush_pending() for details).
     */
    std::vector<std::shared_ptr<TransactionLogListItem>> fileList;

    /**
     * Cashed size of WAL segment files. Someone
     * isn't allowed to call initialize() on a TransactionLogBackup
     * handle until this value is initialized correctly.
     */
    uint32_t wal_segment_size = 0;

    /**
     * Number of WAL files synced. This effectively counts the number
     * of WAL files synced into the transaction log archive during
     * the lifetime of a TransactionLogBackup instance.
     */
    uint64_t wal_synced = 0;

  public:
    TransactionLogBackup(const std::shared_ptr<CatalogDescr> & descr);
    virtual ~TransactionLogBackup();

    virtual bool isInitialized();

    /**
     * Initializes a transaction log backup object
     * for file operations. This is required to start
     * any operations on WAL segment files maintained by
     * a transaction log backup handler.
     */
    virtual void initialize();

    /**
     * Should be called if the caller wants to have
     * the archive directory structure a transaction log
     * backup belongs to created.
     */
    virtual void create();

    /**
     * Sync and flush all pending file operations to disk.
     *
     * NOTE:
     *
     * After calling finalize() you don't need to call initialize() again, in
     * this case the transaction log backup handle remains fully initialized.
     * finalize() just flushes and sync all open and stacked file operations
     * and cleans the pending operations list. Effectively, calling
     * initialize() again after finalize() is a no-op here.
     */
    virtual void finalize();
    virtual std::string backupDirectoryString();

    /**
     * Generate a new filename for a WAL segment based
     * on the specified WAL location.
     */
    std::string walfilename(unsigned int timeline,
                            XLogRecPtr position);

    /**
     * Stack a new file into the transaction log backup handler.
     */
    virtual std::shared_ptr<BackupFile> stackFile(std::string name);

    /**
     * Write a XLOG data message into transaction log backup.
     *
     * Returns the XLOG position it has written the message up to.
     * In case the write() failed, an InvalidXLogRecPtr is returned.
     *
     * If the transaction log backup stream needs to allocate
     * a new XLOG segment file, the current XLOG segment file is
     * flushed, renamed into its final backup segment filename and the
     * flush position saved in the called-by-reference
     * argument flush_position. If no switch has occured, flush_position
     * is set to InvalidXLogRecPtr. This can be used to check whether
     * a new segment was created during write().
     */
    virtual XLogRecPtr write(XLOGDataStreamMessage *message,
                             XLogRecPtr &flush_position,
                             unsigned int timeline);

    virtual void sync_pending();
    virtual void flush_pending();

    /**
     * Returns the current allocated WAL segment file. A nullptr
     * is returned in case nothing is allocated at the moment.
     */
    virtual std::shared_ptr<BackupFile> current_segment_file();

    /**
     * Finalizes the current transaction log segment file. We need
     * to work harder here, since it's renamed from its partial
     * suffix into its final name.
     *
     * If forceWalSegSz is set to true, the method will check
     * if the current WAL file has its current seek location
     * positioned at the end of the configured WAL segment size. If
     * that condition is not met, an error will be thrown.
     */
    virtual void finalizeCurrentWALFile(bool forceWalSegSz);

    /**
     * Sets the expected size of WAL segment files. This is required
     * before calling initialize(), and after that the caller isn't allowed
     * to change this value anymore.
     *
     * NOTE: After calling finalize() you might be able to change
     *       this value again, but it's unwise to change
     *       that for an existing transaction log backup to a different
     *       value before (well, this isn't easily doable within
     *       an existing PostgreSQL PGDATA anyways ...).
     *
     * IMPORTANT:
     *
     * setWalSegmentSize() here doesn't do any validation on the
     * passed size, instead you should use PGStream::getWalSegmentSize()
     * to set the size properly. Starting with PostgreSQL 11 you can't
     * rely on any hard wired sizes here, since the source instance
     * might be initdb'ed with a user defined size, so you need
     * to get the correct size from there anyways.
     */
    virtual void setWalSegmentSize(uint32_t wal_segment_size);

    /**
     * Returns the number of WAL synced by a TransactionLogBackup
     * instance.
     */
    virtual uint64_t countSynced();
  };

  typedef enum {

                SB_NOT_SET,
                SB_WRITE,
                SB_READ

  } StreamDirectoryOperationMode;

  /*
   * Streamed basebackup object representation.
   *
   * This class shouldn't be instantiated directly, but within
   * BaseBackupProcess() and related objects to encapsulate filesystem access
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
   * base backups create a new object instance instead. An StreamBaseBackup is either
   * read- or writable, but not both at the same time.
   */
  class StreamBaseBackup: public Backup {
  private:

    /* Read or write operation mode */
    StreamDirectoryOperationMode mode = SB_NOT_SET;

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
    StreamBaseBackup(const std::shared_ptr<CatalogDescr>& descr,
                     StreamDirectoryOperationMode mode);
    ~StreamBaseBackup();

    virtual bool isInitialized();
    virtual void initialize();
    virtual std::shared_ptr<BackupFile> stackFile(std::string name);
    virtual void finalize();
    virtual void setCompression(BackupProfileCompressType compression);
    virtual BackupProfileCompressType getCompression();
    virtual void create();
    virtual std::string backupDirectoryString();
    virtual void setMode(StreamDirectoryOperationMode mode);
    virtual StreamDirectoryOperationMode getMode();

    /**
     * Read file information from the stream backup directory.
     * If nothing left, an empty string is returned, indicating
     * end of file list.
     */
    virtual std::string read();
  };

}

#endif
