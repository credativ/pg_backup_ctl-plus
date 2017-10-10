#ifndef __FS_ARCHIVE__
#define __FS_ARCHIVE__
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <string>
#include <unordered_map>

#include <common.hxx>
#include <BackupCatalog.hxx>

#ifdef PG_BACKUP_CTL_HAS_ZLIB
/*
 * must stay after common.hxx, since cmake defines
 * availability of zlib there.
 */
#include <zlib.h>
#endif

#ifdef PG_BACKUP_CTL_HAS_ZSTD
/*
 * must stay after common.hxx, since cmake defines
 * availability of zlib there.
 */
#include <zstd.h>
#endif

using namespace credativ;
using namespace std;
using namespace boost::filesystem;
using namespace boost::posix_time;
using namespace boost::iostreams;

namespace credativ {

  typedef struct XLOGLocation {
    string startXLOG;
    string stopXLOG;
    unsigned int hi;
    unsigned int lo;
    bool segment_avail;
  } XLOGLocation;

  /*
   * Base archive exception.
   */
  class CArchiveIssue : public CPGBackupCtlFailure {
  public:
    CArchiveIssue(const char *errstr) throw() : CPGBackupCtlFailure(errstr) {};
    CArchiveIssue(std::string errstr) throw() : CPGBackupCtlFailure(errstr) {};
  };

  /*
   * Base class for archive files
   */
  class BackupFile : public CPGBackupCtlBase {

  protected:

    bool compressed = false;
    bool available = false;

    /* boost::filesystem handle */
    path   handle;

  public:

    BackupFile(path handle);
    virtual ~BackupFile();

    virtual void setAvailable(bool avail);
    virtual void setCompressed(bool compressed);

    virtual bool isCompressed() { return this->compressed; };
    virtual bool isAvailable() { return this->available; };

    virtual void open() = 0;
    virtual void close() = 0;
    virtual void fsync() = 0;
    virtual bool isOpen() = 0;
    virtual bool exists();
    virtual void rename(path& newname) = 0;
    virtual void setOpenMode(std::string mode) = 0;
    virtual size_t write(const char *buf, size_t len) = 0;
    virtual size_t read(char *buf, size_t len) = 0;
    virtual void remove() = 0;

  };

  class ArchiveFile : public BackupFile {
  private:
    FILE  *fp = NULL;

    /*
     * Sets the mode the file is opened. The default
     * is binary read only.
     */
    std::string mode = "rb";

    bool opened = false;
  public:

    ArchiveFile(path pathHandle);
    ~ArchiveFile();

    virtual bool isCompressed();
    virtual void setCompressed(bool compressed);
    virtual bool isOpen();

    /*
     * Opens the file.
     */
    virtual void open();

    virtual size_t write(const char *buf, size_t len);
    virtual size_t read(char *buf, size_t len);
    virtual void rename(path& newname);
    virtual void fsync();
    virtual void close();

    virtual void remove();

    /*
     * Set open mode for this file. The default is "rb".
     */
    virtual void setOpenMode(std::string mode);

    /*
     * Returns the internal file stream pointer.
     */
    virtual FILE* getFileHandle();

    /*
     * Returns the internal filedescriptor referenced by
     * this object.
     */
    virtual int getFileno();

  };

#ifdef PG_BACKUP_CTL_HAS_ZSTD

  /*
   * Implementation for archive files compressed
   * with ZSTD library.
   *
   * NOTE: The current implementation uses ZStream
   *       API calls and in its current form doesn't
   *       support read and writing compressed files
   *       at the same time! When used this way, the
   *       behavior of ZSTDArchiveFile object instances
   *       are undefined!
   */
  class ZSTDArchiveFile : public BackupFile {
  private:

    /*
     * Internal file descriptor
     */
    FILE *fp = NULL;

    /*
     * ZSTD compression/decompression context.
     */
    ZSTD_DCtx *decompressCtx = NULL;
    ZSTD_CCtx *compressCtx = NULL;

    /*
     * Indicates if the file was already opened
     * successfully.
     */
    bool opened = false;

    /*
     * Sets the mode the file is opened. The default
     * is binary read only.
     */
    std::string mode = "rb";

    /*
     * zstd compression level (1-19), default 10
     */
    int compression_level = 10;

  public:
    ZSTDArchiveFile(path pathHandle);
    ~ZSTDArchiveFile();

    virtual bool isCompressed();
    virtual void setCompressed(bool compressed);
    virtual bool isOpen();

    /*
     * Set open mode for this file. The default is "rb".
     */
    virtual void setOpenMode(std::string mode);

    /*
     * Opens a file for ZSTD compression/decompression.
     */
    virtual void open();
    virtual void close();
    virtual size_t write(const char *buf, size_t len);
    virtual size_t read(char *buf, size_t len);
    virtual void fsync();
    virtual FILE *getFileHandle();
    virtual void remove();
    virtual void rename(path& newname);

    /*
     * Extended methods.
     */
    virtual void setCompressionLevel(int level);
  };

#endif

#ifdef PG_BACKUP_CTL_HAS_ZLIB

  class CompressedArchiveFile : public BackupFile {
  private:
    gzFile zh = NULL;
    FILE  *fp = NULL;

    /*
     * Sets the mode the file is opened. The default
     * is binary read only.
     */
    std::string mode = "rb";

    /*
     * Gzip compression level, default 9
     */
    int compressionLevel = 9;
    bool opened = false;
  public:

    CompressedArchiveFile(path pathHandle);
    ~CompressedArchiveFile();

    virtual bool isCompressed();
    virtual void setCompressed(bool compressed);
    virtual bool isOpen();

    /*
     * Opens a compressed gzip handle.
     */
    virtual void open();
    virtual void close();
    virtual size_t write(const char *buf, size_t len);
    virtual size_t read(char *buf, size_t len);
    virtual void fsync();
    virtual void rename(path& newname);

    virtual void remove();

    /*
     * Set open mode for this file. The default is "rb"
     */
    virtual void setOpenMode(std::string mode);

    /*
     * Returns the internal file stream pointer.
     */
    virtual FILE* getFileHandle();

    virtual gzFile getGZHandle();

    /*
     * Extended methods.
     */
    virtual void setCompressionLevel(int level);
  };

#endif

  /*
   * Base class for archive directories. Also encapsulates
   * the complete archive directory tree with the following layout:
   *
   * this->handle
   *            `- path(log/)
   *            `- path(base/)
   */
  class BackupDirectory : public CPGBackupCtlBase {
  protected:
    /*
     * Internal boost::filesystem handle
     */
    path handle;

    /*
     * Subdirectory handles.
     */
    path base;
    path log;

    /*
     * Fsync a specific path.
     */
    static void fsync(path syncPath);
  public:

    BackupDirectory(path handle);
    ~BackupDirectory();

    /*
     * Check if this is an existing directory.
     */
    virtual void verify();

    /*
     * Instantiate the directory (create physical directories)
     */
    virtual void create();

    /*
     * Returns a copy of the internal base directory
     * path handle.
     */
    virtual path basedir();

    /*
     * Returns a copy of the internal log directory
     * path handle.
     */
    virtual path logdir();

    /*
     * Fsync the directory referenced by this
     * object instance.
     */
    virtual void fsync();

    /*
     * Returns the path handle this object instance
     * points to.
     */
    virtual path getArchiveDir();

    /*
     * Returns a file belonging to this directory.
     */
    virtual std::shared_ptr<BackupFile> walfile(std::string name,
                                                BackupProfileCompressType compression);

    /*
     * Factory method returns a new basebackup file handle.
     *
     * This will create a file handle pointing into
     * the base/ subdirectory of the backup archive.
     *
     * Specialized backup methods which use alternative
     * subdirectory location for their backup files,
     * such as streamed base backups, should be implemented by
     * their own descendant classes.
     *
     * NOTE: Nothing will be physically created so far until
     *       the caller starts to call the necessary methods of
     *       the returned BackupFile instance!
     */
    virtual std::shared_ptr<BackupFile> basebackup(std::string name,
                                                   BackupProfileCompressType compression);
  };

  /*
   * Specialized class for archive log directories.
   *
   * This is a specialized descendant class of BackupDirectory,
   * extending the functionality for wal streaming functions.
   */
  class ArchiveLogDirectory : public BackupDirectory {
  protected:
  public:
    ArchiveLogDirectory(std::shared_ptr<BackupDirectory> parent);
    ArchiveLogDirectory(path parent);

    virtual ~ArchiveLogDirectory();

    /*
     * Returns the path to the archive log segment files.
     */
    virtual path getPath();
  };

  /*
   * Specialized class which encapsulates access
   * to streaming basebackup subdirectories.
   *
   * This is a specialized descendant class of BackupDirectory,
   * which extends its ancestor for specific methods for
   * streaming basebackup filesystem organization.
   */
  class StreamingBaseBackupDirectory : public BackupDirectory {
  protected:
    path streaming_subdir;
  public:
    StreamingBaseBackupDirectory(std::string streaming_dirname,
                                 path archiveDir);
    StreamingBaseBackupDirectory(std::string streaming_dirname,
                                 std::shared_ptr<BackupDirectory> parent);

    virtual ~StreamingBaseBackupDirectory();

    /*
     * Returns the path to the streaming base backup
     * directory.
     */
    virtual path getPath();

    /*
     * Returns a file handle representing the new streamed base
     * backup file content.
     */
    virtual std::shared_ptr<BackupFile> basebackup(std::string name,
                                                   BackupProfileCompressType compression);

    /*
     * Instantiate the directory.
     *
     * This creates a streaming base backup subdirectory in
     * <ARCHIVEDIR>/base/ if not already existing.
     */
    virtual void create();

    /*
     * Fsync directories.
     */
    virtual void fsync();

    /*
     * Remove streaming base backup, including files and directory
     * from the filesystem.
     */
    virtual void remove();
  };

  /*
   * Class which represents a backup history file.
   *
   * This method is somewhat special in the regard that it
   * doesn't fully implement all methods from BackupFile (see
   * comments below). However, since we want to pass all
   * types of ArchiveFiles directly as BackupFile somehow (and
   * C++ doesn't really have a nice interface infrastructure),
   * we derive it from BackupFile anyways. Unsupported methods
   * are throwing a CArchiveIssue exception directly (again, see
   * comments below).
   */
  class BackupHistoryFile : public BackupFile {
  private:
    bool opened = false;
  protected:

    XLOGLocation startLoc;
    XLOGLocation stopLoc;
    XLOGLocation chkPtLoc;

    string backupLabel;
    string backupMethod;
    string backupFrom;

    ptime backupStarted;
    ptime backupStopped;
  public:
    BackupHistoryFile(path handle);
    virtual ~BackupHistoryFile();

    virtual void read();
    virtual string getBackupStartTime();
    virtual string getBackupStopTime();
    virtual void setBackupStopTime(string timeStr);
    virtual void setBackupStartTime(string timeStr);
    virtual string getBackupLabel();
    virtual string getBackupMethod();
    virtual string getBackupFrom();
    virtual string getBackupHistoryFilename();

    /*
     * The following methods are inherited from BackupFile,
     * but are just mapped to convenient methods above.
     * e.g. open() is mapped to read(), close(), fsync(), rename()
     * and write() throw a CArchiveIssue exception when used.
     *
     * isOpen() returns true in case read() was already
     * called on a BackupHistoryFile object instance.
     *
     * Also note that read() cannot be called with
     * an explicit buffer, you have to use the read()
     * method without arguments here.
     */
    virtual void open();
    virtual void close();
    virtual void fsync();
    virtual void setOpenMode(std::string mode);
    virtual size_t write(const char *buf, size_t len);
    virtual size_t read(char *buf, size_t len);
    virtual bool isOpen();
    virtual void rename(path& newname);
    virtual void remove();
  };

  /*
   * Class for filesystem level access of the
   * backup archive.
   */
  class CPGBackupCtlFS : protected CPGBackupCtlBase {

  private:
    string archiveDir;

  protected:

    /*
     * Reference to archive directory path.
     */
    path archivePath;

    /*
     * Reads the prepared archive directory into
     * internal structures.
     */
    void readArchiveDirectory();

  public:
    /*
     * Static factory class for archive directory handles.
     */
    static std::shared_ptr<BackupDirectory> getArchiveDirectoryDescr(std::string directory);

    /*
     * Holds BackupHistoryFile objects read in
     * read by readBackupHistory().
     */
    unordered_map<std::string, shared_ptr<BackupHistoryFile>> history;

    /*
     * C'tor, needs archive directory to be specified
     */
    CPGBackupCtlFS(string archiveDir) ;
    virtual ~CPGBackupCtlFS();

    /*
     * Checks if the specified base backup exists
     * in the archive.
     */
    virtual bool backupExists(string backupName);

    /*
     * Checks if the specified XLOG segments
     * exists in the archive.
     */
    virtual bool XLOGSegmentExists(string xlogFile);

    /*
     * Returns the configured archive directory
     */
    string getArchiveDirectory();

    /*
     * Get a catalog descriptor from the given
     * BackupHistoryFile object
     */
    virtual shared_ptr<CatalogDescr> catalogDescrFromBackupHistoryFile(shared_ptr<BackupHistoryFile> file);

    /*
     * Check archive directory.
     *
     * The checkArchiveDirectory() method performs the following checks:
     *
     * - directory exists
     * - subdirectory base/ exists
     * - subdirectory log/ exists
     */
    virtual bool checkArchiveDirectory();

    /*
     * Read the backup history from all backup files in /log.
     *
     * Returns the number of backups found.
     */
    virtual int readBackupHistory();
  };

}

#endif
