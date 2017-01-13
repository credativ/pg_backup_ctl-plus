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

    /*
     * Set open mode for this file. The default is "r"
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

    /*
     * Set open mode for this file. The default is "r"
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
    path handle;

    /*
     * Subdirectory handles.
     */
    path base;
    path log;
  public:

    BackupDirectory(path handle);
    ~BackupDirectory();

    /*
     * Check if this is an existing directory.
     */
    virtual void verify();

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
     * Fsync the directory reference by this
     * object instance.
     */
    virtual void fsync();
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
    CPGBackupCtlFS(string archiveDir) throw(CArchiveIssue);
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
     * Returns the configure archive directory
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
    virtual bool checkArchiveDirectory() throw(CArchiveIssue);

    /*
     * Read the backup history from all backup files in /log.
     *
     * Returns the number of backups found.
     */
    virtual int readBackupHistory() throw(CArchiveIssue);

    /*
     * A static factory method to create a BackupFile instance.
     *
     * NOTE: Nothing will be physically created so far until
     *       the caller starts to call the necessary methods of
     *       the returned BackupFile instance!
     */
    static std::shared_ptr<BackupFile> getBackupFile(path file,
                                                     bool compressed);

  };

}

#endif
