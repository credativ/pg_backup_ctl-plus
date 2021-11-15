#ifndef __FS_ARCHIVE__
#define __FS_ARCHIVE__
#include <boost/filesystem.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/regex.hpp>
#include <string>
#include <unordered_map>

#include <common.hxx>
#include <daemon.hxx>
#include <BackupCatalog.hxx>
#include <backupcleanupdescr.hxx>
#include <memorybuffer.hxx>

#ifdef PG_BACKUP_CTL_HAS_ZLIB
/*
 * must stay after common.hxx, since cmake defines
 * availability of zlib there.
 */
#include <zlib.h>
#endif

using namespace pgbckctl;
using namespace std;
using namespace boost::filesystem;
using namespace boost::posix_time;
using namespace boost::iostreams;

namespace pgbckctl {

  /* Forwarded class definitions */
  class ArchiveLogDirectory;

  /**
   * Encodes XLOG LSN information
   */
  typedef struct XLOGLocation {
    string startXLOG;
    string stopXLOG;
    unsigned int hi;
    unsigned int lo;
    bool segment_avail;
  } XLOGLocation;

  /**
   * Describes types of WAL segment files that
   * can be live within a log/ directory represented
   * by an ArchiveLogDirectory instance.
   */
  typedef enum {
    WAL_SEGMENT_COMPLETE = 1,
    WAL_SEGMENT_PARTIAL,
    WAL_SEGMENT_COMPLETE_COMPRESSED,
    WAL_SEGMENT_PARTIAL_COMPRESSED, /* NOTE: XLOG segments are gzipped only! */
    WAL_SEGMENT_TLI_HISTORY_FILE,
    WAL_SEGMENT_TLI_HISTORY_FILE_COMPRESSED,
    WAL_SEGMENT_INVALID_FILENAME,
    WAL_SEGMENT_UNKNOWN
  } WALSegmentFileStatus;

  /**
   * Verification codes returned by
   * StreamingBaseBackupDirectory::verify().
   */
  typedef enum {

    BASEBACKUP_OK = 100,
    BASEBACKUP_ABORTED,
    BASEBACKUP_IN_PROGRESS,
    BASEBACKUP_START_WAL_MISSING,
    BASEBACKUP_END_WAL_MISSING,
    BASEBACKUP_DIRECTORY_MISSING,
    BASEBACKUP_DESCR_INVALID,
    BASEBACKUP_DIRECTORY_MISMATCH,
    BASEBACKUP_GENERIC_VERIFICATION_FAILURE

  } BaseBackupVerificationCode;

  /**
   * Base archive exception.
   */
  class CArchiveIssue : public CPGBackupCtlFailure {
  public:
    CArchiveIssue(const char *errstr) throw() : CPGBackupCtlFailure(errstr) {};
    CArchiveIssue(std::string errstr) throw() : CPGBackupCtlFailure(errstr) {};
  };

  /**
   * Base class for archive files
   */
  class BackupFile {

  protected:

    bool compressed = false;
    bool available = false;
    bool temporary = false;

    /** boost::filesystem handle */
    path   handle;

    /**
     * Current internal position in file.
     * initialized by read(), lseek() and write().
     */
    off_t currpos = 0;

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
    virtual std::string getOpenMode() = 0;
    virtual size_t write(const char *buf, size_t len) = 0;
    virtual size_t read(char *buf, size_t len) = 0;
    virtual void remove() = 0;
    virtual size_t size();
    virtual off_t lseek(off_t offset, int whence) = 0;
    virtual off_t current_position();
    virtual void setTemporary();
    virtual bool isTemporary();

    /**
     * Returns the filename as a string.
     */
    virtual std::string getFileName();

    /**
     * Returns the full qualified path of the
     * allocated file.
     */
    virtual std::string getFilePath();

  };

  /**
   * Derived from BackupFile, this implements a basic file
   * in the backup archive.
   */
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
    virtual ~ArchiveFile();

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

    /**
     * Returns the mode an ArchiveFile instance was opened with.
     */
    virtual std::string getOpenMode();

    /*
     * Returns the internal file stream pointer.
     */
    virtual FILE* getFileHandle();

    /*
     * Returns the internal filedescriptor referenced by
     * this object.
     */
    virtual int getFileno();

    /*
     * Seek into the file.
     */
    virtual off_t lseek(off_t offset, int whence);

  };

#ifdef PG_BACKUP_CTL_HAS_ZLIB

  /**
   * A compressed archive file, using gzip
   */
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
    //    int compressionLevel = 9;
    bool opened = false;
  public:

    CompressedArchiveFile(path pathHandle);
    virtual ~CompressedArchiveFile();

    virtual bool isCompressed();
    virtual void setCompressed(bool compressed);
    virtual bool isOpen();

    /**
     * Opens a compressed gzip handle.
     */
    virtual void open();
    virtual void close();
    virtual size_t write(const char *buf, size_t len);
    virtual size_t read(char *buf, size_t len);
    virtual void fsync();
    virtual void rename(path& newname);
    virtual off_t lseek(off_t offset, int whence);
    virtual void remove();

    /**
     * Set open mode for this file. The default is "rb"
     */
    virtual void setOpenMode(std::string mode);

    /**
     * Returns the mode this file instance was opened with.
     */
    virtual std::string getOpenMode();

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

  /**
   * Directory tree walker instance
   */
  class DirectoryTreeWalker {
  private:

    /**
     * Internal directory handle to walk down.
     */
    path handle;

    /**
     * Internal iterator handle.
     */
    recursive_directory_iterator dit;

    /**
     * Indicates wether the iterator was already opened.
     */
    bool opened = false;

  public:

    DirectoryTreeWalker(path handle);
    virtual ~DirectoryTreeWalker();

    /**
     * Open the iterator
     */
    virtual void open();

    /**
     * Return path handle from opened iterator and move
     * iterator to next element.
     */
    virtual directory_entry next();

    /**
     * Returns true if the iterator reached the end of.
     * contents.
     */
    virtual bool end();

    /**
     * Returns true if the iterator was already opened.
     */
    bool isOpen();

  };

  /**
   * RootDirectory implements basic functionality for root directory
   * classes. If a directory handle is to be created,
   * these implementations can inherit and get some basic functionaliy
   * like recursive fsync() and more ...
   */
   class RootDirectory {
   protected:

     /*
      * Internal boost::filesystem handle
      */
     path handle;

   public:

     RootDirectory(path handle);
     virtual ~RootDirectory();

     /*
      * Fsync a specific path.
      */
     static void fsync(path syncPath);

     /*
      * Fsync the directory referenced by this
      * object instance.
      */
     virtual void fsync();

     /**
      * Recursively fsync the directory contents and
      * the directory itself.
      *
      * This can also be used to fsync a file.
      *
      * This might be an expensive operation, if the
      * the directory was just created and contains many
      * new or large files.
      */
     static void fsync_recursive(path handle);

     /**
      * Returns the effective path handle this directory
      * currently points to.
      */
     virtual path getPath();

     /**
      * Returns a list of files/directories contained in the streamed base backup.
      *
      * Depending on the type of basebackup (PLAIN) and the size of the database instance,
      * this list can be arbitrary large!
      */
     virtual std::shared_ptr<std::list<directory_entry>> stat();

     /**
      * Returns a directory tree walker instance to traverse
      * filesystem contents of a streamed base backup directory.
      */
     virtual DirectoryTreeWalker walker();

     /**
      * Static version of walker(), can be called to derive a
      * directory tree walker on an arbitrary path handle.
      *
      * Throws in case handle is not a valid directory path.
      */
     static DirectoryTreeWalker walker(path handle);
   };

  /**
   * Base class for archive directories. Also encapsulates
   * the complete archive directory tree with the following layout:
   *
   * this->handle
   *            `- path(log/)
   *            `- path(base/)
   */
  class BackupDirectory : public RootDirectory {
  protected:

    /*
     * Subdirectory handles.
     */
    path base;
    path log;

    /*
     * Verify if a specific path exists in the
     * filesystem.
     *
     * checkPath
     */
    virtual bool checkPathForBackup(path path);

  public:

    BackupDirectory(path handle);
    virtual ~BackupDirectory();

    /**
     * Returns a string describing the specified
     * BaseBackupVerificationCode.
     */
    static std::string verificationCodeAsString(BaseBackupVerificationCode code);

    /**
     * Returns the system temp directory path
     *
     * Throws in case the path doesn't exist already or
     * cannot be determined.
     */
    static path system_temp_directory();

    /**
     * Returns a generated temporary filename.
     *
     * This uses boost::filesystem::unique_path() internally,
     * so the returned path object is a generated filename
     * according to the rules defined there.
     */
    static path temp_filename();

    /**
     * Returns the relative path of dirTo compared to dirFrom. This helper
     * function is implemented in addition to boost's relative_path() method which
     * isn't present in version below 1.60. To still support older boost version
     * we implement our own method here.
     */
    static path relative_path(const path dirFrom, path dirTo);

    /**
     * Fsync the backup directory contents.
     *
     * This recursively fsyncs the log and base directories
     * as well as the contents of the backup directory
     * itself.
     */
    virtual void fsync();

    /**
     * Check if this is an existing archive directory.
     *
     * This method performs additional checks like placing
     * a magic file into the archive directory.
     *
     */
    virtual bool exists();

    /**
     * Instantiate the directory (create physical directories)
     */
    virtual void create();

    /**
     * Returns a copy of the internal base directory
     * path handle.
     */
    virtual path basedir();

    /**
     * Returns a copy of the internal log directory
     * path handle.
     */
    virtual path logdir();

    /**
     * Removes the specified path from the backup directory
     * physically.
     */
    static void unlink_path(path backup_path);

    /**
     * Returns the path handle this object instance
     * points to.
     */
    virtual path getArchiveDir();

    /**
     * Returns a handle to the Archive Log Directory.
     */
    virtual std::shared_ptr<ArchiveLogDirectory> logdirectory();

    /**
     * Returns a WAL segment file belonging to this directory.
     *
     * Internally, nothing special happens. The returned
     * file handle doesn't actually persist on the filesystem until
     * you open()/close() it.
     */
    virtual std::shared_ptr<BackupFile> walfile(std::string name,
                                                BackupProfileCompressType compression);

    /**
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

  /**
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

    /**
     * Returns the path to the archive log segment files.
     */
    virtual path getPath();

    /**
     * Check if this is an existing log directory.
     */
    virtual bool exists();

    /**
     * Calculates an encoded XLOG start position from the *last*
     * XLOG segment file found in the archive directory. Returns
     * an empty string in case no XLOG segments are found.
     *
     * walsegsize should be a valid segment size value, obtained
     * by a PGStream object instance. Note that we don't check
     * the value passed by the caller (but if the caller used PGStream.getWalSegmentSize()
     * the check is actually already performed).
     *
     * The returned XLOG start position starts either by the *end*
     * of the last completed XLOG segment found or at the beginning
     * of the last partial segment.
     */
    virtual std::string getXlogStartPosition(unsigned int &timelineID,
                                             unsigned int &segmentNumber,
                                             unsigned long long xlogsegsize);

    /**
     * Determines the type of the specified XLOG segment file.
     *
     * The specified path handle must be a valid and existing XLOG segment file.
     * Depending on the state of the file, the function returns
     * a WALSegmentFileStatus indicating the status of the segment file.
     *
     * If the specified path handle cannot be resolved to an existing
     * XLOG segment file, the method returns WAL_SEGMENT_UNKNOWN.
     *
     * NOTE: The method doesn't perform any permission checks on the segment
     *       file, this needs to be done by the caller if needed.
     *
     *       If the specified file name is not a valid XLOG segment
     *       filename, WAL_SEGMENT_INVALID_FILENAME is returned.
     *
     */
    virtual WALSegmentFileStatus determineXlogSegmentStatus(path segmentFile);

    /**
     * Gets the previous XLOG segment file for the given
     * XLogRecPtr.
     */
    static std::string XLogPrevFileByRecPtr(XLogRecPtr recptr,
                                            unsigned int timeline,
                                            unsigned long long wal_segment_size);

    /**
     * Gets a XLogRecPtr value, returns the XLOG
     * segment file it belongs to.
     */
    static std::string XLogFileByRecPtr(XLogRecPtr recptr,
                                        unsigned int timeline,
                                        unsigned long long wal_segment_size);

    /**
     * Returns the size of the specified ArchiveLogDirectory
     * file entry. The specified path handle must be a valid
     * existing file in the log/ directory.
     */
    virtual unsigned long long getXlogSegmentSize(path segmentFile,
                                                  unsigned long long xlogsegsize,
                                                  WALSegmentFileStatus status);

    /**
     * Scans through the current contents of the log directory
     * and deletes all files older that the XLogRecPtr offset
     * specified in the BackupCleanDescr structure. The caller should have
     * called identifyDeletionPoints() before doing the phyiscal stuff
     * here to be safe.
     */
    void removeXLogs(std::shared_ptr<BackupCleanupDescr> cleanupDescr,
                     unsigned long long wal_segment_size);

    /**
     * Check specified cleanup descriptor being suitable to perform a
     * XLOG cleanup.
     *
     * If no XLogRecPtr offset or range can be identified, the cleanupDescr mode will
     * be set to NO_WAL_TO_DELETE.
     *
     * If cleanupDescr wasn't initialized with BASEBACKUP_DELETE, an CArchiveIssue
     * will be thrown.
     *
     * For further information, see also the comments in src/catalog/retention.cxx.
     */
    virtual void checkCleanupDescriptor(std::shared_ptr<BackupCleanupDescr> cleanupDescr);

    /**
     * Returns a new, allocated history file handle.
     *
     * The returned file handle is suitable to be written to directly
     * (thus, opened). Writing, closing and syncing the file is left
     * to the caller.
     */
    virtual std::shared_ptr<BackupFile> allocateHistoryFile(int timeline,
                                                            bool compressed);

    /**
     * Opens a timeline history file for reading and returns its contents
     * as a stringstream. If the file doesn't
     * exist or an error occurs, this method will throw.
     *
     * NOTE:
     *
     * The file is opened by the method itself, but *not* closed! This is left
     * to caller.
     */
    virtual void readHistoryFile(int timeline,
                                 bool compressed,
                                 std::stringstream &history_content);

    /**
     * Checks if the specified history file is already allocated
     * in the log directory.
     */
    virtual bool historyFileExists(int timeline,
                                   bool compression);

    /**
     * Returns the timeline history filename for the specified TLI.
     */
    static std::string timelineHistoryFilename(unsigned int tli,
                                               bool compressed);

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
    StreamingBaseBackupDirectory(path streaming_directory);

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

    /**
     * Instantiate the directory.
     *
     * This creates a streaming base backup subdirectory in
     * <ARCHIVEDIR>/base/ if not already existing.
     */
    virtual void create();

    /**
     * Returns the size of an existing streaming basebackup
     * directory.
     *
     * This loops through the directory, summing up all files
     * found there.
     *
     * If the streaming basebackup does not exist, size() will
     * throw.
     */
    virtual size_t size();

    /**
     * Fsync directories.
     */
    virtual void fsync();

    /**
     * Remove streaming base backup, including files and directory
     * from the filesystem.
     */
    virtual void remove();

    /**
     * Verification of content of the specified
     * base backup descriptor
     *
     * The verify_basebackup() method here takes the properties from
     * the fully initialized basebackup descriptor and verifies
     * that it matches the on-disk representation.
     */
    static BaseBackupVerificationCode verify(std::shared_ptr<BaseBackupDescr> bbdescr);

    /**
     * Factory method. Returns a new instance
     * of StreamingBaseBackupDirectory.
     */
    static std::shared_ptr<StreamingBaseBackupDirectory> getInstance(std::string dirname,
                                                                     path archiveDir);
  };

  /**
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
    BackupHistoryFile(path handle, bool readFile);
    virtual ~BackupHistoryFile();

    virtual size_t write_mem(MemoryBuffer &mybuffer);
    virtual size_t read_mem(MemoryBuffer &mybuffer);

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
     * method without arguments here, the same with write().
     *
     * BackupHistoryFile also supports reading and writing
     * to a MemoryBuffer object instance, see above.
     */

    virtual void open();
    virtual void close();
    virtual void fsync();
    virtual void setOpenMode(std::string mode);
    virtual std::string getOpenMode();
    virtual size_t write(const char *buf, size_t len);
    virtual size_t read(char *buf, size_t len);
    virtual bool isOpen();
    virtual void rename(path& newname);
    virtual void remove();
    virtual off_t lseek(off_t offset, int whence);
  };

  /**
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
