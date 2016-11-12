#ifndef __FS_ARCHIVE__
#define __FS_ARCHIVE__
#include <boost/filesystem.hpp>
#include <string>
#include <unordered_map>

#include <common.hxx>
#include <BackupCatalog.hxx>

using namespace credativ;
using namespace std;
using namespace boost::filesystem;
using namespace boost::posix_time;

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

    BackupFile(path handle) throw(CArchiveIssue);
    virtual ~BackupFile();

    virtual void setAvailable(bool avail);
    virtual void setCompressed(bool compressed);

    virtual bool isCompressed() { return this->compressed; };
    virtual bool isAvailable() { return this->available; };

  };

  /*
   * Class which represents a backup history file.
   */
  class BackupHistoryFile : public BackupFile {
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

    virtual void read() throw(CArchiveIssue);
    virtual string getBackupStartTime();
    virtual string getBackupStopTime();
    virtual void setBackupStopTime(string timeStr);
    virtual void setBackupStartTime(string timeStr);
    virtual string getBackupLabel();
    virtual string getBackupMethod();
    virtual string getBackupFrom();
    virtual string getBackupHistoryFilename();
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
  };

}

#endif
