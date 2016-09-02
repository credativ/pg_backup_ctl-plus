#ifndef __FS_ARCHIVE__
#define __FS_ARCHIVE__
#include <boost/filesystem.hpp>

#include <common.hxx>
#include <BackupCatalog.hxx>

using namespace credativ;
using namespace std;
using namespace boost::filesystem;

namespace credativ {

  class CArchiveIssue : public CPGBackupCtlFailure {
  public:
    CArchiveIssue(const char *errstr) throw() : CPGBackupCtlFailure(errstr) {};
  };

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
     * C'tor, needs archive directory to be specified
     */
    CPGBackupCtlFS(string archiveDir) throw(CArchiveIssue);
    virtual ~CPGBackupCtlFS();

    /*
     * Returns the configure archive directory
     */
    string getArchiveDirectory();

    /*
     * Get a catalog descriptor from the given
     * archive directory.
     */
    virtual shared_ptr<CatalogDescr> getCatalogDescrFromArchive();

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
