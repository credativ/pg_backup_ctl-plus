#include <boost/range/iterator_range.hpp>
#include <iostream>
#include <string>
#include <fs-archive.hxx>

using namespace credativ;
using namespace std;
using namespace boost::filesystem;

CPGBackupCtlFS::CPGBackupCtlFS(string archiveDir) throw(CArchiveIssue) {

  /* initialization */
  this->archiveDir = archiveDir;

  /*
   * Initialize path handle for boost::filesystem
   */
  this->archivePath = path(this->archiveDir);
}

string CPGBackupCtlFS::getArchiveDirectory() {
  return this->archiveDir;
}

CPGBackupCtlFS::~CPGBackupCtlFS() {
  /* destructor */
}

shared_ptr<CatalogDescr> CPGBackupCtlFS::getCatalogDescrFromArchive() {
  return make_shared<CatalogDescr>();
}

int CPGBackupCtlFS::readBackupHistory() throw(CArchiveIssue) {

  int countBackups = 0;
  path logDir      = this->archivePath / "log";
  /*
   * Open directory iterator, loop
   * through the segment files and try to find
   * any *.backup history files.
   */
  for (auto& entry : boost::make_iterator_range(directory_iterator(logDir), {}))
    cout << entry << "\n";

  return countBackups;
}

bool CPGBackupCtlFS::checkArchiveDirectory() throw(CArchiveIssue) {

  /*
   * We must check for file_status exceptions
   * itself here.
   */
  try {

    /*
     * Check presence of required archive and its subdirectories.
     */
    if (!exists(this->archivePath))
      throw CArchiveIssue("archive directory doesn't exist");

    if (!exists(this->archivePath / "base"))
      throw CArchiveIssue("archive base directory doesn't exist");

    if (!exists(this->archivePath / "log"))
      throw CArchiveIssue("archive log directory doesn't exist");

    /*
     * is a directory?
     */
    if (!is_directory(this->archivePath))
      throw CArchiveIssue("archive directory is not a directory");

    if (!is_directory(this->archivePath / "base"))
      throw CArchiveIssue("archive base directory is not a directory");

    if (!is_directory(this->archivePath / "log"))
      throw CArchiveIssue("archive base directory is not a directory");
  }
  catch (exception &fse) {
    /*
     * Rethrow as archive issue.
     */
    throw CArchiveIssue(fse.what());
  }

  return true;
}

void CPGBackupCtlFS::readArchiveDirectory() {

}
