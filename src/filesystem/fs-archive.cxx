#include <boost/range/adaptors.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/regex.hpp>
#include <iostream>
#include <string>
#include <fs-archive.hxx>

using namespace credativ;
using namespace std;
using namespace boost::adaptors;
using namespace boost::filesystem;
using boost::regex;

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

  const std::string target_path( this->archiveDir + "/log" );
  const regex my_filter( "[0-9A-F].*.backup.*" );
  boost::smatch what;

  /*
   * Open directory iterator, loop
   * through the segment files and try to filter out
   * any *.backup history files.
   *
   * XXX:
   *
   * This might be an expensive operation, since the
   * log/ directory could have tons of segments files
   */
  for (auto& entry : boost::make_iterator_range(directory_iterator(logDir), {})) {

    /* skip if not a regular file */
    if (!is_regular_file(entry.status()))
      continue;

    if(regex_match(entry.path().filename().string(), what, my_filter)) {
      countBackups++;
    }

  }

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

BackupHistoryFile::BackupHistoryFile(path historyFile) throw(CArchiveIssue) {

}

BackupHistoryFile::~BackupHistoryFile() {
   
}
