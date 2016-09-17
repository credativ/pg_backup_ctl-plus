#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <boost/range/adaptors.hpp>
#include <boost/range/iterator_range.hpp>
#include <boost/regex.hpp>

#include <fs-archive.hxx>

using namespace credativ;
using namespace std;
using namespace boost::adaptors;
using namespace boost::filesystem;
using namespace boost::iostreams;
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

shared_ptr<CatalogDescr> CPGBackupCtlFS::catalogDescrFromBackupHistoryFile(shared_ptr<BackupHistoryFile> file) {
  shared_ptr<CatalogDescr> result;

  result = make_shared<CatalogDescr>();
  result->id = -1; /* is a new one ! */
  result->label = file->getBackupLabel();
  result->directory = this->archiveDir;
  result->compression = false;

  return result;
}

int CPGBackupCtlFS::readBackupHistory() throw(CArchiveIssue) {

  int countBackups = 0;
  path logDir      = this->archivePath / "log";

  const std::string target_path( this->archiveDir + "/log" );

  /* filter for backup history files, should also match *.gz */
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

    /*
     * If we have found a backup history file, push its information
     * into a catalog descriptor.
     */
    if(regex_match(entry.path().filename().string(), what, my_filter)) {

      /*
       * Create a backup history file object representing
       * the content of the current file.
       */
      shared_ptr<BackupHistoryFile> handle = make_shared<BackupHistoryFile>(entry);

      /*
       * Push the handle into our private history list.
       */
      this->history.insert(make_pair(handle->getBackupLabel(), handle));

      /*
       * Check if the base backup is available
       */
      if (!this->backupExists(handle->getBackupLabel()))
        handle->setAvailable(true);

      countBackups++;
    }

  }

  return countBackups;
}

bool CPGBackupCtlFS::backupExists(string backup) {

  bool result = false;

  /*
   * NOTE: a basebackup is either a file or
   * directory, depending on the mode.
   */
  if (is_directory(backup))
    result = (result || true);

  if (is_regular_file(backup))
    result = (result || true);

  return result; /* only reached if not present */
}

bool CPGBackupCtlFS::XLOGSegmentExists(string xlogFile) {
  if (is_regular_file(this->archivePath / "log" / xlogFile))
    return true;

  return false; /* only reached in case no xlogfile present */
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

BackupFile::BackupFile(path handle) throw(CArchiveIssue) {
  this->handle = handle;
}

BackupFile::~BackupFile(){}

void BackupFile::setCompressed(bool compressed) {
  this->compressed = compressed;
}

void BackupFile::setAvailable(bool avail) {
  this->available = avail;
}

BackupHistoryFile::BackupHistoryFile(path handle) : BackupFile(handle) {

  read();

}

BackupHistoryFile::~BackupHistoryFile() {}

/*
 * Read reads and parses the content of the specified
 * backup history file.
 */
void BackupHistoryFile::read() throw(CArchiveIssue) {
  try {
    string line;
    std::ifstream fileHandle;
    std::stringstream  hfile;
    bool compressed = false;

    /* Check state of the history file first */
    file_status state = status(this->handle);

    /*
     * Open the backup history file.
     */
    CPGBackupCtlBase::openFile(fileHandle,
                               hfile,
                               this->handle,
                               &compressed);
    this->setCompressed(compressed);

    while(std::getline(hfile, line)) {

      const char *c_input = line.c_str();
      char xlogfile[MAXXLOGFNAMELEN];
      char anybuf[MAXXLOGFNAMELEN];
      char dateinput[16];
      char timeinput[16];
      char timezone[64];

      /*
       * We need a large buffer for this!
       *
       * PostgreSQL defines MAXPGPATH = 1024 for this, see
       *
       * src/include/pg_config_manual.h
       */

      char backuplabel[MAXPGPATH];

      unsigned int hi;
      unsigned int lo;
      char ch;

      /*
       * Parse each line with this rather
       * crude code (borrowed from src/backend/access/transam/xlog.c)
       */
      if (sscanf(c_input,
                 "START WAL LOCATION: %X/%X (file %24s)%c",
                 &(this->startLoc.hi), &(this->startLoc.lo),
                 xlogfile, &ch) == 3) {
        /* convert this into a C++ string */
        this->startLoc.startXLOG = xlogfile;
      }

      if (sscanf(c_input,
                 "STOP WAL LOCATION: %X/%X (file %24s)%c",
                 &(this->stopLoc.hi), &(this->stopLoc.lo),
                 xlogfile, &ch) == 3) {
        /* convert this into a C++ string */
        this->stopLoc.stopXLOG = xlogfile;
      }

      if (sscanf(c_input,
                 "CHECKPOINT LOCATION: %X/%X%c",
                 &(this->chkPtLoc.hi), &(this->chkPtLoc.lo), &ch) == 2) {
        /* nothing to do */
      }

      if (sscanf(c_input,
                 "BACKUP METHOD: %19s%c",
                 anybuf, &ch) == 1) {
        /* copy this into a C++ string */
        this->backupMethod = anybuf;
      }

      if (sscanf(c_input,
                 "BACKUP FROM: %19s%c",
                 anybuf, &ch) == 1) {
        /* copy this into a C++ string */
        this->backupFrom = anybuf;
      }

      if (sscanf(c_input,
                 "START TIME: %10s %10s %s",
                 dateinput, timeinput, timezone) == 3) {
        /*
         * Make a boost::date class
         */
        this->setBackupStartTime((string(dateinput)
                                  + " "
                                  + string(timeinput)
                                  + " "
                                  + string(timezone)));
      }

      if (sscanf(c_input,
                 "STOP TIME: %10s %10s %s",
                 dateinput, timeinput, timezone) == 3) {
        /*
         * Make a boost::date class
         */
        this->setBackupStopTime((string(dateinput)
                                 + " "
                                 + string(timeinput)
                                 + " "
                                 + string(timezone)));
      }

      /*
       * Should stay last entry!
       */
      if (sscanf(c_input,
                 "LABEL: %s",
                 backuplabel) == 1) {
        /* copy this into a C++ string */
        this->backupLabel = backuplabel;
      }
    }

    /*
     * If the file was opened directly (e.g.
     * it doesn't need to be decompressed), we don't
     * want to leak it...
     */
    //hfile->close();

  } catch(exception &e) {
    /* rethrow as CArchiveIssue */
    throw CArchiveIssue(e.what());
  }
}

void BackupHistoryFile::setBackupStopTime(string timeStr) {
  this->backupStopped = CPGBackupCtlBase::ISO8601_strTo_ptime(timeStr);
}

string BackupHistoryFile::getBackupStopTime() {
  return CPGBackupCtlBase::ptime_to_str(this->backupStopped);
}

void BackupHistoryFile::setBackupStartTime(string timeStr) {
  this->backupStarted = CPGBackupCtlBase::ISO8601_strTo_ptime(timeStr);
}

string BackupHistoryFile::getBackupStartTime() {
  return CPGBackupCtlBase::ptime_to_str(this->backupStarted);
}

string BackupHistoryFile::getBackupHistoryFilename() {
  return this->handle.string();
}

string BackupHistoryFile::getBackupFrom() {
  return this->backupFrom;
}

string BackupHistoryFile::getBackupMethod() {
  return this->backupMethod;
}

string BackupHistoryFile::getBackupLabel() {
  return this->backupLabel;
}
