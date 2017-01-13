#include <unistd.h>
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

BackupDirectory::BackupDirectory(path handle) {
  this->handle = handle;
  this->base = handle / "base";
  this->log  = handle / "log";
}

BackupDirectory::~BackupDirectory() {}

void BackupDirectory::verify() {

  /*
   * Check the archive directory itself.
   * It makes no sense to proceed for base/ and log/
   * subdirectories as long this is not present.
   */

  if (!exists(this->handle)) {
    ostringstream oss;
    oss << "archive directory " << this->handle.string() << " does not exist";
    throw CArchiveIssue(oss.str());
  }

  /*
   * Okay, looks like its there, but don't get fooled by a file.
   */
  if (!is_directory(this->handle)) {
    ostringstream oss;
    oss << "\"" << this->handle.string() << "\" is not a directory";
    throw CArchiveIssue(oss.str());
  }

  /*
   * Looks like this archive directory is okay. Place an updated PG_BACKUP_CTL_INFO
   * file there.
   */
  try {
    path magicFile = path(this->handle / PG_BACKUP_CTL_INFO_FILE);
    CPGBackupCtlBase::writeFileReplace(magicFile.string(),
                                       BackupCatalog::magicNumber()
                                       + " | "
                                       + CPGBackupCtlBase::current_timestamp());
  } catch(CPGBackupCtlFailure& e) {
    /*
     * re-throw as CArchiveIssue
     */
    ostringstream oss;
    oss << "verify command failed: " << e.what();
    throw CArchiveIssue(oss.str());
  }

}

void BackupDirectory::fsync() {

  int dh; /* directory descriptor handle */

  /*
   * If not a directory, error out.
   */
  if (!exists(this->handle)) {
    ostringstream oss;
    oss << "archive directory " << this->handle.string() << " does not exist";
    throw CArchiveIssue(oss.str());
  }

  /*
   * Open the directory to get a valid descriptor for
   * syncing.
   */
  if ((dh = open(this->handle.string().c_str(), O_RDONLY)) < 0) {
    /* error, check errno for error condition  and
     * throw an exception */
    std::ostringstream oss;

    oss << "could not open directory \""
        << this->handle.string()
        << "\" for syncing: "
        << strerror(errno);
    throw CArchiveIssue(oss.str());
  }

  if (::fsync(dh) != 0) {
    std::ostringstream oss;
    oss << "error fsyncing directory \""
        << this->handle.string()
        << "\": "
        << strerror(errno);
    throw CArchiveIssue(oss.str());
  }

}

shared_ptr<BackupDirectory> CPGBackupCtlFS::getArchiveDirectoryDescr(string directory) {
  return make_shared<BackupDirectory>(path(directory));
}

path BackupDirectory::logdir() {
  return this->base / this->log;
}

path BackupDirectory::basedir() {
  return this->base;
}

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

std::shared_ptr<BackupFile> getBackupFile(path file,
                                          bool compressed) {

  /*
   * Iff compressed is requested, create a CompressedBackupFile
   * instance. If false, just return a ArchiveFile instance.
   */
  if (compressed) {
    return make_shared<CompressedArchiveFile>(file);
  } else {
    return make_shared<ArchiveFile>(file);
  }

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

BackupFile::BackupFile(path handle) {
  this->handle = handle;

  /*
   * We implicitly set the file to compressed if
   * we encounter an extension .gz.
   */
  path file_ext = this->handle.extension();

  if (file_ext.string() == ".gz")
    this->setCompressed(true);
}

BackupFile::~BackupFile(){}

void BackupFile::setCompressed(bool compressed) {
  this->compressed = compressed;
}

void BackupFile::setAvailable(bool avail) {
  this->available = avail;
}

bool BackupFile::exists() {
  return (this->available = boost::filesystem::exists(status(this->handle)));
}

/******************************************************************************
 * Implementation of ArchiveFile
 *****************************************************************************/

ArchiveFile::ArchiveFile(path pathHandle) : BackupFile(pathHandle) {
  this->compressed = false;
  this->opened = false;
  this->fp = NULL;
}

ArchiveFile::~ArchiveFile() {

  if (this->fp != NULL) {
    fclose(this->fp);
    this->fp = NULL;
    this->opened = false;
  }

}

bool ArchiveFile::isCompressed() {
  return false;
}

void ArchiveFile::rename(path& newname) {

  /*
   * First, check if newname already exists. If true, error out.
   */
  if (boost::filesystem::exists(status(newname))) {
    std::ostringstream oss;
    oss << "cannot rename "
        << this->handle.string()
        << "to "
        << newname.string()
        << ": file exists";
    throw CArchiveIssue(oss.str());
  }

  /*
   * Old file exists?
   */
  if (!boost::filesystem::exists(status(this->handle))) {
    std::ostringstream oss;
    oss << "cannot rename "
        << this->handle.string()
        << "to "
        << newname.string()
        << ": source file does not exist";
    throw CArchiveIssue(oss.str());
  }

  /*
   * Fsync our old file instance.
   */
  this->fsync();

  /*
   * Close the old handle.
   */
  this->close();

  if (::rename(this->handle.string().c_str(),
               newname.string().c_str()) < 0) {
    std::ostringstream oss;
    oss << "cannot rename "
        << this->handle.string()
        << "to "
        << newname.string()
        << ": "
        << strerror(errno);
    throw CArchiveIssue(oss.str());
  }

  /*
   * Copy the newname handle into our private one.
   */
  this->handle = newname;

  /*
   * Open the new file.
   */
  this->open();

  /*
   * Fsync the new file.
   */
  this->fsync();

  /* and we're done ... */
}

void ArchiveFile::open() {

  /* check if we already hold a valid file stream pointer */
  if (this->fp != NULL) {
    std::ostringstream oss;
    oss << "error opening "
        << "\""
        << this->handle.string()
        << "\": "
        << "file handle already initialized";
    throw CArchiveIssue(oss.str());
  }

  this->fp = fopen(this->handle.string().c_str(),
                   this->mode.c_str());

  if (this->fp == NULL) {
    std::ostringstream oss;
    oss << "could not open file " << this->handle.string();
    throw CArchiveIssue(oss.str());
  }

  this->opened = true;

}

void ArchiveFile::setOpenMode(std::string mode) {

  this->mode = mode;

}

FILE *ArchiveFile::getFileHandle() {
  return this->fp;
}

int ArchiveFile::getFileno() {

  if (this->fp == NULL) {
    throw CArchiveIssue("cannot reference file descriptor for undefined file stream");
  }

  int result = fileno(this->fp);

  if (errno == EBADF) {
    throw CArchiveIssue("invalid file stream handle for fileno");
  }

  return result;

}

size_t ArchiveFile::read(char *buf, size_t len) {

  size_t result;

  if (this->fp == NULL) {
    std::ostringstream oss;
    oss << "attempt to read from uninitialized file \""
        << this->handle.string() << "\"";
    throw CArchiveIssue(oss.str());
  }

  if ((result = fread(buf, len, 1, this->fp)) != 1) {

    std::ostringstream oss;
    oss << "read error for file (size="
        << len
        << ")"
        << this->handle.string()
        << ": "
        << strerror(errno);
    throw CArchiveIssue(oss.str());


  }

  return result;

}

size_t ArchiveFile::write(const char *buf, size_t len) {

  size_t result;

  if (this->fp == NULL) {
    std::ostringstream oss;
    oss << "attempt to write into uninitialized file \""
        << this->handle.string() << "\"";
    throw CArchiveIssue(oss.str());
  }

  if ((result = fwrite(buf, len, 1, this->fp)) != 1) {

    std::ostringstream oss;
    oss << "write error for file (size="
        << len
        << ")"
        << this->handle.string()
        << ": "
        << strerror(errno);
    throw CArchiveIssue(oss.str());

  }

  return result;

}

bool ArchiveFile::isOpen() {
  return this->opened;
}

void ArchiveFile::fsync() {

  if (this->fp == NULL) {
    std::ostringstream oss;
    oss << "attempt to fsync uninitialized file \""
        << this->handle.string() << "\"";
    throw CArchiveIssue(oss.str());
  }

  if (::fsync(fileno(this->fp)) != 0) {
    std::ostringstream oss;
    oss << "error fsyncing file \""
        << this->handle.string()
        << "\": "
        << strerror(errno);
    throw CArchiveIssue(oss.str());
  }

}

void ArchiveFile::close() {

  if (this->fp == NULL) {
    std::ostringstream oss;
    oss << "attempt to close uninitialized file \""
        << this->handle.string() << "\"";
    throw CArchiveIssue(oss.str());
  }

  fclose(this->fp);
  this->fp = NULL;

}

void ArchiveFile::setCompressed(bool compressed) {
  if (compressed)
    throw CArchiveIssue("attempt to set compression to uncompressed file handle");

  /* no-op otherwise */
}

/******************************************************************************
 * Implementation of CompressedArchiveFile
 *****************************************************************************/

#ifdef PG_BACKUP_CTL_HAS_ZLIB

CompressedArchiveFile::CompressedArchiveFile(path pathHandle) : BackupFile(pathHandle) {
  this->compressed = true;
  this->fp = NULL;
  this->zh = NULL;
  this->opened = false;
}

CompressedArchiveFile::~CompressedArchiveFile() {

  if (this->zh != NULL) {
    /*
     * NOTE: this also invalidates our internal
     *       FILE * pointer, since its *NOT* duplicated.
     */
    gzclose(this->zh);
    this->zh = NULL;
    this->fp = NULL;
    this->opened = false;
  }

}

bool CompressedArchiveFile::isCompressed() {
  return true;
}

void CompressedArchiveFile::setCompressed(bool compressed) {
  if (!compressed)
    throw CArchiveIssue("attempt to set uncompressed flag to compressed basebackup file handle");

  /* no-op otherwise */
}

void CompressedArchiveFile::rename(path& newname) {

}

void CompressedArchiveFile::setOpenMode(std::string mode) {

  this->mode = mode;

}

void CompressedArchiveFile::setCompressionLevel(int level) {

  throw CArchiveIssue("overriding default compression level (9) not yet supported");

}

void CompressedArchiveFile::open() {

  /* check if we already hold a valid file stream pointer */
  if (this->fp != NULL) {
    std::ostringstream oss;
    oss << "error opening "
        << "\""
        << this->handle.string()
        << "\": "
        << "file handle already initialized";
    throw CArchiveIssue(oss.str());
  }

  this->fp = fopen(this->handle.string().c_str(),
                   this->mode.c_str());


  if (this->fp == NULL) {
    std::ostringstream oss;
    oss << "could not open compressed file \""
        << this->handle.string() << " "
        << "for writing: "
        << strerror(errno);
    throw CArchiveIssue(oss.str());
  }

  /*
   * NOTE: We'll attach the gzipp'ed stream directly
   *       to the previously opened FILE, otherwise
   *       we'll have to maintain duplicated file descriptor handles
   *       (e.g. when using dup()).
   */
  this->zh = gzdopen(fileno(this->fp), this->mode.c_str());

  if (this->fp == NULL) {
    std::ostringstream oss;
    oss << "could not open compressed file \""
        << this->handle.string() << " "
        << "for writing: "
        << strerror(errno);
    throw CArchiveIssue(oss.str());
  }

  this->opened = true;

}

bool CompressedArchiveFile::isOpen() {
  return this->opened;
}

FILE* CompressedArchiveFile::getFileHandle() {
  return this->fp;
}

gzFile CompressedArchiveFile::getGZHandle() {
  return this->zh;
}

void CompressedArchiveFile::fsync() {

  if (this->fp == NULL) {
    std::ostringstream oss;
    oss << "attempt to fsync uninitialized file \""
        << this->handle.string() << "\"";
    throw CArchiveIssue(oss.str());
  }

  if (::fsync(fileno(this->fp)) != 0) {
    std::ostringstream oss;
    oss << "error fsyncing file \""
        << this->handle.string()
        << "\": "
        << strerror(errno);
    throw CArchiveIssue(oss.str());
  }

}

size_t CompressedArchiveFile::write(const char *buf, size_t len) {

  if (!this->isOpen()) {
    std::ostringstream oss;
    oss << "attempt to write into unitialized file "
        << this->handle.string();
    throw CArchiveIssue(oss.str());
  }

  int wbytes = gzwrite(this->zh, buf, len);

  /*
   * NOTE: return value 0 means for
   *       gzwrite() that it has
   *       encountered an error!
   */
  if (wbytes == 0) {

    std::ostringstream oss;
    const char *gzerrstr;
    int gzerrno;

    oss << "unable to write "
        << len << " "
        << "bytes to file "
        << "\"" << this->handle.string() << "\": ";

    gzerrstr = gzerror(this->zh, &gzerrno);

    if (gzerrno == Z_ERRNO) {
      /* error happened in the filesystem layer */
      oss << strerror(errno);
    } else {
      oss << gzerrstr;
    }

    throw CArchiveIssue(oss.str());
  }

  return wbytes;
}

size_t CompressedArchiveFile::read(char *buf, size_t len) {

  if (!this->isOpen()) {
    std::ostringstream oss;
    oss << "attempt to read from unitialized file "
        << this->handle.string();
    throw CArchiveIssue(oss.str());
  }

  /*
   * In opposite to gzwrite, an error from gzread is indicated
   * with a return code -1.
   */
  int rbytes = gzread(this->zh, buf, len);

  if (rbytes < 0) {

    std::ostringstream oss;
    const char *gzerrstr;
    int gzerrno;

    oss << "unable to write "
        << len << " "
        << "bytes to file "
        << "\"" << this->handle.string() << "\": ";

    gzerrstr = gzerror(this->zh, &gzerrno);

    if (gzerrno == Z_OK) {
      /* check errno */
      oss << strerror(errno);
    } else {
      oss << gzerrstr;
    }

    throw CArchiveIssue(oss.str());

  }

  return rbytes;
}


void CompressedArchiveFile::close() {

  if (this->isOpen()) {

    int rc;

    if ((rc = gzclose(this->zh)) != Z_OK) {

      std::ostringstream oss;

      oss << "could not close file \""
          << this->handle.string() << "\": ";

      switch (rc) {
      case Z_STREAM_ERROR:
        oss << "gzip stream error";
        break;
      case Z_ERRNO:
        oss << strerror(errno);
        break;
      case Z_MEM_ERROR:
        oss << "out of memory";
        break;
      default:
        oss << "unknown error";
        break;
      }

      this->opened = false;
      throw CArchiveIssue(oss.str());
    }

  } else {
    std::ostringstream oss;
    oss << "attempt to close uninitialized file "
        << this->handle.string();
    throw CArchiveIssue(oss.str());
  }

  this->opened = false;

}

#endif

/******************************************************************************
 * Implementation of BackupHistoryFile
 *****************************************************************************/

BackupHistoryFile::BackupHistoryFile(path handle) : BackupFile(handle) {

  this->opened = false;
  read();

}

void BackupHistoryFile::open() {

  read();

}

void BackupHistoryFile::rename(path& newname) {
  throw CArchiveIssue("renaming backup history files not supported");
}

size_t BackupHistoryFile::read(char *buf, size_t len) {

  throw CArchiveIssue("reading backup history files directly is not supported. Use read() instead.");

}

void BackupHistoryFile::setOpenMode(std::string mode) {

  throw CArchiveIssue("backup history file can only be opened read-only");

}

bool BackupHistoryFile::isOpen() {
  return this->opened;
}

void BackupHistoryFile::close() {
  throw CArchiveIssue("close() not implemented for BackupHistoryFile");
}

void BackupHistoryFile::fsync() {
  throw CArchiveIssue("fsync() not implemented for BackupHistoryFile");
}

size_t BackupHistoryFile::write(const char *buf, size_t len) {
  throw CArchiveIssue("write() not implemented for BackupHistoryFile");
}

BackupHistoryFile::~BackupHistoryFile() {}

/*
 * Read reads and parses the content of the specified
 * backup history file.
 */
void BackupHistoryFile::read() {
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

    /*
     * Before starting to read, set opened to false.
     */
    this->opened = false;

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

  } catch(exception &e) {
    /* rethrow as CArchiveIssue */
    this->opened = false;
    throw CArchiveIssue(e.what());
  }

  this->opened = true;
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
