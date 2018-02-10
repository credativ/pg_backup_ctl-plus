#include <unistd.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <boost/range/adaptors.hpp>
#include <boost/range/iterator_range.hpp>

#include <fs-archive.hxx>
#include <fs-pipe.hxx>

using namespace credativ;
using namespace std;
using namespace boost::adaptors;
using namespace boost::filesystem;
using namespace boost::iostreams;
using boost::regex;

/******************************************************************************
 * StreamingBaseBackupDirectory Implementation
 ******************************************************************************/

StreamingBaseBackupDirectory::StreamingBaseBackupDirectory(std::string streaming_dirname,
                                                           path archiveDir)  : BackupDirectory(archiveDir) {
  this->streaming_subdir = this->basedir() / streaming_dirname;
}

StreamingBaseBackupDirectory::StreamingBaseBackupDirectory(std::string streaming_dirname,
                                                           std::shared_ptr<BackupDirectory> parent)
  : BackupDirectory(parent->getArchiveDir()) {

  /*
  this->handle = parent->getArchiveDir();
  this->base   = parent->basedir();
  this->log    = parent->logdir();
  */

  this->streaming_subdir = this->basedir() / streaming_dirname;
}

StreamingBaseBackupDirectory::~StreamingBaseBackupDirectory() {}

void StreamingBaseBackupDirectory::create() {
  if (!exists(this->streaming_subdir)) {
#ifdef __DEBUG__
    cerr << "DEBUG: creating streaming basebackup directory "
         << "\""
         << this->streaming_subdir.string()
         << "\""
         << endl;
#endif
    create_directories(this->streaming_subdir);
  } else {
    std::ostringstream oss;
    oss << "directory " << this->streaming_subdir.string() << "already exists";
    throw CArchiveIssue(oss.str());
  }
}

void StreamingBaseBackupDirectory::fsync() {

  /*
   * If not a directory, error out.
   */
  if (!exists(this->streaming_subdir)) {
    ostringstream oss;
    oss << "archive directory " << this->handle.string() << " does not exist";
    throw CArchiveIssue(oss.str());
  }

  BackupDirectory::fsync(this->streaming_subdir);
}

void StreamingBaseBackupDirectory::remove() {

  if (!exists(this->streaming_subdir)) {
    ostringstream oss;
    oss << "archive directory " << this->handle.string() << " does not exist";
    throw CArchiveIssue(oss.str());
  }

  remove_all(this->streaming_subdir);
}

std::shared_ptr<BackupFile> StreamingBaseBackupDirectory::basebackup(std::string name,
                                                                     BackupProfileCompressType compression) {

  switch(compression) {

  case BACKUP_COMPRESS_TYPE_NONE:

    return std::make_shared<ArchiveFile>(this->streaming_subdir / name);
    break;

  case BACKUP_COMPRESS_TYPE_GZIP:

#ifdef PG_BACKUP_CTL_HAS_ZLIB
    return std::make_shared<CompressedArchiveFile>(this->streaming_subdir / (name + ".gz"));
#else
    throw CArchiveIssue("zlib compression support not compiled in");
#endif
    break;

  case BACKUP_COMPRESS_TYPE_ZSTD:

#ifdef PG_BACKUP_CTL_HAS_ZSTD
    return std::make_shared<ZSTDArchiveFile>(this->streaming_subdir / (name + ".zst"));
#else
    throw CArchiveIssue("zstandard compression support not compiled in");
#endif

  default:
    std::ostringstream oss;
    oss << "could not create archive file: invalid compression type: " << compression;
    throw CArchiveIssue(oss.str());

  }

}

path StreamingBaseBackupDirectory::getPath() {
  return this->streaming_subdir;
}

/******************************************************************************
 * ArchiveLogDirectory Implementation
 ******************************************************************************/

ArchiveLogDirectory::ArchiveLogDirectory(std::shared_ptr<BackupDirectory> parent)
  : BackupDirectory(parent->getArchiveDir()) {}

ArchiveLogDirectory::ArchiveLogDirectory(path parent)
  : BackupDirectory(parent) {}

ArchiveLogDirectory::~ArchiveLogDirectory() {}

path ArchiveLogDirectory::getPath() {
  return this->log;
}

/*
 * This code is highly adopted by PostgreSQL's receivewal.c, though
 * we made some special assumptions here for pg_backup_ctl++.
 *
 * Nothing magic, but it seems we can reuse a proven algorithm.
 *
 * See src/bin/pg_basebackup/receivewal.c in the PostgreSQL sources
 * for details.
 */
string ArchiveLogDirectory::getXlogStartPosition(unsigned int &timelineID,
                                                 unsigned int &segmentNumber,
                                                 unsigned long long xlogsegsize) {

  /* Type of segment found */
  WALSegmentFileStatus filestatus = WAL_SEGMENT_UNKNOWN;

  /* XLogRecPtr result to return */
  string result = "";

  /* Found XLog filename */
  string xlogfilename = "";

  /* real, uncompressed file size */
  unsigned long long fileSize = 0;

  /*
   * First check if logdir is a valid handle.
   */
  if (!exists(this->log)) {
    throw CArchiveIssue("could not read from archive log directory: \""
                        + this->log.string()
                        + "\"log doesn't exist");
  }

  timelineID = 0;
  segmentNumber = 0;

  /*
   * Loop through the archive log directory, examing each
   * file there and check wether it might be a XLOG segment file.
   *
   * If something found, extract a XLogRecPtr from its name and store
   * it.
   */
  for (auto& entry : boost::make_iterator_range(directory_iterator(this->log), {})) {

    std::string direntname = entry.path().filename().string();

    unsigned int current_tli = 0;
    unsigned int current_segno = 0;
    WALSegmentFileStatus current_status = WAL_SEGMENT_UNKNOWN;

    current_status = determineXlogSegmentStatus(entry.path());

    switch (current_status) {
    case WAL_SEGMENT_COMPLETE:
    case WAL_SEGMENT_COMPLETE_COMPRESSED:
    case WAL_SEGMENT_PARTIAL:
    case WAL_SEGMENT_PARTIAL_COMPRESSED:
      {
        /* XLOG segment file, save its name */
        xlogfilename = direntname;
        break;
      }
    case WAL_SEGMENT_UNKNOWN:
    case WAL_SEGMENT_INVALID_FILENAME:
      /*
       * We don't care about any files not really a segment
       * file. It's strange that we found them in the log/ directory,
       * but don't bother here
       */
      continue;
    }

    /*
     * Looks like we haven't found anything ???
     */
    if (xlogfilename.length() <= 0) {
      timelineID = 0;
      segmentNumber = 0;
      return "";
    }

    /* Determine Timeline and segment number of current xlog segment */
#if PG_VERSION_NUM < 110000
    XLogFromFileName(xlogfilename.c_str(), &current_tli, &current_segno);
#else
    XLogFromFileName(xlogfilename.c_str(), &current_tli,
                     &current_segno, xlogsegsize);
#endif

    /* Get the *real* uncompressed file size */
    fileSize = this->getXlogSegmentSize(entry.path(),
                                        xlogsegsize,
                                        current_status);

#ifdef __DEBUG_XLOG__
    cerr << "xlog file=" << xlogfilename << " "
         << "tli=" << current_tli << " "
         << "segmentNumber=" << current_segno << " "
         << "size=" << fileSize
         << endl;
#endif

    if ((fileSize != xlogsegsize)
        && (current_status == WAL_SEGMENT_COMPLETE)
        && (current_status == WAL_SEGMENT_PARTIAL) ) {
#ifdef __DEBUG_XLOG__
      cerr << "invalid file size ("
           << fileSize
           << "bytes) in segment "
           << direntname
           << ": skipping"
           << endl;
#endif
      continue;
    }

    /*
     * Remember current segment number and TLI
     * in case its going forward.
     *
     * We employ exactly the same algorithm than receivewal.c,
     * extract the position from the end of the *last* wal
     * segment file found in the archive. If the highest one wasn't completed,
     * we start streaming from the beginning of the partial segment we've
     * found.
     *
     * See src/bin/pg_basebackup/pg_receivewal.c::FindStreamingStart()
     * for details.
     */
    if ((current_segno > segmentNumber)
        || (current_segno == segmentNumber && current_tli > timelineID)
        || (current_segno == segmentNumber && current_tli == timelineID
            && (filestatus == WAL_SEGMENT_PARTIAL
                || filestatus == WAL_SEGMENT_PARTIAL_COMPRESSED)
            && (current_status != WAL_SEGMENT_PARTIAL
                || current_status != WAL_SEGMENT_PARTIAL_COMPRESSED) ) ) {

#ifdef __DEBUG__XLOG__
      cerr << "HI TLI " << current_tli << " SEGNO "
           << current_segno << " STATUS "
           << current_status << endl;
#endif

      segmentNumber = current_segno;
      timelineID    = current_tli;
      filestatus    = current_status;

    }

  }

  /* If something found, calculate the XLogRecPtr */
  if (segmentNumber > 0) {

    XLogRecPtr recptr;
    std::ostringstream recptrstr;

    /*
     * The starting pointer is either
     *
     * a) the next segment after a completed one, or
     * b) beginning of the current partial segment.
     *
     * Calculate offset into XLOG end (or start)
     */
#if PG_VERSION_NUM >= 110000
    XLogSegNoOffsetToRecPtr(segmentNumber, 0, recptr, xlogsegsize);
    recptr -= XLogSegmentOffset(recptr, xlogsegsize);
#else
    XLogSegNoOffsetToRecPtr(segmentNumber, 0, recptr);
    recptr -= recptr % XLOG_SEG_SIZE;
#endif

    /* Format as string and set return value */
    result = PGStream::encodeXLOGPos(recptr);

  }

  return result;
}

WALSegmentFileStatus ArchiveLogDirectory::determineXlogSegmentStatus(path segmentFile) {

  /*
   * We need to try hard here to get the
   * correct filenames in. We have to distinguish between
   *
   * a) completed uncompressed WAL segments (filter_complete)
   * b) completed compressed WAL segments (filter_complete_compressed)
   * c) partial uncompressed WAL segments (filter_partial)
   * d) partial compressed WAL segments (filter_partial_compressed)
   */
  WALSegmentFileStatus filestatus = WAL_SEGMENT_UNKNOWN;

  const regex filter_complete("[0-9A-F]*");
  const regex filter_complete_compressed("[0-9A-F]*.gz");
  const regex filter_partial("[0-9A-F]*.partial");
  const regex filter_partial_compressed("[0-9A-F]*.partial.gz");

  /*
   * For filename filtering...
   */
  boost::smatch what;

  /*
   * Filename as string, used for regex evaluation
   */
  string xlogfilename = segmentFile.filename().string();

  if (!is_regular_file(segmentFile))
    return filestatus;

  /* Apply XLOG segment filename filters */
  if (regex_match(xlogfilename, what, filter_complete)) {
    filestatus = WAL_SEGMENT_COMPLETE;
  } else if (regex_match(xlogfilename, what, filter_complete_compressed)) {
    filestatus = WAL_SEGMENT_COMPLETE_COMPRESSED;
  } else if (regex_match(xlogfilename, what, filter_partial)) {
    filestatus = WAL_SEGMENT_PARTIAL;
  } else if (regex_match(xlogfilename, what, filter_partial_compressed)) {
    filestatus = WAL_SEGMENT_PARTIAL_COMPRESSED;
  } else {
    /* Seems not a correctly named XLOG segment file. */
    filestatus = WAL_SEGMENT_INVALID_FILENAME;
  }

  return filestatus;
}

unsigned long long ArchiveLogDirectory::getXlogSegmentSize(path segmentFile,
                                                           unsigned long long xlogsegsize,
                                                           WALSegmentFileStatus status) {

  unsigned long long fileSize = 0;

  switch(status) {

    /* Size of uncompressed WAL segments are easy */
  case WAL_SEGMENT_COMPLETE:
  case WAL_SEGMENT_PARTIAL:
    {
      fileSize = file_size(segmentFile);
      break;
    }
  case WAL_SEGMENT_COMPLETE_COMPRESSED:
  case WAL_SEGMENT_PARTIAL_COMPRESSED:
    {
      /*
       * Since we have to deal with gzipped segment files
       * only, we can rely on the last 4 bytes, which is used
       * by gzip to stored the uncompressed size (ISIZE member).
       *
       * We can't use the compressed physical size here, since this
       * is not a reliable check.
       */
#ifdef PG_BACKUP_CTL_HAS_ZLIB
      /*
       * IMPORTANT: We can't use CompressedArchiveFile
       *            here, since it reads compressed data!
       */
      ArchiveFile xlogseg(segmentFile);
      char buf[4];
      int read_result;
      xlogseg.open();

      /*
       * No need to error check here, ArchiveFile
       * already throws an exception in this case.
       */
      xlogseg.lseek(-4, SEEK_END);

      if (xlogseg.read(buf, sizeof(buf)) != 1) {
        xlogseg.close();
        throw CArchiveIssue("short read on segment file "
                            + segmentFile.string()
                            + ": cannot retrieve uncompressed size ("
                            + strerror(errno));
      }

      xlogseg.close();
      read_result = (buf[3] << 24) | (buf[2] << 16) | (buf[1] << 8) | (buf[0]);
      fileSize = read_result;
#else
      /*
       * Huh, we are here although we don't have zlib support
       * compiled in. Error out hard, since this is something
       * we can't really deal with.
       */
      throw CArchiveIssue("attempt to read compressed archive files without zlib support");
#endif
      break;
    }
  default:
    /* We don't handle here WAL_SEGMENT_UNKNOWN
     * and WAL_SEGMENT_INVALID_FILENAME
     */
    fileSize = 0;
  }

  /* ... done */
  return fileSize;
}

/******************************************************************************
 * BackupDirectory Implementation
 ******************************************************************************/

void BackupDirectory::create() {
  if (!exists(this->handle)) {

#ifdef __DEBUG__
    cerr << "DEBUG: creating backup directory structure" << endl;
#endif

    create_directories(this->basedir());
    create_directories(this->logdir());

  } else {

    if (!exists(this->basedir())) {
#ifdef __DEBUG__
      cerr << "DEBUG: creating basedir directory" << endl;
#endif
      create_directory(this->basedir());
    }

    if (!exists(this->logdir())) {
#ifdef __DEBUG__
      cerr << "DEBUG: creating logdir directory" << endl;
#endif
      create_directory(this->logdir());
    }
  }

}

path BackupDirectory::getArchiveDir() {
  return this->handle;
}

BackupDirectory::BackupDirectory(path handle) {
  this->handle = canonical(handle);
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

std::shared_ptr<BackupFile> BackupDirectory::walfile(std::string name,
                                                     BackupProfileCompressType compression) {

  switch(compression) {

  case BACKUP_COMPRESS_TYPE_NONE:
    return std::make_shared<ArchiveFile>(this->log / name);
    break;

  case BACKUP_COMPRESS_TYPE_GZIP:
#ifdef PG_BACKUP_CTL_HAS_ZLIB
    return std::make_shared<CompressedArchiveFile>(this->log / (name + ".gz"));
#else
    throw CArchiveIssue("zlib compression support not compiled in");
#endif
    break;

  case BACKUP_COMPRESS_TYPE_ZSTD:
#ifdef PG_BACKUP_CTL_HAS_ZSTD
    return std::make_shared<ZSTDArchiveFile>(this->log / (name + ".zstd"));
#else
    throw CArchiveIssue("z/standard compression support not compiled in");
#endif
    break;

  default:
    std::ostringstream oss;
    oss << "could not create wal file: invalid compression type: " << compression;
    throw CArchiveIssue(oss.str());

  }
}

std::shared_ptr<BackupFile> BackupDirectory::basebackup(std::string name,
                                                        BackupProfileCompressType compression) {

  switch(compression) {

  case BACKUP_COMPRESS_TYPE_NONE:

    return std::make_shared<ArchiveFile>(this->basedir() / name);
    break;

  case BACKUP_COMPRESS_TYPE_GZIP:

#ifdef PG_BACKUP_CTL_HAS_ZLIB
    return std::make_shared<CompressedArchiveFile>(this->basedir() / (name + ".gz"));
#else
    throw CArchiveIssue("zlib compression support not compiled in");
#endif

    break;

  case BACKUP_COMPRESS_TYPE_ZSTD:
#ifdef PG_BACKUP_CTL_HAS_ZSTD
    return std::make_shared<ZSTDArchiveFile>(this->basedir() / (name + ".zstd"));
#else
    throw CArchiveIssue("z/standard compression support not compiled in");
#endif
    break;

  default:
    std::ostringstream oss;
    oss << "could not create archive file: invalid compression type: " << compression;
    throw CArchiveIssue(oss.str());

  }

}

void BackupDirectory::fsync() {

  /*
   * If not a directory, error out.
   */
  if (!exists(this->handle)) {
    ostringstream oss;
    oss << "archive directory " << this->handle.string() << " does not exist";
    throw CArchiveIssue(oss.str());
  }

  this->fsync(this->handle);

  /*
   * Don't forget to fsync backup subdirectories belong
   * to this archive handle.
   */
  this->fsync(this->logdir());
  this->fsync(this->basedir());

}

void BackupDirectory::fsync(path syncPath) {

  int dh; /* directory descriptor handle */

  /*
   * Open the directory to get a valid descriptor for
   * syncing.
   */
  if ((dh = open(syncPath.string().c_str(), O_RDONLY)) < 0) {
    /* error, check errno for error condition  and
     * throw an exception */
    std::ostringstream oss;

    oss << "could not open directory \""
        << syncPath.string()
        << "\" for syncing: "
        << strerror(errno);
    throw CArchiveIssue(oss.str());
  }

  if (::fsync(dh) != 0) {
    std::ostringstream oss;
    oss << "error fsyncing directory \""
        << syncPath.string()
        << "\": "
        << strerror(errno);
    throw CArchiveIssue(oss.str());
  }

}

shared_ptr<ArchiveLogDirectory> BackupDirectory::logdirectory() {
  return make_shared<ArchiveLogDirectory>(this->handle);
}

shared_ptr<BackupDirectory> CPGBackupCtlFS::getArchiveDirectoryDescr(string directory) {
  return make_shared<BackupDirectory>(path(directory));
}

path BackupDirectory::logdir() {
  return this->log;
}

path BackupDirectory::basedir() {
  return this->base;
}

CPGBackupCtlFS::CPGBackupCtlFS(string archiveDir) {

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

int CPGBackupCtlFS::readBackupHistory() {

  int countBackups = 0;
  path logDir      = this->archivePath / "log";

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

bool CPGBackupCtlFS::checkArchiveDirectory() {

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

size_t BackupFile::size() {
  return file_size(this->handle);
}

std::string BackupFile::getFileName() {
  return this->handle.filename().string();
}

std::string BackupFile::getFilePath() {
  return this->handle.string();
}

off_t BackupFile::current_position() {
  return this->currpos;
}

/******************************************************************************
 * ArchivePipedProcess Implementation
 ******************************************************************************/

ArchivePipedProcess::ArchivePipedProcess(path pathHandle) : BackupFile(pathHandle) {

}

ArchivePipedProcess::ArchivePipedProcess(path pathHandle,
                                         string executable,
                                         vector<string> execArgs)
  : BackupFile(pathHandle) {

  this->jobDescr.executable = executable;
  this->jobDescr.execArgs = execArgs;

}

ArchivePipedProcess::~ArchivePipedProcess() {}

bool ArchivePipedProcess::isOpen() {

  return (this->pid > 0) && this->opened;

}

void ArchivePipedProcess::open() {


  if (this->isOpen()) {
    throw CArchiveIssue("attempt to open piped process which is already open");
  }

  /*
   * ... just to make sure.
   */
  this->opened = false;

  /*
   * open() needs to do some hard work here, since
   * it does:
   *
   * - fork() the current process into a new one, execute
   *   the specified binary with exec()
   *
   * - establish pipe communication with the pipe
   *
   */

  /*
   * Initialize job handle
   */
  this->jobDescr.use_pipe = true;
  this->jobDescr.background_exec = true;

  /*
   * Do the fork()
   *
   * run_process() returns the PID of the forked
   * process that actually execute the binary. This also replaces
   * the child process via execve(). So check if we are the parent
   * and, to be schizo, make sure we exit the child in any case.
   */
  this->pid = run_process(this->jobDescr);

  if (this->pid > 0) {

#ifdef __DEBUG__
    cerr << "executing piped process with PID " << this->pid << endl;
#endif

    /*
     * Seems everything worked so far...
     */
    this->opened = true;

  } else if (this->pid < 0) {
    exit(0);
  } else {
    /* oops, something went wrong here */
    throw CArchiveIssue("could not fork piped process");
  }

}

size_t ArchivePipedProcess::write(const char *buf, size_t len) {

  ssize_t wbytes = 0;
  ssize_t req_bytes = len;
  ssize_t bytes_to_write = len;
  size_t result = 0;

  if (!this->isOpen()) {
    std::ostringstream oss;

    oss << "could not write into pipe with process "
        << this->jobDescr.executable.string()
        << ", file "
        << this->handle.string();
    throw CArchiveIssue(oss.str());
  }

  /*
   * Write directly into the writing end of our internal
   * pipe, if successfully opened. This is pipe_in[1] in this
   * case.
   *
   * Since we aren't using the FILE stream interface here, we
   * use write() instead of fwrite().
   */
  while ((wbytes += ::write(this->jobDescr.pipe_out[1],
                            (char *)buf +  wbytes, bytes_to_write)) < req_bytes) {

    if (wbytes == 0) {
      /* checkout errno if we were interrupted */
      if (errno == EINTR)
        bytes_to_write -= wbytes;
      else {
        std::ostringstream oss;

        oss << "error writing pipe: " << strerror(errno);
        throw CArchiveIssue(oss.str());
      }
    }

    result += wbytes;

  }

  return result;
}

size_t ArchivePipedProcess::read(char *buf, size_t len) {

  ssize_t rbytes = 0;
  ssize_t req_bytes = len;
  ssize_t bytes_to_read = len;
  size_t result = 0;

  if (!this->isOpen()) {
    std::ostringstream oss;

    oss << "could not read from pipe with process "
        << this->jobDescr.executable.string()
        << ", file "
        << this->handle.string();
    throw CArchiveIssue(oss.str());
  }

  /*
   * Read directly from the internal pipe. In this case
   * the read end is pipe_out[0].
   *
   * As we can't use the FILE stream API here, we rely
   * on read() instead of fread().
   */
  while ((rbytes = ::read(this->jobDescr.pipe_out[0],
                          (char *)buf + rbytes, bytes_to_read)) < req_bytes) {

    if (rbytes == 0) {
      /* checkout errno */
      if (errno == EINTR)
        bytes_to_read -= rbytes;
      else if (rbytes < 0) {
        std::ostringstream oss;

        oss << "error reading pipe: " << strerror(errno);
        throw CArchiveIssue(oss.str());
      }
    }

    result += rbytes;
  }

  return result;

}

off_t ArchivePipedProcess::lseek(off_t offset, int whence) {
  throw CArchiveIssue("piped I/O operation doesn't support seek");
}

void ArchivePipedProcess::setOpenMode(string mode) {
  this->mode = mode;
}

void ArchivePipedProcess::fsync() {

  /*
   * We can't operate on a pipe. fsync() here is only
   * supported of the attached file is closed and we don't
   * operate in pipe mode anymore.
   */
  if (this->isOpen()) {
    throw CArchiveIssue("Calling fsync() in pipe mode is not supported. Close the pipe before");
  }

  ArchiveFile afile(this->handle);
  afile.setOpenMode("w");
  afile.open();
  afile.fsync();
  afile.close();

}

void ArchivePipedProcess::rename(path& newname) {

  if (this->isOpen()) {
    std::ostringstream oss;

    oss << "Cannot rename file " << this->handle.string()
        << " into new file " << newname.string()
        << " while opened in pipe mode";
    throw CArchiveIssue(oss.str());
  }

  ArchiveFile afile(this->handle);

  /*
   * NOTE:
   *
   * We just rename() the file and thus are required
   * to open the file in read-only mode.
   */
  afile.setOpenMode("r+");
  afile.rename(newname);

  /*
   * NOTE: ne need to fsync(), rename() did the necessary work already.
   */
  afile.close();

}

void ArchivePipedProcess::close() {

  /*
   * close() here means we are closing our endpoints
   * of the communication pipe. These are in particular:
   *
   * READ end: pipe_out[0]
   * WRITE end: pipe_in[1]
   */
  if (this->isOpen()) {
    ::close(this->jobDescr.pipe_out[0]);
    ::close(this->jobDescr.pipe_in[1]);

    this->opened = false;
  }

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

off_t ArchiveFile::lseek(off_t offset, int whence) {

  int rc;

  /*
   * Valid file handle required.
   */
  if (!this->isOpen()) {
    std::ostringstream oss;
    oss << "cannot seek in file "
        << this->handle.string()
        << ": not opened";
    throw CArchiveIssue(oss.str());
  }

  rc = ::fseek(this->fp, offset, whence);

  if (rc < 0) {
    std::ostringstream oss;
    oss << "could not seek in file "
        << this->handle.string()
        << ": "
        << strerror(errno);
    throw CArchiveIssue(oss.str());
  }

  this->currpos = ftell(this->fp);
  return rc;

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

void ArchiveFile::remove() {

  int rc;

  if (this->fp == NULL)
    throw CArchiveIssue("cannot reference file descriptor for undefined file stream");

  /*
   * Filehandle should be closed, so we don't leak 'em
   */
  if (this->isOpen())
    throw CArchiveIssue("cannot remove file still referenced by handle");

  rc = unlink(this->handle.string().c_str());

  if (rc != 0) {
    std::ostringstream oss;
    oss << "cannot unlink file \"" << this->handle.string().c_str() << "\""
        << " (errno) " << errno;
    throw CArchiveIssue(oss.str());
  }
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

  if (result > 0)
    this->currpos += len;

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

  /*
   * Update internal offset
   */
  if (result > 0)
    this->currpos += len;

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
  this->currpos = 0;

}

void ArchiveFile::setCompressed(bool compressed) {
  if (compressed)
    throw CArchiveIssue("attempt to set compression to uncompressed file handle");

  /* no-op otherwise */
}

/******************************************************************************
 * Implementation of ZSTDArchiveFile
 *****************************************************************************/
#ifdef PG_BACKUP_CTL_HAS_ZSTD

ZSTDArchiveFile::ZSTDArchiveFile(path pathHandle) : BackupFile(pathHandle) {
  this->compressed = true;
  this->fp = NULL;

  /*
   * Initialize ZSTD compress/decompress contexts.
   */
  this->decompressCtx = ZSTD_createDCtx();
  this->compressCtx   = ZSTD_createCCtx();
}

ZSTDArchiveFile::~ZSTDArchiveFile() {

  /*
   * Free decompression/compression contexts.
   */
  if (this->decompressCtx != NULL)
    ZSTD_freeDCtx(this->decompressCtx);

  if (this->compressCtx != NULL)
    ZSTD_freeCCtx(this->compressCtx);

  if (this->isOpen())
    fclose(this->fp);

}

void ZSTDArchiveFile::rename(path& newname) {

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

FILE *ZSTDArchiveFile::getFileHandle() {
  return this->fp;
}

off_t ZSTDArchiveFile::lseek(off_t offset, int whence) {

  int rc;

  /*
   * Valid file handle required.
   */
  if (!this->isOpen()) {
    std::ostringstream oss;
    oss << "cannot seek in file "
        << this->handle.string()
        << ": not opened";
    throw CArchiveIssue(oss.str());
  }

  rc = ::fseek(this->fp, offset, whence);

  if (rc < 0) {
    std::ostringstream oss;
    oss << "could not seek in file "
        << this->handle.string()
        << ": "
        << strerror(errno);
    throw CArchiveIssue(oss.str());
  }

  this->currpos = ftell(this->fp);
  return rc;

}

bool ZSTDArchiveFile::isCompressed() {
  return true;
}

void ZSTDArchiveFile::setCompressed(bool compressed) {
  if (!compressed)
    throw CArchiveIssue("attempt to set uncompressed flag to compressed basebackup file handle");

  /* no-op otherwise */
}

bool ZSTDArchiveFile::isOpen() {
  return this->opened;
}

void ZSTDArchiveFile::setOpenMode(std::string mode) {

  this->mode = mode;

}

void ZSTDArchiveFile::setCompressionLevel(int level) {
  this->compression_level = level;
}

void ZSTDArchiveFile::open() {

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

  this->opened = true;
}

size_t ZSTDArchiveFile::write(const char *buf, size_t len) {

  size_t rc = 0;
  size_t bufOutSize = ZSTD_compressBound(len);
  /*
   * Buffer for outgoing compression data.
   */
  char out[bufOutSize];

  /*
   * Compress input
   */
  rc = ZSTD_compressCCtx(this->compressCtx,
                         out, bufOutSize,
                         buf, len,
                         this->compression_level);

  if (ZSTD_isError(rc)) {
    std::ostringstream oss;
    oss << "ZSTD compression failure for file \""
        << this->handle.string() << "\": "
        << ZSTD_getErrorName(rc);
    throw CArchiveIssue(oss.str());
  }

  /*
   * Write file handle.
   */
  if ((rc = fwrite(out, bufOutSize, 1, this->fp)) != 1) {
    std::ostringstream oss;
    oss << "write error for file (size="
        << len
        << ")"
        << this->handle.string()
        << ": "
        << strerror(errno);
    throw CArchiveIssue(oss.str());
  }

  if (rc > 0)
    this->currpos += bufOutSize;

  return rc;
}

void ZSTDArchiveFile::close() {

  if (this->isOpen()) {

    /*
     * Close file handle
     */
    if (this->fp == NULL) {
      std::ostringstream oss;
      oss << "attempt to close uninitialized file \""
          << this->handle.string() << "\"";
      throw CArchiveIssue(oss.str());
    }

    fclose(this->fp);
    this->fp = NULL;
    this->currpos = 0;

    this->opened = false;
  }

}

void ZSTDArchiveFile::remove() {

  int rc;

  if (this->fp == NULL)
    throw CArchiveIssue("cannot reference file descriptor for undefined file stream");

  /*
   * Filehandle should be closed, so we don't leak 'em
   */
  if (this->isOpen())
    throw CArchiveIssue("cannot remove file still referenced by handle");

  rc = unlink(this->handle.string().c_str());

  if (rc != 0) {
    std::ostringstream oss;
    oss << "cannot unlink file \"" << this->handle.string().c_str() << "\""
        << " (errno) " << errno;
    throw CArchiveIssue(oss.str());
  }

}

void ZSTDArchiveFile::fsync() {

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

size_t ZSTDArchiveFile::read(char *buf, size_t len) {

  size_t rc = 0;
  size_t inBufSize = ZSTD_compressBound(len);

  /*
   * Internal frame buffer.
   */
  char in[inBufSize];

  /* Read frame */
  if ((rc = fread(in, inBufSize, 1, this->fp)) != 1) {
    std::ostringstream oss;
    oss << "read error for ZSTD file (size="
        << len
        << ")"
        << this->handle.string()
        << ": "
        << strerror(errno);
    throw CArchiveIssue(oss.str());
  }

  /*
   * Decompress frame data. Make sure data does fit
   * into the target buffer.
   */
  rc = ZSTD_decompress(buf, len, in, inBufSize);

  if (ZSTD_isError(rc)) {
    std::ostringstream oss;
    oss << "decompression error for ZSTD file (size="
        << len
        << ")"
        << this->handle.string()
        << ": "
        << ZSTD_getErrorName(rc);
    throw CArchiveIssue(oss.str());

  }

  if (rc > 0)
    this->currpos += len;

  return rc;
}

#endif

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

  if (this->isOpen()) {
    /*
     * Don't leak file handles!
     *
     * NOTE: this also invalidates our internal
     *       FILE * pointer, since its *NOT* duplicated.
     */
    this->close();
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
  throw CArchiveIssue("rename() of compressed file name not yet implemented");
}

void CompressedArchiveFile::setOpenMode(std::string mode) {

  this->mode = mode;

}

void CompressedArchiveFile::setCompressionLevel(int level) {

  throw CArchiveIssue("overriding default compression level (9) not yet supported");

}

off_t CompressedArchiveFile::lseek(off_t offset, int whence) {

  int rc;

  /*
   * Valid file handle required.
   */
  if (!this->isOpen()) {
    std::ostringstream oss;
    oss << "cannot seek in file "
        << this->handle.string()
        << ": not opened";
    throw CArchiveIssue(oss.str());
  }

  rc = ::fseek(this->fp, offset, whence);

  if (rc < 0) {
    std::ostringstream oss;
    oss << "could not seek in file "
        << this->handle.string()
        << ": "
        << strerror(errno);
    throw CArchiveIssue(oss.str());
  }

  this->currpos = ::ftell(this->fp);
  return rc;

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

void CompressedArchiveFile::remove() {

  int rc;

  if (this->fp == NULL)
    throw CArchiveIssue("cannot reference file descriptor for undefined file stream");

  /*
   * Filehandle should be closed, so we don't leak 'em
   */
  if (this->isOpen())
    throw CArchiveIssue("cannot remove file still referenced by handle");

  rc = unlink(this->handle.string().c_str());

  if (rc != 0) {
    std::ostringstream oss;
    oss << "cannot unlink file \"" << this->handle.string().c_str() << "\""
        << " (errno) " << errno;
    throw CArchiveIssue(oss.str());
  }

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
      if (gzerrstr != NULL)
        oss << gzerrstr;
    }

    throw CArchiveIssue(oss.str());
  }

  if (wbytes > 0)
    this->currpos += len;

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

  if (rbytes > 0)
    this->currpos += len;

  return rbytes;
}


void CompressedArchiveFile::close() {

  if (this->isOpen()) {

    int rc;

    /*
     * IMPORTANT: gzclose() also invalidates our
     *            internal filehandle, since it's not
     *            duplicated!
     */
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

    this->zh = NULL;
    this->fp = NULL;
    this->currpos = 0;

  } else {
    std::ostringstream oss;
    oss << "attempt to close uninitialized file "
        << this->handle.string();
    throw CArchiveIssue(oss.str());
  }

  /* only reached in case of no errors */
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

BackupHistoryFile::BackupHistoryFile(path handle, bool readFile)
  : BackupFile(handle) {

  if (!readFile) {
    this->opened = false;
  } else {
    read();
  }

}

void BackupHistoryFile::open() {

  read();

}

void BackupHistoryFile::rename(path& newname) {
  throw CArchiveIssue("renaming backup history files not supported");
}

size_t BackupHistoryFile::read_mem(MemoryBuffer &mybuffer) {
  throw CArchiveIssue("not yet implemented");
}

size_t BackupHistoryFile::write_mem(MemoryBuffer &mybuffer) {
  throw CArchiveIssue("not yet implemented");
}

size_t BackupHistoryFile::read(char *buf, size_t len) {

  throw CArchiveIssue("reading backup history files directly is not supported. Use read() instead.");

}

void BackupHistoryFile::remove() {
  throw CArchiveIssue("removing a backup history is not yet supported");
}

void BackupHistoryFile::setOpenMode(std::string mode) {

  throw CArchiveIssue("backup history file can only be opened read-only");

}

off_t BackupHistoryFile::lseek(off_t offset, int whence) {

  throw CArchiveIssue("seeking in a backup history file not supported");

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
    status(this->handle);

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
