#include <unistd.h>
#include <iostream>
#include <iterator>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
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

StreamingBaseBackupDirectory *StreamingBaseBackupDirectory::getInstance(string dirname,
                                                                        path archiveDir) {

  StreamingBaseBackupDirectory *backupdir
    = new StreamingBaseBackupDirectory(dirname, archiveDir);

  return backupdir;

}

BaseBackupVerificationCode StreamingBaseBackupDirectory::verify(std::shared_ptr<BaseBackupDescr> bbdescr) {

  /*
   * Sanity check for specified basebackup descriptor.
   */
  if (bbdescr == nullptr) {
    return BASEBACKUP_DESCR_INVALID;
  }

  if (bbdescr->id < 0) {
    return BASEBACKUP_DESCR_INVALID;
  }

  /*
   * Check specific status codes.
   */
  if (bbdescr->status == "in progress") {
    return BASEBACKUP_IN_PROGRESS;
  }

  if (bbdescr->status == "aborted") {
    return BASEBACKUP_ABORTED;
  }

  /*
   * Check if streaming directory exists.
   */
  if (!exists(bbdescr->fsentry)) {
    return BASEBACKUP_DIRECTORY_MISSING;
  }

  /*
   * TODO: Verify WAL start and end position.
   */

  return BASEBACKUP_OK;
}

size_t StreamingBaseBackupDirectory::size() {

  size_t result = 0;

  for(recursive_directory_iterator it(this->streaming_subdir);
      it != recursive_directory_iterator(); ++it) {

      if(!is_directory(*it))
        result += file_size(*it);

    }

  return result;
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

  case BACKUP_COMPRESS_TYPE_PBZIP:
    {
      std::shared_ptr<ArchivePipedProcess> myfile
        = std::make_shared<ArchivePipedProcess>(this->streaming_subdir / (name + ".bz2"));
      std::string filename = myfile->getFilePath();

      myfile->setExecutable("pbzip2");
      myfile->pushExecArgument("-l");
      myfile->pushExecArgument("-c");
      myfile->pushExecArgument(">");
      myfile->pushExecArgument(filename);

      return myfile;
      break;
    }

  case BACKUP_COMPRESS_TYPE_PLAIN:
    {
      /*
       * When streaming into tar, we want
       * to have the plain directory. Thus we cannot
       * reference a single file here. ArchivePipeProcess
       * should protect us for doing the wrong thing here (tm).
       */
      std::shared_ptr<ArchivePipedProcess> myfile
        = std::make_shared<ArchivePipedProcess>(this->streaming_subdir);
      std::string directory = myfile->getFilePath();

      myfile->setExecutable("tar");
      myfile->pushExecArgument("-C");
      myfile->pushExecArgument(directory);
      myfile->pushExecArgument("-x");
      myfile->pushExecArgument("-f");
      myfile->pushExecArgument("-");

      return myfile;
      break;
    }

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

std::string ArchiveLogDirectory::XLogPrevFileByRecPtr(XLogRecPtr recptr,
                                                      unsigned int timeline,
                                                      unsigned long long wal_segment_size) {

  unsigned int logSegNo;
  char fname[MAXFNAMELEN];

#if PG_VERSION_NUM < 110000
  XLByteToPrevSeg(recptr, logSegNo);
  XLogFileName(fname, timeline, logSegNo);
#else
  XLByteToPrevSeg(recptr, logSegNo, wal_segment_size);
  XLogFileName(fname, timeline, logSegNo, wal_segment_size);
#endif

  return string(fname);

}


string ArchiveLogDirectory::XLogFileByRecPtr(XLogRecPtr recptr,
                                             unsigned int timeline,
                                             unsigned long long wal_segment_size) {

  unsigned int logSegNo;
  char fname[MAXFNAMELEN];

#if PG_VERSION_NUM < 110000
  XLByteToSeg(recptr, logSegNo);
  XLogFileName(fname, timeline, logSegNo);
#else
  XLByteToSeg(recptr, logSegNo, wal_segment_size);
  XLogFileName(fname, timeline, logSegNo, wal_segment_size);
#endif

  return string(fname);

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
    case WAL_SEGMENT_TLI_HISTORY_FILE:
    case WAL_SEGMENT_TLI_HISTORY_FILE_COMPRESSED:
      /*
       * TLI history files aren't interesting here.
       */
      continue;
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

    XLogRecPtr recptr = InvalidXLogRecPtr;
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
    XLogSegNoOffsetToRecPtr(segmentNumber, 0, xlogsegsize, recptr);
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

void ArchiveLogDirectory::removeXLogs(shared_ptr<BackupCleanupDescr> cleanupDescr,
                                      unsigned long long wal_segment_size) {

  /* TLI=0 doesn't exist, so take this as a starting value */
  unsigned int lowest_tli = 0;

  if (cleanupDescr == nullptr) {
    throw CArchiveIssue("physical cleanup of WAL files requires a valid cleanup descriptor");
  }

  /* We can't operate on WAL_CLEANUP_RANGE */
  if (cleanupDescr->mode == WAL_CLEANUP_RANGE) {
    throw CArchiveIssue("cannot operate on ranges during XLOG cleanup");
  }

  /*
   * If the cleanup descriptor is initialized to NO_WAL_TO_DELETE we
   * refuse to delete any XLOG segment files. Instead throw an error.
   */
  if (cleanupDescr->mode == NO_WAL_TO_DELETE) {
    throw CArchiveIssue("removing XLOG segment files requires a valid starting XLOG position");
  }

  /*
   * Loop through the offset list, getting an idea which is the oldest
   * timeline in there. We need this information later to determine wether
   * this TLI is somehow reachable.
   *
   * XXX: This is just a weak guess, since it doesn't mean that this
   *      TLI is really reachable from a later TLI from any
   *      basebackup. Though it prevents us from removing any
   *      XLOG segments when there are still older TLIs around (without
   *      any basebackups requiring it) and thus diggin' holes into
   *      the WAL stream unnecessarily.
   *
   * XXX: Incorporate HISTORY files here.
   */
  for (auto const &offset_item : cleanupDescr->off_list) {

    /* nothing done yet */
    if (lowest_tli == 0) {
      lowest_tli = offset_item.first;
    }

    if (offset_item.first < lowest_tli) {
      lowest_tli = offset_item.first;
    }

  };

  /*
   * Establish directory iterator.
   */
  for(auto & entry : boost::make_iterator_range(directory_iterator(this->getPath()),
                                                {})) {

    std::string direntname     = entry.path().filename().string();
    WALSegmentFileStatus fstat = WAL_SEGMENT_UNKNOWN;

#ifdef __DEBUG_XLOG__
    cerr << "DEBUG XLOG: examining file: " << direntname << endl;
#endif

    /*
     * Retrieve the XLogRecPtr from this segment file.
     */
    fstat = determineXlogSegmentStatus(entry.path());

    /*
     * unlink() is just called on real WAL segment and TLI history files.
     * All other files are left untouched.
     */
    switch(fstat) {
    case WAL_SEGMENT_COMPLETE:
    case WAL_SEGMENT_COMPLETE_COMPRESSED:
    case WAL_SEGMENT_PARTIAL:
    case WAL_SEGMENT_PARTIAL_COMPRESSED:
    case WAL_SEGMENT_TLI_HISTORY_FILE:
    case WAL_SEGMENT_TLI_HISTORY_FILE_COMPRESSED:
      {
        /*
         * Extract the TLI and segment number of this segment file and
         * calculate the *starting* XLogRecPtr into it. If this
         * XLogRecPtr is lower than the requested deletion threshold
         * we unlink() the segment immediately.
         */
        unsigned int xlog_tli = 0;
        unsigned int xlog_segno = 0;
        XLogRecPtr recptr = InvalidXLogRecPtr;
        tli_cleanup_offsets::iterator it;

#if PG_VERSION_NUM < 110000
        XLogFromFileName(direntname.c_str(), &xlog_tli, &xlog_segno);
        XLogSegNoOffsetToRecPtr(xlog_segno, 0, recptr);
#else
        XLogFromFileName(direntname.c_str(), &xlog_tli,
                         &xlog_segno, wal_segment_size);
        XLogSegNoOffsetToRecPtr(xlog_segno, 0, wal_segment_size, recptr);
#endif

        /*
         * Get the offset for the specified timeline from
         * the cleanup descriptor. If no offset can be found, then
         * this means that the cleanup descriptor didn't see this
         * during the retention initialization.
         *
         * In this case, if the timeline is in the past (so lower
         * than any encountered timeline), we drop the XLOG segment,
         * since there's no basebackup depending on it.
         *
         * If the encountered XLogRecPtr is on a timeline seen
         * during retention initialization, we check wether the cleanup_start
         * pos (which is the starting point from where we are going to
         * remove XLOG segment files from the archive) is *equal* or *smaller*
         * than the XLOG segment starting offset retrieved above.
         *
         * If true, then this means that the current XLOG segment file
         * is older and can be removed.
         */
        it = cleanupDescr->off_list.find(xlog_tli);

        if ((it == cleanupDescr->off_list.end()) && (lowest_tli > xlog_tli)) {

          /*
           * TLI not seen in basebackup list and current segment
           * has older TLI.
           */
          remove(entry.path());

        } else if ( (it->first == xlog_tli)
                    && (recptr <= (it->second)->wal_cleanup_start_pos) ) {

#ifdef __DEBUG__
          cerr << "XLogRecPtr is older than requested position("
               << PGStream::encodeXLOGPos(recptr)
               << "), deleting file "
               << direntname
               << endl;
#endif
          remove(entry.path());

        }

        break;
      }
    default:
      continue;
    }
  }

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
   * e) TLI history file (timeline switch)
   * f) compressed TLI history file (timeline switch)
   */
  WALSegmentFileStatus filestatus = WAL_SEGMENT_UNKNOWN;

  const regex filter_complete("[0-9A-F]*");
  const regex filter_complete_compressed("[0-9A-F]*.gz");
  const regex filter_partial("[0-9A-F]*.partial");
  const regex filter_partial_compressed("[0-9A-F]*.partial.gz");
  const regex filter_tli_history_file("[0-9A-F]*.history");
  const regex filter_tli_history_file_compressed("[0-9A-F]*.history.gz");

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
  } else if (regex_match(xlogfilename, what, filter_tli_history_file)) {
    filestatus = WAL_SEGMENT_TLI_HISTORY_FILE;
  } else if (regex_match(xlogfilename, what, filter_tli_history_file_compressed)) {
    filestatus = WAL_SEGMENT_TLI_HISTORY_FILE_COMPRESSED;
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

void ArchiveLogDirectory::checkCleanupDescriptor(std::shared_ptr<BackupCleanupDescr> cleanupDescr) {

  if (cleanupDescr == nullptr)
    throw CArchiveIssue("cannot identify wal deletion points without basebackup data");

  /*
   * We expect cleanup descriptors here being initialized
   * with BASEBACKUP_DELETE.
   */
  if (cleanupDescr->basebackupMode != BASEBACKUP_DELETE)
    throw CArchiveIssue("expected cleanup mode must be set to DROP for WAL segment cleanup");

  /*
   * WAL cleanup mode WAL_CLEANUP_RANGE currently not implemented.
   */
  if (cleanupDescr->mode == WAL_CLEANUP_RANGE)
    throw CArchiveIssue("WAL cleanup with RANGE mode currently not implemented");

  if (cleanupDescr->basebackups.size() == 0) {

    /*
     * Nothing to do, set the deletion mode accordingly.
     */
    cleanupDescr->mode = NO_WAL_TO_DELETE;

#ifdef __DEBUG__
    cerr << "no basebackups to identify WAL cleanup offset found" << endl;
#endif

    return;

  }

}

/******************************************************************************
 * BackupDirectory Implementation
 ******************************************************************************/

string BackupDirectory::verificationCodeAsString(BaseBackupVerificationCode code) {

  string code_str;

  switch(code) {

  case BASEBACKUP_OK:
    code_str = "OK";
    break;

  case BASEBACKUP_ABORTED:
    code_str = "ABORTED";
    break;

  case BASEBACKUP_IN_PROGRESS:
    code_str = "IN PROGRESS";
    break;

  case BASEBACKUP_START_WAL_MISSING:
    code_str = "WAL START POSITION MISSING";
    break;

  case BASEBACKUP_END_WAL_MISSING:
    code_str = "WAL END_POSITION MISSING";
    break;

  case BASEBACKUP_DIRECTORY_MISSING:
    code_str = "BASEBACKUP DIRECTORY MISSING";
    break;

  case BASEBACKUP_DESCR_INVALID:
    code_str = "BASEBACKUP CATALOG DESCRIPTOR INVALID";
    break;

  case BASEBACKUP_DIRECTORY_MISMATCH:
    code_str = "BASEBACKUP CATALOG DIRECTORY AND ON DISK DIRECTORY NOT IDENTICAL";
    break;

  case BASEBACKUP_GENERIC_VERIFICATION_FAILURE:
    code_str = "GENERIC BASEBACKUP VERIFICATION FAILURE";
    break;

  default:
    code_str = "N/A";
    break;

  }

  return code_str;
}

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

void BackupDirectory::fsync_recursive(path handle) {

  if (is_directory(handle)) {

    vector<path> listing;

    copy(directory_iterator(handle), directory_iterator(), std::back_inserter(listing));

    for (vector<path>::const_iterator it(listing.begin()), it_end(listing.end()); it != it_end; ++it) {
      path subhandle = *it;

      /*
       * fsync() contents of current directory, call
       * fsync_worker() recursively on subdir.
       */
      try {
        BackupDirectory::fsync(handle);
      } catch(CArchiveIssue &e) {
        /* ignore any errors while recursing */
      }

      BackupDirectory::fsync_recursive(subhandle);
    }

  } else if (is_regular_file(handle)) {

    ArchiveFile file(handle);

    file.setOpenMode("r");

    try {
      file.open();
      file.fsync();
      file.close();
    } catch (CArchiveIssue &e) {
      /*
       * Ignore any errors while recursing, but don't
       * leak possible opened file descriptors
       */
      if (file.isOpen())
        file.close();
    }

  }

}

void BackupDirectory::unlink_path(path backup_path) {

  /* backup_path should exist */
  if (!exists(backup_path)) {

    ostringstream oss;

    oss << "backup path " << backup_path.string() << " cannot be deleted: does not exist";
    throw CArchiveIssue(oss.str());

  }

  remove_all(backup_path);

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

    /* Should be a valid descriptor here, make sure it's closed */
    close(dh);

    throw CArchiveIssue(oss.str());
  }

  close(dh);
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

  if (file_ext.string() == ".gz") {
    this->setCompressed(true);
  }

  if (file_ext.string() == ".bz2") {
    this->setCompressed(true);
  }

  if (file_ext.string() == ".zstd") {
    this->setCompressed(true);
  }

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

ArchivePipedProcess::ArchivePipedProcess(path pathHandle) : BackupFile(pathHandle) {}

ArchivePipedProcess::ArchivePipedProcess(path pathHandle,
                                         string executable,
                                         vector<string> execArgs)
  : BackupFile(pathHandle) {

  this->jobDescr.executable = executable;
  this->jobDescr.execArgs = execArgs;
  this->fpipe_handle = NULL;

  /*
   * ArchivePipedProcess is allowed to reference a
   * directory here. Check this and save this
   * information.
   */
  if (is_directory(pathHandle)) {
    this->path_is_directory = true;
  }

}

void ArchivePipedProcess::setExecutable(path executable,
                                        bool error_if_not_exists) {

  if (error_if_not_exists) {
    if (!boost::filesystem::exists(executable)) {
      ostringstream oss;
      oss << "executable "
          << executable.string()
          << " for piped process does not exist";
      throw CArchiveIssue(oss.str());
    }
  }

  this->jobDescr.executable = executable;

}

void ArchivePipedProcess::pushExecArgument(std::string arg) {

  if (arg.length() > 0)
    this->jobDescr.execArgs.push_back(arg);

}

void ArchivePipedProcess::remove() {

  int rc;

  if (this->path_is_directory) {
    throw CArchiveIssue("deleting a directory with a piped archive handle is not allowed");
  }

  /*
   * Remove operation is supported in case the specified
   * file is not open.
   */
  if (this->isOpen()) {
    CArchiveIssue("cannot unlink opened file when used within a pipe");
  }

  rc = unlink(this->handle.string().c_str());

  if (rc != 0) {
    std::ostringstream oss;
    oss << "cannot unlink file \"" << this->handle.string().c_str() << "\""
        << " (errno) " << errno;
    throw CArchiveIssue(oss.str());
  }

}

ArchivePipedProcess::~ArchivePipedProcess() {

  if (this->fpipe_handle != NULL) {
    pclose(this->fpipe_handle);
  }

}

bool ArchivePipedProcess::isOpen() {

  return (this->fpipe_handle != NULL) && this->opened;

}

bool ArchivePipedProcess::writeable() {

  /*
   * We need to test all various
   * configurations for write modes. If any
   * other configurations are found, this is treated
   * read-only.
   */
  if (this->mode == "w"
      || this->mode == "wb"
      || this->mode == "w+"
      || this->mode == "wb+") {
    return true;
  }

  return false;

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
   *   the specified binary with exec() (via run_pipelined_command())
   *
   * - establish pipe communication with the pipe
   *
   * The latter is dependent on the open mode which
   * was passed to the instance. We treat everything we
   * cannot interpret via writeable() as read-only.
   *
   */

  /*
   * Initialize job handle
   */
  this->jobDescr.background_exec = true;

  if (this->writeable())
    this->jobDescr.po_mode = "w";
  else
    this->jobDescr.po_mode = "r";

  /*
   * Do the fork()
   *
   * run_pipelined_command() returns the PID of the forked
   * process that actually execute the binary. This also replaces
   * the child process via execve(). So check if we are the parent
   * and, to be schizo, make sure we exit the child in any case.
   */
  this->fpipe_handle = run_pipelined_command(this->jobDescr);

  if (this->fpipe_handle != NULL) {

    /*
     * Seems everything worked so far...
     */
    this->opened = true;

  } else {
    /* oops, something went wrong here */
    throw CArchiveIssue("could not fork piped command");
  }

}

size_t ArchivePipedProcess::write(const char *buf, size_t len) {

  size_t result = 0;

  if (this->path_is_directory) {
    throw CArchiveIssue("write into a piped archive handle with a directory is not supported");
  }

  if (!this->isOpen()) {
    std::ostringstream oss;

    oss << "could not write into pipe with process "
        << this->jobDescr.executable.string()
        << ", file "
        << this->handle.string();
    throw CArchiveIssue(oss.str());
  }

  /*
   * Check if the background process supports
   * read.
   */
  if (this->jobDescr.po_mode != "w") {
    throw CArchiveIssue("cannot write to piped archive file in read-only mode");
  }

  /*
   * Write directly into the writing end of our internal
   * pipe, if successfully opened. Since we use the popen()
   * API via run_pipelined_command(), we use fwrite().
   */
  if ((result = fwrite(buf, len, 1, this->fpipe_handle)) != 1) {
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

size_t ArchivePipedProcess::read(char *buf, size_t len) {

  size_t result = 0;

  if (this->path_is_directory) {
    throw CArchiveIssue("write into a piped archive handle with a directory is not supported");
  }

  if (!this->isOpen()) {
    std::ostringstream oss;

    oss << "could not read from pipe with process "
        << this->jobDescr.executable.string()
        << ", file "
        << this->handle.string();
    throw CArchiveIssue(oss.str());
  }

  /*
   * Check if the background process supports
   * read.
   */
  if (this->jobDescr.po_mode != "r") {
    throw CArchiveIssue("cannot read from piped archive file in write-only mode");
  }

  if ((result = fread(buf, len, 1, this->fpipe_handle)) != 1) {

    if ((::feof(this->fpipe_handle) == 0)
        || (::ferror(this->fpipe_handle) != 0)) {
      std::ostringstream oss;
      oss << "read error for file (size="
          << len
          << ")"
          << this->handle.string()
          << ": "
          << strerror(errno);
      throw CArchiveIssue(oss.str());
    }


  }

  if (result > 0)
    this->currpos += len;


  return result;

}

off_t ArchivePipedProcess::lseek(off_t offset, int whence) {
  throw CArchiveIssue("piped I/O operation doesn't support seek");
}

void ArchivePipedProcess::setOpenMode(string mode) {
  this->mode = mode;
}

void ArchivePipedProcess::fsync() {

  if (this->path_is_directory) {
    ArchiveFile afile(this->handle);

    afile.open();
    afile.fsync();
    afile.close();
  } else {

  }

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

    ::pclose(this->fpipe_handle);
    this->fpipe_handle = NULL;
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
