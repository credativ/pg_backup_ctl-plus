#include <common.hxx>
#include <backup.hxx>
#include <boost/log/trivial.hpp>

using namespace pgbckctl;

Backup::Backup(const std::shared_ptr<CatalogDescr>& descr) {}

Backup::~Backup() {};

TransactionLogBackup::TransactionLogBackup(const std::shared_ptr<CatalogDescr>& descr) : Backup(descr) {

  this->descr = descr;

}

TransactionLogBackup::~TransactionLogBackup() {

  if (this->isInitialized()) {
    this->finalize();
    delete this->directory;
  }

}

bool TransactionLogBackup::isInitialized() {
  return this->initialized;
}

void TransactionLogBackup::create() {

  if (this->isInitialized()) {
    /*
     * Transaction log segments are always stored
     * within the log/ subdirectory under the parent
     * archive directory. Usually these directory
     * already exists, but make sure ...
     */
    this->directory->create();
  } else {
    throw CArchiveIssue("cannot call create on uninitialized transaction log directory handle");
  }

}

XLogRecPtr TransactionLogBackup::write(XLOGDataStreamMessage *message,
                                       XLogRecPtr &flush_position,
                                       unsigned int timeline) {

  shared_ptr<TransactionLogListItem> item = nullptr;
  size_t message_written = 0;
  size_t message_left    = 0;
  int waloffset = 0;
  char *databuf;
  XLogRecPtr position = InvalidXLogRecPtr;

  /*
   * Default value for flush_position is
   * InvalidXLogRecPtr, indicating no flush so far.
   */
  flush_position = InvalidXLogRecPtr;

  /*
   * If not initialized, error out.
   */
  if (!this->isInitialized())
    throw CArchiveIssue("attempt to write into uninitialized TransactionLogBackup handle");

  /*
   * If message isn't valid, throw an exception.
   */
  if (message == nullptr) {
    throw CArchiveIssue("could not write uninitialized XLOG data message");
  }

  /*
   * Calculate WAL position and offsets. The start position
   * for the current write is obtained from the WAL position in this record.
   */
  position = message->getXLOGStartPos();
  waloffset = PGStream::XLOGOffset(position, this->wal_segment_size);
  databuf   = message->buffer();
  message_left = message->dataBufferSize();

  /*
   * Check if there is a stacked WAL segment file.
   */
  if (this->fileList.empty()) {

    /*
     * We need to allocate a new WAL segment file. Before, we
     * do a sanity check, whether the current waloffset really
     * starts at the beginning of a WAL segment.
     *
     * This is the same approach, pg_receivewal does during streaming
     * and it seems sane to adapt this here, since like pg_receivewal,
     * we also rely on segment starting position on startup.
     *
     * We fall through this clause in case everything
     * is positioned as expected.
     */
    if (waloffset != 0) {
      std::ostringstream oss;

      oss << "requested new WAL segment file, but WAL offset( "
          << waloffset
          << ") is beyond WAL segment start";
      throw CArchiveIssue(oss.str());
    }

  } else {

    /*
     * Initialize the item pointer to the current
     * WAL segment file. Check whether it's position is
     * where we expect it.
     */
    item = this->fileList.back();

    if (item->fileHandle->current_position() != waloffset) {
      std::ostringstream oss;
      oss << "unexpected position "
          << item->fileHandle->current_position()
          << ", expected "
          << waloffset;
      throw CArchiveIssue(oss.str());
    }

  }



  while (message_left > 0) {

    /* bytes to write into current WAL segment */
    size_t bw;

    /*
     * The data block here might cross WAL segment
     * boundary. Check that, otherwise we need to stack
     * a new transaction log segment.
     */
    if ( (waloffset + message_left) > this->wal_segment_size ) {

      /*
       * Calculate the offset until the end of the current
       * WAL segment
       */
      bw = this->wal_segment_size - waloffset;

    } else {

      /*
       * No boundary crossed, so remaining
       * bytes are just as-is
       */
      bw = message_left;

    }

    /*
     * If current WAL segment wasn't initialized
     * yet, get a new one (e.g. we crossed a WAL segment
     * boundary.
     */
    if (item == nullptr) {
      this->stackFile(this->walfilename(timeline,
                                        position) + ".partial");
      item = this->fileList.back();
    }

    /*
     * Attempt to write the data message block ...
     *
     * NOTE:
     *
     * write() will fail with an exception in case the
     * requested block could not be written.
     */
    item->fileHandle->write(databuf + message_written,
                            bw);

    /*
     * Mark them being unsynced
     */
    item->sync_pending = item->flush_pending = true;

    /*
     * Calculate next offset to write from...
     */
    message_written += bw;
    message_left -= bw;
    position += bw;
    waloffset += bw;

    /*
     * We have advanced the XLogRecPtr from this XLOG data
     * message block to the new position. We need now to check
     * if the boundary of the current WAL file is reached. If true,
     * finalize the current XLOG segment file and stack a new one.
     */
    if (PGStream::XLOGOffset(position, this->wal_segment_size) == 0) {

      this->finalizeCurrentWALFile(true);

      /*
       * From this point, there shouldn't anything pending,
       * make sure we flushed everything to disk. Note that
       * this will also clear all open XLOG segment files
       * from the internal file list (atm there should always
       * be one, but in the future me might queue them up to some
       * point).
       */
      this->finalize();

#ifdef __DEBUG_XLOG__
      BOOST_LOG_TRIVIAL(debug) << "DEBUG: finalize XLOG segment at offset "
                               << PGStream::encodeXLOGPos(position);
#endif

      /*
       * Remember flush position.
       */
      flush_position = position;

      /*
       * Count synced WAL file.
       */
      this->wal_synced++;

      /*
       * Flag current item handler to be empty,
       * next loop will create a new WAL log segment file.
       */
      item = nullptr;

      /*
       * Reset wal offset to be a starting point again.
       */
      waloffset = 0;

    }

  }

  return position;
}

uint64_t TransactionLogBackup::countSynced() {

  return this->wal_synced;

}

std::string TransactionLogBackup::walfilename(unsigned int timeline,
                                              XLogRecPtr position) {

  char xlogfilename[MAXPGPATH];
  XLogSegNo segment_number ;
  string result;

#if PG_VERSION_NUM < 110000

  XLByteToSeg(position,
              segment_number);

  XLogFileName(xlogfilename,
               timeline,
               segment_number);
#else
  XLByteToSeg(position,
              segment_number,
              wal_segment_size);

  XLogFileName(xlogfilename,
               timeline,
               segment_number,
               wal_segment_size);
#endif

  result = string(xlogfilename);
  return result;
}

std::shared_ptr<BackupFile> TransactionLogBackup::current_segment_file() {

  std::shared_ptr<BackupFile> result = nullptr;
  std::shared_ptr<TransactionLogListItem> item = nullptr;

  if (this->fileList.empty())
    return result;

  item = this->fileList.back();
  return item->fileHandle;

}

void TransactionLogBackup::finalize() {

  /*
   * Sync all stacked file handles. Close all
   * files.
   *
   * NOTE: we call sync_pending() here, which already does all
   *       the necessary synchronisation here.
   */
  this->sync_pending();

  for (auto &item : this->fileList) {
    item->fileHandle->close();
  }

  this->fileList.clear();

}

void TransactionLogBackup::initialize() {

  if (!this->isInitialized()) {

    /* WAL segment size must be greater than 0 and
     * a power of two! */
    if ( (this->wal_segment_size == 0)
         || ( (this->wal_segment_size % 2) != 0) ) {
      std::ostringstream oss;
      oss << "invalid configured wal segment size("
          << this->wal_segment_size
          << ") "
          << "in transaction log backup handle: ";
      throw CArchiveIssue(oss.str());
    }

    this->directory = new BackupDirectory(path(this->descr->directory));
    this->logDirectory = this->directory->logdirectory();
    this->initialized = true;
  }

}

void TransactionLogBackup::setWalSegmentSize(uint32_t wal_segment_size) {

  if (this->isInitialized())
    throw CArchiveIssue("cannot change WAL segment size on initialized transaction log backup handle");

  this->wal_segment_size = wal_segment_size;

}

std::string TransactionLogBackup::backupDirectoryString() {
  return ((ArchiveLogDirectory *)this->directory)->getPath().string();
}

void TransactionLogBackup::sync_pending() {

  for (auto &item : this->fileList) {
    if (item->sync_pending ||
        item->flush_pending) {
      item->fileHandle->fsync();
      item->sync_pending = item->flush_pending = false;
    }
  }

  /*
   * Make sure directory meta information is also
   * synced.
   */
  this->directory->fsync();
}

void TransactionLogBackup::flush_pending() {
  /* currently a no-op, call sync_pending instead */
  this->sync_pending();
}

void TransactionLogBackup::finalizeCurrentWALFile(bool forceWalSegSz) {

  shared_ptr<TransactionLogListItem> item = nullptr;
  path finalName;

  /*
   * Check if there is a currently stacked file...
   */
  if (this->fileList.empty()) {
    throw CArchiveIssue("cannot finalize current file in transaction log backup: no file stacked");
  }

  item = this->fileList.back();

  /*
   * Rename the XLOG segment into final name without .partial suffix, but
   * only if we reached the end of the current WAL file.
   */
  if (item->fileHandle->current_position() == this->wal_segment_size) {

    finalName = change_extension(item->fileHandle->getFilePath(), "");

    /*
     * The current XLOG segment file is opened wb+, since we
     * always want to write from the beginning when streaming is started.
     *
     * Change its open mode during rename(), since we then switch
     * to read-only in this case. This also prevent rename() from truncating
     * the file during rename(), since it wants to reopen the file with its
     * new name.
     */
    item->fileHandle->setOpenMode("r+");

    /* Do the rename() now ... */
    item->fileHandle->rename(finalName);

    /*
     * rename() already synced the file, so no need to sync it again.
     */
    item->sync_pending = item->flush_pending = false;

  }

  if ( forceWalSegSz && (item->fileHandle->size() != this->wal_segment_size) ) {
    std::ostringstream oss;

    oss << "could not finalize current WAL segment: unexpected seek location at "
        << item->fileHandle->current_position();
    throw CArchiveIssue(oss.str());
  }

}

std::shared_ptr<BackupFile> TransactionLogBackup::stackFile(std::string name) {

  std::shared_ptr<TransactionLogListItem> logref = std::make_shared<TransactionLogListItem>();

  /*
   * Stack a new transaction log segment file into
   * current pending file list.
   */
  if (!this->isInitialized()) {
    throw CArchiveIssue("cannot create transaction log backup file: not initialized");
  }

  /* Allocate new segment file handle */
  this->file = this->directory->walfile(name, this->compression);
  this->file->setOpenMode("wb+");
  this->file->open();

  /*
   * Make sure, we start at the beginning of the file.
   */
  this->file->lseek(0, SEEK_SET);

  /*
   * If we are in an uncompressed mode, pad the file.
   */
  // if (this->compression == BACKUP_COMPRESS_TYPE_NONE) {

  //   char *zerobuffer = new char[XLOG_BLCKSZ];
  //   int   wbytes = 0; /* bytes written */

  //   memset(zerobuffer, 0, XLOG_BLCKSZ);

  //   for (wbytes = 0; i < this->wal_segment_size; wbytes += XLOG_BLCKSZ) {

  //     this->file->write(
  //   }
  // }

  /*
   * Stack walfile reference into open file list.
   */
  logref->fileHandle = file;
  logref->filename = name;
  logref->sync_pending = true;
  logref->flush_pending = true;

  this->fileList.push_back(logref);
  return this->file;

}

StreamBaseBackup::StreamBaseBackup(const std::shared_ptr<CatalogDescr>& descr)
  : Backup(descr) {

  this->descr = descr;
  this->identifier = this->createMyIdentifier();
  this->mode = SB_NOT_SET;

}

StreamBaseBackup::StreamBaseBackup(const std::shared_ptr<CatalogDescr>& descr,
                                   StreamDirectoryOperationMode mode)
  : Backup(descr) {

  this->descr = descr;
  this->identifier = this->createMyIdentifier();
  this->mode = mode;

}

void StreamBaseBackup::setCompression(BackupProfileCompressType compression) {
  this->compression = compression;
}

BackupProfileCompressType StreamBaseBackup::getCompression() {
  return this->compression;
}

StreamBaseBackup::~StreamBaseBackup() {

  if (this->isInitialized()) {
    this->finalize();
    delete this->directory;
  }

};

void StreamBaseBackup::setMode(StreamDirectoryOperationMode mode) {

  /*
   * We allow changing operation mode on
   * uninitialized instances!
   */
  if (this->isInitialized())
    throw CArchiveIssue("cannot change stream directory operation mode if already initialized");

  this->mode = mode;

}

StreamDirectoryOperationMode StreamBaseBackup::getMode() {

  return this->mode;

}

std::string StreamBaseBackup::backupDirectoryString() {

  if (this->isInitialized()) {
    return ((StreamingBaseBackupDirectory *)this->directory)->getPath().string();
  } else {
    return std::string("");
  }

}

std::string StreamBaseBackup::createMyIdentifier() {

  return "streambackup-"
    + CPGBackupCtlBase::current_timestamp(true);

}

void StreamBaseBackup::create() {

  if (this->mode == SB_READ) {
    CArchiveIssue("stream backup is in read-only mode");
  }

  if (this->isInitialized()) {
    /*
     * A streamed base backup is hosted within a
     * subdirectory in <ARCHIVEDIR>/base. Create a new one but
     * create() will throw a CArchiveIssue exception in case
     * it already exists.
     */
    this->directory->create();
  } else {
    throw CArchiveIssue("cannot call create on uninitialized streaming directory handle");
  }

}

std::string StreamBaseBackup::read() {
  return string("");
}

void StreamBaseBackup::finalize() {

  if (this->mode == SB_WRITE) {

    /*
     * Sync file handles and their contents.
     */
    for(auto& item : this->fileList) {
      item->fsync();
      item->close();
    }

    /*
     * Sync directory handle
     */
    this->directory->fsync();

    /*
     * Clear internal handles.
     */
    this->file = nullptr;
    this->fileList.clear();


  } else {

    /* SB_READ: nothing to do here yet */

  }

  this->mode = SB_NOT_SET;

}

bool StreamBaseBackup::isInitialized() {
  return (this->initialized && (this->mode != SB_NOT_SET));
}

void StreamBaseBackup::initialize() {

  if (this->mode == SB_NOT_SET) {
    throw CArchiveIssue("cannot initialize stream basebackup handler without operation mode");
  }

  if (!this->isInitialized()) {
    this->directory = new StreamingBaseBackupDirectory(this->identifier,
                                                       path(this->descr->directory));
    this->initialized = true;
  }

}

std::shared_ptr<BackupFile> StreamBaseBackup::stackFile(std::string name) {

  if (!this->isInitialized()) {
    throw CArchiveIssue("cannot create stream backup files: not initialized");
  }

  /*
   * Allocate a new basebackup file. This will overwrite
   * the last used file reference.
   */
  this->file = this->directory->basebackup(name, this->compression);
  this->file->setOpenMode("wb");
  this->file->open();

  /*
   * Stack this reference into the private file stack.
   */
  this->fileList.push_back(this->file);
  return this->file;

}
