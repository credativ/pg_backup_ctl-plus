#ifndef __HAVE_BACKUPCLEANUP_DESCR_
#define __HAVE_BACKUPCLEANUP_DESCR_

namespace pgbckctl {

  /**
   * Define WAL cleanup modes.
   */
  typedef enum {

    WAL_CLEANUP_RANGE,
    WAL_CLEANUP_OFFSET,
    WAL_CLEANUP_ALL,
    NO_WAL_TO_DELETE

  } WALCleanupMode;

  /**
   * Cleanup basebackup list mode.
   *
   * Can be either BACKUP_KEEP or BACKUP_DELETE
   */
  typedef enum {

    NO_BASEBACKUPS,
    BASEBACKUP_KEEP,
    BASEBACKUP_DELETE

  } BasebackupCleanupMode;

  /**
   * A structure describing the XLogRecPtr
   * cleanup threshold and the timelines which
   * it belongs to.
   */
  class xlog_cleanup_off_t {
  public:

    unsigned int timeline;
    unsigned int wal_segment_size;
    XLogRecPtr wal_cleanup_start_pos = InvalidXLogRecPtr;
    XLogRecPtr wal_cleanup_end_pos   = InvalidXLogRecPtr;

  };

  typedef std::map<unsigned int,
                   std::shared_ptr<xlog_cleanup_off_t>> tli_cleanup_offsets;

  /**
   * A BackupCleanupDescr descriptor instance describes
   * which basebackups and WAL segment ranges can be evicted
   * from the archive. It carries a list of basebackup descriptors
   * which is identifying the basebackups to delete or to keep.
   *
   * The newest basebackup is the first in the vector, the older one
   * is the last. The cleanup descriptor also maintains a XLogRecPtr
   * offset or range, depending on the deletion mode specified in
   * the property mode.
   *
   * This identifies the starting (or ending) location of WAL segments which are
   * safe to delete from the archive. Please note that this XLogRecPtr doesn't
   * necessarily belong to the list of basebackups currently elected
   * for eviction from the archive, but might have been influenced
   * by a basebackup to keep or which was pinned before.
   *
   */
  class BackupCleanupDescr {
  public:

    std::vector<std::shared_ptr<BaseBackupDescr>> basebackups;
    BasebackupCleanupMode basebackupMode = BASEBACKUP_KEEP;

    /* List if TLI/XLOG cleanup offset items */
    tli_cleanup_offsets off_list;

    WALCleanupMode mode = NO_WAL_TO_DELETE;

  };
}

#endif
