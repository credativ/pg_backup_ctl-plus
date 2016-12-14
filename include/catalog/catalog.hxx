#ifndef __CATALOG__
#define __CATALOG__

#define CATALOG_MAGIC 0x1000

/*
 * Archive catalog entity
 */
#define SQL_ARCHIVE_ENTITY 1

/*
 * Attributes belonging to archive catalog entity
 */
#define SQL_ARCHIVE_ID_ATTNO 0
#define SQL_ARCHIVE_NAME_ATTNO 1
#define SQL_ARCHIVE_DIRECTORY_ATTNO 2
#define SQL_ARCHIVE_COMPRESSION_ATTNO 3
#define SQL_ARCHIVE_PGHOST_ATTNO 4
#define SQL_ARCHIVE_PGPORT_ATTNO 5
#define SQL_ARCHIVE_PGUSER_ATTNO 6
#define SQL_ARCHIVE_PGDATABASE_ATTNO 7

/*
 * Keep that in sync with above number of cols
 */
#define SQL_ARCHIVE_NCOLS 8

/*
 * Backup catalog entity
 */
#define SQL_BACKUP_ENTITY 2

/*
 * Attributes belonging to backup catalog entity
 */
#define SQL_BACKUP_ID_ATTNO 0
#define SQL_BACKUP_ARCHIVE_ID_ATTNO 1
#define SQL_BACKUP_HISTORY_FILENAME_ATTNO 2
#define SQL_BACKUP_LABEL_ATTNO 3
#define SQL_BACKUP_STARTED_ATTNO 4
#define SQL_BACKUP_STOPPED_ATTNO 5
#define SQL_BACKUP_PINNED_ATTNO 6
#define SQL_BACKUP_STATUS 7

/*
 * Keep that in sync with above number of cols
 */
#define SQL_BACKUP_NCOLS 8

/*
 * Attributes belong to stream tablex
 */
#define SQL_STREAM_ENTITY 3

#define SQL_STREAM_ID_ATTNO 0
#define SQL_STREAM_ARCHIVE_ID_ATTNO 1
#define SQL_STREAM_STYPE_ATTNO 2
#define SQL_STREAM_SLOT_NAME_ATTNO 3
#define SQL_STREAM_SYSTEMID_ATTNO 4
#define SQL_STREAM_TIMELINE_ATTNO 5
#define SQL_STREAM_XLOGPOS_ATTNO 6
#define SQL_STREAM_DBNAME_ATTNO 7
#define SQL_STREAM_STATUS_ATTNO 8
#define SQL_STREAM_REGISTER_DATE_ATTNO 9

/*
 * Keep number of columns in sync with above definitions
 */
#define SQL_STREAM_NCOLS 10

#endif
