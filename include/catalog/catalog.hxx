#ifndef __CATALOG__
#define __CATALOG__

#define CATALOG_MAGIC 108

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

/* XXX: The following are just "virtual" columns used
 *      by most of queries used to retrieve and update archive
 *      information. Most of the time we want to have
 *      database connections joined with current archive
 *      data.
 */
#define SQL_ARCHIVE_PGHOST_ATTNO 4
#define SQL_ARCHIVE_PGPORT_ATTNO 5
#define SQL_ARCHIVE_PGUSER_ATTNO 6
#define SQL_ARCHIVE_PGDATABASE_ATTNO 7
#define SQL_ARCHIVE_DSN_ATTNO 8

/*
 * Keep that in sync with above number of cols
 */
#define SQL_ARCHIVE_NCOLS 4

/*
 * Backup catalog entity
 */
#define SQL_BACKUP_ENTITY 2

/*
 * Attributes belonging to backup catalog entity
 */
#define SQL_BACKUP_ID_ATTNO 0
#define SQL_BACKUP_ARCHIVE_ID_ATTNO 1
#define SQL_BACKUP_XLOGPOS_ATTNO 2
#define SQL_BACKUP_XLOGPOSEND_ATTNO 3
#define SQL_BACKUP_TIMELINE_ATTNO 4
#define SQL_BACKUP_LABEL_ATTNO 5
#define SQL_BACKUP_FSENTRY_ATTNO 6
#define SQL_BACKUP_STARTED_ATTNO 7
#define SQL_BACKUP_STOPPED_ATTNO 8
#define SQL_BACKUP_PINNED_ATTNO 9
#define SQL_BACKUP_STATUS_ATTNO 10
#define SQL_BACKUP_SYSTEMID_ATTNO 11
#define SQL_BACKUP_WAL_SEGMENT_SIZE_ATTNO 12
#define SQL_BACKUP_USED_PROFILE_ATTNO 13
#define SQL_BACKUP_PG_VERSION_NUM_ATTNO 14

/*
 * Computed columns with no corresponding
 * materialized attribute. These are additional
 * attributes fetched by catalog queries into
 * a BaseBackupDescr. They must not be counted
 * below in SQL_BACKUP_NCOLS!
 */
#define SQL_BACKUP_COMPUTED_DURATION 15
#define SQL_BACKUP_COMPUTED_RETENTION_DATETIME 16

/*
 * Keep that in sync with above number of cols
 */
#define SQL_BACKUP_NCOLS 15

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

/*
 * Attributes belonging to backup_profiles catalog table.
 */
#define SQL_BACKUP_PROFILES_ENTITY 4

#define SQL_BCK_PROF_ID_ATTNO 0
#define SQL_BCK_PROF_NAME_ATTNO 1
#define SQL_BCK_PROF_COMPRESS_TYPE_ATTNO 2
#define SQL_BCK_PROF_MAX_RATE_ATTNO 3
#define SQL_BCK_PROF_LABEL_ATTNO 4
#define SQL_BCK_PROF_FAST_CHKPT_ATTNO 5
#define SQL_BCK_PROF_INCL_WAL_ATTNO 6
#define SQL_BCK_PROF_WAIT_FOR_WAL_ATTNO 7
#define SQL_BCK_PROF_NOVERIFY_CHECKSUMS_ATTNO 8
#define SQL_BCK_PROF_MANIFEST_ATTNO 9
#define SQL_BCK_PROF_MANIFEST_CHECKSUMS_ATTNO 10

/*
 * Keep number of columns in sync with above definitions
 */
#define SQL_BACKUP_PROFILES_NCOLS 11

/*
 * Attributes belonging to backup_tablespaces catalog table.
 */
#define SQL_BACKUP_TBLSPC_ENTITY 5

#define SQL_BCK_TBLSPC_BCK_ID_ATTNO 0
#define SQL_BCK_TBLSPC_SPCOID_ATTNO 1
#define SQL_BCK_TBLSPC_SPCLOC_ATTNO 2
#define SQL_BCK_TBLSPC_SPCSZ_ATTNO 3

/*
 * Keep number of columns in sync with above definitions
 */
#define SQL_BCK_TBLSPC_NCOLS 4

/*
 * Attributes belonging to procs catalog table.
 */
#define SQL_PROCS_ENTITY 6

#define SQL_PROCS_PID_ATTNO 0
#define SQL_PROCS_ARCHIVE_ID_ATTNO 1
#define SQL_PROCS_TYPE_ATTNO 2
#define SQL_PROCS_STARTED_ATTNO 3
#define SQL_PROCS_STATE_ATTNO 4
#define SQL_PROCS_SHM_KEY_ATTNO 5
#define SQL_PROCS_SHM_ID_ATTNO 6

/*
 * Keep number of columns in sync with above definitions
 */
#define SQL_PROCS_NCOLS 7

/*
 * connections catalog entry
 */
#define SQL_CON_ENTITY 7

#define SQL_CON_ARCHIVE_ID_ATTNO 0
#define SQL_CON_TYPE_ATTNO 1
#define SQL_CON_DSN_ATTNO 2
#define SQL_CON_PGHOST_ATTNO 3
#define SQL_CON_PGPORT_ATTNO 4
#define SQL_CON_PGUSER_ATTNO 5
#define SQL_CON_PGDATABASE_ATTNO 6

/*
 * Keep number of columns in sync with above definitions
 */
#define SQL_CON_NCOLS 7

/*
 * retention catalog entry
 */
#define SQL_RETENTION_ENTITY 8

#define SQL_RETENTION_ID_ATTNO 0
#define SQL_RETENTION_NAME_ATTNO 1
#define SQL_RETENTION_CREATED_ATTNO 2

/*
 * Keep number of columns in sync with above definitions
 */
#define SQL_RETENTION_NCOLS 3

/*
 * retention rules catalog entity
 */
#define SQL_RETENTION_RULES_ENTITY 9

#define SQL_RETENTION_RULES_ID_ATTNO 0
#define SQL_RETENTION_RULES_TYPE_ATTNO 1
#define SQL_RETENTION_RULES_VALUE_ATTNO 2

/*
 * Keep number of columns in sync with above definitions
 */
#define SQL_RETENTION_RULES_NCOLS 3

#endif
