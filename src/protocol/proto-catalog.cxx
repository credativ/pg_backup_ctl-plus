#include <boost/log/trivial.hpp>

#include <proto-catalog.hxx>
#include <memory>

using namespace credativ;
using namespace credativ::pgprotocol;

PGProtoCatalogHandler::PGProtoCatalogHandler(std::string catalog_name) {

  /*
   * Create internal backup catalog instance and open it
   *
   * NOTE: std::make_unique is C++14, but we still have C++11....:(
   */
  catalog = std::unique_ptr<BackupCatalog>(new BackupCatalog(catalog_name));

}

PGProtoCatalogHandler::PGProtoCatalogHandler(std::string catalog_name,
                                             std::string basebackup_fqfn,
                                             int archive_id,
                                             int worker_id,
                                             int child_id,
                                             std::shared_ptr<WorkerSHM> shm) {

  /*
   * Create internal backup catalog instance and open it
   *
   * NOTE: std::make_unique is C++14, but we still rely on C++11....:(
   */
  catalog = std::unique_ptr<BackupCatalog>(new BackupCatalog(catalog_name));

  /*
   * Attach this catalog handler to the specified archive/basebackup.
   */
  attach(basebackup_fqfn, archive_id, worker_id, child_id, shm);

  this->worker_id = worker_id;
  this->child_id  = child_id;

}

PGProtoCatalogHandler::~PGProtoCatalogHandler() {

  if (catalog->opened())
    catalog->close();

  /* should dispose catalog handle */
  catalog = nullptr;

}

string PGProtoCatalogHandler::getCatalogFullname() {

  return catalog->fullname();

}

std::shared_ptr<BaseBackupDescr> PGProtoCatalogHandler::attach(std::string basebackup_fqfn,
                                                               int archive_id,
                                                               int worker_id,
                                                               int &child_id,
                                                               std::shared_ptr<WorkerSHM> shm) {

  if (archive_id < 0) {

    std::ostringstream oss;

    oss << "invalid archive ID \"" << archive_id << "\"";
    throw CCatalogIssue(oss.str());

  }

  /*
   * worker_id and child_id must be valid.
   */
  if (worker_id < 0) {
    throw CCatalogIssue("cannot attach basebackup with invalid worker_id");
  }

  if (child_id < 0) {
    throw CCatalogIssue("cannot attach basebackup with invalid child_id");
  }

  /*
   * Worker SHM must be valid and attached.
   */
  if (shm == nullptr) {

    throw CCatalogIssue("cannot attach basebackup to undefined shared memory segment");

  }

  if (shm->get_shmid() < 0) {

    throw CCatalogIssue("cannot register basebackup to detached shared memory segment");

  }

  attached_basebackup = catalog->getBaseBackup(basebackup_fqfn, archive_id);

  /*
   * If successful, register the basebackup into our shared
   * memory segment.
   */
  if (isAttached()) {

    sub_worker_info child_info;

    /* Employ a mutex to avoid race conditions between read() and write(). */
    WORKER_SHM_CRITICAL_SECTION_START_P(shm);

    child_info = shm->read(worker_id, child_id);
    child_info.backup_id = attached_basebackup->id;
    shm->write(worker_id, child_id, child_info);

    WORKER_SHM_CRITICAL_SECTION_END;

  } else {

    throw CCatalogIssue("error attaching basebackup in recovery instance");

  }

  /*
   * Remember worker and child ID.
   */
  this->worker_id = worker_id;
  this->child_id  = child_id;

  return attached_basebackup;

}

void PGProtoCatalogHandler::detach(unsigned int worker_id,
                                   unsigned int child_id,
                                   std::shared_ptr<WorkerSHM> shm) {

  /* Fast exit in case nothing attached to */
  if (shm == nullptr || !isAttached()) return;

  /*
   * Erase locked backup_id from shared memory.
   */
  if (isAttached()) {

    sub_worker_info child_info;

    WORKER_SHM_CRITICAL_SECTION_START_P(shm);

    shm->detach_basebackup(worker_id, child_id);

    WORKER_SHM_CRITICAL_SECTION_END;

    attached_basebackup = nullptr;

  }

}

bool PGProtoCatalogHandler::isAttached() {

  return ( (attached_basebackup != nullptr)
           && (attached_basebackup->id >= 0)
           && (attached_basebackup->status == "ready") );

}

void PGProtoCatalogHandler::queryTimelineHistory(std::shared_ptr<PGProtoResultSet> set,
                                                 unsigned int tli) {

  shared_ptr<BackupDirectory> backupDir      = nullptr;
  shared_ptr<ArchiveLogDirectory> archiveDir = nullptr;
  shared_ptr<CatalogDescr> catalogDescr      = nullptr;
  stringstream history_content;
  vector<PGProtoColumnDataDescr> row_data;
  PGProtoColumnDataDescr colvalue[2];

  if (!isAttached()) {
    throw CCatalogIssue("uninitialized catalog handler without a basebackup");
  }

  /* TLI <= 1 makes no sense here, so guard against them. */
  if (tli <= 1) {
    throw CArchiveIssue("timeline history id <= 1 doesn't allocate history files");
  }

  /*
   * Get a backup directory handle.
   */
  backupDir = make_shared<BackupDirectory>(catalog->fullname());
  archiveDir = backupDir->logdirectory();

  /*
   * Archive our basebackup is attached to. We need this to
   * get the catalog parent directory for basebackups.
   */
  catalogDescr = catalog->existsById(attached_basebackup->archive_id);

  /*
   * We got a valid descriptor?
   */
  if (catalogDescr->id < 0) {
    throw CCatalogIssue("could not get a valid catalog descriptor for attached basebackup");
  }

  /*
   * Make a backup directory handle. This handle gives access
   * to the underlying archive log directory.
   */
  backupDir = make_shared<BackupDirectory>(path(catalogDescr->directory));
  archiveDir =  backupDir->logdirectory();

  /*
   * Open the TLI history file.
   */
  history_content= archiveDir->readHistoryFile(tli,
                                               catalogDescr->compression);

  BOOST_LOG_TRIVIAL(debug) << "reading history file for tli="
                           << tli
                           << " done";

  /*
   * Get an archive log directory handle.
   */
  /*
   * The TIMELINE_HISTORY PostgreSQL Streaming Replication command
   * returns the filename and the content of the requested timeline
   * history file in one single row.
   */
  set->addColumn("filename",
                 0,
                 0,
                 PGProtoColumnDescr::PG_TYPEOID_TEXT,
                 -1,
                 0);

  set->addColumn("content",
                 0,
                 0,
                 PGProtoColumnDescr::PG_TYPEOID_BYTEA,
                 -1,
                 0);

  colvalue[0].data
    = ArchiveLogDirectory::timelineHistoryFilename(tli,
                                                   catalogDescr->compression);
  colvalue[0].length = colvalue[0].data.length();

  colvalue[1].data = history_content.str();
  colvalue[1].length = colvalue[1].data.length();

  row_data.push_back(colvalue[0]);
  row_data.push_back(colvalue[1]);

  set->addRow(row_data);

}

void PGProtoCatalogHandler::queryIdentifySystem(std::shared_ptr<PGProtoResultSet> set) {

  vector<PGProtoColumnDataDescr> row_data;
  PGProtoColumnDataDescr colvalue[4];
  ostringstream converter;

  if (!isAttached()) {
    throw CCatalogIssue("uninitialized catalog handler without a basebackup");
  }

  /*
   * IDENTIFY_SYSTEM needs the following information:
   *
   * - systemid
   * - timeline TLI
   * - xlogpos, position of the basebackup archive
   * - dbname, in our case the basebackup full qualified filename
   */
  set->addColumn("systemid",
                 0,
                 0,
                 PGProtoColumnDescr::PG_TYPEOID_TEXT,
                 -1,
                 0);

  set->addColumn("timeline",
                 0,
                 0,
                 PGProtoColumnDescr::PG_TYPEOID_TEXT,
                 -1,
                 0);

  set->addColumn("xlogpos",
                 0,
                 0,
                 PGProtoColumnDescr::PG_TYPEOID_TEXT,
                 -1,
                 0);

  set->addColumn("dbname",
                 0,
                 0,
                 PGProtoColumnDescr::PG_TYPEOID_TEXT,
                 -1,
                 0);
  /*
   * Add column data for systemid.
   */
  colvalue[0].length = attached_basebackup->systemid.length();
  colvalue[0].data   = attached_basebackup->systemid;

  /*
   * ...and timeline
   */
  converter << attached_basebackup->timeline;
  colvalue[1].length = converter.str().length();
  colvalue[1].data   = converter.str();

  /*
   * ...and xlogpos
   */
  colvalue[2].length = attached_basebackup->xlogpos.length();
  colvalue[2].data   = attached_basebackup->xlogpos;

  /*
   * ...and finally the dbname aka basebackup full qualified filename.
   */
  colvalue[3].length = attached_basebackup->fsentry.length();
  colvalue[3].data   = attached_basebackup->fsentry;

  /* Add the row data */
  row_data.push_back(colvalue[0]);
  row_data.push_back(colvalue[1]);
  row_data.push_back(colvalue[2]);
  row_data.push_back(colvalue[3]);

  set->addRow(row_data);

}
