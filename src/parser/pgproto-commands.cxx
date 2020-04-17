#include <iostream>
#include <boost/log/trivial.hpp>

#include <pgproto-commands.hxx>
#include <proto-catalog.hxx>

using namespace credativ;
using namespace credativ::pgprotocol;


/* ****************************************************************************
 * PGProtoStreamingCommand base class
 * ***************************************************************************/

PGProtoStreamingCommand::PGProtoStreamingCommand(std::shared_ptr<PGProtoCmdDescr> descr,
                                                 std::shared_ptr<PGProtoCatalogHandler> catalogHandler,
                                                 std::shared_ptr<RuntimeConfiguration> rtc,
                                                 std::shared_ptr<WorkerSHM> worker_shm) {

  if (descr != nullptr) {
    this->command_handle = descr;
  } else {
    throw PGProtoCmdFailure("could not execute command with undefined command descriptor");
  }

  if (rtc != nullptr) {
    runtime_configuration = rtc;
  } else {
    runtime_configuration = std::make_shared<RuntimeConfiguration>();
  }

  if (descr != nullptr) {
    this->catalogHandler = catalogHandler;
  } else {
    throw PGProtoCmdFailure("could not execute command with undefined catalog handler");
  }

  this->worker_shm = worker_shm;

}

PGProtoStreamingCommand::~PGProtoStreamingCommand() {}

std::string PGProtoStreamingCommand::tag() {

  return command_tag;

}

void PGProtoStreamingCommand::openCatalog(bool readwrite) {

  std::string catalog_name;

  /* Get the catalog name from runtime configuration */
  runtime_configuration->get("recovery_instance.catalog_name")->getValue(catalog_name);

  if (!boost::filesystem::exists(path(catalog_name))) {
    throw PGProtoCmdFailure("catalog does not exist");
  }

  /* Now instantiate and open the backup catalog database */
  catalog = std::make_shared<BackupCatalog>(catalog_name);

  if (readwrite)
    catalog->open_rw();
  else
    catalog->open_ro();

  BOOST_LOG_TRIVIAL(debug) << "catalog database " << catalog_name << " opened";

}

bool PGProtoStreamingCommand::needsArchive() {

  return needs_archive_access;

}

int PGProtoStreamingCommand::step(ProtocolBuffer &buffer) {

  switch(current_step) {

  case 0:
    {

      /* First call, materialize RowDescriptor message. */
      resultSet->descriptor(buffer);
      current_step = PGProtoResultSet::PGPROTO_ROW_DESCR_MESSAGE;

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO row descriptor message created";
      break;

    }

  case PGProtoResultSet::PGPROTO_ROW_DESCR_MESSAGE:
  case PGProtoResultSet::PGPROTO_DATA_DESCR_MESSAGE:
    {

      /*
       * Row descriptor already materialized, create
       * the DataRow message now.
       */
      if (resultSet->data(buffer) <= 0) {

        /* no more data, we're done */
        current_step = -1;

        BOOST_LOG_TRIVIAL(debug) << "PG PROTO data row message end";
        break;
      }

      current_step = PGProtoResultSet::PGPROTO_DATA_DESCR_MESSAGE;

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO data row message sent";
      break;

    }

  }

  return current_step;

}

/* ****************************************************************************
 * PGProtoIdentifySystem command ... IDENTIFY_SYSTEM
 * ***************************************************************************/

PGProtoIdentifySystem::PGProtoIdentifySystem(std::shared_ptr<PGProtoCmdDescr> descr,
                                             std::shared_ptr<PGProtoCatalogHandler> catalogHandler,
                                             std::shared_ptr<RuntimeConfiguration> rtc,
                                             std::shared_ptr<WorkerSHM> worker_shm)
  : PGProtoStreamingCommand(descr, catalogHandler, rtc, worker_shm) {

  command_tag = "IDENTIFY_SYSTEM";

  current_step = 0;
  needs_archive_access = true;

}

PGProtoIdentifySystem::~PGProtoIdentifySystem() {}

void PGProtoIdentifySystem::execute() {

  BOOST_LOG_TRIVIAL(debug) << "IDENTIFY_SYSTEM: using catalog \""
                           << catalogHandler->getCatalogFullname()
                           << "\"";

  resultSet = std::make_shared<PGProtoResultSet>();

  /*
   * Finally execute the query and materialize the result set.
   */
  catalogHandler->queryIdentifySystem(resultSet);

}

void PGProtoIdentifySystem::reset() {

  current_step = 0;

}

/* ****************************************************************************
 * PGProtoTimelineHistory command ... TIMELINE_HISTORY tli
 * ***************************************************************************/

PGProtoTimelineHistory::PGProtoTimelineHistory(std::shared_ptr<PGProtoCmdDescr> descr,
                                               std::shared_ptr<PGProtoCatalogHandler> catalogHandler,
                                               std::shared_ptr<RuntimeConfiguration> rtc,
                                               std::shared_ptr<WorkerSHM> worker_shm)
  : PGProtoStreamingCommand(descr, catalogHandler, rtc, worker_shm) {

  command_tag = "TIMELINE_HISTORY";
  needs_archive_access = true;
  current_step = 0;

}

PGProtoTimelineHistory::~PGProtoTimelineHistory() {}

void PGProtoTimelineHistory::execute() {

  BOOST_LOG_TRIVIAL(debug) << "requesting timeline history file for tli="
                           << command_handle->tli;

  resultSet = make_shared<PGProtoResultSet>();

  /*
   * Catalog handler is responsible to materialize the query result.
   */
  catalogHandler->queryTimelineHistory(resultSet,
                                       command_handle->tli);

}

void PGProtoTimelineHistory::reset() {

  current_step = 0;
  resultSet    = nullptr;

}

/* ****************************************************************************
 * PGProtoListBasebackups command ... LIST_BASEBACKUPS
 * ***************************************************************************/

PGProtoListBasebackups::PGProtoListBasebackups(std::shared_ptr<PGProtoCmdDescr> descr,
                                               std::shared_ptr<PGProtoCatalogHandler> catalogHandler,
                                               std::shared_ptr<RuntimeConfiguration> rtc,
                                               std::shared_ptr<WorkerSHM> worker_shm)
  : PGProtoStreamingCommand(descr, catalogHandler, rtc, worker_shm) {

  command_tag = "LIST_BASEBACKUPS";
  needs_archive_access = false;
  current_step = 0;

}

PGProtoListBasebackups::~PGProtoListBasebackups() {}

void PGProtoListBasebackups::reset() {

  current_step = 0;
  resultSet    = nullptr;

}

void PGProtoListBasebackups::prepareListOfBackups() {

  /* Get list of basebackups */
  std::vector<std::shared_ptr<BaseBackupDescr>> list
    = catalog->getBackupList(archive_descr->archive_name);

  /* Check if a buffer aggregation step() was called before.
   * If true, die hard */
  if (current_step != 0) {
    throw PGProtoCmdFailure("protocol buffer aggregation violation, call reset() before");
  }

  /* Prepare a result set instance */
  resultSet = std::make_shared<PGProtoResultSet>();

  /*
   * Prepare the column list.
   */
  resultSet->addColumn("id",
                       0,
                       0,
                       PGProtoColumnDescr::PG_TYPEOID_TEXT,
                       -1,
                       0);

  resultSet->addColumn("fsentry",
                       0,
                       0,
                       PGProtoColumnDescr::PG_TYPEOID_TEXT,
                       -1,
                       0);

  resultSet->addColumn("started",
                       0,
                       0,
                       PGProtoColumnDescr::PG_TYPEOID_TEXT,
                       -1,
                       0);

  resultSet->addColumn("duration",
                       0,
                       0,
                       PGProtoColumnDescr::PG_TYPEOID_TEXT,
                       -1,
                       0);

  /*
   * Loop through the list. We only consider valid basebackups here, since
   * the command is supposed to inform the caller which basebackups are valid
   * to be used for recovery.
   */
  for(auto &it : list) {

    std::ostringstream converter;
    std::vector<PGProtoColumnDataDescr> data;
    PGProtoColumnDataDescr colvalue;

    converter << it->id;

    colvalue.length = converter.str().length();
    colvalue.data   = converter.str();
    converter.clear();

    /*
     * Add column data to list.
     */
    data.push_back(colvalue);

    colvalue.length = it->fsentry.length();
    colvalue.data   = it->fsentry;

    /*
     * Add column data to list.
     */
    data.push_back(colvalue);

    colvalue.length = it->started.length();
    colvalue.data   = it->started;

    /*
     * Add column data to list.
     */
    data.push_back(colvalue);

    colvalue.length = it->duration.length();
    colvalue.data   = it->duration;

    /*
     * Add column data to list.
     */
    data.push_back(colvalue);

    /*
     * Save the column data list within the result set.
     */
    resultSet->addRow(data);

    BOOST_LOG_TRIVIAL(debug) << "PG PROTO result set row done, currently "
                             << resultSet->rowCount() << " rows in set materialized";

  }

}

void PGProtoListBasebackups::execute() {

  /*
   * Runtime configuration tells us the
   * archive we are connected to.
   */
  int archive_id;

  runtime_configuration->get("recovery_instance.archive_id")->getValue(archive_id);

  BOOST_LOG_TRIVIAL(debug) << "requesting basebackups from archive ID=" << archive_id;

  /* Check whether we're having a valid archive id...   */
  if (archive_id < 0) {

    std::ostringstream oss;

    oss << "invalid archive id " << archive_id;
    throw PGProtoCmdFailure(oss.str());

  }

  try {

    /* Prepare catalog lookups */
    openCatalog();

    /* Lookup archive data */
    archive_descr = catalog->existsById(archive_id);

    BOOST_LOG_TRIVIAL(debug) << "recovery instance attached to archive "
                             << archive_descr->archive_name;

    /* Now do the legwork... */

    prepareListOfBackups();

    catalog->close();

  } catch (CPGBackupCtlFailure &e) {

    if (catalog->opened())
      catalog->close();

    /* re-throw as protocol command failure */
    throw PGProtoCmdFailure(e.what());
  }

}
