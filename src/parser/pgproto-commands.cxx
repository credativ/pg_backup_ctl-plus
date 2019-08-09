#include <iostream>
#include <boost/log/trivial.hpp>

#include <pgsql-proto.hxx>
#include <pgproto-commands.hxx>

#include <BackupCatalog.hxx>

using namespace credativ;
using namespace credativ::pgprotocol;


/* ****************************************************************************
 * PGProtoStreamingCommand base class
 * ***************************************************************************/

PGProtoStreamingCommand::PGProtoStreamingCommand(std::shared_ptr<PGProtoCmdDescr> descr,
                                                 std::shared_ptr<RuntimeConfiguration> rtc) {

  if (descr != nullptr) {
    this->command_handle = descr;
  }

  if (rtc != nullptr) {
    runtime_configuration = rtc;
  } else {
    runtime_configuration = std::make_shared<RuntimeConfiguration>();
  }

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

/* ****************************************************************************
 * PGProtoIdentifySystem command ... IDENTIFY_SYSTEM
 * ***************************************************************************/

PGProtoIdentifySystem::PGProtoIdentifySystem(std::shared_ptr<PGProtoCmdDescr> descr,
                                             std::shared_ptr<RuntimeConfiguration> rtc)
  : PGProtoStreamingCommand(descr, rtc) {

  command_tag = "IDENTIFY_SYSTEM";

}

PGProtoIdentifySystem::~PGProtoIdentifySystem() {}

void PGProtoIdentifySystem::execute() {

}

void PGProtoIdentifySystem::reset() {

  current_step = 0;

}

int PGProtoIdentifySystem::step(ProtocolBuffer &buffer) {

  return -1;

}

/* ****************************************************************************
 * LIST_BASEBACKUPS command ... LIST_BASEBACKUPS
 * ***************************************************************************/

PGProtoListBasebackups::PGProtoListBasebackups(std::shared_ptr<PGProtoCmdDescr> descr,
                                               std::shared_ptr<RuntimeConfiguration> rtc)
  : PGProtoStreamingCommand(descr, rtc) {

  command_tag = "LIST_BASEBACKUPS";

}

PGProtoListBasebackups::~PGProtoListBasebackups() {}

void PGProtoListBasebackups::reset() {

  current_step = 0;
  resultSet = nullptr;

}

int PGProtoListBasebackups::step(ProtocolBuffer &buffer) {

  switch(current_step) {

  case 0:
    {

      /*
       * First call, aggregate the RowDescription message
       * into the buffer
       */
      resultSet->descriptor(buffer);
      current_step = PGProtoResultSet::PGPROTO_ROW_DESCR_MESSAGE;

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO row descriptor message created";
      break;

    }

  case PGProtoResultSet::PGPROTO_ROW_DESCR_MESSAGE:
  case PGProtoResultSet::PGPROTO_DATA_DESCR_MESSAGE:
    {

      /*
       * Row descriptor already stacked, aggregate
       * the DataRow message.
       */
      if (resultSet->data(buffer) <= 0) {

        /* No data row, we're done */
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
   * Loop through the list. We only consider valid basebackups here, since
   * the command is supposed to inform the caller which basebackups are valid
   * to be used for recovery.
   */
  for(auto &it : list) {

    std::ostringstream converter;
    std::vector<PGProtoColumnDataDescr> data;
    PGProtoColumnDataDescr colvalue;

    resultSet->addColumn("id",
                         0,
                         0,
                         PGProtoColumnDescr::PG_TYPEOID_TEXT,
                         sizeof(int),
                         0);

    converter << it->id;

    colvalue.length = converter.str().length();
    colvalue.data   = converter.str();
    converter.clear();

    resultSet->addColumn("fsentry",
                         0,
                         0,
                         PGProtoColumnDescr::PG_TYPEOID_TEXT,
                         sizeof(int),
                         0);

    colvalue.length = it->fsentry.length();
    colvalue.data   = it->fsentry;
    converter.clear();

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

  /* Check wether we're having a valid archive id...   */
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
