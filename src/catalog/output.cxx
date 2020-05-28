#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <vector>

#include <common.hxx>
#include <fs-archive.hxx>
#include <output.hxx>

using namespace credativ;

/* ****************************************************************************
 * Implementation of OutputFormatter
 * ****************************************************************************/

OutputFormatter::OutputFormatter(std::shared_ptr<OutputFormatConfiguration> config,
                                 std::shared_ptr<BackupCatalog> catalog,
                                 std::shared_ptr<CatalogDescr> catalog_descr) {

  /* We throw immediately in case catalog is uninitalized */
  if (catalog == nullptr)
    throw CCatalogIssue("undefined catalog handle not allowed in formatter instance");

  /* We throw immediately in case catalog wasn't opened */
  if (!catalog->available())
    throw CCatalogIssue("formatter instances expect catalog handler to be opened");

  /* We throw immediately if there's no valid catalog archive handle */
  if (catalog_descr == nullptr)
    throw CCatalogIssue("formatter instances need a valid catalog archive handle");

  /* We throw immediatly if there's no valid output config */
  if (config == nullptr)
    throw CCatalogIssue("formatter instances need a valid format config");

  this->catalog = catalog;
  this->catalog_descr = catalog_descr;
  this->config = config;

}

OutputFormatter::~OutputFormatter() {}

std::shared_ptr<OutputFormatter> OutputFormatter::formatter(std::shared_ptr<OutputFormatConfiguration> config,
                                                            std::shared_ptr<BackupCatalog> catalog,
                                                            std::shared_ptr<CatalogDescr> catalog_descr,
                                                            OutputFormatType type) {

  std::shared_ptr<OutputFormatter> formatter = nullptr;

  /* We throw immediately in case catalog is uninitalized */
  if (catalog == nullptr)
    throw CCatalogIssue("undefined catalog handle not allowed in formatter instance");

  /* We throw immediately in case catalog wasn't opened */
  if (!catalog->available())
    throw CCatalogIssue("formatter instances expect catalog handler to be opened");

  /* We throw immediately if there's no valid catalog archive handle */
  if (catalog_descr == nullptr)
    throw CCatalogIssue("formatter instances need a valid catalog archive handle");

  /* We throw immediatly if there's no valid output config */
  if (config == nullptr)
    throw CCatalogIssue("formatter instances need a valid format config");

  switch(type) {

  case OUTPUT_CONSOLE:

    formatter = std::make_shared<ConsoleOutputFormatter>(config, catalog, catalog_descr);
    break;

  case OUTPUT_JSON:

    formatter = std::make_shared<JsonOutputFormatter>(config, catalog, catalog_descr);
    break;

  }

  return formatter;

}

/* ****************************************************************************
 * Implementation of JsonOutputFormatter
 * ****************************************************************************/

ConsoleOutputFormatter::ConsoleOutputFormatter(std::shared_ptr<OutputFormatConfiguration> config,
                                               std::shared_ptr<BackupCatalog> catalog,
                                               std::shared_ptr<CatalogDescr> catalog_descr)
  : OutputFormatter(config, catalog, catalog_descr) {}

ConsoleOutputFormatter::~ConsoleOutputFormatter() {}

void ConsoleOutputFormatter::listBackupsVerbose(std::vector<std::shared_ptr<BaseBackupDescr>> &list,
                                                std::ostringstream &output) {

  /*
   * Format the output.
   */
  output << CPGBackupCtlBase::makeHeader("Basebackups in archive " + catalog_descr->archive_name,
                                       boost::format("%-20s\t%-60s") % "Property" % "Value",
                                       80);

  for (auto &basebackup : list) {

    size_t upstream_total_size = 0;

    /*
     * Directory handle for basebackup directory on disk.
     */
    StreamingBaseBackupDirectory directory(path(basebackup->fsentry).filename().string(),
                                           catalog_descr->directory);

    /*
     * Details for the backup profile used by the current basebackup.
     */
    shared_ptr<BackupProfileDescr> backupProfile
      = this->catalog->getBackupProfile(basebackup->used_profile);

    /*
     * Verify state of the current basebackup.
     */
    BaseBackupVerificationCode bbstatus
      = StreamingBaseBackupDirectory::verify(basebackup);

    output << CPGBackupCtlBase::makeLine(boost::format("%-20s\t%-60s")
                                       % "ID" % basebackup->id);
    output << CPGBackupCtlBase::makeLine(boost::format("%-20s\t%-60s")
                                       % "Pinned" % ( (basebackup->pinned == 0) ? "NO" : "YES" ));
    output << CPGBackupCtlBase::makeLine(boost::format("%-20s\t%-60s")
                                       % "Backup" % basebackup->fsentry);
    output << CPGBackupCtlBase::makeLine(boost::format("%-20s\t%-60s")
                                       % "Catalog State" % basebackup->status);
    output << CPGBackupCtlBase::makeLine(boost::format("%-20s\t%-60s")
                                       % "Label" % basebackup->label);
    output << CPGBackupCtlBase::makeLine(boost::format("%-20s\t%-60s")
                                       % "WAL segment size" % basebackup->wal_segment_size);
    output << CPGBackupCtlBase::makeLine(boost::format("%-20s\t%-60s")
                                       % "Started" % basebackup->started);
    output << CPGBackupCtlBase::makeLine(boost::format("%-20s\t%-60s")
                                       % "Timeline" % basebackup->timeline);
    output << CPGBackupCtlBase::makeLine(boost::format("%-20s\t%-60s")
                                       % "WAL start" % basebackup->xlogpos);
    output << CPGBackupCtlBase::makeLine(boost::format("%-20s\t%-60s")
                                       % "WAL stop" % basebackup->xlogposend);
    output << CPGBackupCtlBase::makeLine(boost::format("%-20s\t%-60s")
                                       % "System ID" % basebackup->systemid);
    output << CPGBackupCtlBase::makeLine(boost::format("%-20s\t%-60s")
                                       % "Used Backup Profile" % backupProfile->name);

    /*
     * Print tablespace information belonging to the current basebackup
     */
    output << CPGBackupCtlBase::makeHeader("tablespaces",
                                         boost::format("%-20s\t%-60s")
                                         % "tablespace property"
                                         % "value", 80);

    for (auto &tablespace : basebackup->tablespaces) {

      /* Check for parent tablespace (also known as pg_default) */
      if (tablespace->spcoid == 0) {

        output << " - " << boost::format("%-20s\%-60s")
          % "upstream location" % "pg_default" << std::endl;
        output << " - " << boost::format("%-20s\%-60s")
          % "upstream size" % tablespace->spcsize << std::endl;

      } else {

        output << " - " << boost::format("%-20s\%-60s")
          % "oid" % tablespace->spcoid << std::endl;
        output << " - " << boost::format("%-20s\%-60s")
          % "upstream location" % tablespace->spclocation << std::endl;
        output << " - " << boost::format("%-20s\%-60s")
          % "upstream size" % tablespace->spcsize << std::endl;

      }

      upstream_total_size += tablespace->spcsize;
    }

    output << "Summary:" << std::endl;
    output << boost::format("%-25s\t%-40s")
      % "Total size upstream:" % CPGBackupCtlBase::prettySize(upstream_total_size * 1024) << std::endl;

    /*
     * Display the state and local size of the basebackup. If the basebackup
     * doesn't exist anymore, print a warning instead.
     */
    if (bbstatus != BASEBACKUP_OK) {

      output << boost::format("%-25s\t%-40s")
        % CPGBackupCtlBase::stdout_red("Backup status (ON-DISK):", true)
        % CPGBackupCtlBase::stdout_red(BackupDirectory::verificationCodeAsString(bbstatus), true) << std::endl;

      output << boost::format("%-25s\t%-40s")
        % "Total local backup size:"
        % "NOT AVAILABLE" << std::endl;


    } else {

      output << boost::format("%-25s\t%-40s")
        % "Backup duration" % basebackup->duration << std::endl;

      output << boost::format("%-25s\t%-40s")
        % CPGBackupCtlBase::stdout_green("Backup status (ON-DISK):", true)
        % CPGBackupCtlBase::stdout_green(BackupDirectory::verificationCodeAsString(bbstatus), true)
             << std::endl;

      output << boost::format("%-25s\t%-40s")
        % "Total local backup size:"
        % CPGBackupCtlBase::prettySize(directory.size()) << std::endl;

    }

    output << CPGBackupCtlBase::makeLine(80) << std::endl;
    output << std::endl;

  }

}

void ConsoleOutputFormatter::listBackups(std::vector<std::shared_ptr<BaseBackupDescr>> &list,
                                         std::ostringstream &output) {

  output << CPGBackupCtlBase::makeHeader("Basebackups in archive " + catalog_descr->archive_name,
                                         boost::format("%-5s\t%-35s\t%-40s") % "ID" % "Backup" % "Size",
                                         80);

  for (auto &basebackup : list) {

    /*
     * Verify state of the current basebackup.
     */
    BaseBackupVerificationCode bbstatus
      = StreamingBaseBackupDirectory::verify(basebackup);
    std::string status = "";

    /*
     * Directory handle for basebackup directory on disk.
     */
    StreamingBaseBackupDirectory directory(path(basebackup->fsentry).filename().string(),
                                           catalog_descr->directory);

    /*
     * Transform basebackup status into its string representation.
     */
    if (bbstatus != BASEBACKUP_OK) {

      status = CPGBackupCtlBase::stdout_red(BackupDirectory::verificationCodeAsString(bbstatus), true);

      output << boost::format("%-5s\t%-35s\t%-40s")
        % basebackup->id
        % basebackup->fsentry
        % "N/A"
           << std::endl;

    } else {

      status = CPGBackupCtlBase::stdout_green(BackupDirectory::verificationCodeAsString(bbstatus), true);

      output << boost::format("%-5s\t%-35s\t%-40s")
        % basebackup->id
        % basebackup->fsentry
        % CPGBackupCtlBase::prettySize(directory.size())
           << std::endl;

    }

    /* Print compact list of basebackups */

    output << "- Details" << std::endl;

    /* Duration */
    output << boost::format("\t%-20s\t%-60s")
      % "Duration" % basebackup->duration
         << std::endl;

    /* Datetime basebackup started */
    output << boost::format("\t%-20s\t%-60s")
      % "Started"
      % basebackup->started
         << std::endl;

    output << boost::format("\t%-20s\t%-60s")
      % "Stopped"
      % basebackup->stopped
         << std::endl;

    output << boost::format("\t%-20s\t%-60s")
      %" Status"
      % status
         << std::endl;

    output << CPGBackupCtlBase::makeLine(80);
    output << std::endl;

  }

}

void ConsoleOutputFormatter::nodeAs(std::vector<std::shared_ptr<BaseBackupDescr>> &list,
                                    std::ostringstream &output) {

  bool verbose;

  config->get("list_backups.verbose")->getValue(verbose);

  if (verbose) {

    this->listBackupsVerbose(list, output);

  } else {

    this->listBackups(list, output);

  }

}

/* ****************************************************************************
 * Implementation of JsonOutputFormatter
 * ****************************************************************************/

JsonOutputFormatter::JsonOutputFormatter(std::shared_ptr<OutputFormatConfiguration> config,
                                         std::shared_ptr<BackupCatalog> catalog,
                                         std::shared_ptr<CatalogDescr> catalog_descr)
  : OutputFormatter(config, catalog, catalog_descr) {}

JsonOutputFormatter::~JsonOutputFormatter() {}

void JsonOutputFormatter::listBackups(std::vector<std::shared_ptr<BaseBackupDescr>> &list,
                                 std::ostringstream &output) {

  namespace pt = boost::property_tree;

  pt::ptree head;
  pt::ptree basebackups;
  std::ostringstream oss;

  oss << list.size();

  head.put("num_basebackups", oss.str());

  BOOST_FOREACH(const std::shared_ptr<BaseBackupDescr> &descr, list) {

    StreamingBaseBackupDirectory directory(path(descr->fsentry).filename().string(),
                                           catalog_descr->directory);
    pt::ptree bbackup;

    oss.str(std::string());
    oss << descr->id;

    bbackup.put("id", oss.str());
    bbackup.put("fsentry", descr->fsentry);
    bbackup.put("started", descr->started);
    bbackup.put("stopped", descr->stopped);
    bbackup.put("size", directory.size());
    bbackup.put("status",
                BackupDirectory::verificationCodeAsString(StreamingBaseBackupDirectory::verify(descr)));

    basebackups.push_back(std::make_pair("", bbackup));
  }

  head.add_child("basebackups", basebackups);

  pt::write_json(output, head);

}

void JsonOutputFormatter::listBackupsVerbose(std::vector<std::shared_ptr<BaseBackupDescr>> &list,
                                             std::ostringstream &output) {

}

void JsonOutputFormatter::nodeAs(std::vector<std::shared_ptr<BaseBackupDescr>> &list,
                                 std::ostringstream &output) {

  bool verbose;

  config->get("list_backups.verbose")->getValue(verbose);

  if (verbose) {

    this->listBackupsVerbose(list, output);

  } else {

    this->listBackups(list, output);

  }

}
