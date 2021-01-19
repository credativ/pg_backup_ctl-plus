#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>
#include <vector>

#include <common.hxx>
#include <fs-archive.hxx>
#include <output.hxx>
#include <retention.hxx>

using namespace credativ;

/* ****************************************************************************
 * Implementation of OutputFormatter
 * ****************************************************************************/

OutputFormatter::OutputFormatter(std::shared_ptr<OutputFormatConfiguration> config,
                                 std::shared_ptr<BackupCatalog> catalog,
                                 std::shared_ptr<CatalogDescr> catalog_descr) {

  /*
   * NOTE: We allow a nullptr for catalog_descr, since it's not a hard requirement
   *       for every output format
   */

  /* We throw immediately in case catalog is uninitalized */
  if (catalog == nullptr)
    throw CCatalogIssue("undefined catalog handle not allowed in formatter instance");

  /* We throw immediately in case catalog wasn't opened */
  if (!catalog->available())
    throw CCatalogIssue("formatter instances expect catalog handler to be opened");

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
                                                            OutputFormatType type) {
  return formatter(config,
                   catalog,
                   std::shared_ptr<CatalogDescr>(nullptr),
                   type);

}

std::shared_ptr<OutputFormatter> OutputFormatter::formatter(std::shared_ptr<OutputFormatConfiguration> config,
                                                            std::shared_ptr<BackupCatalog> catalog,
                                                            std::shared_ptr<CatalogDescr> catalog_descr,
                                                            OutputFormatType type) {
  /*
   * NOTE: We allow a nullptr for catalog_descr, since it's not a hard requirement
   *       for every output format
   */

  std::shared_ptr<OutputFormatter> formatter = nullptr;

  /* We throw immediately in case catalog is uninitalized */
  if (catalog == nullptr)
    throw CCatalogIssue("undefined catalog handle not allowed in formatter instance");

  /* We throw immediately in case catalog wasn't opened */
  if (!catalog->available())
    throw CCatalogIssue("formatter instances expect catalog handler to be opened");

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

void OutputFormatter::nodeAs(std::exception &e,
                             std::ostringstream &output,
                             std::string output_type) {

  namespace pt = boost::property_tree;

  if (output_type == "json") {
    pt::ptree head;

    head.put("severity", "error");
    head.put("message", e.what());
    pt::write_json(output, head);
  }

  if (output_type == "console") {
    output << e.what() << endl;
  }

}

/* ****************************************************************************
 * Implementation of ConsoleOutputFormatter
 * ****************************************************************************/

ConsoleOutputFormatter::ConsoleOutputFormatter(std::shared_ptr<OutputFormatConfiguration> config,
                                               std::shared_ptr<BackupCatalog> catalog,
                                               std::shared_ptr<CatalogDescr> catalog_descr)
  : OutputFormatter(config, catalog, catalog_descr) {}

ConsoleOutputFormatter::~ConsoleOutputFormatter() {}

void ConsoleOutputFormatter::nodeAs(std::shared_ptr<std::list<directory_entry>> fileList,
                                    std::ostringstream &output) {

  for(auto item : *fileList) {

    if (is_regular_file(item.path())) {

      output << CPGBackupCtlBase::makeLine(boost::format("object=%60s type=f size=%d")
                                           % item.path() % file_size(item.path()));

    } else if (is_directory(item.path())) {

      output << CPGBackupCtlBase::makeLine(boost::format("object=%60s type=d")
                                           % item.path());

    }

  }

}

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

      output << "---" << std::endl;

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

      output << "---" << std::endl;

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

void ConsoleOutputFormatter::nodeAs(std::vector<std::shared_ptr<ConnectionDescr>> connections,
                                    std::ostringstream &output) {

  /*
   * Print result header
   */
  output << "List of connections for archive \""
         << catalog_descr->archive_name
         << "\"" << endl;

  /*
   * XXX: createArchive() normally ensures that a
   *      catalog connection definition of type 'basebackup'
   *      (CONNECTION_TYPE_BASEBACKUP) exists, at least. But
   *      we don't rely on this fact, just loop through
   *      the results and spill them out...
   *
   *      getCatalogConnection() returns the shared pointers
   *      ordered by its type.
   */

  for (auto & con : connections) {

    /* item header */
    output << CPGBackupCtlBase::makeHeader("connection type " + con->type,
                                           boost::format("%-15s\t%-60s") % "Attribute" % "Setting",
                                           80) << endl;
    output << boost::format("%-15s\t%-60s") % "DSN" % con->dsn << endl;
    output << boost::format("%-15s\t%-60s") % "PGHOST" % con->pghost << endl;
    output << boost::format("%-15s\t%-60s") % "PGDATABASE" % con->pgdatabase << endl;
    output << boost::format("%-15s\t%-60s") % "PGUSER" % con->pguser << endl;
    output << boost::format("%-15s\t%-60s") % "PGPORT" % con->pgport << endl;

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

void ConsoleOutputFormatter::nodeAs(std::vector<shm_worker_area> &slots,
                                    std::ostringstream &output) {

  for(auto &worker : slots) {

    string archive_name = "N/A";

    if (worker.archive_id >= 0) {
      shared_ptr<CatalogDescr> archive_descr = this->catalog->existsById(worker.archive_id);

      if (archive_descr->id >= 0) {
        archive_name = archive_descr->archive_name;
      }
    }

    cout << "WORKER PID " << worker.pid
         << " | executing " << CatalogDescr::commandTagName(worker.cmdType)
         << " | archive name " << archive_name
         << " | archive ID " << worker.archive_id
         << " | started " << CPGBackupCtlBase::ptime_to_str(worker.started)
         << endl;

    /* Print child info, if any */
    for (unsigned int idx = 0; idx < MAX_WORKER_CHILDS; idx++) {

      sub_worker_info child_info = worker.child_info[idx];

      if (child_info.pid > 0) {
        cout << " `-> CHILD "
             << idx
             << " | PID "
             << child_info.pid
             << " | "
             << ((child_info.backup_id < 0) ? "no backup in use" : "backup used: ID="
                 + CPGBackupCtlBase::intToStr(child_info.backup_id))
             << endl;
      }
    }

  }


}

void ConsoleOutputFormatter::nodeAs(std::vector<std::shared_ptr<RetentionDescr>> &retentionList,
                                    std::ostringstream &output) {

    output << CPGBackupCtlBase::makeHeader("List of retention policies",
                                         boost::format("%-10s\t%-30s\t%-25s") % "ID" % "NAME" % "CREATED",
                                         80);

    for (auto retention : retentionList) {

      output << boost::format("%-10s\t%-30s\t%-25s")
        % retention->id % retention->name % retention->created << endl;

      if (retention->rules.size() > 0) {
        output << boost::format("%-80s") % "RULE(s):"
             << endl;
      }

      for (auto rule : retention->rules) {

        shared_ptr<Retention> instance = Retention::get(rule);

        output << CPGBackupCtlBase::makeLine(boost::format("%-10s\t%-70s")
                                           % " - " % instance->asString());
        output << CPGBackupCtlBase::makeLine(80) << endl;

      }

    }

}

void ConsoleOutputFormatter::nodeAs(std::shared_ptr<RetentionDescr> retentionDescr,
                                    std::ostringstream &output) {

  /* Print contents of this policy */

  std::cout << CPGBackupCtlBase::makeHeader("Details of retention policy " + retentionDescr->name,
                                            boost::format("%-15s\t%-60s") % "Attribute" % "Setting",
                                            80) << std::endl;

  std::cout << boost::format("%-15s\t%-60s") % "ID" % retentionDescr->id << std::endl;
  std::cout << boost::format("%-15s\t%-60s") % "NAME" % retentionDescr->name << std::endl;
  std::cout << boost::format("%-15s\t%-60s") % "CREATED" % retentionDescr->created << std::endl;

  std::cout << std::endl;

  for (auto rule : retentionDescr->rules) {

    /*
     * Build a retention object out of this rule to get
     * its decomposed string representation.
     */
    shared_ptr<Retention> instance = Retention::get(rule);
    std::cout << boost::format("%-15s\t%-60s") % "RULE" % instance->asString() << std::endl;

  }

  std::cout << CPGBackupCtlBase::makeLine(80) << std::endl;

}

void ConsoleOutputFormatter::nodeAs(std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> list,
                                    std::ostringstream &output) {

  /*
   * We must distinguish between ARCHIVE_LIST, ARCHIVE_FILTERED_LIST and
   * ARCHIVE_DETAIL_LIST.
   */
  std::string mode;

  config->get("list_archive.mode")->getValue(mode);

  /* ARCHIVE_LIST and ARCHIVE_FILTERED_LIST are basically the same. */
  if (mode == "filtered" || mode == "full") {
    listArchiveList(list, output);
  }

  if (mode == "detail") {
    listArchiveDetail(list, output);
  }

}

void ConsoleOutputFormatter::listArchiveList(std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> list,
                                             std::ostringstream &output) {

  /* Print headline */
  output << CPGBackupCtlBase::makeHeader("Filtered archive list",
                                         boost::format("%-15s\t%-30s") % "Name" % "Directory",
                                         80);

  /*
   * Print archive properties
   */
  for (auto& descr : *list) {
    output << CPGBackupCtlBase::makeLine(boost::format("%-15s\t%-30s")
                                         % descr->archive_name
                                         % descr->directory);
  }

}

void ConsoleOutputFormatter::listArchiveDetail(std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> list,
                                               std::ostringstream &output) {

  /* Print headline */
  output << CPGBackupCtlBase::makeHeader("Detail view for archive",
                                       boost::format("%-20s\t%-30s") % "Property" % "Setting",
                                       80);

  for (auto& descr: *list) {

    output << CPGBackupCtlBase::makeLine(80);

    output << boost::format("%-20s\t%-30s") % "NAME" % descr->archive_name << endl;
    output << boost::format("%-20s\t%-30s") % "DIRECTORY" % descr->directory << endl;
    output << boost::format("%-20s\t%-30s") % "PGHOST" % descr->coninfo->pghost << endl;
    output << boost::format("%-20s\t%-30d") % "PGPORT" % descr->coninfo->pgport << endl;
    output << boost::format("%-20s\t%-30s") % "PGDATABASE" % descr->coninfo->pgdatabase << endl;
    output << boost::format("%-20s\t%-30s") % "PGUSER" % descr->coninfo->pguser << endl;
    output << boost::format("%-20s\t%-30s") % "DSN" % descr->coninfo->dsn << endl;
    output << boost::format("%-20s\t%-30s") % "COMPRESSION" % descr->compression << endl;
    output << CPGBackupCtlBase::makeLine(80) << endl;

    output << CPGBackupCtlBase::makeLine(80);

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

void ConsoleOutputFormatter::nodeAs(std::shared_ptr<RuntimeConfiguration> rtc,
                                    std::ostringstream &output) {

  auto it_start = rtc->begin();
  auto it_end = rtc->end();

  output << CPGBackupCtlBase::makeHeader("Runtime Variables",
                                       boost::format("%-30s\t%-40s") % "Name" % "Value",
                                       80);

  for(; it_start != it_end; ++it_start) {

    shared_ptr<ConfigVariable> var = it_start->second;
    string str_value;

    var->getValue(str_value);

    output << boost::format("%-30s | %-40s") % var->getName() % str_value
         << endl;

  }

}

void ConsoleOutputFormatter::nodeAs(std::shared_ptr<ConfigVariable> var,
                                    std::ostringstream &output) {

  string str_value;

  /* extract value */
  var->getValue(str_value);

  output << CPGBackupCtlBase::makeHeader("Runtime Variables",
                                       boost::format("%-30s\t%-40s") % "Name" % "Value",
                                       80);
  output << boost::format("%-30s | %-40s") % var->getName() % str_value
         << endl;

}

void ConsoleOutputFormatter::nodeAs(std::string result_str,
                                    std::ostringstream &output) {

  output << result_str << endl;

}

void ConsoleOutputFormatter::nodeAs(std::shared_ptr<BackupProfileDescr> profile,
                                    std::ostringstream &output) {

  output << CPGBackupCtlBase::makeHeader("Details for backup profile " + profile->name,
                                       boost::format("%-25s\t%-40s") % "Property" % "Setting", 80);

  /* Profile Name */
  output << boost::format("%-25s\t%-30s") % "NAME" % profile->name << endl;

  /* Profile compression type */
  output << boost::format("%-25s\t%-30s") % "COMPRESSION"
    % BackupProfileDescr::compressionType(profile->compress_type) << endl;

  /* Profile max rate */
  if (profile->max_rate <= 0) {
    output << boost::format("%-25s\t%-30s") % "MAX RATE" % "NOT RATED"<< endl;
  } else {
    output << boost::format("%-25s\t%-30s") % "MAX RATE(KByte/s)" % profile->max_rate<< endl;
  }

  /* Profile backup label */
  output << boost::format("%-25s\t%-30s") % "LABEL" % profile->label<< endl;

  /* Profile fast checkpoint mode */
  output << boost::format("%-25s\t%-30s") % "FAST CHECKPOINT" % profile->fast_checkpoint<< endl;

  /* Profile WAL included */
  output << boost::format("%-25s\t%-30s") % "WAL INCLUDED" % profile->include_wal<< endl;

  /* Profile WAIT FOR WAL */
  output << boost::format("%-25s\t%-30s") % "WAIT FOR WAL" % profile->wait_for_wal<< endl;

  /* Profile NOVERIFY */
  output << boost::format("%-25s\t%-30s") % "NOVERIFY CHECKSUMS" % profile->noverify_checksums<< endl;

  /* Profile MANIFEST */
  output << boost::format("%-25s\t%-30s") % "MANIFEST" % profile->manifest<< endl;

  /* Profile MANIFEST_CHECKSUMS */
  output << boost::format("%-25s\t%-30s") % "MANIFEST CHECKSUMS" % profile->manifest_checksums<< endl;

}

void ConsoleOutputFormatter::nodeAs(std::shared_ptr<std::list<std::shared_ptr<BackupProfileDescr>>> &list,
                                    std::ostringstream &output) {

  /*
   * Print headline
   */
  output << CPGBackupCtlBase::makeHeader("List of backup profiles",
                                       boost::format("%-25s\t%-15s")
                                       % "Name" % "Backup Label", 80);

  /*
   * Print name and label
   */
  for (auto& descr : *list) {
    output << boost::format("%-25s\t%-15s") % descr->name % descr->label
         << endl;
  }

}

void ConsoleOutputFormatter::nodeAs(std::shared_ptr<StatCatalogArchive> stat,
                                    std::ostringstream &output) {

  output << CPGBackupCtlBase::makeHeader("Archive Catalog Overview",
                                            boost::format("%-15s\t%-30s\t%-20s")
                                            % "Name" % "Directory" % "Host", 80);
  output << boost::format("%-15s\t%-30s\t%-20s")
    % stat->archive_name % stat->archive_directory % stat->archive_host;
  output << endl;

  /*
   * Catalog statistics data.
   */
  output << CPGBackupCtlBase::makeHeader("Backups",
                                            boost::format("%-9s\t%-9s\t%-9s\t%-16s")
                                            % "# total" % "# failed"
                                            % "# running" % "avg duration (s)", 80);
  output << boost::format("%-9s\t%-9s\t%-9s\t%-16s")
    % stat->number_of_backups % stat->backups_failed
    % stat->backups_running
    % stat->avg_backup_duration;
  output << endl;

}

/* ****************************************************************************
 * Implementation of JsonOutputFormatter
 * ****************************************************************************/

JsonOutputFormatter::JsonOutputFormatter(std::shared_ptr<OutputFormatConfiguration> config,
                                         std::shared_ptr<BackupCatalog> catalog,
                                         std::shared_ptr<CatalogDescr> catalog_descr)
  : OutputFormatter(config, catalog, catalog_descr) {}

JsonOutputFormatter::~JsonOutputFormatter() {}

void JsonOutputFormatter::nodeAs(std::shared_ptr<std::list<directory_entry>> fileList,
                                 std::ostringstream &output) {

  namespace pt = boost::property_tree;

  pt::ptree head;
  std::ostringstream num_objects;
  std::map<path, std::list<pt::ptree>> directory_lookup;

  num_objects << fileList->size();
  head.put("num filesystem objects", num_objects.str());

  for(auto item : *fileList) {

    path item_path = item.path();
    pt::ptree fentry;

    if (is_regular_file(item_path)) {

      fentry.put("name", item_path.filename().string());
      fentry.put("size", file_size(item_path));
      fentry.put("type", "file");

      auto lookup = directory_lookup.find(item_path.parent_path());

      /**
       * Lookup directory if already visited.
       */
      if (lookup != directory_lookup.end()) {

        lookup->second.push_back(fentry);

      } else {

        std::list<pt::ptree> fentry_list;
        fentry_list.push_back(fentry);
        directory_lookup[item_path.parent_path()] = fentry_list;

      }

    } else if (is_directory(item_path)) {

        std::list<pt::ptree> fentry_list; /* empty list */
        directory_lookup[item_path] = fentry_list;

    }

  }

  for (auto path_item : directory_lookup) {

    pt::ptree objects;

    for (auto fileobj : path_item.second) {

      if (path_item.second.size() > 0)
        objects.add_child(pt::ptree::path_type(fileobj.get<std::string>("name"), '|'), fileobj);

    }

    num_objects.str("");
    num_objects.clear();
    num_objects << path_item.second.size();

    objects.add("num files", num_objects.str());
    head.add_child(path_item.first.string(), objects);

  }

  pt::write_json(output, head);

}

void JsonOutputFormatter::nodeAs(std::vector<std::shared_ptr<ConnectionDescr>> connections,
                                 std::ostringstream &output) {

  namespace pt = boost::property_tree;

  pt::ptree head;
  pt::ptree clist; /* makes up json array for connection info */
  std::ostringstream oss;

  oss << connections.size();

  head.put("num_connections", oss.str());
  head.put("archive name", catalog_descr->archive_name);

  for (auto & con : connections) {

    pt::ptree item;

    item.put("connection type", con->type);
    item.put("hostname", con->pghost);
    item.put("dbname", con->pgdatabase);
    item.put("user", con->pguser);
    item.put("port", con->pgport);
    item.put("dsn", con->dsn);

    clist.push_back(std::make_pair("", item));

  }

  head.add_child("connections", clist);
  pt::write_json(output, head);

}

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
    bbackup.put("duration", descr->duration);

    oss.str("");
    oss.clear();
    oss << descr->tablespaces.size();

    bbackup.put("num_tablespaces", oss.str());
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

  namespace pt = boost::property_tree;

  pt::ptree head;
  pt::ptree basebackups;
  std::ostringstream oss;

  oss << list.size();
  head.put("num_basebackups", oss.str());

  BOOST_FOREACH(const std::shared_ptr<BaseBackupDescr> &descr, list) {

    size_t upstream_total_size = 0;
    pt::ptree bbackup;
    pt::ptree tablespaces;

    /*
     * Directory handle for basebackup directory on disk.
     */
    StreamingBaseBackupDirectory directory(path(descr->fsentry).filename().string(),
                                           catalog_descr->directory);

    /*
     * Details for the backup profile used by the current basebackup.
     */
    shared_ptr<BackupProfileDescr> backupProfile
      = this->catalog->getBackupProfile(descr->used_profile);

    bbackup.put("id", oss.str());
    bbackup.put("pinned", (descr->pinned ? "yes" : "no"));
    bbackup.put("fsentry", descr->fsentry);
    bbackup.put("catalog state", descr->status);
    bbackup.put("label", descr->label);
    bbackup.put("started", descr->started);
    bbackup.put("stopped", descr->stopped);
    bbackup.put("duration", descr->duration);
    bbackup.put("timeline", descr->timeline);
    bbackup.put("wal start location", descr->xlogpos);
    bbackup.put("wal stop location", descr->xlogposend);
    bbackup.put("system id", descr->systemid);
    bbackup.put("wal segment size", descr->wal_segment_size);
    bbackup.put("size", directory.size());
    bbackup.put("status",
                BackupDirectory::verificationCodeAsString(StreamingBaseBackupDirectory::verify(descr)));

    oss.str("");
    oss.clear();
    oss << descr->tablespaces.size();

    bbackup.put("num_tablespaces", oss.str());

    BOOST_FOREACH(const std::shared_ptr<BackupTablespaceDescr> &tblspc, descr->tablespaces) {

      pt::ptree tblspcinfo;

      tblspcinfo.put("tablespace oid", tblspc->spcoid);
      tblspcinfo.put("tablespace size", tblspc->spcsize);

      /* Check for parent tablespace (also known as pg_default) */
      if (tblspc->spcoid == 0) {
        tblspcinfo.put("tablespace location", "pg_default");
      } else {
        tblspcinfo.put("tablespace location", tblspc->spclocation);
      }

      upstream_total_size += tblspc->spcsize;
      tablespaces.push_back(std::make_pair("", tblspcinfo));

    }

    oss.str("");
    oss.clear();
    oss << upstream_total_size;
    bbackup.put("upstream total size", oss.str());
    bbackup.push_back(std::make_pair("", tablespaces));
    basebackups.push_back(std::make_pair("", bbackup));

  }

  head.add_child("basebackups", basebackups);
  pt::write_json(output, head);

}

boost::property_tree::ptree JsonOutputFormatter::toPtree(std::shared_ptr<RetentionDescr> retentionDescr) {

  namespace pt = boost::property_tree;

  pt::ptree head;
  pt::ptree rules;

  head.put("id", retentionDescr->id);
  head.put("name", retentionDescr->name);
  head.put("created", retentionDescr->created);

  BOOST_FOREACH(const std::shared_ptr<RetentionRuleDescr> &rule, retentionDescr->rules) {

    /*
     * Build a retention object out of this rule to get
     * its decomposed string representation.
     */
    shared_ptr<Retention> instance = Retention::get(rule);
    rules.put("rule", instance->asString());

  }

  head.add_child("rules", rules);

  return head;

}

void JsonOutputFormatter::nodeAs(std::vector<std::shared_ptr<RetentionDescr>> &retentionList,
                                 std::ostringstream &output) {

  namespace pt = boost::property_tree;

  pt::ptree head;
  pt::ptree item;

  head.put("number of retentions", retentionList.size());

  BOOST_FOREACH(const std::shared_ptr<RetentionDescr> descr, retentionList) {
    item.add_child("retention", this->toPtree(descr));
  }

  head.add_child("retentions", item);
  pt::write_json(output, head);

}

void JsonOutputFormatter::nodeAs(std::shared_ptr<RetentionDescr> retentionDescr,
                                 std::ostringstream &output) {

  namespace pt = boost::property_tree;

  pt::ptree result = this->toPtree(retentionDescr);
  pt::write_json(output, result);

}

void JsonOutputFormatter::nodeAs(std::vector<shm_worker_area> &slots,
                                    std::ostringstream &output) {

  namespace pt = boost::property_tree;

  pt::ptree head;
  pt::ptree workers;

  head.put("number of background workers", slots.size());

  if (slots.size() > 0) {
    for(auto &worker : slots) {

      string archive_name      = "N/A";
      unsigned int child_count = 0;
      pt::ptree current;
      pt::ptree child_node;

      if (worker.archive_id >= 0) {
        shared_ptr<CatalogDescr> archive_descr = this->catalog->existsById(worker.archive_id);

        if (archive_descr->id >= 0) {
          archive_name = archive_descr->archive_name;
        }
      }

      current.put("worker id", worker.pid);
      current.put("command tag", CatalogDescr::commandTagName(worker.cmdType));
      current.put("archive name", archive_name);
      current.put("archive id", worker.archive_id);
      current.put("started", CPGBackupCtlBase::ptime_to_str(worker.started));

      /* Print child info, if any */
      for (unsigned int idx = 0; idx < MAX_WORKER_CHILDS; idx++) {

        sub_worker_info child_info = worker.child_info[idx];

        if (child_info.pid > 0) {
          child_count++;

          child_node.put("child id", idx);
          child_node.put("child pid", child_info.pid);
          child_node.put("attached basebackup id", child_info.backup_id);

        }
      }

      if (child_count > 0) {
        current.put("number of childs", child_count);
        current.push_back(std::make_pair("", child_node));
      }

      workers.push_back(std::make_pair("", current));
    }

    head.add_child("background workers",  workers);
  }

  pt::write_json(output, head);

}

void JsonOutputFormatter::nodeAs(std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> list,
                                 std::ostringstream &output) {

  namespace pt = boost::property_tree;

  pt::ptree head;
  pt::ptree archives;

  /*
   * We must distinguish between ARCHIVE_LIST, ARCHIVE_FILTERED_LIST and
   * ARCHIVE_DETAIL_LIST.
   */
  std::string mode;

  config->get("list_archive.mode")->getValue(mode);

  for (auto &descr : *list) {

    pt::ptree item;

    head.put("number of archives", list->size());

    /* ARCHIVE_LIST and ARCHIVE_FILTERED_LIST are basically the same. */
    if (mode == "filtered" || mode == "full") {

      item.put("name", descr->archive_name);
      item.put("directory", descr->directory);

    } else {

      item.put("name", descr->archive_name);
      item.put("directory", descr->directory);
      item.put("pghost", descr->coninfo->pghost);
      item.put("pgport", descr->coninfo->pgport);
      item.put("pgdatabase", descr->coninfo->pgdatabase);
      item.put("pguser", descr->coninfo->pguser);
      item.put("dsn", descr->coninfo->dsn);
      item.put("wal compression", descr->compression);

    }

    archives.add_child(descr->archive_name, item);

  }

  head.add_child("archives", archives);
  pt::write_json(output, head);

}

void JsonOutputFormatter::nodeAs(std::vector<std::shared_ptr<BaseBackupDescr>> &list,
                                 std::ostringstream &output) {

  bool verbose;

  /*
   * Sanity check, make sure we have a valid catalog descriptor.
   */
  if (catalog_descr == nullptr)
    throw CCatalogIssue("could not format basebackup list without valid catalog descriptor");

  config->get("list_backups.verbose")->getValue(verbose);

  if (verbose) {

    this->listBackupsVerbose(list, output);

  } else {

    this->listBackups(list, output);

  }

}

void JsonOutputFormatter::nodeAs(std::shared_ptr<RuntimeConfiguration> rtc,
                                 std::ostringstream &output) {

  namespace pt = boost::property_tree;

  auto it_start = rtc->begin();
  auto it_end = rtc->end();

  pt::ptree head;
  head.put("number of variables", rtc->count_variables());

  for(; it_start != it_end; ++it_start) {

    pt::ptree item;

    shared_ptr<ConfigVariable> var = it_start->second;
    string str_value;

    var->getValue(str_value);

    item.put("value", str_value);

    head.add_child(var->getName(), item);

  }

  pt::write_json(output, head);

}

void JsonOutputFormatter::nodeAs(std::shared_ptr<ConfigVariable> var,
                                 std::ostringstream &output) {

  namespace pt = boost::property_tree;

  pt::ptree head;
  pt::ptree item;
  string str_value;

  var->getValue(str_value);
  item.put("name", var->getName());
  item.put("value", str_value);

  head.add_child("config variable", item);
  pt::write_json(output, head);

}

void JsonOutputFormatter::nodeAs(std::string result_str,
                                 std::ostringstream &output) {

  namespace pt = boost::property_tree;

  pt::ptree head;
  head.put("result", result_str);

  pt::write_json(output, head);

}

void JsonOutputFormatter::listBackupProfileDetail(std::shared_ptr<BackupProfileDescr> descr,
                                                  boost::property_tree::ptree &node) {

  node.put("compress type", BackupProfileDescr::compressionType(descr->compress_type));
  node.put("max rate", descr->max_rate);
  node.put("backup label", descr->label);
  node.put("fast checkpoint", descr->fast_checkpoint);
  node.put("wal included", descr->include_wal);
  node.put("wait for wal", descr->wait_for_wal);
  node.put("noverify checksums", descr->noverify_checksums);
  node.put("manifest", descr->manifest);
  node.put("manifest checksums", descr->manifest_checksums);

}

void JsonOutputFormatter::nodeAs(std::shared_ptr<BackupProfileDescr> profile,
                                 std::ostringstream &output) {

  namespace pt = boost::property_tree;

  pt::ptree head;

  head.put("profile name", profile->name);
  this->listBackupProfileDetail(profile, head);

  pt::write_json(output, head);

}

void JsonOutputFormatter::nodeAs(std::shared_ptr<std::list<std::shared_ptr<BackupProfileDescr>>> &list,
                                 std::ostringstream &output) {

  namespace pt = boost::property_tree;
  pt::ptree head;

  head.put("number of profiles", list->size());

  for(auto &descr : *list) {

    pt::ptree item;

    this->listBackupProfileDetail(descr, item);
    head.add_child(descr->name, item);

  }

  pt::write_json(output, head);

}

void JsonOutputFormatter::nodeAs(std::shared_ptr<StatCatalogArchive> stat,
                                 std::ostringstream &output) {

  namespace pt = boost::property_tree;
  pt::ptree head;
  pt::ptree stats;

  head.put("archive name", stat->archive_name);
  head.put("directory", stat->archive_directory);
  head.put("host", stat->archive_host);

  stats.put("number of backups", stat->number_of_backups);
  stats.put("backups running", stat->backups_running);
  stats.put("avg duration", stat->avg_backup_duration);
  stats.put("backups failed", stat->backups_failed);

  head.add_child("backup statistics", stats);

  pt::write_json(output, head);

}
