#include <sstream>
/* required for string case insensitive comparison */
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/log/trivial.hpp>

#include <catalog.hxx>
#include <BackupCatalog.hxx>
#include <stream.hxx>

using namespace credativ;
using namespace std;

/*
 * Map col id to string.
 */

/*
 * Keep indexes in sync with macros from include/catalog/catalog.hxx !!
 */

std::vector<std::string> BackupCatalog::archiveCatalogCols =
  {
    "id",
    "name",
    "directory",
    "compression"
  };

std::vector<std::string> BackupCatalog::connectionsCatalogCols =
  {
    "archive_id",
    "type",
    "dsn",
    "pghost",
    "pgport",
    "pguser",
    "pgdatabase"
  };

std::vector<std::string> BackupCatalog::backupCatalogCols =
  {
    "id",
    "archive_id",
    "xlogpos",
    "xlogposend",
    "timeline",
    "label",
    "fsentry",
    "started",
    "stopped",
    "pinned",
    "status",
    "systemid",
    "wal_segment_size",
    "used_profile",

    /* the following are computed columns with no materialized representation */
    "strftime('%H hours %M minutes %S seconds', julianday(stopped, 'utc') - julianday(started, 'utc'), '12:00') AS duration ",

  };

std::vector<std::string> BackupCatalog::streamCatalogCols =
  {
    "id",
    "archive_id",
    "stype",
    "slot_name",
    "systemid",
    "timeline",
    "xlogpos",
    "dbname",
    "status",
    "create_date"
  };

std::vector<std::string>BackupCatalog::backupProfilesCatalogCols =
  {
    "id",
    "name",
    "compress_type",
    "max_rate",
    "label",
    "fast_checkpoint",
    "include_wal",
    "wait_for_wal",
    "noverify_checksums",
    "manifest",
    "manifest_checksums"
  };

std::vector<std::string>BackupCatalog::backupTablespacesCatalogCols =
  {
    "backup_id",
    "spcoid",
    "spclocation",
    "spcsize"
  };

std::vector<std::string>BackupCatalog::procsCatalogCols =
  {
    "pid",
    "archive_id",
    "type",
    "started",
    "state",
    "shm_key",
    "shm_id"
  };

std::vector<std::string>BackupCatalog::retentionCatalogCols =
  {
    "id",
    "name",
    "created"
  };

std::vector<std::string>BackupCatalog::retentionRulesCatalogCols =
  {
    "id",
    "type",
    "value"
  };

PushableCols::PushableCols() {}
PushableCols::~PushableCols() {}

void PushableCols::pushAffectedAttribute(int colId) {

  this->affectedAttributes.push_back(colId);

}

std::vector<int> PushableCols::getAffectedAttributes() {
  return affectedAttributes;
}

void PushableCols::clearAffectedAttributes() {
  this->affectedAttributes.clear();
}

void PushableCols::setAffectedAttributes(std::vector<int> affectedAttributes) {
  this->affectedAttributes = affectedAttributes;
}

BackupProfileCompressType BackupProfileDescr::compressionType(std::string type) {

  if (boost::iequals(type, "NONE"))
    return BACKUP_COMPRESS_TYPE_NONE;

  if (boost::iequals(type, "GZIP"))
    return BACKUP_COMPRESS_TYPE_GZIP;

  if (boost::iequals(type, "ZSTD"))
    return BACKUP_COMPRESS_TYPE_ZSTD;

  if (boost::iequals(type, "XZ"))
    return BACKUP_COMPRESS_TYPE_XZ;

  if (boost::iequals(type, "PLAIN"))
    return BACKUP_COMPRESS_TYPE_PLAIN;

  throw CPGBackupCtlFailure("cannot convert backup compress type string");

}

std::string BackupProfileDescr::compressionType(BackupProfileCompressType type) {

  switch(type) {
  case BACKUP_COMPRESS_TYPE_NONE:
    return "NONE";
  case BACKUP_COMPRESS_TYPE_GZIP:
    return "GZIP";
  case BACKUP_COMPRESS_TYPE_ZSTD:
    return "ZSTD";
  case BACKUP_COMPRESS_TYPE_XZ:
    return "XZ";
  case BACKUP_COMPRESS_TYPE_PLAIN:
    return "PLAIN";
  default:
    throw CPGBackupCtlFailure("cannot convert backup compress type id");
  }

}

std::string StatCatalogArchive::gimmeFormattedString() {
  std::ostringstream formatted;

  formatted << CPGBackupCtlBase::makeHeader("Archive Catalog Overview",
                                            boost::format("%-15s\t%-30s\t%-20s")
                                            % "Name" % "Directory" % "Host", 80);
  formatted << boost::format("%-15s\t%-30s\t%-20s")
    % this->archive_name % this->archive_directory % this->archive_host;
  formatted << endl;

  /*
   * Catalog statistics data.
   */
  formatted << CPGBackupCtlBase::makeHeader("Backups",
                                            boost::format("%-9s\t%-9s\t%-9s\t%-16s")
                                            % "# total" % "# failed"
                                            % "# running" % "avg duration (s)", 80);
  formatted << boost::format("%-9s\t%-9s\t%-9s\t%-16s")
    % this->number_of_backups % this->backups_failed
    % this->backups_running
    % this->avg_backup_duration;
  formatted << endl;

  return formatted.str();
}

BasicPinDescr::BasicPinDescr() {
  this->tag = EMPTY_DESCR;
}

BasicPinDescr::~BasicPinDescr() {}

BasicPinDescr *BasicPinDescr::instance(CatalogTag action,
                                       PinOperationType operation) {

  switch(action) {

  case PIN_BASEBACKUP:
    return new PinDescr(operation);
  case UNPIN_BASEBACKUP:
    return new UnpinDescr(operation);
  default:
    break;

  }

  return new BasicPinDescr();

}

void BasicPinDescr::setBackupID(int backupid) {
  this->backupid = backupid;
}

void BasicPinDescr::setBackupID(string backupid) {
  this->backupid = CPGBackupCtlBase::strToInt(backupid);
}

void BasicPinDescr::setCount(string count) {
  this->count = CPGBackupCtlBase::strToUInt(count);
}

void BasicPinDescr::setCount(unsigned int count) {
  this->count = count;
}

int BasicPinDescr::getBackupID() {

  if (this->operation != ACTION_ID)
    throw CPGBackupCtlFailure("attempt to read  backup id in PIN/UNPIN not referencing a backup ID");

  return this->backupid;

}

unsigned int BasicPinDescr::getCount() {

  if (this->operation != ACTION_COUNT)
    throw CPGBackupCtlFailure("attempt to read number to keep in PIN/UNPIN not using a count action");

  return this->count;

}

PinOperationType BasicPinDescr::getOperationType() {

  return this->operation;

}

int BasicPinDescr::bckIDStr(std::string backupid) {

  return CPGBackupCtlBase::strToInt(backupid);

}

CatalogTag BasicPinDescr::action() {
  return this->tag;
}

PinDescr::PinDescr(PinOperationType operation) {

  this->tag = PIN_BASEBACKUP;
  this->operation = operation;

}

UnpinDescr::UnpinDescr(PinOperationType operation) {

  this->tag = UNPIN_BASEBACKUP;
  this->operation = operation;

}

std::string RetentionIntervalDescr::sqlite3_datetime() {

  std::ostringstream result;

  if (this->opr_list.size() == 0)
    return string("");

  result << "datetime('now', 'localtime',";

  for (std::vector<RetentionIntervalOperand>::size_type i = 0;
       i != this->opr_list.size(); i++) {

    result << "?" << (i + 1);

    if (i < this->opr_list.size() - 1)
      result << ", ";

  }

  result << ")";
  return result.str();

}

void RetentionIntervalDescr::push(std::string value) {

  std::vector<string> tokenized;

  /*
   * Tokenize the input expression, if present
   */
  if (value.length() > 0) {

    boost::split(tokenized, value, boost::is_any_of("|"));

    for (auto &tok : tokenized) {

      RetentionIntervalOperand new_opr;

      if (tok.at(0) == '+') {

        new_opr.modifier = RETENTION_MODIFIER_NEWER_DATETIME;
        new_opr.token    = tok.substr(1, tok.length() - 1);

      } else if (tok.at(0) == '-') {

        new_opr.modifier = RETENTION_MODIFIER_OLDER_DATETIME;
        new_opr.token    = tok.substr(1, tok.length() - 1);

      } else {
        CCatalogIssue("unexpected modifier string in operand");
      }

      this->opr_list.push_back(new_opr);

    }

  }

}

RetentionIntervalDescr RetentionIntervalDescr::operator+(std::string operand) {

  RetentionIntervalDescr new_descr;
  RetentionIntervalOperand new_opr;

  new_descr.opr_list = this->opr_list;

  /* If operand is empty, do nothing */
  if (operand != "") {

    new_opr.token = operand;
    new_opr.modifier = RETENTION_MODIFIER_NEWER_DATETIME;

    new_descr.opr_value = operand;
    new_descr.opr_list.push_back(new_opr);

  } else {

    new_descr.opr_value = "";

  }

  return new_descr;

}

RetentionIntervalDescr RetentionIntervalDescr::operator+(RetentionIntervalDescr source) {

  RetentionIntervalDescr new_descr;
  RetentionIntervalOperand new_opr;

  /*
   * Operand ist coming from RHS, LHS will define the
   * current operands queue.
   */
  new_descr.opr_list = this->opr_list;

  /* We need to push the operand value to the queue, if specified */
  if (source.opr_value != "") {

    new_opr.token = source.opr_value;
    new_opr.modifier = RETENTION_MODIFIER_NEWER_DATETIME;

    new_descr.opr_value = source.opr_value;
    new_descr.opr_list.push_back(new_opr);

  }

  return new_descr;

}

RetentionIntervalDescr RetentionIntervalDescr::operator-(RetentionIntervalDescr source) {

  RetentionIntervalDescr new_descr;
  RetentionIntervalOperand new_opr;

  /*
   * Operand is coming from RHS, LHS will define
   * the current operands queue.
   */
  new_descr.opr_list = this->opr_list;

  /* We need to push the operand value to the queue, if specified */
  if (source.opr_value != "") {

    new_opr.token = source.opr_value;
    new_opr.modifier = RETENTION_MODIFIER_OLDER_DATETIME;

    new_descr.opr_value = source.opr_value;
    new_descr.opr_list.push_back(new_opr);

  }

  return new_descr;
}

RetentionIntervalDescr RetentionIntervalDescr::operator-(std::string operand) {

  RetentionIntervalDescr new_descr;
  RetentionIntervalOperand new_opr;

  new_descr.opr_list = this->opr_list;

  /* If operand is empty, do nothing */
  if (operand != "") {

    new_opr.token = operand;
    new_opr.modifier = RETENTION_MODIFIER_OLDER_DATETIME;

    new_descr.opr_value = operand;
    new_descr.opr_list.push_back(new_opr);

  } else {

    new_descr.opr_value = "";

  }

  return new_descr;

}

void RetentionIntervalDescr::push_add(std::string operand) {

  RetentionIntervalOperand opr;

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "push interval operand " << operand;
#endif

  opr.modifier = RETENTION_MODIFIER_NEWER_DATETIME;
  opr.token    = operand;
  this->opr_list.push_back(opr);

}

void RetentionIntervalDescr::push_sub(std::string operand) {

  RetentionIntervalOperand opr;

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "push interval operand " << operand;
#endif

  opr.modifier = RETENTION_MODIFIER_OLDER_DATETIME;
  opr.token    = operand,
  this->opr_list.push_back(opr);

}

std::string RetentionIntervalDescr::getOperandsAsString() {

  std::string result = "";

  for (std::vector<RetentionIntervalOperand>::size_type i = 0;
       i != this->opr_list.size(); i++) {

    RetentionIntervalOperand operand = this->opr_list[i];

    result += operand.token;

    if (i < (this->opr_list.size() - 1))
      result += " ";

  }

  return result;

}

std::string RetentionIntervalOperand::str() {

  std::string result = "";

  switch(this->modifier) {

  case RETENTION_MODIFIER_NEWER_DATETIME:
    result += "+";
    break;

  case RETENTION_MODIFIER_OLDER_DATETIME:
    result += "-";
    break;

  default:
    /* Other modifiers don't need special treatment here */
    break;
  }

  return result += this->token;

}

std::string RetentionIntervalDescr::compile() {

  std::string result = "";

  for (auto const &operand : this->opr_list) {

    if (result.length() > 0)
      result += "|";

    switch(operand.modifier) {

    case RETENTION_MODIFIER_NEWER_DATETIME:
      result += "+";
      break;

    case RETENTION_MODIFIER_OLDER_DATETIME:
      result += "-";
      break;

    default:
      throw CCatalogIssue("unexpected operator action in retention interval operand");

    }

    result += operand.token;

#ifdef __DEBUG__
    BOOST_LOG_TRIVIAL(debug) << "DEBUG: opr list " << result;
#endif

  }

  return result;

}

RetentionIntervalDescr::RetentionIntervalDescr() {

  /* default constructor */

}

RetentionIntervalDescr::RetentionIntervalDescr(std::string expression) {

  this->push(expression);
  this->opr_value = this->opr_list.back().token;

}

CatalogDescr::~CatalogDescr() {}

CatalogDescr& CatalogDescr::operator=(CatalogDescr& source) {

  this->tag = source.tag;
  this->id = source.id;
  this->archive_name = source.archive_name;
  this->retention_name = source.retention_name;
  this->label = source.label;
  this->compression = source.compression;
  this->directory = source.directory;
  this->check_connection = source.check_connection;
  this->force_systemid_update = source.force_systemid_update;
  this->forceXLOGPosRestart = source.forceXLOGPosRestart;
  this->coninfo->pghost = source.coninfo->pghost;
  this->coninfo->pgport = source.coninfo->pgport;
  this->coninfo->pguser = source.coninfo->pguser;
  this->coninfo->pgdatabase = source.coninfo->pgdatabase;
  this->coninfo->dsn = source.coninfo->dsn;
  this->coninfo->type = source.coninfo->type;
  this->basebackup_id = source.basebackup_id;
  this->verbose_output = source.verbose_output;

  /* job control */
  this->detach = source.detach;

  /* if a pinDescr was initialized, we need to make our own instance */
  switch (source.tag) {
  case PIN_BASEBACKUP:
    {
      this->pinDescr = PinDescr(source.pinDescr.getOperationType());

      if (source.pinDescr.getOperationType() == ACTION_ID)
        this->pinDescr.setBackupID(source.pinDescr.getBackupID());

      if (source.pinDescr.getOperationType() == ACTION_COUNT)
        this->pinDescr.setCount(source.pinDescr.getCount());

      break;
    }
  case UNPIN_BASEBACKUP:
    {
      this->pinDescr = UnpinDescr(source.pinDescr.getOperationType());

      if (source.pinDescr.getOperationType() == ACTION_ID)
        this->pinDescr.setBackupID(source.pinDescr.getBackupID());

      if (source.pinDescr.getOperationType() == ACTION_COUNT)
        this->pinDescr.setCount(source.pinDescr.getCount());

      break;
    }
  default:
    /* nothing to do here */
    break;
  }

  /*
   * Copy over the retention policy descriptor, if allocated.
   */
  this->retention = source.getRetentionPolicyP();

  /* Copy over runtime configuration, if present */
  if (source.getRuntimeConfiguration() != nullptr)
    this->runtime_config = source.getRuntimeConfiguration();

  /*
   * Copy over recovery instance descriptor. This is a shared
   * pointer, so referencing here should be enough.
   */
  if (source.getRecoveryStreamDescr() != nullptr)
    this->recoveryStream = source.getRecoveryStreamDescr();

  /*
   * In case this instance was instantiated
   * by a SET <variable> parser command, copy
   * over state variable as well
   */
  this->var_type = source.var_type;
  this->var_name = source.var_name;
  this->var_val_str = source.var_val_str;
  this->var_val_int = source.var_val_int;
  this->var_val_bool = source.var_val_bool;

  return *this;
}

std::shared_ptr<RecoveryStreamDescr> CatalogDescr::getRecoveryStreamDescr() {

  return this->recoveryStream;

}

OutputFormatType CatalogDescr::getOutputFormat() {

  std::string output_format;

  this->runtime_config->get("output.format")->getValue(output_format);

  if (output_format == "json")
    return OUTPUT_JSON;

  if (output_format == "console")
    return OUTPUT_CONSOLE;

  throw CPGBackupCtlFailure("unknown option to output.format configuration variable");

}

void CatalogDescr::makeRecoveryStreamDescr() {

  if (this->recoveryStream != nullptr) {
    throw CCatalogIssue("recovery stream descriptor already initialized");
  }

  this->recoveryStream = std::make_shared<RecoveryStreamDescr>();
  this->recoveryStream->version = CPGBackupCtlBase::getVersionString();

}

void CatalogDescr::setRecoveryStreamAddr(std::string const& address) {

  /* If the recovery descriptor wasn't initialized yet, abort */
  if (this->recoveryStream == nullptr)
    throw CCatalogIssue("recovery stream descriptor not initialized yet");

  if (address.length() == 0)
    throw CCatalogIssue("cannot add empty address to recovery stream descriptor");

  BOOST_LOG_TRIVIAL(debug) << "adding listener address " << address;
  this->recoveryStream->listen_on.push_back(address);

}

void CatalogDescr::setRecoveryStreamPort(std::string const& portnumber) {

  /* If the recovery descriptor wasn't initialized yet, abort */
  if (this->recoveryStream == nullptr)
    throw CCatalogIssue("recovery stream descriptor not initialized yet");

  /* Something between 0 and 65535 ... */
  this->recoveryStream->port = CPGBackupCtlBase::strToInt(portnumber);

  if (this->recoveryStream->port >= 65536)
    throw CCatalogIssue("port number cannot be above 65535");

}

void CatalogDescr::setPrintVerbose(bool const& verbose) {
  this->verbose_output = verbose;
}

void CatalogDescr::setVariableName(string const& name) {
  this->var_name = name;
}

void CatalogDescr::setVariableValueInteger(string const& var_value) {
  this->var_type    = VAR_TYPE_INTEGER;
  this->var_val_int = CPGBackupCtlBase::strToInt(var_value);
}

void CatalogDescr::setVariableValueString(string const& var_value) {
  this->var_type    = VAR_TYPE_STRING;
  this->var_val_str = var_value;
}

void CatalogDescr::setVariableValueBool(bool const& var_value) {
  this->var_val_bool = var_value;
  this->var_type     = VAR_TYPE_BOOL;
}

void CatalogDescr::setStreamingForceXLOGPositionRestart( bool const& restart ) {
  this->forceXLOGPosRestart = restart;
}

PinOperationType CatalogDescr::pinOperation() {

  return this->pinDescr.getOperationType();

}

shared_ptr<RetentionDescr> CatalogDescr::getRetentionPolicyP() {

  return this->retention;

}

void CatalogDescr::makeRuleFromParserState(string const &value) {

  shared_ptr<RetentionRuleDescr> rule = nullptr;
  RetentionRuleId ruleId = RETENTION_NO_RULE;

  /*
   * Analyse the current parser state. According to
   * the flags there, build the rule descriptor.
   *
   * We don't accept parser state with RETENTION_NO_ACTION,
   * since we don't know what to do in this case.
   */

  switch(this->rps.action) {

  case RETENTION_ACTION_DROP:

    switch(this->rps.modifier) {

    case RETENTION_MODIFIER_NEWER_DATETIME:
      ruleId = RETENTION_DROP_NEWER_BY_DATETIME;
      break;
    case RETENTION_MODIFIER_OLDER_DATETIME:
      ruleId = RETENTION_DROP_OLDER_BY_DATETIME;
      break;
    case RETENTION_MODIFIER_LABEL:
      ruleId = RETENTION_DROP_WITH_LABEL;
      break;

    case RETENTION_MODIFIER_NUM:
      ruleId = RETENTION_DROP_NUM;
      break;

    case RETENTION_MODIFIER_CLEANUP:
      ruleId = RETENTION_CLEANUP;
      break;

    default:
      throw CCatalogIssue("unexpected retention parser modifier");
    }

    break;

  case RETENTION_ACTION_KEEP:

    switch(this->rps.modifier) {

    case RETENTION_MODIFIER_NEWER_DATETIME:
      ruleId = RETENTION_KEEP_NEWER_BY_DATETIME;
      break;
    case RETENTION_MODIFIER_OLDER_DATETIME:
      ruleId = RETENTION_KEEP_OLDER_BY_DATETIME;
      break;
    case RETENTION_MODIFIER_LABEL:
      ruleId = RETENTION_KEEP_WITH_LABEL;
      break;

    case RETENTION_MODIFIER_NUM:
      ruleId = RETENTION_KEEP_NUM;
      break;

    case RETENTION_MODIFIER_CLEANUP:
      throw CCatalogIssue("a CLEANUP retention modifier cannot be attached to a KEEP action");

    default:
      throw CCatalogIssue("unexpected retention parser modifier");

    }

    break;

  default:
    throw CCatalogIssue("unexpected retention parser state");
  };

  this->makeRetentionDescr(ruleId);
  rule = make_shared<RetentionRuleDescr>();
  rule->type = ruleId;
  rule->value = value;

  this->retention->rules.push_back(rule);

}

void CatalogDescr::setRetentionActionModifier(RetentionParsedModifier const &modifier) {

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug)<< "modifier set";
#endif

  this->rps.modifier = modifier;

}

void CatalogDescr::setRetentionAction(RetentionParsedAction const &action) {

  this->rps.action = action;

}

void CatalogDescr::detachRetentionDescr() {

  this->retention = nullptr;

}

void CatalogDescr::makeRetentionDescr(RetentionRuleId const &ruleid) {

 /*
  * If not yet initialized, create a new retention
  * policy descriptor.
  */
 if (this->retention == nullptr) {
   this->retention = make_shared<RetentionDescr> ();

   /* copy over the catalog descriptor identifier */
   this->retention->name = this->archive_name;
 }

}

void CatalogDescr::makeRetentionRule(RetentionRuleId const &ruleid,
                                     string const &value) {

  std::shared_ptr<RetentionRuleDescr> rule = nullptr;

  /* We require an instantiated retention policy descriptor */
  this->makeRetentionDescr(ruleid);

  rule = make_shared<RetentionRuleDescr>();
  rule->type = ruleid;
  rule->value = value;

  retention->rules.push_back(rule);

}

void CatalogDescr::retentionIntervalExprFromParserState(std::string const& expr_value,
                                                        std::string const& intv_mod) {

  char operation = '\0';
  RetentionRuleId ruleid = RETENTION_NO_RULE;
  std::shared_ptr<RetentionRuleDescr> rule = nullptr;
  RetentionIntervalDescr interval;
  RetentionIntervalOperand operand;
  /*
   * We need a valid parser state to build the retention interval
   * policy from.
   */
  if (this->rps.action == RETENTION_NO_ACTION) {
    throw CCatalogIssue("cannot build retention interval rule from invalid parser state");
  }

  /*
   * We must always operate on the last rule in the retention
   * rule list, since that's the current one.
   *
   * Check if the retention action modifier is set to
   * RETENTION_MODIFIER_OLDER_DATETIME or RETENTION_MODIFIER_NEWER_DATETIME.
   */
  if ( (this->rps.modifier != RETENTION_MODIFIER_NEWER_DATETIME)
       && (this->rps.modifier != RETENTION_MODIFIER_OLDER_DATETIME) ) {

    throw CCatalogIssue("unexpected retention action modifier in parser state");

  }

  if (this->retention == nullptr) {


    this->retention = make_shared<RetentionDescr>();
    this->retention->name = this->archive_name;

  }

  /*
   * No policy attached to this catalog descriptor yet, create one,
   * representing the operation mode of this interval expression.
   *
   */

  switch(this->rps.action) {

  case RETENTION_ACTION_DROP:
    {
      switch(this->rps.modifier) {
      case RETENTION_MODIFIER_OLDER_DATETIME:
        operation = '-';
        ruleid = RETENTION_DROP_OLDER_BY_DATETIME;
        break;
      case RETENTION_MODIFIER_NEWER_DATETIME:
        operation = '+';
        ruleid = RETENTION_DROP_NEWER_BY_DATETIME;
        break;
      default:
        throw CCatalogIssue("unexpected retention action modifier in parser state");
      }
      break;
    }

  case RETENTION_ACTION_KEEP:
    {

      switch(this->rps.modifier) {
      case RETENTION_MODIFIER_OLDER_DATETIME:
        operation = '-';
        ruleid = RETENTION_KEEP_OLDER_BY_DATETIME;
        break;
      case RETENTION_MODIFIER_NEWER_DATETIME:
        operation = '+';
        ruleid = RETENTION_KEEP_NEWER_BY_DATETIME;
        break;
      default:
        throw CCatalogIssue("unexpected retention action modifier in parser state");
      }
      break;
    }

  default:
    throw CCatalogIssue("unexpected retention action in parser state");
  } /* switch */

  if (!this->retention->rules.empty()) {

    rule = this->retention->rules.back();

  } else {

    rule = make_shared<RetentionRuleDescr>();
    rule->type = ruleid;
    rule->value = "";
    this->retention->rules.push_back(rule);

  }

  if (rule == nullptr) {
    throw CCatalogIssue("unexpected uninitialized retention rule in catalog descriptor");
  }

  /* sanity check */
  switch(rule->type) {

  case RETENTION_KEEP_NEWER_BY_DATETIME:
  case RETENTION_KEEP_OLDER_BY_DATETIME:
  case RETENTION_DROP_NEWER_BY_DATETIME:
  case RETENTION_DROP_OLDER_BY_DATETIME:
    break;
  default:
    throw CCatalogIssue("interval expression requires DATETIME retention policy");

  }

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "DEBUG: retention interval expression already parsed: "
                           << rule->value;
#endif

  /*
   * Parse current value of the RetentionRuleDescr, if present, so that the interval
   * descriptor has an updated list of operands.
   *
   * XXX: This is not optimal, since we need to parse and recompile the
   *      expression for each assignment.
   *
   */
  if (rule->value.length() > 0) {

    interval.push(rule->value);

  }

  /* Assign expression */
  operand.token    = expr_value + " " + intv_mod;

  /* Make sure we set the correct operator from current
   * parser state */
  if (operation == '+') {
    operand.modifier = RETENTION_MODIFIER_NEWER_DATETIME;
    interval.opr_list.push_back(operand);
  } else if (operation == '-') {
    operand.modifier = RETENTION_MODIFIER_OLDER_DATETIME;
    interval.opr_list.push_back(operand);
  }

  rule->value = interval.compile();

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "DEBUG: compiled interval expr: " << rule->value;
#endif

}


void CatalogDescr::makePinDescr(PinOperationType const& operation) {

  switch(this->tag) {

  case PIN_BASEBACKUP:
    this->pinDescr = PinDescr(operation);
    break;
  case UNPIN_BASEBACKUP:
    this->pinDescr = UnpinDescr(operation);
    break;
  default:
    {
      ostringstream oss;
      oss << "invalid command tag "
          << CatalogDescr::commandTagName(this->tag)
          << " to create a pin/unpin operation";
      throw CPGBackupCtlFailure(oss.str());
    }

  }
}

void CatalogDescr::makePinDescr(PinOperationType const& operation,
                                string const& argument) {

  /*
   * The makePinDescr(PinOperationType) method already checks for valid
   * command tags, so leave error checking to it.
   *
   */
  this->makePinDescr(operation);

  if (operation == ACTION_ID) {
    this->pinDescr.setBackupID(argument);
  } else if (operation == ACTION_COUNT) {
    this->pinDescr.setCount(argument);
  }

}

std::string CatalogDescr::commandTagName(CatalogTag tag) {
  switch(tag) {
  case EMPTY_DESCR:
    return "NO OP";
  case CREATE_ARCHIVE:
    return "CREATE ARCHIVE";
  case CREATE_BACKUP_PROFILE:
    return "CREATE BACKUP PROFILE";
  case CREATE_CONNECTION:
    return "CREATE CONNECTION";
  case DROP_ARCHIVE:
    return "DROP ARCHIVE";
  case DROP_BACKUP_PROFILE:
    return "DROP BACKUP PROFILE";
  case ALTER_ARCHIVE:
    return "ALTER ARCHIVE";
  case VERIFY_ARCHIVE:
    return "VERIFY ARCHIVE";
  case START_BASEBACKUP:
    return "START BASEBACKUP";
  case LIST_ARCHIVE:
    return "LIST ARCHIVE";
  case LIST_BACKUP_PROFILE:
  case LIST_BACKUP_PROFILE_DETAIL:
    return "LIST BACKUP PROFILE";
  case LIST_BACKUP_CATALOG:
    return "LIST BACKUP CATALOG";
  case START_LAUNCHER:
    return "START LAUNCHER";
  case BACKGROUND_WORKER_COMMAND:
    return "BACKGROUND WORKER COMMAND";
  case LIST_CONNECTION:
    return "LIST CONNECTION";
  case DROP_CONNECTION:
    return "DROP CONNECTION";
  case START_STREAMING_FOR_ARCHIVE:
    return "START STREAMING FOR ARCHIVE";
  case STOP_STREAMING_FOR_ARCHIVE:
    return "STOP STREAMING FOR ARCHIVE";
  case EXEC_COMMAND:
    return "EXEC";
  case SHOW_WORKERS:
    return "SHOW WORKERS";
  case SHOW_VARIABLES:
    return "SHOW VARIABLES";
  case SHOW_VARIABLE:
    return "SHOW VARIABLE";
  case SET_VARIABLE:
    return "SET VARIABLE";
  case RESET_VARIABLE:
    return "RESET VARIABLE";
  case LIST_BACKUP_LIST:
    return "LIST BASEBACKUPS";
  case PIN_BASEBACKUP:
    return "PIN";
  case UNPIN_BASEBACKUP:
    return "UNPIN";
  case CREATE_RETENTION_POLICY:
    return "CREATE RETENTION POLICY";
  case LIST_RETENTION_POLICIES:
    return "LIST RETENTION POLICIES";
  case LIST_RETENTION_POLICY:
    return "LIST RETENTION POLICY";
  case DROP_RETENTION_POLICY:
    return "DROP RETENTION POLICY";
  case APPLY_RETENTION_POLICY:
    return "APPLY RETENTION POLICY";
  case DROP_BASEBACKUP:
    return "DROP BASEBACKUP";
  case START_RECOVERY_STREAM_FOR_ARCHIVE:
    return "START RECOVERY STREAM";

  default:
    return "UNKNOWN";
  }
}

std::string CatalogDescr::getCommandTagAsStr() {
  return CatalogDescr::commandTagName(this->tag);
}

void CatalogDescr::setJobDetachMode(bool const& detach) {
  this->detach = detach;
}

void CatalogDescr::setProfileManifest(bool const& manifest) {

  backup_profile->manifest = manifest;
  backup_profile->pushAffectedAttribute(SQL_BCK_PROF_MANIFEST_ATTNO);

}

void CatalogDescr::setProfileManifestChecksumsIdent(std::string const& manifest_checksum_ident) {

  backup_profile->manifest_checksums = manifest_checksum_ident;
  backup_profile->pushAffectedAttribute(SQL_BCK_PROF_MANIFEST_CHECKSUMS_ATTNO);

}

void CatalogDescr::setProfileAffectedAttribute(int const& colId) {
  this->backup_profile->pushAffectedAttribute(colId);
}

void CatalogDescr::setProfileNoVerify(bool const &noverify) {
  this->backup_profile->noverify_checksums = noverify;
  this->backup_profile->pushAffectedAttribute(SQL_BCK_PROF_NOVERIFY_CHECKSUMS_ATTNO);
}

void CatalogDescr::setProfileWaitForWAL(bool const& wait) {
  this->backup_profile->wait_for_wal = wait;
  this->backup_profile->pushAffectedAttribute(SQL_BCK_PROF_WAIT_FOR_WAL_ATTNO);
}

void CatalogDescr::setProfileCheckpointMode(bool const& fastmode) {
  this->backup_profile->fast_checkpoint = fastmode;
  this->backup_profile->pushAffectedAttribute(SQL_BCK_PROF_FAST_CHKPT_ATTNO);
}

void CatalogDescr::setProfileWALIncluded(bool const& included) {
  this->backup_profile->include_wal = included;
  this->backup_profile->pushAffectedAttribute(SQL_BCK_PROF_INCL_WAL_ATTNO);
}

void CatalogDescr::setProfileBackupLabel(std::string const& label) {
  this->backup_profile->label = label;
  this->backup_profile->pushAffectedAttribute(SQL_BCK_PROF_LABEL_ATTNO);
}

void CatalogDescr::setProfileMaxRate(std::string const& max_rate) {
  this->backup_profile->max_rate = CPGBackupCtlBase::strToInt(max_rate);
  this->pushAffectedAttribute(SQL_BCK_PROF_MAX_RATE_ATTNO);
}

void CatalogDescr::setProfileCompressType(BackupProfileCompressType const& type) {
  this->backup_profile->compress_type = type;
  this->pushAffectedAttribute(SQL_BCK_PROF_COMPRESS_TYPE_ATTNO);
}

std::shared_ptr<BackupProfileDescr> CatalogDescr::getBackupProfileDescr() {
  return this->backup_profile;
}

void CatalogDescr::setProfileName(std::string const& profile_name) {
  this->backup_profile->name = profile_name;
  this->pushAffectedAttribute(SQL_BCK_PROF_NAME_ATTNO);
}

void CatalogDescr::setDbName(std::string const& db_name) {
  this->coninfo->pgdatabase = db_name;
  this->pushAffectedAttribute(SQL_ARCHIVE_PGDATABASE_ATTNO);
}

void CatalogDescr::setExecString(std::string const& execStr) {
  this->execString = execStr;
}

void CatalogDescr::setCommandTag(credativ::CatalogTag const& tag) {
  this->tag = tag;

  /*
   * Set some useful defaults in case not already done.
   */
  if (this->coninfo->type == ""
      || this->coninfo->type == ConnectionDescr::CONNECTION_TYPE_UNKNOWN) {

    switch(tag) {
    case CREATE_ARCHIVE:
    case ALTER_ARCHIVE:
    case DROP_ARCHIVE:
      this->coninfo->type = ConnectionDescr::CONNECTION_TYPE_BASEBACKUP;
      break;
    case CREATE_CONNECTION:
    case LIST_CONNECTION:
    case DROP_CONNECTION:
      this->coninfo->type = ConnectionDescr::CONNECTION_TYPE_STREAMER;
      break;
    default:
      this->coninfo->type = ""; /* force errors later */
    }

  }

}

void CatalogDescr::setBasebackupID(std::string const &bbid) {

  BOOST_LOG_TRIVIAL(debug) << "DEBUG: setting basebackup id " << bbid;
  this->basebackup_id = CPGBackupCtlBase::strToInt(bbid);

}

void CatalogDescr::setForceSystemIDUpdate(bool const& force_sysid_update) {
  this->force_systemid_update = force_sysid_update;
}

void CatalogDescr::setVerifyOption(VerifyOption const& option) {

  switch(option) {

  case VERIFY_DATABASE_CONNECTION:
    this->check_connection = true;
    break;

  default:
    /* unhandled option flag here, don't treat it as an error */
    break;
  }
}

void CatalogDescr::setRetentionName(std::string const& ident) {
  this->retention_name = ident;
  this->pushAffectedAttribute(SQL_RETENTION_NAME_ATTNO);
}

void CatalogDescr::setIdent(std::string const& ident) {
  this->archive_name = ident;
  this->pushAffectedAttribute(SQL_ARCHIVE_NAME_ATTNO);
}

void CatalogDescr::setConnectionType(std::string const& type) {
  this->coninfo->type = type;
  this->coninfo->pushAffectedAttribute(SQL_CON_TYPE_ATTNO);
}

void CatalogDescr::setHostname(std::string const& hostname) {
  this->coninfo->pghost = hostname;
  this->coninfo->pushAffectedAttribute(SQL_CON_PGHOST_ATTNO);
}

void CatalogDescr::setUsername(std::string const& username) {
  this->coninfo->pguser = username;
  this->coninfo->pushAffectedAttribute(SQL_CON_PGUSER_ATTNO);
}

void CatalogDescr::setPort(std::string const& portNumber) {
  this->coninfo->pgport = CPGBackupCtlBase::strToInt(portNumber);
  this->coninfo->pushAffectedAttribute(SQL_CON_PGPORT_ATTNO);
}

void CatalogDescr::setDirectory(std::string const& directory) {
  this->directory = directory;
}

void CatalogDescr::setArchiveId(int const& archive_id) {
  this->id = archive_id;
  this->coninfo->archive_id = this->id;

  this->pushAffectedAttribute(SQL_ARCHIVE_ID_ATTNO);
  this->coninfo->pushAffectedAttribute(SQL_CON_ARCHIVE_ID_ATTNO);
}

void CatalogDescr::setDSN(std::string const& dsn) {

  std::vector<int> attrs;

  this->coninfo->dsn = dsn;

  /*
   * DSN assignment causes to invalidate any other
   * connection parameter assigned directly.
   */
  this->coninfo->pghost = "";
  this->coninfo->pgport = -1;
  this->coninfo->pguser = "";
  this->coninfo->pgdatabase = "";

  /*
   * NOTE: we also need to remove them from the attributes list.
   */

  /*
   * Copy over any element not referencing direct
   * connection settings.
   */
  for(auto& colId : this->coninfo->getAffectedAttributes()) {

    switch(colId) {
    case SQL_CON_ARCHIVE_ID_ATTNO:
    case SQL_CON_TYPE_ATTNO:
      /* fall through until here */
      attrs.push_back(colId);
      break;
    default:
      break;
    }
  }

  attrs.push_back(SQL_CON_DSN_ATTNO);
  this->coninfo->setAffectedAttributes(attrs);

}

BackupCatalog::BackupCatalog() {
  this->isOpen = false;
  this->db_handle = NULL;
}

BackupCatalog::BackupCatalog(string sqliteDB) {
  this->isOpen = false;
  this->db_handle = NULL;

  /*
   * Identifiers
   */
  this->sqliteDB = sqliteDB;

  /*
   * Initialize/open catalog database.
   */
  this->open_rw();

  /*
   * Check catalog ...
   */
  this->checkCatalog();
}

std::string BackupCatalog::fullname() {
  return this->sqliteDB;
}

std::string BackupCatalog::name() {
  return path(this->sqliteDB).filename().string();
}

void BackupCatalog::startTransaction() {

  int rc;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  rc = sqlite3_exec(this->db_handle,
                    "BEGIN TRANSACTION EXCLUSIVE;",
                    NULL,
                    NULL,
                    NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "error starting catalog transaction: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }
}

void BackupCatalog::commitTransaction() {

  int rc;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  rc = sqlite3_exec(this->db_handle,
                    "COMMIT;",
                    NULL, NULL, NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "error committing catalog transaction: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

}

void BackupCatalog::rollbackTransaction() {

  int rc;

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  rc = sqlite3_exec(this->db_handle,
                    "ROLLBACK;",
                    NULL, NULL, NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "error rollback catalog transaction: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

}

std::shared_ptr<CatalogProc> BackupCatalog::fetchCatalogProcData(sqlite3_stmt *stmt,
                                                                 std::vector<int> affectedAttributes) {

  int currindex = 0; /* SQLite3 column index starts at zero! */
  std::shared_ptr<CatalogProc> procInfo = std::make_shared<CatalogProc>();

  if (stmt == NULL) {
    throw CCatalogIssue("cannot fetch proc information from catalog: invalid statement handle");
  }

  for (auto& colId : affectedAttributes) {

    switch (colId) {

    case SQL_PROCS_PID_ATTNO:
      procInfo->pid = sqlite3_column_int(stmt, currindex);
      break;

    case SQL_PROCS_ARCHIVE_ID_ATTNO:
      procInfo->archive_id = sqlite3_column_int(stmt, currindex);
      break;

    case SQL_PROCS_TYPE_ATTNO:
      /* can't be NULL, but for safety reason do a NULL check */
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        procInfo->type = (char *) sqlite3_column_text(stmt, currindex);
      break;

    case SQL_PROCS_STARTED_ATTNO:
      /* can't be NULL, but for safety reasons do a NULL check */
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        procInfo->started = (char *) sqlite3_column_text(stmt, currindex);
      break;

    case SQL_PROCS_STATE_ATTNO:
      /* can't be NULL, but for safety reasons do a NULL check */
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        procInfo->state = (char *) sqlite3_column_text(stmt, currindex);
      break;

    case SQL_PROCS_SHM_KEY_ATTNO:
      /* can be NULL, so check for NULL value. In case it's null, assign
       * a negative value, indicating that here's nothing to read */
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        procInfo->shm_key = (key_t) sqlite3_column_int(stmt, currindex);
      else
        procInfo->shm_key = (key_t) -1;
      break;

    case SQL_PROCS_SHM_ID_ATTNO:
      /* can be NULL, so check for NULL value. In case it's null, assign
       * a negative value, indicating that here's nothing to read */
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        procInfo->shm_id = sqlite3_column_int(stmt, currindex);
      else
        procInfo->shm_id = -1;
      break;

    default:
      /* oops, should we throw a CCatalogIssue exception ? */
      break;
    }

    currindex++;
  }

  return procInfo;
}

void BackupCatalog::fetchConnectionData(sqlite3_stmt *stmt,
                                        std::shared_ptr<ConnectionDescr> conDescr) {
  int currindex = 0; /* SQLite3 column index starts at zero! */

  if (stmt == NULL)
    throw CCatalogIssue("cannot fetch connection information from catalog: invalid statement handle");

  if (conDescr == nullptr)
    throw CCatalogIssue("cannot fetch connection information from catalog: invalid connection handle");

  for (auto& colId : conDescr->getAffectedAttributes()) {
    switch(colId) {

    case SQL_CON_ARCHIVE_ID_ATTNO:
      conDescr->archive_id = sqlite3_column_int(stmt, currindex);
      break;

    case SQL_CON_TYPE_ATTNO:
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        conDescr->type = (char *) sqlite3_column_text(stmt, currindex);
      break;

    case SQL_CON_PGHOST_ATTNO:
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        conDescr->pghost = (char *) sqlite3_column_text(stmt, currindex);
      break;

    case SQL_CON_PGPORT_ATTNO:
      conDescr->pgport = sqlite3_column_int(stmt, currindex);
      break;

    case SQL_CON_PGUSER_ATTNO:
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        conDescr->pguser = (char *) sqlite3_column_text(stmt, currindex);
      break;

    case SQL_CON_PGDATABASE_ATTNO:
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        conDescr->pgdatabase = (char *) sqlite3_column_text(stmt, currindex);
      break;

    case SQL_CON_DSN_ATTNO:
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL) {
        conDescr->dsn = (char *) sqlite3_column_text(stmt, currindex);
      }
      break;

    default:
      /* oops, should we throw a CCatalogIssue exception ? */
      break;
    }

    currindex++;
  }

}

std::shared_ptr<StreamIdentification> BackupCatalog::fetchStreamData(sqlite3_stmt *stmt,
                                                                     std::vector<int> affectedRows) {

  int currindex = 0; /* column index starts at 0 */
  std::shared_ptr<StreamIdentification> ident(nullptr);

  if (stmt == NULL)
    throw("cannot fetch stream data: uninitialized statement handle");

  ident = make_shared<StreamIdentification>();

  for(auto& colId : affectedRows) {

    switch (colId) {
    case SQL_STREAM_ID_ATTNO:
      ident->id = sqlite3_column_int(stmt, currindex);
      break;
    case SQL_STREAM_ARCHIVE_ID_ATTNO:
      ident->archive_id = sqlite3_column_int(stmt, currindex);
      break;
    case SQL_STREAM_STYPE_ATTNO:
      ident->stype = (char *) sqlite3_column_text(stmt, currindex);
      break;
    case SQL_STREAM_SLOT_NAME_ATTNO:
      /*
       * NOTE: can be NULL!
       */
      if (sqlite3_column_type(stmt, currindex) != SQLITE_NULL)
        ident->slot_name = (char *) sqlite3_column_text(stmt, currindex);
      break;
    case SQL_STREAM_SYSTEMID_ATTNO:
      ident->systemid = sqlite3_column_int(stmt, currindex);
      break;
    case SQL_STREAM_TIMELINE_ATTNO:
      ident->timeline = sqlite3_column_int(stmt, currindex);
      break;
    case SQL_STREAM_XLOGPOS_ATTNO:
      ident->xlogpos = (char *) sqlite3_column_text(stmt, currindex);
      break;
    case SQL_STREAM_DBNAME_ATTNO:
      ident->dbname = (char *) sqlite3_column_text(stmt, currindex);
      break;
    case SQL_STREAM_STATUS_ATTNO:
      ident->status = (char *) sqlite3_column_text(stmt, currindex);
      break;
    case SQL_STREAM_REGISTER_DATE_ATTNO:
      ident->create_date = (char *) sqlite3_column_text(stmt, currindex);
      break;
    default:
      /* oops */
      break;
    }

    currindex++;

  }

  return ident;

}

std::shared_ptr<BaseBackupDescr> BackupCatalog::fetchBackupIntoDescr(sqlite3_stmt *stmt,
                                                                     std::shared_ptr<BaseBackupDescr> descr,
                                                                     Range colIdRange) {

  if (stmt == NULL)
    throw CCatalogIssue("cannot fetch backup data: uninitialized statement handle");

  if (descr == nullptr)
    throw CCatalogIssue("cannot fetch backup data: invalid descriptor handle");

  std::vector<int> attr = descr->getAffectedAttributes();
  int current_stmt_col = colIdRange.start();

  for (auto &current : attr) {

    /*
     * Sanity check, stop if range end ist reached.
     */
    if (current_stmt_col > colIdRange.end()) {
      break;
    }

    switch(current) {

    case SQL_BACKUP_ID_ATTNO:
      descr->id = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BACKUP_ARCHIVE_ID_ATTNO:
      descr->archive_id = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BACKUP_XLOGPOS_ATTNO:
      descr->xlogpos = (char *)sqlite3_column_text(stmt, current_stmt_col);
      break;

    case SQL_BACKUP_XLOGPOSEND_ATTNO:
      {
        /* can be null in case of aborted basebackups */
        if (sqlite3_column_type(stmt, current_stmt_col) != SQLITE_NULL)
          descr->xlogposend = (char *)sqlite3_column_text(stmt, current_stmt_col);
        else
          descr->xlogposend = "";

        break;
      }

    case SQL_BACKUP_TIMELINE_ATTNO:
      descr->timeline = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BACKUP_LABEL_ATTNO:
      descr->label = (char *) sqlite3_column_text(stmt, current_stmt_col);
      break;

    case SQL_BACKUP_FSENTRY_ATTNO:
      descr->fsentry = (char *) sqlite3_column_text(stmt, current_stmt_col);
      break;

    case SQL_BACKUP_STARTED_ATTNO:
      {
        /* can be null, so take care */
        if (sqlite3_column_type(stmt, current_stmt_col) != SQLITE_NULL)
          descr->started = (char *) sqlite3_column_text(stmt, current_stmt_col);
        else
          descr->started = "";

        break;
      }

    case SQL_BACKUP_STOPPED_ATTNO:
      {
        /* can be null, so take care */
        if (sqlite3_column_type(stmt, current_stmt_col) != SQLITE_NULL)
          descr->stopped = (char *) sqlite3_column_text(stmt, current_stmt_col);
        else
          descr->stopped = "";

        break;
      }

    case SQL_BACKUP_PINNED_ATTNO:
      {
        /* can be null, so take care */
        if (sqlite3_column_type(stmt, current_stmt_col) != SQLITE_NULL)
          descr->pinned = sqlite3_column_int(stmt, current_stmt_col);
        else
          descr->pinned = 0;

        break;
      }

    case SQL_BACKUP_STATUS_ATTNO:
      {
        /* can be null, so take care */
        if (sqlite3_column_type(stmt, current_stmt_col) != SQLITE_NULL)
          descr->status = (char *) sqlite3_column_text(stmt, current_stmt_col);
        else
          descr->status = "";

        break;
      }

    case SQL_BACKUP_SYSTEMID_ATTNO:
      descr->systemid = (char *) sqlite3_column_text(stmt, current_stmt_col);
      break;

    case SQL_BACKUP_WAL_SEGMENT_SIZE_ATTNO:
      descr->wal_segment_size = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BACKUP_USED_PROFILE_ATTNO:
      descr->used_profile = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BACKUP_COMPUTED_RETENTION_DATETIME:

      /* this column tag identifies a computed value, be aware for nullable expressions */
      if (sqlite3_column_type(stmt, current_stmt_col) != SQLITE_NULL)
        descr->exceeds_retention_rule = sqlite3_column_int(stmt, current_stmt_col);

      break;

    case SQL_BACKUP_COMPUTED_DURATION:

      /* this column tag identifies a computed value, be aware for nullable expressions */
      if (sqlite3_column_type(stmt, current_stmt_col) != SQLITE_NULL)
        descr->duration = (char *) sqlite3_column_text(stmt, current_stmt_col);

      break;

    default:
      break;

    }

    current_stmt_col++;
  }

  return descr;
}

std::shared_ptr<BackupProfileDescr> BackupCatalog::fetchBackupProfileIntoDescr(sqlite3_stmt *stmt,
                                                                               std::shared_ptr<BackupProfileDescr> descr,
                                                                               Range colIdRange) {
  if (stmt == NULL)
    throw CCatalogIssue("cannot fetch archive data: uninitialized statement handle");

  if (descr == nullptr)
    throw CCatalogIssue("cannot fetch archive data: invalid descriptor handle");

  std::vector<int> attr = descr->getAffectedAttributes();
  int current_stmt_col = colIdRange.start();

  for (unsigned int current = 0; current < attr.size(); current++) {

    /*
     * Sanity check, stop if range end is reached.
     */
    if (current_stmt_col > colIdRange.end())
      break;

    switch (attr[current]) {

    case SQL_BCK_PROF_ID_ATTNO:

      descr->profile_id = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_NAME_ATTNO:

      /*
       * name cannot be NULL, so no NULL check required.
       */
      descr->name = (char *)sqlite3_column_text(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_COMPRESS_TYPE_ATTNO:

      /*
       * BackupProfileCompressType is just stored as an integer in the catalog,
       * so we need to explicitely typecast.
       */
      descr->compress_type = (BackupProfileCompressType) sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_MAX_RATE_ATTNO:
      descr->max_rate = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_LABEL_ATTNO:

      /*
       * label cannot be NULL, so no NULL check required.
       */
      descr->label = (char *)sqlite3_column_text(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_FAST_CHKPT_ATTNO:

      descr->fast_checkpoint = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_INCL_WAL_ATTNO:

      descr->include_wal = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_WAIT_FOR_WAL_ATTNO:

      descr->wait_for_wal = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_NOVERIFY_CHECKSUMS_ATTNO:
      descr->noverify_checksums = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_MANIFEST_ATTNO:
      descr->manifest = sqlite3_column_int(stmt, current_stmt_col);
      break;

    case SQL_BCK_PROF_MANIFEST_CHECKSUMS_ATTNO:
      descr->manifest_checksums = (char *)sqlite3_column_text(stmt, current_stmt_col);
      break;

    default:
      break;
    }

    current_stmt_col++;
  }

  return descr;

}

shared_ptr<CatalogDescr> BackupCatalog::fetchArchiveDataIntoDescr(sqlite3_stmt *stmt,
                                                                  shared_ptr<CatalogDescr> descr) {

  if (stmt == NULL)
    throw("cannot fetch archive data: uninitialized statement handle");

  if (descr == nullptr)
    throw("cannot fetch archive data: invalid descriptor handle");

  /*
   * Save archive properties into catalog
   * descriptor
   */
  descr->id = sqlite3_column_int(stmt, SQL_ARCHIVE_ID_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_DIRECTORY_ATTNO) != SQLITE_NULL)
    descr->directory = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_DIRECTORY_ATTNO);

  descr->compression = sqlite3_column_int(stmt, SQL_ARCHIVE_COMPRESSION_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_PGHOST_ATTNO) != SQLITE_NULL)
    descr->coninfo->pghost = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_PGHOST_ATTNO);

  descr->coninfo->pgport = sqlite3_column_int(stmt, SQL_ARCHIVE_PGPORT_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_PGUSER_ATTNO) != SQLITE_NULL)
    descr->coninfo->pguser = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_PGUSER_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_PGDATABASE_ATTNO) != SQLITE_NULL)
    descr->coninfo->pgdatabase = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_PGDATABASE_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_NAME_ATTNO) != SQLITE_NULL)
    descr->archive_name = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_NAME_ATTNO);

  if (sqlite3_column_type(stmt, SQL_ARCHIVE_DSN_ATTNO) != SQLITE_NULL)
    descr->coninfo->dsn = (char *)sqlite3_column_text(stmt, SQL_ARCHIVE_DSN_ATTNO);

  return descr;

}

shared_ptr<CatalogDescr> BackupCatalog::existsById(int archive_id) {

  sqlite3_stmt *stmt;
  int rc;
  shared_ptr<CatalogDescr> result = make_shared<CatalogDescr>();

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /*
   * Check for the specified directory. Note that SQLite3 here
   * uses filesystem locking, so we can't just do row-level
   * locking on our own.
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          "SELECT * FROM archive WHERE id = ?1;",
                          -1,
                          &stmt,
                          NULL);

  rc = sqlite3_bind_int(stmt, 1, archive_id);

  /* ... perform SELECT */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  while(rc == SQLITE_ROW && rc != SQLITE_DONE) {

    this->fetchArchiveDataIntoDescr(stmt, result);
    rc = sqlite3_step(stmt);

    if (rc != SQLITE_DONE) {
      throw CArchiveIssue("unexpected result by retrieving archive by id");
    }

  }

  sqlite3_finalize(stmt);

  /* result->id >= 0 means valid result */
  return result;

}

shared_ptr<CatalogDescr> BackupCatalog::existsByName(std::string name) {

  sqlite3_stmt *stmt;
  int rc;
  shared_ptr<CatalogDescr> result = make_shared<CatalogDescr>();

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /*
   * Check for the specified directory. Note that SQLite3 here
   * uses filesystem locking, so we can't just do row-level
   * locking on our own.
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          "SELECT * FROM archive WHERE name = ?1;",
                          -1,
                          &stmt,
                          NULL);

  sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_STATIC);

  /* ... perform SELECT */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  while(rc == SQLITE_ROW && rc != SQLITE_DONE) {

    this->fetchArchiveDataIntoDescr(stmt, result);
    rc = sqlite3_step(stmt);

  }

  sqlite3_finalize(stmt);

  /* result->id >= 0 means valid result */
  return result;

}

shared_ptr<CatalogDescr> BackupCatalog::exists(std::string directory) {

  sqlite3_stmt *stmt;
  int rc;
  shared_ptr<CatalogDescr> result = make_shared<CatalogDescr>();

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  /*
   * Check for the specified directory. Note that SQLite3 here
   * uses filesystem locking, so we can't just do row-level
   * locking on our own.
   */

  rc = sqlite3_prepare_v2(this->db_handle,
                          "SELECT * FROM archive WHERE directory = ?1;",
                          -1,
                          &stmt,
                          NULL);

  sqlite3_bind_text(stmt, 1, directory.c_str(), -1, SQLITE_STATIC);

  /* ... perform SELECT */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /* ... empty result set or single row expected */
  while (rc == SQLITE_ROW && rc != SQLITE_DONE) {

    this->fetchArchiveDataIntoDescr(stmt, result);
    rc = sqlite3_step(stmt);

  }

  sqlite3_finalize(stmt);

  /* result->id >= 0 means valid result */
  return result;

}

void BackupCatalog::dropBackupProfile(std::string name) {

  sqlite3_stmt *stmt;
  int rc;
  if (!this->available()) {
    throw CCatalogIssue("catalog database not openend");
  }

  rc = sqlite3_prepare_v2(this->db_handle,
                          "DELETE FROM backup_profiles WHERE name = ?1;",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << " cannot prepare query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /* Bind WHERE condition */
  sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_STATIC);

  /* ... and execute ... */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;

    oss << "error dropping backup profile: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::dropArchive(std::string name) {

  sqlite3_stmt *stmt;
  int rc;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  /*
   * Drop the archive by name.
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          "DELETE FROM archive WHERE name = ?1;",
                          -1,
                          &stmt,
                          NULL);

  /*
   * Bind WHERE condition ...
   */
  sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_STATIC);

  /*
   * ... and execute the DELETE statement.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result for DROP ARCHIVE in query: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);

}

std::shared_ptr<StatCatalogArchive> BackupCatalog::statCatalog(std::string archive_name) {

  sqlite3_stmt *stmt;
  std::string query;
  int rc;
  std::shared_ptr<StatCatalogArchive> result = std::make_shared<StatCatalogArchive>();

  /*
   * per default, set the stats result set to be empty.
   */
  result->archive_id = -1;

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  query =
"SELECT "
  "(SELECT COUNT(*) FROM backup WHERE archive_id = a.id) AS number_of_backups, "
  "(SELECT COUNT(*) FROM backup b "
                   "WHERE b.archive_id = a.id "
                         "AND b.status = 'aborted') AS backups_failed, "
  "(SELECT COUNT(*) FROM backup b "
                   "WHERE b.archive_id = a.id "
                         "AND b.status = 'in progress') AS backups_running, "
  "a.id, "
  "a.name, "
  "a.directory, "
  "CASE WHEN length(COALESCE(c.pghost, '')) > 0 THEN c.pghost ELSE c.dsn END AS pghost, "
  "(SELECT SUM(spcsize) FROM backup_tablespaces bt "
                            "JOIN backup b ON b.id = bt.backup_id "
                            "JOIN archive a2 ON a.id = b.archive_id "
                       "WHERE a2.id = a.id) AS approx_sz, "
  "(SELECT MAX(stopped) FROM backup b "
                       "WHERE b.archive_id = a.id) AS latest_finished, "
  "(SELECT CASE WHEN (started IS NOT NULL AND stopped IS NOT NULL) "
          "THEN AVG(CAST((julianday(stopped) - julianday(started)) * 24 * 60 * 60 AS integer)) "
          "ELSE 0 "
          "END AS val_avg_duration "
   "FROM "
   "backup b "
   "WHERE b.archive_id = a.id) AS avg_duration "
"FROM "
  "archive a JOIN connections c ON c.archive_id = a.id "
"WHERE "
  "a.name = ?1 AND c.type = 'basebackup';";

  /*
   * Prepare the query...
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          query.c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind parameters ...
   */
  sqlite3_bind_text(stmt, 1, archive_name.c_str(), -1, SQLITE_STATIC);

  /*
   * ... and execute the query.
   */
  rc = sqlite3_step(stmt);

  /*
   * Only a single line result set expected.
   */
  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    std::ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result in catalog query: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  result->number_of_backups = sqlite3_column_int(stmt, 0);
  result->backups_failed    = sqlite3_column_int(stmt, 1);
  result->backups_running   = sqlite3_column_int(stmt, 2);
  result->archive_id        = sqlite3_column_int(stmt, 3);

  if (sqlite3_column_type(stmt, 4) != SQLITE_NULL)
    result->archive_name      = (char *) sqlite3_column_text(stmt, 4);

  if (sqlite3_column_type(stmt, 5) != SQLITE_NULL)
    result->archive_directory = (char *) sqlite3_column_text(stmt, 5);

  if (sqlite3_column_type(stmt, 6) != SQLITE_NULL)
    result->archive_host      = (char *) sqlite3_column_text(stmt, 6);

  result->estimated_total_size = sqlite3_column_int(stmt, 7);

  if (sqlite3_column_type(stmt, 8) != SQLITE_NULL)
    result->latest_finished      = (char *) sqlite3_column_text(stmt, 8);

  result->avg_backup_duration  = sqlite3_column_int(stmt, 9);

  /*
   * We're done.
   */
  sqlite3_finalize(stmt);

  return result;
}

void BackupCatalog::dropRetentionPolicy(string retention_name) {

  sqlite3_stmt *stmt = NULL;
  ostringstream delete_sql;
  int rc;

  /*
   * Database available ?
   */
  if (!this->available()) {
    throw CArchiveIssue("catalog database not opened");
  }

  if (retention_name.length() < 1) {
    throw CArchiveIssue("cannot drop retention policy with empty identifier");
  }

  /*
   * Compose SQL string for deletion...
   */
  delete_sql << "DELETE FROM retention WHERE name = ?1;";

  rc = sqlite3_prepare_v2(this->db_handle,
                          delete_sql.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;

    oss << "cannot prepare query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind retention name
   */
  sqlite3_bind_text(stmt, 1, retention_name.c_str(), -1, SQLITE_STATIC);

  /*
   * Execute the DELETE ...
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {

    ostringstream oss;

    oss << "error dropping retention policy \""
        << retention_name
        << "\": "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CArchiveIssue(oss.str());

  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::createRetentionPolicy(std::shared_ptr<RetentionDescr> retentionPolicy) {

  ostringstream insert;
  sqlite3_stmt *stmt;
  int rc;

  vector<int> attrsRetention;
  vector<int> attrsRules;

  /*
   * Database available ?
   */
  if (! this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /*
   * Prepare the insert. A retention policy is only valid to create,
   * if there's a rule attached to it. The creation of a retention
   * policy thus involves at least two INSERT operations, one
   * to create the retention policy in the retention table, and another
   * one to create the attached rule(s) in the retention_rules table.
   *
   * NOTE: The caller should ideally use a transaction, but we
   *       don't force it here.
   */
  if (retentionPolicy->rules.size() < 1) {
    throw CCatalogIssue("cannot create retention policy with no rules");
  }

  attrsRetention.push_back(SQL_RETENTION_NAME_ATTNO);
  attrsRetention.push_back(SQL_RETENTION_CREATED_ATTNO);

  attrsRules.push_back(SQL_RETENTION_RULES_ID_ATTNO);
  attrsRules.push_back(SQL_RETENTION_RULES_TYPE_ATTNO);
  attrsRules.push_back(SQL_RETENTION_RULES_VALUE_ATTNO);

  insert << "INSERT INTO retention("
         << this->affectedColumnsToString(SQL_RETENTION_ENTITY, attrsRetention)
         << ") VALUES(?1, datetime('now'))";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "executing SQL: " << insert.str();
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          insert.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;

    oss << "cannot prepare query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind attributes from retention policy descriptor.
   */
  this->SQLbindRetentionPolicyAttributes(retentionPolicy,
                                         attrsRetention,
                                         stmt,
                                         Range(1, 1));

  /*
   * Execute the insert. After having successfully
   * inserted the retention descriptor, we need it's
   * primary key for its rule dataset.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;

    oss << "error creating retention policy in catalog database:"
        << sqlite3_errmsg(this->db_handle);

    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  retentionPolicy->id = sqlite3_last_insert_rowid(this->db_handle);

  /*
   * Release statement, we need it for the INSERT operation
   * for the retention rule dataset.
   */
  sqlite3_finalize(stmt);
  insert.str("");
  insert.clear(); /* resets error flags, but normally we don't need to care here ... */

  /*
   * Prepare the INSERT operation for retention rules.
   */
  insert << "INSERT INTO retention_rules("
    << this->affectedColumnsToString(SQL_RETENTION_RULES_ENTITY,
                                     attrsRules)
    << ") VALUES(";

  for (unsigned int i = 0; i < attrsRules.size(); i++) {
    insert << "?" << (i + 1);
    if (i < (attrsRules.size() - 1))
      insert << ", ";
  }

  insert << ");";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "executing SQL: " << insert.str();
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          insert.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;

    oss << "cannot prepare query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Loop through the rule list, inserting
   * each one into the catalog database.
   */
  for (auto &rule : retentionPolicy->rules) {

    rule->id = retentionPolicy->id;
    this->SQLbindRetentionRuleAttributes(rule,
                                         attrsRules,
                                         stmt,
                                         Range(1, attrsRules.size()));

    rc = sqlite3_step(stmt);

    if (rc != SQLITE_DONE) {

      ostringstream oss;

      oss << "error while inserting rule description into catalog: "
          << sqlite3_errmsg(this->db_handle);
      sqlite3_finalize(stmt);
      throw CCatalogIssue(oss.str());
    }

  }

  sqlite3_finalize(stmt);

  /* and we're done */
}

void BackupCatalog::performPinAction(BasicPinDescr *descr,
                                     std::vector<int> basebackupIds) {

  ostringstream update;
  sqlite3_stmt *stmt;
  int rc;
  unsigned int num_bind_id;

  /*
   * Negative values indicate an error
   */
  if (basebackupIds.size() == 0) {
    throw CArchiveIssue("no basebackup IDs to pin or unpin specified");
  }

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  if (descr->action() == PIN_BASEBACKUP)
    update << "UPDATE backup SET pinned = 1 WHERE id IN (";
  else
    update << "UPDATE backup SET pinned = 0 WHERE id IN (";

  /*
   * Build basebackup ID bind list.
   */
  for (num_bind_id = 1; num_bind_id <= basebackupIds.size(); num_bind_id++) {

    update << "?" << num_bind_id;

    if (num_bind_id < basebackupIds.size())
      update << ", ";

  }

  /*
   * Finalize update statement
   */
  update << ");";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "generate UPDATE SQL: "
                           << update.str();
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          update.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query:" << sqlite3_errmsg(db_handle);

    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind ID values.
   */
  for (num_bind_id = 1; num_bind_id <= basebackupIds.size(); num_bind_id++) {

#ifdef __DEBUG__
    BOOST_LOG_TRIVIAL(debug) << "DEBUG: binding basebackup ID "
                             << basebackupIds[num_bind_id - 1];
#endif

    sqlite3_bind_int(stmt, num_bind_id, basebackupIds[num_bind_id - 1]);
  }

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "failed to pin/unpin basebackup IDs "
        << ": " << sqlite3_errmsg(db_handle);
    sqlite3_finalize(stmt);
    throw CArchiveIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::deleteBaseBackup(int basebackupId) {

  int rc;
  sqlite3_stmt *stmt;

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  rc = sqlite3_prepare_v2(this->db_handle,
                          "DELETE FROM backup WHERE id = ?1;",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {

    std::ostringstream oss;

    oss << "error preparing to delete basebackup: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());

  }

  sqlite3_bind_int(stmt, 1, basebackupId);

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;

    oss << "could not delete basebackup: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());

  }

  sqlite3_finalize(stmt);

}

void BackupCatalog::exceedsRetention(std::shared_ptr<BaseBackupDescr> basebackup,
                                     RetentionRuleId retention_mode,
                                     RetentionIntervalDescr interval) {

  std::string interval_expr = "";
  sqlite3_stmt *stmt        = NULL;
  int exceeds_retention_rule = 0;
  std::ostringstream sql;
  std::ostringstream retention_expr;
  int rc;
  int intvOprCount = 0;

  /* basebackup should be valid */
  if (basebackup == nullptr) {
    throw CCatalogIssue("cannot check datetime retention for uninitialized basebackup descriptor");
  }

  if (basebackup->id < 0) {
    throw CCatalogIssue("cannot check datetime retention for invalid basebackup descriptor");
  }

  /*
   * A retention interval without valid operands
   * is considered a no-op value here.
   */
  if (interval.opr_list.size() == 0) {
    throw CCatalogIssue("attempt to apply an empty retention interval to basebackup listing");
  } else {

    intvOprCount = interval.opr_list.size();

  }

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /*
   * Get the formatted datetime() expression from the
   * interval descriptor.
   */
  interval_expr = interval.sqlite3_datetime();

  /*
   * Get the formatted SQLite3 SQL expression
   * from the interval descriptor.
   */
  switch(retention_mode) {

  case RETENTION_KEEP_OLDER_BY_DATETIME:
  case RETENTION_DROP_OLDER_BY_DATETIME:
    {
      retention_expr << "stopped < " << interval_expr;
      break;
    }

  case RETENTION_KEEP_NEWER_BY_DATETIME:
  case RETENTION_DROP_NEWER_BY_DATETIME:
    {
      retention_expr << "stopped > " << interval_expr;
      break;
    }

  default:
    throw CCatalogIssue("invalid retention mode when getting backup list");

  }

  /* Prepare SQL command */
  sql << "SELECT "
      << retention_expr.str()
      << " AS exceeds_retention_rule FROM backup b WHERE archive_id = ?"
      << intvOprCount + 1
      << " AND id = ?"
      << intvOprCount + 2
      << ";";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "generate SQL: " << sql.str();
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          sql.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * First, bind retention datetime values. This makes
   * up the retention expression, which looks like:
   *
   * datetime('now', ?1, ?2, ?3, ...)
   */
  for (std::vector<RetentionIntervalOperand>::size_type i = 0;
       i != interval.opr_list.size(); i++) {

    RetentionIntervalOperand operand = interval.opr_list[i];
    std::string opr_value = operand.str();

    cerr << "bind datetime operand " << opr_value << endl;
    sqlite3_bind_text(stmt, (i + 1), opr_value.c_str(), -1, SQLITE_STATIC);

  }

  /* Bind predicate values to query... */
  cerr << "bind ARCHIVE ID " << basebackup->archive_id << endl;
  sqlite3_bind_int(stmt, intvOprCount + 1, basebackup->archive_id);
  cerr << "bind ID " << basebackup->id << endl;
  sqlite3_bind_int(stmt, intvOprCount + 2, basebackup->id);

  /* ...and execute the query */
  rc = sqlite3_step(stmt);

  /* Only one result row expected */
  if (rc != SQLITE_DONE && rc != SQLITE_ROW) {

    ostringstream oss;

    oss << "unexpected result when checking retention rule: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());

  }

  /* SQLite does not have a bool type, we're using integer instead */
  exceeds_retention_rule = sqlite3_column_int(stmt, 0);

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "exceeds_retention_rule value retrieved: " << exceeds_retention_rule;
#endif

  if (exceeds_retention_rule <= 0)
    basebackup->exceeds_retention_rule = false;
  else
    basebackup->exceeds_retention_rule = true;

  cerr << "exceeds retention rule value: " << basebackup->exceeds_retention_rule << endl;

  /* ... close result and exit */
  sqlite3_finalize(stmt);

}

std::shared_ptr<BaseBackupDescr> BackupCatalog::getBaseBackup(std::string basebackup_fqfn,
                                                              int archive_id) {

  int rc;
  sqlite3_stmt *stmt;
  ostringstream query;
  string backupCols;
  shared_ptr<BaseBackupDescr> basebackup = make_shared<BaseBackupDescr>();
  vector<int> backupAttrs;
  vector<int> tblspcAttrs;

    if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /* mark descriptor empty */
  basebackup->id = -1;

  /*
   * Generate list of columns to retrieve...
   */
  backupAttrs.push_back(SQL_BACKUP_ID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_ARCHIVE_ID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_XLOGPOS_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_XLOGPOSEND_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_TIMELINE_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_LABEL_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_FSENTRY_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STARTED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STOPPED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STATUS_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_SYSTEMID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_PINNED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_WAL_SEGMENT_SIZE_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_USED_PROFILE_ATTNO);

  tblspcAttrs.push_back(SQL_BCK_TBLSPC_BCK_ID_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCOID_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCLOC_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCSZ_ATTNO);

  backupCols = BackupCatalog::SQLgetColumnList(SQL_BACKUP_ENTITY,
                                               backupAttrs);

    query << "SELECT "
        << backupCols
        << ", "

    /*
     * IMPORTANT:
     *
     * We can't use SQLgetColumnList here(), since we want to
     * prevent NULL
     */

        << "COALESCE(bt.backup_id, -1) AS backup_id, "
        << "COALESCE(bt.spcoid, -1) AS spcoid, "
        << "COALESCE(spclocation, 'no location') AS spclocation, "
        << "COALESCE(spcsize, -1) AS spcsize, "
        << "b.wal_segment_size AS wal_segment_size, "
        << "b.used_profile AS backup_profile_id "
        << "FROM "
        << "backup b LEFT JOIN backup_tablespaces bt ON (b.id = bt.backup_id) "
        << "WHERE b.fsentry = ?1 AND b.archive_id = ?2;";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "generate SQL: " << query.str();
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind WHERE conditions.
   */
  sqlite3_bind_text(stmt, 1, basebackup_fqfn.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 2, archive_id);

  /*
   * Execute the statement.
   */
  rc = sqlite3_step(stmt);

  /*
   * If multiple tablespaces are in the basebackup,
   * there are more than one row to process!
   */

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {

    ostringstream oss;

    oss << "error retrieving backup list from catalog database: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());

  }

  /*
   * If no rows returned, there's effectively
   * nothing more to do here (no basebackup by
   * the specified ID found).
   */
  if (rc == SQLITE_DONE) {
    sqlite3_finalize(stmt);
    return basebackup;
  }

  /*
   * If only a single tablespace is part of the
   * basebackup, make sure we loop through it.
   *
   * Code here is doubled in getBackupList() (except the loop header),
   * but it's not sure whether the code retains here this way. So just
   * repeat it and leave it to some future work to optimize it.
   */

  do {

    shared_ptr<BackupTablespaceDescr> tablespace = make_shared<BackupTablespaceDescr>();

    basebackup->setAffectedAttributes(backupAttrs);
    tablespace->setAffectedAttributes(tblspcAttrs);

    /*
     * If not initialized, retrieve properties into
     * our BaseBackupDescr instance.
     */
    if (basebackup->id < 0) {
      this->fetchBackupIntoDescr(stmt, basebackup, Range(0, backupAttrs.size() - 1));
    }

    /*
     * Since we fetch tablespace and basebackup information in one
     * query, we only push basebackup information if the last
     * encountered backup_id differs into the our result list.
     *
     * In opposite to getBackupList(), we have only a single backup id
     * here to fetch, so there's no need to check if we have to
     * create a new BaseBackupDescr instance.
     */
    this->fetchBackupTablespaceIntoDescr(stmt,
                                         tablespace,
                                         Range(backupAttrs.size(), backupAttrs.size()
                                               + tblspcAttrs.size() -1));

    if (tablespace->backup_id >= 0) {
      basebackup->tablespaces.push_back(tablespace);
    }

    rc = sqlite3_step(stmt);

  } while(rc == SQLITE_ROW);

  /* clean up */
  sqlite3_finalize(stmt);

  return basebackup;
}

std::shared_ptr<BaseBackupDescr> BackupCatalog::getBaseBackup(int basebackupId,
                                                              int archive_id) {

  int rc;
  sqlite3_stmt *stmt;
  ostringstream query;
  string backupCols;
  shared_ptr<BaseBackupDescr> basebackup = make_shared<BaseBackupDescr>();
  vector<int> backupAttrs;
  vector<int> tblspcAttrs;

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /* mark descriptor empty */
  basebackup->id = -1;

  /*
   * Generate list of columns to retrieve...
   */
  backupAttrs.push_back(SQL_BACKUP_ID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_ARCHIVE_ID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_XLOGPOS_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_XLOGPOSEND_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_TIMELINE_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_LABEL_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_FSENTRY_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STARTED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STOPPED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STATUS_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_SYSTEMID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_PINNED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_WAL_SEGMENT_SIZE_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_USED_PROFILE_ATTNO);

  tblspcAttrs.push_back(SQL_BCK_TBLSPC_BCK_ID_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCOID_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCLOC_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCSZ_ATTNO);

  backupCols = BackupCatalog::SQLgetColumnList(SQL_BACKUP_ENTITY,
                                               backupAttrs);

  query << "SELECT "
        << backupCols
        << ", "

    /*
     * IMPORTANT:
     *
     * We can't use SQLgetColumnList here(), since we want to
     * prevent NULL
     */

        << "COALESCE(bt.backup_id, -1) AS backup_id, "
        << "COALESCE(bt.spcoid, -1) AS spcoid, "
        << "COALESCE(spclocation, 'no location') AS spclocation, "
        << "COALESCE(spcsize, -1) AS spcsize, "
        << "b.wal_segment_size AS wal_segment_size, "
        << "b.used_profile AS backup_profile_id "
        << "FROM "
        << "backup b LEFT JOIN backup_tablespaces bt ON (b.id = bt.backup_id) "
        << "WHERE b.id = ?1 AND b.archive_id = ?2;";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "generate SQL: " << query.str();
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind WHERE conditions.
   */
  sqlite3_bind_int(stmt, 1, basebackupId);
  sqlite3_bind_int(stmt, 2, archive_id);

  /*
   * Execute the statement.
   */
  rc = sqlite3_step(stmt);

  /*
   * If multiple tablespaces are in the basebackup,
   * there are more than one row to process!
   */

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {

    ostringstream oss;

    oss << "error retrieving backup list from catalog database: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());

  }

  /*
   * If no rows returned, there's effectively
   * nothing more to do here (no basebackup by
   * the specified ID found).
   */
  if (rc == SQLITE_DONE) {
    sqlite3_finalize(stmt);
    return basebackup;
  }

  /*
   * If only a single tablespace is part of the
   * basebackup, make sure we loop through it.
   *
   * Code here is doubled in getBackupList() (except the loop header),
   * but it's not sure whether the code retains here this way. So just
   * repeat it and leave it to some future work to optimize it.
   */

  do {

    shared_ptr<BackupTablespaceDescr> tablespace = make_shared<BackupTablespaceDescr>();

    basebackup->setAffectedAttributes(backupAttrs);
    tablespace->setAffectedAttributes(tblspcAttrs);

    /*
     * If not initialized, retrieve properties into
     * our BaseBackupDescr instance.
     */
    if (basebackup->id < 0) {
      this->fetchBackupIntoDescr(stmt, basebackup, Range(0, backupAttrs.size() - 1));
    }

    /*
     * Since we fetch tablespace and basebackup information in one
     * query, we only push basebackup information if the last
     * encountered backup_id differs into the our result list.
     *
     * In opposite to getBackupList(), we have only a single backup id
     * here to fetch, so there's no need to check if we have to
     * create a new BaseBackupDescr instance.
     */
    this->fetchBackupTablespaceIntoDescr(stmt,
                                         tablespace,
                                         Range(backupAttrs.size(), backupAttrs.size()
                                               + tblspcAttrs.size() -1));

    if (tablespace->backup_id >= 0) {
      basebackup->tablespaces.push_back(tablespace);
    }

    rc = sqlite3_step(stmt);

  } while(rc == SQLITE_ROW);

  /* clean up */
  sqlite3_finalize(stmt);

  return basebackup;
}

std::shared_ptr<BaseBackupDescr> BackupCatalog::getBaseBackup(BaseBackupRetrieveMode mode,
                                                              int archive_id,
                                                              bool valid_only) {

  std::shared_ptr<BaseBackupDescr> result = std::make_shared<BaseBackupDescr>();
  sqlite3_stmt *stmt = NULL;
  string backupCols = "";
  std::ostringstream query;
  std::vector<int> backupAttrs;
  int rc;

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /* List of columns to retrieve */
  backupAttrs.push_back(SQL_BACKUP_ID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_ARCHIVE_ID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_XLOGPOS_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_XLOGPOSEND_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_TIMELINE_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_LABEL_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_FSENTRY_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STARTED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STOPPED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STATUS_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_SYSTEMID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_PINNED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_WAL_SEGMENT_SIZE_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_USED_PROFILE_ATTNO);

  /* Safe column list to descriptor */
  result->setAffectedAttributes(backupAttrs);

  /* Column list */
  backupCols = BackupCatalog::SQLgetColumnList(SQL_BACKUP_ENTITY,
                                               backupAttrs);

  /* Generate the query */
  query << "SELECT "
        << backupCols
        << " FROM backup b ";

  /*
   * Check if we are instructed to consider every state
   * of basebackup or just the ones with "ready"
   */
  if (valid_only) {
    query << "WHERE status = 'ready' AND archive_id = ?1 ";
  }

  /*
   * Prepare the WHERE condition, depending on the
   * specified retrieval mode.
   */
  switch(mode) {
  case BASEBACKUP_OLDEST:
    {
      query << "ORDER BY stopped ASC LIMIT 1;";
      break;
    }
  case BASEBACKUP_NEWEST:
    {
      query << "ORDER BY stopped DESC LIMIT 1;";
      break;
    }
  }

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "DEBUG: execute SQL " << query.str();
#endif

  /* Prepare the query */
  rc = sqlite3_prepare(this->db_handle,
                       query.str().c_str(),
                       -1,
                       &stmt,
                       NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  if (sqlite3_bind_int(stmt, 1, archive_id) != SQLITE_OK) {
    ostringstream oss;

    oss << "could not bind archive_id(\""
        << archive_id << "\")"
        << " in catalog query: "
        << sqlite3_errmsg(this->db_handle);

    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /* Execute the query */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    ostringstream oss;

    oss << "error retrieving basebackup candidate: "
        << sqlite3_errmsg(this->db_handle);

    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /*
   * We expect only one tuple so far
   *
   * XXX: This might change if we introduce other retrieval modes
   *      in BaseBackupRetrieveMode...
   */
  if (rc == SQLITE_ROW) {

    this->fetchBackupIntoDescr(stmt, result,
                                 Range(0, backupAttrs.size() - 1));

  }

  sqlite3_finalize(stmt);
  return result;
}

/*
 * Currenty this repeats most of the logic of its
 * overload pendant, but i'm too lazy to refactor
 * the code atm.
 *
 * retention_mode is only allowed to operate on
 * either RETENTION_KEEP_NEWER_BY_DATETIME,
 * RETENTION_DROP_NEWER_BY_DATETIME, RETENTION_KEEP_OLDER_BY_DATETIME
 * or RETENTION_DROP_OLDER_BY_DATETIME.
 *
 * This makes four operation modes, but we are only interested
 * whether the datetime value is required to be older or newer
 * than the selected basebackups.
 */
std::vector<std::shared_ptr<BaseBackupDescr>>
BackupCatalog::getBackupListRetentionDateTime(std::string archive_name,
                                              RetentionIntervalDescr intervalDescr,
                                              RetentionRuleId retention_mode) {

  std::ostringstream query;
  std::vector<int> backupAttrs;
  std::vector<int> tblspcAttrs;
  sqlite3_stmt *stmt;

  std::string backupCols;
  std::vector<std::shared_ptr<BaseBackupDescr>> list;
  int current_backup_id = -1;
  int rc;
  std::string interval_expr = "";
  std::ostringstream retention_expr;

  /*
   * A retention interval without valid operands
   * is considered a no-op value here.
   */
  if (intervalDescr.opr_list.size() == 0) {
    throw CCatalogIssue("attempt to apply an empty retention interval to basebackup listing");
  }

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /*
   * Get the formatted datetime() expression from the
   * interval descriptor.
   */
  interval_expr = intervalDescr.sqlite3_datetime();

  /*
   * Build the retention expression.
   */
  switch(retention_mode) {

  case RETENTION_KEEP_OLDER_BY_DATETIME:
  case RETENTION_DROP_OLDER_BY_DATETIME:
    {
      retention_expr << "stopped < " << interval_expr;
      break;
    }

  case RETENTION_KEEP_NEWER_BY_DATETIME:
  case RETENTION_DROP_NEWER_BY_DATETIME:
    {
      retention_expr << "stopped > " << interval_expr;
      break;
    }

  default:
    throw CCatalogIssue("invalid retention mode when getting backup list");
  }

  /*
   * Generate list of columns to retrieve...
   */
  backupAttrs.push_back(SQL_BACKUP_ID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_ARCHIVE_ID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_XLOGPOS_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_XLOGPOSEND_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_TIMELINE_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_LABEL_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_FSENTRY_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STARTED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STOPPED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STATUS_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_SYSTEMID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_PINNED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_WAL_SEGMENT_SIZE_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_USED_PROFILE_ATTNO);

  /* computed columns to fetch */
  backupAttrs.push_back(SQL_BACKUP_COMPUTED_DURATION);

  tblspcAttrs.push_back(SQL_BCK_TBLSPC_BCK_ID_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCOID_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCLOC_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCSZ_ATTNO);

  backupCols = BackupCatalog::SQLgetColumnList(SQL_BACKUP_ENTITY,
                                               backupAttrs);

  /*
   * Get the list of backups for the specified archive.
   */
  query << "SELECT "
        << backupCols
        << ", "

    /*
     * IMPORTANT:
     *
     * We can't use SQLgetColumnList here(), since we want to
     * prevent NULL
     */

        << "COALESCE(bt.backup_id, -1) AS backup_id, "
        << "COALESCE(bt.spcoid, -1) AS spcoid, "
        << "COALESCE(spclocation, 'no location') AS spclocation, "
        << "COALESCE(spcsize, -1) AS spcsize, "
        << "b.wal_segment_size AS wal_segment_size, "
        << "b.used_profile AS backup_profile_id, "

    /*
     * Checking the datetime retention policy here is done directly
     * via SQLite3, but we have to make sure to fetch the value
     * manually. So watch out for its column index if you change something
     * here.
     */
        << retention_expr.str()
        << "FROM "
        << "backup b LEFT JOIN backup_tablespaces bt ON (b.id = bt.backup_id) "
        << "WHERE archive_id = (SELECT id FROM archive WHERE name = ?" << (intervalDescr.opr_list.size() + 1)
        << ") ORDER BY b.started DESC;";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "generate SQL: " << query.str();
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * First, bind retention datetime values. This makes
   * up the retention expression, which looks like:
   *
   * datetime(stopped, ?1, ?2, ?3, ...)
   */
  for (std::vector<RetentionIntervalOperand>::size_type i = 0;
       i != intervalDescr.opr_list.size(); i++) {

    RetentionIntervalOperand operand = intervalDescr.opr_list[i];
    std::string opr_value = operand.str();

    sqlite3_bind_text(stmt, i, opr_value.c_str(), -1, SQLITE_STATIC);
  }

  /*
   * ... and finally the archive_name
   */
  sqlite3_bind_text(stmt, intervalDescr.opr_list.size() + 1,
                    archive_name.c_str(), -1, SQLITE_STATIC);

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {

    ostringstream oss;

    oss << "error retrieving backup list from catalog database: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());

  }

  /*
   * Make sure we retrieve computed columns as well. We can
   * do this right here only, since otherwise SQLgetColumnList() would
   * have generated a wrong SELECT target list.
   */
  backupAttrs.push_back(SQL_BACKUP_COMPUTED_RETENTION_DATETIME);
  backupAttrs.push_back(SQL_BACKUP_COMPUTED_DURATION);

  /*
   * Retrieve list of backups.
   */
  while (rc == SQLITE_ROW) {

    shared_ptr<BaseBackupDescr> bbdescr = make_shared<BaseBackupDescr>();
    shared_ptr<BackupTablespaceDescr> tablespace = make_shared<BackupTablespaceDescr>();

    /* pivot pointer */
    shared_ptr<BaseBackupDescr> curr_descr = nullptr;

    bbdescr->setAffectedAttributes(backupAttrs);
    tablespace->setAffectedAttributes(tblspcAttrs);

    this->fetchBackupIntoDescr(stmt,
                               bbdescr,
                               Range(0, backupAttrs.size() - 1));

    /*
     * Since we fetch tablespace and basebackup information in one
     * query, we only push basebackup information if the last
     * encountered backup_id differs.
     */
    if (current_backup_id != bbdescr->id) {

      list.push_back(bbdescr);
      current_backup_id = bbdescr->id;

    }

    this->fetchBackupTablespaceIntoDescr(stmt,
                                         tablespace,

                                         /*
                                          * NOTE:
                                          *
                                          * The offset for tablespace columns always
                                          * starts after the attributes from the backup
                                          * catalog table
                                          */

                                         Range(backupAttrs.size(), backupAttrs.size() + tblspcAttrs.size() - 1));

    if (tablespace->backup_id >= 0) {

      /* Okay, looks like a valid tablespace entry */
      curr_descr = list.back();

      if (curr_descr == nullptr) {
        /* oops */
        sqlite3_finalize(stmt);
        throw CCatalogIssue("unexpected state in base backup list in getBackupList()");
      }

      curr_descr->tablespaces.push_back(tablespace);

    }

    /* Now move the result forward to the next row */
    rc = sqlite3_step(stmt);

  }

  sqlite3_finalize(stmt);
  return list;
}

std::vector<std::shared_ptr<BaseBackupDescr>>
BackupCatalog::getBackupList(std::string archive_name) {

  std::ostringstream query;
  std::vector<int> backupAttrs;
  std::vector<int> tblspcAttrs;
  sqlite3_stmt *stmt;

  std::string backupCols;
  std::vector<std::shared_ptr<BaseBackupDescr>> list;
  int current_backup_id = -1;
  int rc;

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /*
   * Generate list of columns to retrieve...
   */
  backupAttrs.push_back(SQL_BACKUP_ID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_ARCHIVE_ID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_XLOGPOS_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_XLOGPOSEND_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_TIMELINE_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_LABEL_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_FSENTRY_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STARTED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STOPPED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_STATUS_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_SYSTEMID_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_PINNED_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_WAL_SEGMENT_SIZE_ATTNO);
  backupAttrs.push_back(SQL_BACKUP_USED_PROFILE_ATTNO);

  /* computed columns to fetch */
  backupAttrs.push_back(SQL_BACKUP_COMPUTED_DURATION);

  tblspcAttrs.push_back(SQL_BCK_TBLSPC_BCK_ID_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCOID_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCLOC_ATTNO);
  tblspcAttrs.push_back(SQL_BCK_TBLSPC_SPCSZ_ATTNO);

  backupCols = BackupCatalog::SQLgetColumnList(SQL_BACKUP_ENTITY,
                                               backupAttrs);

  /*
   * Get the list of backups for the specified archive.
   */
  query << "SELECT "
        << backupCols
        << ", "

    /*
     * IMPORTANT:
     *
     * We can't use SQLgetColumnList here(), since we want to
     * prevent NULL
     */

        << "COALESCE(bt.backup_id, -1) AS backup_id, "
        << "COALESCE(bt.spcoid, -1) AS spcoid, "
        << "COALESCE(spclocation, 'no location') AS spclocation, "
        << "COALESCE(spcsize, -1) AS spcsize, "
        << "b.wal_segment_size AS wal_segment_size, "
        << "b.used_profile AS backup_profile_id "
        << "FROM "
        << "backup b LEFT JOIN backup_tablespaces bt ON (b.id = bt.backup_id) "
        << "WHERE archive_id = (SELECT id FROM archive WHERE name = ?1) ORDER BY b.started DESC;";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "generate SQL: " << query.str();
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_bind_text(stmt, 1, archive_name.c_str(), -1, SQLITE_STATIC);

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {

    ostringstream oss;

    oss << "error retrieving backup list from catalog database: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());

  }

  /*
   * Retrieve list of backups.
   */
  while (rc == SQLITE_ROW) {

    shared_ptr<BaseBackupDescr> bbdescr = make_shared<BaseBackupDescr>();
    shared_ptr<BackupTablespaceDescr> tablespace = make_shared<BackupTablespaceDescr>();

    /* pivot pointer */
    shared_ptr<BaseBackupDescr> curr_descr = nullptr;

    bbdescr->setAffectedAttributes(backupAttrs);
    tablespace->setAffectedAttributes(tblspcAttrs);

    this->fetchBackupIntoDescr(stmt,
                               bbdescr,
                               Range(0, backupAttrs.size() - 1));

    /*
     * Since we fetch tablespace and basebackup information in one
     * query, we only push basebackup information if the last
     * encountered backup_id differs.
     */
    if (current_backup_id != bbdescr->id) {

      list.push_back(bbdescr);
      current_backup_id = bbdescr->id;

    }

    this->fetchBackupTablespaceIntoDescr(stmt,
                                         tablespace,

                                         /*
                                          * NOTE:
                                          *
                                          * The offset for tablespace columns always
                                          * starts after the attributes from the backup
                                          * catalog table
                                          */

                                         Range(backupAttrs.size(), backupAttrs.size() + tblspcAttrs.size() - 1));

    if (tablespace->backup_id >= 0) {

      /* Okay, looks like a valid tablespace entry */
      curr_descr = list.back();

      if (curr_descr == nullptr) {
        /* oops */
        sqlite3_finalize(stmt);
        throw CCatalogIssue("unexpected state in base backup list in getBackupList()");
      }

      curr_descr->tablespaces.push_back(tablespace);

    }

    /* Now move the result forward to the next row */
    rc = sqlite3_step(stmt);

  }

  sqlite3_finalize(stmt);
  return list;
}

void BackupCatalog::updateArchiveAttributes(shared_ptr<CatalogDescr> descr,
                                            std::vector<int> affectedAttributes) {
  sqlite3_stmt *stmt;
  ostringstream updateSQL;
  int rc;
  unsigned int boundCols = 0;

  if(!this->available())
    throw CCatalogIssue("catalog database not opened");

  /*
   * In case affectedAttributes is empty (or NULL), we throw an
   * error.
   */
  if (affectedAttributes.size() <= 0)
    throw CCatalogIssue("cannot update archive attributes with empty attribute list");

  /*
   * Loop through the affected columns list and build
   * a comma separated list of col=? pairs.
   */
  updateSQL << "UPDATE archive SET ";

  /*
   * Use an ordinary indexed loop ...
   */
  for(boundCols = 0; boundCols < affectedAttributes.size(); boundCols++) {

    /* boundCol must start at index 1 !! */
    updateSQL << BackupCatalog::SQLgetUpdateColumnTarget(SQL_ARCHIVE_ENTITY,
                                                         affectedAttributes[boundCols])
              << (boundCols + 1);

    if (boundCols < affectedAttributes.size() - 1) {
      updateSQL << ", ";
    }
  }

  /*
   * ...and attach the where clause
   */
  ++boundCols;
  updateSQL << " WHERE id = ?" << boundCols << ";";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "generate UPDATE SQL " << updateSQL.str();
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          updateSQL.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Assign bind variables. Please note that we rely
   * on the order of affectedAttributes to match the
   * previously formatted UPDATE SQL string.
   */
  this->SQLbindArchiveAttributes(descr,
                                 affectedAttributes,
                                 stmt,
                                 Range(1, boundCols));

  sqlite3_bind_int(stmt, boundCols,
                   descr->id);

  /*
   * Execute the statement.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "error updating archive in catalog database: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);

  /*
   * Also update connection info, but only
   * if affected attributes are set accordingly.
   */
  if (descr->coninfo->getAffectedAttributes().size() > 0) {
    this->updateCatalogConnection(descr->coninfo,
                                  descr->archive_name,
                                  ConnectionDescr::CONNECTION_TYPE_BASEBACKUP);
  }
}

std::string BackupCatalog::mapAttributeId(int catalogEntity,
                                          int colId) {

  std::string result = "";

  switch(catalogEntity) {

  case SQL_ARCHIVE_ENTITY:
    result = BackupCatalog::archiveCatalogCols[colId];
    break;
  case SQL_BACKUP_ENTITY:
    result = BackupCatalog::backupCatalogCols[colId];
    break;
  case SQL_STREAM_ENTITY:
    result = BackupCatalog::streamCatalogCols[colId];
    break;
  case SQL_BACKUP_PROFILES_ENTITY:
    result = BackupCatalog::backupProfilesCatalogCols[colId];
    break;
  case SQL_BACKUP_TBLSPC_ENTITY:
    result = BackupCatalog::backupTablespacesCatalogCols[colId];
    break;
  case SQL_PROCS_ENTITY:
    result = BackupCatalog::procsCatalogCols[colId];
    break;
  case SQL_CON_ENTITY:
    result = BackupCatalog::connectionsCatalogCols[colId];
    break;
  case SQL_RETENTION_ENTITY:
    result = BackupCatalog::retentionCatalogCols[colId];
    break;
  case SQL_RETENTION_RULES_ENTITY:
    result = BackupCatalog::retentionRulesCatalogCols[colId];
    break;
  default:
    {
      std::ostringstream oss;
      oss << "unkown catalog entity: " << catalogEntity;
      throw CCatalogIssue(oss.str());
    }
  }

  return result;
}

string BackupCatalog::SQLgetColumnList(int catalogEntity, std::vector<int> attrs) {

  unsigned int i;
  std::ostringstream collist;

  for (i = 0; i < attrs.size(); i++) {
    collist << BackupCatalog::mapAttributeId(catalogEntity, attrs[i]);

    if (i < (attrs.size() - 1))
      collist << ", ";
  }

  return collist.str();

}

string BackupCatalog::SQLgetUpdateColumnTarget(int catalogEntity,
                                               int colId) {
  ostringstream oss;

  oss << BackupCatalog::mapAttributeId(catalogEntity, colId) << "=?";
  return oss.str();
}

std::shared_ptr<std::list<std::shared_ptr<BackupProfileDescr>>>
BackupCatalog::getBackupProfiles() {

  sqlite3_stmt *stmt;
  auto result = make_shared<std::list<std::shared_ptr<BackupProfileDescr>>>();

  /*
   * Build the query.
   */
  ostringstream query;
  Range range(0, 10);

  query << "SELECT id, name, compress_type, max_rate, label, "
        << "fast_checkpoint, include_wal, wait_for_wal, noverify_checksums, "
        << "manifest, manifest_checksums "
        << "FROM backup_profiles ORDER BY name;";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "QUERY: " << query.str();
#endif

  std::vector<int> attr;
  attr.push_back(SQL_BCK_PROF_ID_ATTNO);
  attr.push_back(SQL_BCK_PROF_NAME_ATTNO);
  attr.push_back(SQL_BCK_PROF_COMPRESS_TYPE_ATTNO);
  attr.push_back(SQL_BCK_PROF_MAX_RATE_ATTNO);
  attr.push_back(SQL_BCK_PROF_LABEL_ATTNO);
  attr.push_back(SQL_BCK_PROF_FAST_CHKPT_ATTNO);
  attr.push_back(SQL_BCK_PROF_INCL_WAL_ATTNO);
  attr.push_back(SQL_BCK_PROF_WAIT_FOR_WAL_ATTNO);
  attr.push_back(SQL_BCK_PROF_NOVERIFY_CHECKSUMS_ATTNO);
  attr.push_back(SQL_BCK_PROF_MANIFEST_ATTNO);
  attr.push_back(SQL_BCK_PROF_MANIFEST_CHECKSUMS_ATTNO);

  int rc = sqlite3_prepare_v2(this->db_handle,
                              query.str().c_str(),
                              -1,
                              &stmt,
                              NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Execute the prepared statement
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc!= SQLITE_DONE) {
    ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  try {

    while(rc == SQLITE_ROW) {
      auto item = make_shared<BackupProfileDescr>();
      item->setAffectedAttributes(attr);
      this->fetchBackupProfileIntoDescr(stmt, item, range);
      result->push_back(item);

      rc = sqlite3_step(stmt);
    }

  } catch(exception& e) {
    /* re-throw exception, but don't leak the sqlite statement handle */
    sqlite3_finalize(stmt);
    throw e;
  }

  sqlite3_finalize(stmt);
  return result;
}

std::shared_ptr<BackupProfileDescr> BackupCatalog::getBackupProfile(int profile_id) {

  std::shared_ptr<BackupProfileDescr> descr = std::make_shared<BackupProfileDescr>();
  sqlite3_stmt *stmt;
  int rc;
  std::ostringstream query;
  Range range(0, 10);

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /*
   * Any negative id is treated as an error internally.
   */
  if (profile_id < 0) {
    ostringstream oss;

    oss << "invalid backup profile id (\"" << profile_id
        << "\") while trying to fetch backup profile data";
    throw CCatalogIssue(oss.str());
  }

  /*
   * Build the query
   */
  query << "SELECT id, name, compress_type, max_rate, label, "
        << "fast_checkpoint, include_wal, wait_for_wal, noverify_checksums, "
        << "manifest, manifest_checksums "
        << "FROM backup_profiles WHERE id = ?1;";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "QUERY : " << query.str();
#endif

  /*
   * Prepare the query
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  /* Should match column order of query */
  descr->pushAffectedAttribute(SQL_BCK_PROF_ID_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_NAME_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_COMPRESS_TYPE_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_MAX_RATE_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_LABEL_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_FAST_CHKPT_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_INCL_WAL_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_WAIT_FOR_WAL_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_NOVERIFY_CHECKSUMS_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_MANIFEST_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_MANIFEST_CHECKSUMS_ATTNO);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind the values.
   */
  sqlite3_bind_int(stmt, 1, profile_id);

  /* ...and execute ... */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  try {
    /*
     * At this point only a single tuple expected...
     */
    if (rc == SQLITE_ROW) {
      this->fetchBackupProfileIntoDescr(stmt, descr, range);
    }

  } catch (exception &e) {
    /* don't leak sqlite3 statement handle */
    sqlite3_finalize(stmt);
    /* re-throw exception for caller */
    throw e;
  }

  sqlite3_finalize(stmt);
  return descr;

}

std::shared_ptr<BackupProfileDescr> BackupCatalog::getBackupProfile(std::string name) {

  std::shared_ptr<BackupProfileDescr> descr = std::make_shared<BackupProfileDescr>();

  sqlite3_stmt *stmt;
  int rc;
  std::ostringstream query;
  Range range(0, 10);

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /*
   * Build the query
   */
  query << "SELECT id, name, compress_type, max_rate, label, "
        << "fast_checkpoint, include_wal, wait_for_wal, noverify_checksums, "
        << "manifest, manifest_checksums "
        << "FROM backup_profiles WHERE name = ?1;";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "QUERY : " << query.str();
#endif

  /*
   * Prepare the query
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  /* Should match column order of query */
  descr->pushAffectedAttribute(SQL_BCK_PROF_ID_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_NAME_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_COMPRESS_TYPE_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_MAX_RATE_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_LABEL_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_FAST_CHKPT_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_INCL_WAL_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_WAIT_FOR_WAL_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_NOVERIFY_CHECKSUMS_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_MANIFEST_ATTNO);
  descr->pushAffectedAttribute(SQL_BCK_PROF_MANIFEST_CHECKSUMS_ATTNO);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind the values.
   */
  sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_STATIC);

  /* ...and execute ... */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  try {
    /*
     * At this point only a single tuple expected...
     */
    if (rc == SQLITE_ROW) {
      this->fetchBackupProfileIntoDescr(stmt, descr, range);
    }

  } catch (exception &e) {
    /* don't leak sqlite3 statement handle */
    sqlite3_finalize(stmt);
    /* re-throw exception for caller */
    throw e;
  }

  sqlite3_finalize(stmt);
  return descr;
}

void BackupCatalog::createBackupProfile(std::shared_ptr<BackupProfileDescr> profileDescr) {

  sqlite3_stmt *stmt;
  std::ostringstream insert;
  int rc;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  insert << "INSERT INTO backup_profiles("
         << "name, compress_type, max_rate, label, "
         << "fast_checkpoint, include_wal, wait_for_wal, noverify_checksums, manifest, manifest_checksums) "
         << "VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10);";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "createBackupProfile query: " << insert.str();
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          insert.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind new backup profile data.
   */
  Range range(1, 10);
  this->SQLbindBackupProfileAttributes(profileDescr,
                                       profileDescr->getAffectedAttributes(),
                                       stmt,
                                       range);
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;

    oss << "error creating backup profile in catalog database: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);

    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::createArchive(shared_ptr<CatalogDescr> descr) {

  sqlite3_stmt *stmt;
  int rc;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  if (descr->coninfo->type != ConnectionDescr::CONNECTION_TYPE_BASEBACKUP)
    throw CCatalogIssue("archives can create connections of type basebackup only");

  rc = sqlite3_prepare_v2(this->db_handle,
                          "INSERT INTO archive(name, directory, compression) "
                          "VALUES (?1, ?2, ?3);",
                          -1,
                          &stmt,
                          NULL);
  sqlite3_bind_text(stmt, 1,
                    descr->archive_name.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 2,
                    descr->directory.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 3,
                   descr->compression);

  // sqlite3_bind_text(stmt, 4,
  //                   descr->pghost.c_str(), -1 , SQLITE_STATIC);
  // sqlite3_bind_int(stmt, 5,
  //                  descr->pgport);
  // sqlite3_bind_text(stmt, 6,
  //                   descr->pguser.c_str(), -1, SQLITE_STATIC);
  // sqlite3_bind_text(stmt, 7,
  //                   descr->pgdatabase.c_str(), -1, SQLITE_STATIC);

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;

    oss << "error creating archive in catalog database: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);

    throw CCatalogIssue(oss.str());
  }

  /*
   * Initialize the CatalogDescr with its
   * new ID.
   */
  descr->setArchiveId(sqlite3_last_insert_rowid(this->db_handle));
  sqlite3_finalize(stmt);

}

int BackupCatalog::SQLbindBackupTablespaceAttributes(std::shared_ptr<BackupTablespaceDescr> tblspcDescr,
                                                     std::vector<int> affectedAttributes,
                                                     sqlite3_stmt *stmt,
                                                     Range range) {
  int result = range.start();

  if (stmt == NULL) {
    throw CCatalogIssue("cannot bind updated attributes: invalid statement handle");
  }

  for (auto& colId : affectedAttributes) {

    /*
     * Stop, if result has reached end of range.
     */
    if (result > range.end())
      break;

    switch(colId) {

    case SQL_BCK_TBLSPC_BCK_ID_ATTNO:
      sqlite3_bind_int(stmt, result, tblspcDescr->backup_id);
      break;

    case SQL_BCK_TBLSPC_SPCOID_ATTNO:
      sqlite3_bind_int(stmt, result, tblspcDescr->spcoid);
      break;

    case SQL_BCK_TBLSPC_SPCLOC_ATTNO:
      sqlite3_bind_text(stmt, result,
                        tblspcDescr->spclocation.c_str(),
                        -1,
                        SQLITE_STATIC);
      break;

    case SQL_BCK_TBLSPC_SPCSZ_ATTNO:
      sqlite3_bind_int(stmt, result, tblspcDescr->spcsize);
      break;

    default:
      {
        ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }

    result++;
  }

  return result;
}

int BackupCatalog::SQLbindConnectionAttributes(std::shared_ptr<ConnectionDescr> conDescr,
                                               std::vector<int> affectedAttributes,
                                               sqlite3_stmt *stmt,
                                               Range range) {
  int result = range.start();

  if (stmt == NULL)
    throw CCatalogIssue("cannot bind updated attributes: invalid statement handle");

  for (auto& colId : affectedAttributes) {
    /*
     * Stop, if result has reached end of range.
     */
    if (result > range.end())
      break;

    switch(colId) {

    case SQL_CON_ARCHIVE_ID_ATTNO:
      sqlite3_bind_int(stmt, result, conDescr->archive_id);
      break;

    case SQL_CON_TYPE_ATTNO:
      /* type cannot be null */
      sqlite3_bind_text(stmt, result,
                        conDescr->type.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_CON_PGHOST_ATTNO:
      sqlite3_bind_text(stmt, result,
                        conDescr->pghost.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_CON_PGPORT_ATTNO:
      sqlite3_bind_int(stmt,
                       result,
                       conDescr->pgport);
      break;

    case SQL_CON_PGUSER_ATTNO:
      sqlite3_bind_text(stmt, result,
                        conDescr->pguser.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_CON_PGDATABASE_ATTNO:
      sqlite3_bind_text(stmt, result,
                        conDescr->pgdatabase.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_CON_DSN_ATTNO:
      sqlite3_bind_text(stmt, result,
                        conDescr->dsn.c_str(),
                        -1, SQLITE_STATIC);
      break;

    default:
      {
        std::ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }
    result ++;
  }

  return result;
}

int BackupCatalog::SQLbindProcsAttributes(std::shared_ptr<CatalogProc> procInfo,
                                          std::vector<int> affectedAttributes,
                                          sqlite3_stmt *stmt,
                                          Range range) {

  int result = range.start();

  if (stmt == NULL)
    throw CCatalogIssue("cannot bind updated attributes: invalid statement handle");

  for (auto& colId : affectedAttributes) {

    /*
     * Stop, if result has reached end of range.
     */
    if (result > range.end())
      break;

    switch(colId) {

    case SQL_PROCS_PID_ATTNO:
      sqlite3_bind_int(stmt, result, procInfo->pid);
      break;

    case SQL_PROCS_ARCHIVE_ID_ATTNO:
      sqlite3_bind_int(stmt, result, procInfo->archive_id);
      break;

    case SQL_PROCS_TYPE_ATTNO:
      sqlite3_bind_text(stmt, result,
                        procInfo->type.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_PROCS_STARTED_ATTNO:
      sqlite3_bind_text(stmt, result,
                        procInfo->started.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_PROCS_STATE_ATTNO:
      sqlite3_bind_text(stmt, result,
                        procInfo->state.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_PROCS_SHM_KEY_ATTNO:
      sqlite3_bind_int(stmt, result, procInfo->shm_key);
      break;

    case SQL_PROCS_SHM_ID_ATTNO:
      sqlite3_bind_int(stmt, result, procInfo->shm_id);
      break;

    default:
      {
        ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }

    result++;
  }

  return result;
}

int BackupCatalog::SQLbindBackupAttributes(std::shared_ptr<BaseBackupDescr> bbdescr,
                                           sqlite3_stmt *stmt,
                                           Range range) {

  int result = range.start();

  if (stmt == NULL)
    throw CCatalogIssue("cannot bind attributes: invalid statement handle");

  for (auto& colId : bbdescr->getAffectedAttributes()) {

    /*
     * Stop, if result has reached end of range.
     */
    switch(colId) {
    case SQL_BACKUP_ID_ATTNO:
      sqlite3_bind_int(stmt, result, bbdescr->id);
      break;

    case SQL_BACKUP_ARCHIVE_ID_ATTNO:
      sqlite3_bind_int(stmt, result, bbdescr->archive_id);
      break;

    case SQL_BACKUP_XLOGPOS_ATTNO:
      sqlite3_bind_text(stmt, result,
                        bbdescr->xlogpos.c_str(), -1, SQLITE_STATIC);
        break;

    case SQL_BACKUP_XLOGPOSEND_ATTNO:
      sqlite3_bind_text(stmt, result,
                        bbdescr->xlogposend.c_str(), -1, SQLITE_STATIC);
      break;

    case SQL_BACKUP_TIMELINE_ATTNO:
      sqlite3_bind_int(stmt, result, bbdescr->timeline);
      break;

    case SQL_BACKUP_LABEL_ATTNO:
      sqlite3_bind_text(stmt, result,
                        bbdescr->label.c_str(), -1, SQLITE_STATIC);
      break;

    case SQL_BACKUP_FSENTRY_ATTNO:
      sqlite3_bind_text(stmt, result,
                        bbdescr->fsentry.c_str(), -1, SQLITE_STATIC);
      break;

    case SQL_BACKUP_STARTED_ATTNO:
      sqlite3_bind_text(stmt, result,
                        bbdescr->started.c_str(), -1, SQLITE_STATIC);
      break;

    case SQL_BACKUP_STOPPED_ATTNO:
      sqlite3_bind_text(stmt, result,
                        bbdescr->stopped.c_str(), -1, SQLITE_STATIC);
      break;

    case SQL_BACKUP_PINNED_ATTNO:
      sqlite3_bind_int(stmt, result, bbdescr->pinned);
      break;

    case SQL_BACKUP_STATUS_ATTNO:
      sqlite3_bind_text(stmt, result,
                        bbdescr->status.c_str(), -1, SQLITE_STATIC);
      break;

    case SQL_BACKUP_SYSTEMID_ATTNO:
      sqlite3_bind_text(stmt, result,
                        bbdescr->systemid.c_str(), -1, SQLITE_STATIC);
      break;

    case SQL_BACKUP_WAL_SEGMENT_SIZE_ATTNO:
      sqlite3_bind_int(stmt, result,
                       bbdescr->wal_segment_size);
      break;

    case SQL_BACKUP_USED_PROFILE_ATTNO:
      sqlite3_bind_int(stmt, result,
                       bbdescr->used_profile);
      break;

    case SQL_BACKUP_COMPUTED_RETENTION_DATETIME:
      /* computed values must not be bound */
      throw CCatalogIssue("attempt to bind expression column exceeds_retention_rule");
      break; /* not reached */

    case SQL_BACKUP_COMPUTED_DURATION:
      /* computed values must not be bound */
      throw CCatalogIssue("attempt to bind expression column duration");
      break; /* not reached */

    default:
      {
        ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }

    result++;
  }

  return result;

}

int BackupCatalog::SQLbindRetentionRuleAttributes(shared_ptr<RetentionRuleDescr> rule,
                                                  vector<int> &affectedAttributes,
                                                  sqlite3_stmt *stmt,
                                                  Range range) {

  int result = range.start();

  if (stmt == NULL)
    throw CCatalogIssue("cannot bind retention rule attributes: invalid statement handle");

  for (auto &colId : affectedAttributes) {

    if (result > range.end())
      break;

    switch(colId) {

    case SQL_RETENTION_RULES_ID_ATTNO:
      sqlite3_bind_int(stmt, result, rule->id);
      break;
    case SQL_RETENTION_RULES_TYPE_ATTNO:
      sqlite3_bind_int(stmt, result, (int) rule->type);
      break;
    case SQL_RETENTION_RULES_VALUE_ATTNO:
      sqlite3_bind_text(stmt, result,
                        rule->value.c_str(), -1, SQLITE_STATIC);
      break;

    default:
      {
        ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }

    result++;
  }

  return result;
}

int BackupCatalog::SQLbindRetentionPolicyAttributes(shared_ptr<RetentionDescr> retention,
                                                    vector<int> &affectedAttributes,
                                                    sqlite3_stmt *stmt,
                                                    Range range) {

  int result = range.start();

  if (stmt == NULL)
    throw CCatalogIssue("cannot bind retention attributes: invalid statement handle");

  for (auto &colId : affectedAttributes) {

    /*
     * Stop, if result has reached end of range.
     */
    if (result > range.end())
      break;

    switch (colId) {

    case SQL_RETENTION_ID_ATTNO:
      sqlite3_bind_int(stmt, result, retention->id);
      break;
    case SQL_RETENTION_NAME_ATTNO:
      sqlite3_bind_text(stmt, result,
                        retention->name.c_str(), -1, SQLITE_STATIC);
      break;
    case SQL_RETENTION_CREATED_ATTNO:
      sqlite3_bind_text(stmt, result,
                        retention->created.c_str(), -1, SQLITE_STATIC);
      break;

    default:
      {
        ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }

    result++;

  }

  return result;
}

int BackupCatalog::SQLbindBackupProfileAttributes(std::shared_ptr<BackupProfileDescr> profileDescr,
                                                  std::vector<int> affectedAttributes,
                                                  sqlite3_stmt *stmt,
                                                  Range range) {


  int result = range.start();

  if (stmt == NULL)
    throw CCatalogIssue("cannot bind updated attributes: invalid statement handle");

  for (auto& colId : affectedAttributes) {

    /*
     * Stop, if result has reached end of range.
     */
    if (result > range.end())
      break;

    switch(colId) {

    case SQL_BCK_PROF_ID_ATTNO:
      sqlite3_bind_int(stmt, result, profileDescr->profile_id);
      break;

    case SQL_BCK_PROF_NAME_ATTNO:
      sqlite3_bind_text(stmt, result,
                        profileDescr->name.c_str(), -1, SQLITE_STATIC);
      break;

    case SQL_BCK_PROF_COMPRESS_TYPE_ATTNO:
      sqlite3_bind_int(stmt, result, profileDescr->compress_type);
      break;

    case SQL_BCK_PROF_MAX_RATE_ATTNO:
      sqlite3_bind_int(stmt, result, profileDescr->max_rate);
      break;

    case SQL_BCK_PROF_LABEL_ATTNO:
      sqlite3_bind_text(stmt, result,
                        profileDescr->label.c_str(), -1, SQLITE_STATIC);
      break;

    case SQL_BCK_PROF_FAST_CHKPT_ATTNO:
      sqlite3_bind_int(stmt, result, profileDescr->fast_checkpoint);
      break;

    case SQL_BCK_PROF_INCL_WAL_ATTNO:
      sqlite3_bind_int(stmt, result, profileDescr->include_wal);
      break;

    case SQL_BCK_PROF_WAIT_FOR_WAL_ATTNO:
      sqlite3_bind_int(stmt, result, profileDescr->wait_for_wal);
      break;

    case SQL_BCK_PROF_NOVERIFY_CHECKSUMS_ATTNO:
      sqlite3_bind_int(stmt, result, profileDescr->noverify_checksums);
      break;

    case SQL_BCK_PROF_MANIFEST_ATTNO:
      sqlite3_bind_int(stmt, result, profileDescr->manifest);
      break;

    case SQL_BCK_PROF_MANIFEST_CHECKSUMS_ATTNO:
      sqlite3_bind_text(stmt, result,
                        profileDescr->manifest_checksums.c_str(), -1, SQLITE_STATIC);
      break;

    default:
      {
        ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }

    result++;
  }

  return result;
}

int BackupCatalog::SQLbindStreamAttributes(StreamIdentification &ident,
                                           std::vector<int> affectedAttributes,
                                           sqlite3_stmt *stmt,
                                           Range range) {

  int result = range.start();

  if (stmt == NULL)
    throw CCatalogIssue("cannot bind updated attributes: invalid statement handle");

  for (auto& colId : affectedAttributes) {

    /*
     * Stop, if result has reached end of range.
     */
    if (result > range.end())
      break;

    switch(colId) {

    case SQL_STREAM_ID_ATTNO:
      sqlite3_bind_int(stmt, result, ident.id);
      break;

    case SQL_STREAM_ARCHIVE_ID_ATTNO:
      sqlite3_bind_int(stmt, result, ident.archive_id);
      break;

    case SQL_STREAM_STYPE_ATTNO:
      sqlite3_bind_text(stmt, result,
                        ident.stype.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_STREAM_SLOT_NAME_ATTNO:
      sqlite3_bind_text(stmt, result,
                        ident.slot_name.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_STREAM_SYSTEMID_ATTNO:
      sqlite3_bind_text(stmt, result,
                        ident.systemid.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_STREAM_TIMELINE_ATTNO:
      sqlite3_bind_int(stmt, result, ident.timeline);
      break;

    case SQL_STREAM_XLOGPOS_ATTNO:
      sqlite3_bind_text(stmt, result,
                        ident.xlogpos.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_STREAM_DBNAME_ATTNO:
      sqlite3_bind_text(stmt, result,
                        ident.dbname.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_STREAM_STATUS_ATTNO:
      sqlite3_bind_text(stmt, result,
                        ident.status.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_STREAM_REGISTER_DATE_ATTNO:
      sqlite3_bind_text(stmt, result,
                        ident.create_date.c_str(),
                        -1, SQLITE_STATIC);
      break;

    default:
      {
        ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }

    /* increment column index */
    result++;
  }

  return result;
}

int BackupCatalog::SQLbindArchiveAttributes(shared_ptr<CatalogDescr> descr,
                                            std::vector<int> affectedAttributes,
                                            sqlite3_stmt *stmt,
                                            Range range) {

  int result = range.start();

  if (stmt == NULL)
    throw CCatalogIssue("cannot bind updated attributes: invalid statement handle");

  for (auto& colId : affectedAttributes) {

    /*
     * Stop, if result has reached end of range.
     */
    if (result > range.end())
      break;

    switch(colId) {

    case SQL_ARCHIVE_ID_ATTNO:
      sqlite3_bind_int(stmt, result,
                      descr->id);
      break;

    case SQL_ARCHIVE_NAME_ATTNO:
      sqlite3_bind_text(stmt, result,
                        descr->archive_name.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_ARCHIVE_DIRECTORY_ATTNO:
      sqlite3_bind_text(stmt, result,
                        descr->directory.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_ARCHIVE_COMPRESSION_ATTNO:
      sqlite3_bind_int(stmt, result,
                       descr->compression);
      break;

    case SQL_ARCHIVE_PGHOST_ATTNO:
      sqlite3_bind_text(stmt, result,
                        descr->coninfo->pghost.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_ARCHIVE_PGPORT_ATTNO:
      sqlite3_bind_int(stmt, result,
                       descr->coninfo->pgport);
      break;

    case SQL_ARCHIVE_PGUSER_ATTNO:
      sqlite3_bind_text(stmt, result,
                        descr->coninfo->pguser.c_str(),
                        -1, SQLITE_STATIC);
      break;

    case SQL_ARCHIVE_PGDATABASE_ATTNO:
      sqlite3_bind_text(stmt, result,
                        descr->coninfo->pgdatabase.c_str(),
                        -1, SQLITE_STATIC);
      break;

    default:
      {
        ostringstream oss;
        oss << "invalid column index: \"" << colId << "\"";
        throw CCatalogIssue(oss.str());
      }
    }

    result++;
  }

  return result;
}


void BackupCatalog::close() {
  if (available()) {

    int rc = sqlite3_close(this->db_handle);

    if (rc == SQLITE_OK) {
      this->isOpen    = false;
      this->db_handle = NULL;

    } else if (rc == SQLITE_BUSY) {
      throw CCatalogIssue("attempt to close busy database connection");
    }

  }
}

BackupCatalog::~BackupCatalog() {
  if (available())
    this->close();
}

std::string BackupCatalog::affectedColumnsToString(std::vector<int> affectedAttributes) {

  ostringstream result;

  for (unsigned int i = 0; i < affectedAttributes.size(); i++) {
    result << archiveCatalogCols[affectedAttributes[i]];

    if (i < (affectedAttributes.size() - 1))
      result << ", ";

  }

  return result.str();
}

std::string BackupCatalog::SQLmakePlaceholderList(std::vector<int> affectedAttributes) {

  ostringstream result;

  for(unsigned int i = 1; i <= affectedAttributes.size(); i++) {
    result << "?" << i;

    if (i < affectedAttributes.size())
      result << ", ";
  }

  return result.str();
}

std::string BackupCatalog::affectedColumnsToString(int entity,
                                                   std::vector<int> affectedAttributes,
                                                   std::string prefix) {

  ostringstream result;

  /*
   * prefix can't be zero length
   */
  if (prefix.length() < 1) {
    throw CCatalogIssue("affectedColumnsToString() can't be used with zero-length prefix");
  }

  for (unsigned int colId = 0; colId < affectedAttributes.size(); colId++) {
    result << prefix << "." << this->mapAttributeId(entity, affectedAttributes[colId]);

    if (colId < (affectedAttributes.size() -1))
      result << ", ";
  }

  return result.str();
}

std::string BackupCatalog::affectedColumnsToString(int entity,
                                                   std::vector<int> affectedAttributes) {

  ostringstream result;

  for (unsigned int colId = 0; colId < affectedAttributes.size(); colId++) {
    result << this->mapAttributeId(entity, affectedAttributes[colId]);

    if (colId < (affectedAttributes.size() -1))
      result << ", ";
  }

  return result.str();

}

std::string BackupCatalog::SQLgetFilterForArchive(std::shared_ptr<CatalogDescr> descr,
                                                  std::vector<int> affectedAttributes,
                                                  Range rangeBindID,
                                                  std::string op) {

  ostringstream result;
  int bindId = rangeBindID.start();

  for(auto colId : affectedAttributes) {
    result << archiveCatalogCols[colId] << "=?" << bindId;

    if (bindId < rangeBindID.end())
      result << " " << op << " ";

    bindId++;
  }

  return result.str();
}

std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> BackupCatalog::getArchiveList(std::shared_ptr<CatalogDescr> descr,
                                                                                        std::vector<int> affectedAttributes) {

  /*
   * Statement handle
   */
  sqlite3_stmt *stmt;

  /*
   * Result map for filtered list
   */
  auto result = make_shared<std::list<std::shared_ptr<CatalogDescr>>>();

  /*
   * Build the filtered query.
   */
  ostringstream query;
  Range range(1, affectedAttributes.size());

  query << "SELECT a.id, a.name, a.directory, a.compression, c.pghost, "
        << "c.pgport, c.pguser, c.pgdatabase, c.dsn "
        << " FROM archive a JOIN connections c ON c.archive_id = a.id"
        << " WHERE c.type = 'basebackup' AND "
        << this->SQLgetFilterForArchive(descr,
                                        affectedAttributes,
                                        range,
                                        " OR ")
        << " ORDER BY name;";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "QUERY: " << query.str();
#endif

  /*
   * Prepare the query.
   */
  int rc = sqlite3_prepare_v2(this->db_handle,
                              query.str().c_str(),
                              -1,
                              &stmt,
                              NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  this->SQLbindArchiveAttributes(descr,
                                 affectedAttributes,
                                 stmt,
                                 range);

  /*
   * ... and execute the statement.
   */

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc!= SQLITE_DONE) {
    ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  try {

    while(rc == SQLITE_ROW) {

      auto item = make_shared<CatalogDescr>();

      this->fetchArchiveDataIntoDescr(stmt, item);
      result->push_back(item);
      rc = sqlite3_step(stmt);

    }

  } catch(exception& e) {
    /* re-throw exception, but don't leak sqlite statement handle */
    sqlite3_finalize(stmt);
    throw e;
  }

  sqlite3_finalize(stmt);
  return result;
}

shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> BackupCatalog::getArchiveList() {

  /*
   * Statement handle for SQLite3 catalog db.
   */
  sqlite3_stmt *stmt;

  /*
   * Prepare the result map.
   */
  auto result = make_shared<std::list<std::shared_ptr<CatalogDescr>>>();

  /*
   * Build the query.
   */
  std::ostringstream query;
  query << "SELECT a.id, a.name, a.directory, a.compression, c.pghost, c.pgport, "
        << "c.pguser, c.pgdatabase, c.dsn "
        << "FROM archive a JOIN connections c ON c.archive_id = a.id "
        << "WHERE c.type = 'basebackup' ORDER BY name";

  int rc = sqlite3_prepare_v2(this->db_handle,
                              query.str().c_str(),
                              -1,
                              &stmt,
                              NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "cannot prepare query: " << sqlite3_errmsg(db_handle);
    throw CCatalogIssue(oss.str());
  }

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc!= SQLITE_DONE) {
    ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  try {

    /*
     * loop through the result, make a catalog descr
     * and push it into the result list;
     */
    while (rc == SQLITE_ROW) {

      auto item = make_shared<CatalogDescr>();

      this->fetchArchiveDataIntoDescr(stmt, item);
      result->push_back(item);
      rc = sqlite3_step(stmt);

    }

  } catch (exception &e) {
    /* re-throw exception, but don't leak sqlite statement handle */
    sqlite3_finalize(stmt);
    throw e;
  }

  sqlite3_finalize(stmt);
  return result;
}

void BackupCatalog::setStreamStatus(int streamid,
                                    std::string status) {

  sqlite3_stmt *stmt;

  int rc;

  if (!this->available())
    throw CCatalogIssue("could not update stream status: database not opened");

  rc = sqlite3_prepare_v2(this->db_handle,
                          "UPDATE stream SET status = ?1 WHERE id = ?2;",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "cannot prepare updating stream status for id " << streamid;
    throw CCatalogIssue(oss.str());
  }

  sqlite3_bind_text(stmt, 1, status.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 2, streamid);

  rc= sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "failed to update stream status for id " << streamid;
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /* and we're done */
  sqlite3_finalize(stmt);
}

void BackupCatalog::dropStream(int streamid) {

  int rc;
  sqlite3_stmt *stmt;
  std::ostringstream query;

  if (!this->available()) {
    throw CCatalogIssue("could not drop stream: database not opened");
  }

  rc = sqlite3_prepare_v2(this->db_handle,
                          "DELETE FROM stream WHERE id = ?1;",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "error preparing to delete stream: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_bind_int(stmt, 1, streamid);

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error dropping stream: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);

}

void BackupCatalog::registerBasebackup(int archive_id,
                                       std::shared_ptr<BaseBackupDescr> backupDescr) {

  int rc;
  sqlite3_stmt *stmt;

  if (!this->available()) {
    throw CCatalogIssue("could not register basebackup: database not opened");
  }

  rc = sqlite3_prepare_v2(this->db_handle,
                          "INSERT INTO backup(archive_id, xlogpos, timeline, label, fsentry, started, systemid,"
                          " wal_segment_size, used_profile) "
                          "VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "error code "
        << rc
        << " when preparing to register basebackup: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_bind_int(stmt, 1, archive_id);
  sqlite3_bind_text(stmt, 2, backupDescr->xlogpos.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 3, backupDescr->timeline);
  sqlite3_bind_text(stmt, 4, backupDescr->label.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 5, backupDescr->fsentry.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 6, backupDescr->started.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 7, backupDescr->systemid.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 8, backupDescr->wal_segment_size);
  sqlite3_bind_int(stmt, 9, backupDescr->used_profile);

  /*
   * Execute the statement.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error registering basebackup: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Assign the generated ROW ID to this new basebackup entry.
   */
  backupDescr->id = sqlite3_last_insert_rowid(this->db_handle);

  /*
   * ... and we're done
   */
  sqlite3_finalize(stmt);
}

void BackupCatalog::finalizeBasebackup(std::shared_ptr<BaseBackupDescr> backupDescr) {

  int rc;
  sqlite3_stmt *stmt;

  if (!this->available()) {
    throw CCatalogIssue("could not finalize basebackup: database not opened");
  }

  /*
   * Descriptor should have a valid backup id
   */
  if (backupDescr->id < 0) {
    throw CCatalogIssue("could not finalize basebackup: invalid backup id");
  }

  /*
   * Check if this descriptor has an xlogposend value assigned.
   */
  if (backupDescr->xlogposend == "") {
    throw CCatalogIssue("could not finalize basebackup: expected xlog end position");
  }

  rc = sqlite3_prepare_v2(this->db_handle,
                          "UPDATE backup SET status = 'ready', stopped = ?1, xlogposend = ?2 "
                          "WHERE id = ?3 AND archive_id = ?4;",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "error finalizing basebackup: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  backupDescr->stopped = CPGBackupCtlBase::current_timestamp();

  /*
   * Bind values
   */
  sqlite3_bind_text(stmt, 1, backupDescr->stopped.c_str(),
                    -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 2, backupDescr->xlogposend.c_str(),
                    -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 3, backupDescr->id);
  sqlite3_bind_int(stmt, 4, backupDescr->archive_id);

  /*
   * Execute the statement
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error finalizing basebackup: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::abortBasebackup(std::shared_ptr<BaseBackupDescr> backupDescr) {

  int rc;
  sqlite3_stmt *stmt;
  std::ostringstream query;

  if (!this->available()) {
    throw CCatalogIssue("could not update basebackup status: catalog not available");
  }

  /*
   * Descriptor should have a valid backup id
   */
  if (backupDescr->id < 0) {
    throw CCatalogIssue("could not mark basebackup aborted: invalid backup id");
  }

  query << "UPDATE backup SET status = 'aborted' WHERE id = ?1 AND archive_id = ?2;";

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "error code "
        << rc
        << " when preparing to mark basebackup as aborted: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind parameters...
   */
  sqlite3_bind_int(stmt, 1, backupDescr->id);
  sqlite3_bind_int(stmt, 2, backupDescr->archive_id);

  rc = sqlite3_step(stmt);

  /*
   * Check command status.
   */
  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error registering stream: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::updateStream(int streamid,
                                 std::vector<int> affectedColumns,
                                 StreamIdentification &streamident) {

  int rc;
  sqlite3_stmt *stmt;
  ostringstream updateSQL;
  unsigned int boundCols;

  if(!this->available()) {
    throw CCatalogIssue("could not update stream: database not opened");
  }

  /*
   * affectedColumns should specify at least one
   * catalog column being updated.
   */
  if (affectedColumns.size() <= 0)
    throw CCatalogIssue("cannot update stream with empty attribute list");

  /*
   * Build UPDATE SQL command...
   */
  updateSQL << "UPDATE stream SET ";

  /*
   * Loop through the affected columns list and
   * build a comma seprated list of col=? pairs.
   */

  for (boundCols = 0; boundCols < affectedColumns.size(); boundCols++) {

    updateSQL << BackupCatalog::SQLgetUpdateColumnTarget(SQL_STREAM_ENTITY,
                                                         affectedColumns[boundCols])
              << (boundCols + 1);

    if (boundCols < affectedColumns.size() -1 ) {
      updateSQL << ", ";
    }

  }

  /*
   * WHERE clause identifes tuple per stream id
   */
  updateSQL << " WHERE id = ?" << (++boundCols) << ";";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "generate UPDATE SQL " << updateSQL.str();
#endif

  /*
   * Prepare the SQL statement.
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          updateSQL.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  /*
   * Bind UPDATE values. Please note that we rely
   * on the order of affectedAttributes to match
   * the previously formatted UPDATE SQL string.
   */
  this->SQLbindStreamAttributes(streamident,
                                affectedColumns,
                                stmt,
                                Range(1, boundCols));

  /*
   * Don't forget to bind values to the UPDATE WHERE clause...
   */
  sqlite3_bind_int(stmt, boundCols, streamid);

  /*
   * Execute the UPDATE
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "error updating stream in catalog database: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

std::shared_ptr<CatalogProc> BackupCatalog::getProc(int archive_id, std::string type) {

  int rc;
  sqlite3_stmt *stmt;
  std::shared_ptr<CatalogProc> procInfo(nullptr);
  std::vector<int> attrs;

  std::string query =
    "SELECT pid, archive_id, type, "
    "started, state, shm_key, shm_id FROM procs WHERE archive_id = ?1 "
    "AND type = ?2;";

  procInfo = std::make_shared<CatalogProc>();

  if (!this->available()) {
    throw CCatalogIssue("could not request proc information: database not opened");
  }

  /*
   * Prepare the query...
   */
#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "SELECT SQL: " << query;
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "error querying procs catalog table for archive ID "
        << archive_id << ", type " << type
        << ": " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Record attributes we want to retrieve ...
   */
  attrs.push_back(SQL_PROCS_PID_ATTNO);
  attrs.push_back(SQL_PROCS_ARCHIVE_ID_ATTNO);
  attrs.push_back(SQL_PROCS_TYPE_ATTNO);
  attrs.push_back(SQL_PROCS_STARTED_ATTNO);
  attrs.push_back(SQL_PROCS_STATE_ATTNO);
  attrs.push_back(SQL_PROCS_SHM_KEY_ATTNO);
  attrs.push_back(SQL_PROCS_SHM_ID_ATTNO);

  /*
   * Bind WHERE predicate values...
   */
  sqlite3_bind_int(stmt, 1, archive_id);
  sqlite3_bind_text(stmt, 2, type.c_str(), -1, SQLITE_STATIC);

  /*
   * Execute the statement.
   */
  rc = sqlite3_step(stmt);

  if (rc == SQLITE_ERROR) {
    std::ostringstream oss;
    oss << "error selecting proc information :"
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Only one tuple expected ...
   */
  if(rc == SQLITE_ROW) {

    /* Fetch proc data into new CatalogProc handle */
    procInfo = this->fetchCatalogProcData(stmt,
                                          attrs);

    if ((rc = sqlite3_step(stmt)) != SQLITE_DONE) {
      std::ostringstream oss;
      oss << "unexpected number for rows: getProc()";
      sqlite3_finalize(stmt);
      throw CCatalogIssue(oss.str());
    }

  }

  sqlite3_finalize(stmt);
  return procInfo;
}

void BackupCatalog::registerProc(std::shared_ptr<CatalogProc> procInfo) {

  int rc;
  sqlite3_stmt *stmt;
  std::ostringstream query;
  std::vector<int> attrs = procInfo->getAffectedAttributes();

  if (!this->available())
    throw CCatalogIssue("could not register process handle in catalog: database not opened");

  /*
   * Prepare INSERT SQL command ...
   */
  query
    << "INSERT INTO procs("
    << BackupCatalog::SQLgetColumnList(SQL_PROCS_ENTITY, attrs)
    << ") VALUES(";

  for (unsigned int i = 0; i < attrs.size(); i++) {
    query << "?" << (i + 1);
    if (i < (attrs.size() - 1))
      query << ", ";
  }

  query << ")";

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "INSERT SQL: " << query.str();
#endif

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss
      << "error code "
      << rc
      << " when preparing to register proc handle (PID "
      << procInfo->pid
      << ": "
      << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind values.
   */
  this->SQLbindProcsAttributes(procInfo,
                               attrs,
                               stmt,
                               Range(1, attrs.size()));

  /*
   * ... and execute the INSERT command.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss
      << "error registering process handle (PID "
      << procInfo->pid << "): "
      << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::unregisterProc(int pid,
                                   int archive_id) {

  int rc;
  sqlite3_stmt *stmt;
  std::shared_ptr<CatalogProc> procInfo = std::make_shared<CatalogProc>();
  std::ostringstream query;
  std::vector<int> attrs;

  if (!this->available())
    throw CCatalogIssue("could not unregister process handle from catalog: databas not opened");

  /*
   * Prepare the DELETE SQL command...
   */
  query << "DELETE FROM procs WHERE pid = ?1 AND archive_id = ?2;";
  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);
#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "DELETE SQL: " << query.str();
  BOOST_LOG_TRIVIAL(debug) << "   bound attrs pid="
                           << pid
                           << ", archive_id="
                           << archive_id;
#endif

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss
      << "error code "
      << rc
      << " when unregistering process handle (PID "
      << pid << "): "
      << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind values...
   */
  procInfo->pid = pid;
  procInfo->archive_id = archive_id;

  procInfo->pushAffectedAttribute(SQL_PROCS_PID_ATTNO);
  procInfo->pushAffectedAttribute(SQL_PROCS_ARCHIVE_ID_ATTNO);
  attrs = procInfo->getAffectedAttributes();
  this->SQLbindProcsAttributes(procInfo,
                               attrs,
                               stmt,
                               Range(1, attrs.size()));

  /*
   * ... and execute the DELETE statement.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss
      << "error unregistering process handle (PID "
      << pid << "): "
      << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::updateProc(std::shared_ptr<CatalogProc> procInfo,
                               std::vector<int> affectedAttributes,
                               int pid,
                               int archive_id) {

  int rc;
  sqlite3_stmt *stmt;
  std::ostringstream updateSQL;
  unsigned int boundCols = 0;

  if (!this->available())
    throw CCatalogIssue("could not update process handle: database not opened");

  /*
   * Build UPDATE SQL command string.
   */
  updateSQL << "UPDATE procs SET ";

  /*
   * Prepare the update statement.
   */
  for (boundCols = 0; boundCols < affectedAttributes.size(); boundCols++) {

    /* boundCols must start at index 1 ! */
    updateSQL << BackupCatalog::SQLgetUpdateColumnTarget(SQL_PROCS_ENTITY,
                                                         affectedAttributes[boundCols])
              << (boundCols + 1);

    if (boundCols < affectedAttributes.size() -1 ) {
      updateSQL << ", ";
    }
  }

  /*
   * ... don't forget the WHERE clause ...
   */
  ++boundCols;
  updateSQL << " WHERE pid = ?" << boundCols;
  ++boundCols;
  updateSQL << "AND archive_id = ?" << boundCols << ";";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "generate UPDATE SQL " << updateSQL.str();
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          updateSQL.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  /*
   * Assign bind variables. Please note that we rely
   * on the order of affectedAttributes to match the
   * previously formatted UPDATE SQL string. Note that
   * we must not recognized the bind values for the
   * WHERE condition here, since we don't want to copy
   * around the affectedAttributes vector. Thus, we do
   * it manually afterwards.
   */
  this->SQLbindProcsAttributes(procInfo,
                               affectedAttributes,
                               stmt,
                               Range(1, (boundCols - 2)));

  sqlite3_bind_int(stmt, (boundCols - 1), pid);
  sqlite3_bind_int(stmt, boundCols, archive_id);

  /*
   * Execute the UPDATE statement.
   */
  sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error updating catalog proc handle: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);
}

void BackupCatalog::registerStream(int archive_id,
                                   std::string type,
                                   StreamIdentification& streamident) {

  int rc;
  sqlite3_stmt *stmt;
  string ts_created = CPGBackupCtlBase::current_timestamp();

  if (!this->available()) {
    throw CCatalogIssue("could not register stream: database not opened");
  }

  rc = sqlite3_prepare_v2(this->db_handle,
                          "INSERT INTO stream("
                          "archive_id, stype, systemid, timeline, xlogpos, dbname, status, create_date, slot_name)"
                          " VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9);",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "error code "
        << rc
        << " when preparing to register stream: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_bind_int(stmt, 1, archive_id);
  sqlite3_bind_text(stmt, 2, type.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 3, streamident.systemid.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 4, streamident.timeline);
  sqlite3_bind_text(stmt, 5, streamident.xlogpos.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 6, streamident.dbname.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 7,
                    StreamIdentification::STREAM_PROGRESS_IDENTIFIED,
                    -1,
                    SQLITE_STATIC);

  /*
   * Set the creation date of this stream.
   */
  sqlite3_bind_text(stmt, 8, ts_created.c_str(), -1, SQLITE_STATIC);

  /*
   * ... and finally the name of the replication slot belonging
   * to this stream.
   */
  sqlite3_bind_text(stmt, 9, streamident.slot_name.c_str(), -1, SQLITE_STATIC);

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error registering stream: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Assign the new stream id to our current
   * stream identification object.
   */
  streamident.id = sqlite3_last_insert_rowid(this->db_handle);

  sqlite3_finalize(stmt);
}

shared_ptr<RetentionDescr> BackupCatalog::fetchRetentionPolicy(sqlite3_stmt *stmt,
                                                               shared_ptr<RetentionDescr> retention,
                                                               Range colIdRange) {

  vector<int> attrs = retention->getAffectedAttributes();
  int current_stmt_col = colIdRange.start();

  /*
   * Some sanity checks.
   */
  if (stmt == NULL) {
    throw CCatalogIssue("cannot fetch retention policy: uninitialized statement handle");
  }

  if (retention == nullptr) {
    throw CCatalogIssue("cannot fetch retention policy: uninitialized descriptor handle");
  }

  /* loop through the attributes list, fetch the corresponding column value */

  for (auto &current_col : attrs) {

    /*
     * Sanity check, stop if range end is reached.
     */
    if (current_stmt_col > colIdRange.end())
      break;

    switch(current_col) {

    case SQL_RETENTION_ID_ATTNO:
      {
        retention->id = sqlite3_column_int(stmt, current_stmt_col);
        break;
      }
    case SQL_RETENTION_NAME_ATTNO:
      {
        /* retention.name is a varchar, but can't be NULL */
        retention->name = (char *) sqlite3_column_text(stmt, current_stmt_col);
        break;
      }
    case SQL_RETENTION_CREATED_ATTNO:
      {
        /* the same with created, this timestamp is not allowed to be NULL */
        retention->created = (char *) sqlite3_column_text(stmt, current_stmt_col);
        break;
      }

    default:
      {
        throw CCatalogIssue("unknown column identifier in fetchRetentionPolicy()");
        break;
      }
    }

    current_stmt_col++;

  }

  return retention;

}

shared_ptr<RetentionRuleDescr> BackupCatalog::fetchRetentionRule(sqlite3_stmt *stmt,
                                                                 std::shared_ptr<RetentionRuleDescr> retentionRule,
                                                                 Range colIdRange) {
  vector<int> attrs = retentionRule->getAffectedAttributes();
  int current_stmt_col = colIdRange.start();

  /*
   * Sanity checks.
   */
  if (stmt == NULL) {
    throw CCatalogIssue("cannot fetch retention rule: uninitialized statement handle");
  }

  if (retentionRule == nullptr) {
    throw CCatalogIssue("cannot fetch retention rule: uninitialized descriptor handle");
  }

  for (auto &current_col : attrs) {

    switch(current_col) {

    case SQL_RETENTION_RULES_ID_ATTNO:
      {
        retentionRule->id = sqlite3_column_int(stmt, current_stmt_col);
        break;
      }

    case SQL_RETENTION_RULES_TYPE_ATTNO:
      {
        retentionRule->type = (RetentionRuleId) sqlite3_column_int(stmt, current_stmt_col);
        break;
      }

    case SQL_RETENTION_RULES_VALUE_ATTNO:
      {
        /* value can't be NULL */
        retentionRule->value = (char *) sqlite3_column_text(stmt, current_stmt_col);
        break;
      }

    default:
      {
        throw CCatalogIssue("unknown column identifier fetchRetentionRule()");
        break;
      }
    }

    current_stmt_col++;

  }

  return retentionRule;

}

void BackupCatalog::getRetentionPolicies(vector<shared_ptr<RetentionDescr>> &list,
                                         vector<int> attributesRetention,
                                         vector<int> attributesRules) {

  bool with_rules = false;
  bool has_rid    = false;
  sqlite3_stmt *stmt = NULL;
  int curr_retention_id = -1;
  int rc;

  ostringstream get_retention_sql;

  if (!this->available()) {
    throw CCatalogIssue("database not available");
  }

  if (attributesRetention.size() < 1) {
    throw CCatalogIssue("getRetentionPolicies() needs at least one attribute to fetch");
  }

  /*
   * We must include the retention id in the result set. Otherwise
   * we can't reliably retrieve any rules associated to the policy
   * below.
   */
  for (auto &colId : attributesRetention) {
    if (colId == SQL_RETENTION_ID_ATTNO)
      has_rid = true;
  }

  /*
   * Add the retention id attribute implicitely in case its
   * missing. This could be considered bad style, since
   * we change the specified vector under the hood, but since
   * it's a local copy we're safe.
   */
  if (!has_rid) {
    attributesRetention.push_back(SQL_RETENTION_ID_ATTNO);
  }

  if ( (with_rules = (attributesRules.size() >= 1)) ) {

    /* fetch with rules */

    get_retention_sql
      << "SELECT "
      << this->affectedColumnsToString(SQL_RETENTION_ENTITY, attributesRetention, "r")
      << ", "
      << this->affectedColumnsToString(SQL_RETENTION_RULES_ENTITY, attributesRules, "rr")
      << " FROM retention r JOIN retention_rules rr ON r.id = rr.id "
      << "ORDER BY r.name ASC;";

  } else {

    get_retention_sql
      << "SELECT "
      << this->affectedColumnsToString(SQL_RETENTION_ENTITY, attributesRetention, "r")
      << " FROM retention r ORDER BY r.name ASC";

  }

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "executing SQL: " << get_retention_sql.str();
#endif

  /*
   * Prepare the query.
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          get_retention_sql.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;

    oss << "could not prepare query to get retention policies: "
        << sqlite3_errmsg(this->db_handle);

    throw CCatalogIssue(oss.str());
  }

  /*
   * Execute the statement ...
   */
  rc = sqlite3_step(stmt);

  if (rc == SQLITE_ERROR) {
    ostringstream oss;
    oss << "error retrieving retention policies from catalog: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  while (rc == SQLITE_ROW) {

    shared_ptr<RetentionDescr> retention = make_shared<RetentionDescr>();

    /* make sure descriptor is marked empty */
    retention->id = -1;

    /*
     * Retention policies might be selected in association
     * with their retention rules. Thus the result set looks
     * like this (if all attributes are specified for retrieval):
     *
     * retention id | name | created | retention id | rule type | rule value
     * retention id | name | created | retention id | rule type | rule value
     * retention id | name | created | retention id | rule type | rule value
     * ...
     *
     * Where the retention properties are the same for each retention. Thus,
     * we must check during each iteration whether the retention id has changed,
     * which indicates a new retention policy to be fetched. We start with a retention
     * id set to -1, which indicates an empty retention policy set.
     *
     */

    /*
     * Fetch the retention policy. We need to fetch them
     * even if it didn't changed, since we need to compare
     * the retention id
     */
    retention->setAffectedAttributes(attributesRetention);
    this->fetchRetentionPolicy(stmt, retention,
                               Range(0, attributesRetention.size() - 1));

    /* Check whether this is a new retention policy item in the result set */
    if ( (curr_retention_id != retention->id)
         && (retention->id >= 0) ) {

      list.push_back(retention);
      curr_retention_id = retention->id;

    }

    /*
     * We stick rules, if requested, always into the
     * last retention policy we added to our list.
     */
    if (with_rules) {
      shared_ptr<RetentionRuleDescr> rule = make_shared<RetentionRuleDescr>();

      /* make sure rule is marked empty */
      rule->id = -1;

      /* don't forget attributes list */
      rule->setAffectedAttributes(attributesRules);

      /* ... and fetch it */
      this->fetchRetentionRule(stmt,
                               rule,

                               /*
                                * NOTE: column index offsets starts
                                *       *after* the retention attributes
                                *       list !!
                                */
                               Range(attributesRules.size(),
                                     (attributesRules.size() + attributesRules.size() - 1)));

      /*
       * We always add rules to last retention item
       * in the result list.
       */
      if (list.size() < 1) {
        throw CCatalogIssue("unexpected size(0) of retention policies result list");
      }

      (list.back())->rules.push_back(rule);
    }

    /* next one */
    rc = sqlite3_step(stmt);

    if (rc == SQLITE_ERROR) {
      ostringstream oss;
      oss << "error retrieving retention policies from catalog: "
          << sqlite3_errmsg(this->db_handle);
      sqlite3_finalize(stmt);
      throw CCatalogIssue(oss.str());
    }

  }

  sqlite3_finalize(stmt);

}

shared_ptr<RetentionDescr> BackupCatalog::getRetentionPolicy(string name) {

  shared_ptr<RetentionDescr> retentionPolicy = make_shared<RetentionDescr> ();
  sqlite3_stmt *stmt;
  ostringstream query;
  vector<int> attrsRetention;
  vector<int> attrsRules;
  int rc;

  if (!this->available()) {
    throw CCatalogIssue("database not available");
  }

  /*
   * Returned retention policy descriptor is empty, indicated
   * by -1.
   */
  retentionPolicy->id = -1;

  /*
   * Some sanity checks.
   */
  if (name.length() < 1) {
    throw CCatalogIssue("cannot get retention policy with empty name");
  }

  attrsRetention.push_back(SQL_RETENTION_ID_ATTNO);
  attrsRetention.push_back(SQL_RETENTION_NAME_ATTNO);
  attrsRetention.push_back(SQL_RETENTION_CREATED_ATTNO);

  attrsRules.push_back(SQL_RETENTION_RULES_ID_ATTNO);
  attrsRules.push_back(SQL_RETENTION_RULES_TYPE_ATTNO);
  attrsRules.push_back(SQL_RETENTION_RULES_VALUE_ATTNO);

  query << "SELECT "
        << this->affectedColumnsToString(SQL_RETENTION_ENTITY, attrsRetention, "a")
        << ", "
        << this->affectedColumnsToString(SQL_RETENTION_RULES_ENTITY, attrsRules, "b")
        << " FROM retention a JOIN retention_rules b ON a.id = b.id WHERE a.name = ?1 ORDER BY a.name ASC, b.type ASC;";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "generated SQL " << query.str();
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if ( rc != SQLITE_OK) {
    ostringstream oss;

    oss << "could not prepare query to get connection:"
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());

  }

  /*
   * bind retention policy identifier and execute the query.
   */
  sqlite3_bind_text(stmt, 1, name.c_str(), -1, SQLITE_STATIC);
  rc = sqlite3_step(stmt);

  /* Only one or no rows expected here */

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    ostringstream oss;

    oss << "unexpected result in catalog query: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);

    throw CCatalogIssue(oss.str());
  }

  /*
   * Fetch policy properties into the new descriptor, if any.
   */
  if (rc == SQLITE_ROW) {
    retentionPolicy->setAffectedAttributes(attrsRetention);
    retentionPolicy = this->fetchRetentionPolicy(stmt,
                                                 retentionPolicy,
                                                 Range(0, attrsRetention.size() - 1));
  }

  /*
   * Now loop over all current rows, retrieving information
   * about associated retention rules. They are attached to
   * the retention descriptor directly.
   */
  while ((rc == SQLITE_ROW) && (retentionPolicy->id >= 0)) {

    shared_ptr<RetentionRuleDescr> retention_rule = make_shared<RetentionRuleDescr> ();

    retention_rule->setAffectedAttributes(attrsRules);
    retention_rule = this->fetchRetentionRule(stmt,
                                              retention_rule,

                                              /*
                                               * NOTE: column index offsets starts after
                                               *       the retention attributes list!
                                               */
                                              Range(attrsRetention.size(),
                                                    (attrsRetention.size() + attrsRules.size() - 1)));

    /* Save retention rule to its policy descriptor */
    retentionPolicy->rules.push_back(retention_rule);

#ifdef __DEBUG__
    BOOST_LOG_TRIVIAL(debug) << "DEBUG: retention rule id " << retention_rule->type;
#endif

    /* next one */
    rc = sqlite3_step(stmt);
  }

  /*
   * Free all SQLite resources and we're done.
   */
  sqlite3_finalize(stmt);
  return retentionPolicy;

}

shared_ptr<BackupTablespaceDescr>
BackupCatalog::fetchBackupTablespaceIntoDescr(sqlite3_stmt *stmt,
                                              shared_ptr<BackupTablespaceDescr> tablespace,
                                              Range colIdRange) {

  vector<int> attrs = tablespace->getAffectedAttributes();
  int current_stmt_col = colIdRange.start();

  if (stmt == NULL)
    throw CCatalogIssue("cannot fetch backup tablespace: uninitialized statement handle");

  if (tablespace == nullptr)
    throw CCatalogIssue("cannot fetch backup tablespace: invalid descriptor handle");

  for(auto &current : attrs) {

    /*
     * Sanity check, stop if range end is reached.
     */
    if (current_stmt_col > colIdRange.end())
      break;

    switch(current) {

    case SQL_BCK_TBLSPC_BCK_ID_ATTNO:
      {
        tablespace->backup_id = sqlite3_column_int(stmt, current_stmt_col);
        break;
      }

    case SQL_BCK_TBLSPC_SPCOID_ATTNO:
      {
        tablespace->spcoid = sqlite3_column_int(stmt, current_stmt_col);
        break;
      }


    case SQL_BCK_TBLSPC_SPCLOC_ATTNO:
      {
        tablespace->spclocation = (char *) sqlite3_column_text(stmt, current_stmt_col);
        break;
    }

    case SQL_BCK_TBLSPC_SPCSZ_ATTNO:
      {
        tablespace->spcsize = sqlite3_column_int(stmt, current_stmt_col);
        break;
      }

    default:
      {
        throw CCatalogIssue("unknown column identifier in fetchBackupTablespaceIntoDescr()");
        break;
      }
    }

    current_stmt_col++;

  }

  return tablespace;
}

vector<shared_ptr<BackupTablespaceDescr>>
BackupCatalog::getBackupTablespaces(int backup_id,
                                    vector<int> attrs) {

  vector<shared_ptr<BackupTablespaceDescr>> result;
  sqlite3_stmt *stmt;
  int rc;
  ostringstream query;

  /*
   * Sanity check, it doesn't make
   * sense to fetch no attributes.
   */
  if (attrs.size() == 0)
    throw CCatalogIssue("empty attribute list specified when fetching backup tablespaces");

  query << "SELECT "
        << this->affectedColumnsToString(SQL_BACKUP_TBLSPC_ENTITY,
                                         attrs)
        << " FROM backup_tablespaces bt "
        << "WHERE backup_id = ?1;";

  /*
   * Prepare the query.
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "could not prepare query to get backup tablespaces: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_bind_int(stmt, 1, backup_id);

  /*
   * Seems everything went smooth so far, so execute
   * the query.
   */
  rc = sqlite3_step(stmt);

  if (rc == SQLITE_ERROR) {
    ostringstream oss;
    oss << "error retrieving backup tablespaces from catalog: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /*
   * If we have some rows, add them
   * to our result tablespace list.
   */
  while (rc == SQLITE_ROW) {

    shared_ptr<BackupTablespaceDescr> tablespace = make_shared<BackupTablespaceDescr>();

    this->fetchBackupTablespaceIntoDescr(stmt, tablespace, Range(0, attrs.size() - 1));
    result.push_back(tablespace);
    rc = sqlite3_step(stmt);

  }

  sqlite3_finalize(stmt);
  return result;

}

void BackupCatalog::registerTablespaceForBackup(std::shared_ptr<BackupTablespaceDescr> tblspcDescr) {

  sqlite3_stmt *stmt;
  std::ostringstream query;
  std::vector<int> attrs;
  int rc;

  if (!this->available()) {
    throw CCatalogIssue("database not available");
  }

  /*
   * We expect the backup_id identifier to be set.
   */
  if (tblspcDescr->backup_id < 0) {
    throw CCatalogIssue("backup id required to register tablespace for backup");
  }

  /*
   * Attributes required to register the new tablespace.
   */
  attrs.push_back(SQL_BCK_TBLSPC_BCK_ID_ATTNO);
  attrs.push_back(SQL_BCK_TBLSPC_SPCOID_ATTNO);
  attrs.push_back(SQL_BCK_TBLSPC_SPCLOC_ATTNO);
  attrs.push_back(SQL_BCK_TBLSPC_SPCSZ_ATTNO);

  /*
   * Generate INSERT SQL.
   */
  query << "INSERT INTO backup_tablespaces("
        << BackupCatalog::SQLgetColumnList(SQL_BACKUP_TBLSPC_ENTITY,
                                           /* vector with col IDs */
                                           attrs)
        << ") VALUES(?1, ?2, ?3, ?4);";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "DEBUG: registerTablespaceForBackup() query: "
                           << query.str();
#endif

  /*
   * Prepare the statement and bind values.
   */

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "could not prepare query to register tablespace: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_bind_int(stmt, 1, tblspcDescr->backup_id);
  sqlite3_bind_int(stmt, 2, tblspcDescr->spcoid);
  sqlite3_bind_text(stmt, 3, tblspcDescr->spclocation.c_str(),
                    -1, SQLITE_STATIC);
  sqlite3_bind_int(stmt, 4, tblspcDescr->spcsize);

  /*
   * Execute query...
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error registering stream: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Remember new registered id of tablespace ...
   */
  tblspcDescr->id = sqlite3_last_insert_rowid(this->db_handle);

  /*
   * ... and we're done.
   */
  sqlite3_finalize(stmt);
}

std::vector<std::shared_ptr<ConnectionDescr>>
BackupCatalog::getCatalogConnection(int archive_id) {

  sqlite3_stmt *stmt;
  int rc;
  ostringstream query;
  std::vector<std::shared_ptr<ConnectionDescr>> result;
  std::vector<int> affectedAttrs;

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  /*
   * We don't like negative ID values ...
   *
   * XXX: The API treats ID < 0 everywhere as uninitialized data,
   *      so tell the caller that he's doing something wrong.
   */
  if (archive_id < 0)
    throw CCatalogIssue("invalid archive ID specified");

  /*
   * We need to tell the columns we want to retrieve ...
   */
  affectedAttrs.push_back(SQL_CON_ARCHIVE_ID_ATTNO);
  affectedAttrs.push_back(SQL_CON_TYPE_ATTNO);
  affectedAttrs.push_back(SQL_CON_DSN_ATTNO);
  affectedAttrs.push_back(SQL_CON_PGHOST_ATTNO);
  affectedAttrs.push_back(SQL_CON_PGPORT_ATTNO);
  affectedAttrs.push_back(SQL_CON_PGUSER_ATTNO);
  affectedAttrs.push_back(SQL_CON_PGDATABASE_ATTNO);

  query << "SELECT "
        << this->affectedColumnsToString(SQL_CON_ENTITY,
                                         affectedAttrs)
        << " FROM connections WHERE archive_id = ?1 ORDER BY type ASC;";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "generated SQL " << query.str();
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "could not prepare query to get connection: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind WHERE clause values...
   */
  sqlite3_bind_int(stmt, 1, archive_id);

  /* ... execute SELECT */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "unexpected result in catalog query: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Empty result set or single row expected.
   */
  try {

    do {

      /*
       * new connection descriptor
       */
      std::shared_ptr<ConnectionDescr> conDescr
        = std::make_shared<ConnectionDescr>();

      /* retrieve attribute values from result set */
      conDescr->setAffectedAttributes(affectedAttrs);
      this->fetchConnectionData(stmt, conDescr);

      result.push_back(conDescr);

      /* next one */
      rc = sqlite3_step(stmt);

    } while (rc == SQLITE_ROW && rc != SQLITE_DONE);

  } catch(CCatalogIssue& e) {
    sqlite3_finalize(stmt);
    throw e;
  }

  sqlite3_finalize(stmt);
  return result;
}

void BackupCatalog::getCatalogConnection(std::shared_ptr<ConnectionDescr> conDescr,
                                         int archive_id,
                                         std::string type) {
  sqlite3_stmt *stmt;
  int rc;
  ostringstream query;

  /*
   * Since we scripple directly on the specified
   * pointer, make sure it's initialized.
   */
  if (conDescr == nullptr)
    throw CCatalogIssue("could not get connection info: result pointer is not initialized");

  query << "SELECT "
        << this->affectedColumnsToString(SQL_CON_ENTITY,
                                         conDescr->getAffectedAttributes())
        << " FROM connections WHERE archive_id = ?1 AND type = ?2;";

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "generated SQL: " << query.str();
#endif

  rc = sqlite3_prepare_v2(this->db_handle,
                          query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "could not prepare query to get connection: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind WHERE clause values...
   */
  sqlite3_bind_int(stmt, 1, archive_id);
  sqlite3_bind_text(stmt, 2, type.c_str(), -1, SQLITE_STATIC);

  /* ... execute SELECT */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW && rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "unexpected result in catalog query: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Mark result descriptor empty.
   */
  conDescr->archive_id = -1;
  conDescr->type = ConnectionDescr::CONNECTION_TYPE_UNKNOWN;

  /*
   * Empty result set or single row expected.
   */
  try {
    while (rc == SQLITE_ROW && rc != SQLITE_DONE) {
      this->fetchConnectionData(stmt, conDescr);
      break;
    }
  } catch(CCatalogIssue& e) {
    sqlite3_finalize(stmt);
    throw e;
  }

  sqlite3_finalize(stmt);
}

void
BackupCatalog::createCatalogConnection(std::shared_ptr<ConnectionDescr> conDescr) {

  sqlite3_stmt *stmt;
  std::ostringstream insert;
  int rc;

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  insert << "INSERT INTO connections("
         << this->affectedColumnsToString(SQL_CON_ENTITY,
                                          conDescr->getAffectedAttributes())
         << ") VALUES ("
         << this->SQLmakePlaceholderList(conDescr->getAffectedAttributes())
         << ");";

  rc = sqlite3_prepare_v2(this->db_handle,
                          insert.str().c_str(),
                          -1, &stmt, NULL);

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "Generated SQL: " << insert.str();
#endif

  if (rc != SQLITE_OK) {
    std::ostringstream oss;
    oss << "could not prepare query for connection descr: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  this->SQLbindConnectionAttributes(conDescr,
                                    conDescr->getAffectedAttributes(),
                                    stmt,
                                    Range(1, conDescr->getAffectedAttributes().size()));

  /*
   * Execute the prepared statement.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    std::ostringstream oss;
    oss << "error creating database connection entry: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);

}

void BackupCatalog::updateCatalogConnection(std::shared_ptr<ConnectionDescr> conDescr,
                                            std::string archive_name,
                                            std::string type) {
  sqlite3_stmt *stmt;
  ostringstream updateSQL;
  int rc;
  unsigned int boundCols = 0;
  std::vector<int> attrs;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  attrs = conDescr->getAffectedAttributes();
  updateSQL << "UPDATE connections SET ";

  /*
   * generate column list being updated.
   */
  for (boundCols = 0; boundCols < attrs.size(); boundCols++) {

    /*
     * boundCols must start at index 1!
     */
    updateSQL << BackupCatalog::SQLgetUpdateColumnTarget(SQL_CON_ENTITY,
                                                         attrs[boundCols])
              << (boundCols + 1);

    if (boundCols < attrs.size() - 1)
      updateSQL << ", ";
  }

  /*
   * ...attach WHERE clause.
   */
  ++boundCols;
  updateSQL << " WHERE archive_id = (SELECT id FROM archive WHERE name = ?"
            << boundCols;
  ++boundCols;
  updateSQL << ") AND type = ?"
            << boundCols
            << ";";

  /*
   * ... prepare the query.
   */
  rc = sqlite3_prepare_v2(this->db_handle,
                          updateSQL.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "could not prepare query to update connection: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind attributes, take care for the additional
   * WHERE condition values!
   */
  this->SQLbindConnectionAttributes(conDescr,
                                    attrs,
                                    stmt,
                                    Range(1, boundCols));
  sqlite3_bind_text(stmt,
                    (boundCols - 1),
                    archive_name.c_str(),
                    -1,
                    SQLITE_STATIC);

  sqlite3_bind_text(stmt,
                    boundCols,
                    type.c_str(),
                    -1,
                    SQLITE_STATIC);

  /*
   * Execute the query.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "error updating connection in catalog database: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  /* we're done */
  sqlite3_finalize(stmt);
}

void
BackupCatalog::dropCatalogConnection(std::string archive_name, std::string type) {

  sqlite3_stmt *stmt;
  ostringstream delete_query;
  int rc;

  if (!this->available()) {
    throw CCatalogIssue("catalog database not opened");
  }

  delete_query << "DELETE FROM connections "
    "WHERE archive_id = (SELECT id FROM archive WHERE name = ?1) "
    "AND type = ?2;";

  rc = sqlite3_prepare_v2(this->db_handle,
                          delete_query.str().c_str(),
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "could not prepare query to drop connection: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Bind statement variables
   */
  sqlite3_bind_text(stmt, 1, archive_name.c_str(), -1, SQLITE_STATIC);
  sqlite3_bind_text(stmt, 2, type.c_str(), -1, SQLITE_STATIC);

  /*
   * Execute the statement.
   */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_DONE) {
    ostringstream oss;
    oss << "could not delete connection: "
        << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_finalize(stmt);

}

shared_ptr<vector<string>> BackupCatalog::SQL(string sql) {

  shared_ptr<vector<string>> list = make_shared<vector<string>>();
  sqlite3_stmt *stmt;
  int rc;

  /*
   * Make sure database is opened.
   */
  if (!isOpen) {
    ostringstream oss;

    oss << "database is not open";
    throw CCatalogIssue(oss.str());
  }

  rc = sqlite3_prepare_v2(this->db_handle,
                          sql.c_str(),
                          -1,
                          &stmt,
                          NULL);
  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "could not prepare query: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  /* perform the SELECT */
  rc = sqlite3_step(stmt);

  /* Check for empty result */
  if (rc == SQLITE_DONE) {
    sqlite3_finalize(stmt);
    return list;
  }

  /* Seems we have a result list,
   * push 'em all into our vector */
  while (rc == SQLITE_ROW && rc != SQLITE_DONE) {

    string list_item;

    if (sqlite3_column_type(stmt, 0) != SQLITE_TEXT) {
      ostringstream oss;

      oss << "only single text columns allowed in SQL() method";
      sqlite3_finalize(stmt);
      throw CCatalogIssue(oss.str());
    }

    list_item = (char *)sqlite3_column_text(stmt, 0);
    list->push_back(list_item);

    rc = sqlite3_step(stmt);
  }

  sqlite3_finalize(stmt);
  return list;
}

void
BackupCatalog::getStreams(std::string archive_name,
                          std::vector<std::shared_ptr<StreamIdentification>> &result) {

  std::vector<int> streamRows;
  sqlite3_stmt *stmt;
  int rc;

  rc = sqlite3_prepare_v2(this->db_handle,
                          "SELECT * FROM stream WHERE archive_id = (SELECT id FROM archive WHERE name = ?1);",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "could not prepare query for stream/archive "
        << archive_name
        << ": "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  sqlite3_bind_text(stmt, 1, archive_name.c_str(), -1, SQLITE_STATIC);

  /* perform the SELECT */
  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW  && rc != SQLITE_DONE) {
    ostringstream oss;
    sqlite3_finalize(stmt);
    oss << "unexpected result in catalog query: " << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  streamRows.push_back(SQL_STREAM_ID_ATTNO);
  streamRows.push_back(SQL_STREAM_ARCHIVE_ID_ATTNO);
  streamRows.push_back(SQL_STREAM_STYPE_ATTNO);
  streamRows.push_back(SQL_STREAM_SLOT_NAME_ATTNO);
  streamRows.push_back(SQL_STREAM_SYSTEMID_ATTNO);
  streamRows.push_back(SQL_STREAM_TIMELINE_ATTNO);
  streamRows.push_back(SQL_STREAM_XLOGPOS_ATTNO);
  streamRows.push_back(SQL_STREAM_DBNAME_ATTNO);
  streamRows.push_back(SQL_STREAM_STATUS_ATTNO);
  streamRows.push_back(SQL_STREAM_REGISTER_DATE_ATTNO);

  while (rc == SQLITE_ROW && rc != SQLITE_DONE) {

    /* fetch stream data into new StreamIdentification */
    std::shared_ptr<StreamIdentification> item
      = this->fetchStreamData(stmt,
                              streamRows);
    result.push_back(item);
    rc = sqlite3_step(stmt);

  }

  sqlite3_finalize(stmt);

}

bool BackupCatalog::tableExists(string tableName) {

  int table_exists = false;

  if (this->available()) {

    sqlite3_stmt *stmt;
    int rc;

    rc = sqlite3_prepare_v2(this->db_handle,
                            "SELECT 1 FROM sqlite_master WHERE name = ?1;",
                            -1,
                            &stmt,
                            NULL);
    sqlite3_bind_text(stmt, 1, tableName.c_str(), -1, SQLITE_STATIC);

    if (rc != SQLITE_OK) {
      ostringstream oss;
      oss << "could not prepare query for table "
          << tableName
          << ": "
          << sqlite3_errmsg(this->db_handle);
      throw CCatalogIssue(oss.str());
    }

    rc = sqlite3_step(stmt);

    if (rc != SQLITE_ROW) {
      ostringstream oss;
      oss << "could not execute query for table info: "
          << sqlite3_errmsg(this->db_handle);
      sqlite3_finalize(stmt);
      throw CCatalogIssue(oss.str());
    }

    table_exists = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);

  } else {
    throw CCatalogIssue("database not available");
  }

  return ((table_exists == 1) ? true : false);
}

int BackupCatalog::getCatalogVersion() {

  sqlite3_stmt *stmt;
  int version;
  int rc;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  rc = sqlite3_prepare_v2(this->db_handle,
                          "SELECT number FROM version;",
                          -1,
                          &stmt,
                          NULL);

  if (rc != SQLITE_OK) {
    ostringstream oss;
    oss << "could not catalog version from database: "
        << sqlite3_errmsg(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  rc = sqlite3_step(stmt);

  if (rc != SQLITE_ROW) {
    ostringstream oss;
    oss << "could not execute query for table info: " << sqlite3_errmsg(this->db_handle);
    sqlite3_finalize(stmt);
    throw CCatalogIssue(oss.str());
  }

  version = sqlite3_column_int(stmt, 0);
  sqlite3_finalize(stmt);

  return version;
}

void BackupCatalog::checkCatalog() {

  int version = -1;

  if (!this->available())
    throw CCatalogIssue("catalog database not opened");

  if (!this->tableExists("version"))
    throw CCatalogIssue("catalog database doesn't have a \"version\" table");

  if (!this->tableExists("archive"))
    throw CCatalogIssue("catalog database doesn't have an \"archive\" table");

  if (!this->tableExists("backup"))
    throw CCatalogIssue("catalog database doesn't have a \"backup\" table");

  if (!this->tableExists("backup_tablespaces"))
    throw CCatalogIssue("catalog database doesn't have a \"backup_tablespaces\" table");

  if (!this->tableExists("stream"))
    throw CCatalogIssue("catalog database doesn't have a \"stream\" table");

  if (!this->tableExists("backup_profiles"))
    throw CCatalogIssue("catalog database doesn't have a \"backup_profiles\" table");

  /*
   * Version check. Examine whether CATALOG_MAGIC and the
   * current database schema version match. This is a weak check,
   * since one with access to our sqlite database is able to fake
   * this entry. But better than nothing...
   */
  version = this->getCatalogVersion();

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "catalog version "
                           << version
                           << ", catalog magic "
                           << this->getCatalogMagic()
                           << " linked against PostgreSQL "
                           << PGStream::compiledPGVersionNum();
#endif

  if (version < this->getCatalogMagic())
    throw CCatalogIssue("catalog database schema version too old");

}

int BackupCatalog::getCatalogMagic() {
  return CATALOG_MAGIC;
}

void BackupCatalog::open_rw() {

  int rc;

  rc = sqlite3_open(this->sqliteDB.c_str(), &(this->db_handle));
  if(rc) {
    ostringstream oss;
    /*
     * Something went wrong...
     */
    oss << "cannot open catalog: " << sqlite3_errmsg(this->db_handle);
    sqlite3_close(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  setPragma();

  this->isOpen = (this->db_handle != NULL);
}

bool BackupCatalog::opened() {

  return this->isOpen;

}

void BackupCatalog::open_ro() {

  int rc;

  rc = sqlite3_open_v2(this->sqliteDB.c_str(),
                       &(this->db_handle),
                       SQLITE_OPEN_READONLY,
                       NULL);

  if(rc) {
    ostringstream oss;
    /*
     * Something went wrong...
     */
    oss << "cannot open catalog read only: " << sqlite3_errmsg(this->db_handle);
    sqlite3_close(this->db_handle);
    throw CCatalogIssue(oss.str());
  }

  setPragma();

  this->isOpen = (this->db_handle != NULL);

}

void BackupCatalog::setPragma() {

  int rc;
  char *errmsg;

  rc = sqlite3_exec(this->db_handle,
                    "PRAGMA foreign_keys=ON;",
                    NULL,
                    NULL,
                    &errmsg);

  if ((rc == SQLITE_ABORT) || (errmsg != NULL)) {
    ostringstream oss;
    oss << "error setting SQLite Pragma: " << errmsg;
    sqlite3_free(errmsg);
    throw CCatalogIssue(oss.str());
  }

  rc = sqlite3_exec(this->db_handle,
                    "PRAGMA journal_mode=WAL;",
                    NULL,
                    NULL,
                    &errmsg);

  if ((rc == SQLITE_ABORT) || (errmsg != NULL)) {
    ostringstream oss;
    oss << "error setting SQLite Pragma: " << errmsg;
    sqlite3_free(errmsg);
    throw CCatalogIssue(oss.str());
  }

  /*
   * Doing catalog maintenance can cause large
   * delays in some cases, so it's okay to
   * define a large value here.
   *
   * We usually try hard to *not* hold SQLite transactions
   * very long, but this can't be guaranteed all over
   * the place.
   */
  rc = sqlite3_exec(this->db_handle,
                    "PRAGMA busy_timeout=60000;",
                    NULL,
                    NULL,
                    &errmsg);

  if ((rc == SQLITE_ABORT) || (errmsg != NULL)) {
    ostringstream oss;
    oss << "error setting SQLite Pragma: " << errmsg;
    sqlite3_free(errmsg);
    throw CCatalogIssue(oss.str());
  }

}

void BackupCatalog::setCatalogDB(string sqliteDB) {
  /* we don't care whether the database exists already! */
  this->sqliteDB = sqliteDB;
}

bool BackupCatalog::available() {
  return ((this->isOpen) && (this->db_handle != NULL));
}
