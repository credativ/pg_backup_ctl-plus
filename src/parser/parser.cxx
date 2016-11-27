#include <BackupCatalog.hxx>
#include <catalog.hxx>
#include <commands.hxx>
#include <parser.hxx>

#include <iostream>
#include <boost/tokenizer.hpp>

/* required for string case insensitive comparison */
#include <boost/algorithm/string.hpp>

using namespace boost::filesystem;

PGBackupCtlCommand::PGBackupCtlCommand(CmdToken tag) {
  this->tag = tag;
  this->propTag = PROPERTY_EMPTY;
}

PGBackupCtlCommand::~PGBackupCtlCommand() {

}

bool PGBackupCtlCommand::propertyMissing(std::string key) {
  return (this->properties.find(key) == this->properties.end());
}

void PGBackupCtlCommand::execute(std::string catalogDir)
  throw (CPGBackupCtlFailure) {

  shared_ptr<CatalogDescr> descr(nullptr);

  /*
   * We don't execute uninitialized command
   * handles, but also don't throw an error
   * (we consider an empty command just as no-op).
   */
  if (this->tag == EMPTY_CMD)
    return;

  /*
   * First at all we need to create a catalog descriptor
   * which will then support initializing the backup catalog.
   */
  descr = this->getExecutableDescr();

  /*
   * Now establish the catalog instance.
   */
  shared_ptr<BackupCatalog> catalog
    = make_shared<BackupCatalog>(catalogDir, descr->directory);

  try {

    BaseCatalogCommand *execCmd;

    /*
     * Must cast to derived class.
     */
    execCmd = dynamic_cast<BaseCatalogCommand*>(descr.get());
    execCmd->setCatalog(catalog);
    execCmd->execute(false);

    /*
     * And we're done...
     */

    catalog->close();
  } catch (exception &e) {
    /*
     * Don't suppress any exceptions from here, but
     * make sure, we close the catalog safely.
     */
    if (catalog->available())
      catalog->close();
    throw CCatalogIssue(e.what());
  }
}

shared_ptr<CatalogDescr> PGBackupCtlCommand::getExecutableDescr() {

  shared_ptr<BaseCatalogCommand> result(nullptr);

  if (this->tag == EMPTY_CMD)
    return result;

  switch (this->propTag) {

  case PROPERTY_VERIFY_START: {
    /*
     * Is a VERIFY command...
     */
    switch(this->tag) {
    case VERIFY_CMD:
      {
        result = make_shared<VerifyArchiveCatalogCommand>();

        if (this->propertyMissing("NAME")) {
          throw CParserIssue("missing command property \"NAME\"");
        }
        else {
          result->archive_name = this->getPropertyString("NAME");
          result->pushAffectedAttribute(SQL_ARCHIVE_NAME_ATTNO);
        }

        result->tag = VERIFY_ARCHIVE;
        break;
      }
    default:
      ostringstream oss;
      oss << "unexpected parser command \"" << this->tag << "\"";
      throw CParserIssue(oss.str());
    }

    break;
  }

  case PROPERTY_ARCHIVE_START: {
    switch (this->tag) {
    case CREATE_CMD: {

      /* CREATE ARCHIVE command ... */
      result = make_shared<CreateArchiveCatalogCommand>();

      if (this->propertyMissing("NAME"))
         throw CParserIssue("missing command property \"NAME\"");
      else {
        result->archive_name = this->getPropertyString("NAME");
        result->pushAffectedAttribute(SQL_ARCHIVE_NAME_ATTNO);
      }

      if (this->propertyMissing("DIRECTORY"))
        throw CParserIssue("missing command property \"DIRECTORY\"");
      else {
        result->directory = this->getPropertyString("DIRECTORY");
        result->pushAffectedAttribute(SQL_ARCHIVE_DIRECTORY_ATTNO);
      }

      if (this->propertyMissing("PGHOST"))
        throw CParserIssue("missing command property \"PGHOST\"");
      else {
        result->pghost = this->getPropertyString("PGHOST");
        result->pushAffectedAttribute(SQL_ARCHIVE_PGHOST_ATTNO);
      }

      if (this->propertyMissing("PGPORT"))
        throw CParserIssue("missing command property \"PGPORT\"");
      else {
        result->pgport = this->getPropertyInt("PGPORT");
        result->pushAffectedAttribute(SQL_ARCHIVE_PGPORT_ATTNO);
      }

      if (this->propertyMissing("PGDATABASE"))
        throw CParserIssue("missing command property \"PGDATABASE\"");
      else {
        result->pgdatabase = this->getPropertyString("PGDATABASE");
        result->pushAffectedAttribute(SQL_ARCHIVE_PGDATABASE_ATTNO);
      }

      if (this->propertyMissing("PGUSER"))
        throw CParserIssue("missing command property \"PGUSER\"");
      else {
        result->pguser = this->getPropertyString("PGUSER");
        result->pushAffectedAttribute(SQL_ARCHIVE_PGUSER_ATTNO);
      }

      /*
       * mark the descriptor according to parsed action.
       */
      result->tag = CREATE_ARCHIVE;
      break;

    }
    case LIST_CMD:
      {
        /* LIST ARCHIVE command */
        result = make_shared<ListArchiveCatalogCommand>();

        /* helper object reference */
        ListArchiveCatalogCommand * listCmd
          = dynamic_cast<ListArchiveCatalogCommand *>(result.get());

        /*
         * LIST ARCHIVE without the NAME property lists
         * *all* archives currently registered. If the NAME
         * property is given, we spit out a detailed view
         * the specified archive only.
         *
         * Iff additional properties are set (PGHOST, PGDATABASE, ...)
         * we create a ARCHIVE_FILTERED_LIST output, which just
         * lists archives with the specified properties.
         */
        if (this->propertyMissing("NAME")) {
          listCmd->setOutputMode(ARCHIVE_LIST);
        } else {
          listCmd->setOutputMode(ARCHIVE_DETAIL_LIST);
          listCmd->pushAffectedAttribute(SQL_ARCHIVE_NAME_ATTNO);
          listCmd->archive_name = this->getPropertyString("NAME");
        }

        /*
         * Go through remaining property list and check wether
         * a filter view is requested in that case. Don't forget
         * to adjust the output mode in this case (will overwrite the
         * selected mode above in this case).
         */

        if (!this->propertyMissing("PGHOST")) {
          listCmd->pushAffectedAttribute(SQL_ARCHIVE_PGHOST_ATTNO);
          listCmd->setOutputMode(ARCHIVE_FILTERED_LIST);
          listCmd->pghost = this->getPropertyString("PGHOST");
        }

        if (!this->propertyMissing("PGPORT")) {
          listCmd->pushAffectedAttribute(SQL_ARCHIVE_PGPORT_ATTNO);
          listCmd->pgport = this->getPropertyInt("PGPORT");
          listCmd->setOutputMode(ARCHIVE_FILTERED_LIST);
        }

        if (!this->propertyMissing("PGUSER")) {
          listCmd->pushAffectedAttribute(SQL_ARCHIVE_PGUSER_ATTNO);
          listCmd->setOutputMode(ARCHIVE_FILTERED_LIST);
          listCmd->pguser = this->getPropertyString("PGUSER");
        }

        if (!this->propertyMissing("PGDATABASE")) {
          listCmd->pushAffectedAttribute(SQL_ARCHIVE_PGDATABASE_ATTNO);
          listCmd->setOutputMode(ARCHIVE_FILTERED_LIST);
          listCmd->pgdatabase = this->getPropertyString("PGDATABASE");
        }

        if (!this->propertyMissing("DIRECTORY")) {
          listCmd->pushAffectedAttribute(SQL_ARCHIVE_DIRECTORY_ATTNO);
          listCmd->setOutputMode(ARCHIVE_FILTERED_LIST);
          listCmd->directory = this->getPropertyString("DIRECTORY");
        }

        result->tag = LIST_ARCHIVE;
        break;
      }
    case DROP_CMD: {

      result = make_shared<DropArchiveCatalogCommand>();

      /*
       * Check for missing properties.
       */
      if (this->propertyMissing("NAME"))
        throw CParserIssue("missing command property \"NAME\"");
      else {
        result->archive_name = this->getPropertyString("NAME");
        result->pushAffectedAttribute(SQL_ARCHIVE_NAME_ATTNO);
      }

      /*
       * Tag the executable descriptor accordingly.
       */
      result->tag = DROP_ARCHIVE;
      break;
    }
    case ALTER_CMD: {

      result = make_shared<AlterArchiveCatalogCommand>();

      if (this->propertyMissing("NAME"))
        throw CParserIssue("missing command property \"NAME\"");
      else {
        /*
         * NAME is a no-op property here, since we don't allow
         * to change the NAME of an archive once it was created.
         * So don't mark it as an affected attribute in the
         * properties list.
         */
        result->archive_name = this->getPropertyString("NAME");
      }

      /*
       * NOTE: the following properties are optional, since we only
       *       might want to alter partial settings. So don't error
       *       out in case something is missing.
       */
      if (!this->propertyMissing("DIRECTORY")) {
        result->directory = this->getPropertyString("DIRECTORY");
        result->pushAffectedAttribute(SQL_ARCHIVE_DIRECTORY_ATTNO);
      }

      if (!this->propertyMissing("PGHOST")) {
        result->pghost = this->getPropertyString("PGHOST");
        result->pushAffectedAttribute(SQL_ARCHIVE_PGHOST_ATTNO);
      }

      if (!this->propertyMissing("PGPORT")) {
        result->pgport = this->getPropertyInt("PGPORT");
        result->pushAffectedAttribute(SQL_ARCHIVE_PGPORT_ATTNO);
      }

      if (!this->propertyMissing("PGDATABASE")) {
        result->pgdatabase = this->getPropertyString("PGDATABASE");
        result->pushAffectedAttribute(SQL_ARCHIVE_PGDATABASE_ATTNO);
      }

      if (!this->propertyMissing("PGUSER")) {
        result->pguser = this->getPropertyString("PGUSER");
        result->pushAffectedAttribute(SQL_ARCHIVE_PGUSER_ATTNO);
      }

      result->tag = ALTER_ARCHIVE;
      break;
    }
    default:
      ostringstream oss;
      oss << "unexpected parser command \"" << this->tag << "\"";
      throw CParserIssue(oss.str());
    }
    break;
  }
  default:
    throw CParserIssue("unexpected parser command");
  }

  return result;
}

int PGBackupCtlCommand::getPropertyInt(std::string key) 
  throw (CParserIssue) {

  int result = 0;

  try {
    istringstream iss(this->properties[key]);
    iss >> result;
  } catch(exception &e) {
    /* re-throw as parser issue. */
    throw CParserIssue(e.what());
  }

  return result;
}

std::string PGBackupCtlCommand::getPropertyString(std::string key)
  throw(CParserIssue) {
  string result = "";

  try {
    result = this->properties[key];
  } catch(exception &e) {
    /* re-throw as parser issue. */
    throw CParserIssue(e.what());
  }

  return result;
}

PGBackupCtlParser::PGBackupCtlParser() {
  this->command = make_shared<PGBackupCtlCommand>(EMPTY_CMD);
}

PGBackupCtlParser::PGBackupCtlParser(path sourceFile) {

  this->sourceFile = sourceFile;
  this->command = make_shared<PGBackupCtlCommand>(EMPTY_CMD);

}

PGBackupCtlParser::~PGBackupCtlParser() {

}

shared_ptr<PGBackupCtlCommand> PGBackupCtlParser::getCommand() {
  return this->command;
}

void PGBackupCtlParser::saveCommandProperty(std::string key, std::string value) {

  this->command->properties.insert(make_pair(key, value));

}

void PGBackupCtlParser::parseLine(std::string line)
  throw(CParserIssue) {

  boost::char_separator<char> sep(" ");
  boost::tokenizer<boost::char_separator<char>> tokens(line, sep);

  for (const auto& t: tokens) {

#ifdef __DEBUG__
    std::cout << "token " << t << " " << std::endl;
#endif

    /*
     * Start of command?
     */
    if (this->command->propTag == PROPERTY_EMPTY) {

      if (boost::iequals(t, "CREATE")) {
        this->command->tag = CREATE_CMD;

      } else if (boost::iequals(t, "VERIFY")) {
        this->command->tag = VERIFY_CMD;

      } else if (boost::iequals(t, "DROP")) {
        this->command->tag = DROP_CMD;

      } else if (boost::iequals(t, "LIST")) {
        this->command->tag = LIST_CMD;

      } else if (boost::iequals(t, "ALTER")) {
        this->command->tag = ALTER_CMD;

      } else if (boost::iequals(t, "ARCHIVE")
                 && (this->command->tag != EMPTY_CMD)) {

        /*
         * Parse property of this ARCHIVE command.
         */
        switch(this->command->tag) {

        case ALTER_CMD:
        case CREATE_CMD:
        case LIST_CMD:
        case DROP_CMD:
          this->command->propTag = PROPERTY_ARCHIVE_START;
          break;
        case VERIFY_CMD:
          this->command->propTag = PROPERTY_VERIFY_START;
          break;
        default:
          throw CParserIssue("unexpected command tag");
        }

      }

      else if (boost::iequals(t, "BASEBACKUP")
               && (this->command->tag != EMPTY_CMD)) {
        this->command->propTag = PROPERTY_BASEBACKUP_START;

      } else {
        ostringstream oss;
        oss << "unexpected token: \"" << t << "\"";
        throw CParserIssue(oss.str());
      }

    }

    else if ((this->command->tag == VERIFY_CMD)
             && (this->command->propTag == PROPERTY_VERIFY_START)) {
#ifdef __DEBUG__
      std::cerr << "verify property:" << t << std::endl;
#endif

      if (boost::iequals(t, "NAME")) {
        this->command->propTag = PROPERTY_VERIFY_ARCHIVE_NAME;
      }
      else {
        ostringstream oss;
        oss << "syntax error near \"" << t << "\"";
        throw CParserIssue(oss.str());
      }

    }

    /*
     * if cmd tag is initialized and we have
     * scanned a starting property tag, continue.
     */
    else if ((this->command->tag != EMPTY_CMD)
             && (this->command->propTag == PROPERTY_BASEBACKUP_START)) {

#ifdef __DEBUG__
      std::cerr << "basebackup property:" << t << std::endl;
#endif

      if (boost::iequals(t, "FOR")) {
        this->command->propTag = PROPERTY_BASEBACKUP_ARCHIVE_NAME;
      }

      else {
        ostringstream oss;
        oss << "syntax error near \"" << t << "\"";
        throw CParserIssue(oss.str());
      }

    }

    else if ((this->command->tag != EMPTY_CMD)
             && (this->command->propTag == PROPERTY_ARCHIVE_START)) {

#ifdef __DEBUG__
      std::cerr << "archive property:" << t << std::endl;
#endif

      /*
       * We scan now all properties specified to the CREATE|DROP|LIST ARCHIVE
       * command. The order of the property identifier doesn't matter,
       * however, we must be sure everything mandatory is specified, currently
       * the mandatory properties are:
       *
       * NAME
       * DIRECTORY
       * PGHOST
       * PGDATABASE
       * PGUSER
       */

      if (boost::iequals(t, "NAME")) {
        this->command->propTag = PROPERTY_ARCHIVE_NAME;
      }

      else if (boost::iequals(t, "DIRECTORY")) {

        /* remember look behind to read a directory name */
        this->command->propTag = PROPERTY_ARCHIVE_DIRECTORY;
      }

      else if (boost::iequals(t, "PGHOST")) {

        /* remember look behind to read a pg hostname */
        this->command->propTag = PROPERTY_ARCHIVE_PGHOST;
      }

      else if (boost::iequals(t, "PGUSER")) {

        /* remember look behind to read a pg role name */
        this->command->propTag = PROPERTY_ARCHIVE_PGUSER;
      }

      else if (boost::iequals(t, "PGDATABASE")) {

        /* remember look behind to read a pg role name */
        this->command->propTag = PROPERTY_ARCHIVE_PGDATABASE;
      }

      /*
       * PGPORT is optional
       */
      else if (boost::iequals(t, "PGPORT")) {

        /* remember look behind to read a pg role name */
        this->command->propTag = PROPERTY_ARCHIVE_PGPORT;
      }

      else {
        ostringstream oss;
        oss << "syntax error near \"" << t << "\"";
        throw CParserIssue(oss.str());
      }

    }

    else if ((this->command->tag == VERIFY_CMD)
             && (this->command->propTag == PROPERTY_VERIFY_ARCHIVE_NAME)) {

      /* we want an archive name */
      this->saveCommandProperty("NAME", t);
      this->command->propTag = PROPERTY_VERIFY_START;

    }

    else if ((this->command->tag != EMPTY_CMD)
             && (this->command->propTag == PROPERTY_ARCHIVE_NAME)) {

      /* we want a directory name */
      this->saveCommandProperty("NAME", t);
      this->command->propTag = PROPERTY_ARCHIVE_START;

    }

    else if ((this->command->tag != EMPTY_CMD)
                 && (this->command->propTag == PROPERTY_BASEBACKUP_ARCHIVE_NAME)) {
      /* we want a directory name */
      this->saveCommandProperty("NAME", t);
      this->command->propTag = PROPERTY_BASEBACKUP_START;
    }

    else if ((this->command->tag != EMPTY_CMD)
             && (this->command->propTag == PROPERTY_ARCHIVE_DIRECTORY)) {
      /* we want a directory name */
      this->saveCommandProperty("DIRECTORY", t);
      this->command->propTag = PROPERTY_ARCHIVE_START;
    }

    else if ((this->command->tag != EMPTY_CMD)
             && (this->command->propTag == PROPERTY_ARCHIVE_PGUSER)) {
      /* we want a role name */
      this->saveCommandProperty("PGUSER", t);
      this->command->propTag = PROPERTY_ARCHIVE_START;
    }

    else if ((this->command->tag != EMPTY_CMD)
             && (this->command->propTag == PROPERTY_ARCHIVE_PGDATABASE)) {
      /* we want a database name */
      this->saveCommandProperty("PGDATABASE", t);
      this->command->propTag = PROPERTY_ARCHIVE_START;
    }

    else if ((this->command->tag != EMPTY_CMD)
             && (this->command->propTag == PROPERTY_ARCHIVE_PGHOST)) {
      /* we want a host name */
      this->saveCommandProperty("PGHOST", t);
      this->command->propTag = PROPERTY_ARCHIVE_START;
    }

    else if ((this->command->tag != EMPTY_CMD)
             && (this->command->propTag == PROPERTY_ARCHIVE_PGPORT)) {
      /* we want a port number */
      this->saveCommandProperty("PGPORT", t);
      this->command->propTag = PROPERTY_ARCHIVE_START;
    }
  } /* for */

  /*
   * NOTE: We don't check for command properties during
   *       the parsing phase, this is delayed until execution
   *       of the command (see PGBackupCtlCommand::execute()
   *       and PGBackupCtlCommand::getExecutableDescr() for details).
   */
}

void PGBackupCtlParser::parseFile() throw(CParserIssue) {

  std::ifstream fileHandle;
  std::stringstream fs;
  bool compressed = false;
  std::string line;

  /*
   * Check state of the source file. Throws
   * an exception in case something is wrong.
   */
  file_status state = status(this->sourceFile);

  /*
   * Use the internal openFile() method
   * from CPGBackupCtlBase.
   */
  this->openFile(fileHandle,
                 fs,
                 this->sourceFile,
                 &compressed);

  while (std::getline(fs, line)) {

    this->parseLine(line);

  }
}
