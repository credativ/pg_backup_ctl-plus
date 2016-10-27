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

PGBackupCtlParser::PGBackupCtlParser(std::stringstream& in) {

}

PGBackupCtlParser::PGBackupCtlParser(std::string in) {

}

PGBackupCtlParser::PGBackupCtlParser(path sourceFile) {

  this->sourceFile = sourceFile;
  this->command = make_shared<PGBackupCtlCommand>(EMPTY_CMD);

}

PGBackupCtlParser::~PGBackupCtlParser() {

}

void PGBackupCtlParser::parseArchiveCommand(boost::tokenizer<boost::char_separator<char>> tokens) {


}

void PGBackupCtlParser::parseLine(std::string line) {

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
      } else if (boost::iequals(t, "DROP")) {
        this->command->tag = DROP_CMD;
      } else if (boost::iequals(t, "LIST")) {
        this->command->tag = LIST_CMD;
      } else if (boost::iequals(t, "ARCHIVE")
                 && (this->command->tag != EMPTY_CMD)) {

        /*
         * Parse property of this ARCHIVE command.
         */
        this->command->propTag = PROPERTY_ARCHIVE_START;

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
    
    /*
     * if cmd tag is initialized and we have
     * scanned a starting property tag, continue.
     */
    else if ((this->command->tag != EMPTY_CMD)
             && (this->command->propTag == PROPERTY_BASEBACKUP_START)) {

#ifdef __DEBUG__
      std::cout << "basebackup property:" << t << std::endl;
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
      std::cout << "archive property:" << t << std::endl;
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

      else {
        ostringstream oss;
        oss << "syntax error near \"" << t << "\"";
        throw CParserIssue(oss.str());
      }

    }

    else if ((this->command->tag != EMPTY_CMD)
             && (this->command->propTag == PROPERTY_ARCHIVE_NAME)) {

      /* we want a directory name */
      std::cout << "NAME property: \"" << t << "\"" << std::endl;
      this->command->propTag = PROPERTY_ARCHIVE_START;

    }

    else if ((this->command->tag != EMPTY_CMD)
                 || (this->command->propTag == PROPERTY_BASEBACKUP_ARCHIVE_NAME)) {
      /* we want a directory name */
      std::cout << "NAME property: \"" << t << "\"" << std::endl;
      this->command->propTag = PROPERTY_BASEBACKUP_START;
    }

    else if ((this->command->tag != EMPTY_CMD)
             && (this->command->propTag == PROPERTY_ARCHIVE_DIRECTORY)) {
      /* we want a directory name */
      std::cout << "DIRECTORY property: \"" << t << "\"" << std::endl;
      this->command->propTag = PROPERTY_ARCHIVE_START;
    }

    else if ((this->command->tag != EMPTY_CMD)
             && (this->command->propTag == PROPERTY_ARCHIVE_PGUSER)) {
      /* we want a directory name */
      std::cout << "PGUSER property: \"" << t << "\"" << std::endl;
      this->command->propTag = PROPERTY_ARCHIVE_START;
    }

    else if ((this->command->tag != EMPTY_CMD)
             && (this->command->propTag == PROPERTY_ARCHIVE_PGDATABASE)) {
      /* we want a directory name */
      std::cout << "PGDATABASE property: \"" << t << "\"" << std::endl;
      this->command->propTag = PROPERTY_ARCHIVE_START;
    }

    else if ((this->command->tag != EMPTY_CMD)
             && (this->command->propTag == PROPERTY_ARCHIVE_PGHOST)) {
      /* we want a directory name */
      std::cout << "PGHOST property: \"" << t << "\"" << std::endl;
      this->command->propTag = PROPERTY_ARCHIVE_START;
    }
  }
}

void PGBackupCtlParser::parseFile() {

  std::ifstream fileHandle;
  std::stringstream fs;
  bool compressed = false;
  std::string line;

  std::cout << "parsing file " << this->sourceFile.string() << std::endl;

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
