#include <BackupCatalog.hxx>
#include <catalog.hxx>
#include <commands.hxx>
#include <parser.hxx>

#include <iostream>
#include <boost/tokenizer.hpp>

/* required for string case insensitive comparison */
#include <boost/algorithm/string/predicate.hpp>

/*
 * For the boost builtin parser.
 */
#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_object.hpp>
#include <boost/spirit/repository/include/qi_kwd.hpp>
#include <boost/spirit/repository/include/qi_keywords.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/fusion/include/io.hpp>
#include <boost/fusion/adapted/std_pair.hpp>
#include <boost/bind.hpp>
#include <complex>
#include <functional>

using namespace boost::filesystem;

/*
 * This is the core parser implementation for credativ::boostparser
 *
 * Internal parser implementation
 * uses boost::spirit. Make them private to the
 * parser module within their own namespace.
 */
namespace credativ {
  namespace boostparser {

    namespace qi      = boost::spirit::qi;
    namespace ascii   = boost::spirit::ascii;
    namespace phoenix = boost::phoenix;
    namespace fusion  = boost::fusion;

    template<typename Iterator>
    struct PGBackupCtlBoostParser
      : qi::grammar<Iterator, ascii::space_type> {

    private:

      credativ::CatalogDescr cmd;

    public:

      credativ::CatalogDescr getCommand() { return this->cmd; }

      PGBackupCtlBoostParser() : PGBackupCtlBoostParser::base_type(start, "pg_backup_ctl command") {
        using qi::_val;
        using qi::_1;
        using qi::_2;
        using qi::lit;
        using qi::lexeme;
        using qi::char_;
        using qi::uint_;
        using qi::eps;
        using qi::graph;
        using qi::no_case;
        using qi::no_skip;

        /*
         * Basic error handling requires this.
         */
        using qi::on_error;
        using qi::fail;
        using phoenix::construct;
        using phoenix::val;

        /*
         * Parser rule definitions
         */
        start %= eps > (
                        /* CREATE command syntax start */
                        cmd_create > (
                                      cmd_create_archive
                                      | cmd_create_backup_profile
                                      )

                        /* LIST command syntax start */
                        | cmd_list > (
                                      cmd_list_archive
                                      | cmd_list_backup
                                      )

                        /* ALTER command */
                        | cmd_alter > (
                                       cmd_alter_archive
                                       | cmd_alter_backup_profile
                                       )

                        /*
                         * DROP command syntax start
                         */
                        | cmd_drop > (
                                      /*
                                       * DROP ARCHIVE <name> command
                                       */
                                      cmd_drop_archive > identifier
                                      [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ]

                                      /*
                                       * DROP BACKUP PROFILE
                                       */
                                      | cmd_drop_backup_profile)

                        /*
                         * VERIFY ARCHIVE <name> command
                         */
                        | cmd_verify_archive > identifier
                        [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ]

                        /*
                         * START BASEBACKUP FOR ARCHIVE <name> command
                         */
                        | cmd_start_basebackup
                        > identifier
                        [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ]
                        > -(with_profile)

                        ); /* start rule end */

        /*
         * ALTER ARCHIVE <name> command
         */
        cmd_alter_archive_opt = identifier
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ]
          > no_case[ lexeme[ lit("SET") ] ]
          ^ directory
          [ boost::bind(&CatalogDescr::setDirectory, &cmd, ::_1) ]
          ^ hostname
          [ boost::bind(&CatalogDescr::setHostname, &cmd, ::_1) ]
          ^ database
          [ boost::bind(&CatalogDescr::setDbName, &cmd, ::_1) ]
          ^ username
          [ boost::bind(&CatalogDescr::setUsername, &cmd, ::_1) ]
          ^ portnumber
          [ boost::bind(&CatalogDescr::setPort, &cmd, ::_1) ];


        /*
         * CREATE, DROP, ALTER and LIST tokens...
         */
        cmd_create = no_case[lexeme[ lit("CREATE") ]] ;

        cmd_drop = no_case[lexeme[ lit("DROP") ]];

        cmd_list = no_case[lexeme[ lit("LIST") ]];

        cmd_alter = no_case[lexeme[ lit("ALTER") ]];

        /*
         * LIST BACKUP CATALOG [<backup>] ...
         *             | PROFILE [ <profile> ] ... command
         */
        cmd_list_backup = no_case[ lexeme[ lit("BACKUP") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, LIST_BACKUP_CATALOG) ]
          > ( ( no_case[ lexeme[ lit("CATALOG") ] ]
                > identifier
                [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ] )
              | ( no_case[lexeme [ lit("PROFILE") ] ]
                  [ boost::bind(&CatalogDescr::setCommandTag, &cmd, LIST_BACKUP_PROFILE) ]
                  > -(identifier)
                  [ boost::bind(&CatalogDescr::setProfileName, &cmd, ::_1) ]
                  [  boost::bind(&CatalogDescr::setCommandTag, &cmd, LIST_BACKUP_PROFILE_DETAIL) ] ) );


        /*
         * LIST ARCHIVE [<name>] command
         */
        cmd_list_archive = no_case[ lexeme[ lit("ARCHIVE") ] ]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, LIST_ARCHIVE) ]
          > -(identifier)
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ];

        /*
         * CREATE BACKUP PROFILE <name> command
         */
        cmd_create_backup_profile = no_case[lexeme [ lit("BACKUP") ]]
          > no_case[lexeme [ lit("PROFILE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, CREATE_BACKUP_PROFILE) ]
          > identifier
          [ boost::bind(&CatalogDescr::setProfileName, &cmd, ::_1) ]
          > backup_profile_opts;

        backup_profile_opts =
          -(profile_compression_option)
          > -(profile_max_rate_option
              [ boost::bind(&CatalogDescr::setProfileMaxRate, &cmd, ::_1) ])
          > -(profile_backup_label_option)
          > -(profile_wal_option)
          > -(profile_checkpoint_option)
          > -(profile_wait_for_wal_option);

        /*
         * CREATE ARCHIVE <name> command
         */
        cmd_create_archive = no_case[lexeme [ lit("ARCHIVE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, CREATE_ARCHIVE) ]

          > identifier
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ]

          > no_case[ lexeme[ lit("PARAMS") ] ]

          > directory
          [ boost::bind(&CatalogDescr::setDirectory, &cmd, ::_1) ]
          > hostname
          [ boost::bind(&CatalogDescr::setHostname, &cmd, ::_1) ]
          > database
          [ boost::bind(&CatalogDescr::setDbName, &cmd, ::_1) ]
          > username
          [ boost::bind(&CatalogDescr::setUsername, &cmd, ::_1) ]
          > portnumber
          [ boost::bind(&CatalogDescr::setPort, &cmd, ::_1) ];


        cmd_verify_archive = no_case[lexeme[ lit("VERIFY") ]] > no_case[lexeme [ lit("ARCHIVE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, VERIFY_ARCHIVE) ];

        cmd_drop_backup_profile = no_case[lexeme[ lit("BACKUP") ]]
          > no_case[lexeme[ lit("PROFILE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, DROP_BACKUP_PROFILE) ]
          > identifier
          [ boost::bind(&CatalogDescr::setProfileName, &cmd, ::_1) ];

        cmd_drop_archive = no_case[lexeme[ lit("ARCHIVE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, DROP_ARCHIVE) ];

        cmd_alter_archive = no_case[lexeme[ lit("ARCHIVE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, ALTER_ARCHIVE) ]
          > cmd_alter_archive_opt;

        cmd_start_basebackup = no_case[lexeme[ lit("START") ]] > no_case[lexeme[ lit("BASEBACKUP") ]]
          > no_case[lexeme[ lit("FOR") ]] > no_case[lexeme[ lit("ARCHIVE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, START_BASEBACKUP) ];

        /*
         * Property clauses
         */
        property_string = lexeme [ +(graph - (",;[]{} ")) ];
        hostname = no_case[lexeme[ lit("PGHOST") ]] > "=" > property_string;
        database = no_case[lexeme[ lit("PGDATABASE") ]] > "=" > property_string;
        username = no_case[lexeme[ lit("PGUSER") ]] > "=" > property_string;
        directory = no_case[lexeme[ lit("DIRECTORY") ]] > "=" > directory_string;
        portnumber = no_case[lexeme[ lit("PGPORT") ]] > "=" > +(char_("0-9"));

        /*
         * Rule to read in COMPRESSION=<BACKUP_COMPRESSION_TYPE>
         */
        profile_compression_option =
          no_case[lexeme[ lit("COMPRESSION") ]]
          > "="
          > (no_case[lexeme[ lit("GZIP") ]]
             [ boost::bind(&CatalogDescr::setProfileCompressType, &cmd, BACKUP_COMPRESS_TYPE_GZIP) ]
             | no_case[lexeme[ lit("NONE") ]]
             [ boost::bind(&CatalogDescr::setProfileCompressType, &cmd, BACKUP_COMPRESS_TYPE_NONE) ]
             | no_case[lexeme[ lit("ZSTD") ]]
             [ boost::bind(&CatalogDescr::setProfileCompressType, &cmd, BACKUP_COMPRESS_TYPE_ZSTD) ]);

        /*
         * CREATE BACKUP PROFILE ...  MAX_RATE=<kbps>
         */
        profile_max_rate_option = no_case[lexeme[ lit("MAX_RATE") ]]
          > "="
          > +(char_("0-9"));

        /*
         * CREATE BACKUP PROFILE ... LABEL="<label>"
         *
         * We want to allow file and directory names as a
         * label.
         */
        profile_backup_label_option = no_case[lexeme[ lit("LABEL") ]]
          > "="
          > directory_string
          [boost::bind(&CatalogDescr::setProfileBackupLabel, &cmd, ::_1)];

        /*
         * CREATE BACKUP PROFILE ... WAL=<INCLUDED|EXCLUDED>
         */
        profile_wal_option = no_case[lexeme[ lit("WAL") ]]
          > "="
          > (no_case[lexeme[ lit("INCLUDED") ]]
             [ boost::bind(&CatalogDescr::setProfileWALIncluded, &cmd, true) ]
             |
             no_case[lexeme[ lit("EXCLUDED") ]]
             [ boost::bind(&CatalogDescr::setProfileWALIncluded, &cmd, false) ]
             );

        /*
         * CREATE BACKUP PROFILE ... CHECKPOINT=FAST|DELAYED
         */
        profile_checkpoint_option = no_case[lexeme[ lit("CHECKPOINT") ]]
          > "="
          > (no_case[lexeme[ lit("FAST") ]]
             [ boost::bind(&CatalogDescr::setProfileCheckpointMode, &cmd, true) ]
             | no_case[lexeme[ lit("DELAYED") ]]
             [ boost::bind(&CatalogDescr::setProfileCheckpointMode, &cmd, false) ]
             );

        /*
         * CREATE BACKUP PROFILE ... WAIT_FOR_WAL=TRUE|FALSE
         */
        profile_wait_for_wal_option = no_case[lexeme[ lit("WAIT_FOR_WAL") ]]
          > "="
          > (no_case[lexeme[ lit("TRUE") ]]
             [ boost::bind(&CatalogDescr::setProfileWaitForWAL, &cmd, true) ]
             | no_case[lexeme[ lit("FALSE") ]]
             [ boost::bind(&CatalogDescr::setProfileWaitForWAL, &cmd, false) ]
             );

        /*
         * We try to support both, quoted and unquoted identifiers. With quoted
         * identifiers, we disallow any embedded double quotes, too.
         */
        identifier = ( lexeme [ '"' >> +(char_("a-zA-Z0-9")) >> '"' ]
                       | lexeme [ +(char_("a-zA-Z0-9")) ] );

        /* We enforce quoting for path strings */
        directory_string = no_skip[ '"' >> +(char_ - ('"') ) >> '"' ];

        /* PROFILE property */
        with_profile = no_case[lexeme [ lit("PROFILE") ]] >> identifier
          [ boost::bind(&CatalogDescr::setProfileName, &cmd, ::_1) ];

        /*
         * error handling
         */
        on_error<fail>(start,
                       std::cerr
                       << val("Error! Expecting ")
                       << qi::_4
                       << val(" here: \"")
                       << construct<std::string>(qi::_3, qi::_2)
                       << val("\" ")
                       << std::endl
                       );

        start.name("command start");
        cmd_create.name("CREATE start");
        cmd_drop.name("DROP start");
        cmd_list.name("LIST start");
        cmd_alter.name("ALTER start");
        cmd_create_archive.name("CREATE ARCHIVE");
        cmd_create_backup_profile.name("CREATE BACKUP PROFILE");
        cmd_verify_archive.name("VERIFY ARCHIVE");
        cmd_drop_archive.name("DROP ARCHIVE");
        cmd_drop_backup_profile.name("DROP BACKUP_PROFILE");
        cmd_alter_archive.name("ALTER ARCHIVE");
        cmd_alter_archive_opt.name("ALTER ARCHIVE options");
        cmd_start_basebackup.name("START BASEBACKUP");
        cmd_list_archive.name("LIST ARCHIVE");
        cmd_list_backup.name("LIST BACKUP");
        identifier.name("object identifier");
        hostname.name("ip or hostname");
        profile_compression_option.name("COMPRESSION=GZIP|NONE");
        profile_max_rate_option.name("MAX_RATE=maximum transfer rate in KB/s");
        profile_wal_option.name("WAL=INCLUDED|EXCLUDED");
        profile_backup_label_option.name("LABEL=label string");
        profile_checkpoint_option.name("CHECKPOINT=FAST|DELAYED");
        profile_wait_for_wal_option.name("WAIT_FOR_WAL=TRUE|FALSE");
        database.name("database identifier");
        username.name("username identifier");
        portnumber.name("port number");
        directory_string.name("directory path");
        directory.name("DIRECTORY=path");
        backup_profile_opts.name("backup profile parameters");
        with_profile.name("backup profile name");
      }

      /*
       * Rule return declarations.
       */
      qi::rule<Iterator, ascii::space_type> start;
      qi::rule<Iterator, ascii::space_type> cmd_create, cmd_drop, cmd_list, cmd_alter;
      qi::rule<Iterator, ascii::space_type> cmd_create_archive,
                          cmd_verify_archive,
                          cmd_drop_archive,
                          cmd_alter_archive,
                          cmd_alter_archive_opt,
                          cmd_start_basebackup,
                          cmd_list_archive,
                          cmd_create_backup_profile,
                          cmd_list_backup,
                          cmd_drop_backup_profile,
                          cmd_alter_backup_profile,
                          backup_profile_opts;
      qi::rule<Iterator, std::string(), ascii::space_type> identifier;
      qi::rule<Iterator, std::string(), ascii::space_type> hostname,
                          database,
                          directory,
                          username,
                          portnumber,
                          profile_wal_option,
                          profile_wait_for_wal_option,
                          profile_checkpoint_option,
                          profile_max_rate_option,
                          profile_compression_option,
                          profile_backup_label_option,
                          with_profile;
      qi::rule<Iterator, std::string(), ascii::space_type> property_string,
                          directory_string;

    };

  }
}

PGBackupCtlCommand::PGBackupCtlCommand(CatalogTag tag) {
  this->catalogDescr = make_shared<CatalogDescr>();
  this->catalogDescr->tag = tag;
}

PGBackupCtlCommand::PGBackupCtlCommand(CatalogDescr descr) {
  this->catalogDescr = std::make_shared<CatalogDescr>(descr);
}

PGBackupCtlCommand::~PGBackupCtlCommand() {}

void PGBackupCtlCommand::execute(std::string catalogDir) {

  shared_ptr<CatalogDescr> descr(nullptr);

  /*
   * We only accept a CatalogDescr
   * which can be transformed into an
   * executable descriptor.
   */
  if (this->catalogDescr->tag == EMPTY_DESCR) {
    throw CPGBackupCtlFailure("catalog descriptor is not executable");
  }

  /*
   * First at all we need to create a catalog descriptor
   * which will then support initializing the backup catalog.
   */
  descr = this->getExecutableDescr();

  if (descr == nullptr) {
    throw CPGBackupCtlFailure("cannot execute uninitialized descriptor handle");
  }

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

  /*
   * Based on the assigned catalog descriptor we
   * form an executable descriptor with all derived properties.
   */
  shared_ptr<BaseCatalogCommand> result(nullptr);

  switch(this->catalogDescr->tag) {

  case CREATE_ARCHIVE: {
    result = make_shared<CreateArchiveCatalogCommand>(this->catalogDescr);
    break;
  }

  case DROP_ARCHIVE: {
    result = make_shared<DropArchiveCatalogCommand>(this->catalogDescr);
    break;
  }

  case ALTER_ARCHIVE: {
    result = make_shared<AlterArchiveCatalogCommand>(this->catalogDescr);
    break;
  }

  case VERIFY_ARCHIVE: {
    result = make_shared<VerifyArchiveCatalogCommand>(this->catalogDescr);
    break;
  }

  case START_BASEBACKUP: {
    result = make_shared<StartBasebackupCatalogCommand>(this->catalogDescr);
    break;
  }

  case LIST_BACKUP_CATALOG: {
    result = make_shared<ListBackupCatalogCommand>(this->catalogDescr);
    break;
  }

  case LIST_ARCHIVE: {

    shared_ptr<ListArchiveCatalogCommand> listCmd = std::make_shared<ListArchiveCatalogCommand>(this->catalogDescr);

    /*
     * If an archive name is specified, we request
     * a detail view of this archive.
     */
    if (this->catalogDescr->archive_name != "") {
      listCmd->setOutputMode(ARCHIVE_DETAIL_LIST);
    } else {
      listCmd->setOutputMode(ARCHIVE_LIST);
    }

    result = listCmd;
    break;
  }

  case CREATE_BACKUP_PROFILE: {
    result = make_shared<CreateBackupProfileCatalogCommand>(this->catalogDescr);
    break;
  }

  case LIST_BACKUP_PROFILE:
  case LIST_BACKUP_PROFILE_DETAIL:
    result = make_shared<ListBackupProfileCatalogCommand>(this->catalogDescr);
    break;

  case DROP_BACKUP_PROFILE:
    result = make_shared<DropBackupProfileCatalogCommand>(this->catalogDescr);
    break;

  default:
    /* no-op, but we return nullptr ! */
    break;
  }


  return result;
}

PGBackupCtlParser::PGBackupCtlParser() {}

PGBackupCtlParser::PGBackupCtlParser(path sourceFile) {

  this->sourceFile = sourceFile;
  this->command = make_shared<PGBackupCtlCommand>(EMPTY_DESCR);

}

PGBackupCtlParser::~PGBackupCtlParser() {

}

shared_ptr<PGBackupCtlCommand> PGBackupCtlParser::getCommand() {
  return this->command;
}

void PGBackupCtlParser::parseLine(std::string in) 
  throw(CParserIssue) {

  using boost::spirit::ascii::space;
  typedef std::string::iterator iterator_type;
  typedef credativ::boostparser::PGBackupCtlBoostParser<iterator_type> PGBackupCtlBoostParser;

  /*
   * establish internal boost parser instance.
   */
  PGBackupCtlBoostParser myparser;

  std::string::iterator iter = in.begin();
  std::string::iterator end  = in.end();

  bool parse_result = phrase_parse(iter, end, myparser, space);

  if (parse_result && iter == end) {

    CatalogDescr cmd = myparser.getCommand();
    this->command = make_shared<PGBackupCtlCommand>(cmd);

#ifdef __DEBUG__
    cout << "command " << cmd.tag << endl;
    cout << "parsed ident " << cmd.archive_name << endl;
    cout << "parsed hostname " << cmd.pghost << endl;
    cout << "parsed database " << cmd.pgdatabase << endl;
    cout << "parsed username " << cmd.pguser << endl;
    cout << "parsed directory " << cmd.directory << endl;
    cout << "parsed portnumber " << cmd.pgport << endl;
#endif

  }
  else
    throw CParserIssue("aborted due to parser error");

}

void PGBackupCtlParser::parseFile() throw(CParserIssue) {

  std::ifstream fileHandle;
  std::stringstream fs;
  bool compressed = false;
  std::string line;
  std::ostringstream cmdStr;

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

  /*
   * Read input into a single command string:
   * The parser doesn't handle carriage returns et al.
   */
  while (std::getline(fs, line)) {
    if (cmdStr.tellp() > 0)
      cmdStr << " ";
    cmdStr << line;
  }

  cout << cmdStr.str() << endl;
  this->parseLine(cmdStr.str());
}
