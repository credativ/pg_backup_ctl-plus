/*******************************************************************************
 *
 * pg_backup_ctl++ - A sophisticated backup tool for PostgreSQL databases
 *
 ******************************************************************************/

#include <boost/log/trivial.hpp>

#include <iostream>
#include <string>
#include <popt.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <signal.h>
#include <sys/wait.h>

#include <pg_backup_ctl.hxx>
#include <tab_completion.hxx>
#include <common.hxx>
#include <signalhandler.hxx>
#include <fs-archive.hxx>
#include <parser.hxx>
#include <rtconfig.hxx>

using namespace credativ;
using namespace std;

#define PG_BACKUP_CTL_SUCCESS 0
#define PG_BACKUP_CTL_CATALOG_ERROR 1
#define PG_BACKUP_CTL_ARCHIVE_ERROR 2
#define PG_BACKUP_CTL_PARSER_ERROR 3
#define PG_BACKUP_CTL_GENERIC_ERROR 255

/*
 * Readline loop wants to exit?
 * Set by handle_signal.
 */
volatile bool wants_exit = false;

/*
 * Exit flag indicating that
 * a running command wants to exit.
 */
volatile bool command_abort_requested = false;

/*
 * Global signal handler object.
 */
ConditionalSignalHandler *sigStop = new ConditionalSignalHandler(&command_abort_requested);

/*
 * Runtime configuration handler.
 */
std::shared_ptr<RuntimeConfiguration> RtCfg = nullptr;

/*
 * Handle for command line arguments
 */
typedef struct PGBackupCtlArgs {
  char *hostname;
  char *password;
  char *port;
  char *archiveDir; /* mandatory */
  char *archive_name; /* mandatory for importing/creating archives */
  char *backup_profile; /* optional for start-streaming action */
  char *restoreDir;
  char *relocatedTblspcDir;
  char *action;
  char *actionFile; /* commands read from file */
  char *catalogDir; /* mandatory or compiled in default */
  char **variables = NULL; /* list of runtime variables to set */
  bool  useCompression;
  int   start_launcher = 0;
  int   start_wal_streaming = 0;
} PGBackupCtlArgs;

static void handle_signal_on_input(int sig) {
  if ( (sig == SIGQUIT)
       || (sig == SIGTERM) ) {

    wants_exit = true;
    command_abort_requested = true;

  }

  if ( sig == SIGINT ) {

    command_abort_requested = true;

  }
}

/*
 * SIGCHLD signal handler for parent processes.
 */
static void _pgbckctl_sigchld_handler(int sig) {

  if (sig == SIGCHLD) {
    while (waitpid((pid_t)(-1), 0, WNOHANG) > 0) {}
  }

}

static void printActionHelp() {

  cout <<
    "--action supports the following commands: \n"
       << "\n"
       << "   init-old-archive: initializes an existing pg_backup_ctl archive\n"
       << "\n"
       << "   launcher: initializes and start a launcher instance for the specified catalog\n"
       << "\n"
       << "   start-streaming: start WAL streaming for the specified archive (requires --archive-name)\n"
       << "\n"
       << "   help            : this screen\n"
       << "\n";
}

/*
 * Process command line arguments
 */
static void processCmdLineArgs(int argc,
                               const char **argv,
                               PGBackupCtlArgs *handle) {
  char processedArg;

  /*
   * Make proper handle initialization.
   */
  handle->hostname   = NULL;
  handle->archiveDir = NULL;
  handle->action     = NULL;
  handle->archive_name = NULL;
  handle->catalogDir   = NULL;
  handle->actionFile   = NULL;
  handle->start_launcher = 0;
  handle->start_wal_streaming = 0;
  handle->backup_profile      = NULL;

  /*
   * Set libpopt options.
   */
  poptOption options[] = {

    { "hostname", 'h', POPT_ARG_STRING,
      &handle->hostname, 0, "PostgreSQL instance hostname"},
    { "archive-directory", 'A', POPT_ARG_STRING,
      &handle->archiveDir, 0, "Backup archive directory"},
    { "action", 'a', POPT_ARG_STRING,
      &handle->action, 0, "Backup action command"},
    { "archive-name", 'N', POPT_ARG_STRING,
      &handle->archive_name, 0, "Name of the archive" },
    { "catalog", 'C', POPT_ARG_STRING,
      &handle->catalogDir, 0, PG_BACKUP_CTL_SQLITE},
    { "action-file", 'F', POPT_ARG_STRING,
      &handle->actionFile, 0, "command file"},
    { "launcher", 'L', POPT_ARG_NONE,
      &handle->start_launcher, 0, "start background launcher and exit" },
    { "wal-streamer", 'W', POPT_ARG_NONE,
      &handle->start_wal_streaming, 0, "start WAL streamer on specified archive and exit (requires --archive-name)" },
    { "backup-profile", 'P', POPT_ARG_STRING,
      &handle->backup_profile, 0, "specifies a backup profile used by specified actions" },
    { "variable", 'V', POPT_ARG_ARGV,
      &handle->variables, 0, "runtime variables to be set during execution" },

    POPT_AUTOHELP { NULL, 0, 0, NULL, 0 }
  };

  poptContext context = poptGetContext(argv[0], argc,
                                       (const char **) argv, options, 0);

  /*
   * Process popt arguments.
   */
  processedArg = poptGetNextOpt(context);

  /*
   * Check for bad command line arguments,
   * throw a CPGBackupCtlFailure exception in case
   * something is wrong.
   */
  if (processedArg < -1) {
    /* bad command line argument */
    string errstr;
    errstr = string(poptBadOption(context, POPT_BADOPTION_NOALIAS))
      + string(": ")
      + string((char *)poptStrerror(processedArg));
    throw CPGBackupCtlFailure(errstr.c_str());
  }

}

/*
 * Build runtime configuration.
 */
void init_RtCfg() {

  RtCfg = make_shared<RuntimeConfiguration>();

  std::unordered_set<std::string> enums;

  /*
   * Output format
   */
  enums.insert("json");
  enums.insert("console");

  RtCfg->create("output.format", "console", "console", enums);
  enums.clear();

  /*
   * walstreamer.wait_timeout
   */
  RtCfg->create("walstreamer.wait_timeout", 60, 60, 0, 86400);

  /*
   * The on-error-exit bool parameter causes pg_backup_ctl++ to
   * exit immediately if it gets an error. This most of the time is
   * interesting when executing pg_backup_ctl++ in interactive mode.
   */
  RtCfg->create("interactive.on_error_exit", false, false);

  /*
   * The log_level parameter tells pg_backup_ctl++ what to log.
   */

  std::shared_ptr<ConfigVariable> log_level
    = RtCfg->create("logging.level",
#ifdef __DEBUG__
                    string("debug"),
#else
                    string("info"),
#endif
#ifdef __DEBUG__
                    string("debug")
#else
                    string("info")
#endif
                    );

  /* RuntimeConfiguration::create() doesn't have the assign hook available,
   * so recall after assigning the hook */
  log_level->set_assign_hook(CPGBackupCtlBase::set_log_severity);
  log_level->reassign();

}

/*
 * Checks whether the on_error_variable was set and returns
 * its value.
 */
bool on_error_exit() {

  shared_ptr<ConfigVariable> var = RtCfg->get("interactive.on_error_exit");
  bool forced_exit;

  var->getValue(forced_exit);
  return forced_exit;

}

/*
 * Initialize a command handle with the given command string.
 * The returned command is suitable to be executed immediately.
 *
 * The command string is an ordinary pg_backup_ctl parser command
 * which is parsed and composed into a PGBackupCtlCommand object.
 * Throws CPGBackupCtlFailure and derivatives.
 *
 */
std::shared_ptr<PGBackupCtlCommand> makeCommand(std::string in) {

  std::shared_ptr<PGBackupCtlCommand> command = nullptr;

  try {

    PGBackupCtlParser parser(RtCfg);
    JobSignalHandler *stopHandler;

    /*
     * Parse the command string. This will instantiate
     * a shared_ptr with a PGBackupCtlCommand object, if valid
     */
    parser.parseLine(in);
    command = parser.getCommand();

    /*
     * Assign the stop signal handler. This is the same
     * as in handle_interactive, since we want to be able
     * to abort long running commands.
     */
    stopHandler = dynamic_cast<JobSignalHandler *>(sigStop);
    command->assignSigStopHandler(stopHandler);

  } catch(CParserIssue &pe) {
    BOOST_LOG_TRIVIAL(error) << "parser error: " << pe.what();
    throw pe;
  } catch(CCatalogIssue &ce) {
    BOOST_LOG_TRIVIAL(error) << "parser error: " << ce.what();
    throw ce;
  } catch(std::exception &e) {

    throw e;
  }

  return command;
}


/*
 * Entry point for interactive commands.
 */
static void handle_interactive(std::string in,
                               PGBackupCtlArgs *args) {
  PGBackupCtlParser parser(RtCfg);
  shared_ptr<PGBackupCtlCommand> command;

  try {

    CatalogTag cmdType;

    /*
     * Push the requested command into history
     */
    add_history(in.c_str());

    /*
     * ...and parse and instantiate the command
     * handle.
     */
    command = makeCommand(in);

    /*
     * ... and execute the command.
     */
    cmdType = command->execute(string(args->catalogDir));

    cout << CatalogDescr::commandTagName(cmdType) << endl;

  } catch (exception& e) {
    BOOST_LOG_TRIVIAL(error) << "command execution failure: " << e.what();

    /*
     * Check if runtime configuration variable interactive.on_error_exit
     * was set to TRUE. If yes, re-throw this exception.
     */
    if (on_error_exit())
      throw e;
  }

}

static int handle_inputfile(PGBackupCtlArgs *args) {

  PGBackupCtlParser parser(path(string(args->actionFile)),
                           RtCfg);
  shared_ptr<PGBackupCtlCommand> command;

  try {

    /*
     * Read in the file.
     */
    parser.parseFile();
    command = parser.getCommand();

  } catch (CPGBackupCtlFailure& e) {

    /*
     * Parsing exception catched will force return code
     * PG_BACKUP_CTL_PARSER_ERROR.
     */
    BOOST_LOG_TRIVIAL(error) << "parser error: " << e.what();
    return PG_BACKUP_CTL_PARSER_ERROR;

  }

  /*
   * Parser should have created a valid
   * command handle, suitable to be executed within
   * the current catalog.
   */
  try {
    command->execute(string(args->catalogDir));
  } catch(CPGBackupCtlFailure& e) {

    BOOST_LOG_TRIVIAL(error) << "command execution failure: " << e.what();
    return PG_BACKUP_CTL_CATALOG_ERROR;

  }

  /* Everything seems ok so far. */
  return PG_BACKUP_CTL_SUCCESS;
}

static void executeCommand(PGBackupCtlArgs *args) {

  if (strcmp(args->action, "start-streaming") == 0) {

    /*
     * Start a WAL streamer for the requested backup archive.
     */
    ostringstream cmd_str;
    shared_ptr<PGBackupCtlCommand> command = nullptr;

    /*
     * --archive-name is mandatory here.
     */
    if ( (args->archive_name == NULL) ) {
      ostringstream oss;
      oss << "--archive-name required for command \"start-streaming\"";
      throw CPGBackupCtlFailure(oss.str());
    }

    /*
     * Build the START STREAMING command according to
     * the options passed to pg_backup_ctl++
     */
    cmd_str << "START STREAMING FOR ARCHIVE " << args->archive_name;

    /*
     * ... and execute the command.
     */
    command = makeCommand(cmd_str.str());
    command->execute(string(args->catalogDir));

    exit(0);

  }

  if (strcmp(args->action, "launcher") == 0) {

    /*
     * Starts the pg_backup_ctl job launcher process.
     *
     * We use the START LAUNCHER command string here to instantiate
     * a corresponding command handler.
     */
    shared_ptr<PGBackupCtlCommand> command = makeCommand("START LAUNCHER");
    command->execute(string(args->catalogDir));

    exit (0);

  }

  if (strcmp(args->action, "init-old-archive") == 0) {

    bool is_new_archive = false;

    /*
     * Archive directory is mandatory
     */
    if (args->archiveDir == NULL)
      throw CArchiveIssue("no archive directory specified");

    /*
     * init-old-archive requires a name.
     */
    if (args->archive_name == NULL)
      throw CArchiveIssue("--archive-name is mandatory for --init-old-archive");

    /*
     * Open the sqlite3 database
     */
    BackupCatalog catalog = BackupCatalog(string(args->catalogDir));

    /*
     * Start a transaction in the catalog database.
     */
    catalog.startTransaction();

    /*
     * Read the structure of the specified archive directory
     * and import it into the sqlite backup catalog. Please
     * note that we check if an existing catalog is already registered
     * with the same archive directory.
     */
    CPGBackupCtlFS fs = CPGBackupCtlFS(args->archiveDir);
    shared_ptr<CatalogDescr> descr = catalog.exists(args->archiveDir);

    if (descr->id < 0) {

      /*
       * The specified archive directory is not yet
       * registered in the catalog database. We can't create
       * it immediately, since we need to know whether any compressed
       * files are found (which is reflected in the catalog
       * as well). So just remember this task for later action.
       */
      is_new_archive = true;

      /*
       * Set properties
       */
      descr->directory = args->archiveDir;
      descr->archive_name = args->archive_name;

    }

    try {
      fs.checkArchiveDirectory();

      /*
       * Read in all backup history files from archive.
       */
      fs.readBackupHistory();

      /*
       * Iterate through all history files found and get
       * a CatalogDescr from it. Pass it down to the catalog to
       * create a new backup entry.
       */
      for (auto it : fs.history) {
        shared_ptr<BackupHistoryFile> file = it.second;

#ifdef __DEBUG__
        BOOST_LOG_TRIVIAL(debug) << "backup found: " << file->getBackupLabel()
                                 << " stopped at " << file->getBackupStopTime();
#endif

        if (file->isCompressed()) {
          cout << "found compressed backup history file "
               << file->getBackupHistoryFilename() << endl;
          descr->compression = 1;
        }

        /*
         * Try to get a catalog descriptor from this history file
         * and remember it.
         */
        fs.catalogDescrFromBackupHistoryFile(file);

      }

      if (is_new_archive) {
        catalog.createArchive(descr);
        cout << "new archive dir " << args->archiveDir << " registered in catalog" << endl;
      } else {
        cout << "archive directory " << args->archiveDir << " updated" << endl;
      }

      /* Commit the catalog transaction and we're done */
      catalog.commitTransaction();

    } catch (CArchiveIssue &e) {
      BOOST_LOG_TRIVIAL(error) << e.what();
      catalog.rollbackTransaction();
    }
  }
  else if (strcmp(args->action, "help") == 0) {

    printActionHelp();

  }
  else {
    string errstr;
    errstr = string("unknown command: ") + string(args->action);
    throw CPGBackupCtlFailure(errstr.c_str());
  }

}

int main(int argc, const char **argv) {

  char *cmd_str = NULL;
  shared_ptr<CPGBackupCtlBase> backup = make_shared<CPGBackupCtlBase>();
  PGBackupCtlArgs args;

  /*
   * Certain actions fork off child processes. To prevent
   * zombies, we need a SIGCHLD signal handler.
   */
  if (signal(SIGCHLD, _pgbckctl_sigchld_handler) == SIG_ERR) {
    BOOST_LOG_TRIVIAL(error) << "error setting up parent signal handler";
    exit(PG_BACKUP_CTL_GENERIC_ERROR);
  }

  try {

    /**
     * Build map of runtime parameters.
     */
    init_RtCfg();

    /*
     * Process command line arguments.
     */
    processCmdLineArgs(argc, argv, &args);

    /*
     * Catalog required, if not use compiled in default.
     */
    if (args.catalogDir == NULL) {
      /*
       * We use a dynamically allocated string object here,
       * since this needs to live through our whole
       * program lifetime.
       */
      args.catalogDir = (char *)(new string(PG_BACKUP_CTL_SQLITE))->c_str();
      BOOST_LOG_TRIVIAL(info) << "--catalog not specified, using " << args.catalogDir;
    }

    /*
     * Iff --action and --action-file are specified concurrently, throw
     * an error.
     */
    if ((args.action != NULL)
        && (args.actionFile != NULL)) {
      throw CArchiveIssue("--action and --action-file cannot be specified concurrently");
    }

    /*
     * Iff --action or --action-file is specified concurrently
     * with --launcher, exit.
     */
    if ( ( (args.action != NULL) || (args.actionFile != NULL) )
         && (args.start_launcher > 0) ) {
      throw CArchiveIssue("--action or --action-file cannot be specified with --launcher");
    }

    /*
     * The same with --wal-streamer
     */
    if ( ( (args.action != NULL) || (args.actionFile != NULL) )
         && (args.start_wal_streaming > 0) ) {
      throw CArchiveIssue("--action or --action-file cannot be specified with --wal-streamer");
    }

    /*
     * ... and check for mutual exclusive options.
     */
    if ( ( args.start_launcher > 0 ) && ( args.start_wal_streaming > 0 ) ) {
      throw CArchiveIssue("--launcher and --wal-streamer cannot be specified at the same time");
    }

    /*
     * All required command line arguments read, proceed ...
     * This is also the main entry point into
     * the backup machinery.
     *
     * We support action commands directly passed to
     * the program and a small grammar automating certain
     * tasks. It doesn't make sense to parse them equally,
     * so check them separately...
     *
     * NOTE: --action has precedence over all others.
     *       Then --action-file and other action command line
     *       parameters and finally interactive command
     *       line processing via readline.
     */
    if (args.action != NULL) {
      executeCommand(&args);
      exit(0);
    }

    if (args.actionFile != NULL) {
      int rc = handle_inputfile(&args);
      return rc;
    }

    /* ***************************************************
     * Command line action command line parameters here...
     * ***************************************************/

    if (args.start_launcher > 0) {

      /*
       * Starts the pg_backup_ctl job launcher process.
       *
       * We use the START LAUNCHER command string here to instantiate
       * a corresponding command handler.
       */
      shared_ptr<PGBackupCtlCommand> command = makeCommand("START LAUNCHER");

      command->execute(string(args.catalogDir));
      exit (0);

    }

    if (args.start_wal_streaming > 0) {

      shared_ptr<PGBackupCtlCommand> command = nullptr;
      ostringstream cmd_str;

      /* --archive-name is required */
      if (args.archive_name == NULL) {
        throw CArchiveIssue("--archive-name is mandatory with --wal-streamer");
      }

      cmd_str << "START STREAMING FOR ARCHIVE " << args.archive_name;

      /*
       * Execute the command and exit. This might throw here, but
       * the outer exception block will handle any errors here
       * accordingly.
       */
      command = makeCommand(cmd_str.str());
      command->execute(string(args.catalogDir));

      exit(0);
    }

    /* ***************************************************
     * Command line action command line parameters ends!
     * ***************************************************/

    /*
     * Before entering interactive input mode, setup the proper
     * signal handling...
     */
    if (signal(SIGQUIT, handle_signal_on_input) == SIG_ERR) {
      BOOST_LOG_TRIVIAL(error) << "error setting up input signal handler";
      exit(PG_BACKUP_CTL_GENERIC_ERROR);
    }

    if (signal(SIGINT, handle_signal_on_input) == SIG_ERR) {
      BOOST_LOG_TRIVIAL(error) << "error setting up input signal handler";
      exit(PG_BACKUP_CTL_GENERIC_ERROR);
    }

    if (signal(SIGTERM, handle_signal_on_input) == SIG_ERR) {
      BOOST_LOG_TRIVIAL(error) << "error setting up input signal handler";
      exit(PG_BACKUP_CTL_GENERIC_ERROR);
    }

    /* prepare readline support */
    init_readline(string(args.catalogDir),
                  RtCfg);

    while(!wants_exit) {

      /* input buffer */
      string input = "";

      while (!wants_exit
             && (cmd_str = readline("pg_backup_ctl++> ")) != NULL) {

        if (strcmp(cmd_str, "quit") == 0) {
          wants_exit = true;
          break;
        }

        /*
         * End of the command is indicated by ';'.
         * Currently, this is not handled by the parser itself.
         */
        if (cmd_str[strlen(cmd_str) - 1] == ';') {
          input += string(cmd_str, strlen(cmd_str) - 1);

          /*
           * We also need to reset the global readline state.
           */
          step_readline();

          break;
        } else {
          /*
           * No action needed, so we continue reading input.
           * To support commandseparation by newline, the
           * command string is appended by a blank.
           * This behaviour is similar to our parseFile routine.
           */
          input += string(cmd_str)+" ";
        }

      }

      /*
       * On EOF, readline() returns NULL, so check that, which
       * means we should also exit outer loop
       */
      if (cmd_str == NULL) {
        wants_exit=true;
        continue;
      }

      /*
       * inner loop reports exit attempt, so give it priority
       * and discard any previous input.
       * It's okay to just break out here, since wants_exit
       * is already set by signal.
       */

      if (wants_exit) {
        cout << "quit" << endl;
        break;
      }

      handle_interactive(input, &args);
      free(cmd_str);
    }

    exit(0);

  } catch (exception& e) {
    BOOST_LOG_TRIVIAL(error) << e.what();
    exit(1);
  }

  exit(0);
}
