/*******************************************************************************
 *
 * pg_backup_ctl++ - A sophisticated backup tool for PostgreSQL databases
 *
 ******************************************************************************/

#include <iostream>
#include <string>
#include <popt.h>
#include <readline/readline.h>

#include <pg_backup_ctl.hxx>
#include <common.hxx>
#include <fs-archive.hxx>
#include <parser.hxx>

using namespace credativ;
using namespace std;

#define PG_BACKUP_CTL_SUCCESS 0
#define PG_BACKUP_CTL_CATALOG_ERROR 1
#define PG_BACKUP_CTL_ARCHIVE_ERROR 2
#define PG_BACKUP_CTL_PARSER_ERROR 3
#define PG_BACKUP_CTL_GENERIC_ERROR 255

/*
 * Handle for command line arguments
 */
typedef struct PGBackupCtlArgs {
  char *hostname;
  char *password;
  char *port;
  char *archiveDir; /* mandatory */
  char *archive_name; /* mandatory for importing/creating archives */
  char *restoreDir;
  char *relocatedTblspcDir;
  char *action;
  char *actionFile; /* commands read from file */
  char *catalogDir; /* mandatory or compiled in default */
  bool useCompression;
} PGBackupCtlArgs;

static void printActionHelp() {

  cout <<
    "--action supports the following commands: \n"
       << "\n"
       << "   init-old-archive: initializes an existing pg_backup_ctl archive\n"
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
  memset(handle, 0x0, sizeof(PGBackupCtlArgs));

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
 * Entry point for interactive commands.
 */
static void handle_interactive(std::string in,
                               PGBackupCtlArgs *args) {
  PGBackupCtlParser parser;
  shared_ptr<PGBackupCtlCommand> command;
  
  parser.parseLine(in);

  /*
   * Parser should have created a valid
   * command handle, suitable to be executed within
   * the current catalog.
   */
  command = parser.getCommand();
  command->execute(string(args->catalogDir));
}

static int handle_inputfile(PGBackupCtlArgs *args) {

  PGBackupCtlParser parser(path(string(args->actionFile)));
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
    cerr << "parser error: " << e.what() << endl;
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

    cerr << "command execution failure: " << e.what() << endl;
    return PG_BACKUP_CTL_CATALOG_ERROR;

  }
}

static void executeCommand(PGBackupCtlArgs *args) {

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
    BackupCatalog catalog = BackupCatalog(string(args->catalogDir),
                                          string(args->archiveDir));

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
       * it immediately, since we need to know wether any compressed
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
        cerr << "backup found: " << file->getBackupLabel() 
             << " stopped at " << file->getBackupStopTime() 
             <<  endl;
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
      cerr << e.what() << "\n";
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

  shared_ptr<CPGBackupCtlBase> backup = make_shared<CPGBackupCtlBase>();
  PGBackupCtlArgs args;
  char            *cmd_str;

  try {
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
       * since this needs to live through our while
       * program lifetime.
       */
      args.catalogDir = (char *)(new string(PG_BACKUP_CTL_SQLITE))->c_str();
      cerr << "--catalog not specified, using " << args.catalogDir << endl;
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
     *       Then --action-file and interactive command
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

    cerr << "catalog " << args.catalogDir << endl;
    while ((cmd_str = readline("pg_backup_ctl++> ")) != NULL) {
      handle_interactive(string(cmd_str), &args);
      free(cmd_str);
    }

    exit(0);

  }
  catch (exception& e) {
    cerr << e.what() << "\n";
    exit(1);
  }

  exit(0);
}
