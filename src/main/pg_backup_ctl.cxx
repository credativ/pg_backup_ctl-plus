/*******************************************************************************
 *
 * pg_backup_ctl++ - A sophisticated backup tool for PostgreSQL databases
 *
 ******************************************************************************/

#include <iostream>
#include <string>
#include <popt.h>

#include <pg_backup_ctl.hxx>
#include <common.hxx>
#include <fs-archive.hxx>

using namespace credativ;
using namespace std;

/*
 * Handle for command line arguments
 */
typedef struct PGBackupCtlArgs {
  char *hostname;
  char *password;
  char *port;
  char *archiveDir; /* mandatory */
  char *restoreDir;
  char *relocatedTblspcDir;
  char *action;
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
    { "catalog", 'C', POPT_ARG_STRING,
      &handle->catalogDir, 0, PG_BACKUP_CTL_SQLITE},

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

static void executeCommand(PGBackupCtlArgs *args) {

  /*
   * Empty action not allowed
   */
  if (args->action == NULL)
    throw CArchiveIssue("no action specified (--action)");

  /*
   * Catalog required, if not use compiled in default.
   */
  if (args->catalogDir == NULL) {
    cerr << "--catalog not specified, using " << PG_BACKUP_CTL_SQLITE << endl;
    args->catalogDir = (char *)string(PG_BACKUP_CTL_SQLITE).c_str();
  }

  if (strcmp(args->action, "init-old-archive") == 0) {

    /*
     * Archive directory is mandatory
     */
    if (args->archiveDir == NULL)
      throw CArchiveIssue("no archive directory specified");

    /*
     * Open the sqlite3 database
     */
    BackupCatalog catalog = BackupCatalog(PG_BACKUP_CTL_SQLITE,
                                          string(args->archiveDir));

    /*
     * Read the structure of the specified archive directory
     * and import it into the sqlite backup catalog. Please
     * note that we check if an existing catalog is already registered
     * with the same archive directory.
     */
    CPGBackupCtlFS fs = CPGBackupCtlFS(args->archiveDir);

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

        /*
         * Try to get a catalog descriptor from this history file
         */

      }
    } catch (CArchiveIssue &e) {
      cerr << e.what() << "\n";
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

  try {
    /*
     * Process command line arguments.
     */
    processCmdLineArgs(argc, argv, &args);

    /*
     * All required command line arguments read, proceed ...
     * This is also the main entry point into
     * the backup machinery.
     */
    executeCommand(&args);
  }
  catch (exception& e) {
    cerr << e.what() << "\n";
    exit(1);
  }

  exit(0);
}
