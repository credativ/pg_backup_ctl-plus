#include <common.hxx>
#include <BackupCatalog.hxx>

#define DAEMON_RUN 0
#define DAEMON_TERM_NORMAL 1
#define DAEMON_TERM_EMERGENCY 2
#define DAEMON_STATUS_UPDATE 3


namespace credativ {

  struct job_info {

    /*
     * PID from fork(). 0 is the background
     * process, others launcher processes.
     */
    pid_t pid;

    /*
     * Background process detaches from
     * parent.
     */
    bool detach = false;

    /*
     * Instruct launcher control to close
     * all standard filehandles. This currently includes:
     * - STDIN
     * - STDOUT
     * - STDERR
     */
    bool close_std_fd = false;

    /*
     * Full descriptive string for the target catalog. E.g.
     * path to sqlite3 database.
     */
    std::string catalogName;

    /*
     * Catalog/Command descriptor. The descriptor should
     * never ever initialized by the caller, only after
     * the launched subprocess itself!!
     */
    std::shared_ptr<CatalogDescr> catalogDescr = nullptr;

  };

  /*
   * Launcher errors are mapped to
   * LauncherFailure exceptions.
   */
  class LauncherFailure : public credativ::CPGBackupCtlFailure {
  public:
    LauncherFailure(const char *errstr) throw() : CPGBackupCtlFailure(errstr) {};
    LauncherFailure(std::string errstr) throw() : CPGBackupCtlFailure(errstr) {};
  };

}

extern pid_t launch(job_info& info);
