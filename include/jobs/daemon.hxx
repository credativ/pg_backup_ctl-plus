#include <common.hxx>
#include <BackupCatalog.hxx>

#define DAEMON_RUN 0
#define DAEMON_TERM_NORMAL 1
#define DAEMON_TERM_EMERGENCY 2
#define DAEMON_STATUS_UPDATE 3
#define DAEMON_FAILURE 4

namespace credativ {

  /* Forwarded declarations */
  class BackgroundWorker;

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
     * Catalog/Command descriptor. The descriptor usually is
     * initialized by the caller and passed to the worker
     * process.
     */
    std::shared_ptr<BackgroundWorkerCommandHandle> cmdHandle;

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

  /*
   * Worker errors are mapped to
   * WorkerFailure exceptions.
   */
  class WorkerFailure : public credativ::CPGBackupCtlFailure {
  public:
    WorkerFailure(const char *errstr) throw() : CPGBackupCtlFailure(errstr) {};
    WorkerFailure(std::string errstr) throw() : CPGBackupCtlFailure(errstr) {};
  };

  /*
   * Launcher and worker classes.
   * Encapsulates routines for job maintenance.
   */
  class BackgroundWorker {
  private:
    bool checkPID(pid_t pid);
    void registerMe();
  protected:
    /*
     * Structure holding compacted status information
     * for a background worker instance. Also used
     * for BackupCatalog interaction.
     *
     * Initialized by c'tor.
     */
    std::shared_ptr<CatalogProc> procInfo = nullptr;

    /*
     * Job action handle, carries information what
     * this background worker should do and catalog
     * information.
     */
    job_info ji;

    /*
     * catalog handle, usually initialized by c'tor.
     */
    std::shared_ptr<BackupCatalog> catalog = nullptr;
  public:
    BackgroundWorker(job_info info);
    ~BackgroundWorker();

    /*
     * Initializes properties of this workers and
     * registers it into the catalog.
     */
    virtual void initialize();

    /*
     * Prepare a worker for clean shutdown (smart shutdown
     * request). Also removes every runtime information
     * from the catalog.
     */
    virtual void prepareShutdown();
  };
}

extern pid_t launch(job_info& info);
