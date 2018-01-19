#ifndef __DAEMON_HXX__
#define __DAEMON_HXX__

#include <common.hxx>
#include <jobhandles.hxx>
#include <BackupCatalog.hxx>

#define DAEMON_RUN 0
#define DAEMON_TERM_NORMAL 1
#define DAEMON_TERM_EMERGENCY 2
#define DAEMON_STATUS_UPDATE 3
#define DAEMON_FAILURE 4

namespace credativ {

  /* Forwarded declarations */
  class BackgroundWorker;
  class BaseCatalogCommand;
  class BackupCatalog;

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

  pid_t launch(job_info& info);
  void establish_launcher_cmd_queue(job_info &info);
  void send_launcher_cmd(job_info& info, std::string command);
  std::string recv_launcher_cmd(job_info &info, bool &cmd_received);
  pid_t worker_command(job_info &info, std::string command);

  /**
   * Runs a blocking child subprocess.
   */
  pid_t run_process(job_info &info);

}

#endif
