#ifndef __DAEMON_HXX__
#define __DAEMON_HXX__

#include <common.hxx>
#include <jobhandles.hxx>
#include <shm.hxx>
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

    /**
     * Current state of this worker object.
     */
    LauncherStatus launcher_status = LAUNCHER_SHUTDOWN;

  protected:

    /**
     * A background worker is basically a
     * launcher, which primarily forks and executes
     * command handles in the background. To control
     * various behavior, we employ a shared memory
     * segment which safes some of its properties.
     *
     * The LauncherSHM shared memory area is to
     * control whether a launcher instance is already started
     * and to safe operation states. Only one launcher
     * shared memory segment exists per catalog and
     * only the launcher itself should be attached to it.
     */
    LauncherSHM my_shm;

    /**
     * The worker shared memory area is to control
     * running state of background processes launched
     * by the launcher process.
     */
    WorkerSHM *worker_shm = nullptr;

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

    /**
     * Reaper handler.
     */
    background_reaper *reaper = nullptr;

    /*
     * catalog handle, usually initialized by c'tor.
     */
    std::shared_ptr<BackupCatalog> catalog = nullptr;

  public:
    BackgroundWorker(job_info info);
    ~BackgroundWorker();

    /**
     * Initializes properties of this workers and
     * registers it into the catalog.
     */
    virtual void initialize();

    /**
     * Prepare a worker for clean shutdown (smart shutdown
     * request). Also removes every runtime information
     * from the catalog.
     */
    virtual void prepareShutdown();

    /**
     * Returns a pointer to the worker shared memory segment.
     */
    virtual WorkerSHM *workerSHM();

    /**
     * Returns a copy of the associated job_handle structure.
     */
    virtual job_info jobInfo();

    /*
     * Release the launcher identity.
     *
     * This should be called after a fork of
     * a background worker to release the launcher role, e.g.
     * like credativ::worker_command() does.
     *
     * Basically the release_launcher_role() leaves the
     * background launcher shared memory.
     */
    virtual void release_launcher_role();

    /**
     * Returns operation status
     */
    virtual LauncherStatus status();

    /**
     * Mark background worker as running
     */
    virtual void run();

    /**
     * Assigns an external reaper handle.
     *
     * A reaper handle recognizes dead PIDs that are
     * are required to be reaped from our internal worker
     * shared memory segment.
     *
     * The reason why we need such an external interface is that
     * a signal handler (especially SIGCHLD handlers) don't have
     * access to our internal worker shared memory area. The
     * dead PIDS are reaped by calling execute_reaper(), which
     * should happen periodically to avoid wasting too much
     * worker slots in shared memory.
     */
    virtual void assign_reaper(background_reaper *reaper);

    /**
     * Executes the reaping process of dead PIDs. This is a no-op
     * in case no reaper handle was assigned via assign_reaper();
     */
    virtual void execute_reaper();

  };

  pid_t launch(job_info& info);
  void establish_launcher_cmd_queue(job_info &info);
  void send_launcher_cmd(job_info& info, std::string command);
  std::string recv_launcher_cmd(job_info &info, bool &cmd_received);
  pid_t worker_command(BackgroundWorker &worker, std::string command);

  /**
   * Returns true in case a background launcher process
   * for the given catalog instance is really running.
   */
  bool launcher_is_running(std::shared_ptr<CatalogProc> procInfo);

  /**
   * Runs a blocking child subprocess.
   */
  pid_t run_process(job_info &info);

  /**
   * Runs a blocking child subprocess via popen().
   *
   * Allows unidirectional pipes only. This, job_info.use_pipe
   * and its corresponding pipe_in/pipe_out handles are ignored.
   *
   * Since popen() uses the file stream API, calling run_pipelined_command()
   * initializes the fpipe_handle and requires the correct po_mode fields
   * in the specified job_info structure to be set. Thus, a valid job
   * handle suitable to call run_pipelined_command() has these fields set:
   *
   * job_info.executable
   * job_info.execArgs
   * job_info.background_exec = true
   * job_info.po_mode
   */
  FILE * run_pipelined_command(job_info &info);
}

#endif
