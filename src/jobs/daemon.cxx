#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <syslog.h>
#include <string.h>
#include <signal.h>
#include <istream>

#ifdef __DEBUG__
#include <iostream>
#endif

#include <daemon.hxx>
#include <parser.hxx>
#include <commands.hxx>

#define MSG_QUEUE_MAX_TOKEN_SZ 255

using namespace credativ;

volatile sig_atomic_t _pgbckctl_shutdown_mode = DAEMON_RUN;
extern int errno;

/*
 * Forwarded declarations.
 */
static pid_t daemonize(job_info &info);

/*
 * Signal handler objects
 */
AtomicSignalHandler *termHandler = new AtomicSignalHandler(&_pgbckctl_shutdown_mode,
                                                           DAEMON_TERM_NORMAL);
AtomicSignalHandler *emergencyHandler = new AtomicSignalHandler(&_pgbckctl_shutdown_mode,
                                                                DAEMON_TERM_EMERGENCY);

BackgroundWorker::BackgroundWorker(job_info info) {

  this->ji = info;
  this->catalog = this->ji.cmdHandle->getCatalog();
  this->procInfo = std::make_shared<CatalogProc>();

}

BackgroundWorker::~BackgroundWorker() {

}

bool BackgroundWorker::checkPID(pid_t pid) {

  bool result = true;

#ifdef __DEBUG__
  cerr << "checkPID(): checking PID " << pid << endl;
#endif

  /*
   * Send SIGNAL 0 to the specified PID
   */
  if (kill(pid, 0) < 0) {
    /* check errno */
    int rc = errno;

    if (rc == ESRCH) {
      /* PID does not exist */
#ifdef __DEBUG__
      cerr << "checkPID(): " << pid << " does not exist" << endl;
#endif
      result = false;
    }

    if (rc == EPERM) {
      /* No permissions for this PID ... */
#ifdef __DEBUG__
      cerr << "checkPID(): " << pid << " permission denied" << endl;
#endif
      result = false;
    }

    return result;
  }

  return result;
}

void BackgroundWorker::prepareShutdown() {

  /*
   * NOTE: We don't catch any exceptions here,
   * this is done by the initialize() caller, since
   * we remap those errors into a WorkerFailure exception.
   */

  /*
   * Mark us as shutdown.
   */
  catalog->startTransaction();

  try {
    catalog->unregisterProc(procInfo->pid, procInfo->archive_id);
  } catch (std::exception& e) {
    catalog->rollbackTransaction();
    /* don't hide exception from caller */
    throw e;
  }

  catalog->commitTransaction();
}

void BackgroundWorker::registerMe() {

  std::shared_ptr<CatalogProc> tempProc;

  /*
   * NOTE: We don't catch any exceptions here,
   * this is done by the initialize() caller, since
   * we remap those errors into a WorkerFailure exception.
   */

  /*
   * Register a dummy worker for now. This means
   * this worker is *not* associated with a specific
   * archive.
   */

  /*
   * Registering a worker needs to recognize the
   * following possible preconditions:
   *
   * - If a former worker was already launched on an archive,
   *   we just update its process state in the catalog. Thus, we
   *   need to check if an entry for this worker already exists.
   *   If TRUE, we update its PID and set its state to RUNNING.
   *
   * - If a former worker crashed or did an emergency exit,
   *   we'll find an orphaned running state of this worker.
   *   We need to try hard to check, wether this process is
   *   still alive. The current procedure for this is as
   *   follows:
   *
   *   signal(0) the stored PID to check if the PID still
   *   is alive and belongs to us.
   *
   * - If no entry is found, just create one with the
   *   proper attributes.
   */

  procInfo->pid = getpid();
  procInfo->archive_id = -1;
  procInfo->type = CatalogProc::PROC_TYPE_LAUNCHER;
  procInfo->state = CatalogProc::PROC_STATUS_RUNNING;
  procInfo->started = CPGBackupCtlBase::current_timestamp();
  procInfo->shm_key = -1;

  /*
   * Check if there is a catalog entry for this kind
   * of worker.
   */
  catalog->startTransaction();

  try {

    std::cerr << "checking for worker for archive " << procInfo->archive_id << std::endl;
    tempProc = catalog->getProc(procInfo->archive_id,
                                procInfo->type);

    if (tempProc != nullptr
        && tempProc->pid > 0) {

      /*
       * There is an existing entry for this kind of worker.
       * Check, if the PID is still alive and belongs to us.
       *
       * XXX: This is not really safe yet, since the PID
       *      could have been reused in the meantime. We
       *      need a further check here, to verify that
       *      the existing PID is not really one of us.
       */
      if (!checkPID(tempProc->pid)) {

        /*
         * The retrieved PID is dead. Remove the old one.
         */
        catalog->unregisterProc(tempProc->pid, tempProc->archive_id);

      } else {
        /* oops, PID seems alive */
        std::ostringstream oss;
        oss << "worker with PID "
            << tempProc->pid
            << ", type "
            << tempProc->type
            << " already active";
        throw WorkerFailure(oss.str());
      }

    }

    /*
     * Tell the catalog which fields we need
     */
    procInfo->pushAffectedAttribute(SQL_PROCS_PID_ATTNO);
    procInfo->pushAffectedAttribute(SQL_PROCS_ARCHIVE_ID_ATTNO);
    procInfo->pushAffectedAttribute(SQL_PROCS_TYPE_ATTNO);
    procInfo->pushAffectedAttribute(SQL_PROCS_STARTED_ATTNO);
    procInfo->pushAffectedAttribute(SQL_PROCS_STATE_ATTNO);
    procInfo->pushAffectedAttribute(SQL_PROCS_SHM_KEY_ATTNO);

    catalog->registerProc(procInfo);

  } catch (CPGBackupCtlFailure& e) {

    catalog->rollbackTransaction();

    /* don't hide exception from caller */
    throw e;

  }

  /* should still be in progress */
  catalog->commitTransaction();

}

void BackgroundWorker::initialize() {

  /*
   * Shouldn't be a nullptr...
   */
  if (this->catalog == nullptr)
    throw WorkerFailure("catalog handle for worker not initialized");

  if (this->procInfo == nullptr)
    throw WorkerFailure("background worker information not initialized");

#ifdef __DEBUG__
  cerr << "worker PID " << getpid()
       << " command tag " << this->ji.cmdHandle->tag << endl;
  cerr << "worker catalog " << this->ji.cmdHandle->getCatalog()->name() << endl;
#endif

  /*
   * Initialization phase should run within an
   * try...catch block to make sure we can react
   * on e.g. database failures. We remap those errors
   * into a WorkerFailure exception since we don't expect
   * the launcher to operate on all kind of exceptions
   * thrown by the pg_backup_ctl++ API...
   */
  try {

    this->registerMe();

  } catch(CPGBackupCtlFailure& e) {

#ifdef __DEBUG__
    cerr << "error registering launcher process, forcing shutdown: "
         << e.what()
         << endl;
#endif

    _pgbckctl_shutdown_mode = DAEMON_TERM_EMERGENCY;
  }

}

LauncherSHM::LauncherSHM() {

  this->shm = nullptr;
}

LauncherSHM::~LauncherSHM() {

  if (this->shm != nullptr) {
    delete this->shm;
  }

}

/*
 * pg_backup_ctl launcher signal handler
 */
static void _pgbckctl_sighandler(int sig) {

  /*
   * Check wether we are called recursively. Reraise
   * the signal again, if true.
   */
  if (_pgbckctl_shutdown_mode != DAEMON_RUN) {
    raise(sig);
  }

  if (sig == SIGTERM) {
    _pgbckctl_shutdown_mode = DAEMON_TERM_NORMAL;
  }

  if (sig == SIGINT) {
    _pgbckctl_shutdown_mode = DAEMON_TERM_EMERGENCY;
  }

  if (sig == SIGQUIT) {
    _pgbckctl_shutdown_mode = DAEMON_TERM_EMERGENCY;
  }

  if (sig == SIGHUP) {
    /* not yet */
  }

  if (sig == SIGUSR1) {
    _pgbckctl_shutdown_mode = DAEMON_STATUS_UPDATE;
  }

}

static pid_t daemonize(job_info &info) {

  /*
   * Holds PID of parent and child process
   */
  pid_t pid;
  pid_t sid;
  int forkerrno;

  /*
   * First at all, fork off the launcher. This
   * parent will fork again to launch the actual
   * background process and will exit.
   */

  if (info.detach) {

    cout << "parent launcher detaching" << endl;

    pid = fork();
    forkerrno = errno;

    if (pid < 0) {
      std::ostringstream oss;
      oss << "fork error " << strerror(forkerrno);
      throw LauncherFailure(oss.str());
    }

    if (pid > 0) {
      cout << "parent launcher forked with pid " << pid << ", detaching" << endl;
      waitpid(pid, NULL, WNOHANG);

      /*
       * This will force to return to the caller, child
       * will resume to fork() specific worker process...
       */
      info.pid = pid;
      return pid;
    }

  }

  /*
   * The background process will have the following
   * conditions set:
   *
   * - Change into the archive directory
   * - Close all STDIN and STDOUT file descriptors,
   *   if requested
   */
  if (info.close_std_fd) {
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
  }

  /*
   * Install signal handlers.
   */
  signal(SIGTERM, _pgbckctl_sighandler);
  signal(SIGINT, _pgbckctl_sighandler);
  signal(SIGQUIT, _pgbckctl_sighandler);
  signal(SIGHUP, _pgbckctl_sighandler);
  signal(SIGUSR1, _pgbckctl_sighandler);

  /*
   * Now launch the background job.
   */
  pid = fork();

  if (pid < 0) {
    std::ostringstream oss;
    oss << "fork error " << strerror(forkerrno);
    throw LauncherFailure(oss.str());
  }

  if (pid > 0) {

    int wstatus;

    /*
     * This is the launcher's parent process.
     *
     * Set the session  leader.
     */
    if (info.detach) {
      sid = setsid();
      if (sid < 0) {
        std::ostringstream oss;
        oss << "could not set session leader: " << strerror(errno);
        throw LauncherFailure(oss.str());
      }
    }

    /*
     * Install SIGUSR1 handler for launcher.
     */
    do {

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_NORMAL) {
        std::cout << "launcher shutdown request received"
                  << std::endl;
        break;
      }

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_EMERGENCY) {
        std::cout << "launcher emergency shutdown request received"
                  << std::endl;
        break;
      }

      if (waitpid(-1, &wstatus, WUNTRACED | WCONTINUED | WNOHANG) == -1) {
        std::cerr << "waitpid() error " << endl;
        exit(DAEMON_FAILURE);
      }

      /*
       * Detach, after that, the child run's
       * as a daemon.
       */
      if (info.detach)
        break;
      usleep(10);

    } while(true);

    exit(_pgbckctl_shutdown_mode);
  }

  if (pid == 0) {
    /*
     * This is the initial background worker (aka launcher) process
     */
    BackgroundWorker worker(info);

    /*
     * Flag, indicating that we got a valid command string
     * from the msg queue.
     */
    bool cmd_ok;

    /*
     * Register launcher in the database.
     */
    worker.initialize();

    /* mark this as a background worker */
      info.background_exec = true;

    /*
     * Setup message queue.
     */
    establish_launcher_cmd_queue(info);

    /*
     * Enter processing loop
     */
    while(true) {

      usleep(1000);

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_NORMAL) {
        std::cout << "shutdown request received" << std::endl;

        /*
         * This is considered a smart shutdown, where we
         * clean up all stuff which seems to be necessary for
         * clean startup again.
         */
        try {
          worker.prepareShutdown();
        } catch (std::exception& e) {
          cerr << "smart shutdown catched error: " << e.what() << endl;
        }
        break;
      }

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_EMERGENCY) {
        std::cout << "emergency shutdown request received" << std::endl;
        break;
      }

      /*
       * Check message queue wether there is something to do.
       */
      std::string command = recv_launcher_cmd(info, cmd_ok);

      if (cmd_ok) {

        /*
         * We got a command string. Establish a command
         * handler and proceed. We don't do this ourselves, but
         * fork off a new command process to actually execute the
         * command.
         */
        try {

          cerr << "BACKGROUND COMMAND: " << command << endl;

          /*
           * Execute the command
           */
          if (worker_command(info, command) == (pid_t) 0) {

            /* child should exit after having done its duty */
            exit(0);
          }

        } catch (CParserIssue &pe) {
          cerr << "parser failed: " << pe.what() << endl;
          exit(DAEMON_FAILURE);
        } catch (WorkerFailure &e) {
          /* something shitty has happend ... */
          cerr << "fork() failed: " << e.what() << endl;
          exit(DAEMON_FAILURE);
        } catch (std::exception &e) {
          cerr << "background worker failure: " << e.what() << endl;
          exit(DAEMON_FAILURE);
        }

      }

    }

    exit(_pgbckctl_shutdown_mode);
  } /* child execution code */

  info.pid = pid;
  return pid;

}

void credativ::establish_launcher_cmd_queue(job_info& info) {

  std::string message_queue_name;

  using namespace boost::interprocess;

  try {

    if (info.command_queue == nullptr) {

      /*
       * Create or open/attach the specified message queue.
       * Be aware that we need to choose the correct one, since
       * every launcher and workers are catalog related.
       */
      message_queue_name = "pg_backup_ctl::command_queue::"
        + info.cmdHandle->getCatalog()->name();

      info.command_queue = new message_queue(open_or_create,
                                             message_queue_name.c_str(),
                                             255,
                                             MSG_QUEUE_MAX_TOKEN_SZ);
    }

  } catch(interprocess_exception &e) {

    /*
     * Caller shouldn't deal with interprocess_exception directly,
     * so map it to LauncherFailure.
     */
    throw LauncherFailure(e.what());
  }

}

void credativ::send_launcher_cmd(job_info& info, std::string command) {

  using namespace boost::interprocess;

  /*
   * Having a uninitialized message queue forces
   * a generic failure.
   */
  if (info.command_queue == nullptr) {
    throw CPGBackupCtlFailure("could not send command with uninitialized message queue");
  }

  /*
   * If command string has zero length, omit the command.
   */
  if (command.length() <= 0)
    return;

  /*
   * Command can't be larger than MSG_QUEUE_MAX_TOKEN_SZ.
   */
  if (command.length() > MSG_QUEUE_MAX_TOKEN_SZ) {
    throw CPGBackupCtlFailure("token size exceeds max message queue token size");
  }

  try {

    /*
     * interprocess_exception is not recognized
     * by the pg_backup_ctl++ API, map it to a CPGBackupCtlFailure
     * instead.
     */

    if (!info.command_queue->try_send(command.data(), command.length(), 0)) {
      throw CPGBackupCtlFailure("timeout while sending message into message queue");
    }

  } catch(interprocess_exception &e) {
    throw CPGBackupCtlFailure(e.what());
  }
}

std::string credativ::recv_launcher_cmd(job_info &info, bool &cmd_received) {

  using namespace boost::interprocess;

  std::string command = "";

  /*
   * Having a uninitialized message queue forces
   * a generic failure.
   */
  if (info.command_queue == nullptr) {
    throw CPGBackupCtlFailure("could not send command with uninitialized message queue");
  }

  try {

    /*
     * Recv buffer. Holds up to MSG_QUEUE_MAX_TOKEN_SZ bytes.
     */
    char recvbuffer[MSG_QUEUE_MAX_TOKEN_SZ];
    message_queue::size_type recv_size;
    unsigned int prio;

    memset(recvbuffer, 0, MSG_QUEUE_MAX_TOKEN_SZ);
    if (info.command_queue->try_receive(&recvbuffer, MSG_QUEUE_MAX_TOKEN_SZ, recv_size, prio)) {
      cmd_received = true;
    } else {
      cmd_received = false;
    }

    command = recvbuffer;

  } catch(interprocess_exception &e) {
    throw CPGBackupCtlFailure(e.what());
  }

  return command;
}

/**
 * Launches a fully subprocess suitable to
 * execute background processes. This shouldn't be called
 * multiple times per catalog instance, since they are going
 * to share the same message queue otherwise.
 */
pid_t credativ::launch(job_info& info) {

  daemonize(info);
  return info.pid;

}

/**
 * worker_command() runs from the launcher and
 * forks a new process executing the
 * the command passed to info. If info doesn't
 * hold a proper command handle, nothing will
 * be forked or executed (so this is effectively
 * a no-op then).
 *
 * The command passed to worker_command() should be
 * a command understood by the PGBackupCtlParser. Thus, the
 * caller should be prepared to handle parser errors.
 */
pid_t credativ::worker_command(job_info &info, std::string command) {

  pid_t pid;

  if (info.cmdHandle == nullptr)
    return (pid_t) -1;

  if (!info.background_exec)
    return (pid_t) -1;

  if ((pid = fork()) == (pid_t) 0) {

    /* worker child */
    PGBackupCtlParser parser;
    std::shared_ptr<PGBackupCtlCommand> bgrnd_cmd_handler;
    JobSignalHandler *cmdSignalHandler;
    CatalogTag cmdType;

    cerr << "background job executing command " << command << endl;

    /*
     * Parse command.
     */
    parser.parseLine(command);

    /*
     * Parser has instantiated a command handler
     * iff success.
     */
    bgrnd_cmd_handler = parser.getCommand();

    /*
     * Set signal handlers.
     */
    cmdSignalHandler = dynamic_cast<JobSignalHandler *>(termHandler);
    bgrnd_cmd_handler->assignSigStopHandler(cmdSignalHandler);

    cmdSignalHandler = dynamic_cast<JobSignalHandler *>(emergencyHandler);
    bgrnd_cmd_handler->assignSigIntHandler(cmdSignalHandler);

    /*
     * Now it's time to execute the command.
     */
    cmdType = bgrnd_cmd_handler->execute(info.cmdHandle->getCatalog()->fullname());

    /* Exit, if done */
    exit(0);

  } else if (pid < (pid_t) 0) {

    /*
     * fork() error, this is severe, so report
     * that by throwing a worker exception. This
     * affects the launcher process directly!
     */

  } else {

    /*
     * Launcher process, here's actually
     * nothing to do.
     */

  }
  return pid;
}

/**
 * run_process is supposed to run an executable
 * in a background process via execve() and
 * a bidirectional pipe where the caller
 * can write to and read from.
 */
pid_t credativ::run_process(job_info& info) {

  pid_t pid;

  /*
   * run_process() requires background job preparation.
   */
  if (!info.background_exec)
    throw WorkerFailure("running a process needs background execution");

  /*
   * Prepare pipe if requested. This must be happening
   * in the caller before we fork our background job.
   */
  if (info.use_pipe) {

    /*
     * Output pipe, used to read FROM STDIN in the child
     */
    if (::pipe(info.pipe_in) < 0) {
      std::ostringstream oss;
      oss << "failed to initialize parent output pipe: " << strerror(errno);
      throw WorkerFailure(oss.str());
    }

    if (::pipe(info.pipe_out) < 0) {
      std::ostringstream oss;
      oss << "failed to initialize parent input pipe: " << strerror(errno);
      throw WorkerFailure(oss.str());
    }

  }

  /*
   * Do the fork() ...
   */
  if ((pid = fork()) == (pid_t) 0) {

    /*
     * This is the child process, actually executing the executable.
     */
    string execstr = info.executable.string();

    /*
     * Create array for arguments. reserve two extra
     * pointers, since we store the executable name *and*
     * the finalizing NULL there, too
     */
    char *args[info.execArgs.size() + 2];

    args[0] = new char[execstr.length() + 1];
    memset(args[0], '\0', execstr.length() + 1);
    strncpy(args[0], execstr.c_str(), execstr.length());

    for(unsigned int i = 1; i <= info.execArgs.size(); i++) {
      std::string item = info.execArgs[i - 1];

      args[i] = new char[item.length() + 1];
      memset(args[i], '\0', item.length() + 1);
      strncpy(args[i], item.c_str(), item.length());
    }

    args[info.execArgs.size() + 1] = NULL;

    if (info.use_pipe) {

      /*
       * pipe_in is STDIN, pipe_out is STDOUT
       */
      close(info.pipe_in[1]);
      close(info.pipe_out[0]);

      if (info.pipe_in[0] != STDIN_FILENO) {

        if (dup2(info.pipe_in[0], STDIN_FILENO) != STDIN_FILENO)
          throw WorkerFailure("could not bind pipe to STDIN");

        close(info.pipe_in[0]);
      }

      if (info.pipe_out[1] != STDOUT_FILENO) {

        if (dup2(info.pipe_out[1], STDOUT_FILENO) != STDOUT_FILENO)
          throw WorkerFailure("could not bind pipe to STDOUT");

        close(info.pipe_out[1]);
      }

    }

    if (info.close_std_fd) {

      close(STDIN_FILENO);
      close(STDOUT_FILENO);
      close(STDERR_FILENO);

    }

    if (::execve(execstr.c_str(), args, NULL) < 0) {
      std::ostringstream oss;
      oss << "error executing "
          << info.executable.string()
          << ": "
          << strerror(errno);
      cerr << oss.str() << endl;
      exit(-1);
    }

  } else if (pid < (pid_t) 0) {

    /*
     * Whoops, something went wrong here.
     */
    std::ostringstream oss;
    oss << "error forking process for executable "
        << info.executable << ": "
        << strerror(errno);
    throw WorkerFailure(oss.str());
  } else {

    /* Parent process */

    /*
     * Caller closes it's pipe fd's
     */
    close(info.pipe_in[0]);
    close(info.pipe_out[1]);

  }

  return pid;
}
