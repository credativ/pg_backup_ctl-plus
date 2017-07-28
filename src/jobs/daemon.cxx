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

using namespace credativ;

volatile sig_atomic_t _pgbckctl_shutdown_mode = DAEMON_RUN;
extern int errno;

/*
 * Forwarded declarations.
 */
static pid_t daemonize(job_info &info);

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
   * Install signal handlers.
   */
  signal(SIGTERM, _pgbckctl_sighandler);
  signal(SIGINT, _pgbckctl_sighandler);
  signal(SIGHUP, _pgbckctl_sighandler);
  signal(SIGUSR1, _pgbckctl_sighandler);

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
   * Now launch the background job.
   */
  pid = fork();

  if (pid < 0) {
    std::ostringstream oss;
    oss << "fork error " << strerror(forkerrno);
    throw LauncherFailure(oss.str());
  }

  if (pid > 0) {

    /*
     * This is the launcher process.
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

    /* If in detach mode, exit */
    while (waitpid(pid, NULL, WNOHANG) != pid) {

      cout << "launcher process loop" << endl;

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_NORMAL) {
        std::cout << "shutdown request received" << std::endl;
        kill(SIGTERM, pid);
        break;
      }

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_EMERGENCY) {
        std::cout << "emergency shutdown request received" << std::endl;
        kill(SIGINT, pid);
        break;
      }

      sleep(1);
    }

    if (info.detach)
      exit(0);

  }

  if (pid == 0) {
    /*
     * This is the background process
     */

    /*
     * Enter processing loop
     */
    while(true) {

      sleep(30);
      std::cout << "worker processing loop" << std::endl;

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_NORMAL) {
        std::cout << "shutdown request received" << std::endl;
        break;
      }

      if (_pgbckctl_shutdown_mode == DAEMON_TERM_EMERGENCY) {
        std::cout << "emergency shutdown request received" << std::endl;
        break;
      }

    }

    exit(_pgbckctl_shutdown_mode);
  } /* child execution code */

  info.pid = pid;
  return pid;

}

pid_t launch(job_info& info) {

  daemonize(info);
  return info.pid;
}
