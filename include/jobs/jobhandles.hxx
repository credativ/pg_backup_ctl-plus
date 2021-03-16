#ifndef __HAVE_JOBHANDLES_HXX__
#define __HAVE_JOBHANDLES_HXX__

#include <boost/interprocess/ipc/message_queue.hpp>
//#include <BackupCatalog.hxx>
#include <commands.hxx>

namespace pgbckctl {

  class BaseCatalogCommand;

  /**
   * Status of background launcher
   */
  typedef enum {

    LAUNCHER_STARTUP,
    LAUNCHER_RUN,
    LAUNCHER_DIE,
    LAUNCHER_SHUTDOWN

  } LauncherStatus;

  /**
   * A descriptor area for background jobs.
   */
  typedef struct job_info {
  public:

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
     *
     * Note that it usually doesn't make sense to
     * specify use_pipe and close_std_fd at the same time.
     * use_pipe binds STDIN, STDOUT to the read and write
     * ends of a pipe.
     */
    bool close_std_fd = false;

    /**
     * Catalog/Command descriptor. The descriptor usually is
     * initialized by the caller and passed to the worker
     * process.
     */
    std::shared_ptr<BaseCatalogCommand> cmdHandle;

    /**
     * Pipe handler, only initialized if use_pipe is also set to true.
     */
    bool use_pipe = false;

    /**
     * Pipe handles for background executables.
     */
    int pipe_in[2];
    int pipe_out[2];

    /**
     * FILE stream handle for pipelined background
     * command via popen().
     */
    FILE *fpipe_handle = NULL;

    /**
     * Argument for popen() to open the
     * pipe in either read or write-only mode. Allowed
     * valus are (according to man(3) popen): "w" for write
     * and "r" for read only. Default is "w".
     */
    std::string po_mode = "w";

    /**
     * Tells the background job to execute either the specified
     * executable with execArgs arguments or the specified cmdHandle.
     * This uses Boost::Process and replaces the child process context
     * with the executable specified. If the command exits, the child
     * will be terminated accordingly.
     */
    bool background_exec = false;

    /**
     * executable as path object.
     */
    boost::filesystem::path executable;

    /**
     * Argument passed down to the specified background executable. Please
     * note that the arguments are exactly applied in the same order as
     * added to the vector.
     */
    std::vector<std::string> execArgs;

    /*
     * Launcher Message Queue Handler.
     */
    boost::interprocess::message_queue *command_queue = nullptr;

  } job_info;


}

#endif
