#ifndef __HAVE_JOBHANDLES_HXX__
#define __HAVE_JOBHANDLES_HXX__

#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/managed_xsi_shared_memory.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
//#include <BackupCatalog.hxx>
#include <commands.hxx>

namespace credativ {

  class BaseCatalogCommand;
  class background_reaper;
  class background_worker_shm_reaper;

  /**
   * Exception class for shared memory errors.
   */
  class SHMFailure : public credativ::CPGBackupCtlFailure {
  public:
    SHMFailure(const char *errstr) throw() : CPGBackupCtlFailure(errstr) {};
    SHMFailure(std::string errstr) throw() : CPGBackupCtlFailure(errstr) {};
  };

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
   * Describes the role of a process.
   */
  typedef enum {

    BACKGROUND_LAUNCHER,
    BACKGROUND_WORKER,
    NO_BACKGROUND

  } BackgroundJobType;

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

  /**
   * Shared memory structure for launcher control data.
   */
  typedef struct shm_launcher_area {
    pid_t pid;
  } shm_launcher_area;

  /**
   * Shared memory structure for worker control data.
   *
   * A worker shared memory segment usually holds up to
   * WorkerSHM::max_workers # of this structures.
   */
  typedef struct shm_worker_area {

    pid_t pid = -1; /* -1 means unused slot */
    CatalogTag cmdType;
    boost::posix_time::ptime started;

  } shm_worker_area;

  /**
   * Base class for process specific shared memory segments.
   */
  class ProcessSHM {
  protected:

    /**
     * SHM key to identify internal shared memory segment.
     */
    key_t shm_key = -1;

    /**
     * The name of the shared memory segment.
     */
    std::string shm_ident = "";

    /**
     * Mutex to access internal shared memory objects.
     */
    boost::interprocess::interprocess_mutex *mtx = nullptr;

    /**
     * Internal XSI shared memory handle.
     */
    boost::interprocess::managed_xsi_shared_memory *shm = nullptr;

  public:
    ProcessSHM();
    virtual ~ProcessSHM();

    /**
     * Attach or create a shared memory segment.
     * Implementation specific.
     */
    virtual bool attach(std::string catalog, bool attach_only) = 0;
    virtual void detach() = 0;

    /**
     * Returns the number of attached processes
     * to the launcher shared memory segment.
     */
    virtual shmatt_t getNumberOfAttached();

    /**
     * Returns the number of attached processes for
     * the specified shmid. Static version of getNumberOfAttached(),
     * that can be used on any shmid.
     */
    static shmatt_t getNumberOfAttached(int check_shmid);

    /**
     * Returns the shmid, if initialized. Otherwise -1.
     */
    virtual int get_shmid();

    /**
     * Returns the shared memory key used to attach
     * the internal shared memory segment. If not initialized,
     * -1 is returned.
     */
    virtual key_t get_shmkey();

    /**
     * Returns the identifier attached to
     * the internal shared memory segment pointer.
     */
    virtual std::string getIdent();

    /**
     * Returns the requested shared memory size. Abstract,
     * since intepretation differs between shared memory segment types.
     */
    virtual size_t getSize() = 0;

  };

  /*
   * A wrapper for shared memory access.
   *
   * LauncherSHM encapsulates routines to access a shared
   * memory area used by a launcher process.
   *
   * IMPORTANT:
   *
   *   The implementation expects only *ONE*!! launcher
   *   per catalog. It must always be ensured, that in any
   *   case no other launcher will attach to this shared memory
   *   area.
   */
  class LauncherSHM : public ProcessSHM {
  protected:

    /**
     * Size of launcher shared memory. Currently 4kB.
     */
    static const size_t size = 4096;

    /**
     * Mapped memory region from shared memory.
     */
    shm_launcher_area *shm_mem_ptr = nullptr;

  public:
    LauncherSHM();
    virtual ~LauncherSHM();

    /**
     * Creates and attaches a launcher shared memory XSI
     * segment. If the shared memory exists, simply attach to it
     *
     * Catalog is the name of the catalog identifier the launcher
     * belongs to.
     *
     * If attach_only is specified, attach() just tries to
     * open an existing launcher shared memory segment. If it does
     * exist, false will be returned.
     *
     * If successful, attach() returns true.
     */
    virtual bool attach(std::string catalog, bool attach_only);

    /**
     * Set specified launcher pid (stored into our shared memory
     * segment). Throws a LauncherSHMFailure exception in case
     * we aren't already attached to a shared memory segment.
     */
    virtual pid_t setPID(pid_t pid);

    /**
     * Detaches from the shared memory segment.
     */
    virtual void detach();

    /**
     * Returns the size of the shared memory segment. In this
     * case the size if always 4kB.
     */
    virtual size_t getSize();
  };

  /**
   * A shared memory area for background workers.
   *
   * A worker is always registered in the shared memory
   * area here, which is supposed to be created by a catalog
   * launcher process.
   */
  class WorkerSHM : public ProcessSHM {
  protected:

    /**
     * Max workers allowed to attach. The current
     * default number is configured in pg_backup_ctl.hxx.in
     */
    unsigned int max_workers = PGBCKCTL_MAX_WORKERS;

    /*
     * Current size of shared memory area, default
     * is 0, indicating an uninitialized segment.
     */
    size_t size = 0;

    /**
     * Calculates the size of the shared memory area. The formula
     * currently used is:
     *
     * (sizeof(shm_worker_area)) * max_workers
     *    + sizeof(boost::interprocess::interprocess_mutex)
     *    + 4
     */
    size_t calculateSHMsize();

    /**
     * Workers shared memory area.
     */
    shm_worker_area *shm_mem_ptr = nullptr;

    /**
     * Upper index for shm_mem_ptr. This is initialized
     * after allocating the shared memory area during
     * attach().
     *
     * Usually this is just max_workers - 1.
     */
    unsigned int upper = 0;

    /**
     * Number of currently allocated worker slots.
     *
     * The number of allocated slots are controlled by
     * allocate() and free().
     */
    unsigned int allocated = 0;

  public:

    friend class background_worker_shm_reaper;

    WorkerSHM();
    virtual ~WorkerSHM();

    /**
     * Maximum worker processes allowed to attach
     * to this shared memory segment.
     */
    unsigned int getMaxWorkers();

    /**
     * Sets the maximum workers allowed to attach. Throws
     * in case the shared memory segment is already in use (attach()
     * was called before).
     */
    virtual void setMaxWorkers(unsigned int max_workers);

    /**
     * Attach to a worker shared memory segment
     */
    virtual bool attach(std::string catalog, bool attach_only);

    /**
     * Detach from a worker shared memory segment. Doesn't
     * deallocate the shared memory segment physically from the system.
     */
    virtual void detach();

    /**
     * Returns the requested shared memory segment size.
     */
    virtual size_t getSize();

    /**
     * Writes the specified items into the shared memory
     * slot on the specified index. Caller should
     * have locked the shared memory operation to protect
     * against concurrent changes.
     *
     * Throws in case we aren't attached.
     */
    virtual void write(unsigned int slot_index,
                       shm_worker_area &item);

    /**
     * Allocates a new worker slot and writes
     * the properties of item into the new slot.
     *
     * Throws in case we aren't attached.
     *
     * allocate() is different from write(). The latter
     * doesn't try to get a free slot index and doesn't increase
     * the allocated counter internally. Though write doesn't check
     * if the slot was allocated before, too.
     */
    virtual unsigned int allocate(shm_worker_area &item);

    /**
     * Returns a copy of the specified worker area at
     * the specified shared memory slot. Caller should have locked
     * the shared memory against concurrent changes.
     *
     * Throws in case we aren't attached.
     */
    virtual shm_worker_area read(unsigned int slot_index);

    /**
     * Resets the specified worker slot to represent a free
     * slot. Also sets the last index being freed.
     */
    virtual void free(unsigned int slot_index);

    /**
     * Resets all worker slots to be empty.
     *
     * Throws in case we aren't attached.
     */
    virtual void reset();

    /**
     * Tells wether the specified slot index
     * is empty.
     */
    virtual bool isEmpty(unsigned int slot_index);

    /**
     * Returns a slot index usable by a new
     * worker.
     */
    virtual unsigned int getFreeIndex();

    /**
     * Locks the shared memory against concurrent changes.
     * Throws in case the shared memory area is not attached.
     */
    virtual void lock();

    /**
     * Unlocks the shared memory to allow concurrent changes.
     * Throws in case the shared memory area is not attached.
     */
    virtual void unlock();
  };

  class background_reaper {
  public:
    std::stack<pid_t> dead_pids;
    virtual void reap() = 0;
  };

  class background_worker_shm_reaper : public background_reaper {
  protected:
    WorkerSHM *shm;
  public:
    background_worker_shm_reaper();
    virtual ~background_worker_shm_reaper();

    virtual void set_shm_handle(WorkerSHM *shm);
    virtual void reap();
  };

}

#endif
