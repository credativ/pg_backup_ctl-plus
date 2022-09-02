#ifndef HAVE_FS_COPY
#define HAVE_FS_COPY

#include <pg_backup_ctl.hxx>
#include "fs-archive.hxx"

/* warning seems very aggressive here, but we want to know
 * during compilation which module is active here */
#ifdef PG_BACKUP_CTL_HAS_LIBURING
#warning using experimental io_uring library support
#endif

#include <stack>
#include <mutex>
#include <thread>
#include <condition_variable>

#ifdef PG_BACKUP_CTL_HAS_LIBURING
#include <io_uring_instance.hxx>
#endif

namespace pgbckctl {

  /* Forwarded declarations */
  class BackupCopyManager;

  class TargetDirectory : public RootDirectory {
  private:

  public:
    TargetDirectory(path directory);
    virtual ~TargetDirectory();
  };

  class BaseCopyManager {
  protected:

    /* Forwarded declaration */
    class _copyOperations;

    /**
     * Maximum copy instances to use.
     */
    unsigned short max_copy_instances = 1;

    class _copyItem {
    private:

      /** Exit forced */
      bool exit_forced = false;

    protected:

      /** I/O thread */
      std::shared_ptr<std::thread> io_thread = nullptr;

      /** internal slot reference for copy operations list */
      int slot = -1;

      /** I/O Thread legwork method */
      virtual void work(BaseCopyManager::_copyOperations &ops_handler,
                        path inputFileName,
                        path outFileName) const = 0;

    public:

      /**
       * default constructor, initializes read/write queue, but not
       * input/output file handles. the caller needs to setup the uring queues
       * manually via IOUringInstance::setup() ...
       */
      explicit _copyItem(int slot);

      /** Destructor, stops copy operation and frees all resources */
      virtual ~_copyItem();

      /**
       * Request abort of copy operation.
       */
      virtual void exitForced();

      /**
       * Does the legwork of copying the queued file
       */
      virtual void go(BaseCopyManager::_copyOperations &ops_handler,
                      path inputFileName,
                      path outputFileName) = 0;

    };

    /**
     * Maintains a thread pool of running copy operations.
     */
    class _copyOperations {
    public:

      /** Currently running operations */
      std::array<std::shared_ptr<_copyItem>, MAX_PARALLEL_COPY_INSTANCES> ops;

      /**
       * Stack of available ops slots.
       *
       * The stack is only once filled in by operations itself, when having entered
       * the processing loop (see @link IOUringCopyManager::start()) we just atomically check
       * which slots are available, protected by the active_ops_mutex.
       */
      std::stack<unsigned int> ops_free;

      /**
       * The condition_variable notify_cv is responsible to notify operations
       * that something is to do and otherwise having to wait.
       */
      std::condition_variable notify_cv;
      bool needs_work = false;

      /** Protects concurrent access to operations */
      std::mutex active_ops_mutex;

      /*
       * Flag set if no more files left to process. We set this to true as soon as there
       * are no files left to process. This allows the BackupCopyManager to call wait()
       * to finalize any remaining copy operations safely, since no concurrent creation of new
       * copy items happen anymore. See the start() implementations of the various copy
       * managers.
       */
      bool finalize = false;

      /** Abort operations requested */
      bool exit = false;
    };

    _copyOperations ops;

    std::shared_ptr<BackupDirectory> source = nullptr;
    std::shared_ptr<TargetDirectory> target = nullptr;

    /** A SIGTERM signal handler */
    JobSignalHandler *stopHandler = nullptr;

    /** A SIGINT signal handler */
    JobSignalHandler *intHandler  = nullptr;

  public:

    BaseCopyManager(std::shared_ptr<BackupDirectory> in,
                    std::shared_ptr<TargetDirectory> out);
    virtual ~BaseCopyManager();

    /**
     * Abstract start() method to start a copy operation.
     */
    virtual void start() = 0;

    /**
     * Abstract stop() method to stop a copy operation.
     */
    virtual void stop() = 0;

    /**
     * Waits for copy operation to finish.
     */
    virtual void wait() = 0;

    /**
     * Assign source directory.
     */
    void setSourceDirectory(std::shared_ptr<BackupDirectory> in);

    /**
     * Assign target directory.
     */
    void setTargetDirectory(std::shared_ptr<TargetDirectory> out);

    /** Factory method */
    static std::shared_ptr<BackupCopyManager> get(std::shared_ptr<StreamingBaseBackupDirectory> in,
                                                  std::shared_ptr<TargetDirectory> out);

    /**
     * Assign a stop signal handler to signal aborting a copy operation.
     */
     void assignSigStopHandler(JobSignalHandler *handler);

     /**
      * Assign an interruption signal handler.
      */
      void assignSigIntHandler(JobSignalHandler *handler);

      /** Returns the number of configured parallel copy threads */
      virtual unsigned short getNumberOfCopyInstances();

      /** Sets the number of parallel workers */
      virtual void setNumberOfCopyInstances(unsigned short instances);

  };

#ifdef PG_BACKUP_CTL_HAS_LIBURING

  class IOUringCopyManager : public BaseCopyManager {
  private:

    /**
     * Creates a new _copyItem for the specified directory entry.
     * slot is the slot ID within the operations manager.
     *
     * @param dentry: directory entry to work on (regular file)
     * @param slot: the ID of the slot within the operations manager.
     *
     * NOTE: it is dangerous to call this method outside a critical section. The
     *       caller has to make sure a critical section was established before entering.
     */
    virtual void makeCopyItem(const directory_entry &dentry,
                              const unsigned int slot);

  protected:

    /**
     * io_uring specific implementation of internal copy management
     */
    class _iouring_copyItem final : public BaseCopyManager::_copyItem {
    protected:

      /** I/O Thread legwork method */
      virtual void work(BaseCopyManager::_copyOperations &ops_handler,
                        path inputFileName,
                        path outFileName) const;

    public:

      _iouring_copyItem(unsigned int slot) noexcept;
      ~_iouring_copyItem() final;

      /**
       * Does the legwork of copying the queued file
       */
      virtual void go(BaseCopyManager::_copyOperations &ops_handler,
                      path inputFileName,
                      path outputFileName);

    };

  public:

    IOUringCopyManager(std::shared_ptr<BackupDirectory> in,
                       std::shared_ptr<TargetDirectory> out);

    IOUringCopyManager(std::shared_ptr<BackupDirectory> in,
                       std::shared_ptr<TargetDirectory> out,
                       unsigned short instances);

    virtual ~IOUringCopyManager() {}

    virtual void start();
    virtual void stop();
    virtual void wait();

  };

  class CopyManager : public IOUringCopyManager {
  public:
    CopyManager(std::shared_ptr<BackupDirectory> in,
                std::shared_ptr<TargetDirectory> out)
                : IOUringCopyManager(in, out) {}
    virtual ~CopyManager() {}
  };

#else

  class LegacyCopyManager : public BaseCopyManager {
  private:

    /**
     * Creates a new _copyItem for the specified directory entry.
     * slot is the slot ID within the operations manager.
     *
     * @param dentry: directory entry to work on (regular file)
     * @param slot: the ID of the slot within the operations manager.
     *
     * NOTE: it is dangerous to call this method outside a critical section. The
     *       caller has to make sure a critical section was established before entering.
     */
    virtual void makeCopyItem(const directory_entry &de,
                              const unsigned int slot);

  protected:

    class _legacy_copyItem final : public BaseCopyManager::_copyItem {
    protected:

      /** I/O Thread legwork method */
      virtual void work(BaseCopyManager::_copyOperations &ops_handler,
                        path inputFileName,
                        path outFileName) const;

    public:

      _legacy_copyItem(unsigned int slot) noexcept;
      ~_legacy_copyItem();

      /**
       * Does the legwork of copying the queued file
       */
      virtual void go(BaseCopyManager::_copyOperations &ops_handler,
                      path inputFileName,
                      path outputFileName);

    };

  public:
    LegacyCopyManager(std::shared_ptr<BackupDirectory> in,
                      std::shared_ptr<TargetDirectory> out);
    virtual void start();
    virtual void stop();
    virtual void wait();
  };

  class CopyManager : public LegacyCopyManager {
  public:
    CopyManager(std::shared_ptr<BackupDirectory> in,
                std::shared_ptr<TargetDirectory> out) : LegacyCopyManager(in, out) {}
    virtual ~CopyManager() {}
    };

#endif

  /**
   * Copy Manager instance. Encapsulates all the logic to copy files/directories locally.
   */
  class BackupCopyManager : public CopyManager {
  public:
    BackupCopyManager(std::shared_ptr<BackupDirectory> in,
                      std::shared_ptr<TargetDirectory> out) : CopyManager(in, out) {}
    virtual ~BackupCopyManager() {}
  };

}

#endif
