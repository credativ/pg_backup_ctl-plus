#ifndef HAVE_FS_COPY
#define HAVE_FS_COPY

#include <pg_backup_ctl.hxx>
#include "fs-archive.hxx"

#ifdef PG_BACKUP_CTL_HAS_LIBURING
#include <io_uring_instance.hxx>
#include <stack>
#include <mutex>
#include <thread>
#include <condition_variable>
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
     * Assign source directory.
     */
    virtual void setSourceDirectory(std::shared_ptr<BackupDirectory> in);

    /**
     * Assign target directory.
     */
    virtual void setTargetDirectory(std::shared_ptr<TargetDirectory> out);

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
  };

#ifdef PG_BACKUP_CTL_HAS_LIBURING

  class IOUringCopyManager : public BaseCopyManager {
  private:

    /* Forwarded declaration */
    class _copyOperations;

    class _copyItem {
    private:

      /** internal slot reference for copy operations list */
      int slot = -1;

      /** Exit forced */
      bool exit_forced = false;

      /** input file to read */
      std::shared_ptr<ArchiveFile> inFile;

      /** output file to write contents of input file to */
      std::shared_ptr<ArchiveFile> outFile;

      /** I/O thread */
      std::shared_ptr<std::thread> io_thread = nullptr;

      /** io_uring instance belonging to this copy item, read queue */
      std::shared_ptr<IOUringInstance> read_ring = nullptr;

      /** io_uring instance belonging to this copy item, write queue */
      std::shared_ptr<IOUringInstance> write_ring = nullptr;

      /** I/O Thread legwork method */
      void work();

    public:

      /**
       * default constructor, initializes read/write queue, but not
       * input/output file handles. the caller needs to setup the uring queues
       * manually via IOUringInstance::setup() ...
       */
      _copyItem(int slot);

      /**
       * Initializes a fully usable copy item instance.
       */
      _copyItem(std::shared_ptr<ArchiveFile> in,
                std::shared_ptr<ArchiveFile> out,
                int slot);

      /** Destructor, stops copy operation and frees all resources */
      virtual ~_copyItem();

      /**
       * Request abort of copy operation.
       */
      void exitForced();

      /**
       * Does the legwork of copying the queued file
       */
      void go(IOUringCopyManager::_copyOperations &ops_handler);

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
      std::stack<int> ops_free;

      /**
       * The condition_variable notify is responsible to notify operations
       * that something is to do and otherwise having to wait.
       */
      std::condition_variable notify_cv;
      bool needs_work = false;

      /** Protects concurrent access to operations */
      std::mutex active_ops_mutex;

      /** Abort operations requested */
      bool exit = false;
    };

    _copyOperations ops;

    /**
     * Maximum copy instances to use.
     */
    unsigned short max_copy_instances = 1;

    /**
     * Perform copy operation. This is the internal entry point to start
     * copying a directory structure into the instance target directory.
     *
     * copy() essentially prepares the io_uring infrastructure and starts two
     * threads, one for reading the source file and write to the uring, the other
     * to consume the data and write it out to the target. These pair of copy threads
     * can be started up to the maximum number of max_copy_instances which effectively
     * sums up to #total_threads = (max_copy_instances * 2).
     */
     virtual void performCopy();

  public:

    IOUringCopyManager(std::shared_ptr<BackupDirectory> in,
                       std::shared_ptr<TargetDirectory> out);

    IOUringCopyManager(std::shared_ptr<BackupDirectory> in,
                       std::shared_ptr<TargetDirectory> out,
                       unsigned short instances);

    virtual ~IOUringCopyManager() {}

    virtual void start();
    virtual void stop();

    virtual void setNumberOfCopyInstances(unsigned short instances);
    virtual unsigned short getNumberOfCopyInstances();

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
  public:
    LegacyCopyManager(std::shared_ptr<BackupDirectory> in,
                      std::shared_ptr<TargetDirectory> out);
    void start() {}
    void stop() {}
  };


  class CopyManager : public LegacyCopyManager {
  public:
    CopyManager(std::shared_ptr<BackupDirectory> in,
                std::shared_ptr<TargetDirectory> out) : LegacyCopyManager(in, out) {}
    virtual ~CopyManager() {}
    };

#endif

  class BackupCopyManager : public CopyManager {
  public:
    BackupCopyManager(std::shared_ptr<BackupDirectory> in,
                      std::shared_ptr<TargetDirectory> out) : CopyManager(in, out) {}
    virtual ~BackupCopyManager() {}
  };

}

#endif
