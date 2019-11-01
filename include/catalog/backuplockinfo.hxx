#ifndef __HAVE_BACKUPLOCKINFO_HXX__
#define __HAVE_BACKUPLOCKINFO_HXX__

#include <catalog.hxx>
#include <shm.hxx>

namespace credativ {

  /**
   * Locking exception with a HINT.
   */
  class CLockingFailureHint : public CCatalogIssue {
  public:
    std::string hint = "";
    CLockingFailureHint(const char *errstr) throw() : CCatalogIssue(errstr) {};
    CLockingFailureHint(std::string errstr) throw() : CCatalogIssue(errstr) {};
    CLockingFailureHint(std::string errstr, std::string hint) throw() :
      CCatalogIssue(errstr) { this->hint = hint; }
  };

  typedef enum {

                LOCKED_BY_SHM,
                LOCKED_BY_PIN,
                LOCKED_BY_INVALID_STATE,
                NOT_LOCKED

  } BackupLockInfoType;

  /**
   * Base class for backup lock information.
   *
   * Specific implementations should derive from
   * BackupLockInfo.
   */
  class BackupLockInfo {
  public:

    BackupLockInfo();
    virtual ~BackupLockInfo();

    virtual BackupLockInfoType locked(std::shared_ptr<BaseBackupDescr> backup) = 0;

  };

  /**
   * BackupPinnedValidLockInfo
   *
   * Checks wether a basebackup is either pinned or in an invalid
   * state.
   *
   * Locked means here, that the basebackup is not valid or explicitely
   * pinned.
   */
  class BackupPinnedValidLockInfo : public BackupLockInfo {

  public:

    BackupPinnedValidLockInfo();
    virtual ~BackupPinnedValidLockInfo();

    virtual BackupLockInfoType locked(std::shared_ptr<BaseBackupDescr> backup);

  };

  /**
   * SHMBackupLockInfo
   *
   * Checks of the basebackup is locked by a background
   * worker via shared memory.
   */
  class SHMBackupLockInfo : public BackupLockInfo {
  private:

    /**
     * Internal handle to background worker shared memory
     * area.
     */
    std::shared_ptr<WorkerSHM> worker_shm = nullptr;

  public:

    /**
     * Initialize the lock info with a worker shared
     * memory handle. The handle must be valid and attached
     * to a shared memory segment.
     *
     * Throws a CatalogIssue exception in case conditions
     * aren't met.
     */
    SHMBackupLockInfo(std::shared_ptr<WorkerSHM> shm);
    virtual ~SHMBackupLockInfo();

    /**
     * Check of the specified basebackup is locked
     * by an entry in the worker shared memory area.
     */
    virtual BackupLockInfoType locked(std::shared_ptr<BaseBackupDescr> backup);

  };

  /**
   * BackupLockInfo Aggregator class.
   *
   * Class implementations required to check basebackup
   * interlocking should inherit from this aggregator
   * to perform interlocking checks more easily.
   *
   * See retention.cxx for an example.
   */
  class BackupLockInfoAggregator {
  protected:

    /**
     * Internal list of BackupLockInfo instances to
     * check interlocking.
     */
    std::vector<std::shared_ptr<BackupLockInfo>> locks;

  public:

    BackupLockInfoAggregator();
    virtual ~BackupLockInfoAggregator();

    /**
     * Adds a BackupLockInfo instance to the aggregator.
     */
    virtual void addLockInfo(std::shared_ptr<BackupLockInfo> lockInfo);

    /**
     * Shortcut to check if any locks are referenced
     * by an aggregator instance.
     */
    virtual bool lockInfoPresent();

    /**
     * Aggregator function. Returns the BackupLockInfoType of
     * the lock currently locking the specified basebackup.
     *
     * If no lock is currently present on the specified backup,
     * NOT_LOCKED is returned.
     *
     * This is also the case if the list of aggregated lock info
     * instances is empty. Use count() to determine if any lock info
     * instances are aggregated.
     */
    virtual BackupLockInfoType locked(std::shared_ptr<BaseBackupDescr> backup);

    /**
     * Returns the number of aggregated lock info instances.
     */
    virtual unsigned int count();

  };

}

#endif
