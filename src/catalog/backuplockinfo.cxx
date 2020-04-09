#include <pgbckctl_exception.hxx>
#include <daemon.hxx>
#include <backuplockinfo.hxx>

using namespace credativ;

/* ****************************************************************************
 * Class BackupLockInfoAggregator
 * ****************************************************************************/

BackupLockInfoAggregator::BackupLockInfoAggregator() {}

BackupLockInfoAggregator::~BackupLockInfoAggregator() {}

bool BackupLockInfoAggregator::lockInfoPresent() {

  return (locks.size() > 0);

}

void BackupLockInfoAggregator::addLockInfo(std::shared_ptr<BackupLockInfo> lockInfo) {

  if (lockInfo == nullptr) {

    throw CLockingFailureHint("cannot register undefined lock info type");

  } else {

    locks.push_back(lockInfo);

  }

}

BackupLockInfoType BackupLockInfoAggregator::locked(std::shared_ptr<BaseBackupDescr> backup) {

  BackupLockInfoType locktype = NOT_LOCKED;

  /* Shortcut here, if no lock info present, make immediate exit */
  if (count() == 0)
    return locktype;

  for(auto const &lockInfo : locks) {

    if ((locktype = lockInfo->locked(backup)) != NOT_LOCKED) {

      break;

    }

  }

  /* If we reach here, no lock found */
  return locktype;

}

unsigned int BackupLockInfoAggregator::count() {

  return locks.size();

}

/* ****************************************************************************
 * Base class BackupLockInfo
 * ****************************************************************************/

BackupLockInfo::BackupLockInfo() {}

BackupLockInfo::~BackupLockInfo() {}

/* ****************************************************************************
 * Basebackup pinned or valid lock info implementation.
 * ****************************************************************************/

BackupPinnedValidLockInfo::BackupPinnedValidLockInfo() {}

BackupPinnedValidLockInfo::~BackupPinnedValidLockInfo() {}

BackupLockInfoType BackupPinnedValidLockInfo::locked(std::shared_ptr<BaseBackupDescr> backup) {

  BackupLockInfoType result = NOT_LOCKED;

  if (backup->pinned)
    result = LOCKED_BY_PIN;

  if (backup->status != BaseBackupDescr::BASEBACKUP_STATUS_READY)
    result = LOCKED_BY_INVALID_STATE;

  return result;

}

/* ****************************************************************************
 * Shared memory lock info.
 * ****************************************************************************/

SHMBackupLockInfo::SHMBackupLockInfo(std::shared_ptr<WorkerSHM> shm) {

  /* shared memory handle must be valid */
  if (shm == nullptr) {
    throw CCatalogIssue("cannot assign uninitialized shared memory segment to lock info manager");
  }

  if (shm->get_shmid() == -1) {
    throw CCatalogIssue("attempt to assign unattached worker shared memory to lock info manager");
  }

  worker_shm = shm;

}

SHMBackupLockInfo::~SHMBackupLockInfo() {

  worker_shm = nullptr;

}

BackupLockInfoType SHMBackupLockInfo::locked(std::shared_ptr<BaseBackupDescr> backup) {

  BackupLockInfoType result = NOT_LOCKED;

  /* Backup descriptor must be valid */

  if (backup == nullptr) {
    throw CCatalogIssue("attempt to assign uninitialized basebackup handle to backup lock info");
  }

  if (backup->id < 0) {
    throw CCatalogIssue("attempt to assign invalid basebackup id to backup lock info");
  }

  /*
   * Get a lock on the shared memory segment, check if the
   * requested basebackup ID is in use.
   *
   * We need to loop through all worker slots and have a look
   * into possible child slots. This is rather expensive, but we
   * expect the list not to be very long.
   */
  WORKER_SHM_CRITICAL_SECTION_START_P(worker_shm);

  for (unsigned int i = 0; i < worker_shm->getMaxWorkers(); i++) {

    shm_worker_area worker_info = worker_shm->read(i);

    /**
     * Looks like there is a child with an attached basebackup,
     * let's have a look whether it's ours.
     */
    if (worker_info.basebackup_in_use) {

      for(unsigned int j = 0; j < MAX_WORKER_CHILDS; j++) {

        sub_worker_info child_info = worker_shm->read(i, j);

        /* Valid PID registered on this child slot? */
        if (child_info.pid > 0) {

          if (backup->id == child_info.backup_id) {

            result = LOCKED_BY_SHM;
            break;

          }
        }

      } /* inner child loop */

    }

    /* If we've already found us, exit */
    if (result != NOT_LOCKED)
      break;

  } /* outer worker slot loop */

  WORKER_SHM_CRITICAL_SECTION_END;

  return result;

}
