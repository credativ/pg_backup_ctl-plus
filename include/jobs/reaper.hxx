#ifndef __HAVE_REAPER_HXX__
#define __HAVE_REAPER_HXX__

#include <BackupCatalog.hxx>
#include <shm.hxx>

namespace pgbckctl {

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
