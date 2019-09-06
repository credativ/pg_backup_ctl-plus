#include <boost/log/trivial.hpp>

/* project includes */
#include <reaper.hxx>

/******************************************************************************
 * background_worker_shm_reaper & objects implementation start
 ******************************************************************************/

background_worker_shm_reaper::background_worker_shm_reaper()
  : background_reaper() {

  this->shm = nullptr;

}

background_worker_shm_reaper::~background_worker_shm_reaper() {

  /* nothing special to do here */

}

void background_worker_shm_reaper::set_shm_handle(WorkerSHM *shm) {

  this->shm = shm;

}

void background_worker_shm_reaper::reap() {

  /*
   * Nothing to do if we have no SHM pointer.
   */
  if (this->shm == nullptr)
    return;

  while(!this->dead_pids.empty()) {

    pid_t deadpid = this->dead_pids.top();
    this->dead_pids.pop();

#ifdef __DEBUG__
    BOOST_LOG_TRIVIAL(debug) << "WARN: reaping dead PID "
                             << deadpid
                             << " from shared memory";
#endif

    /*
     * Ugly, but we need to loop through the shared memory
     * area to find the PID we need to drop.
     *
     * NOTE: We do this without interlocking, since
     *       we believe to reset the PID to 0 atomically.
     *
     *       This might by racy, but in the worst case we'll
     *       miss this potential free slot and won't find a
     *       remaining one. In this case the worker will
     *       fail and exit.
     */
    for (unsigned int i = 0 ; i < this->shm->getMaxWorkers(); i++) {

      shm_worker_area *ptr = (shm_worker_area *) (this->shm->shm_mem_ptr + i);

      if (ptr != NULL && ptr->pid == deadpid) {

        ptr->pid = 0;

      }

    }
  }

}
