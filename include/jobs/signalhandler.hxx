#ifndef __HAVE_PGBCKCTL_SIGNALHANDLER__
#define __HAVE_PGBCKCTL_SIGNALHANDLER__

#include <signal.h>

namespace pgbckctl {

  class JobSignalHandler {
  public:
    JobSignalHandler();
    virtual ~JobSignalHandler();

    virtual bool check() = 0;
  };

  class ConditionalSignalHandler : public JobSignalHandler {
  protected:
    volatile bool *ref_bvalue = NULL;
  public:
    ConditionalSignalHandler();
    ConditionalSignalHandler(volatile bool *bvalue);
    virtual ~ConditionalSignalHandler();

    virtual bool ref(volatile bool *bvalue);
    virtual bool check();
  };

  class AtomicSignalHandler : public JobSignalHandler {
  protected:
    int ref_value = -1;
    volatile sig_atomic_t *ref_var = nullptr;
  public:
    AtomicSignalHandler();
    AtomicSignalHandler(volatile sig_atomic_t *ref_var, int ref_value);
    virtual ~AtomicSignalHandler();

    virtual int ref(volatile sig_atomic_t *ref_var, int ref_value);
    virtual bool check();
  };

  /*
   * A generic class suitable to inherit signal checker functionality
   * into class implementations.
   */
  class StopSignalChecker {
  protected:
    JobSignalHandler *stopHandler = nullptr;
  public:

    /*
     * Returns true if the assign stopHandler is set accordingly.
     * If no stophandler was assigned, stopHandlerWantsExit() always
     * returns false.
     */
    virtual bool stopHandlerWantsExit();

    /**
     * Assign a stop signal handler. This handler is used to check whether we
     * received an asynchronous stop signal somehow.
     */
    virtual void assignStopHandler(JobSignalHandler *handler);

    /**
     * Returns a pointer to the signal handler assigned to a checker instance.
     */
    virtual JobSignalHandler * getSignalHandler();

  };

}

#endif
