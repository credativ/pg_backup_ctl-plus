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
}

#endif
