#ifndef __HAVE_PGBCKCTL_SIGNALHANDLER__
#define __HAVE_PGBCKCTL_SIGNALHANDLER__

namespace credativ {

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
}

#endif
