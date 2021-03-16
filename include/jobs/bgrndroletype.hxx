#ifndef __HAVE_BACKGROUND_ROLE_TYPE_HXX__
#define __HAVE_BACKGROUND_ROLE_TYPE_HXX__

namespace pgbckctl {

  /**
   * Describes the role of a process.
   */
  typedef enum {

                BACKGROUND_LAUNCHER,
                BACKGROUND_WORKER,
                BACKGROUND_WORKER_CHILD,
                NO_BACKGROUND

  } BackgroundJobType;

}

#endif
