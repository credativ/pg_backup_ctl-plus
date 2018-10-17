#ifndef __RECOVERY_HXX__
#define __RECOVERY_HXX__

#include <common.hxx>

namespace credativ {

  /**
   * Base class for recovery/restore implementations.
   *
   * A restore class should derive from this base class.
   */
  class Recovery : public CPGBackupCtlBase {
  public:

    Recovery();
    virtual ~Recovery();

  };
}

#endif
