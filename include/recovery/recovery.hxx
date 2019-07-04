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

    /*
     * Abstract initialization method.
     *
     * Implements specific methods to initialize
     * recovery methods.
     */
    virtual void init() = 0;
  };

  /**
   * Recovers a tar basebackup from the archive.
   */
  class TarRecovery : public Recovery {
  public:

    TarRecovery();
    virtual ~TarRecovery();

    /*
     * Initializes tar recovery procedure. Must be called
     * before attempting start().
     */
    virtual void init();

  };

}

#endif
