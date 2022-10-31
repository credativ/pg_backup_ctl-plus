#ifndef __HAVE_PGBCKCTL_EXCEPTION_HXX__
#define __HAVE_PGBCKCTL_EXCEPTION_HXX__

#include <string>

namespace pgbckctl {

  /**
   * Base exception class
   */
  class CPGBackupCtlFailure : public std::exception {

  protected:
    std::string errstr;

  public:

    explicit CPGBackupCtlFailure(const char *errString) noexcept(false) : errstr() {
      errstr = errString;
    }

    explicit CPGBackupCtlFailure(std::string errString) noexcept(false) : errstr() {
      errstr = errString;
    }

    virtual ~CPGBackupCtlFailure()  {}

    const char *what() const noexcept(true) override {
      return errstr.c_str();
    }

  };

}

#endif
