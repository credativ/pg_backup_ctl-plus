#ifndef __HAVE_PGBCKCTL_EXCEPTION_HXX__
#define __HAVE_PGBCKCTL_EXCEPTION_HXX__

#include <string>

namespace credativ {

  /**
   * Base exception class
   */
  class CPGBackupCtlFailure : public std::exception {

  protected:
    std::string errstr;

  public:

    CPGBackupCtlFailure(const char *errString) throw() : errstr() {
      errstr = errString;
    }

    CPGBackupCtlFailure(std::string errString) throw() : errstr() {
      errstr = errString;
    }

    virtual ~CPGBackupCtlFailure() throw() {}

    const char *what() const throw() {
      return errstr.c_str();
    }

  };

}

#endif
