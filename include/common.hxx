#ifndef __PGBACKUPCTL_COMMON__
#include "pg_backup_ctl.hxx"
#define __PGBACKUPCTL_COMMON__

#include <stdexcept>

namespace credativ {

  /*
   * Base exception class
   */
  class CPGBackupCtlFailure : public std::exception {

  protected:
    std::string errstr;

  public:

    CPGBackupCtlFailure(const char *errString) throw() : errstr() {
      errstr = errString;
    }

    virtual ~CPGBackupCtlFailure() _NOEXCEPT {};

    const char *what() const throw() {
      return errstr.c_str();
    }

  };

  /*
   * Base class for all descendants in the
   * pg_backup_ctl++ class hierarchy.
   */
  class CPGBackupCtlBase {

  public:
    static const int version_major_num = PG_BACKUP_CTL_MAJOR;
    static const int version_minor_num = PG_BACKUP_CTL_MINOR;

    /*
     * C'tor
     */
    CPGBackupCtlBase();
    virtual ~CPGBackupCtlBase();

    static std::string getVersionString();
  };

}

#endif
