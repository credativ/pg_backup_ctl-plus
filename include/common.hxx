#ifndef __PGBACKUPCTL_COMMON__
#include "pg_backup_ctl.hxx"
#define __PGBACKUPCTL_COMMON__

#include <boost/date_time.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/filesystem.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/format.hpp>
#include <iostream>
#include <fstream>
#include <stdexcept>

/*
 * Maximum length of XLOG filename
 *
 * src/include/access/xlog_internal.h
 */
#define MAXXLOGFNAMELEN 25

/*
 * Max length of a backup label
 *
 * src/include/pg_config_manual.h
 */
#define MAXPGPATH 1024

#define PG_BACKUP_CTL_INFO_FILE "PG_BACKUP_CTL_MAGIC"

namespace credativ {

  /*
   * Range of integer values
   */
  class Range {
  private:
    int startval;
    int endval;
  public:
    Range(int start, int end);
    virtual ~Range();

    virtual int start();
    virtual int end();
  };

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

    CPGBackupCtlFailure(std::string errString) throw() : errstr() {
      errstr = errString;
    }

    virtual ~CPGBackupCtlFailure() throw() {}

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
    static int strToInt(std::string in);
    static std::string intToStr(int in);
    static boost::posix_time::ptime ISO8601_strTo_ptime(std::string input);
    static std::string ptime_to_str(boost::posix_time::ptime input);
    static std::string current_timestamp();
    static void openFile(std::ifstream& file,
                         std::stringstream& out,
                         boost::filesystem::path pathHandle,
                         bool *compressed);
    static std::string makeLine(int width);
    static std::string makeLine(boost::format& formatted);
    static std::string makeHeader(std::string caption,
                                  boost::format& format,
                                  int width);

    /*
     * Writes the specified msg into the specified file, while
     * replacing its whole content!
     */
    static void writeFileReplace(std::string filePath,
                                 std::string msg)
      throw (CPGBackupCtlFailure);
  };

}

#endif
