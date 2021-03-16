#ifndef __PGBACKUPCTL_COMMON__
#define __PGBACKUPCTL_COMMON__

#include <cstdint>
#include <boost/date_time.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/filesystem.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/format.hpp>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <iostream>
#include <fstream>
#include <stdexcept>

/* time handling supporting includes */
#include <ctime>
#include <ratio>
#include <chrono>

#include "pg_backup_ctl.hxx"
#include <pgbckctl_exception.hxx>

#define PG_BACKUP_CTL_INFO_FILE "PG_BACKUP_CTL_MAGIC"

#define PGBCKCTL_UINT64CONST(X) static_cast<uint64_t>((X))

/**
 * Plattform specific code
 *
 * NOTE: UINT64CONST define comes from postgres c.h
 */
static
inline uint64_t uint64_to_host_byteorder(uint64_t x) {
  return
    ((x << 56) & PGBCKCTL_UINT64CONST(0xff00000000000000)) |
    ((x << 40) & PGBCKCTL_UINT64CONST(0x00ff000000000000)) |
    ((x << 24) & PGBCKCTL_UINT64CONST(0x0000ff0000000000)) |
    ((x << 8) & PGBCKCTL_UINT64CONST(0x000000ff00000000)) |
    ((x >> 8) & PGBCKCTL_UINT64CONST(0x00000000ff000000)) |
    ((x >> 24) & PGBCKCTL_UINT64CONST(0x0000000000ff0000)) |
    ((x >> 40) & PGBCKCTL_UINT64CONST(0x000000000000ff00)) |
    ((x >> 56) & PGBCKCTL_UINT64CONST(0x00000000000000ff));
}

#ifndef PG_BACKUP_CTL_BIG_ENDIAN

#define SWAP_UINT64(val) uint64_to_host_byteorder((val))

#else
#define SWAP_UINT64(val) (val)

#endif

static inline void
uint64_hton_sendbuf(char *buf, uint64_t val) {

  uint64_t nbo_val = SWAP_UINT64(val);
  memcpy(buf, &nbo_val, sizeof(nbo_val));

}

/*
 * Common objects starts here.
 */
namespace pgbckctl {

  /**
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
   * SyncedFile - A descriptor struct
   * which implements a handle for file_descriptor_sink
   * based file operations.
   */
  struct SyncedBinaryOutFile {

    FILE *fp;
    int fd;
    boost::iostreams::file_descriptor_sink sink;
    boost::iostreams::filtering_ostream *out;

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
     * Some special constants which might be interesting
     * all over the code.
     */
    static const unsigned int invalid_oid = 0;

    /*
     * C'tor
     */
    CPGBackupCtlBase();
    virtual ~CPGBackupCtlBase();

    static std::string getVersionString();
    static int strToInt(std::string in);
    static unsigned int strToUInt(std::string in);
    static std::string intToStr(int in);
    static std::string uintToStr(unsigned int in);
    static boost::posix_time::ptime ISO8601_strTo_ptime(std::string input);
    static std::string ptime_to_str(boost::posix_time::ptime input);
    static void openFile(std::ifstream& file,
                         std::stringstream& out,
                         boost::filesystem::path pathHandle,
                         bool *compressed);
    static std::string makeLine(int width);
    static std::string makeLine(boost::format& formatted);
    static std::string makeHeader(std::string caption,
                                  boost::format& format,
                                  int width);

    /**
     * Format string with color escape sequence. If
     * STDOUT is *not* a terminal, those routines
     * are effectively a no-op.
     */
    static std::string stdout_red(std::string in, bool bold);
    static std::string stdout_green(std::string in, bool bold);

    /*
     * Writes the specified msg into the specified file, while
     * replacing its whole content!
     */
    static void writeFileReplace(std::string filePath,
                                 std::string msg);

    static boost::iostreams::filtering_ostream *
       prepareBinaryOutFile(boost::filesystem::path pathHandle,
                            std::ofstream& outstream);

    void prepareSyncedBinaryOutFile(boost::filesystem::path pathHandle,
                                    SyncedBinaryOutFile& handle);

    void syncAndClose(SyncedBinaryOutFile& handle);

    static void writeChunk(SyncedBinaryOutFile file,
                           char *binaryblock,
                           size_t size);

    /**
     * Creates a string used as a backup label with
     * the current date/time formatted.
     */
    static std::string basebackup_filename();

    /**
     * Returns the current date/time as an ANSI formatted
     * string YYYY-MM-DD H24:MIN:SS. If asFilename is set to
     * true, the string will be formatted to be able to be used
     * as a filename.
     */
    static std::string current_timestamp(bool asFilename = false);

    /**
     * Calculates a duration of high resolution time points in milliseconds.
     */
    static std::chrono::milliseconds calculate_duration_ms(std::chrono::high_resolution_clock::time_point start,
                                                           std::chrono::high_resolution_clock::time_point stop);

    /**
     * Calculates a duration of high resolution time points in microseconds.
     */
    static std::chrono::microseconds calculate_duration_us(std::chrono::high_resolution_clock::time_point start,
                                                           std::chrono::high_resolution_clock::time_point stop);

    /**
     * Returns a high resolution time point
     */
    static std::chrono::high_resolution_clock::time_point current_hires_time_point();

    /**
     * Extracts the number of milliseconds from the given duration.
     */
    static uint64_t duration_get_ms(std::chrono::milliseconds ms);

    /**
     * Extracts the number of microseconds from the given duration.
     */
    static uint64_t duration_get_us(std::chrono::microseconds us);

    /**
     * Returns a milliseconds duration.
     */
    static std::chrono::milliseconds ms_get_duration(long ms);

    /**
     * Replace string occurence "from" to "to" in "str".
     *
     */
    void strReplaceAll(std::string& str, const std::string& from, const std::string& to);

    /**
     * Format given size value into kB, Mb or Gb
     */
    static std::string prettySize(size_t size);

    /**
     * Resolves the given executable name (specified as boost::filesystem::path)
     * object whether it can be found in PATH.
     */
    static bool resolve_file_path(std::string filename);

    /**
     * Controls log level severity via boost::log interface
     */
    static void set_log_severity(std::string severity);
  };

}

#endif
