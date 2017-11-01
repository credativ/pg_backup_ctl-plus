#ifndef __PGBACKUPCTL_COMMON__
#include "pg_backup_ctl.hxx"
#define __PGBACKUPCTL_COMMON__

/* PostgreSQL API includes */
#include <postgres_fe.h>
#include <access/xlog_internal.h>

/* Required for MAXPGPATH */
#include <pg_config_manual.h>

/* Required for MAXXLOGFNAMELEN */
#include <access/xlog_internal.h>

#include <boost/date_time.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/filesystem.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/format.hpp>
#include <iostream>
#include <fstream>
#include <stdexcept>

/* time handling supporting includes */
#include <ctime>
#include <ratio>
#include <chrono>

#define PG_BACKUP_CTL_INFO_FILE "PG_BACKUP_CTL_MAGIC"

namespace credativ {

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

  /**
   * A very lightweight in-memory buffer
   * class
   */
  class MemoryBuffer {
  protected:
    /**
     * Internal buffer array.
     */
    char *memory_buffer = NULL;

    /**
     * Internal memory buffer size.
     */
    size_t size;
  public:
    MemoryBuffer();
    MemoryBuffer(size_t initialsz);
    ~MemoryBuffer();

    /**
     * Allocate internal buffer. If an existing buffer exists,
     * it will be deallocated, its contents being thrown away.
     */
    virtual void allocate(size_t size);

    /**
     * Returns the internal allocated size.
     * A return value of 0 should be treated as
     * an unallocated buffer.
     */
    virtual size_t getSize();

    /**
     * Write into the buffer at the specified offset. Returns
     * the number of bytes written into the buffer. If the
     * end of the buffer is reached, 0 is returned.
     */
    virtual size_t write(const char *buf, size_t bufsize, size_t off);

    /**
     * Reads readsz from offset of the internal buffer.
     */
    virtual size_t read(char *buf, size_t readsz, size_t off);

    /**
     * Clears contents of the internal memory buffer.
     *
     * Effectively, this sets all bytes to NULL.
     */
    void clear();

    /**
     * Assigns contents of the specified buffer.
     *
     * NOTE: This allocates a new internal buffer, in opposite
     *       to a clear()/write() sequence.
     */
    virtual void assign(char *buf, size_t sz);

    std::ostream& operator<<(std::ostream& out);
    MemoryBuffer& operator=(MemoryBuffer &out);
    char& operator[](int index);
  };

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
     * C'tor
     */
    CPGBackupCtlBase();
    virtual ~CPGBackupCtlBase();

    static std::string getVersionString();
    static int strToInt(std::string in);
    static std::string intToStr(int in);
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
     * Calculates a duration of high resolution time points.
     */
    static std::chrono::milliseconds calculate_duration_ms(std::chrono::high_resolution_clock::time_point start,
                                                           std::chrono::high_resolution_clock::time_point stop);

    /**
     * Returns a high resolution time point
     */
    static std::chrono::high_resolution_clock::time_point current_hires_time_point();

    /**
     * Extracts the number of milliseconds from the given duration.
     */
    static long long duration_get_ms(std::chrono::milliseconds ms);

  };

}

#endif
