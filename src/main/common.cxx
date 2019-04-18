#include <iostream>
#include <fstream>
#include <sstream>
#include <string>

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/iterator_range.hpp>

#include "common.hxx"

using namespace credativ;
using namespace std;
using namespace boost::posix_time;
using namespace boost::iostreams;
using namespace std::chrono;



Range::Range(int start, int end) {

  if (start > end)
    throw CPGBackupCtlFailure("integer range end is larger than start");

  this->startval = start;
  this->endval = end;
}

Range::~Range() {};

int Range::start() { return this->startval; }
int Range::end() { return this->endval; }

CPGBackupCtlBase::CPGBackupCtlBase() {
  /* currently empty */
}

CPGBackupCtlBase::~CPGBackupCtlBase() {
  /* nothing special */
}

string CPGBackupCtlBase::getVersionString() {
  return string("pg_backup_ctl++, version "
                + intToStr(PG_BACKUP_CTL_MAJOR)
                + "."
                + intToStr(PG_BACKUP_CTL_MINOR));
}

bool CPGBackupCtlBase::resolve_file_path(std::string filename) {

  namespace bfs = boost::filesystem;

  char *pathnames = getenv("PATH");
  std::vector<std::string> path_list;

  if (pathnames == NULL)
    return false;

  boost::split(path_list, pathnames, boost::is_any_of(":"));

  for(auto &path_name : path_list) {

    /*
     * We build a boost::filesystem path object and use
     * its exists() method to look up the absolute path
     * of the specified file. It's likely that most of the time
     * this will throw (since the file doesn't live in the
     * majority of paths here ;). Thus, we just check for
     * boost::system::errc::success, which indicates a match.
     */
    bfs::path lookup_file_path = bfs::path(path_name) / bfs::path(filename);
    boost::system::error_code ec;

    bfs::exists(lookup_file_path, ec);

    if (ec.value() == boost::system::errc::success)
      return true;

  }

  return false;
}

void CPGBackupCtlBase::writeFileReplace(std::string filePath,
                                        std::string msg) {

  boost::filesystem::path file(filePath);
  boost::filesystem::ofstream out(file);

  /*
   * Check if file is ready
   */
  if (out.is_open())
    out << msg;
  else {
    ostringstream oss;
    oss << "cannot write info file " << filePath << " ";
    throw CPGBackupCtlFailure(oss.str());
  }
}

/*
 * Prepares a gzipp'ed or plain ostringstring to write to.
 */
filtering_ostream* CPGBackupCtlBase::prepareBinaryOutFile(boost::filesystem::path pathHandle,
                                                          std::ofstream& outstream) {

  filtering_ostream
    *zipped = new filtering_ostream();
  outstream.open(pathHandle.string(), std::ios_base::out | std::ios_base::binary);

  /*
   * The specified path handle should hold a file extension which will
   * *.gz in case a file compression output is requested.
   */
  boost::filesystem::path file_ext = pathHandle.extension();
  if (file_ext.string() == ".gz") {
    zipped->push(gzip_compressor());
  }

  zipped->push(outstream);

  /*
   * ...and we're done.
   */
  return zipped;
}

/**
 * C++ doesn't have this ready, see
 *
 * https://stackoverflow.com/questions/3418231/replace-part-of-a-string-with-another-string
 *
 * for discussion.
 */
void CPGBackupCtlBase::strReplaceAll(std::string& str, const std::string& from, const std::string& to) {
  size_t start_pos = 0;
  if(from.empty())
    return;

  while((start_pos = str.find(from, start_pos)) != std::string::npos) {
    str.replace(start_pos, from.length(), to);
    start_pos += to.length(); // In case 'to' contains 'from', like replacing 'x' with 'yx'
  }
}

void CPGBackupCtlBase::prepareSyncedBinaryOutFile(boost::filesystem::path pathHandle,
                                                  SyncedBinaryOutFile& handle) {

  handle.fp = fopen(pathHandle.string().c_str(), "w");

  if (handle.fp == NULL) {
    std::ostringstream oss;
    oss << "could not open file " << pathHandle.string() << strerror(errno);
    throw CPGBackupCtlFailure(oss.str());
  }

  handle.fd = fileno(handle.fp);
  handle.sink = file_descriptor_sink(handle.fd, never_close_handle);

  handle.out = new filtering_ostream();

  boost::filesystem::path file_ext = pathHandle.extension();

  if (file_ext.string() == ".gz") {
    handle.out->push(gzip_compressor());
  }

  handle.out->push(handle.sink);
}

void CPGBackupCtlBase::syncAndClose(SyncedBinaryOutFile& handle) {

  if (handle.sink.is_open()) {
    handle.out->flush();
    fsync(handle.sink.handle());
    handle.sink.close();
    delete handle.out;
  }

}

/*
 * Write to the specified file referenced by the
 * ostringstream out.
 *
 * NOTE: Since this method assumes out is already opened
 *       and prepared by prepareBinaryOutFile().
 */
void CPGBackupCtlBase::writeChunk(SyncedBinaryOutFile file,
                                  char *binaryblock,
                                  size_t size) {

  file.out->write(binaryblock, size);

}

void CPGBackupCtlBase::openFile(std::ifstream& file,
                                std::stringstream& out,
                                boost::filesystem::path pathHandle,
                                bool *compressed) {

  filtering_istream unzipped;
  file.open(pathHandle.string(), std::ios_base::in | std::ios_base::binary);

  /*
   * Is this history file compressed? We check this
   * against the file extension, which should be *.gz (others
   * weren't used by pg_backup_ctl in the past...).
   */
  boost::filesystem::path file_ext = pathHandle.extension();

  if (file_ext.string() == ".gz"
      || file_ext.string() == ".zstd") {
    /*
     * This is a gzipp'ed backup history file. We need
     * to read and uncompress the data before scanning
     * it line by line.
     */
    unzipped.push(gzip_decompressor());
    *compressed = true;
  } else {
    /* Read lines from plain history file */
    *compressed = false;
  }

  unzipped.push(file);

  /*
   * Note: boost::iostreams::copy needs
   * a stringstream object, others won't work
   * as expected.
   */
  boost::iostreams::copy(unzipped, out);
  file.close();

}

ptime CPGBackupCtlBase::ISO8601_strTo_ptime(string input) {

  istringstream iss(input);
  ptime         result;
  locale        loc = locale(std::locale::classic(),
                             new time_input_facet("%Y-%m-%d %H:%M:%S%F %z"));
  iss.imbue(loc);
  iss >> result;

  return result;
}

string CPGBackupCtlBase::current_timestamp(bool asFilename) {

  std::time_t t  = std::time(NULL);
  char res[100];
  std::string result;
  std::string format;

  if (asFilename) {
    format = "%Y%m%d%H%M%S";
  } else {
    format = "%Y-%m-%d %H:%M:%S";
  }

  memset(res, 0, sizeof(res));

  if (std::strftime(res, sizeof(res), format.c_str(), std::localtime((&t))))
    result = string(res);
  else
    result = "";

  return result;
}

high_resolution_clock::time_point CPGBackupCtlBase::current_hires_time_point() {

  return high_resolution_clock::now();

}

std::chrono::milliseconds CPGBackupCtlBase::ms_get_duration(long ms) {
  return std::chrono::milliseconds(ms);
}

std::chrono::milliseconds CPGBackupCtlBase::calculate_duration_ms(high_resolution_clock::time_point start,
                                                                  high_resolution_clock::time_point stop) {

  auto result = duration_cast<std::chrono::milliseconds>(stop - start);
  return result;

}

std::chrono::microseconds CPGBackupCtlBase::calculate_duration_us(high_resolution_clock::time_point start,
                                                                  high_resolution_clock::time_point stop) {

  auto result = duration_cast<std::chrono::microseconds>(stop - start);
  return result;

}

uint64 CPGBackupCtlBase::duration_get_ms(std::chrono::milliseconds ms) {
  return ms.count();
}

uint64 CPGBackupCtlBase::duration_get_us(std::chrono::microseconds us) {
  return us.count();
}

string CPGBackupCtlBase::ptime_to_str(ptime input) {
  ostringstream oss;

  locale        loc = locale(std::locale::classic(),
                             new time_input_facet("%Y-%m-%d %H:%M:%S%F %z"));

  oss.imbue(loc);
  oss << input;
  return oss.str();
}

unsigned int CPGBackupCtlBase::strToUInt(std::string in) {

  std::istringstream iss(in);
  unsigned int result;
  iss >> result;

  return result;
}

int CPGBackupCtlBase::strToInt(std::string in) {
  std::istringstream iss(in);
  int result;
  iss >> result;

  return result;
}

string CPGBackupCtlBase::uintToStr(unsigned int in) {
  stringstream ss;

  ss << in;
  return ss.str();
}

string CPGBackupCtlBase::intToStr(int in) {
  stringstream ss;

  ss << in;
  return ss.str();
}

std::string CPGBackupCtlBase::makeLine(int width) {

  ostringstream line;

  for (int i = 0; i < width; i++) {
    line << '-';
  }

  return line.str();

}

std::string CPGBackupCtlBase::makeLine(boost::format& formatted) {

  std::ostringstream line;

  line << formatted << endl;
  return line.str();

}

std::string CPGBackupCtlBase::makeHeader(std::string caption,
                                         boost::format& format,
                                         int width) {

  std::ostringstream header;

  header << caption << endl;
  header << CPGBackupCtlBase::makeLine(width) << endl;
  header << format << endl;
  header << CPGBackupCtlBase::makeLine(width) << endl;

  return header.str();
}

std::string CPGBackupCtlBase::basebackup_filename() {

  std::ostringstream filename;
  filename << "basebackup-" << current_timestamp(true);

  return filename.str();

}

std::string CPGBackupCtlBase::stdout_red(std::string in, bool bold) {

  /* In case not a terminal, return plain string */
  if (!isatty(fileno(stdout)))
    return in;

  if (bold)
    return string("\033[1;31m" + in + " \033[0m");
  else
    return string("\033[0;31m" + in + " \033[0m");

}

std::string CPGBackupCtlBase::stdout_green(std::string in, bool bold) {

    /* In case not a terminal, return plain string */
  if (!isatty(fileno(stdout)))
    return in;

  if (bold)
    return string("\033[1;32m" + in + " \033[0m");
  else
    return string("\033[0;32m" + in + " \033[0m");

}

/*
 * This code is borrowed from PostgreSQL's pg_size_pretty()
 * function, see src/backend/utils/adt/dbsize.c
 */
std::string CPGBackupCtlBase::prettySize(size_t size) {

  std::ostringstream result;
  /*
   * bytes are converted into kB starting above 10240 bytes.
   */
  size_t limit_plain = 10 * 1024;
  size_t limit = (2 * limit_plain) - 1;

  if (size < limit_plain) {
    result << size << " bytes";
    return result.str();
  } else {

    size >>= 10;

    if (size < limit) {
      result << size << " kB";
      return result.str();
    } else {

      size >>= 10;

      if (size < limit) {
        result << size << " MB";
        return result.str();
      } else {

        size >>= 10;

        if (size < limit) {
          result << size << " GB";
          return result.str();
        } else {
          result << size << " TB";
          return result.str();
        }
      }
    }
  }

  return "NaN";
}
