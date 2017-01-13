#include <iostream>
#include <fstream>
#include <sstream>
#include <string>

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

void CPGBackupCtlBase::writeFileReplace(std::string filePath,
                                        std::string msg)
  throw(CPGBackupCtlFailure) {

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

  if (file_ext.string() == ".gz") {
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

string CPGBackupCtlBase::ptime_to_str(ptime input) {
  ostringstream oss;

  locale        loc = locale(std::locale::classic(),
                             new time_input_facet("%Y-%m-%d %H:%M:%S%F %z"));

  oss.imbue(loc);
  oss << input;
  return oss.str();
}

int CPGBackupCtlBase::strToInt(std::string in) {
  std::istringstream iss(in);
  int result;
  iss >> result;

  return result;
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
