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

string CPGBackupCtlBase::current_timestamp() {

  std::time_t t  = std::time(NULL);
  char res[100];
  std::string result;

  memset(res, 0, sizeof(res));

  if (std::strftime(res, sizeof(res), "%Y-%m-%d %H:%M:%S", std::localtime((&t))))
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
