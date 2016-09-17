#include <iostream>
#include <fstream>
#include <sstream>
#include <string>

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

void CPGBackupCtlBase::test() {

  //Read from the first command line argument, assume it's gzipped
  std::ifstream file("abc", std::ios_base::in | std::ios_base::binary);
  boost::iostreams::filtering_streambuf<boost::iostreams::input> inbuf;
  inbuf.push(boost::iostreams::gzip_decompressor());
  inbuf.push(file);
  //Convert streambuf to istream
  std::istream instream(&inbuf);
  //Iterate lines
  std::string line;
  while(std::getline(instream, line)) {
    std::cout << line << std::endl;
  }
  //Cleanup
  file.close();

}

void CPGBackupCtlBase::openFile(std::ifstream& file,
                                std::stringstream& out,
                                boost::filesystem::path pathHandle,
                                bool *compressed) {

  filtering_istream/*<boost::iostreams::input>*/ unzipped;
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
  boost::iostreams::copy(unzipped, out);
  
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

string CPGBackupCtlBase::ptime_to_str(ptime input) {
  ostringstream oss;

  locale        loc = locale(std::locale::classic(),
                             new time_input_facet("%Y-%m-%d %H:%M:%S%F %z"));

  oss.imbue(loc);
  oss << input;
  return oss.str();
}

string CPGBackupCtlBase::intToStr(int in) {
  stringstream ss;

  ss << in;
  return ss.str();
}
