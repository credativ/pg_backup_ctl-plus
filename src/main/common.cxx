#include <sstream>
#include <string>
#include "common.hxx"
using namespace credativ;
using namespace std;

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

string CPGBackupCtlBase::intToStr(int in) {
  stringstream ss;
  
  ss << in;
  return ss.str();
}
