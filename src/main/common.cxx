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
  return string("pg_backup_ctl++, version " + to_string(PG_BACKUP_CTL_MAJOR) + "." + to_string(PG_BACKUP_CTL_MINOR));
}
