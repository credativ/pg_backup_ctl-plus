#ifndef __BACKUP_CATALOG__
#define __BACKUP_CATALOG__

using namespace std;

namespace credativ {

  class CatalogDescr : protected CPGBackupCtlBase {
  public:
    CatalogDescr() {};
    ~CatalogDescr() {};
  };

}

#endif
