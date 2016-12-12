#ifndef __BACKUP_HXX__
#define __BACKUP_HXX__

#include <BackupCatalog.hxx>

namespace credativ {

  class Backup : CPGBackupCtlBase {
  public:
    Backup(const std::shared_ptr<CatalogDescr>& descr);
    virtual ~Backup();

    virtual void create() = 0;
  };

  class StreamBaseBackup: public Backup {
  public:
    StreamBaseBackup(const std::shared_ptr<CatalogDescr>& descr);
    ~StreamBaseBackup();

    virtual void create();
  };

}

#endif
