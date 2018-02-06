#ifndef __HAVE_RETENTION__
#define __HAVE_RETENTION__

#include <common.hxx>
#include <BackupCatalog.hxx>

namespace credativ {

  /**
   * Retention is the base class for retention
   * rules.
   */
  class Retention : public CPGBackupCtlBase {
  protected:

    /**
     * Internal catalog handle.
     */
    std::shared_ptr<BackupCatalog> catalog = nullptr;

  public:

    Retention();
    Retention(std::shared_ptr<BackupCatalog> catalog);
    virtual ~Retention();

    /**
     * Assign catalog handle.
     */
    virtual void setCatalog(std::shared_ptr<BackupCatalog> catalog);

    /**
     * Factory method, returns a Retention object instance.
     */
    static std::shared_ptr<Retention> get(string retention_name,
                                          std::shared_ptr<BackupCatalog> catalog);

  };

  class GenericRetentionRule : public CPGBackupCtlBase {
  public:

    GenericRetentionRule();
    virtual ~GenericRetentionRule();

  };
}

#endif
