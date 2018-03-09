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

    /**
     * Applies a retention policy on the given list of
     * basebackups.
     */
    virtual int apply(std::vector<std::shared_ptr<BaseBackupDescr>> list) = 0;

  };

  /**
   * A generic retention rule.
   */
  class GenericRetentionRule : public CPGBackupCtlBase {
  public:

    GenericRetentionRule();
    GenericRetentionRule(std::shared_ptr<CatalogDescr> descr);
    virtual ~GenericRetentionRule();

  };

  /**
   * A PinRetention is to some degree a special
   * kind of retention. Instead of deleting and cleaning
   * up basebackups from the catalog, it just applies PIN or
   * UNPIN actions to the basebackup catalog entries, depending
   * on the action passed to it. A PinDescr or UnpinDescr describes
   * the action on what to do.
   */
  class PinRetention : public Retention {
  public:

    /*
     * The BasicPinDescr is either a PinDescr or UnpinDescr
     * instance, describing the actions for either PIN or
     * UNPIN.
     */
    PinRetention(BasicPinDescr *descr,
                 std::shared_ptr<BackupCatalog> catalog);
    virtual ~PinRetention();

    /**
     * Apply the pin/unpin retention to the list
     * of basebackups.
     *
     * Any basebackups which met the pin or unpin criteria
     * are pinned or unpinned afterwards.
     */
    virtual int apply(std::vector<std::shared_ptr<BaseBackupDescr>> list);
  };

}

#endif
