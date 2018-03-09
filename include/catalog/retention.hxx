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
  private:

    struct _count_pin_context {
      unsigned int performed = 0;
      unsigned int count = 0;
    };

    /*
     * An instance of BasicPinDescr (either PinDescr
     * or UnpinDescr) does a PIN or UNPIN on the
     * specified basebackups meeting the criteria
     * transported by this descriptor.
     */
    BasicPinDescr *pinDescr = nullptr;

    /**
     * Performs a PIN action with the pin
     * policy defined in pinDescr.
     */
    void performPin(std::shared_ptr<BaseBackupDescr> bbdescr);

    /**
     * Performs an UNPIN action with the
     * unpin policy defined in pinDescr.
     */
    void performUnpin(std::shared_ptr<BaseBackupDescr> bbdescr);

  public:

    /*
     * The BasicPinDescr is either a PinDescr or UnpinDescr
     * instance, describing the actions for either PIN or
     * UNPIN.
     *
     * Will throw if the specified BasicPinDescr pointer
     * is a nullptr or set to ACTION_UNDEFINED.
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
     *
     * The caller should pass a list of basebackup descriptors which
     * are sorted in descending order by their started timestamp (like
     * getBackupList() already returns). If a basebackup descriptor
     * is flagged with the aborted tag or in progress, it will be not
     * considered and apply() will step to the next one, if any.
     *
     * If the list is empty, -1 will be returned, otherwise
     * the number of basebackups meeting the pin/unpin criteria
     * is returned. Can throw if catalog database access violations
     * or errors occur (mainly CArchiveIssue exceptions).
     */
    virtual int apply(std::vector<std::shared_ptr<BaseBackupDescr>> list);
  };

}

#endif
