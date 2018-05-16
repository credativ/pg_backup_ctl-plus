#ifndef __HAVE_RETENTION__
#define __HAVE_RETENTION__

#include <common.hxx>
#include <BackupCatalog.hxx>

namespace credativ {

  /**
   * Retention is the base class for retention
   * rules.
   */
  class Retention {
  protected:

    /**
     * Internal cleanup descriptor. Initialized by calling
     * apply(), can be resetted by calling reset().
     */
    std::shared_ptr<BackupCleanupDescr> cleanupDescr = nullptr;

    /**
     * Internal rule action specifier. Determines wether
     * we are instructed to keep or drop the basebackups meeting
     * the label regular expression.
     */
    RetentionRuleId ruleType;

    /**
     * Internal catalog database handler.
     */
    std::shared_ptr<BackupCatalog> catalog = nullptr;

    /**
     * Internal catalog descriptor, identifies
     * the archive we are operating on.
     */
    std::shared_ptr<CatalogDescr> archiveDescr = nullptr;

    /**
     * asString() implements the string represention
     * of a Retention instance.
     */
    virtual std::string asString() = 0;

    /**
     * Specified basebackup descriptor is marked
     * for being kept.
     *
     * This moves the specified shared pointer at position index
     * from the dropList into the keepList.
     */
    virtual void keep(std::vector<std::shared_ptr<BaseBackupDescr>> &keepList,
                      std::vector<std::shared_ptr<BaseBackupDescr>> &dropList,
                      std::shared_ptr<BaseBackupDescr> bbdescr,
                      unsigned int index);

  public:

    Retention();
    Retention(std::shared_ptr<CatalogDescr> archiveDescr,
              std::shared_ptr<BackupCatalog> catalog);
    virtual ~Retention();

    /**
     * Resets internal retention state information. Is a no-op,
     * if apply() wasn't called before.
     */
    virtual void reset();

    /**
     * Assign catalog handle.
     */
    virtual void setCatalog(std::shared_ptr<BackupCatalog> catalog);

    /**
     * Assign archive catalog descriptor.
     */
    virtual void setArchiveCatalogDescr(std::shared_ptr<CatalogDescr> archiveDescr);

    /**
     * Returns the currently associated catalog archive handle.
     */
    std::shared_ptr<CatalogDescr> getArchiveCatalogDescr();

    /**
     * Returns the currently associated backup catalog database handle.
     */
    std::shared_ptr<BackupCatalog> getBackupCatalog();

    /**
     * Factory method, returns a Retention object instances, implementing
     * the specific retention method to apply.
     */
    static std::vector<std::shared_ptr<Retention>> get(string retention_name,
                                                       std::shared_ptr<CatalogDescr> archiveDescr,
                                                       std::shared_ptr<BackupCatalog> catalog);

    /**
     * Applies a retention policy on the given list of
     * basebackups. Returns the number of basebackups
     * which got the retention policy applied.
     */
    virtual unsigned int apply(std::vector<std::shared_ptr<BaseBackupDescr>> list) = 0;

    /**
     * Returns the string representation of a retention rule. Must be implemented
     * for each specific retention policy instance.
     */
    virtual std::string operator=(Retention &src);

    /**
     * Set the retention rule type id supported by a Retention
     * instance and its ancestors.
     */
    virtual void setRetentionRuleType(const RetentionRuleId ruleType) = 0;

    /**
     * Returns the rule type id encoded by a specific Retention instance
     * and its ancestors. This can also be used to identify the actions
     * encoded to such an object instance.
     */
    virtual RetentionRuleId getRetentionRuleType();

  };

  /**
   * Label retention policy.
   *
   * Implements a label retention policy, based on regular
   * expression applied to the specified list of basebackups
   *
   * If the regular expression matches, then the basebackup will
   * be scheduled for removal.
   *
   */
  class LabelRetention : public Retention {
  protected:

    /**
     * Regex expression.
     */
    boost::regex label_filter;

    /**
     * Implementation of the string representation routine
     * of a LabelRetention instance. Returns the label retention
     * rule hold by a LabelRetention instance as string.
     */
    virtual std::string asString();

  public:

    LabelRetention();
    LabelRetention(LabelRetention &src);
    LabelRetention(std::string regex_str,
                   std::shared_ptr<CatalogDescr> archiveDescr,
                   std::shared_ptr<BackupCatalog> catalog);
    virtual ~LabelRetention();

    /**
     * Applies a retention policy on the given list of
     * basebackups. Returns the number of basebackups
     * which got the retention policy applied.
     */
    virtual unsigned int apply(std::vector<std::shared_ptr<BaseBackupDescr>> list);

    /**
     * Set regular expression to evaluate by a LabelRetention instance.
     */
    virtual void setRegularExpr(string regex_str);

    /**
     * Returns the compiled regular expression handler.
     */
    virtual boost::regex getRegularExpr();

    /**
     * Sets the rule action identifier. The only allowed values
     * are RETENTION_KEEP_WITH_LABEL and RETENTION_DROP_WITH_LABEL. Other
     * value will throw a CCatalogIssue exception.
     */
    virtual void setRetentionRuleType(const RetentionRuleId ruleType);
  };

  /**
   * A PinRetention is to some degree a special
   * kind of retention. Instead of deleting and cleaning
   * up basebackups from the catalog, it just applies PIN or
   * UNPIN actions to the basebackup catalog entries, depending
   * on the action passed to it. A PinDescr or UnpinDescr describes
   * the action on what to do.
   *
   * Since a pin/unpin operation interacts with the backup catalog
   * database, any method might throw.
   */
  class PinRetention : public Retention {
  private:

    /*
     * Private execution counters, summarized
     * in this struct.
     */
    struct _count_pin_context {

      /*
       * number of basebackups choosen
       */
      unsigned int performed = 0;

      /*
       * If ACTION_COUNT, the number of basebackups
       * to pin
       */
      unsigned int count = 0;

      /*
       * Number of basebackups to consider
       */
      unsigned int expected = 0;

    } count_pin_context;

    /*
     * An instance of BasicPinDescr (either PinDescr
     * or UnpinDescr) does a PIN or UNPIN on the
     * specified basebackups meeting the criteria
     * transported by this descriptor.
     */
    BasicPinDescr *pinDescr = nullptr;

    /**
     * Performs the change on the backup catalog database.
     * The specified list contains all vectors which are
     * affected by the current action described with the
     * PinDescr instance.
     */
    void performDatabaseAction(std::vector<int> &basebackupIds);

    /**
     * Dispatches the BasicPinDescr
     * to its specific routine.
     */
    unsigned int dispatchPinAction(std::vector<std::shared_ptr<BaseBackupDescr>> &list);

    /**
     * Perform a ACTION_COUNT pin action. Will pin or
     * unpin, depending on the PinDesr associated
     * with a PinRetention instance.
     *
     * The action_Count() requires the specified list to be
     * presorted in descending order, sorted by the started date
     * to work properly!
     */
    unsigned int action_Count(std::vector<std::shared_ptr<BaseBackupDescr>> &list);

    /**
     * action_Pinned() implements the algorithm for UNPIN PINNED
     * commands (ACTION_PINNED).
     *
     * Currently, we support the PINNED operation for UNPIN
     * actions only, so this function will throw when called
     * within a PIN_BASEBACKUP context.
     *
     * Returns the number of basebackups unpinned.
     */
    unsigned int action_Pinned(std::vector<std::shared_ptr<BaseBackupDescr>> &list);

    /**
     * Do a pin/unpin operation for a specified basebackup ID.
     *
     * We accept an vector of basebackupIds nevertheless (for being
     * consistent with the other action_* methods), though the
     * interface currently just provides a single basebackup ID. This
     * is en par with the other action_* methods.
     */
    unsigned int action_ID(std::vector<std::shared_ptr<BaseBackupDescr>> &list);

    /**
     * Perform PIN/UNPIN on the newest or oldest basebackup
     * derived from the specified basebackup descriptor list.
     *
     * NOTE: action_NewestOrOldest() expects the list to be sorted
     *       descending order by their started timestamp. See
     *       BackupCatalog::getBackupList() for details.
     *
     *       If the list is not correctly sorted, the choosen
     *       basebackup for the pin/unpin operation is abitrary.
     */
    unsigned int action_NewestOrOldest(std::vector<std::shared_ptr<BaseBackupDescr>> &list);

  protected:

    /**
     * Returns the string representation of a PIN/UNPIN action.
     */
    virtual string asString();

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
                 std::shared_ptr<CatalogDescr> archiveDescr,
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
     * is flagged *not* ready, it will be not considered and apply()
     * will step to the next one, if any.
     *
     * If list is empty, apply() will do nothing and return 0.
     *
     * The number of basebackups meeting the pin/unpin criteria
     * is returned. Can throw if catalog database access violations
     * or errors occur (mainly CArchiveIssue exceptions).
     */
    virtual unsigned int apply(std::vector<std::shared_ptr<BaseBackupDescr>> list);

    /*
     * After having called apply(), returns the number of
     * objects identified for pin(), unpin().
     */
    virtual unsigned int pinsPerformed();

    /**
     * If a PinRetention should be applied multiple times,
     * the caller should use reset() to reset the internal state
     * of a PinRetention instance before calling apply() again on a
     * new backup ID set.
     */
    virtual void reset();

    /**
     * Implementation of pin/unpin retention rule identifier.
     */
    virtual void setRetentionRuleType(const RetentionRuleId ruleType);

  };

}

#endif
