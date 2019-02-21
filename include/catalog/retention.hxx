#ifndef __HAVE_RETENTION__
#define __HAVE_RETENTION__

#include <common.hxx>
#include <BackupCatalog.hxx>

namespace credativ {

  /**
   * Retention exception with a HINT.
   */
  class CRetentionFailureHint : public CCatalogIssue {
  public:
    std::string hint = "";
    CRetentionFailureHint(const char *errstr) throw() : CCatalogIssue(errstr) {};
    CRetentionFailureHint(std::string errstr) throw() : CCatalogIssue(errstr) {};
    CRetentionFailureHint(std::string errstr, std::string hint) throw() :
      CCatalogIssue(errstr) { this->hint = hint; }
  };

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
    RetentionRuleId ruleType = RETENTION_NO_RULE;

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
     * Specified basebackup descriptor is moved from source list
     * into the target list.
     *
     * This copies the specified shared pointer at position index
     * from source list into the target list. source list stays
     * unchanged!
     */
    virtual void move(std::vector<std::shared_ptr<BaseBackupDescr>> &target,
                      std::vector<std::shared_ptr<BaseBackupDescr>> source,
                      std::shared_ptr<BaseBackupDescr> bbdescr,
                      unsigned int index);

  public:

    Retention();
    Retention(std::shared_ptr<CatalogDescr> archiveDescr,
              std::shared_ptr<BackupCatalog> catalog);
    Retention(std::shared_ptr<RetentionRuleDescr> rule);
    virtual ~Retention();

    /**
     * Resets internal retention state information. Is a no-op,
     * if apply() wasn't called before.
     */
    virtual void reset();

    /**
     * Initialize a retention policy with a given cleanup descriptor
     * from an earlier retention.
     */
    virtual void init(std::shared_ptr<BackupCleanupDescr> cleanupDescr) = 0;

    /**
     * Initialize the retention policy instance.
     */
    virtual void init() = 0;

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

    /*
     * Returns the catalog cleanup descriptor, initialized
     * after having apply() on an instance of Retention. Will return
     * a nullptr in case apply() wasn't called before or reset() was issued
     * before.
     */
    virtual std::shared_ptr<BackupCleanupDescr> getCleanupDescr();

    /*
     * Sets the specified XLOG pointers within the cleanup descriptor
     * to keep at least all *younger* XLOG segments starting after the
     * starting offset.
     */
    static bool XLogCleanupOffsetKeep(shared_ptr<BackupCleanupDescr> cleanupDescr,
                                      XLogRecPtr start,
                                      unsigned int timeline,
                                      unsigned int wal_segment_size);

    /**
     * Factory method, returns a Retention object instances identified
     * by retention_name from the catalog, implementing
     * the specific retention method to apply.
     */
    static std::vector<std::shared_ptr<Retention>> get(string retention_name,
                                                       std::shared_ptr<CatalogDescr> archiveDescr,
                                                       std::shared_ptr<BackupCatalog> catalog);

    /**
     * Factory method, returns a specific retention object instance
     * constructed according the RetentionRuleDescr specified.
     *
     * This factory method is primarily useful for temporary, non-executable
     * instance of retention policies which aren't intended to be applied
     * yet.
     */
    static std::shared_ptr<Retention> get(std::shared_ptr<RetentionRuleDescr> ruleDescr);

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
     * asString() implements the string represention
     * of a Retention instance.
     */
    virtual std::string asString() = 0;

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

  public:

    LabelRetention();
    LabelRetention(LabelRetention &src);
    LabelRetention(std::shared_ptr<RetentionRuleDescr> descr);
    LabelRetention(std::string regex_str,
                   std::shared_ptr<CatalogDescr> archiveDescr,
                   std::shared_ptr<BackupCatalog> catalog);
    virtual ~LabelRetention();

    /**
     * Initialize a retention policy with a given cleanup descriptor
     * from an earlier retention.
     *
     * In addition to init() without any argument, this initializes
     * the internal rule state according to the state currently set
     * in the specified BackupCleanupDescr.
     *
     * This must happen before finally calling apply(), which
     * executes the encoded rule against the given basebackup set.
     */
    virtual void init(std::shared_ptr<BackupCleanupDescr> cleanupDescr);

    /**
     * Initialize the retention policy instance.
     *
     * This must happen before finally calling apply(), which
     * executes the encoded rule against the given basebackup set.
     */
    virtual void init();

    /**
     * Implementation of the string representation routine
     * of a LabelRetention instance. Returns the label retention
     * rule hold by a LabelRetention instance as string.
     */
    virtual std::string asString();

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
   * DateTimeRetention policy class.
   *
   * Basically applies a retention policy based
   * on a timestamp definition to the catalog.
   */
  class DateTimeRetention : public Retention {
  private:

    /* Assigned retention interval expression */
    RetentionIntervalDescr interval;

    /* Set interval expression */
    void setIntervalExpr(std::string value);

  public:

    DateTimeRetention();
    DateTimeRetention(DateTimeRetention &src);
    DateTimeRetention(std::string datetime_expr,
                      std::shared_ptr<CatalogDescr> archiveDescr,
                      std::shared_ptr<BackupCatalog> catalog);
    DateTimeRetention(std::shared_ptr<RetentionRuleDescr> rule);

    virtual ~DateTimeRetention();

    /**
     * Initialize a DateTimeRetention instance with a given
     * cleanup descriptor.
     */
    virtual void init(std::shared_ptr<BackupCleanupDescr> cleanupDescr);

    /**
     * Initialize internal state of DateTimeRetention policy.
     */
    virtual void init();

    /*
     * Applies a retention policy on the given list of
     * basebackups. Returns the number of basebackups
     * which got the retention policy applied.
     */
    virtual unsigned int apply(std::vector<std::shared_ptr<BaseBackupDescr>> list);

    /**
     * asString() returns the Retention Rule string representation.
     *
     * NOTE: This is the preferred method to get the syntactic represention
     *       from that catalog as a string. If you want to have the plain
     *       interval expression as a string, use getInterval() instead.
     */
    virtual std::string asString();

    /**
     * Returns the interval expression of a retention policy rule
     * as a string.
     */
    virtual std::string getInterval();

    /**
     * Set the retention rule type id.
     */
    virtual void setRetentionRuleType(const RetentionRuleId ruleType);
  };

  /**
   * CountRetention policy class.
   *
   * CountRetention implements a retention policy where
   * just a number of basebackups starting from the newest to
   * oldest are selected either to keep or to drop.
   */
  class CountRetention : public Retention {
  private:

    /*
     * Retention count.
     *
     * The default count of -1 indicates that
     * a retention wasn't properly set.
     */
    int count = -1;

    /**
     * keep_num()
     *
     * Applies the RETENTION_KEEP_NUM policy to the list
     * of basebackups.
     */
    virtual unsigned int keep_num(std::vector<std::shared_ptr<BaseBackupDescr>> &list);

    /**
     * drop_num()
     *
     * Applies the RETENTION_DROP_NUM policy to the list
     * of basebackups.
     */
    virtual unsigned int drop_num(std::vector<std::shared_ptr<BaseBackupDescr>> &list);

  public:

    CountRetention();
    CountRetention(CountRetention &src);
    CountRetention(unsigned int count,
                   std::shared_ptr <CatalogDescr> archiveDescr,
                   std::shared_ptr <BackupCatalog> catalog);
    CountRetention(std::shared_ptr <RetentionRuleDescr> rule);

    virtual ~CountRetention();

    /**
     * Initialize a CountRetention policy class with a given
     * cleanup descriptor.
     */
    virtual void init(std::shared_ptr<BackupCleanupDescr> prevCleanupDescr);

    /**
     * Initialize internal state of CountRetention policy.
     */
    virtual void init();

    /**
     * Applies a retention policy on the given list of
     * basebackups. Returns the number of basebackups
     * which got the retention policy applied.
     */
    virtual unsigned int apply(std::vector<std::shared_ptr<BaseBackupDescr>> list);

    /**
     * Returns the string representation of a CountRetention policy value.
     */
    virtual std::string asString();

    /**
     * Set the retention rule type.
     */
    virtual void setRetentionRuleType(const RetentionRuleId ruleType);

    /**
     * Set the retention count.
     */
    virtual void setValue(int count);

    /**
     * Reset internal state.
     *
     * This is an overloaded version of Retention::reset(), which
     * additionally sets the count property back to its default.
     */
    virtual void reset();

  };

  /**
   * CleanupRetention policy class.
   *
   * A CleanupRetention policy instance cleans the list
   * of failed basebackups.
   */
  class CleanupRetention : public Retention {
  private:

    /*
     * Cleanup rule value, currently always set to "cleanup"
     */
    std::string cleanup_value = "cleanup";

  public:

    CleanupRetention();
    CleanupRetention(CleanupRetention &src);
    CleanupRetention(std::shared_ptr<CatalogDescr> archiveDescr,
                     std::shared_ptr<BackupCatalog> catalog);
    CleanupRetention(std::shared_ptr<RetentionRuleDescr> rule);

    /**
     * Set the retention rule type. This is effectively allows
     * RETENTION_CLEANUP rules only, since a CleanupRetention
     * policy implements exactly *one* policy: cleanup.. ;)
     */
    virtual void setRetentionRuleType(const RetentionRuleId ruleType);

    /**
     * Applies a retention policy on the given list of
     * basebackups. Returns the number of basebackups
     * which got the retention policy applied.
     */
    virtual unsigned int apply(std::vector<std::shared_ptr<BaseBackupDescr>> list);

    /**
     * Returns the string representation of this rule.
     */
    virtual std::string asString();

    /**
     * Initialize a CountRetention policy class with a given
     * cleanup descriptor.
     */
    virtual void init(std::shared_ptr<BackupCleanupDescr> prevCleanupDescr);

    /**
     * Initialize internal state of CountRetention policy.
     */
    virtual void init();
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
     * Initialize a retention policy with a given cleanup descriptor
     * from an earlier retention.
     */
    virtual void init(std::shared_ptr<BackupCleanupDescr> cleanupDescr);

    /**
     * Initialize the retention policy instance.
     */
    virtual void init();

    /**
     * Returns the string representation of a PIN/UNPIN action.
     */
    virtual string asString();

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
