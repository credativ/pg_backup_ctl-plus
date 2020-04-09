#include <retention.hxx>
#include <boost/pointer_cast.hpp>
#include <boost/range/adaptor/reversed.hpp>
#include <boost/log/trivial.hpp>

using namespace credativ;

Retention::Retention(std::shared_ptr<CatalogDescr> archiveDescr,
                     std::shared_ptr<BackupCatalog> catalog) {

  if (archiveDescr == nullptr)
    throw CArchiveIssue("cannot initialize retention policy with undefined archive descriptor");

  if (archiveDescr->tag == EMPTY_DESCR)
    throw CArchiveIssue("cannot initialize retention policy with empty archive descriptor");

  if (catalog == nullptr)
    throw CArchiveIssue("cannot initialize retention policy with undefined catalog database handle");

  this->archiveDescr = archiveDescr;
  this->catalog = catalog;

}

Retention::~Retention() {}

Retention::Retention() {

  this->archiveDescr = nullptr;
  this->catalog = nullptr;

}

Retention::Retention(shared_ptr<RetentionRuleDescr> rule) {

  this->ruleType = rule->type;

}

void Retention::setArchiveCatalogDescr(std::shared_ptr<CatalogDescr> archiveDescr) {

  if (archiveDescr->id < 0)
    throw CCatalogIssue("could not assign invalid archive ID to retention policy");

  this->archiveDescr = archiveDescr;

}

void Retention::setCatalog(std::shared_ptr<BackupCatalog> catalog) {

  this->catalog = catalog;

}

std::shared_ptr<BackupCleanupDescr> Retention::getCleanupDescr() {

  return this->cleanupDescr;

}

string Retention::operator=(Retention &src) {

  return src.asString();

}

std::shared_ptr<CatalogDescr> Retention::getArchiveCatalogDescr() {

  return this->archiveDescr;

}

std::shared_ptr<BackupCatalog> Retention::getBackupCatalog() {

  return this->catalog;

}

RetentionRuleId Retention::getRetentionRuleType() {
  return this->ruleType;
}

void Retention::reset() {

  if (this->cleanupDescr != nullptr) {

    this->cleanupDescr->basebackups.clear();
    this->cleanupDescr == nullptr;

  }

}

shared_ptr<Retention> Retention::get(shared_ptr<RetentionRuleDescr> ruleDescr) {

  shared_ptr<Retention> result = nullptr;

  /*
   * Some sanity checks ...
   */
  if (ruleDescr == nullptr) {
    throw CCatalogIssue("invalid retention rule descriptor in factory method");
  }

  if (ruleDescr->id < 0) {
    throw CCatalogIssue("retention rule must be fully initialized");
  }

  switch (ruleDescr->type) {

  case RETENTION_KEEP_WITH_LABEL:
  case RETENTION_DROP_WITH_LABEL:
    {
      result = make_shared<LabelRetention>(ruleDescr);
      break;
    }

  case RETENTION_KEEP_NUM:
  case RETENTION_DROP_NUM:
    {
      result = make_shared<CountRetention>(ruleDescr);
      break;
    }

  case RETENTION_KEEP_NEWER_BY_DATETIME:
  case RETENTION_DROP_NEWER_BY_DATETIME:
  case RETENTION_KEEP_OLDER_BY_DATETIME:
  case RETENTION_DROP_OLDER_BY_DATETIME:
    {
      result = make_shared<DateTimeRetention>(ruleDescr);
      break;
    }

  case RETENTION_CLEANUP:
    {
      result = make_shared<CleanupRetention>(ruleDescr);
      break;
    }

  default:
    {
      ostringstream oss;

      oss << "unsupported retention rule type: " << ruleDescr->type;
      throw CCatalogIssue(oss.str());
    }

    break; /* not reached */

  }

  return result;
}

std::vector<std::shared_ptr<Retention>> Retention::get(string retention_name,
                                                       std::shared_ptr<CatalogDescr> archiveDescr,
                                                       std::shared_ptr<BackupCatalog> catalog) {

  shared_ptr<RetentionDescr> retentionDescr = nullptr;
  vector<shared_ptr<Retention>> result;

  /*
   * Preliminary checks, there's nothing to do if either
   * no catalog or an empty retention policy identifier was specified.
   */
  if (retention_name.length() == 0)
    return result;

  if (catalog == nullptr) {
    throw CCatalogIssue("could not retrieve retention rules. catalog database not initialized");
  }

  if (! catalog->available()) {
    throw CCatalogIssue("could not retrieve retention rules: catalog database not opened");
  }

  /*
   * Fetch the rule policy via the specified retention_name.
   */
  retentionDescr = catalog->getRetentionPolicy(retention_name);

  /*
   * Check out if this descriptor is valid.
   */
  if ((retentionDescr != nullptr) && (retentionDescr->id >= 0)) {

    /*
     * Yes, looks so. Instantiate corresponding rule instance
     * and push into our result vector.
     */
    for (auto ruleDescr : retentionDescr->rules) {

      std::shared_ptr<Retention> retentionPtr = nullptr;

      /* make sure, ruleDescr is valid */
      if ((ruleDescr != nullptr) && (ruleDescr->id >= 0)
          && (ruleDescr->type != RETENTION_NO_RULE)) {

        switch(ruleDescr->type) {

        case RETENTION_KEEP_WITH_LABEL:
        case RETENTION_DROP_WITH_LABEL:
          {
            retentionPtr = make_shared<LabelRetention>(ruleDescr->value,
                                                       archiveDescr,
                                                       catalog);
            retentionPtr->setRetentionRuleType(ruleDescr->type);
            break;
          }

        case RETENTION_KEEP_NUM:
        case RETENTION_DROP_NUM:
          {
            retentionPtr = make_shared<CountRetention>(CPGBackupCtlBase::strToInt(ruleDescr->value),
                                                       archiveDescr,
                                                       catalog);
            retentionPtr->setRetentionRuleType(ruleDescr->type);

            break;
          }

        case RETENTION_KEEP_NEWER_BY_DATETIME:
        case RETENTION_DROP_NEWER_BY_DATETIME:
        case RETENTION_KEEP_OLDER_BY_DATETIME:
        case RETENTION_DROP_OLDER_BY_DATETIME:
          {
            retentionPtr = make_shared<DateTimeRetention>(ruleDescr->value,
                                                          archiveDescr,
                                                          catalog);
            retentionPtr->setRetentionRuleType(ruleDescr->type);

            break;
          }

        case RETENTION_CLEANUP:
          {
            retentionPtr = make_shared<CleanupRetention>(archiveDescr,
                                                         catalog);
            retentionPtr->setRetentionRuleType(ruleDescr->type);

            break;
          }
        default:
	  {
	    ostringstream oss;

	    oss << "unsupported retention rule type: " << ruleDescr->type;
	    throw CCatalogIssue(oss.str());
	  }

          break; /* not reached */

        } /* switch..case */
      }

      /*
       * Stick the rule into our result vector.
       */
      if (retentionPtr != nullptr)
        result.push_back(retentionPtr);

    }
  }

  return result;
}

bool Retention::XLogCleanupOffsetKeep(shared_ptr<BackupCleanupDescr> cleanupDescr,
                                      XLogRecPtr start,
                                      unsigned int timeline,
                                      unsigned int wal_segment_size) {

  bool result = false;
  tli_cleanup_offsets::iterator it;
  shared_ptr<xlog_cleanup_off_t> cleanup_offset = nullptr;

  /*
   * We always define the offset for XLOG cleanup
   * to be on the starting offset to its segment.
   */
  XLogRecPtr start_segment_ptr = PGStream::XLOGSegmentStartPosition(start,
                                                                    wal_segment_size);

  /*
   * Sanity check, cleanup descriptor is valid.
   */
  if (cleanupDescr == nullptr)
    return result;

  /*
   * Check whether the current timeline is already initialized.
   */
  it = cleanupDescr->off_list.find(timeline);

  if (it == cleanupDescr->off_list.end()) {

    cleanup_offset = make_shared<xlog_cleanup_off_t>();
    cleanup_offset->timeline = timeline;
    cleanup_offset->wal_segment_size = wal_segment_size;
    cleanup_offset->wal_cleanup_start_pos = start_segment_ptr;

    /*
     * Insert the new timeline cleanup offset.
     */
    if (!cleanupDescr->off_list.insert(std::pair<int, shared_ptr<xlog_cleanup_off_t>>(timeline,
                                                                                      cleanup_offset)).second) {

      /*
       * This is unexpected. We have checked whether the timeline
       * already has a cleanup offset attached. If we're landing here,
       * this means something went terribly wrong.
       */
      throw CArchiveIssue("could not attach new timeline cleanup offset");

    }

#ifdef __DEBUG__XLOG__
    BOOST_LOG_TRIVIAL(debug) << "DEBUG: insert new tli/offset "
                             << timeline
                             << "/"
                             << cleanup_offset->wal_cleanup_start_pos;
#endif

    result = true;

  } else {

    cleanup_offset = it->second;

    /*
     * Iff given offset is *older* in the stream, assign it,
     * since we need to keep the other XLOG segments located after
     * it.
     */
    if (start_segment_ptr < cleanup_offset->wal_cleanup_start_pos) {

      cleanup_offset->wal_cleanup_start_pos = start_segment_ptr;

      result = true;

    }

  }

  return result;

}

void Retention::move(vector<shared_ptr<BaseBackupDescr>> &target,
                     vector<shared_ptr<BaseBackupDescr>> source,
                     shared_ptr<BaseBackupDescr> bbdescr,
                     unsigned int index) {

  /* Nothing to do if source list is empty. */
  if (source.size() == 0)
    return;

  /*
   * Index must be within bounds.
   */
  if (index >= source.size()) {
    ostringstream oss;

    oss << "cannot move basebackup to list: index("
        << index
        << ") out of bounds (exceeds size "
        << source.size()
        << ")";

    throw CArchiveIssue(oss.str());
  }

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "DEBUG: moving index(" << index << ") to target list";
#endif

  target.push_back(bbdescr);

}

/* *****************************************************************************
 * CountRetention implementation
 * ****************************************************************************/

CountRetention::CountRetention() {}

CountRetention::CountRetention(CountRetention &src) {

  catalog = src.getBackupCatalog();
  archiveDescr = src.getArchiveCatalogDescr();


}

CountRetention::CountRetention(unsigned int count,
                               std::shared_ptr<CatalogDescr> archiveDescr,
                               std::shared_ptr<BackupCatalog> catalog)
  : Retention(archiveDescr, catalog) {

  this->count = count;

}

CountRetention::CountRetention(std::shared_ptr<RetentionRuleDescr> rule)
  : Retention(rule) {

  /*
   * We accept a RETENTION_KEEP_NUM or RETENTION_DROP_NUM
   * only
   */
  if (rule->type != RETENTION_KEEP_NUM
      && rule->type != RETENTION_DROP_NUM) {
    throw CCatalogIssue("invalid rule type for count retention policy");
  }

  this->setValue(CPGBackupCtlBase::strToInt(rule->value));

}

CountRetention::~CountRetention() {}

void CountRetention::reset() {

  Retention::reset();
  this->count = -1;

}

void CountRetention::setValue(int count) {

  if (count < 0) {
    ostringstream oss;

    oss << "invalid value \"" << count << "\" for retention count";
    throw CCatalogIssue(oss.str());

  }

  this->count = count;

}

string CountRetention::asString() {

  ostringstream encode;

  switch(this->ruleType) {

  case RETENTION_KEEP_NUM:
    encode << "KEEP ";
    break;

  case RETENTION_DROP_NUM:
    encode << "DROP ";
    break;

  default:
    throw CCatalogIssue("unrecognized rule type for count retention");
  }

  encode << "+" << this->count;
  return encode.str();

}

void CountRetention::setRetentionRuleType(const RetentionRuleId ruleType) {

  switch(ruleType) {
  case RETENTION_KEEP_NUM:
  case RETENTION_DROP_NUM:
    this->ruleType = ruleType;
    break;
  default:
    {
      ostringstream oss;

      oss  << "count retention policy is incompatible with type id " << ruleType;
      throw CCatalogIssue(oss.str());
    }

    break;
  }

}

unsigned int CountRetention::apply(std::vector<std::shared_ptr<BaseBackupDescr>> list) {

  unsigned int result = 0;

  /*
   * List must be a valid vector. Zero counted
   * lists are a no-op.
   */
  if (list.size() == 0) {
    return result;
  }

  /*
   * There's no sense in traversing the basebackup list
   * iff there are less basebackups present than the count
   * retention wants to have. So save the work and abort
   * at this point;
   */
  if (list.size() < (unsigned int)this->count)
    return result;

  /*
   * We need a valid cleanup descriptor. If not already
   * present, throw.
   */
  if (this->cleanupDescr == nullptr) {
    throw CArchiveIssue("cannot apply retention rule without initialization: call init() before");
  }

  /*
   * Loop through the list, but stop as soon as we reached the retention count.
   *
   * Since we support KEEPing or DROPing <count> basebackups, we need
   * to be careful how we interpret <count>:
   *
   * In case of RETENTION_KEEP_NUM, we count backwards from the newest basebackup
   * ignoring any live basebackups until we've reached <count>. All basebackups
   * older than the first <count> basebackups are cleaned up then.
   *
   * In case of RETENTION_DROP_NUM, we count from the oldest basebackups *forward*,
   * and stop as soon we've reached <count>. All basebackups encountered up to
   * <count> basebackups are removed then.
   *
   * NOTE: Pinned basebackups don't count! We skip "in progress" backups as well as
   *       aborted. Only valid basebackups are considered.
   */
  if (this->ruleType == RETENTION_KEEP_NUM)

    result = this->keep_num(list);

  else if (this->ruleType == RETENTION_DROP_NUM)

    result = this->drop_num(list);

  else {

    std::ostringstream oss;

    oss << "unsupported retention policy rule type: \"" << this->ruleType << "\"";
    throw CCatalogIssue(oss.str());

  }

  return result;

}

unsigned int CountRetention::drop_num(std::vector<std::shared_ptr<BaseBackupDescr>> &list) {

  unsigned int currindex       = 0;
  unsigned int bbhealthy_num   = 0; /* number of valid basebackups */
  std::vector<std::shared_ptr<BaseBackupDescr>>::reverse_iterator it;

  /*
   * We must know before starting how many healthy basebackups
   * are held in the catalog, otherwise we aren't allowed to apply
   * the DROP retention to this archive.
   *
   * This means we scan the list of basebackup descriptors
   * twice, but given that in most cases this list won't
   * have zillions of entries that looks okay.
   */
  for (auto bbdescr : list) {

    /*
     * We don't distinguish between valid and invalid basebackups,
     * pinned is pinned...
     *
     * XXX: This situation is unlikely anyways, since parts of the
     *      implemention for PIN prevents pinning invalid basebackups...
     */
    if (locked(bbdescr) != NOT_LOCKED) {

      continue;

    } else {

      bbhealthy_num++;

    }

  }

  /*
   * Shortcut here, if number of healthy backups is
   * smaller than the requested retention count, exit immediately.
   */
  if (bbhealthy_num <= (unsigned int) this->count) {
    ostringstream failure;

    failure << "retention count violates current number of valid basebackups: "
            << "\""
            << bbhealthy_num
            << "\"";

    throw CRetentionFailureHint(failure.str(),
                                "retention count must be smaller than the number of valid basebackups");

  }

  /*
   * Start again at the end of the list, move <count> basebackups
   * upwards until we've reached the requested threshold.
   *
   * This way the caller has a chance to check whether the retention
   * could be successfully applied.
   *
   * Since we traverse the list from the oldest to the newest
   * basebackup, we examine the XLOG cleanup offset just
   * once, when we reached the end of the retention count.
   * Otherwise XLogCleanupOffsetKeep() won't work, since it only
   * allows us to adjust the cleanup offset backwards.
   *
   * NOTE: The caller should make sure that the number
   *       of basebackups in the candidates list satisfies
   *       the requested retention count.
   */

  for (it = list.rbegin(); it != list.rend(); ++it) {

    std::shared_ptr<BaseBackupDescr> bbdescr = *it;
    XLogRecPtr cleanup_recptr = InvalidXLogRecPtr;

    /*
     * Check if this basebackup descriptor is valid.
     * Ignore pinned, aborted or basebackups "in-progress".
     */
    if (locked(bbdescr) != NOT_LOCKED) {
      continue;
    }

    /*
     * Be paranoid:
     *
     * This is an additional check whether we would
     * violate the number of valid basebackups currently
     * in the archive by deleting the current basebackup.
     *
     * Check if moving the current basebackup into
     * the deletion candidates list will exceed number
     * of valid basebackups. If true, abort.
     */
    if ((currindex + 1) >= bbhealthy_num) {

      ostringstream failure;

      failure << "retention count violates current number of valid basebackups: "
              << "\""
              << bbhealthy_num
              << "\"";

      throw CRetentionFailureHint(failure.str(),
                                  "retention count must be smaller than the number of valid basebackups");

    }

    /*
     * Move the basebackups into deletion list.
     */
    this->move(this->cleanupDescr->basebackups,
               list,
               bbdescr,
               currindex);

    /* Increase count */
    currindex++;

    /*
     * Since count is signed, we need to make sure we compare
     * the right range of values. I think this is safe here, since
     * an overflow isn't likely to occur within this workflow
     */
    if (currindex >= (unsigned int)this->count) {

      /*
       * Identify the XLogRecPtr where the cleanup thresholds starts.
       */
      cleanup_recptr = PGStream::XLOGPrevSegmentStartPosition(PGStream::decodeXLOGPos(bbdescr->xlogpos),
                                                              bbdescr->wal_segment_size);

      Retention::XLogCleanupOffsetKeep(cleanupDescr,
                                       cleanup_recptr,
                                       bbdescr->timeline,
                                       bbdescr->wal_segment_size);

      break;

    }

  }

  return currindex;

}

unsigned int CountRetention::keep_num(std::vector<std::shared_ptr<BaseBackupDescr>> &list) {

  unsigned int bbcounted = 0;
  unsigned int currindex = 0;
  std::vector<std::shared_ptr<BaseBackupDescr>>::iterator it;

  /*
   * Start at the top of the list, traversing each basebackup
   * down to the oldest. We stop as soon as we reached <count>
   * basebackups to keep. We can't just elect a start from the
   * given retention count, since there might be basebackups to skip
   * (e.g. backups in  progress).
   *
   * If the list ends *before* we can satisfy the retention policy,
   * we abort.
   * This way the caller has a chance to check whether the retention
   * could be successfully applied.
   */
  for (it = list.begin(); it != list.end(); ++it) {

    std::shared_ptr<BaseBackupDescr> bbdescr = *it;
    XLogRecPtr cleanup_recptr = InvalidXLogRecPtr;

    /*
     * Fall through until we reach requested retention count.
     * All following basebackups need to be dropped then.
     *
     * We skip pinned and basebackups with state "in progress".
     * Aborted basebackups aren't considered either.
     */
    if (locked(bbdescr) != NOT_LOCKED) {

      /*
       * Identify the XLogRecPtr where the cleanup thresholds starts.
       *
       * We must move them backwards to the previous segment before
       * the current basebackups started, otherwise we are going
       * to remove them.
       */
      cleanup_recptr = PGStream::XLOGPrevSegmentStartPosition(PGStream::decodeXLOGPos(bbdescr->xlogpos),
                                                              bbdescr->wal_segment_size);

      Retention::XLogCleanupOffsetKeep(cleanupDescr,
                                       cleanup_recptr,
                                       bbdescr->timeline,
                                       bbdescr->wal_segment_size);

      /* And next iteration, since this basebackups needs to be kept. */
      continue;

    }

    /* Increase counter and check whether we have reached desired
     * retention count. If still not there, keep the basebackup in any case. */
    bbcounted++;

    if (bbcounted <= (unsigned int) this->count) {

      /*
       * Identify the XLogRecPtr where the cleanup thresholds starts.
       *
       * We must move them backwards to the previous segment before
       * the current basebackups started, otherwise we are going
       * to remove them.
       */
      cleanup_recptr = PGStream::XLOGPrevSegmentStartPosition(PGStream::decodeXLOGPos(bbdescr->xlogpos),
                                                              bbdescr->wal_segment_size);

      Retention::XLogCleanupOffsetKeep(cleanupDescr,
                                       cleanup_recptr,
                                       bbdescr->timeline,
                                       bbdescr->wal_segment_size);

      continue;
    }

    /*
     * This is a match, so mark the basebackup to be deleted. We set the
     * cleanup XLogRecPtr threshold to the end of this basebackup.
     *
     * The XLogRecPtr cleanup threshold should already be positioned
     * the last XLOG segment we have to keep, so just move
     * the basebackup descriptor into the deletion list.
     *
     * NOTE: We must not reuse bbcounted here, the move list
     *       (aka list of deletion candidates) uses its own
     *       index here, starting at zero.
     */

    this->move(this->cleanupDescr->basebackups,
               list,
               bbdescr,
               currindex);
    currindex++;

  }

  return bbcounted;

}

void CountRetention::init() {

  /*
   * Initialize the cleanup descriptor. Currently we just support
   * deleting by a starting XLogRecPtr and removing all subsequent (older)
   * WAL files from the archive.
   */
  this->cleanupDescr                  = make_shared<BackupCleanupDescr>();
  this->cleanupDescr->mode            = WAL_CLEANUP_OFFSET;
  this->cleanupDescr->basebackupMode  = BASEBACKUP_DELETE;

}

void CountRetention::init(std::shared_ptr<BackupCleanupDescr> prevCleanupDescr) {

  /*
   * We don't expect an initialized cleanupDescr here, this usually
   * means we were called previously already. Throw here, since
   * we can't guarantee reasonable results out from here in this case.
   */
  if (this->cleanupDescr != nullptr)
    throw CArchiveIssue("cannot apply retention module repeatedly, "
                        "call Retention::reset() before");

  this->cleanupDescr = prevCleanupDescr;

}

/* *****************************************************************************
 * CleanupRetention implementation
 * ****************************************************************************/

CleanupRetention::CleanupRetention() {}

CleanupRetention::CleanupRetention(std::shared_ptr<CatalogDescr> archiveDescr,
                                   std::shared_ptr<BackupCatalog> catalog)
  : Retention(archiveDescr, catalog) {

  this->ruleType = RETENTION_CLEANUP;

}

CleanupRetention::CleanupRetention(std::shared_ptr<RetentionRuleDescr> rule)
  : Retention(rule) {

  /*
   * Take care for correct rule types. We only accept
   * cleanup Policies here.
   */
  if ( rule->type != RETENTION_CLEANUP ) {
    throw CCatalogIssue("cleanup retention can only be instantiated with a CLEANUP policy");
  }

  this->ruleType = rule->type;

  /*
   * Currently, value of a RETENTION_CLEANUP policy is just a dummy
   * (e.g. always set to "cleanup"). But we might need to change that
   * some day, so copy over the specific value from the given
   * rule descriptor in any case.
   */
  this->cleanup_value = rule->value;

}

CleanupRetention::CleanupRetention(CleanupRetention &src) {

  catalog = src.getBackupCatalog();
  archiveDescr = src.getArchiveCatalogDescr();

}

void CleanupRetention::setRetentionRuleType(const RetentionRuleId ruleType) {

  if (ruleType != RETENTION_CLEANUP)
    throw CCatalogIssue("cleanup retention policy handles RETENTION_CLEANUP rules only");

  this->ruleType = ruleType;

}

unsigned int CleanupRetention::apply(std::vector<std::shared_ptr<BaseBackupDescr>> list) {

  unsigned int currindex = 0;
  unsigned int result = 0;

  /*
   * List must be a valid vector. If it is empty,
   * this is effectively a no-op.
   */
  if (list.size() == 0) {
    return 0;
  }

  /*
   * Also, we need a valid cleanup descriptor, which
   * at least requires to have called init() before...
   */

  if (this->cleanupDescr == nullptr) {
    throw CArchiveIssue("cannot apply retention rule without initialization: call init() before");
  }

  /*
   * Loop through the list of basebackups which need
   * to be cleaned up.
   *
   * "Cleaning" up here means, we look out for basebackups which are in
   * either one of the following states:
   *
   * aborted : The basebackups streaming process was interrupted and finished
   *           before succeeding to stream all necessary files.
   *
   * invalid: the basebackups doesn't have alle required WALs available and
   *          don't include all required WALs (WAL=EXCLUDED).
   *
   */
  for (auto &bbdescr : list) {

    /*
     * XLogRecPtr stores the current XLOG cleanup threshold.
     */
    XLogRecPtr cleanup_recptr = InvalidXLogRecPtr;
    BackupLockInfoType lockType = NOT_LOCKED;

    /*
     * Check if the basebackup is pinned. If true, we are forced
     * to keep it, even if it is aborted. But if the latter is the case,
     * print a warning.
     */
    if ((lockType = locked(bbdescr)) == LOCKED_BY_PIN) {

      BOOST_LOG_TRIVIAL(info) << "ignoring PINNED basebackup " << bbdescr->fsentry;

    }

    /* check if the basebackup was aborted */
    if (bbdescr->status == BaseBackupDescr::BASEBACKUP_STATUS_ABORTED) {

      /*
       * Schedule this basebackup for removal, but only if
       * it's not pinned. In case of the latter, print a warning
       * but keep the basebackup
       */
      if (lockType == LOCKED_BY_PIN) {

        BOOST_LOG_TRIVIAL(warning)
          << "ABORTED basebackup "
          << bbdescr->fsentry
          << " is pinned but WAL will be removed";

        /*
         * Since an ABORTED basebackup doesn't have a trustable xlogposend location,
         * we move the cleanup_xlogrecptr to the starting position of its XLOG stream.
         *
         * This will render the basebackup really useless, but pinned is pinned....
         */
        cleanup_recptr = PGStream::decodeXLOGPos(bbdescr->xlogpos);
        Retention::XLogCleanupOffsetKeep(cleanupDescr,
                                         cleanup_recptr,
                                         bbdescr->timeline,
                                         bbdescr->wal_segment_size);

      } else {

        /* Schedule basebackup for removal. */
        this->move(this->cleanupDescr->basebackups,
                   list,
                   bbdescr,
                   currindex);

        result++;

      }

    }

    if (bbdescr->status == BaseBackupDescr::BASEBACKUP_STATUS_IN_PROGRESS) {

      /*
       * Okay, this is hard, we need to know whether any other
       * streaming worker is currently responsible for this
       * basebackup. Since we can't reliable do this here, we print
       * a WARNING and skip this basebackup for now.
       *
       * The current workaround is to give the user a HINT that
       * he's required to check this basebackup manually, or, if
       * necessary, need to drop it.
       *
       * Also, a basebackup IN PROGRESS doesn't have a xlogposend XLogRecPtr
       * which can be trusted. Since we cannot make sure that this is a
       * orphaned basebackup atm, we will abort the retention policy.
       *
       * It's too dangerous to proceed, since there's the possibility
       * to remove XLOG files that belongs to an unfinished basebackup.
       */
      throw CRetentionFailureHint("abort cleanup retention, since a basebackup is still in progress",
                                  "if this basebackup is broken somehow, you'll need to cleanup it manually");

    }

    currindex++;

  }

  return result;

}

std::string CleanupRetention::asString() {

  return std::string("CLEANUP");

}

void CleanupRetention::init() {

  /*
   * Initialize the cleanup descriptor. Currently we just support
   * deleting by a starting XLogRecPtr and removing all subsequent (older)
   * WAL files from the archive.
   */
  this->cleanupDescr                  = make_shared<BackupCleanupDescr>();
  this->cleanupDescr->mode            = WAL_CLEANUP_OFFSET;
  this->cleanupDescr->basebackupMode  = BASEBACKUP_DELETE;

}

void CleanupRetention::init(std::shared_ptr<BackupCleanupDescr> prevCleanupDescr) {

  /*
   * We don't expect an initialized cleanupDescr here, this usually
   * means we were called previously already. Throw here, since
   * we can't guarantee reasonable results out from here in this case.
   */
  if (this->cleanupDescr != nullptr)
    throw CArchiveIssue("cannot apply retention module repeatedly, "
                        "call Retention::reset() before");

  this->cleanupDescr = prevCleanupDescr;

}

/* *****************************************************************************
 * DateTimeRetention implementation
 * ****************************************************************************/

DateTimeRetention::DateTimeRetention() {}

DateTimeRetention::DateTimeRetention(DateTimeRetention &src) {

  catalog = src.getBackupCatalog();
  archiveDescr = src.getArchiveCatalogDescr();

  this->setIntervalExpr(src.getInterval());

}

DateTimeRetention::DateTimeRetention(std::shared_ptr<RetentionRuleDescr> descr)
  : Retention(descr) {

  if ( (descr->type != RETENTION_KEEP_NEWER_BY_DATETIME)
       && (descr->type != RETENTION_KEEP_OLDER_BY_DATETIME)
       && (descr->type != RETENTION_DROP_OLDER_BY_DATETIME)
       && (descr->type != RETENTION_DROP_NEWER_BY_DATETIME) ) {

    throw ("datetime retention rule can only be created with { KEEP | DROP } { NEWER | OLDER } THAN");

  }

  this->setIntervalExpr(descr->value);

}

DateTimeRetention::DateTimeRetention(std::string datetime_expr,
                                     std::shared_ptr<CatalogDescr> archiveDescr,
                                     std::shared_ptr<BackupCatalog> catalog)
  : Retention(archiveDescr, catalog) {

  this->setIntervalExpr(datetime_expr);

}

DateTimeRetention::~DateTimeRetention() {}

void DateTimeRetention::setIntervalExpr(std::string value) {

#ifdef __DEBUG__
  BOOST_LOG_TRIVIAL(debug) << "interval expr " << value;
#endif

  interval.push(value);

}

void DateTimeRetention::init() {

  /*
   * Initialize the cleanup descriptor. Currently we just
   * support by a starting XLogRecPtr and removing all subsequent (older)
   * WAL files from the archive.
   */
  this->cleanupDescr                 = make_shared<BackupCleanupDescr>();
  this->cleanupDescr->mode           = WAL_CLEANUP_OFFSET;
  this->cleanupDescr->basebackupMode = BASEBACKUP_DELETE;

}

void DateTimeRetention::init(std::shared_ptr<BackupCleanupDescr> prevCleanupDescr) {

  /*
   * We don't expect an initialized cleanupDescr here, this usually
   * means we were called previously already. Throw here, since
   * we can't guarantee reasonable results out from here in this case.
   */
  if (this->cleanupDescr != nullptr)
    throw CArchiveIssue("cannot apply retention module repeatedly, "
                        "call Retention::reset() before");

  this->cleanupDescr = prevCleanupDescr;


}

unsigned int DateTimeRetention::apply(std::vector<std::shared_ptr<BaseBackupDescr>> list) {

  unsigned int currindex = 0;

  /*
   * Loop through the list of basebackups. We need to check
   * whether the stopped timestamp exceeds the specified datetime
   * threshold. We true, move the basebackup into the deletion candidates
   * list, but only if it is not pinned.
   */
  for (auto &bbdescr : list) {

    XLogRecPtr cleanup_recptr = InvalidXLogRecPtr;

    /*
     * Get locking state of this basebackup.
     *
     * Please note that the lock is just checked here for
     * concurrent basebackup streams. We don't apply any
     * lock ourselves here.
     *
     * This might be racy in case a basebackup stream wants
     * to use a basebackup which is concurrently deleted. In the
     * normale case though the client will get an error that either
     * the basebackup disappeared or something during the initialization
     * went wrong.
     */
    BackupLockInfoType lockType = locked(bbdescr);

    /* Check whether retention policy is exceeded */
    this->catalog->exceedsRetention(bbdescr,
                                    this->ruleType,
                                    this->interval);

    if (bbdescr->exceeds_retention_rule) {


      /* Check whether this basebackup is "in-progress" */
      if (bbdescr->status == BaseBackupDescr::BASEBACKUP_STATUS_IN_PROGRESS) {

        BOOST_LOG_TRIVIAL(warning) << "basebackup can be deleted, but is in-progress, ignoring";

        cleanup_recptr = PGStream::XLOGPrevSegmentStartPosition(PGStream::decodeXLOGPos(bbdescr->xlogpos),
                                                                bbdescr->wal_segment_size);
        Retention::XLogCleanupOffsetKeep(cleanupDescr,
                                         cleanup_recptr,
                                         bbdescr->timeline,
                                         bbdescr->wal_segment_size);

      } else {

        if ( (lockType == LOCKED_BY_PIN)
             || (lockType == LOCKED_BY_SHM) ) {

          BOOST_LOG_TRIVIAL(info) << "basebackup is pinned or concurrently in use, ignoring";

          cleanup_recptr = PGStream::XLOGPrevSegmentStartPosition(PGStream::decodeXLOGPos(bbdescr->xlogpos),
                                                                  bbdescr->wal_segment_size);
          Retention::XLogCleanupOffsetKeep(cleanupDescr,
                                           cleanup_recptr,
                                           bbdescr->timeline,
                                           bbdescr->wal_segment_size);

        } else {

          this->move(this->cleanupDescr->basebackups,
                     list,
                     bbdescr,
                     currindex);

          /* Deletion offset is end xlogrecptr of this basebackup */
          cleanup_recptr = PGStream::decodeXLOGPos(bbdescr->xlogposend);
          Retention::XLogCleanupOffsetKeep(cleanupDescr,
                                           cleanup_recptr,
                                           bbdescr->timeline,
                                           bbdescr->wal_segment_size);

          currindex++;

#ifdef __DEBUG__
          BOOST_LOG_TRIVIAL(info) << "DEBUG: basebackup can be deleted, retention rule applies";
#endif

        }

      }
    } else {

#ifdef __DEBUG__
      BOOST_LOG_TRIVIAL(debug) << "DEBUG: keep basebackup ID " << bbdescr->id;
#endif

      /*
       * Keep this basebackup in any ways, since it does not
       * exceed retention
       */
      cleanup_recptr = PGStream::XLOGPrevSegmentStartPosition(PGStream::decodeXLOGPos(bbdescr->xlogpos),
                                                              bbdescr->wal_segment_size);
      Retention::XLogCleanupOffsetKeep(cleanupDescr,
                                       cleanup_recptr,
                                       bbdescr->timeline,
                                       bbdescr->wal_segment_size);
    }

  }

  return currindex;

}

std::string DateTimeRetention::asString() {

  ostringstream result;

  if (this->ruleType == RETENTION_KEEP_NEWER_BY_DATETIME) {
    result << "KEEP NEWER THAN " << this->interval.getOperandsAsString();
  }

  if (this->ruleType == RETENTION_KEEP_OLDER_BY_DATETIME) {
    result << "KEEP OLDER THAN " << this->interval.getOperandsAsString();
  }

  if (this->ruleType == RETENTION_DROP_NEWER_BY_DATETIME) {
    result << "DROP NEWER THAN " << this->interval.getOperandsAsString();
  }

  if (this->ruleType == RETENTION_DROP_OLDER_BY_DATETIME) {
    result << "DROP OLDER THAN " << this->interval.getOperandsAsString();
  }

  return result.str();
}

std::string DateTimeRetention::getInterval() {

  return this->interval.compile();

}

void DateTimeRetention::setRetentionRuleType(const RetentionRuleId ruleType) {

  /*
   * Allowed value here are:
   *
   * RETENTION_KEEP_NEWER_BY_DATETIME
   * RETENTION_DROP_NEWER_BY_DATETIME
   * RETENTION_DROP_OLDER_BY_DATETIME
   * RETENTION_KEEP_OLDER_BY DATETIME
   *
   * Others will throw
   */
  switch(ruleType) {
  case RETENTION_KEEP_NEWER_BY_DATETIME:
  case RETENTION_KEEP_OLDER_BY_DATETIME:
  case RETENTION_DROP_NEWER_BY_DATETIME:
  case RETENTION_DROP_OLDER_BY_DATETIME:
    this->ruleType = ruleType;
    break;
  default:
    {
      ostringstream oss;

      oss << "label retention policy is incompatible with rule type id " << ruleType;
      throw CCatalogIssue(oss.str());
    }

    break;
  }

}

/* *****************************************************************************
 * LabelRetention implementation
 * ****************************************************************************/

LabelRetention::LabelRetention() {}

LabelRetention::LabelRetention(LabelRetention &src) {

  catalog = src.getBackupCatalog();
  archiveDescr = src.getArchiveCatalogDescr();
  label_filter = src.getRegularExpr();

}

LabelRetention::LabelRetention(std::shared_ptr<RetentionRuleDescr> descr)
  : Retention(descr) {

  if ( (descr->type != RETENTION_KEEP_WITH_LABEL)
       && (descr->type != RETENTION_DROP_WITH_LABEL) )
    throw CCatalogIssue("label retention rule can only be created with KEEP or DROP WITH LABEL");

  this->setRegularExpr(descr->value);

}

LabelRetention::LabelRetention(std::string regex_str,
                               std::shared_ptr<CatalogDescr> archiveDescr,
                               std::shared_ptr<BackupCatalog> catalog) : Retention(archiveDescr, catalog) {

  this->setRegularExpr(regex_str);

}

LabelRetention::~LabelRetention() {}

string LabelRetention::asString() {

  ostringstream oss;

  if (this->ruleType == RETENTION_KEEP_WITH_LABEL) {

    oss << "KEEP WITH LABEL ";

  } else {

    /* since label retention only understoods RETENTION_KEEP_WITH_LABEL
     * or RETENTION_DROP_WITH_LABEL, there's no other choice here. */

    oss << "DROP WITH LABEL ";

  }

  oss << this->label_filter.str();
  return oss.str();

}

void LabelRetention::init(shared_ptr<BackupCleanupDescr> prevCleanupDescr) {

  /*
   * We don't expect an initialized cleanupDescr here, this usually
   * means we were called previously already. Throw here, since
   * we can't guarantee reasonable results out from here in this case.
   */
  if (this->cleanupDescr != nullptr)
    throw CArchiveIssue("cannot apply retention module repeatedly, "
                        "call Retention::reset() before");

  this->cleanupDescr = prevCleanupDescr;

}

void LabelRetention::init() {

  /*
   * Initialize the cleanup descriptor. Currently we just support
   * deleting by a starting XLogRecPtr and removing all subsequent (older)
   * WAL files from the archive.
   */
  this->cleanupDescr                  = make_shared<BackupCleanupDescr>();
  this->cleanupDescr->mode            = WAL_CLEANUP_OFFSET;
  this->cleanupDescr->basebackupMode  = BASEBACKUP_DELETE;

}

unsigned int LabelRetention::apply(vector<shared_ptr<BaseBackupDescr>> deleteList) {

  unsigned int currindex = 0;
  unsigned int result = 0;
  vector<shared_ptr<BaseBackupDescr>> keep;

  /*
   * Nothing to do if list of basebackups is empty.
   */
  if (deleteList.size() == 0)
    return 0;

  /*
   * Make sure init() was called before.
   */
  if (this->cleanupDescr == nullptr) {
    throw CArchiveIssue("cannot apply retention rule without initialization: call init() before");
  }

  /*
   * Loop through the list of basebackups, filtering out every basebackup
   * that matches the backup label identified by the basebackup descriptor.
   *
   * The cleanup descriptor will get any basebackup descriptors which should
   * be deleted, thus the cleanup mode *must* be BASEBACKUP_DELETE here. Depending
   * on the action (RETENTION_KEEP_WITH_LABEL or RETENTION_DROP_WITH_LABEL) we
   * must keep the matches or not. If a match is instructed for being kept,
   * we simply don't push it into the cleanup descriptor.
   */
  for(auto &bbdescr : deleteList) {

    bool modified_xlog_cleanup_offset = false;
    boost::smatch what;

    /*
     * Get the start position of the current basebackup and extract
     * its XLogRecPtr (xlogpos), which describes the starting segment
     * required for this basebackup. Then calculate the previous segment, since
     * this is the one where the cleanup offsets starts.
     *
     * We must be careful for a basebackup in progress, so treat this as a
     * normal basebackup we are required to keep.
     *
     * An aborted basebackup does have a starting xlogpos but in normal
     * cases no xlogposend. In case a basebackup is aborted, we include its
     * starting XLOG segment and avoid to use the previous one.
     */
    XLogRecPtr bbxlogrecptr_start = InvalidXLogRecPtr;

    if (bbdescr->status == BaseBackupDescr::BASEBACKUP_STATUS_ABORTED) {

      bbxlogrecptr_start = PGStream::decodeXLOGPos(bbdescr->xlogpos);

    } else if ((bbdescr->status == BaseBackupDescr::BASEBACKUP_STATUS_READY)
               || (bbdescr->status == BaseBackupDescr::BASEBACKUP_STATUS_IN_PROGRESS)) {

      bbxlogrecptr_start = PGStream::XLOGPrevSegmentStartPosition(PGStream::decodeXLOGPos(bbdescr->xlogpos),
                                                                  bbdescr->wal_segment_size);

    }

    /*
     * Apply regex to label.
     *
     * The outcoming action here depends on the RetentionRuleId
     * flag set within ruleType. This tells us whether we want to keep
     * (RETENTION_KEEP_WITH_LABEL) or drop (RETENTION_DROP_WITH_LABEL)
     * the scanned basebackups.
     */
    if (regex_match(bbdescr->label, what, this->label_filter)) {

      /*
       * This is a match.
       *
       * The basebackup needs either being dropped or kept, depending
       * on the current retention action.
       */
      switch(this->ruleType) {

      case RETENTION_KEEP_WITH_LABEL:
        {
          /*
           * KEEP basebackup.
           */

          /*
           * Move the XLogRecPtr backwards to make sure we keep XLOG segments
           * belonging to this basebackup.
           */
          modified_xlog_cleanup_offset = Retention::XLogCleanupOffsetKeep(cleanupDescr,
                                                                          bbxlogrecptr_start,
                                                                          bbdescr->timeline,
                                                                          bbdescr->wal_segment_size);

          break;
        }

      case RETENTION_DROP_WITH_LABEL:
        {
          /*
           * DROP basebackup.
           *
           * The current regex matches should be dropped.
           *
           * We need to check for any PINs before moving the
           * basebackup descriptor into the cleanup descriptor.
           *
           * If we detect a PIN, we keep this backup. The same
           * applies to a basebackup currently "in progress"...
           */
          if (locked(bbdescr) != NOT_LOCKED) {

            BOOST_LOG_TRIVIAL(info) << "keeping basebackup \""
                                    << bbdescr->fsentry
                                    << "\"";

            /*
             * A drop rule with a match, but this basebackup is either
             * in progress or pinned, so make sure we keep its
             * XLOG segments.
             *
             */
            modified_xlog_cleanup_offset = Retention::XLogCleanupOffsetKeep(cleanupDescr,
                                                                            bbxlogrecptr_start,
                                                                            bbdescr->timeline,
                                                                            bbdescr->wal_segment_size);


          } else {

            BOOST_LOG_TRIVIAL(info) << "selected basebackup \""
                                    << bbdescr->fsentry
                                    << "\" for deletion";

            this->move(this->cleanupDescr->basebackups,
                       deleteList,
                       bbdescr,
                       currindex);
            result++;

          }

          break;
        }

      default:

        /* ouch, this RETENTION action is not supported here */
        throw CArchiveIssue("label retention rule cannot be applied to specified retention action");

      } /* switch...case */

    } else {

      /*
       * No match. Depending on the RETENTION action specified within
       * ruleType, we need to keep or drop the current basebackup. The
       * actions here are:
       *
       * - RETENTION_DROP_WITH_LABEL: No match here, so keep it, which basically
       *                               means no special action here.
       *
       * - RETENTION_KEEP_WITH_LABEL: No match here, but since we only should
       *                              keep matches, the basebackup should be deleted.
       *                              We must take care for pinned basebackups, though,
       *                              which should be kept.
       */
      switch(this->ruleType) {

      case RETENTION_DROP_WITH_LABEL:
        {
          /*
           * Since there's no match, keep this basebackup. Make sure we
           * position the cleanup XLogRecPtr right before this basebackup has
           * started.
           */
          modified_xlog_cleanup_offset = Retention::XLogCleanupOffsetKeep(cleanupDescr,
                                                                          bbxlogrecptr_start,
                                                                          bbdescr->timeline,
                                                                          bbdescr->wal_segment_size);

          break;
        }

      case RETENTION_KEEP_WITH_LABEL:
        {

          /*
           * No match, but we are told to keep matches. So push
           * this basebackup into the cleanupdescr if not pinned.
           */
          if (locked(bbdescr) != NOT_LOCKED) {

            BOOST_LOG_TRIVIAL(info) << "keeping pinned basebackup \""
                                    << bbdescr->fsentry
                                    << "\"";

            /*
             * This is a KEEP rule with no match, but either pinned or
             * in progress, so make sure we keep its XLOG segments.
             */
            modified_xlog_cleanup_offset = Retention::XLogCleanupOffsetKeep(cleanupDescr,
                                                                            bbxlogrecptr_start,
                                                                            bbdescr->timeline,
                                                                            bbdescr->wal_segment_size);


          } else {

            BOOST_LOG_TRIVIAL(info) << "selected basebackup \""
                                    << bbdescr->fsentry
                                    << "\" for deletion";

            this->move(this->cleanupDescr->basebackups,
                       deleteList,
                       bbdescr,
                       currindex);
            result++;

          }

          break;
        }


      default:
        throw CArchiveIssue("label retention rule cannot be applied to specified retention action");
      } /* switch...case */

    } /* if regex_match() */

#ifdef __DEBUG_XLOG__
    if (modified_xlog_cleanup_offset) {
      BOOST_LOG_TRIVIAL(debug) << "modified XLOG cleanup offset: recptr = "
                               << PGStream::encodeXLOGPos(bbxlogrecptr_start);
    }
#endif

    /* Increase current position counter */
    currindex++;

  } /* for ... loop */


  return result;

}

void LabelRetention::setRegularExpr(string regex_str) {

  if (regex_str.length() > 0) {
    this->label_filter = boost::regex(regex_str);
  } else {
    throw CCatalogIssue("zero-length regular expression for label retention detected");
  }

}

boost::regex LabelRetention::getRegularExpr() {
  return this->label_filter;
}

void LabelRetention::setRetentionRuleType(const RetentionRuleId ruleType) {

  switch(ruleType) {
  case RETENTION_KEEP_WITH_LABEL:
  case RETENTION_DROP_WITH_LABEL:
    this->ruleType = ruleType;
    break;
  default:
    {
      ostringstream oss;

      oss << "label retention policy is incompatible with rule type id " << ruleType;
      throw CCatalogIssue(oss.str());
    }

    break;
  }
}

/* *****************************************************************************
 * PinRetention implementation
 * ****************************************************************************/

PinRetention::PinRetention(BasicPinDescr *descr,
                           std::shared_ptr<CatalogDescr> archiveDescr,
                           std::shared_ptr<BackupCatalog> catalog)
  : Retention(archiveDescr, catalog) {

  /* throw in case the descr is invalid */
  if (descr == nullptr) {
    throw CArchiveIssue("pin descriptor is not initialized");
  }

  if (descr->getOperationType() == ACTION_UNDEFINED) {
    throw CArchiveIssue("cannot apply retention with an undefined pin operation");
  }

  this->pinDescr = descr;
}

PinRetention::~PinRetention() {}

void PinRetention::reset() {

  this->count_pin_context.performed = 0;
  this->count_pin_context.count = 0;
  this->count_pin_context.expected = 0;

}

void PinRetention::init(shared_ptr<BackupCleanupDescr> prevCleanupDescr) {}
void PinRetention::init() {}

unsigned int PinRetention::action_Pinned(vector<shared_ptr<BaseBackupDescr>> &list) {

  /*
   * NOTE: pin statistics are set in count_pin_context, too!
   */

  /*
   * Stack IDs for basebackups to operate on.
   */
  vector <int> basebackupIds;

  /*
   * Currently, action_Pinned() implements the PINNED action
   * which can only be specified via an UNPIN command. Thus, we
   * protect action_Pinned() from being called within
   * a PIN_BASEBACKUP command context.
   */
  if (this->pinDescr->action() == PIN_BASEBACKUP)
    throw CArchiveIssue("cannot use CURRENT with a PIN command");

  this->count_pin_context.expected = list.size();

  /*
   * So this is a UNPIN CURRENT command, which means that
   * we are instructed to unpin all currently pinned basebackups.
   *
   * The algorithm is simple: Loop through the list of
   * BaseBackupDescr, examing each whether it its pinned or not.
   * If true, stack it into the basebackupIds vector and
   * call the unpin action.
   */
  for(auto &bbdescr : list) {

    /*
     * Don't operate on uninitialized basebackup descriptors.
     */
    if (bbdescr->id < 0) {
      continue;
    }

    /*
     * Here we don't care in which state a basebackup is. Since
     * we only operate on UNPIN actions, we want to make
     * sure that any accidently pinned basebackup can be unpinned.
     */
    if (bbdescr->pinned != 0) {
      basebackupIds.push_back(bbdescr->id);
      this->count_pin_context.performed++;
    }
  }

  /* proceed, execute the batch operation for the
   * list of affected basebackupIds.
   *
   * This might throw.
   */
  this->performDatabaseAction(basebackupIds);

  /*
   * ... and we're done.
   */
  return this->count_pin_context.performed;

}

unsigned int PinRetention::action_NewestOrOldest(vector<shared_ptr<BaseBackupDescr>> &list) {

  /*
   * Depending on the pin operation type encoded into
   * the PinDescr instance, we either need to get the
   * oldest or newest basebackup from the specified list.
   *
   * We expect the list of basebackup descriptors being
   * sorted by their started timestamp, in descending order
   * (newest first). So action_NewestOrOldest() just fetches
   * the head or end of the list.
   *
   * The trivia here is to find a *valid* newest or oldest,
   * depending on the state of the basebackup.
   */

  /* Here we have just one backup ID to operate on, but the
   * API always expects a batch list*/
  unsigned int currindex = 0;
  vector<int> basebackupIds;
  PinOperationType pinOper = this->pinDescr->getOperationType();

  /*
   * Loop through the list. Depending on the PinOperationType
   * we got from the PinDescr, we choose the first one (either
   * from the back of the list or from the head).
   */
  if (pinOper == ACTION_NEWEST) {

    for(currindex = 0; currindex < list.size(); currindex++) {

      /* fetch descriptor */
      shared_ptr<BaseBackupDescr> bbdescr = list[currindex];

      /* check if initialized */
      if (bbdescr->id < 0) {
        continue;
      }

      /* verify status */
      if(bbdescr->status != BaseBackupDescr::BASEBACKUP_STATUS_READY) {
        continue;
      }

      /* Verify on-disk representation */
      if (StreamingBaseBackupDirectory::verify(bbdescr) != BASEBACKUP_OK) {
        continue;
      }

      /*
       * Okay, this is the newest, push it into our
       * batch list and exit the loop.
       */
      basebackupIds.push_back(bbdescr->id);
      this->count_pin_context.performed++;
      break;

    }

  } else if (pinOper == ACTION_OLDEST) {

    for (auto bbdescr : boost::adaptors::reverse(list)) {

      /* check if initialized */
      if (bbdescr->id < 0) {
        continue;
      }

      /* verify status */
      if(bbdescr->status != BaseBackupDescr::BASEBACKUP_STATUS_READY) {
        continue;
      }

      /* Verify on-disk representation */
      if (StreamingBaseBackupDirectory::verify(bbdescr) != BASEBACKUP_OK) {
        continue;
      }

      /*
       * Okay, this is the oldest, push it into our
       * batch list and exit the loop.
       */
      basebackupIds.push_back(bbdescr->id);
      this->count_pin_context.performed++;
      break;

    }

  } else {
    throw CArchiveIssue("unsupported pin operation for action NEWEST or OLDEST");
  }

  /*
   * Execute the database batch operation.
   */
  this->performDatabaseAction(basebackupIds);

  return this->count_pin_context.performed;
}

unsigned int PinRetention::action_ID(vector<shared_ptr<BaseBackupDescr>> &list) {

  /*
   * List might contain multiple backupIds, we support
   * to operate on each item directly.
   *
   * Loop through the list and blindly do the pin/unpin
   * operation.
   */
  vector<int> basebackupIds;

  for(auto &bbdescr : list) {

    /* if uninitialized, ignore the basebackup descriptor */
    if (bbdescr->id < 0) {
      continue;
    }

    /*
     * Don't perform pin operations on invalid basebackup IDs.
     */
    if (bbdescr->status != BaseBackupDescr::BASEBACKUP_STATUS_READY) {
      continue;
    }

    /*
     * If the basebackup does not exist on disk, don't pin it.
     */
    if (StreamingBaseBackupDirectory::verify(bbdescr) != BASEBACKUP_OK) {
      continue;
    }

    /*
     * Stack the basebackup ID into our batch list.
     */
    basebackupIds.push_back(bbdescr->id);
    this->count_pin_context.performed++;
  }

  /* Perform the database operation pin/unpin */
  this->performDatabaseAction(basebackupIds);

  return this->count_pin_context.performed;
}

unsigned int PinRetention::action_Count(vector<shared_ptr<BaseBackupDescr>> &list) {

  /*
   * NOTE: counting statistics are used in count_pin_context!
   */

  /*
   * Stack the basebackup IDs suitable for pin/unpin
   * into their own list. This list is then passed down
   * to the BackupCatalog and filesystem machinery.
   */
  vector<int> basebackupIds;

  /*
   * The algorithm used here is simple:
   *
   * Loop through the list of BaseBackupDescr and count
   * each one suitable for a pin (or unpin). If already pinned or
   * unpinned, do an increment nevertheless. Stop as soon as we have
   * reach the count threshold or the end of the list.
   */
  for (auto &bbitem : list) {

    /*
     * Don't operate on uninitialized basebackup descriptors.
     */
    if (bbitem->id < 0) {
      continue;
    }

    /*
     * Is this backup ready? If not, step over, it will
     * never be pinned.
     */
    if (bbitem->status != BaseBackupDescr::BASEBACKUP_STATUS_READY) {
      continue;
    }

    /*
     * Basebackup available on disk? If not, don't pin it.
     */
    if (StreamingBaseBackupDirectory::verify(bbitem) != BASEBACKUP_OK)
      continue;

    basebackupIds.push_back(bbitem->id);
    this->count_pin_context.performed++;

    /*
     * If we have reached the number of counts, exit.
     */
    if (this->count_pin_context.performed >= this->count_pin_context.count)
      break;

  }

  /* may throw ... */
  this->performDatabaseAction(basebackupIds);

  /* ... and we're done */
  return this->count_pin_context.performed;
}

unsigned int PinRetention::pinsPerformed() {

  return this->count_pin_context.performed;

}

void PinRetention::performDatabaseAction(vector<int> &basebackupIds) {

  /*
   * Shortcut, this is a no-op if the list
   * is empty.
   */
  if (basebackupIds.size() == 0) {
    return;
  }

  /*
   * Now execute the catalog UPDATE.
   */
  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  this->catalog->performPinAction(this->pinDescr, basebackupIds);

}

unsigned int PinRetention::dispatchPinAction(vector<shared_ptr<BaseBackupDescr>> &list) {

  PinOperationType policy;
  unsigned int result = 0;

  /*
   * Determine retention policy to perform. The rule we have
   * to use is encoded into our pinDescr instance.
   */
  policy = this->pinDescr->getOperationType();

  /*
   * Now dispatch the policy to its encoded action.
   */
  switch(policy) {
  case ACTION_ID:
    {
      this->count_pin_context.count = 0;
      this->count_pin_context.performed = 0;
      this->count_pin_context.expected = list.size();

      /* Execute */
      result = this->action_ID(list);
      break;
    }
  case ACTION_COUNT:
    {
      this->count_pin_context.count = this->pinDescr->getCount();
      this->count_pin_context.performed = 0;
      this->count_pin_context.expected = this->pinDescr->getCount();

      /* Execute */
      result = this->action_Count(list);

      break;
    }
  case ACTION_NEWEST:
  case ACTION_OLDEST:
    {
      this->count_pin_context.count = list.size();
      this->count_pin_context.performed = 0;
      this->count_pin_context.expected = 1;

      /* Execute */
      result = this->action_NewestOrOldest(list);

      break;
    }
  case ACTION_PINNED:
    {
      this->count_pin_context.count = list.size();
      this->count_pin_context.performed = 0;
      this->count_pin_context.expected = list.size();

      /* Execute */
      result = this->action_Pinned(list);

      break;
    }
  default:
    throw CArchiveIssue("pin retention policy is undefined");
    break;
  }

  return result;
}

unsigned int PinRetention::apply(vector<shared_ptr<BaseBackupDescr>> list) {

  int result = 0;

  if (this->pinDescr == nullptr) {
    /* we can't do anything without a valid pin descriptor */
    throw CArchiveIssue("cannot dispatch pin action with an uninitialized pin descriptor");
  }

  /*
   * Shortcut, we don't need anything to do in case
   * list is empty
   */
  if (list.size() == 0) {
    return result;
  }

  result = this->dispatchPinAction(list);

  return result;
}

void PinRetention::setRetentionRuleType(const RetentionRuleId ruleType) {

  switch(ruleType) {
  case RETENTION_PIN:
  case RETENTION_UNPIN:
    this->ruleType = ruleType;
    break;
  default:
    {
      ostringstream oss;

      oss << "pin/unpin retention policy can't support rule type " << ruleType;
      throw CCatalogIssue(oss.str());
    }

  }

}

string PinRetention::asString() {

  PinOperationType policy = this->pinDescr->getOperationType();
  ostringstream oss;

  switch(policy) {
  case ACTION_ID:
    oss << this->pinDescr->getBackupID();
    break;
  case ACTION_COUNT:
    oss << "+" << this->pinDescr->getCount();
    break;
  case ACTION_NEWEST:
    oss << "NEWEST";
    break;
  case ACTION_OLDEST:
    oss << "OLDEST";
    break;
  case ACTION_PINNED:
    oss << "PINNED";
    break;
  default:
    throw CCatalogIssue("could not parse pin/unpin action into string");
  }

  return oss.str();
}
