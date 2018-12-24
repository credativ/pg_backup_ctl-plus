#include <retention.hxx>
#include <boost/pointer_cast.hpp>
#include <boost/range/adaptor/reversed.hpp>

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


    /* fall through, since the following are not implemented yet */
  case RETENTION_KEEP_NUM:
  case RETENTION_DROP_NUM:

  case RETENTION_KEEP_BY_DATETIME:
  case RETENTION_DROP_BY_DATETIME:
    throw CCatalogIssue("retention policy not implemented yet");

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

      /* make sure, ruleDescr is valid */
      if ((ruleDescr != nullptr) && (ruleDescr->id >= 0)
          && (ruleDescr->type != RETENTION_NO_RULE)) {

        switch(ruleDescr->type) {

        case RETENTION_KEEP_WITH_LABEL:
        case RETENTION_DROP_WITH_LABEL:
          {
            shared_ptr<Retention> retentionPtr = make_shared<LabelRetention>(ruleDescr->value,
                                                                             archiveDescr,
                                                                             catalog);
            retentionPtr->setRetentionRuleType(ruleDescr->type);

            /*
             * Stick the rule into our result vector.
             */
            result.push_back(retentionPtr);

            break;
          }

        case RETENTION_KEEP_NUM:
        case RETENTION_DROP_NUM:
          throw CCatalogIssue("retention policy not implemented yet");
          break;
        case RETENTION_KEEP_BY_DATETIME:
        case RETENTION_DROP_BY_DATETIME:
          {
            shared_ptr<Retention> retentionPtr = make_shared<DateTimeRetention>(ruleDescr->value,
                                                                                archiveDescr,
                                                                                catalog);
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
   * Check wether the current timeline is already initialized.
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
       * This is unexpected. We have checked wether the timeline
       * already has a cleanup offset attached. If we're landing here,
       * this means something went terribly wrong.
       */
      throw CArchiveIssue("could not attach new timeline cleanup offset");

    }

#ifdef __DEBUG__XLOG__
    cerr << "DEBUG: insert new tli/offset "
         << timeline
         << "/"
         << cleanup_offset->wal_cleanup_start_pos
         << endl;
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
  cerr << "DEBUG: moving index(" << index << ") to target list" << endl;
#endif

  target.push_back(bbdescr);

}

/* *****************************************************************************
 * DateTimeRetention implementation
 * ****************************************************************************/

DateTimeRetention::DateTimeRetention() {}

DateTimeRetention::DateTimeRetention(std::shared_ptr<RetentionRuleDescr> descr)
  : Retention(descr) {

}

DateTimeRetention::DateTimeRetention(std::string datetime_expr,
                                     std::shared_ptr<CatalogDescr> archiveDescr,
                                     std::shared_ptr<BackupCatalog> catalog)
  : Retention(archiveDescr, catalog) {

}

DateTimeRetention::~DateTimeRetention() {}

void DateTimeRetention::init() {}

void DateTimeRetention::init(std::shared_ptr<BackupCleanupDescr> cleanupDescr) {}

unsigned int DateTimeRetention::apply(std::vector<std::shared_ptr<BaseBackupDescr>> list) {}

std::string DateTimeRetention::asString() {}

void DateTimeRetention::setRetentionRuleType(const RetentionRuleId ruleType) {}

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
    throw ("label retention rule can only be created with KEEP or DROP WITH LABEL");

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

    boost::smatch what;
    bool modified_xlog_cleanup_offset = false;

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
     * flag set within ruleType. This tells us wether we want to keep
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
          if ( (bbdescr->pinned)
               || (bbdescr->status == BaseBackupDescr::BASEBACKUP_STATUS_IN_PROGRESS) ) {

            cout << "INFO: keeping basebackup \""
                 << bbdescr->fsentry
                 << "\""
                 << endl;

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

            cout << "INFO: selected basebackup \""
                 << bbdescr->fsentry
                 << "\" for deletion"
                 << endl;

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
          if ( (bbdescr->pinned)
               || (bbdescr->status == BaseBackupDescr::BASEBACKUP_STATUS_IN_PROGRESS) ) {

            cout << "INFO: keeping pinned basebackup \""
                 << bbdescr->fsentry
                 << "\""
                 << endl;

            /*
             * This is a KEEP rule with no match, but either pinned or
             * in progress, so make sure we keep its XLOG segments.
             */
            modified_xlog_cleanup_offset = Retention::XLogCleanupOffsetKeep(cleanupDescr,
                                                                            bbxlogrecptr_start,
                                                                            bbdescr->timeline,
                                                                            bbdescr->wal_segment_size);


          } else {

            cout << "INFO: selected basebackup \""
                 << bbdescr->fsentry
                 << "\" for deletion"
                 << endl;

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
      cerr << "modified XLOG cleanup offset: recptr = "
           << PGStream::encodeXLOGPos(bbxlogrecptr_start)
           << endl;
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
   * BaseBackupDescr, examing each wether it its pinned or not.
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
