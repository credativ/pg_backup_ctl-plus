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

void Retention::setArchiveCatalogDescr(std::shared_ptr<CatalogDescr> archiveDescr) {

  if (archiveDescr->id < 0)
    throw CCatalogIssue("could not assign invalid archive ID to retention policy");

  this->archiveDescr = archiveDescr;

}

void Retention::setCatalog(std::shared_ptr<BackupCatalog> catalog) {

  this->catalog = catalog;

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
          {
            shared_ptr<Retention> retentionPtr = make_shared<LabelRetention>(ruleDescr->value,
                                                                             archiveDescr,
                                                                             catalog);
            result.push_back(retentionPtr);
            break;
          }
        case RETENTION_DROP_WITH_LABEL:
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

        } /* switch..case */
      }
    }
  }

  return result;
}

void Retention::keep(vector<shared_ptr<BaseBackupDescr>> &keepList,
                     vector<shared_ptr<BaseBackupDescr>> &dropList,
                     shared_ptr<BaseBackupDescr> bbdescr,
                     unsigned int index) {

  /*
   * Varous sanity checks follow.
   */

  /* Nothing to do if dropList ist empty. */
  if (dropList.size() == 0)
    return;

  /*
   * Index must be within bounds of dropList.
   */
  if (index >= dropList.size())
    throw CArchiveIssue("cannot move basebackup to KEEP list: index out of bounds (exceeds size)");

  keepList.push_back(bbdescr);
  dropList.erase(dropList.begin() + index);

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

unsigned int LabelRetention::apply(vector<shared_ptr<BaseBackupDescr>> deleteList) {

  unsigned int currindex = 0;
  unsigned int result = 0;
  vector<shared_ptr<BaseBackupDescr>> keep;

  /*
   * We don't expect an initialized cleanupDescr here, this usually
   * means we were called previously already. Throw here, since
   * we can't guarantuee reasonable results out from here.
   */
  if (this->cleanupDescr != nullptr)
    throw CArchiveIssue("cannot apply retention module repeatedly, call Retention::reset() before");

  /*
   * Nothing to do if list of basebackups is empty.
   */
  if (deleteList.size() == 0)
    return 0;

  /*
   * Initialize the cleanup descriptor. Currently we just support
   * deleting by a starting XLogRecPtr and removing all subsequent (older)
   * WAL files from the archive.
   */
  this->cleanupDescr                  = make_shared<BackupCleanupDescr>();
  this->cleanupDescr->mode            = WAL_CLEANUP_OFFSET;
  this->cleanupDescr->basebackupMode = BASEBACKUP_KEEP;

  /*
   * Loop through the list of basebackups, filtering out every basebackup
   * that matches the backup label identified by the basebackup descriptor.
   * Stick the descriptor used to identify the basebackup to keep into the
   * cleanup descriptor.
   *
   * The basebackups we want to keep are attached to the "keep list", which
   * will be used to identify the deletion XLOG Redo pointer, identifying the
   * first XLOG segment file to be deleted.
   */
  for(auto &bbdescr : deleteList) {

    /*StreamingBaseBackupDirectory dir(path(bbdescr->fsentry).filename().string(),
      this->archiveDescr->directory); */
    boost::smatch what;

    /* Increase current position counter */
    currindex++;

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
           *
           * The current regex label match should be kept.
           *
           * This means we need to move the current basebackups from
           * the deleteList into the list of basebackups to keep.
           *
           * There's no need to check PINs and such here, since we are forced
           * to keep this basebackup anyways.
           */
          this->keep(this->cleanupDescr->basebackups,
                     deleteList,
                     bbdescr,
                     currindex);

          break;
        }

      case RETENTION_DROP_WITH_LABEL:
        {
          /*
           * DROP basebackup.
           *
           * The current regex matches should be dropped.
           *
           * We need to check for any PINs before leaving the
           * basebackup descriptor in the delete list. If we detect a PIN,
           * we keep this backup.
           */
          if (bbdescr->pinned) {

            cout << "INFO: keeping pinned basebackup \""
                 << bbdescr->fsentry
                 << "\""
                 << endl;

            this->keep(this->cleanupDescr->basebackups,
                       deleteList,
                       bbdescr,
                       currindex);
          } else {

            cout << "INFO: selected basebackup \""
                 << bbdescr->fsentry
                 << "\" for deletion"
                 << endl;

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
       * - RETENTION_DROP_WITH_LABEL: No match here, so keep it, which means
       *                              to move the basebackup descriptor into the keep list.
       *
       * - RETENTION_KEEP_WITH_LABEL: No match here, but since we only should
       *                              keep matches, leave the basebackup descriptor in
       *                              the delete list.
       */
      switch(this->ruleType) {

      case RETENTION_DROP_WITH_LABEL:
        {

          /*
           * No need to check for PINs, since no match and we are forced
           * to keep this basebackup though.
           */
          this->keep(this->cleanupDescr->basebackups,
                     deleteList,
                     bbdescr,
                     currindex);
        }

      case RETENTION_KEEP_WITH_LABEL:
        {

          /*
           * No match, but we are told to keep matches. So leave this
           * basebackup within the deleteList, but only if it's *not*
           * pinned.
           */
          if (bbdescr->pinned) {

            cout << "INFO: keeping pinned basebackup \""
                 << bbdescr->fsentry
                 << "\""
                 << endl;

            this->keep(this->cleanupDescr->basebackups,
                       deleteList,
                       bbdescr,
                       currindex);
          } else {

            cout << "INFO: selected basebackup \""
                 << bbdescr->fsentry
                 << "\" for deletion"
                 << endl;

            result++;

          }

        }


      default:
        throw CArchiveIssue("label retention rule cannot be applied to specified retention action");
      } /* switch...case */

    } /* if regex_match() */

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

      /* basebackup directory handle for verification */
      StreamingBaseBackupDirectory dir(path(bbdescr->fsentry).filename().string(),
                                       this->archiveDescr->directory);

      /* check if initialized */
      if (bbdescr->id < 0) {
        continue;
      }

      /* verify status */
      if(bbdescr->status != BaseBackupDescr::BASEBACKUP_STATUS_READY) {
        continue;
      }

      /* Verify on-disk representation */
      if (dir.verify_basebackup(bbdescr) != BASEBACKUP_OK) {
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

      /* basebackup directory handle for verification */
      StreamingBaseBackupDirectory dir(path(bbdescr->fsentry).filename().string(),
                                       this->archiveDescr->directory);

      /* check if initialized */
      if (bbdescr->id < 0) {
        continue;
      }

      /* verify status */
      if(bbdescr->status != BaseBackupDescr::BASEBACKUP_STATUS_READY) {
        continue;
      }

      /* Verify on-disk representation */
      if (dir.verify_basebackup(bbdescr) != BASEBACKUP_OK) {
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

    StreamingBaseBackupDirectory dir(path(bbdescr->fsentry).filename().string(),
                                     this->archiveDescr->directory);

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
    if (dir.verify_basebackup(bbdescr) != BASEBACKUP_OK) {
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

    StreamingBaseBackupDirectory dir(path(bbitem->fsentry).filename().string(),
                                     this->archiveDescr->directory);

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
    if (dir.verify_basebackup(bbitem) != BASEBACKUP_OK)
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
