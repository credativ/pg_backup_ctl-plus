#include <boost/format.hpp>
#include <commands.hxx>
#include <daemon.hxx>
#include <stream.hxx>
#include <fs-pipe.hxx>
#include <shm.hxx>

using namespace credativ;

BaseCatalogCommand::~BaseCatalogCommand() {}

void BaseCatalogCommand::copy(CatalogDescr& source) {

  this->tag = source.tag;
  this->id  = source.id;
  this->archive_name = source.archive_name;
  this->label        = source.label;
  this->compression  = source.compression;
  this->directory    = source.directory;
  this->check_connection = source.check_connection;
  this->forceXLOGPosRestart = source.forceXLOGPosRestart;

  /*
   * Connection properties
   */
  this->coninfo->type         = source.coninfo->type;
  this->coninfo->pghost       = source.coninfo->pghost;
  this->coninfo->pgport       = source.coninfo->pgport;
  this->coninfo->pguser       = source.coninfo->pguser;
  this->coninfo->pgdatabase   = source.coninfo->pgdatabase;
  this->coninfo->dsn          = source.coninfo->dsn;
  this->backup_profile = source.getBackupProfileDescr();

  /*
   * Job control properties
   */
  this->detach = source.detach;
  this->execString = source.execString;

  this->setAffectedAttributes(source.getAffectedAttributes());
  this->coninfo->setAffectedAttributes(source.coninfo->getAffectedAttributes());

}

void BaseCatalogCommand::assignSigStopHandler(JobSignalHandler *handler) {

  /*
   * Setting a nullptr to our internal sig stop handler
   * is treated as an general error.
   */
  if (handler == nullptr) {
    throw CPGBackupCtlFailure("attempt to assign uninitialized stop signal handler");
  }

  this->stopHandler = handler;
}

void BaseCatalogCommand::assignSigIntHandler(JobSignalHandler *handler) {

  /*
   * Setting a nullptr to our internal sig int handler
   * is treated as an general error.
   */
  if (handler == nullptr) {
    throw CPGBackupCtlFailure("attempt to assign uninitialized stop signal handler");
  }

  this->intHandler = handler;
}

ShowWorkersCommandHandle::ShowWorkersCommandHandle(std::shared_ptr<BackupCatalog> catalog) {
  this->setCommandTag(tag);
  this->catalog = catalog;
}

ShowWorkersCommandHandle::ShowWorkersCommandHandle(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

ShowWorkersCommandHandle::ShowWorkersCommandHandle(){}

ShowWorkersCommandHandle::~ShowWorkersCommandHandle() {}

void ShowWorkersCommandHandle::execute(bool noop) {

  /*
   * To get the list of all currently running workers
   * in catalog, we need to attach to the shared memory
   * area for background workers.
   *
   * We fetch all occupied worker shared memory
   * slots into a local vector in one step to avoid
   * doing catalog lookups while holding the shared memory
   * lock.
   */
  WorkerSHM shm;
  vector<shm_worker_area> slots_used;

  shm.attach(this->catalog->fullname(), true);

  shm.lock();
  for (unsigned int i = 0; i < shm.getMaxWorkers(); i++) {

    try {

      shm_worker_area worker = shm.read(i);

      if (worker.pid > 0) {

        slots_used.push_back(worker);

      }

    } catch(SHMFailure &e) {
      /* in any case, unlock() the SHM and re-throw. */
      shm.unlock();
      throw e;
    }

  }

  shm.unlock();
  shm.detach();

  /*
   * Now it's time to print the contents. We can do
   * whatever we want now, since no critical locks
   * are held.
   */
  for(auto &worker : slots_used) {

    string archive_name = "N/A";

    if (worker.archive_id >= 0) {
      shared_ptr<CatalogDescr> archive_descr = this->catalog->existsById(worker.archive_id);

      if (archive_descr->id >= 0) {
        archive_name = archive_descr->archive_name;
      }
    }

    cout << "WORKER PID " << worker.pid
         << " | executing " << CatalogDescr::commandTagName(worker.cmdType)
         << " | archive name " << archive_name
         << " | archive ID " << worker.archive_id
         << " | started " << CPGBackupCtlBase::ptime_to_str(worker.started)
         << endl;
  }
}

ExecCommandCatalogCommand::ExecCommandCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

ExecCommandCatalogCommand::ExecCommandCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->setCommandTag(tag);
  this->catalog = catalog;
}

void ExecCommandCatalogCommand::execute(bool flag) {

  job_info jobDescr;
  char buf_byte;
  pid_t pid;

  jobDescr.background_exec = true;
  jobDescr.use_pipe = true;
  jobDescr.executable = path(this->execString);

  jobDescr.close_std_fd = false;

  pid = run_process(jobDescr);

  if (pid < (pid_t)0) {
    cerr << "could not execute command" << endl;
    return;
  }

  while (::read(jobDescr.pipe_out[0], &buf_byte, 1) > 0) {
    cout << buf_byte;
  }

}

DropConnectionCatalogCommand::DropConnectionCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

DropConnectionCatalogCommand::DropConnectionCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->setCommandTag(tag);
  this->catalog = catalog;
}

void
DropConnectionCatalogCommand::execute(bool flag) {

  if (this->catalog == nullptr) {
    throw CArchiveIssue("could not execute drop command: no catalog");
  }

  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  /*
   * Start transaction, this will lock the
   * database during this operation.
   */
  this->catalog->startTransaction();

  try {

    std::shared_ptr<CatalogDescr> tempDescr;
    string contype;

    /*
     * Checkout if our archive_name really exists.
     */
    tempDescr = this->catalog->existsByName(this->archive_name);

    /*
     * BackupCatalog::existsByName() returns an empty
     * catalog descriptor.
     */
    if (tempDescr != nullptr && tempDescr->id >= 0) {

      /*
       * Archive exists, drop its streaming connection, but
       * check if the requested connection really exists.
       *
       * XXX: the parser has initialized our
       *      connection descr handle accordingly.
       */

      /* ... we're interested in archive_id only. */
      this->setArchiveId(tempDescr->id);
      this->coninfo->pushAffectedAttribute(SQL_CON_ARCHIVE_ID_ATTNO);

      /* save the connection type for error reporting later */
      contype = this->coninfo->type;

      this->catalog->getCatalogConnection(this->coninfo,
                                          this->id,
                                          this->coninfo->type);

      if (this->coninfo->archive_id < 0) {
        throw CCatalogIssue("archive \""
                            + this->archive_name
                            + "\" does not have a connection of type \""
                            + contype
                            +"\"");
      }

      this->catalog->dropCatalogConnection(this->archive_name,
                                           this->coninfo->type);

    } else {
      /* requested archive name is unknown */
      throw CCatalogIssue("archive \"" + this->archive_name + "\" does not exist");
    }

  } catch (CPGBackupCtlFailure& e) {
    this->catalog->rollbackTransaction();

    /* re-throw exception to caller */
    throw e;
  }

  /* ... and we're done */
  this->catalog->commitTransaction();
}

ListConnectionCatalogCommand::ListConnectionCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

ListConnectionCatalogCommand::ListConnectionCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->setCommandTag(tag);
  this->catalog = catalog;
}

void ListConnectionCatalogCommand::execute(bool flag) {

  if (this->catalog == nullptr) {
    throw CArchiveIssue("could not execute list command: no catalog");
  }

  /*
   * Prepare catalog, start transaction ...
   */
  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  this->catalog->startTransaction();

  try {

    std::shared_ptr<CatalogDescr> tempDescr;
    std::vector<std::shared_ptr<ConnectionDescr>> connections;

    /*
     * Check if specified archive is valid.
     */
    tempDescr = this->catalog->existsByName(this->archive_name);

    /*
     * Normally we don't get a nullptr back from
     * existsByName() ...
     */
    if (tempDescr != nullptr
        && tempDescr->id >= 0) {

      /*
       * Archive exists and existsByName() has initialized
       * our temporary descriptor with its archive_id we need to
       * retrieve associated catalog database connections.
       */
      connections = this->catalog->getCatalogConnection(tempDescr->id);

      /*
       * Print result header
       */
      cout << "List of connections for archive \""
           << this->archive_name
           << "\""
           << endl;

      /*
       * XXX: createArchive() normally ensures that a
       *      catalog connection definition of type 'basebackup'
       *      (CONNECTION_TYPE_BASEBACKUP) exists, at least. But
       *      we don't rely on this fact, just loop through
       *      the results and spill them out...
       *
       *      getCatalogConnection() returns the shared pointers
       *      ordered by its type.
       */
      for(auto & con : connections) {

        /* item header */
        cout << CPGBackupCtlBase::makeHeader("connection type " + con->type,
                                             boost::format("%-15s\t%-60s") % "Attribute" % "Setting",
                                             80);
        cout << boost::format("%-15s\t%-60s") % "DSN" % con->dsn << endl;
        cout << boost::format("%-15s\t%-60s") % "PGHOST" % con->pghost << endl;
        cout << boost::format("%-15s\t%-60s") % "PGDATABASE" % con->pgdatabase << endl;
        cout << boost::format("%-15s\t%-60s") % "PGUSER" % con->pguser << endl;
        cout << boost::format("%-15s\t%-60s") % "PGPORT" % con->pgport << endl;
      }
    } else {

      ostringstream oss;
      oss << "archive \"" << this->archive_name << "\" does not exist";
      throw CCatalogIssue(oss.str());

    }

  } catch(CPGBackupCtlFailure& e) {

    this->catalog->rollbackTransaction();

    /* re-throw to caller */
    throw e;

  }

  /* ... and we're done */
  this->catalog->commitTransaction();
}

CreateConnectionCatalogCommand::CreateConnectionCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->setCommandTag(CREATE_CONNECTION);
  this->catalog = catalog;
}

CreateConnectionCatalogCommand::CreateConnectionCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

void CreateConnectionCatalogCommand::execute(bool flag) {

  if (this->catalog == nullptr) {
    throw CArchiveIssue("could not execute create connection command: no catalog");
  }

  /*
   * Prepare catalog, start transaction...
   */
  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  this->catalog->startTransaction();

  try {

    /*
     * Check if the requested archive really exists.
     */
    std::shared_ptr<CatalogDescr> tempArchiveDescr = nullptr;
    shared_ptr<ConnectionDescr> tempConDescr = std::make_shared<ConnectionDescr>();

    tempArchiveDescr = this->catalog->existsByName(this->archive_name);

    if (tempArchiveDescr->id < 0) {
      std::ostringstream oss;
      /* Requested archive doesn't exist */
      oss << "archive \""
          << this->archive_name
          << "\" does not exist";
      throw CCatalogIssue(oss.str());
    }

    /*
     * Archive exists, proceed.
     *
     * We need to take care to assign the archive_id to our
     * own object instance, since the API currently doesn't
     * support direct archive_name lookups during connection
     * creation. This also makes sure, that the current command
     * has a valid archive identification.
     */
    this->setArchiveId(tempArchiveDescr->id);

    /*
     * Check, if there is already a connection type colliding
     * with the new one.
     */
    tempConDescr->pushAffectedAttribute(SQL_CON_ARCHIVE_ID_ATTNO);
    tempConDescr->pushAffectedAttribute(SQL_CON_TYPE_ATTNO);
    this->catalog->getCatalogConnection(tempConDescr,
                                        tempArchiveDescr->id,
                                        this->coninfo->type);

    if (tempConDescr->archive_id >= 0
        && tempConDescr->type != ConnectionDescr::CONNECTION_TYPE_UNKNOWN) {

      /*
       * This connection type already has a definition
       * for the requested archive, error out.
       */
      std::ostringstream oss;
      oss << "archive \""
          << this->archive_name
          << "\" already has a connection of this type configured"
          << endl;
      throw CCatalogIssue(oss.str());
    }

    /*
     * Everything looks good; create the new connection.
     */
    this->catalog->createCatalogConnection(this->coninfo);

    /* ... and we're done, commit transaction */
    this->catalog->commitTransaction();

  } catch(CPGBackupCtlFailure& e) {

    this->catalog->rollbackTransaction();

    /* re-raise */
    throw e;

  }
}

StartLauncherCatalogCommand::StartLauncherCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->tag = START_LAUNCHER;
  this->catalog = catalog;
}

StartLauncherCatalogCommand::StartLauncherCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

void StartLauncherCatalogCommand::execute(bool flag) {

  std::shared_ptr<CatalogProc> procInfo = nullptr;
  job_info job_info;
  pid_t pid;
  shmatt_t nattach;

  if (this->catalog == nullptr) {
    throw CArchiveIssue("could not execute catalog command: no catalog");
  }

  /*
   * First check the catalog if a launcher is already running. This is
   * done with the following steps:
   *
   * - Retrieve the catalog proc entry for the launcher
   *   If a launcher is active, there's an entry for it (or if it
   *   has crashed earlier).
   *
   * - Retrieve the SHMID and check wether the launcher is already
   *   attached.
   *
   *   A catalog entry would also be present in case a launcher has crashed.
   *   In that case the catalog proc entry is orphaned and we immediately
   *   see that by checking LauncherSHM::getNumberOfAttached(), which is
   *   expected to always zero in that case!
   *
   * - In case we don't find a catalog proc entry we don't have the
   *   SHMID to check for. This usually means the launcher is shut down
   *   and the catalog proc entry was removed (or there was never one before).
   *
   *   If someone tries, he could just delete the catalog proc entry
   *   underneath of a running launcher. If that happens, we're out ...
   */
  procInfo = this->catalog->getProc(-1, CatalogProc::PROC_TYPE_LAUNCHER);
  nattach = 0;

  try {
    if (procInfo != nullptr && procInfo->pid > 0) {

      /*
       * If we get a SHMFailure exception here, then the shmid
       * stored in the catalog doesn't exist anymore. In this case
       * just go further and start the launcher.
       */
      nattach = LauncherSHM::getNumberOfAttached(procInfo->shm_id);

    }
  } catch(SHMFailure &e) {

    /*
     * A SHMFailure here usually means the request shmid
     * doesn't exist or isn't readable anymore. This
     * indicates a crashed launcher instance, since the
     * catalog entry is orphaned.
     */
    nattach = 0;

    cerr << "WARNING: catalog shm id " << procInfo->shm_id << " is orphaned" << endl;
  }

  if (nattach > 0) {
    throw CCatalogIssue("Launcher for this catalog instance already running");
  }

  /*
   * Detach from current interactive terminal, if requested (default).
   */
  job_info.detach = this->detach;

  /*
   * Close standard file descriptors (STDOUT, STDIN, STDERR, ...), otherwise
   * background process will clobber our interactive terminal.
   */
  job_info.close_std_fd = false;

  /*
   * Assign catalog descriptor handle to the job info.
   */
  job_info.cmdHandle = std::make_shared<BackgroundWorkerCommandHandle>(this->catalog);

  /*
   * Finally launch the background worker.
   */
  pid = launch(job_info);

  if (pid > 0) {

    cout << "background launcher launched at pid " << pid << endl;

      /*
       * Send a test message.
       */
    /*
     * Establish message queue, of not yet there.
     */
    establish_launcher_cmd_queue(job_info);
    cout << "sending test message to message queue...";
    //    send_launcher_cmd(job_info, "status update force");
    cout << "done" << endl;

  } else {
    /*
     * This code shouldn't be reached, since the forked child process
     * should take care to exit in their child code after executing
     * the background action.
     */
    exit(0);
  }
}

StopStreamingForArchiveCommandHandle::StopStreamingForArchiveCommandHandle(std::shared_ptr<BackupCatalog> catalog) {
  this->tag = STOP_STREAMING_FOR_ARCHIVE;
  this->catalog = catalog;
}

StopStreamingForArchiveCommandHandle::StopStreamingForArchiveCommandHandle(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

StopStreamingForArchiveCommandHandle::StopStreamingForArchiveCommandHandle() {}

StopStreamingForArchiveCommandHandle::~StopStreamingForArchiveCommandHandle() {}

void StopStreamingForArchiveCommandHandle::execute(bool noop) {

  /*
   * Get the archive id for the specified archive name.
   * Delay any operation on the worker shared memory
   * area until we're done with our database work.
   */
  shared_ptr<CatalogDescr> temp_descr = this->catalog->existsByName(this->archive_name);

  /*
   * Archive worker PID we got from SHM, 0 if not found.
   */
  pid_t archive_pid = -1;

  /*
   * Shared memory handle.
   */
  WorkerSHM shmhandle;

  if (temp_descr->id < 0) {
    throw CArchiveIssue("archive " + this->archive_name + "does not exist");
  }

  shmhandle.attach(this->catalog->fullname(), false);

  /*
   * Operation on the worker shared memory is done
   * under a lock. We need to scan the shared memory
   * area for the specified archive id. We do this in one
   * go without any other interaction to keep the critical
   * section as short as possible.
   *
   * Another thing to note: Don't send a signal right
   * out of the loop to the victim process, since it would have to
   * wait for our critical section anyways. The launcher will try
   * clean up the worker SHM slot index right away.
   */
  shmhandle.lock();

  try {

    shm_worker_area worker_info;

    for (unsigned int i = 0; i < shmhandle.getMaxWorkers(); i++) {

      worker_info = shmhandle.read(i);

      if (worker_info.archive_id == temp_descr->id) {

        /* Matching archive ID, store it away and exit loop */
        archive_pid = worker_info.pid;
        break;

      }

    }

  } catch(SHMFailure &shme) {
    /* if something goes wrong here, make sure
     * we detach from SHM and unlock */
    shmhandle.unlock();
    shmhandle.detach();

    throw CArchiveIssue(shme.what());
  }

  shmhandle.unlock();
  shmhandle.detach();

  if (archive_pid > 0) {
    ::kill(archive_pid, SIGTERM);
    cout << "NOTICE: terminated worker pid " << archive_pid << " for archive " << temp_descr->archive_name << endl;
  } else {
    throw CArchiveIssue("no streaming worker for archive " + temp_descr->archive_name + " found");
  }

}

StartStreamingForArchiveCommand::StartStreamingForArchiveCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->tag = START_STREAMING_FOR_ARCHIVE;
  this->catalog = catalog;
}

StartStreamingForArchiveCommand::StartStreamingForArchiveCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

StartStreamingForArchiveCommand::~StartStreamingForArchiveCommand() {

  if (this->pgstream != nullptr) {

    if (this->pgstream->connected())
      this->pgstream->disconnect();

    delete this->pgstream;
  }

}

void StartStreamingForArchiveCommand::prepareStream() {

  /*
   * Calling BackupCatalog::getStreams() gets us a list
   * of currently registered streams. We are specifically
   * looking for an existing ConnectionDescr::CONNECTION_TYPE_STREAMER
   * stream handle.
   */
  std::shared_ptr<StreamIdentification> myStream = nullptr;
  std::vector<std::shared_ptr<StreamIdentification>> streamList;

  this->catalog->startTransaction();
  this->catalog->getStreams(this->archive_name, streamList);
  this->catalog->commitTransaction();

  for (auto &stream : streamList) {

    if (stream->stype == ConnectionDescr::CONNECTION_TYPE_STREAMER) {
      /* Looks like we found one */
      myStream = stream;
    }

  }

  /*
   * Default settings for slot initialization.
   */
  pgstream->streamident.slot = std::make_shared<PhysicalReplicationSlot>();
  pgstream->streamident.slot->reserve_wal = true;
  pgstream->streamident.slot->existing_ok = true;
  pgstream->streamident.slot->no_identok  = false;

  if (myStream == nullptr) {

    /*
     * The requested stream doesn't exist, we need to create a new one.
     * We use a generated UUID identifier for this, generateSlotNameUUID()
     * will do the legwork for us. Note that we need an identified connection
     * handle before, which is done in the command handler before calling
     * us.
     */
    pgstream->generateSlotNameUUID(this->archive_name);

    /*
     * Replication slot for this archive required.
     * We do not care if the slot already exists, but we want
     * a slot to reserve WAL.
     */
    pgstream->createPhysicalReplicationSlot(pgstream->streamident.slot);

    /*
     * It might occur that we didn't find a registered stream, but
     * the replication slot already exists. We treat this condition
     * as an error and exit, but before that we mark the stream as failed.
     */
    if (pgstream->streamident.slot->status == REPLICATION_SLOT_EXISTS) {
      std::ostringstream oss;

      oss << "Replication slot "
          << pgstream->streamident.slot_name
          << " exists. This usually means someone has created a slot with the same identifier";

      /* Now throw the error */
      throw StreamingFailure(oss.str());
    }

    /*
     * Looks good.
     *
     * Register the stream into the catalog.
     * This also sets the state of the new stream descriptor
     * to STREAM_PROGRESS_IDENTIFIED.
     */
    this->catalog->startTransaction();
    this->catalog->registerStream(this->id,
                                   ConnectionDescr::CONNECTION_TYPE_STREAMER,
                                  pgstream->streamident);
    this->catalog->commitTransaction();

  } else {

    /*
     * We've found a registered stream. This usually means, the associated
     * replication slot belonging to this catalog stream already exists
     * on the source database. We are going to create it anyways but ignore
     * any duplicate in this case (NOTE: We could attempt a separate
     * database connection and lookup the slot via pg_stat_replication_slots,
     * but we don't have this facility yet).
     *
     * Copy over some of the static properties for the current instantiated
     * StreamIdentification, but not everything! For example xlogpos have
     * already changed, the same with status and create_date.
     *
     * Also check the systemid, we need to be carefully if something
     * has changed here.
     *
     */

    if (!pgstream->streamident.systemid.compare(myStream->systemid)) {
      std::ostringstream oss;

      oss << "current systemid "
          << pgstream->streamident.systemid
          << " does not match systemid "
          << myStream->systemid
          << " from catalog ";
      throw StreamingFailure(oss.str());
    }

    pgstream->streamident.id = myStream->id;
    pgstream->streamident.stype = myStream->stype;

    pgstream->streamident.slot_name = myStream->slot_name;
    pgstream->createPhysicalReplicationSlot(pgstream->streamident.slot);

    if (pgstream->streamident.slot->status == REPLICATION_SLOT_EXISTS) {

#ifdef __DEBUG_XLOG__
      cerr
        << "replication slot "
        << pgstream->streamident.slot_name
        << " exists, trying to reuse it"
        << endl;
#endif

      /*
       * The slot already exist, we should use the last reported XLOG
       * position from the catalog.
       *
       * XXX: Please note that the WALStreamerProcess should examine
       *      the on-disk archive as well. This happens when we find
       *      a live catalog stream entry which wasn't cleanly marked
       *      STREAM_PROGRESS_SHUTDOWN. In other cases, we can rely
       *      on the catalog position.
       */

      /*
       * If the stream was marked for shutdown, we rely
       * on the last reported XLOG position there.
       */
      if (myStream->status == StreamIdentification::STREAM_PROGRESS_SHUTDOWN) {

#ifdef __DEBUG__
        cout << "using xlog position "
             << myStream->xlogpos
             << " timeline "
             << myStream->timeline
             << " from catalog"
             << endl;
#endif

        /*
         * If START STREAMING ... RESTART was requested, don't
         * update the XLOG position we got by IDENTIFY SYSTEM. Instead,
         * restart the stream from the server's position.
         */
        if (!this->forceXLOGPosRestart) {

          XLogRecPtr startpos = PGStream::decodeXLOGPos(myStream->xlogpos);

          /*
           * Make sure we initialize the starting position
           * to segment beginning.
           */
          startpos = pgstream->XLOGSegmentStartPosition(startpos);
          pgstream->streamident.xlogpos  = PGStream::encodeXLOGPos(startpos);
          pgstream->streamident.timeline = myStream->timeline;

        }
      }

      /*
       * If the encountered stream is still marked
       * STREAM_PROGRESS_STREAMING, we might have an uncleanly
       * stopped WALStreamerProcess or another stream is already
       * active here. The latter shouldn't happen, since we check
       * this before arriving here, so we need to properly initialize
       * the XLOG start position from the archive, we can't trust
       * what we actually have in the catalogs.
       *
       * We should obtain a valid XLOG start position either
       * from the archive or PGStream::identify(), assign this to
       * the catalog stream descriptor. Since we have a
       * REPLICATION_SLOT_EXISTS here, this means that the slot
       * already existed, so scan the log archive and all
       * segments there to obtain the latest XLogRecPtr
       * from the latest completed log.
       *
       * TBD
       */
      if (myStream->status == StreamIdentification::STREAM_PROGRESS_STREAMING
          || myStream->status == StreamIdentification::STREAM_PROGRESS_IDENTIFIED) {

        unsigned int new_tli = 0;
        unsigned int new_segno = 0;
        std::string xlogpos;

#ifdef __DEBUG_XLOG__
        cerr << "not a clean streamer shutdown, trying to get xlog position from log archive" << endl;
#endif

        if (!this->forceXLOGPosRestart) {
          xlogpos = logdir->getXlogStartPosition(new_tli,
                                                 new_segno,
                                                 pgstream->getWalSegmentSize());

          if (xlogpos.length() <= 0) {
            /*
             * Oops, we couldn't extract a valid XLOG record from the archive.
             * This usually means the archive didn't have anything interesting, so
             * we just assume the archive is empty and we start from the beginning.
             *
             * In this case, just rely on the XLOG position reported
             * by IDENTIFY_SYSTEM.
             *
             * XXX: Report this somehow to the user.
             */
            myStream->xlogpos = pgstream->streamident.xlogpos;
            myStream->timeline = pgstream->streamident.timeline;

#ifdef __DEBUG_XLOG__
            cerr
              << "no transaction logs found, will start from catalog XLOG position"
              << endl;
#endif

          } else {
            /*
             * We could extract a new XLogRecPtr from the archive.
             */
#ifdef __DEBUG_XLOG__
            cerr << "new XLOG start position determined by archive: "
                 << xlogpos
                 << endl;
#endif
            pgstream->streamident.xlogpos = xlogpos;
            pgstream->streamident.timeline = new_tli;
            myStream->xlogpos = xlogpos;
            myStream->timeline = new_tli;
          }
        } else {

          /*
           * We are forced to rely on the XLOG position from
           * IDENTIFY SYSTEM, so leave it as-is.
           */
          myStream->xlogpos = pgstream->streamident.xlogpos;
          myStream->timeline = pgstream->streamident.timeline;

        }

      }

    } else if (pgstream->streamident.slot->status == REPLICATION_SLOT_OK) {

      /*
       * The replication slot was created before. We are going
       * to start streaming by using the last reported xlog
       * position from the system.
       *
       * StreamIdentification::xlogpos will be already initialized
       * by PGStream::identify(), so nothing special to be done here for now.
       */
    }

  }

  /*
   * When starting up, we choose the write_position to
   * be identical with the previously examined xlogpos
   * start position.
   */
  pgstream->streamident.write_position = PGStream::decodeXLOGPos(pgstream->streamident.xlogpos);

  /*
   * Update the write offset of the stream identification to
   * reflect the new starting position for the current XLOG
   * file.
   */
  pgstream->streamident.updateStartSegmentWriteOffset();

  cerr << "pgstream start write offset " << pgstream->streamident.write_pos_start_offset << endl;

  /*
   * Update the slot to reflect current state.
   */
  this->updateStreamCatalogStatus(pgstream->streamident);

}

void StartStreamingForArchiveCommand::updateStreamCatalogStatus(StreamIdentification &ident) {

  std::vector<int> affectedAttrs;

  affectedAttrs.clear();
  affectedAttrs.push_back(SQL_STREAM_XLOGPOS_ATTNO);
  affectedAttrs.push_back(SQL_STREAM_TIMELINE_ATTNO);
  affectedAttrs.push_back(SQL_STREAM_STATUS_ATTNO);

  this->catalog->startTransaction();
  this->catalog->updateStream(ident.id,
                              affectedAttrs,
                              ident);
  this->catalog->commitTransaction();

}

void StartStreamingForArchiveCommand::finalizeStream() {

  /* this is a no-op yet */

}

void StartStreamingForArchiveCommand::execute(bool noop) {

  /*
   * Die hard in case no catalog available.
   */
  if (this->catalog == nullptr) {
    throw CArchiveIssue("could not execute catalog command: no catalog");
  }

  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  /*
   * IMPORTANT:
   *
   * Don't employ transactions here! Status updates will
   * be lost otherwise!
   */

  /*
   * Check, if archive exists.
   */
  temp_descr = this->catalog->existsByName(this->archive_name);

  if (temp_descr->id < 0) {
    /*
     * Don't need to rollback, outer exception handler will do this
     */
    std::ostringstream oss;
    oss << "archive\""
        << this->archive_name
        << "\" does not exist";

    throw CCatalogIssue(oss.str());
  }

  /*
   * Check if we are supposed to run a background streaming
   * process via launcher.
   */
  if (this->detach) {

    job_info jobDescr;

    /*
     * Rebuild the command from scratch.
     */
    std::string archive_name = this->archive_name;
    std::ostringstream cmd_str;

    cerr << "DETACHING requested" << endl;

    cmd_str << "START STREAMING FOR ARCHIVE "
            << archive_name;

    if (this->forceXLOGPosRestart) {
      cmd_str << " RESTART";
    }

    cmd_str << " NODETACH";

    /* Assign a dummy catalog command handle to the job descriptor */
    jobDescr.cmdHandle = std::make_shared<BackgroundWorkerCommandHandle>(this->catalog);
    establish_launcher_cmd_queue(jobDescr);
    send_launcher_cmd(jobDescr, cmd_str.str());

    /* All done, we should exit this command handler */
    return;

  }

  /*
   * Assign archive id to ourselves. We don't have
   * our internal ID ready yet, but for further catalog
   * actions this is required.
   */
  this->id = temp_descr->id;

  /*
   * Prepare directory and backup handles.
   */
  this->archivedir
    = CPGBackupCtlFS::getArchiveDirectoryDescr(this->temp_descr->directory);
  this->logdir = archivedir->logdirectory();

  /* Make sure target directories exists */
  this->archivedir->create();

  /* prepare stream and start streaming */
  try {

    XLogRecPtr startpos = InvalidXLogRecPtr;

    /*
     * Get the streaming connection for this archive. Please note that we
     * have to fallback to archive default connection.
     */

    temp_descr->coninfo->pushAffectedAttribute(SQL_CON_ARCHIVE_ID_ATTNO);
    temp_descr->coninfo->pushAffectedAttribute(SQL_CON_TYPE_ATTNO);
    temp_descr->coninfo->pushAffectedAttribute(SQL_CON_DSN_ATTNO);
    temp_descr->coninfo->pushAffectedAttribute(SQL_CON_PGHOST_ATTNO);
    temp_descr->coninfo->pushAffectedAttribute(SQL_CON_PGPORT_ATTNO);
    temp_descr->coninfo->pushAffectedAttribute(SQL_CON_PGUSER_ATTNO);
    temp_descr->coninfo->pushAffectedAttribute(SQL_CON_PGDATABASE_ATTNO);

    /*
     * We need to try harder here, if anynone has defined a separate
     * streaming connection for this archive. If no streamer type is found,
     * switch back to basebackup type and use that.
     */
    this->catalog->getCatalogConnection(temp_descr->coninfo,
                                        temp_descr->id,
                                        ConnectionDescr::CONNECTION_TYPE_STREAMER);

    if (temp_descr->coninfo->archive_id < 0
        && temp_descr->coninfo->type == ConnectionDescr::CONNECTION_TYPE_UNKNOWN) {
      /* use archive default connection */
      this->catalog->getCatalogConnection(temp_descr->coninfo,
                                          temp_descr->id,
                                          ConnectionDescr::CONNECTION_TYPE_BASEBACKUP);
    }

#ifdef __DEBUG__
    cout << "streaming connection DSN " << temp_descr->coninfo->dsn << endl;
#endif
    /*
     * Connection definition should be ready now, create PGStream
     * connection handle and go further.
     */
    std::shared_ptr<WALStreamerProcess> walstreamer = nullptr;

    this->pgstream = new PGStream(temp_descr);
    pgstream->connect();

    /*
     * Prepare backup handler
     *
     * We cannot do this earlier, since we need to
     * know the WAL segment size of the source instance.
     */
    this->backup = make_shared<TransactionLogBackup>(temp_descr);
    this->backup->setWalSegmentSize(pgstream->getWalSegmentSize());
    this->backup->initialize();

    /*
     * Identify system
     */
    pgstream->identify();

    /*
     * Since we always want to start from the _beginning_ of
     * a XLOG segment, we need to setup the current server's
     * XLOG position to segment start.
     */
    cerr << "IDENTIFY XLOG says: " << pgstream->streamident.xlogpos << endl;
    startpos = pgstream->streamident.xlogposDecoded();
    cerr << "IDENTIFY XLOG after decode says: " << PGStream::encodeXLOGPos(startpos) << endl;
    startpos = pgstream->XLOGSegmentStartPosition(startpos);
    pgstream->streamident.xlogpos = PGStream::encodeXLOGPos(startpos);


#ifdef __DEBUG_XLOG__
    cerr << "IDENTIFICATION (TLI/XLOGPOS) "
         << pgstream->streamident.timeline
         << "/"
         << pgstream->streamident.xlogpos
         << " XLOG_SEG_SIZE "
         << pgstream->getWalSegmentSize()
         << " SYSID "
         << pgstream->streamident.systemid
         << endl;
#endif

    /*
     * Before calling prepareStream() we need to
     * set the archive_id to the Stream Identification. This
     * will engage the stream with the archive information.
     */
    pgstream->streamident.stype = ConnectionDescr::CONNECTION_TYPE_STREAMER;
    pgstream->streamident.archive_id = temp_descr->coninfo->archive_id;
    pgstream->streamident.status = StreamIdentification::STREAM_PROGRESS_IDENTIFIED;

    /*
     * Now prepare the stream.
     */
    prepareStream();

    /*
     * Create a walstreamer handle.
     */
    walstreamer = pgstream->walstreamer();

    /*
     * Assign stop handler, this is just a reference
     * to our own stop handler.
     */
    walstreamer->assignStopHandler(this->stopHandler);

    /*
     * We want the walstreamer to stream into our current log archive.
     */
    walstreamer->setBackupHandler(this->backup);

    /*
     * Enter infinite loop as long as receive() tells
     * us that we can continue.
     */
    while (1) {

      MemoryBuffer timelineHistory;
      string historyFilename;
      StreamIdentification walstreamerIdent;

      /*
       * Get the timeline history file content, but only if we
       * are on a timeline greater than 1. The first timeline
       * never writes a history file, thus ignore it.
       */
      if (walstreamer->getCurrentTimeline() > 1) {
        pgstream->timelineHistoryFileContent(timelineHistory,
                                             historyFilename,
                                             walstreamer->getCurrentTimeline());
#ifdef __DEBUG_XLOG_
        std::cerr << "got history file " << historyFilename
                  << " and its content" << std::endl;
#endif
      }

      walstreamer->start();

      /*
       * Set catalog state to streaming
       */
      walstreamerIdent = walstreamer->identification();
      this->updateStreamCatalogStatus(walstreamerIdent);

      if (!walstreamer->receive()) {

        ArchiverState reason = walstreamer->reason();

        /*
         * Receive exited for some reason. Check out why.
         * We need to check of mostly three reasons here:
         *
         * 1) The connected upstream disconnected the stream for some reason,
         *    so we need to shutdown.
         * 2) We are instructed to shutdown. This is not that different
         *    from 1).
         * 3) The stream changed its timeline. Handle this and restart
         *    the stream.
         */
        if (reason == ARCHIVER_TIMELINE_SWITCH) {

#ifdef __DEBUG_XLOG_
          std::cerr << "timeline switch detected" << std::endl;
#endif
          /*
           * The walstreamer already did all the necessary legwork
           * to properly shutdown the stream, so that we can go forward.
           *
           * The next step is to retrieve the new timeline history file and
           * then restart the stream.
           *
           * We don't do this here, the next loop will retrieve
           * the timeline history file content and restart the stream.
           */
          continue;

        } else if (reason == ARCHIVER_SHUTDOWN) {

          StreamIdentification currentIdent;

#ifdef __DEBUG_XLOG__
          std::cerr << "preparing WAL streamer for shutdown" << std::endl;
#endif
          currentIdent = walstreamer->identification();
          this->updateStreamCatalogStatus(currentIdent);
          break;

        } else {

          /* oops, this is unexpected here */
          std::cerr << "unexpected WAL streamer state: " << reason << std::endl;
          break;

        }
      }

    }

    cerr << "recv aborted, WAL streamer state " << walstreamer->reason() << endl;

    /*
     * Usually receive() above will catch us in a loop,
     * if we arrive here this means we need to exit safely.
     */
    finalizeStream();

  } catch(CPGBackupCtlFailure &e) {
    throw e;
  }
}

ListBackupCatalogCommand::ListBackupCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->tag = LIST_BACKUP_CATALOG;
  this->catalog = catalog;
}

ListBackupCatalogCommand::ListBackupCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

void ListBackupCatalogCommand::execute(bool flag) {

  /*
   * Die hard in case no catalog available.
   */
  if (this->catalog == nullptr)
    throw CArchiveIssue("could not execute catalog command: no catalog");

  if (!this->catalog->available())
    this->catalog->open_rw();

  /*
   * Enter transaction
   */
  try {

    std::shared_ptr<StatCatalog> stat = nullptr;
    std::shared_ptr<CatalogDescr> temp_descr = nullptr;

    this->catalog->startTransaction();

    /*
     * Check if requested archive exists in the catalog.
     */
    temp_descr = this->catalog->existsByName(this->archive_name);

    if (temp_descr->id < 0) {
      /*
       * Don't need to rollback, outer exception handler will do this
       */
      std::ostringstream oss;
      oss << "cannot stat catalog: archive\""
          << this->archive_name
          << "\" does not exist";

      throw CCatalogIssue(oss.str());
    }
    /* list of all backups in every archive */
    stat = this->catalog->statCatalog(this->archive_name);
    cout << stat->gimmeFormattedString();

    this->catalog->commitTransaction();

  } catch (CPGBackupCtlFailure &e) {
    this->catalog->rollbackTransaction();
    throw e; /* don't hide exception from caller */
  }
}

StartBasebackupCatalogCommand::StartBasebackupCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->tag = START_BASEBACKUP;
  this->catalog = catalog;
}

StartBasebackupCatalogCommand::StartBasebackupCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

void StartBasebackupCatalogCommand::execute(bool background) {

  std::shared_ptr<CatalogDescr> temp_descr(nullptr);
  std::shared_ptr<BackupProfileDescr> backupProfile(nullptr);

  /*
   * Basebackup stream process handler.
   */
  std::shared_ptr<BaseBackupProcess> bbp(nullptr);

  /* Track if basebackup was registered already */
  bool basebackup_registered = false;

  /*
   * Die hard in case no catalog descriptor available.
   */
  if (this->catalog == nullptr)
    throw CArchiveIssue("could not execute archive command: no catalog");

  /*
   * Open the catalog, if not done yet.
   */
  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  this->catalog->startTransaction();

  try {
    /*
     * Check if the specified archive_name is present, get
     * its descriptor.
     */
    temp_descr = this->catalog->existsByName(this->archive_name);

    /*
     * existsByName() doesn't retrieve the archive
     * database connection specification into our
     * catalog descriptor, we need to do it ourselves.
     */
    if (temp_descr->id >= 0) {

      temp_descr->coninfo->pushAffectedAttribute(SQL_CON_ARCHIVE_ID_ATTNO);
      temp_descr->coninfo->pushAffectedAttribute(SQL_CON_TYPE_ATTNO);
      temp_descr->coninfo->pushAffectedAttribute(SQL_CON_DSN_ATTNO);
      temp_descr->coninfo->pushAffectedAttribute(SQL_CON_PGHOST_ATTNO);
      temp_descr->coninfo->pushAffectedAttribute(SQL_CON_PGPORT_ATTNO);
      temp_descr->coninfo->pushAffectedAttribute(SQL_CON_PGUSER_ATTNO);
      temp_descr->coninfo->pushAffectedAttribute(SQL_CON_PGDATABASE_ATTNO);

      this->catalog->getCatalogConnection(temp_descr->coninfo,
                                          temp_descr->id,
                                          ConnectionDescr::CONNECTION_TYPE_BASEBACKUP);
    }
    this->catalog->commitTransaction();

  } catch(CPGBackupCtlFailure& e) {
    /* oops */
    this->catalog->rollbackTransaction();
    throw e;
  }

  if (temp_descr->id < 0) {
    /* Requested archive doesn't exist, error out */
    std::ostringstream oss;

    oss << "archive " << this->archive_name << " doesn't exist";
    this->catalog->rollbackTransaction();
    throw CArchiveIssue(oss.str());
  }

  /*
   * If the PROFILE keyword was specified, select
   * the requested profile from the catalog. If it doesn't
   * exist, throw an error.
   *
   * Iff PROFILE was omitted, select the default profile.
   */
  if (this->backup_profile->name != "") {

#ifdef __DEBUG__
    cerr << "DEBUG: checking for profile " << this->backup_profile->name << endl;
#endif

    this->catalog->startTransaction();

    try {
      backupProfile = catalog->getBackupProfile(this->backup_profile->name);
      this->catalog->commitTransaction();
    } catch(CPGBackupCtlFailure &e) {
      this->catalog->rollbackTransaction();
      throw e;
    }

    /*
     * If the requested backup profile was not found, raise
     * an exception.
     */
    if (backupProfile->profile_id < 0) {
      std::ostringstream oss;
      oss << "backup profile \"" << this->backup_profile->name << "\" does not exist";
      throw CArchiveIssue(oss.str());
    }

  } else {

    this->catalog->startTransaction();

    try {

      /*
       * The PROFILE keyword wasn't specified to
       * the START BASEBACKUP command. Request the default
       * profile...
       */
      backupProfile = catalog->getBackupProfile("default");
      this->catalog->commitTransaction();

    } catch(CPGBackupCtlFailure &e) {
      this->catalog->rollbackTransaction();
      throw e;
    }

#ifdef __DEBUG__
    cerr << "PROFILE keyword not specified, using \"default\" backup profile" << endl;
#endif

    /*
     * Iff the "default" profile doesn't exist, tell
     * the user that this is an unexpected error condition.
     */
    if (backupProfile->profile_id < 0) {
      std::ostringstream oss;
      oss << "\"default\" profile not found: please check your backup catalog or create a new one";
      throw CArchiveIssue(oss.str());
    }

  }

  try {

    PGStream pgstream(temp_descr);

    /*
     * Create base backup stream handler.
     */
    std::shared_ptr<StreamBaseBackup> backupHandle
      = std::make_shared<StreamBaseBackup>(temp_descr);

    /*
     * Meta information handle for streamed tablespace.
     */
    std::shared_ptr<BackupTablespaceDescr> tablespaceDescr = nullptr;

    std::shared_ptr<BaseBackupDescr> basebackupDescr = nullptr;

    /*
     * Backup profile tells us the compression mode to use...
     */
    backupHandle->setCompression(backupProfile->compress_type);

    /*
     * Get connection to the PostgreSQL instance.
     */
    pgstream.connect();

#ifdef __DEBUG__
    cerr << "DEBUG: connecting stream" << endl;
#endif

    /*
     * Identify this replication connection.
     */
    pgstream.identify();

#ifdef __DEBUG__
    cerr << "DEBUG: identify stream" << endl;
#endif

    /*
     * Get basebackup stream handle.
     */
    bbp = pgstream.basebackup(backupProfile);

    /*
     * Set signal handler
     */
    bbp->assignStopHandler(this->stopHandler);


#ifdef __DEBUG__
    cerr << "DEBUG: basebackup stream handle initialize" << endl;
#endif

    /*
     * Enter basebackup stream.
     */
    bbp->start();

    /*
     * Now its time to register this basebackup handle to
     * the catalog. Do the rollback in our own exception handler
     * in case of errors.
     *
     * NOTE: Since the streaming directory handle is just passed to
     *       stepTablespace(), but we need to register the basebackup
     *       before, we must set the fsentry to the basebackup descriptor
     *       ourselves!
     */
    basebackupDescr = bbp->getBaseBackupDescr();

    this->catalog->startTransaction();

    try {

      /*
       * Prepare backup handler. Should successfully create
       * target streaming directory...
       */
      backupHandle->initialize();
      backupHandle->create();

      basebackupDescr->archive_id = temp_descr->id;
      basebackupDescr->fsentry = backupHandle->backupDirectoryString();

      cerr << "directory handle path " << basebackupDescr->fsentry << endl;

      this->catalog->registerBasebackup(temp_descr->id,
                                        basebackupDescr);
      this->catalog->commitTransaction();

    } catch(CPGBackupCtlFailure &e) {
      this->catalog->rollbackTransaction();
      throw e;
    }

    /*
     * Remember successful registration of this basebackup.
     */
    basebackup_registered = true;

#ifdef __DEBUG__
    cerr << "DEBUG: basebackup stream started" << endl;
#endif

    /*
     * Initialize list of tablespaces, if any.
     */
    bbp->readTablespaceInfo();

#ifdef __DEBUG__
    cerr << "DEBUG: basebackup tablespace meta info requested" << endl;
#endif

    /*
     * Loop through tablespaces and stream their contents.
     */
    while(bbp->stepTablespace(backupHandle,
                              tablespaceDescr)) {
#ifdef __DEBUG__
      cerr << "streaming tablespace OID "
           << tablespaceDescr->spcoid
           << ",size " << tablespaceDescr->spcsize
           << endl;
#endif
      /*
       * Since we register each backup tablespace, we need
       * to record the backup id, it belongs to.
       */
      tablespaceDescr->backup_id = basebackupDescr->id;
      catalog->registerTablespaceForBackup(tablespaceDescr);
      bbp->backupTablespace(tablespaceDescr);

      /*
       * Check the state of the last tablespace being
       * copied. If we were interrupted, we abort the
       * backup processing loop here immediately.
       */
      if (bbp->getState() != BASEBACKUP_TABLESPACE_READY)
        throw StreamingFailure("streaming basebackup aborted");
    }

    /*
     * Call end position in backup stream.
     */
    bbp->end();

    /*
     * And disconnect
     */
#ifdef __DEBUG__
    cerr << "DEBUG: disconnecting stream" << endl;
#endif

    pgstream.disconnect();
  } catch(CPGBackupCtlFailure& e) {

    bool txinprogress = false;

    /*
     * If the basebackup was already registered, mark it
     * as aborted. This is sad even if all went through, but only
     * disconnecting from the server throws an error, but we treat this
     * as an error nevertheless. We must be careful here, since catalog
     * operations itself can throw exceptions and we don't mask
     * those errors with the current one.
     */
    try {
      if (basebackup_registered) {
        this->catalog->startTransaction();
        txinprogress = true;

#ifdef __DEBUG__
        cerr << "DEBUG: marking basebackup as aborted" << endl;
#endif

        this->catalog->abortBasebackup(bbp->getBaseBackupDescr());
        this->catalog->commitTransaction();
      }
    } catch (CPGBackupCtlFailure &e) {
      if (txinprogress)
        this->catalog->rollbackTransaction();
    }

    /* re-throw ... */
    throw e;

  }

  /*
   * Everything seems okay for now, finalize the backup
   * registration.
   */
  this->catalog->startTransaction();

  try {
    this->catalog->finalizeBasebackup(bbp->getBaseBackupDescr());
    this->catalog->commitTransaction();
  } catch (CPGBackupCtlFailure &e) {
    this->catalog->rollbackTransaction();
    throw e;
  }

}

DropBackupProfileCatalogCommand::DropBackupProfileCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->tag = DROP_BACKUP_PROFILE;
  this->catalog = catalog;
}

DropBackupProfileCatalogCommand::DropBackupProfileCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

void DropBackupProfileCatalogCommand::execute(bool extended) {

  /*
   * Die hard in case no catalog available.
   */
  if (this->catalog == nullptr)
    throw CArchiveIssue("could not execute archive command: no catalog");

  /*
   * Open the catalog, if not yet done.
   */
  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  try {

    catalog->startTransaction();

    /*
     * Check if the specified backup profile exists. Error
     * out with an exception if not found.
     */
    std::shared_ptr<BackupProfileDescr> profileDescr = this->getBackupProfileDescr();
    std::shared_ptr<BackupProfileDescr> temp_descr = catalog->getBackupProfile(profileDescr->name);

    /*
     * NOTE: BackupCatalog::getBackupProfile() always returns an
     *       BackupProfileDescr, but in case the specified
     *       name doesn't exists it is initialized with profile_id = -1
     */
    if (temp_descr->profile_id < 0) {
      std::ostringstream oss;
      oss << "backup profile \"" << profileDescr->name << "\"" << endl;
      throw CCatalogIssue(oss.str());
    }

    catalog->dropBackupProfile(profileDescr->name);
    catalog->commitTransaction();

  } catch(exception& e) {
    catalog->rollbackTransaction();
    /* re-throw exception */
    throw e;
  }
}

ListBackupProfileCatalogCommand::ListBackupProfileCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->tag = LIST_BACKUP_PROFILE;
  this->catalog = catalog;
}

ListBackupProfileCatalogCommand::ListBackupProfileCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

void ListBackupProfileCatalogCommand::execute(bool extended) {

  shared_ptr<CatalogDescr> temp_descr(nullptr);

  /*
   * Die hard in case no catalog available.
   */
  if (this->catalog == nullptr)
    throw CArchiveIssue("could not execute archive command: no catalog");

  /*
   * Open the catalog, if not yet done.
   */
  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  try {

    catalog->startTransaction();

    /*
     * Get a list of current backup profiles. If the name is requested,
     * show the details only.
     */
    if (this->tag == LIST_BACKUP_PROFILE) {

      auto profileList = catalog->getBackupProfiles();

      /*
       * Print headline
       */
      cout << CPGBackupCtlBase::makeHeader("List of backup profiles",
                                           boost::format("%-25s\t%-15s")
                                           % "Name" % "Backup Label", 80);

      /*
       * Print name and label
       */
      for (auto& descr : *profileList) {
        cout << boost::format("%-25s\t%-15s") % descr->name % descr->label
             << endl;
      }

    } else if (this->tag == LIST_BACKUP_PROFILE_DETAIL) {

      std::shared_ptr<BackupProfileDescr> profile
        = this->catalog->getBackupProfile(this->getBackupProfileDescr()->name);

      cout << CPGBackupCtlBase::makeHeader("Details for backup profile " + profile->name,
                                           boost::format("%-25s\t%-40s") % "Property" % "Setting", 80);

      /* Profile Name */
      cout << boost::format("%-25s\t%-30s") % "NAME" % profile->name << endl;

      /* Profile compression type */
      switch(profile->compress_type) {
      case BACKUP_COMPRESS_TYPE_NONE:
        cout << boost::format("%-25s\t%-30s") % "COMPRESSION" % "NONE" << endl;
        break;
      case BACKUP_COMPRESS_TYPE_GZIP:
        cout << boost::format("%-25s\t%-30s") % "COMPRESSION" % "GZIP" << endl;
        break;
      case BACKUP_COMPRESS_TYPE_ZSTD:
        cout << boost::format("%-25s\t%-30s") % "COMPRESSION" % "ZSTD" << endl;
        break;
      case BACKUP_COMPRESS_TYPE_PBZIP:
        cout << boost::format("%-25s\t%-30s") % "COMPRESSION" % "PBZIP" << endl;
        break;
      case BACKUP_COMPRESS_TYPE_PLAIN:
        cout << boost::format("%-25s\t%-30s") % "COMPRESSION" % "PLAIN" << endl;
        break;
      default:
        cout << boost::format("%-25s\t%-30s") % "COMPRESSION" % "UNKNOWN or N/A" << endl;
        break;
      }

      /* Profile max rate */
      if (profile->max_rate <= 0) {
        cout << boost::format("%-25s\t%-30s") % "MAX RATE" % "NOT RATED" << endl;
      } else {
        cout << boost::format("%-25s\t%-30s") % "MAX RATE(KByte/s)" % profile->max_rate << endl;
      }

      /* Profile backup label */
      cout << boost::format("%-25s\t%-30s") % "LABEL" % profile->label << endl;

      /* Profile fast checkpoint mode */
      cout << boost::format("%-25s\t%-30s") % "FAST CHECKPOINT" % profile->fast_checkpoint << endl;

      /* Profile WAL included */
      cout << boost::format("%-25s\t%-30s") % "WAL INCLUDED" % profile->include_wal << endl;

      /* Profile WAIT FOR WAL */
      cout << boost::format("%-25s\t%-30s") % "WAIT FOR WAL" % profile->wait_for_wal << endl;
    }

    catalog->commitTransaction();

  } catch (exception& e) {
    this->catalog->rollbackTransaction();
    throw e;
  }

}

CreateBackupProfileCatalogCommand::CreateBackupProfileCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

  /*
   * BaseCatalogCommand::copy() has no idea of submodule objects belongig
   * to a given catalog descriptor. We need to copy them explicitely.
   */
  this->profileDescr = descr->getBackupProfileDescr();

}

CreateBackupProfileCatalogCommand::CreateBackupProfileCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->tag = CREATE_BACKUP_PROFILE;
  this->catalog = catalog;
}

CreateBackupProfileCatalogCommand::CreateBackupProfileCatalogCommand() {
  this->tag = CREATE_BACKUP_PROFILE;
}

void CreateBackupProfileCatalogCommand::setProfile(std::shared_ptr<BackupProfileDescr> profileDescr) {
  this->profileDescr = profileDescr;
}

void CreateBackupProfileCatalogCommand::verify(bool print_version) {

  job_info jobDescr;

  namespace bf = boost::filesystem;

  jobDescr.background_exec = true;

  /*
   * When creating a backup profile we check
   * if certain compression methods are possible
   * since they are backed by command line tools. Atm
   * these are:
   *
   * PBZIP - requires pbzip2 command line tool
   * PLAIN - requires the tar command line tool
   */
  switch(this->profileDescr->compress_type) {

  case BACKUP_COMPRESS_TYPE_PLAIN:
    {
      path tar("tar");
      ArchivePipedProcess app(tar);
      char buf[4];

      app.setExecutable(tar);
      app.pushExecArgument("--version");

      app.setOpenMode("r");
      app.open();

      while(app.read(buf, 1) >= 1)
        cout << buf;

      app.close();
      break;
    }

  case BACKUP_COMPRESS_TYPE_PBZIP:
    {
      path pbzip("pbzip2");
      ArchivePipedProcess app(pbzip);
      char buf[4];

      app.setExecutable(pbzip);
      app.pushExecArgument("--version");

      app.setOpenMode("r");
      app.open();

      while(app.read(buf, 1) >= 1)
        cout << buf;

      app.close();
      break;
    }

  default:
    break; /* nothing to do here */
  }

}

void CreateBackupProfileCatalogCommand::execute(bool existsOk) {

  if (this->catalog == nullptr)
    throw CArchiveIssue("could not execute archive command: no catalog");

#ifdef __DEBUG__
  cout << "name: " << this->profileDescr->name << endl;
  cout << "compression: " << this->profileDescr->compress_type << endl;
  cout << "max rate: " << this->profileDescr->max_rate << endl;
  cout << "label: " << this->profileDescr->label << endl;
  cout << "wal included: " << this->profileDescr->include_wal << endl;
  cout << "wait for wal: " << this->profileDescr->wait_for_wal << endl;
#endif

  /*
   * Open the catalog, if necessary.
   */
  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  /*
   * Do some preliminary checks...
   */
  this->verify();

  try {

    /*
     * Start catalog transaction.
     */
    this->catalog->startTransaction();

    /*
     * Check if the specified backup profile already exists
     */
    std::shared_ptr<BackupProfileDescr> temp_descr = catalog->getBackupProfile(this->profileDescr->name);

    if (temp_descr->profile_id < 0) {

      /*
       * Create the new profile.
       *
       * NOTE: we can't bindly use the new BackupProfileDescr passed down
       *       from the parser, since it might not have seen all
       *       the attributes, the user might decided not to overwrite its default.
       *       Hence, pass down all attributes which are required to create
       *       a new entry.
       */

      std::vector<int> attr;
      attr.push_back(SQL_BCK_PROF_NAME_ATTNO);
      attr.push_back(SQL_BCK_PROF_COMPRESS_TYPE_ATTNO);
      attr.push_back(SQL_BCK_PROF_MAX_RATE_ATTNO);
      attr.push_back(SQL_BCK_PROF_LABEL_ATTNO);
      attr.push_back(SQL_BCK_PROF_FAST_CHKPT_ATTNO);
      attr.push_back(SQL_BCK_PROF_INCL_WAL_ATTNO);
      attr.push_back(SQL_BCK_PROF_WAIT_FOR_WAL_ATTNO);

      this->profileDescr->setAffectedAttributes(attr);
      this->catalog->createBackupProfile(this->profileDescr);

    } else {

      /* profile already exists! */
      if (!existsOk) {
        ostringstream oss;
        oss << "backup profile " << this->profileDescr->name << " already exists";
        throw CCatalogIssue(oss.str());
      }

    }

    this->catalog->commitTransaction();

  } catch(CPGBackupCtlFailure& e) {
    this->catalog->rollbackTransaction();
    throw e;
  }

}

VerifyArchiveCatalogCommand::VerifyArchiveCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

VerifyArchiveCatalogCommand::VerifyArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->tag = VERIFY_ARCHIVE;
  this->catalog = catalog;
}

VerifyArchiveCatalogCommand::VerifyArchiveCatalogCommand() {
  this->tag = VERIFY_ARCHIVE;
}

void VerifyArchiveCatalogCommand::execute(bool missingOK) {

  /*
   * Check if the specified archive really exists.
   */
  shared_ptr<CatalogDescr> temp_descr(nullptr);

  /*
   * Die hard in case no catalog available.
   */
  if (this->catalog == nullptr)
    throw CArchiveIssue("could not execute archive command: no catalog");

  /*
   * Open the catalog, if not yet done.
   */
  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  cout << "checking archive structure: ";

  /*
   * Check archive directory structure.
   */
  try {

    catalog->startTransaction();

    temp_descr = catalog->existsByName(this->archive_name);

    if (temp_descr->id < 0) {
      ostringstream oss;
      oss << "archive " << this->archive_name << " does not exist";
      throw CCatalogIssue(oss.str());
    }

    /*
     * There is an archive with the specified identifier registered.
     * Get an archive directory handle and check wether its directory structure
     * is intact.
     */
    shared_ptr<BackupDirectory> archivedir = CPGBackupCtlFS::getArchiveDirectoryDescr(temp_descr->directory);
    archivedir->verify();
    cout << "OK" << endl;

    this->catalog->commitTransaction();

  } catch(CPGBackupCtlFailure& e) {

    this->catalog->rollbackTransaction();
    throw e;

  }

  /*
   * If requested, check connection of the specified archive.
   * We won't reached this in case the directory has failed.
   *
   * We get a list of all connections specified for
   * the given archive ID and test CONNECTION_TYPE_BASEBACKUP
   * and CONNECTION_TYPE_STREAMER.
   */
  if (this->check_connection) {

    /*
     * Get the list of (possible) streaming or basebackup connections.
     * Other types are ignored (if any).
     *
     * We rely on existsByName() to have initialized temp_descr properly, if
     * the specified archive exists.
     */
    if (temp_descr != nullptr && temp_descr->id >= 0) {

      vector<shared_ptr<ConnectionDescr>> connection_list;

      connection_list = catalog->getCatalogConnection(temp_descr->id);

      /*
       * At least on connection (type basebackup is expected)
       */
      if (connection_list.size() == 0) {
        throw CArchiveIssue("specified archive does not have a database connection");
      }

      for (auto &con : connection_list) {

        temp_descr->coninfo = con;

        if (con->type == ConnectionDescr::CONNECTION_TYPE_BASEBACKUP) {


          PGStream stream(temp_descr);

          cout << "check basebackup database connection: ";
          stream.testConnection();
          cout << "OK" << endl;

        }

        if (con->type == ConnectionDescr::CONNECTION_TYPE_STREAMER) {

          PGStream stream(temp_descr);

          cout << "check streaming database connection: ";
          stream.testConnection();
          cout << "OK" << endl;
        }

      }
    }
  }

}

ListArchiveCatalogCommand::ListArchiveCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

ListArchiveCatalogCommand::ListArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->tag = LIST_ARCHIVE;
  this->catalog = catalog;
}

ListArchiveCatalogCommand::ListArchiveCatalogCommand() {
  this->tag = LIST_ARCHIVE;
}

void ListArchiveCatalogCommand::execute(bool extendedOutput) {

  shared_ptr<CatalogDescr> temp_descr(nullptr);

  /*
   * Die hard in case no catalog available.
   */
  if (this->catalog == nullptr)
    throw CArchiveIssue("could not execute archive command: no catalog");

  /*
   * Open the catalog, if not yet done.
   */
  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  try {
    catalog->startTransaction();

    if (this->mode == ARCHIVE_LIST) {

      /*
       * Get a list of current registered archives in the catalog.
       */
      auto archiveList = catalog->getArchiveList();

      /*
       * Print headline
       */
      cout << CPGBackupCtlBase::makeHeader("List of archives",
                                           boost::format("%-15s\t%-30s") % "Name" % "Directory",
                                           80);

      /*
       * Print archive properties
       */
      for (auto& descr : *archiveList) {
        cout << CPGBackupCtlBase::makeLine(boost::format("%-15s\t%-30s")
                                           % descr->archive_name
                                           % descr->directory);
      }
    }
    else if (this->mode == ARCHIVE_FILTERED_LIST) {

      /*
       * Get a filtered list for the specified properties.
       */
      auto archiveList = catalog->getArchiveList(make_shared<CatalogDescr>(*this),
                                                 this->affectedAttributes);

      /* Print headline */
      cout << CPGBackupCtlBase::makeHeader("Filtered archive list",
                                           boost::format("%-15s\t%-30s") % "Name" % "Directory",
                                           80);

      /*
       * Print archive properties
       */
      for (auto& descr : *archiveList) {
        cout << CPGBackupCtlBase::makeLine(boost::format("%-15s\t%-30s")
                                           % descr->archive_name
                                           % descr->directory);
      }

    } else if (this->mode == ARCHIVE_DETAIL_LIST) {
      /*
       * Parser has marked this as a detailed view, which
       * means that we just had to display the details
       * of the specified archive NAME.
       *
       * NOTE: In this case we usually don't
       * expect multiple results, however, the code supports this
       * at this point nevertheless, since we just call
       * getArchiveList with the affected NAME property attached only.
       */

      /*
       * Get a filtered list for the specified properties.
       */
      auto archiveList = catalog->getArchiveList(make_shared<CatalogDescr>(*this),
                                                 this->affectedAttributes);

      /* Print headline */
      cout << CPGBackupCtlBase::makeHeader("Detail view for archive",
                                           boost::format("%-20s\t%-30s") % "Property" % "Setting",
                                           80);

      for (auto& descr: *archiveList) {
        cout << boost::format("%-20s\t%-30s") % "NAME" % descr->archive_name << endl;
        cout << boost::format("%-20s\t%-30s") % "DIRECTORY" % descr->directory << endl;
        cout << boost::format("%-20s\t%-30s") % "PGHOST" % descr->coninfo->pghost << endl;
        cout << boost::format("%-20s\t%-30d") % "PGPORT" % descr->coninfo->pgport << endl;
        cout << boost::format("%-20s\t%-30s") % "PGDATABASE" % descr->coninfo->pgdatabase << endl;
        cout << boost::format("%-20s\t%-30s") % "PGUSER" % descr->coninfo->pguser << endl;
        cout << boost::format("%-20s\t%-30s") % "DSN" % descr->coninfo->dsn << endl;
        cout << boost::format("%-20s\t%-30s") % "COMPRESSION" % descr->compression << endl;
        cout << CPGBackupCtlBase::makeLine(80) << endl;
      }
    }

  } catch (CPGBackupCtlFailure& e) {
    /* rollback transaction */
    catalog->rollbackTransaction();
    throw e;
  }

  catalog->close();

}

void ListArchiveCatalogCommand::setOutputMode(ListArchiveOutputMode mode) {
  this->mode = mode;
}

AlterArchiveCatalogCommand::AlterArchiveCatalogCommand() {
  this->tag = ALTER_ARCHIVE;
}

AlterArchiveCatalogCommand::AlterArchiveCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));
}

AlterArchiveCatalogCommand::AlterArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->tag = ALTER_ARCHIVE;
  this->catalog = catalog;
}

void AlterArchiveCatalogCommand::execute(bool ignoreMissing) {

  shared_ptr<CatalogDescr> temp_descr(nullptr);

  /*
   * Die hard in case no catalog available.
   */
  if (this->catalog == nullptr)
    throw CArchiveIssue("could not execute archive command: no catalog");

  /*
   * Open the catalog, if not yet done.
   */
  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  catalog->startTransaction();

  try {

    /*
     * Check if the specified archive name really exists.
     * If false and ignoreMissing is false, error out. Otherwise
     * proceed, which turns this action into a no-op, effectively.
     */
    temp_descr = this->catalog->existsByName(this->archive_name);
    if (temp_descr->id >= 0) {

      this->id = temp_descr->id;
      this->catalog->updateArchiveAttributes(make_shared<CatalogDescr>(*this),
                                             this->affectedAttributes);
    } else {

      if (!ignoreMissing) {
        ostringstream oss;
        oss << "could not alter archive: archive name \"" << this->archive_name << "\" does not exists";
        throw CArchiveIssue(oss.str());
      }

    }

    this->catalog->commitTransaction();

  } catch(CPGBackupCtlFailure& e) {
    this->catalog->rollbackTransaction();
    throw e;
  }

}

DropArchiveCatalogCommand::DropArchiveCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));
}

DropArchiveCatalogCommand::DropArchiveCatalogCommand(std::shared_ptr<BackupCatalog> catalog) {
  this->catalog = catalog;
  this->tag     = DROP_ARCHIVE;
}

DropArchiveCatalogCommand::DropArchiveCatalogCommand() {
  this->tag = DROP_ARCHIVE;
}

void DropArchiveCatalogCommand::execute(bool existsOk) {

  shared_ptr<CatalogDescr> temp_descr(nullptr);

  /*
   * Die hard in case no catalog available.
   */
  if (this->catalog == nullptr)
    throw CArchiveIssue("could not execute archive command: no catalog");

  /*
   * Open the catalog, if not yet done.
   */
  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  catalog->startTransaction();

  try {
    /*
     * existsOk means in this case we check wether
     * the archive exists. If false, raise an error, otherwise
     * just pass which turns this method into a no-op.
     */
    temp_descr = catalog->existsByName(this->archive_name);

    if (temp_descr->id >= 0) {

      /* archive name exists, so drop it... */
      catalog->dropArchive(this->archive_name);

    } else {

      if (!existsOk) {
        ostringstream oss;
        oss << "specified archive name \"" << this->archive_name << "\" does not exists";

        /*
         * Note: cleanup handled below in exception handler.
         */
        throw CArchiveIssue(oss.str());
      }
    }

    catalog->dropArchive(this->archive_name);
    catalog->commitTransaction();

  } catch (CPGBackupCtlFailure& e) {

    /*
     * handle error condition, but don't suppress the
     * exception from the caller
     */
    catalog->rollbackTransaction();
    throw e;

  }

}

CreateArchiveCatalogCommand::CreateArchiveCatalogCommand() {
  this->tag = CREATE_ARCHIVE;
}

CreateArchiveCatalogCommand::CreateArchiveCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

std::shared_ptr<BackupCatalog> BaseCatalogCommand::getCatalog() {
  return this->catalog;
}

void BaseCatalogCommand::setCatalog(std::shared_ptr<BackupCatalog> catalog) {
  this->catalog = catalog;
}

CreateArchiveCatalogCommand::CreateArchiveCatalogCommand(shared_ptr<BackupCatalog> catalog) {

  this->catalog = catalog;
  this->tag = CREATE_ARCHIVE;

}

void CreateArchiveCatalogCommand::execute(bool existsOk) {

  shared_ptr<CatalogDescr> temp_descr(nullptr);

  /*
   * Die hard in case no catalog available.
   */
  if (this->catalog == NULL)
    throw CArchiveIssue("could not execute archive command: no catalog");

  /*
   * Open the catalog, if not yet done
   */
  if (!this->catalog->available()) {
    this->catalog->open_rw();
  }

  this->catalog->startTransaction();

  /*
   * If existsOk is TRUE and we have an
   * existing archive entry in the catalog, proceed.
   * Otherwise we die a hard and unpleasent immediate
   * death...
   */
  temp_descr = this->catalog->exists(this->directory);

  try {
    if (temp_descr->id < 0) {

      /*
       * This is a new archive entry.
       */
      this->catalog->createArchive((temp_descr = make_shared<CatalogDescr>(*this)));

      /*
       * Create the corresponding database connection entry.
       *
       * NOTE: createArchive() should have set the archive id
       *       in the catalog descriptor if successful.
       */
      temp_descr->setConnectionType(ConnectionDescr::CONNECTION_TYPE_BASEBACKUP);
      this->catalog->createCatalogConnection(temp_descr->coninfo);

      this->catalog->commitTransaction();

    }
    else {

      if (!existsOk) {
        ostringstream oss;
        oss << "archive already exists: \"" << this->directory << "\"";
        throw CArchiveIssue(oss.str());
      }

      /*
       * ID of the existing catalog entry needed.
       */
      this->id = temp_descr->id;
      this->catalog->startTransaction();

      /*
       * Update the existing archive catalog entry.
       */
      this->catalog->updateArchiveAttributes(make_shared<CatalogDescr>(*this),
                                             this->affectedAttributes);

      /*
       * ... and we're done.
       */
      this->catalog->commitTransaction();
    }
  } catch(CPGBackupCtlFailure& e) {
    this->catalog->rollbackTransaction();
    /* re-throw ... */
    throw e;
  }

}

BackgroundWorkerCommandHandle::BackgroundWorkerCommandHandle(std::shared_ptr<BackupCatalog> catalog) {
  this->tag = BACKGROUND_WORKER_COMMAND;
  this->subTag = EMPTY_DESCR;
  this->catalog = catalog;
}

BackgroundWorkerCommandHandle::BackgroundWorkerCommandHandle(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

  /*
   * Rewrite identity command tag, subTag transports
   * the encapsulated catalog command.
   */
  this->subTag = this->tag;
  this->tag = BACKGROUND_WORKER_COMMAND;

}

void BackgroundWorkerCommandHandle::execute(bool noop) {

}
