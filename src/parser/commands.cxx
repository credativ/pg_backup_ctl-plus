#include <boost/format.hpp>
#include <commands.hxx>
#include <stream.hxx>

using namespace credativ;

BaseCatalogCommand::~BaseCatalogCommand() {}

void BaseCatalogCommand::copy(CatalogDescr& source) {

  this->tag = source.tag;
  this->id  = source.id;
  this->archive_name = source.archive_name;
  this->label        = source.label;
  this->compression  = source.compression;
  this->directory    = source.directory;
  this->pghost       = source.pghost;
  this->pgport       = source.pgport;
  this->pguser       = source.pguser;
  this->pgdatabase   = source.pgdatabase;

  this->setAffectedAttributes(source.getAffectedAttributes());

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

  } catch(CPGBackupCtlFailure& e) {

    this->catalog->rollbackTransaction();
    throw e;

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
        cout << boost::format("%-20s\t%-30s") % "PGHOST" % descr->pghost << endl;
        cout << boost::format("%-20s\t%-30d") % "PGPORT" % descr->pgport << endl;
        cout << boost::format("%-20s\t%-30s") % "PGDATABASE" % descr->pgdatabase << endl;
        cout << boost::format("%-20s\t%-30s") % "PGUSER" % descr->pguser << endl;
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

StartBaseBackupCatalogCommand::StartBaseBackupCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

}

StartBaseBackupCatalogCommand::StartBaseBackupCatalogCommand() {
  this->tag = START_BASEBACKUP;
}

void StartBaseBackupCatalogCommand::execute(bool ignored) {

  std::shared_ptr<CatalogDescr> temp_descr(nullptr);

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

  /*
   * Check if the specified archive_name is present, get
   * its descriptor.
   */
  temp_descr = this->catalog->existsByName(this->archive_name);

  if (temp_descr->id < 0) {
    /* Requested archive doesn't exist, error out */
    std::ostringstream oss;
    oss << "archive " << this->archive_name << " doesn't exist";
    throw CArchiveIssue(oss.str());
  }

  try {

    /*
     * Setup PostgreSQL streaming object.
     */
    using namespace credativ::streaming;

    PGStream pgstream(temp_descr);

    /*
     * Get connection to the PostgreSQL instance.
     */
    pgstream.connect();

    /*
     * Identify this replication connection.
     */
    pgstream.identify();

    /*
     * Register information for the current basebackup in the catalog.
     */
    this->catalog->registerStream(temp_descr->id, pgstream.streamident);

    CompressedArchiveFile file(path(temp_descr->directory) / "test.gz");
    file.setOpenMode("w");
    file.open();
    file.write("This is a file with compression", 31);
    file.fsync();
    file.close();

#ifdef __DEBUG__
    std::cerr << "STREAM reg, id "
              << pgstream.streamident.id
              << " date "
              << pgstream.streamident.create_date
              << std::endl;
#endif

    /*
     * Enter basebackup stream.
     */

    /*
     * ...and finally drop the current stream.
     */
    this->catalog->dropStream(pgstream.streamident.id);

    /*
     * And disconnect
     */
    pgstream.disconnect();

  } catch(CPGBackupCtlFailure& e) {

    this->catalog->rollbackTransaction();
    /* re-throw ... */
    throw e;

  }

  this->catalog->commitTransaction();

}

CreateArchiveCatalogCommand::CreateArchiveCatalogCommand() {
  this->tag = CREATE_ARCHIVE;
}

CreateArchiveCatalogCommand::CreateArchiveCatalogCommand(std::shared_ptr<CatalogDescr> descr) {

  this->copy(*(descr.get()));

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
  temp_descr =this->catalog->exists(this->directory);

  try {
    if (temp_descr->id < 0) {

      /*
       * This is a new archive entry.
       */
      this->catalog->createArchive(make_shared<CatalogDescr>(*this));
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

