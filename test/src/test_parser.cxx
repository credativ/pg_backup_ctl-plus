#define BOOST_TEST_MODULE TestParser
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <parser.hxx>
#include <BackupCatalog.hxx>

using namespace credativ;

/*
 * Number of successful parser command checks.
 *
 * NOTE: This needs to be in sync if you add or remove parser
 *       command checks.
 */
#define NUM_SUCCESSFUL_PARSER_COMMANDS 62
#define COMMAND_IS_VALID(cmd, number) ( ((cmd) != nullptr) && ((number)++ > 0) )

BOOST_AUTO_TEST_CASE(TestParser)
{
  std::shared_ptr<RuntimeConfiguration> rtconfig = nullptr;
  std::shared_ptr<PGBackupCtlCommand> command = nullptr;
  PGBackupCtlParser parser;

  int count_parser_checks = 0;

  /* Need a runtime configuration handler */
  BOOST_REQUIRE_NO_THROW( rtconfig = std::make_shared<RuntimeConfiguration>() );

  /* no debug output */
  std::shared_ptr<ConfigVariable> log_level;
  BOOST_REQUIRE_NO_THROW( (log_level = rtconfig->create("logging.level",
                                                        std::string("info"),
                                                        std::string("info"))) );
  BOOST_REQUIRE_NO_THROW( log_level->set_assign_hook(CPGBackupCtlBase::set_log_severity) );
  BOOST_REQUIRE_NO_THROW( log_level->reassign() );

  /* Runtime configuration is empty */
  BOOST_REQUIRE_NO_THROW( parser = PGBackupCtlParser(rtconfig) );

  /* Empty command, should throw */
  BOOST_CHECK_THROW( parser.parseLine(""),
                     CParserIssue );

  /* Should throw, invalid command */
  BOOST_CHECK_THROW( parser.parseLine("WRONG COMMAND"),
                     CParserIssue );

  /* 1 LIST BACKUP CATALOG command, should succeed */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("LIST BACKUP CATALOG test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == LIST_BACKUP_CATALOG) );

  /* 2 LIST ARCHIVE command, should succeed */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("LIST ARCHIVE") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == LIST_ARCHIVE) );

  /* 3 LIST BACKUP PROFILE command, should succeed */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("LIST BACKUP PROFILE") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == LIST_BACKUP_PROFILE) );


  /* 4 LIST BASEBACKUPS command, should succeed */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("LIST BASEBACKUPS IN ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == LIST_BACKUP_LIST) );

  /* 5 LIST BASEBACKUPS ... VERBOSE command, should succeed */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("LIST BASEBACKUPS IN ARCHIVE test VERBOSE") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == LIST_BACKUP_LIST) );

  /* 6 LIST CONNECTION command, should succeed */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("LIST CONNECTION FOR ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == LIST_CONNECTION) );

  /* 7 LIST RETENTION POLICIES command, should succeed */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("LIST RETENTION POLICIES") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == LIST_RETENTION_POLICIES) );

  /* 8 LIST RETENTION POLICY command, should succeed */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("LIST RETENTION POLICY policy") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == LIST_RETENTION_POLICY) );

  /* 9 CREATE BACKUP PROFILE <name> command, should work (creates a profile with default options */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<BackupProfileDescr> backup_profile = descr->getBackupProfileDescr();

    /* Check for all default values */
    BOOST_TEST( (backup_profile != nullptr) );
    BOOST_TEST( (!backup_profile->manifest) );
    BOOST_TEST( (backup_profile->name == "test") );
    BOOST_TEST( (backup_profile->compress_type == BACKUP_COMPRESS_TYPE_NONE) );
    BOOST_TEST( (backup_profile->max_rate == 0) );
    BOOST_TEST( (backup_profile->label == "PG_BCK_CTL BASEBACKUP") );
    BOOST_TEST( (!backup_profile->fast_checkpoint) );
    BOOST_TEST( (!backup_profile->include_wal) );
    BOOST_TEST( (backup_profile->wait_for_wal) );
    BOOST_TEST( (!backup_profile->noverify_checksums) );
    BOOST_TEST( (backup_profile->manifest_checksums == "CRC32C") );

    /* default checksum mode is CRC32C */
    BOOST_TEST( (backup_profile->manifest_checksums == "CRC32C") );

  }

  /* 10 CREATE BACKUP PROFILE test MANIFEST INCLUDED */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MANIFEST INCLUDED") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<BackupProfileDescr> backup_profile = descr->getBackupProfileDescr();

    BOOST_TEST( (backup_profile != nullptr) );
    BOOST_TEST( (backup_profile->manifest) );

    /* default checksum mode is CRC32C */
    BOOST_TEST( (backup_profile->manifest_checksums == "CRC32C") );

  }

  /* 11 CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS NONE */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MANIFEST INCLUDED WITH CHECKSUMS NONE") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<BackupProfileDescr> backup_profile = descr->getBackupProfileDescr();

    BOOST_TEST( (backup_profile != nullptr) );
    BOOST_TEST( (backup_profile->manifest) );
    BOOST_TEST( (backup_profile->manifest_checksums == "NONE") );

  }

  /* 12 CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS SHA224 */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MANIFEST INCLUDED WITH CHECKSUMS SHA224") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<BackupProfileDescr> backup_profile = descr->getBackupProfileDescr();

    BOOST_TEST( (backup_profile != nullptr) );
    BOOST_TEST( (backup_profile->manifest) );
    BOOST_TEST( (backup_profile->manifest_checksums == "SHA224") );

  }

  /* 13 CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS SHA256 */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MANIFEST INCLUDED WITH CHECKSUMS SHA256") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<BackupProfileDescr> backup_profile = descr->getBackupProfileDescr();

    BOOST_TEST( (backup_profile != nullptr) );
    BOOST_TEST( (backup_profile->manifest) );
    BOOST_TEST( (backup_profile->manifest_checksums == "SHA256") );

  }

  /* 14 CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS SHA384 */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MANIFEST INCLUDED WITH CHECKSUMS SHA384") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<BackupProfileDescr> backup_profile = descr->getBackupProfileDescr();

    BOOST_TEST( (backup_profile != nullptr) );
    BOOST_TEST( (backup_profile->manifest) );
    BOOST_TEST( (backup_profile->manifest_checksums == "SHA384") );

  }

  /* 15 CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS SHA512 */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MANIFEST INCLUDED WITH CHECKSUMS SHA512") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<BackupProfileDescr> backup_profile = descr->getBackupProfileDescr();

    BOOST_TEST( (backup_profile != nullptr) );
    BOOST_TEST( (backup_profile->manifest) );
    BOOST_TEST( (backup_profile->manifest_checksums == "SHA512") );

  }

  /* 16 CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS CRC32 */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MANIFEST INCLUDED WITH CHECKSUMS CRC32C") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<BackupProfileDescr> backup_profile = descr->getBackupProfileDescr();

    BOOST_TEST( (backup_profile != nullptr) );
    BOOST_TEST( (backup_profile->manifest) );
    BOOST_TEST( (backup_profile->manifest_checksums == "CRC32C") );

  }

  /* 17 RESTORE FROM ARCHIVE abc BASEBACKUP 1 TO DIRECTORY="/tmp/backup" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("RESTORE FROM ARCHIVE abc BASEBACKUP 1 TO DIRECTORY=\"/tmp/backup\"" ) );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == RESTORE_BACKUP) );

  /* 18 RESTORE abc BASEBACKUP 1 TO DIRECTORY="/tmp/backup" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("RESTORE abc BASEBACKUP 1 TO DIRECTORY=\"/tmp/backup\"" ) );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == RESTORE_BACKUP) );

  /* 19 RESTORE FROM ARCHIVE abc BASEBACKUP latest TO DIRECTORY="/tmp/restore" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("RESTORE abc BASEBACKUP latest TO DIRECTORY=\"/tmp/backup\"" ) );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == RESTORE_BACKUP) );

  /* 20 RESTORE FROM ARCHIVE abc BASEBACKUP current TO DIRECTORY="/tmp/restore" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("RESTORE abc BASEBACKUP current TO DIRECTORY=\"/tmp/backup\"" ) );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == RESTORE_BACKUP) );

  /* 21 RESTORE FROM ARCHIVE abc BASEBACKUP newest TO DIRECTORY="/tmp/restore" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("RESTORE abc BASEBACKUP current TO DIRECTORY=\"/tmp/backup\"" ) );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == RESTORE_BACKUP) );

  /* 22 RESTORE FROM ARCHIVE abc BASEBACKUP oldest TO DIRECTORY="/tmp/restore" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("RESTORE abc BASEBACKUP current TO DIRECTORY=\"/tmp/backup\"" ) );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == RESTORE_BACKUP) );

  /* 23 RESTORE abc BASEBACKUP current TO DIRECTORY="/tmp/restore-11" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("RESTORE abc BASEBACKUP current TO DIRECTORY=\"/tmp/backup-11\"" ) );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == RESTORE_BACKUP) );

  /* 24 RESTORE abc BASEBACKUP current TO DIRECTORY="/tmp/restore-11" TABLESPACE MAP ALL="/tmp/tablespaces-11" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("RESTORE abc BASEBACKUP current TO DIRECTORY=\"/tmp/backup-11\" TABLESPACE MAP ALL=\"/tmp/tablespaces-11\"" ) );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == RESTORE_BACKUP) );

  /* 25 RESTORE abc BASEBACKUP current TO DIRECTORY="/tmp/restore-11" TABLESPACE MAP 18990="/tmp/tablespace_1 18991="/tmp/tablespace_2" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("RESTORE abc BASEBACKUP current TO DIRECTORY=\"/tmp/backup-11\" TABLESPACE MAP 18990=\"/tmp/tablespace_1\" 18991=\"/tmp/tablespace_2\"" ) );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == RESTORE_BACKUP) );

  /*
   * Should fail, OID=0 is reserved for pg_default tablespace and isn't allowed to be redirected
   *
   * RESTORE abc BASEBACKUP current TO DIRECTORY="/tmp/restore-11" TABLESPACE MAP 0=/tmp/failed"
   *
   * Please note that we don't count this test here, since negative parser tests aren't
   * part of NUM_SUCCESSFUL_PARSER_COMMANDS
   */
  BOOST_REQUIRE_THROW( parser.parseLine("RESTORE abc BASEBACKUP current TO DIRECTORY=\"/tmp/backup-11\" TABLESPACE MAP 0=/tmp/failed" ),
                       CCatalogIssue );

  /*
   * Should fail, OID=113 is specified twice.
   *
   * RESTORE abc BASEBACKUP current TO DIRECTORY="/tmp/restore-11" TABLESPACE MAP 113=/tmp/1 113=/tmp/2"
   *
   * Please note that we don't count this test here, since negative parser tests aren't
   * part of NUM_SUCCESSFUL_PARSER_COMMANDS
   */
  BOOST_REQUIRE_THROW( parser.parseLine("RESTORE abc BASEBACKUP current TO DIRECTORY=\"/tmp/backup-11\" TABLESPACE MAP 113=\"/tmp/1\" 113=\"/tmp/2\"" ),
                       CCatalogIssue );

  /*
   * Should fail, since tablespace restore location /tmp is already mapped to OID 113 before.
   *
   * RESTORE abc BASEBACKUP current TO DIRECTORY="/tmp/restore-11" TABLESPACE MAP 113=/tmp/1 114=/tmp/1"
   *
   * Please note that we don't count this test here, since negative parser tests aren't
   * part of NUM_SUCCESSFUL_PARSER_COMMANDS
   */
  BOOST_REQUIRE_THROW( parser.parseLine("RESTORE abc BASEBACKUP current TO DIRECTORY=\"/tmp/backup-11\" TABLESPACE MAP 113=\"/tmp/1\" 114=\"/tmp/1\"" ),
                       CCatalogIssue );

  /* 26 STAT ARCHIVE abc BASEBACKUP 31 */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("STAT ARCHIVE abc BASEBACKUP 31") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == STAT_ARCHIVE_BASEBACKUP) );

  /* 27 CREATE BACKUP PROFILE test MAX_RATE 987651 */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MAX_RATE 987651") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<BackupProfileDescr> backup_profile = descr->getBackupProfileDescr();

    BOOST_TEST( (backup_profile != nullptr) );
    BOOST_TEST( (backup_profile->max_rate == 987651) );

  }

  /* 28 CREATE BACKUP PROFILE test COMPRESSION GZIP */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test COMPRESSION GZIP") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<BackupProfileDescr> backup_profile = descr->getBackupProfileDescr();

    BOOST_TEST( (backup_profile != nullptr) );
    BOOST_TEST( (backup_profile->compress_type = BACKUP_COMPRESS_TYPE_GZIP) );

  }

  /* 29 CREATE BACKUP PROFILE test COMPRESSION XZ */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test COMPRESSION XZ") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<BackupProfileDescr> backup_profile = descr->getBackupProfileDescr();

    BOOST_TEST( (backup_profile != nullptr) );
    BOOST_TEST( (backup_profile->compress_type = BACKUP_COMPRESS_TYPE_XZ) );

  }

  /* 30 CREATE BACKUP PROFILE test COMPRESSION ZSTD */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test COMPRESSION ZSTD") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<BackupProfileDescr> backup_profile = descr->getBackupProfileDescr();

    BOOST_TEST( (backup_profile != nullptr) );
    BOOST_TEST( (backup_profile->compress_type = BACKUP_COMPRESS_TYPE_ZSTD) );

  }

  /* 31 CREATE BACKUP PROFILE test COMPRESSION PLAIN */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test COMPRESSION PLAIN") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<BackupProfileDescr> backup_profile = descr->getBackupProfileDescr();

    BOOST_TEST( (backup_profile != nullptr) );
    BOOST_TEST( (backup_profile->compress_type = BACKUP_COMPRESS_TYPE_PLAIN) );

  }

  /* 32 CREATE ARCHIVE test PARAMS DIRECTORY="/tmp" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE ARCHIVE test PARAMS DIRECTORY=\"/tmp/_path123_.somewhere\" DSN=\"host=localhost application_name=backup dbname=test\"") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_ARCHIVE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();

    BOOST_TEST( (descr->archive_name == "test") );
    BOOST_TEST( (descr->directory == "/tmp/_path123_.somewhere") );

    /* Check connection info */
    BOOST_TEST( (descr->coninfo != nullptr) );
    BOOST_TEST( (descr->coninfo->dsn == "host=localhost application_name=backup dbname=test") );

  }

  /* 33 CREATE ARCHIVE test PARAMS DIRECTORY="/tmp" PGHOST=localhost PGDATABASE=test PGUSER=test PGPORT=5466 */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE ARCHIVE test PARAMS DIRECTORY=\"/tmp\" PGHOST=localhost PGDATABASE=test PGUSER=test PGPORT=5466") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_ARCHIVE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();

    BOOST_TEST( (descr->archive_name == "test") );
    BOOST_TEST( (descr->directory == "/tmp") );

    /* Check connection info */
    BOOST_TEST( (descr->coninfo != nullptr) );
    BOOST_TEST( (descr->coninfo->pghost == "localhost") );
    BOOST_TEST( (descr->coninfo->pgport == 5466) );
    BOOST_TEST( (descr->coninfo->pguser == "test") );
    BOOST_TEST( (descr->coninfo->pgdatabase == "test") );

  }

  /* 34 CREATE ARCHIVE test PARAMS DIRECTORY="/tmp" PGDATABASE=test PGPORT=5466 */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE ARCHIVE test PARAMS DIRECTORY=\"/tmp\" PGHOST=localhost PGDATABASE=test PGUSER=test PGPORT=5466") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_ARCHIVE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();

    BOOST_TEST( (descr->archive_name == "test") );
    BOOST_TEST( (descr->directory == "/tmp") );

    /* Check connection info */
    BOOST_TEST( (descr->coninfo != nullptr) );
    BOOST_TEST( (descr->coninfo->pgport == 5466) );
    BOOST_TEST( (descr->coninfo->pgdatabase == "test") );

  }

  /* 35 APPLY RETENTION POLICY test TO ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("APPLY RETENTION POLICY test TO ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == APPLY_RETENTION_POLICY) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    BOOST_TEST( (descr != nullptr) );
    BOOST_TEST( (descr->retention_name == "test") );

  }

  /* 36 ALTER ARCHIVE test SET DSN="host=localhost user=bernd port=5432" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("ALTER ARCHIVE test SET DSN=\"host=localhost user=bernd port=5432\"") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == ALTER_ARCHIVE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    BOOST_TEST( (descr != nullptr) );
    BOOST_TEST( (descr->archive_name == "test") );

    /* Check connection descriptor for new DSN */
    BOOST_TEST( (descr->coninfo->dsn == "host=localhost user=bernd port=5432") );

  }

  /* 37 ALTER ARCHIVE test SET PGHOST=localhost PGUSER=bernd" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("ALTER ARCHIVE test SET PGHOST=test.dbserver.lan PGUSER=bernd") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == ALTER_ARCHIVE) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    BOOST_TEST( (descr != nullptr) );
    BOOST_TEST( (descr->archive_name == "test") );

    /* Check connection descriptor for new DSN */
    BOOST_TEST( (descr->coninfo->pghost == "test.dbserver.lan") );
    BOOST_TEST( (descr->coninfo->pguser == "bernd") );

  }

  /* 38 CREATE RETENTION POLICY cleanup CLEANUP" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE RETENTION POLICY cleanup CLEANUP") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_RETENTION_POLICY) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<RetentionDescr> retentionPolicy;

    BOOST_TEST( (descr != nullptr) );

    retentionPolicy = descr->getRetentionPolicyP();
    BOOST_TEST( (retentionPolicy != nullptr) );
    BOOST_TEST( (retentionPolicy->name == "cleanup") );

    /* Check rule, in this case a single CLEANUP rule is expected */
    BOOST_TEST( (retentionPolicy->rules.size() == 1) );

    std::shared_ptr<RetentionRuleDescr> rule = retentionPolicy->rules[0];
    BOOST_TEST( (rule->type == RETENTION_CLEANUP) );

  }

  /* 39 CREATE RETENTION POLICY dropafteroneday DROP OLDER THAN 1 DAYS" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE RETENTION POLICY dropafteroneday DROP OLDER THAN 1 DAYS") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_RETENTION_POLICY) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<RetentionDescr> retentionPolicy;

    BOOST_TEST( (descr != nullptr) );

    retentionPolicy = descr->getRetentionPolicyP();
    BOOST_TEST( (retentionPolicy != nullptr) );
    BOOST_TEST( (retentionPolicy->name == "dropafteroneday") );

    /* Check rule, in this case a single RETENTION_DROP_OLDER_BY_DATETIME rule is expected */
    BOOST_TEST( (retentionPolicy->rules.size() == 1) );

    std::shared_ptr<RetentionRuleDescr> rule = retentionPolicy->rules[0];
    BOOST_TEST( (rule->type == RETENTION_DROP_OLDER_BY_DATETIME) );

    RetentionIntervalDescr intv;
    BOOST_REQUIRE_NO_THROW((intv = RetentionIntervalDescr(rule->value)));
    BOOST_TEST( (intv.compile() == "-1 days") );

    /*
     * Should generate a sqlite3 datetime() function call with one placeholder for
     * the interval operand
     */
    BOOST_TEST( (intv.sqlite3_datetime() == "datetime('now','localtime',?1)") );

  }

  /* 40 CREATE RETENTION POLICY dropone DROP +1" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE RETENTION POLICY droptwo DROP +2") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_RETENTION_POLICY) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<RetentionDescr> retentionPolicy;

    BOOST_TEST( (descr != nullptr) );

    retentionPolicy = descr->getRetentionPolicyP();
    BOOST_TEST( (retentionPolicy != nullptr) );
    BOOST_TEST( (retentionPolicy->name == "droptwo") );

    /* Check rule, in this case a single RETENTION_DROP_NUM rule is expected */
    BOOST_TEST( (retentionPolicy->rules.size() == 1) );

    std::shared_ptr<RetentionRuleDescr> rule = retentionPolicy->rules[0];
    BOOST_TEST( (rule->type == RETENTION_DROP_NUM) );
    BOOST_TEST( (rule->value == "2") );

  }

  /* 41 CREATE RETENTION POLICY dropfancybackup DROP WITH LABEL .*fancy\\s+backup.*" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE RETENTION POLICY dropfancybackup DROP WITH LABEL .*fancy\\s+backup.*") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_RETENTION_POLICY) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<RetentionDescr> retentionPolicy;

    BOOST_TEST( (descr != nullptr) );

    retentionPolicy = descr->getRetentionPolicyP();
    BOOST_TEST( (retentionPolicy != nullptr) );
    BOOST_TEST( (retentionPolicy->name == "dropfancybackup") );

    /* Check rule, in this case a single RETENTION_DROP_WITH_LABEL rule is expected */
    BOOST_TEST( (retentionPolicy->rules.size() == 1) );

    std::shared_ptr<RetentionRuleDescr> rule = retentionPolicy->rules[0];
    BOOST_TEST( (rule->type == RETENTION_DROP_WITH_LABEL) );
    BOOST_TEST( (rule->value == ".*fancy\\s+backup.*") );

  }

  /* 42 CREATE RETENTION POLICY keepimportantbackup KEEP WITH LABEL .*very\\s+important\\s+backup.*" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE RETENTION POLICY keepimportantbackup KEEP WITH LABEL .*very\\s+important\\s+backup.*") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_RETENTION_POLICY) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<RetentionDescr> retentionPolicy;

    BOOST_TEST( (descr != nullptr) );

    retentionPolicy = descr->getRetentionPolicyP();
    BOOST_TEST( (retentionPolicy != nullptr) );
    BOOST_TEST( (retentionPolicy->name == "keepimportantbackup") );

    /* Check rule, in this case a single RETENTION_KEEP_WITH_LABEL rule is expected */
    BOOST_TEST( (retentionPolicy->rules.size() == 1) );

    std::shared_ptr<RetentionRuleDescr> rule = retentionPolicy->rules[0];
    BOOST_TEST( (rule->type == RETENTION_KEEP_WITH_LABEL) );
    BOOST_TEST( (rule->value == ".*very\\s+important\\s+backup.*") );

  }

  /* 43 CREATE RETENTION POLICY keepimportantbackup KEEP NEWER THAN 14 DAYS 6 HOURS 30 MINUTES */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE RETENTION POLICY keepimportantbackup KEEP NEWER THAN 14 DAYS 6 HOURS 30 MINUTES") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_RETENTION_POLICY) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<RetentionDescr> retentionPolicy;

    BOOST_TEST( (descr != nullptr) );

    retentionPolicy = descr->getRetentionPolicyP();
    BOOST_TEST( (retentionPolicy != nullptr) );
    BOOST_TEST( (retentionPolicy->name == "keepimportantbackup") );

    /* Check rule, in this case a single RETENTION_KEEP_NEWER_BY_DATETIME rule is expected */
    BOOST_TEST( (retentionPolicy->rules.size() == 1) );

    std::shared_ptr<RetentionRuleDescr> rule = retentionPolicy->rules[0];
    BOOST_TEST( (rule->type == RETENTION_KEEP_NEWER_BY_DATETIME) );
    BOOST_TEST( (rule->value == "+14 days|+6 hours|+30 minutes") );

    RetentionIntervalDescr intv(rule->value);

    /*
     * sqlite3_datetime() should format a datetime() function
     * expression with 3 placeholders. */
    BOOST_TEST( (intv.sqlite3_datetime() == "datetime('now','localtime',?1, ?2, ?3)") );

    /* Compile expression */
    BOOST_TEST( (intv.compile() == "+14 days|+6 hours|+30 minutes") );

  }

  /* 44 CREATE RETENTION POLICY dropveryold DROP OLDER THAN 1 YEARS 6 MONTHS 15 HOURS 30 MINUTES */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE RETENTION POLICY dropveryold DROP OLDER THAN 1 YEARS 6 MONTHS 15 HOURS 30 MINUTES") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_RETENTION_POLICY) );

    std::shared_ptr<CatalogDescr> descr = command->getExecutableDescr();
    std::shared_ptr<RetentionDescr> retentionPolicy;

    BOOST_TEST( (descr != nullptr) );

    retentionPolicy = descr->getRetentionPolicyP();
    BOOST_TEST( (retentionPolicy != nullptr) );
    BOOST_TEST( (retentionPolicy->name == "dropveryold") );

    /* Check rule, in this case a single RETENTION_DROP_OLDER_BY_DATETIME rule is expected */
    BOOST_TEST( (retentionPolicy->rules.size() == 1) );

    std::shared_ptr<RetentionRuleDescr> rule = retentionPolicy->rules[0];
    BOOST_TEST( (rule->type == RETENTION_DROP_OLDER_BY_DATETIME) );
    BOOST_TEST( (rule->value == "-1 years|-6 months|-15 hours|-30 minutes") );

    RetentionIntervalDescr intv(rule->value);

    /*
     * sqlite3_datetime() should format a datetime() function
     * expression with 4 placeholders. */
    BOOST_TEST( (intv.sqlite3_datetime() == "datetime('now','localtime',?1, ?2, ?3, ?4)") );

    /* Compile expression */
    BOOST_TEST( (intv.compile() == "-1 years|-6 months|-15 hours|-30 minutes") );

  }

  /* 45 CREATE STREAMING CONNECTION FOR ARCHIVE test DSN "host=localhost application_name='pg_backup_ctl++ wal streamer' port=56565" */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE STREAMING CONNECTION FOR ARCHIVE test DSN \"host=localhost application_name='pg_backup_ctl++ wal streamer' port=56565\"") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == CREATE_CONNECTION) );

  }

  /* 46 LIST ARCHIVE test VERBOSE */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("LIST ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == LIST_ARCHIVE) );

  }

  /* 47 LIST CONNECTION FOR ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("LIST CONNECTION FOR ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == LIST_CONNECTION) );

  }

  /* 48 DROP ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("DROP ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == DROP_ARCHIVE) );

  }

  /* 49 DROP STREAMING CONNECTION FROM ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("DROP STREAMING CONNECTION FROM ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == DROP_CONNECTION) );

  }

  /* 50 PIN OLDEST IN ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("PIN oldest IN ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    std::shared_ptr<CatalogDescr> descr = nullptr;

    BOOST_TEST( (command->getCommandTag() == PIN_BASEBACKUP) );

    BOOST_REQUIRE_NO_THROW( (descr = command->getExecutableDescr()) );
    BOOST_TEST( (descr != nullptr) );
    BOOST_TEST( (descr->archive_name == "test") );
    BOOST_TEST( (descr->pinDescr.getOperationType() == ACTION_OLDEST) );

  }

  /* 51 PIN NEWEST IN ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("PIN newest IN ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    std::shared_ptr<CatalogDescr> descr = nullptr;

    BOOST_TEST( (command->getCommandTag() == PIN_BASEBACKUP) );

    BOOST_REQUIRE_NO_THROW( (descr = command->getExecutableDescr()) );
    BOOST_TEST( (descr != nullptr) );
    BOOST_TEST( (descr->archive_name == "test") );
    BOOST_TEST( (descr->pinDescr.getOperationType() == ACTION_NEWEST) );

  }

  /* 52 PIN +3 IN ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("PIN +3 IN ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    std::shared_ptr<CatalogDescr> descr = nullptr;

    BOOST_TEST( (command->getCommandTag() == PIN_BASEBACKUP) );

    BOOST_REQUIRE_NO_THROW( (descr = command->getExecutableDescr()) );
    BOOST_TEST( (descr != nullptr) );
    BOOST_TEST( (descr->archive_name == "test") );
    BOOST_TEST( (descr->pinDescr.getOperationType() == ACTION_COUNT) );

    BOOST_REQUIRE_NO_THROW( (descr->pinDescr.getCount()) );
    BOOST_TEST( (descr->pinDescr.getCount() == 3) );

  }

  /* 53 UNPIN +3 IN ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("UNPIN +3 IN ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    std::shared_ptr<CatalogDescr> descr = nullptr;

    BOOST_TEST( (command->getCommandTag() == UNPIN_BASEBACKUP) );

    BOOST_REQUIRE_NO_THROW( (descr = command->getExecutableDescr()) );
    BOOST_TEST( (descr != nullptr) );
    BOOST_TEST( (descr->archive_name == "test") );
    BOOST_TEST( (descr->pinDescr.getOperationType() == ACTION_COUNT) );

    BOOST_REQUIRE_NO_THROW( (descr->pinDescr.getCount()) );
    BOOST_TEST( (descr->pinDescr.getCount() == 3) );

  }

  /* 54 UNPIN NEWEST IN ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("UNPIN NEWEST IN ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    std::shared_ptr<CatalogDescr> descr = nullptr;

    BOOST_TEST( (command->getCommandTag() == UNPIN_BASEBACKUP) );

    BOOST_REQUIRE_NO_THROW( (descr = command->getExecutableDescr()) );
    BOOST_TEST( (descr != nullptr) );
    BOOST_TEST( (descr->archive_name == "test") );
    BOOST_TEST( (descr->pinDescr.getOperationType() == ACTION_NEWEST) );

  }

  /* 55 UNPIN OLDEST IN ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("UNPIN OLDEST IN ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    std::shared_ptr<CatalogDescr> descr = nullptr;

    BOOST_TEST( (command->getCommandTag() == UNPIN_BASEBACKUP) );

    BOOST_REQUIRE_NO_THROW( (descr = command->getExecutableDescr()) );
    BOOST_TEST( (descr != nullptr) );
    BOOST_TEST( (descr->archive_name == "test") );
    BOOST_TEST( (descr->pinDescr.getOperationType() == ACTION_OLDEST) );

  }

  /* 56 START BASEBACKUP FOR ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("START BASEBACKUP FOR ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    BOOST_TEST( (command->getCommandTag() == START_BASEBACKUP) );

  }

  /* 57 START BASEBACKUP FOR ARCHIVE test PROFILE foo */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("START BASEBACKUP FOR ARCHIVE test PROFILE foo") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    std::shared_ptr<CatalogDescr> descr = nullptr;

    BOOST_TEST( (command->getCommandTag() == START_BASEBACKUP) );
    BOOST_REQUIRE_NO_THROW( (descr = command->getExecutableDescr()) );

    BOOST_TEST( (descr->getBackupProfileDescr() != nullptr) );
    BOOST_TEST( (descr->getBackupProfileDescr()->name == "foo") );

  }

  /* 58 START BASEBACKUP FOR ARCHIVE test PROFILE bla FORCE_SYSTEMID_UPDATE */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("START BASEBACKUP FOR ARCHIVE test PROFILE bla FORCE_SYSTEMID_UPDATE") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    std::shared_ptr<CatalogDescr> descr = nullptr;

    BOOST_TEST( (command->getCommandTag() == START_BASEBACKUP) );
    BOOST_REQUIRE_NO_THROW( (descr = command->getExecutableDescr()) );

    BOOST_TEST( (descr->getBackupProfileDescr() != nullptr) );
    BOOST_TEST( (descr->getBackupProfileDescr()->name == "bla") );
    BOOST_TEST( (descr->force_systemid_update) );

  }

  /* 59 START RECOVERY STREAM FOR ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("START RECOVERY STREAM FOR ARCHIVE test") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    std::shared_ptr<CatalogDescr> descr = nullptr;
    std::shared_ptr<RecoveryStreamDescr> recovery = nullptr;

    BOOST_TEST( (command->getCommandTag() == START_RECOVERY_STREAM_FOR_ARCHIVE) );
    BOOST_REQUIRE_NO_THROW( (descr = command->getExecutableDescr()) );

    BOOST_TEST( (descr != nullptr) );
    BOOST_REQUIRE_NO_THROW( (recovery = descr->getRecoveryStreamDescr()) );
    BOOST_TEST( (recovery != nullptr) );

    BOOST_TEST( (recovery->port == RecoveryStreamDescr::DEFAULT_RECOVERY_STREAM_PORT) );

  }

  /* 60 START RECOVERY STREAM FOR ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("START RECOVERY STREAM FOR ARCHIVE test PORT 8880") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    std::shared_ptr<CatalogDescr> descr = nullptr;
    std::shared_ptr<RecoveryStreamDescr> recovery = nullptr;

    BOOST_TEST( (command->getCommandTag() == START_RECOVERY_STREAM_FOR_ARCHIVE) );
    BOOST_REQUIRE_NO_THROW( (descr = command->getExecutableDescr()) );

    BOOST_TEST( (descr != nullptr) );
    BOOST_REQUIRE_NO_THROW( (recovery = descr->getRecoveryStreamDescr()) );
    BOOST_TEST( (recovery != nullptr) );

    BOOST_TEST( (recovery->port == 8880) );

  }

  /* 61 START RECOVERY STREAM FOR ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("START RECOVERY STREAM FOR ARCHIVE test PORT 8880 LISTEN_ON (::1)") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    std::shared_ptr<CatalogDescr> descr = nullptr;
    std::shared_ptr<RecoveryStreamDescr> recovery = nullptr;

    BOOST_TEST( (command->getCommandTag() == START_RECOVERY_STREAM_FOR_ARCHIVE) );
    BOOST_REQUIRE_NO_THROW( (descr = command->getExecutableDescr()) );

    BOOST_TEST( (descr != nullptr) );
    BOOST_REQUIRE_NO_THROW( (recovery = descr->getRecoveryStreamDescr()) );
    BOOST_TEST( (recovery != nullptr) );

    BOOST_TEST( (recovery->port == 8880) );

    /*
     * There should be at least one listen address
     *
     * XXX: Currently we support just one bind instance of recovery
     *      streams
     */
    BOOST_TEST( (recovery->listen_on.size() == 1) );
    BOOST_TEST( (recovery->listen_on[0] == "::1") );

  }

  /* 62 START RECOVERY STREAM FOR ARCHIVE test */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("START RECOVERY STREAM FOR ARCHIVE test LISTEN_ON (::1)") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if (COMMAND_IS_VALID(command, count_parser_checks)) {

    std::shared_ptr<CatalogDescr> descr = nullptr;
    std::shared_ptr<RecoveryStreamDescr> recovery = nullptr;

    BOOST_TEST( (command->getCommandTag() == START_RECOVERY_STREAM_FOR_ARCHIVE) );
    BOOST_REQUIRE_NO_THROW( (descr = command->getExecutableDescr()) );

    BOOST_TEST( (descr != nullptr) );
    BOOST_REQUIRE_NO_THROW( (recovery = descr->getRecoveryStreamDescr()) );
    BOOST_TEST( (recovery != nullptr) );

    BOOST_TEST( (recovery->port == 7432) );

    /*
     * There should be at least one listen address
     *
     * XXX: Currently we support just one bind instance of recovery
     *      streams
     */
    BOOST_TEST( (recovery->listen_on.size() == 1) );
    BOOST_TEST( (recovery->listen_on[0] == "::1") );

  }

  /* IMPORTANT: Keep that check in sync with the number of
   * successful parser checks NUM_SUCCESSFUL_PARSER_COMMANDS
   *
   * !!! THIS SHOULD BE THE LAST CHECK !!!
   */
  BOOST_TEST( count_parser_checks == NUM_SUCCESSFUL_PARSER_COMMANDS );

}
