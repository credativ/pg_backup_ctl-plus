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
#define NUM_SUCCESSFUL_PARSER_COMMANDS 26
#define COMMAND_IS_VALID(cmd, number) ( ((cmd) != nullptr) && ((number)++ > 0) )

BOOST_AUTO_TEST_CASE(TestParser)
{
  std::shared_ptr<RuntimeConfiguration> rtconfig = nullptr;
  std::shared_ptr<PGBackupCtlCommand> command = nullptr;
  PGBackupCtlParser parser;

  int count_parser_checks = 0;

  /* Need a runtime configuration handler */
  BOOST_REQUIRE_NO_THROW( rtconfig = std::make_shared<RuntimeConfiguration>() );

  /* Runtime configuration is empty */
  BOOST_REQUIRE_NO_THROW( parser = PGBackupCtlParser(rtconfig) );

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

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

  /* 10 CREATE BACKUP PROFILE test MANIFEST TRUE */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MANIFEST TRUE") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

  /* 11 CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS NONE */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS NONE") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

  /* 12 CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS SHA224 */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS SHA224") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

  /* 13 CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS SHA256 */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS SHA256") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

  /* 14 CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS SHA384 */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS SHA384") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

  /* 15 CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS SHA512 */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS SHA512") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

  /* 16 CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS CRC32 */
  BOOST_REQUIRE_NO_THROW( parser.parseLine("CREATE BACKUP PROFILE test MANIFEST_CHECKSUMS CRC32C") );

  command = parser.getCommand();
  BOOST_TEST( (command != nullptr) );

  if COMMAND_IS_VALID(command, count_parser_checks) BOOST_TEST( (command->getCommandTag() == CREATE_BACKUP_PROFILE) );

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

  /* IMPORTANT: Keep that check in sync with the number of
     successful parser checks NUM_SUCCESSFUL_PARSER_COMMANDS */
  BOOST_TEST( count_parser_checks == NUM_SUCCESSFUL_PARSER_COMMANDS );

}
