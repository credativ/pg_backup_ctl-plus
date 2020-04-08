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
#define NUM_SUCCESSFUL_PARSER_COMMANDS 9
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

  /* IMPORTANT: Keep that check in sync with the number of
     successful parser checks NUM_SUCCESSFUL_PARSER_COMMANDS */
  BOOST_TEST( count_parser_checks == NUM_SUCCESSFUL_PARSER_COMMANDS );

}
