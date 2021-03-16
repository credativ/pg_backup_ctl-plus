#define BOOST_TEST_MODULE TestOutputFormatter
#include <boost/test/unit_test.hpp>
#include <descr.hxx>
#include <output.hxx>

BOOST_AUTO_TEST_CASE(TestOutputFormatter)
{

  std::shared_ptr<BackupCatalog> catalog(nullptr);
  std::shared_ptr<OutputFormatConfiguration> output_configuration(nullptr);
  std::shared_ptr<OutputFormatter> formatter(nullptr);

  /*
   * OutputFormatter requires a configuration instance for
   * output parameters.
   */
  BOOST_REQUIRE_NOT_THROW( output_configuration
                           = std::make_shared<OutputFormatConfiguration>() );

  BOOST_REQUIRE_NO_THROW( catalog
                          = std::make_shared<BackupCatalog>(".pg_backup_ctl.sqlite") );

  /*
   * Instantiation of OutputFormatter should throw, since catalog not opened.
   */
  BOOST_CHECK_THROW( OutputFormatter::formatter(output_configuration,
                                                catalog,
                                                nullptr) );

}
