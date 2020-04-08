#define BOOST_TEST_MODULE TestBackupCatalog
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <BackupCatalog.hxx>

using namespace credativ;

BOOST_AUTO_TEST_CASE(TestBackupCatalogSetup)
{

  std::shared_ptr<BackupCatalog> catalog = nullptr;

  /* 1 should not throw */
  BOOST_REQUIRE_NO_THROW( BackupCatalog() );

  /* 2 Should throw */
  BOOST_CHECK_THROW( BackupCatalog ("/tmp/foobar"),
                     std::exception);

  /* 3 Create valid SQLite database handle */
  BOOST_REQUIRE_NO_THROW( catalog
                          = std::make_shared<BackupCatalog>(".pg_backup_ctl.sqlite") );

  /* 4 Open backup catalog for read/write */
  BOOST_REQUIRE_NO_THROW( catalog->open_rw() );

  /* 6 Close backup catalog database */
  BOOST_REQUIRE_NO_THROW( catalog->close() );

}

BOOST_AUTO_TEST_CASE(TestBackupCatalogManip)
{

  std::shared_ptr<BackupCatalog> catalog = nullptr;

  /* 1 should not throw */
  BOOST_REQUIRE_NO_THROW( catalog
                          = std::make_shared<BackupCatalog>(".pg_backup_ctl.sqlite") );

  /* 2 Open backup catalog for read/write */
  BOOST_REQUIRE_NO_THROW( catalog->open_rw() );

  /* 3 Backup catalog should be available */
  BOOST_CHECK( catalog->available() );

  /* 4 Close works */
  BOOST_REQUIRE_NO_THROW( catalog->close() );

  /* 5 Catalog not available anymore */
  BOOST_TEST( !catalog->available() );

}
