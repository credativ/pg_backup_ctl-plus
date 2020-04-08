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

  /* 4 Create archive "test" */
  {
    std::shared_ptr<CatalogDescr> desc = std::make_shared<CatalogDescr>();
    std::shared_ptr<CatalogDescr> check_desc;

    /*
     * Set connection type. this is important since BackupCatalog::createArchive()
     * insists this to be set!
     */
    desc->coninfo->type = ConnectionDescr::CONNECTION_TYPE_BASEBACKUP;
    desc->archive_name = "test";
    desc->directory = "/tmp";
    desc->compression = false;

    BOOST_REQUIRE_NO_THROW( catalog->startTransaction() );

    BOOST_REQUIRE_NO_THROW( catalog->createArchive(desc) );

    /* Check whether archive exists */
    BOOST_REQUIRE_NO_THROW( check_desc = catalog->existsByName("test") );
    BOOST_CHECK( check_desc->id != -1 );

    /* Drop archive and recheck */
    BOOST_REQUIRE_NO_THROW( catalog->dropArchive("test") );

    BOOST_REQUIRE_NO_THROW( check_desc = catalog->existsByName("test") );
    BOOST_CHECK( check_desc->id == -1 );

    BOOST_REQUIRE_NO_THROW( catalog->commitTransaction() );
  }

  /* 5 Close works */
  BOOST_REQUIRE_NO_THROW( catalog->close() );

  /* 5 Catalog not available anymore */
  BOOST_TEST( !catalog->available() );

}
