#define BOOST_TEST_MODULE TestBackupCatalog
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <BackupCatalog.hxx>

using namespace pgbckctl;

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

BOOST_AUTO_TEST_CASE(TestBackupCatalogCreateArchive)
{

  std::shared_ptr<BackupCatalog> catalog = nullptr;

  /* 1 should not throw */
  BOOST_REQUIRE_NO_THROW( catalog
                          = std::make_shared<BackupCatalog>(".pg_backup_ctl.sqlite") );

  /* 2 Open backup catalog for read/write */
  BOOST_REQUIRE_NO_THROW( catalog->open_rw() );

  /* 3 Backup catalog should be available */
  BOOST_CHECK( catalog->available() );

  /*
   * 4 Create archive "test"
   *
   * NOTE: BackupCatalog::createArchive doesn't require
   *                      pushAffectedAttributes() anymore,
   *                      since we always need the same number
   *                      of attributes here.
   */
  {
    std::shared_ptr<CatalogDescr> desc = std::make_shared<CatalogDescr>();
    std::shared_ptr<CatalogDescr> check_desc;

    BOOST_REQUIRE_NO_THROW( catalog->startTransaction() );


    /* CatalogDescr::archive_name required, should throw */
    BOOST_CHECK_THROW( catalog->createArchive(desc),
                       CCatalogIssue );

    desc->archive_name = "test";

    /* CatalogDescr::directory required, should throw */
    BOOST_CHECK_THROW( catalog->createArchive(desc),
                       CCatalogIssue );

    desc->directory = "/tmp";

    /*
     * Set connection type. this is important since BackupCatalog::createArchive()
     * insists this to be set! Thus, this should throw.
     */
    BOOST_CHECK_THROW( catalog->createArchive(desc),
                       CCatalogIssue );
    desc->coninfo->type = ConnectionDescr::CONNECTION_TYPE_BASEBACKUP;

    /* Should succeed now */
    /* Force compression off, though not required */
    desc->compression = false;
    BOOST_REQUIRE_NO_THROW( catalog->createArchive(desc) );

    /* Check whether archive exists */
    BOOST_REQUIRE_NO_THROW( check_desc = catalog->existsByName("test") );
    BOOST_CHECK( check_desc->id != -1 );
    BOOST_TEST( check_desc->id > -1 );
    BOOST_CHECK_EQUAL( check_desc->archive_name, "test" );
    BOOST_CHECK_EQUAL( check_desc->directory, "/tmp" );
    BOOST_CHECK_EQUAL( check_desc->compression, false );

    /* Create a basebackup streaming connection */

    /* This must fail, since no connection DSN/attributes currently set */
    BOOST_CHECK_THROW( catalog->createCatalogConnection(check_desc->coninfo),
                       CCatalogIssue );

    /* Assign a DSN string and attributes, this should succeed now */
    check_desc->coninfo->pushAffectedAttribute(SQL_CON_DSN_ATTNO);
    check_desc->coninfo->pushAffectedAttribute(SQL_CON_ARCHIVE_ID_ATTNO);
    check_desc->coninfo->pushAffectedAttribute(SQL_CON_TYPE_ATTNO);

    check_desc->coninfo->dsn = "host=bar.server.name dbname=foo user=test";
    check_desc->coninfo->archive_id = check_desc->id;
    check_desc->coninfo->type = ConnectionDescr::CONNECTION_TYPE_BASEBACKUP;

    BOOST_REQUIRE_NO_THROW( catalog->createCatalogConnection(check_desc->coninfo) );

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
