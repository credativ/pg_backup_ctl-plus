#define BOOST_TEST_MODULE TestCopyManager
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <fs-copy.hxx>

using namespace pgbckctl;

#ifdef PG_BACKUP_CTL_HAS_LIBURING

BOOST_AUTO_TEST_CASE(TestTempFile)
{

  std::shared_ptr<IOUringInstance> iouring = std::make_shared<IOUringInstance>();

  /**
   * Write out a random string to a temp file via IOUringInstance
   */
  std::string any_string = "This is some test data";

  /*
   * Read back string from temporary file.
   */
  std::string another_string;
  MemoryBuffer buf(any_string.length());

  /**
   * Create a temp file
   */
  std::shared_ptr<ArchiveFile> file
    = std::make_shared<ArchiveFile>(BackupDirectory::system_temp_directory()
                                    / BackupDirectory::temp_filename());
  file->setTemporary();

  /* Open the file and write, sync and re-read data */
  file->setOpenMode("a+");
  file->open();
  file->write(any_string.c_str(), any_string.length());
  file->fsync();
  file->lseek(SEEK_SET, 0);
  file->read(buf.ptr(), buf.getSize());
  another_string.assign(buf.ptr());

  /* Data re-read must match former string */
  BOOST_TEST((any_string == another_string));

  /* Check if the temporary was indeed deleted */
  boost::filesystem::exists(file->getFileName());

  file->close();

}

BOOST_AUTO_TEST_CASE(TestCopyManager)
{

  //std::shared_ptr<BackupCopyManager> copyMgr = std::make_shared<BackupCopyManager>(nullptr, nullptr);

}

#endif
