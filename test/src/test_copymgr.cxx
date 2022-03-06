#define BOOST_TEST_MODULE TestCopyManager
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <fs-copy.hxx>

using namespace pgbckctl;

BOOST_AUTO_TEST_CASE(TestRelativePath) {

  path aDir("/a/b/c/d");
  path bDir("/a/b");

  path result = BackupDirectory::relative_path(aDir, bDir);
  BOOST_TEST(result.string() == "c/d");

  aDir = path("/a/b");
  bDir = path("/a/b/c/d");

  result = BackupDirectory::relative_path(aDir, bDir);
  BOOST_TEST(result.string() == "");

  aDir = path("/a/b");
  bDir = path("/");

  result = BackupDirectory::relative_path(aDir, bDir);
  BOOST_TEST(result.string() == "a/b");

  aDir = path("/");
  bDir = path("");

  result = BackupDirectory::relative_path(aDir, bDir);
  BOOST_TEST(result.string() == "/");

  aDir = path("");
  bDir = path("/a/b");

  result = BackupDirectory::relative_path(aDir, bDir);
  BOOST_TEST(result.string() == "");

  aDir = path("e/f/g");
  bDir = path("/c/d/e");

  result = BackupDirectory::relative_path(aDir, bDir);
  BOOST_TEST(result.string() == "e/f/g");

}

BOOST_AUTO_TEST_CASE(TestTempFile)
{

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

#ifdef PG_BACKUP_CTL_HAS_LIBURING

BOOST_AUTO_TEST_CASE(TestIOUring)
{

  std::string test_data = "This is some test data";
  //std::shared_ptr<IOUringInstance> iouring = std::make_shared<IOUringInstance>();
  std::shared_ptr<BackupCopyManager> copyMgr = nullptr;

  path sourcePath = path(BackupDirectory::system_temp_directory() / "_copyMgrTestSource");
  path targetPath = path(BackupDirectory::system_temp_directory() / "_copyMgrTestTarget");

  if (!boost::filesystem::exists(sourcePath))
    create_directories(sourcePath);

  if (!boost::filesystem::exists(targetPath)) {
    boost::filesystem::create_directories(targetPath);
  }

  /* Create target directories to test in TMPDIR */
  std::shared_ptr<BackupDirectory> sourceDir
          = std::make_shared<BackupDirectory>(sourcePath);
  std::shared_ptr<TargetDirectory> targetDir
          = std::make_shared<TargetDirectory>(targetPath);

  /*
   * NOTE: don't be tempted to call exists() on sourceDir. Just check
   *       whether the source and target directories exists via boost:filesystem
   *       methods directly. The first one would perform additional checks on the archive
   *       filesystem structure we don't want to do here atm.
   */
  if (!boost::filesystem::exists(sourceDir->basedir())) {
    boost::filesystem::create_directories(sourceDir->basedir());
  }

  /**
   * Create a temp file
   */
  std::shared_ptr<ArchiveFile> infile
          = std::make_shared<ArchiveFile>(sourceDir->basedir() / BackupDirectory::temp_filename());
  std::shared_ptr<ArchiveFile> outfile
          = std::make_shared<ArchiveFile>(BackupDirectory::system_temp_directory()
                                          / BackupDirectory::temp_filename());

  std::cerr << "infile: \""
            << infile->getFileName()
            << "\" outfile: \""
            << outfile->getFileName()
            << "\""
            << std::endl;

  /* Open the file and write, sync */
  infile->setOpenMode("w+");
  infile->open();
  infile->write(test_data.c_str(), test_data.length());
  infile->fsync();
  infile->close();

  copyMgr = std::make_shared<BackupCopyManager>(sourceDir, targetDir);
  copyMgr->start();

  //boost::filesystem::remove_all(sourcePath);
  //boost::filesystem::remove_all(targetPath);

}

#endif
