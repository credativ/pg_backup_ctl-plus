#define BOOST_TEST_MODULE TestCopyManager
#include <vector>
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

BOOST_AUTO_TEST_CASE(TestCopyManager)
{

  std::string test_data = "B";
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
   *
   * We use a larger, a small and an empty file.
   */
  std::shared_ptr<ArchiveFile> infile_large
    = std::make_shared<ArchiveFile>(sourceDir->basedir() / BackupDirectory::temp_filename());
  std::shared_ptr<ArchiveFile> infile_small
    = std::make_shared<ArchiveFile>(sourceDir->basedir() / BackupDirectory::temp_filename());
  std::shared_ptr<ArchiveFile> infile_empty
          = std::make_shared<ArchiveFile>(sourceDir->basedir() / BackupDirectory::temp_filename());

  std::map<std::shared_ptr<ArchiveFile>, size_t> test_files;

  test_files.emplace(std::make_pair(infile_large, 210000000));
  test_files.emplace(std::make_pair(infile_small, 32799));
  test_files.emplace(std::make_pair(infile_empty, 0));

  for (auto fp : test_files) {

    std::shared_ptr<ArchiveFile> fh = fp.first;
    size_t fh_size = fp.second;

    /* Open the file and write, sync */
    fh->setOpenMode("w+");
    fh->open();

    for (size_t i = 0; i < fh_size; i++) {
      fh->write(test_data.c_str(), test_data.length());
    }
    fh->fsync();
    fh->close();
  }

  copyMgr = std::make_shared<BackupCopyManager>(sourceDir, targetDir);
  copyMgr->setNumberOfCopyInstances(4);
  copyMgr->start();
  copyMgr->wait();

  //boost::filesystem::remove_all(sourcePath);
  //boost::filesystem::remove_all(targetPath);

}
