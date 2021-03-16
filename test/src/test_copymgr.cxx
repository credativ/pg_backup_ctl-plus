#define BOOST_TEST_MODULE TestCopyManager
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <io_uring_instance.hxx>

#ifdef PG_BACKUP_CTL_HAS_LIBURING

using namespace credativ;

BOOST_AUTO_TEST_CASE(TestIOUringInstance)
{

  std::shared_ptr<IOUringInstance> iouring = std::make_shared<IOUringInstance>();

}

#endif
