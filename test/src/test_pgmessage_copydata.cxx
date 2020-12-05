#define BOOST_TEST_MODULE TestPGMessageCopyData
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <pgmessage.hxx>
#include <proto-buffer.hxx>
#include <pgsql-proto.hxx>
#include <exception>

using namespace credativ;

BOOST_AUTO_TEST_CASE(TestPGMessageCopyDataSetup)
{
  std::shared_ptr<PGMessageCopyData> message = nullptr;

  BOOST_REQUIRE_NO_THROW( message = std::make_shared<PGMessageCopyData>() );
}

BOOST_AUTO_TEST_CASE(TestPGMessageCopyDataSetData)
{
  std::string msg = "Ein Test";
  auto buffer = std::make_shared<ProtocolBuffer>();
  PGMessageCopyData message;

  BOOST_REQUIRE_NO_THROW(message.setData(msg));
}

BOOST_AUTO_TEST_CASE(TestPGMessageCopyDataGetData)
{
  std::string msg = "Ein Test";
  auto buffer = std::make_shared<ProtocolBuffer>();
  PGMessageCopyData message;
  message.setData(msg);

  BOOST_REQUIRE_NO_THROW(message.getData() == msg);
}

BOOST_AUTO_TEST_CASE(TestPGMessageCopyDataWriteToBuffer)
{
  std::string msg = "Ein Test";
  auto buffer = std::make_shared<ProtocolBuffer>();
  PGMessageCopyData message;

  message.setData(msg);
  BOOST_REQUIRE_NO_THROW(message.writeTo(buffer));

  BOOST_TEST(msg.size() + 5 == message.getSize());
}

BOOST_AUTO_TEST_CASE(TestPGMessageCopyDataReadFromBuffer)
{
  auto buffer = std::make_shared<ProtocolBuffer>();
  PGMessageCopyData message;

  message.writeTo(buffer);
  buffer->first();

  BOOST_REQUIRE_NO_THROW(message.readFrom(buffer));
}

BOOST_AUTO_TEST_CASE(TestPGMessageCopyDataGetSize)
{
  PGMessageCopyData message;

  BOOST_TEST(5 == message.getSize());
}


