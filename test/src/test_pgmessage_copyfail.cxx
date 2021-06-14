#define BOOST_TEST_MODULE TestPGMessageCopyFailCopyFail
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <pgmessage.hxx>
#include <proto-buffer.hxx>
#include <pgsql-proto.hxx>
#include <exception>

using namespace pgbckctl;

BOOST_AUTO_TEST_CASE(TestPGMessageCopyFailSetup)
{
  std::shared_ptr<PGMessageCopyFail> message = nullptr;
  BOOST_REQUIRE_NO_THROW(message = std::make_shared<PGMessageCopyFail>());
}

BOOST_AUTO_TEST_CASE(TestPGMessageCopyFailGetSize)
{
  auto message = std::make_shared<PGMessageCopyFail>();

  BOOST_REQUIRE_NO_THROW( message->getSize() );
  BOOST_TEST( 5 == message->getSize() );
}

BOOST_AUTO_TEST_CASE(TestPGMessageCopyFailWriteToBuffer)
{

  auto buffer = std::make_shared<ProtocolBuffer>();
  auto message = std::make_shared<PGMessageCopyFail>();

  BOOST_REQUIRE_NO_THROW( message->writeTo(buffer));
  BOOST_TEST( 5 == buffer->getSize() );
}

BOOST_AUTO_TEST_CASE(TestPGMessageCopyFailReadFromBuffer)
{
  auto buffer = std::make_shared<ProtocolBuffer>();
  auto message = std::make_shared<PGMessageCopyFail>();

  message->writeTo(buffer);
  buffer->first();

  BOOST_REQUIRE_NO_THROW( message->readFrom(buffer));
  BOOST_TEST( 5 == message->getSize() );
}

BOOST_AUTO_TEST_CASE(TestPGMessageCopyFailSetMessage)
{
  std::string msg = "Generic error message!";
  auto message = std::make_shared<PGMessageCopyFail>();

  BOOST_REQUIRE_NO_THROW( message->setMessage(msg) );
  BOOST_TEST(5 + msg.size() == message->getSize());
}

BOOST_AUTO_TEST_CASE(TestPGMessageCopyFailGetMessage)
{
  std::string msg = "Generic error message!";
  auto message = std::make_shared<PGMessageCopyFail>();

  BOOST_REQUIRE_NO_THROW(message->setMessage(msg));
  BOOST_TEST(msg == message->getMessage());
}


