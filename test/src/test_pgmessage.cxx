#define BOOST_TEST_MODULE TestPGMessage
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <pgmessage.hxx>
#include <proto-buffer.hxx>
#include <pgsql-proto.hxx>
#include <exception>


using namespace credativ;

BOOST_AUTO_TEST_CASE(TestPGMessageSetup)
{
  std::shared_ptr<PGMessage> message = nullptr;

  BOOST_REQUIRE_NO_THROW( message = std::make_shared<PGMessage>() );
}

BOOST_AUTO_TEST_CASE(TestPGMessageWriteToBuffer)
{
  std::shared_ptr<ProtocolBuffer> buffer = std::make_shared<ProtocolBuffer>();
  PGMessage message;

  BOOST_REQUIRE_NO_THROW( message.writeTo(buffer));
}

BOOST_AUTO_TEST_CASE(TestPGMessageWriteToBufferNoBuffer)
{
  std::shared_ptr<ProtocolBuffer> buffer = nullptr;
  PGMessage message;

  BOOST_CHECK_THROW( message.writeTo(buffer), CopyProtocolFailure);
}

BOOST_AUTO_TEST_CASE(TestPGMessageReadFromBuffer)
{
  auto buffer = std::make_shared<ProtocolBuffer>();
  PGMessage message;

  message.writeTo(buffer);
  buffer->first();

  BOOST_REQUIRE_NO_THROW( message.readFrom(buffer));
}

BOOST_AUTO_TEST_CASE(TestPGMessageReadFromBufferNoBuffer)
{
  std::shared_ptr<ProtocolBuffer> buffer = nullptr;
  PGMessage message;

  BOOST_CHECK_THROW(message.readFrom(buffer), CopyProtocolFailure);
}

BOOST_AUTO_TEST_CASE(TestPGMessageGetSize)
{
  PGMessage message;

  BOOST_TEST(5 == message.getSize());
}
