#define BOOST_TEST_MODULE PGMessageCopyResponse
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <pgmessage.hxx>
#include <proto-buffer.hxx>
#include <pgsql-proto.hxx>
#include <pgproto-copy.hxx>
#include <exception>


using namespace credativ;

BOOST_AUTO_TEST_CASE(TestPGMessageCopyResponseSetup)
{
  std::shared_ptr<PGMessageCopyResponse> message = nullptr;

  BOOST_REQUIRE_NO_THROW( message = std::make_shared<PGMessageCopyResponse>() );
}

BOOST_AUTO_TEST_CASE(TestPGMessageWriteToBuffer)
{
  auto buffer = std::make_shared<ProtocolBuffer>();
  PGMessageCopyResponse message;

  BOOST_REQUIRE_NO_THROW( message.writeTo(buffer));
}

BOOST_AUTO_TEST_CASE(TestPGMessageCopyResponseReadFromBuffer)
{
  auto buffer = std::make_shared<ProtocolBuffer>();
  PGMessageCopyResponse message;

  BOOST_REQUIRE_NO_THROW( message.writeTo(buffer));;
}

BOOST_AUTO_TEST_CASE(TestPGMessageCopyResponseGetSize)
{
  PGMessageCopyResponse message;

  BOOST_TEST(9 == message.getSize());
}

BOOST_AUTO_TEST_CASE(TestPGMessageCopyResponseSetFormat)
{
  PGProtoCopyFormat format = PGProtoCopyFormat(10, true);
  PGMessageCopyResponse message;

  BOOST_REQUIRE_NO_THROW( message.setFormats(&format));
  BOOST_TEST( 29 == message.getSize() );
}

/*BOOST_AUTO_TEST_CASE(TestPGMessageCopyResponseGetFormat)
{
  PGProtoCopyFormat format_in = PGProtoCopyFormat(10, true);
  PGProtoCopyFormat format_out;
  ProtocolBuffer buffer;
  std::shared_ptr<PGMessageCopyResponse> message = nullptr;

  BOOST_REQUIRE_NO_THROW( message = std::make_shared<PGMessageCopyResponse>() );
  BOOST_REQUIRE_NO_THROW( message->setFormats(&format_in));
  BOOST_TEST( 29 == message->getSize() );
  BOOST_REQUIRE_NO_THROW( message->getFormats(&format_out));
  BOOST_TEST( 10 == format_out.count() );
}*/
