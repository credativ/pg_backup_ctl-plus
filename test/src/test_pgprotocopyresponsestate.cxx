#define BOOST_TEST_MODULE TestPGProtoCopyResponseState
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <proto-buffer.hxx>
#include <pgsql-proto.hxx>
#include <pgproto-copy.hxx>
#include <pgmessage.hxx>

BOOST_AUTO_TEST_CASE(TestPGProtoCopyResponseStateState)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyInResponseState>();
  BOOST_TEST(context.state->state() == Init);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyInResponseNoCopyFormats)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyInResponseState>();
  context.output_buffer = std::make_shared<ProtocolBuffer>();


  BOOST_CHECK_THROW(context.state->write(context), std::exception);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyInResponseNoOutputBuffer)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyInResponseState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);

  BOOST_CHECK_THROW(context.state->write(context), std::exception);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyInResponse)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyInResponseState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_REQUIRE_NO_THROW(context.state->write(context));

  context.output_buffer->first();

  PGMessageType type;
  context.output_buffer->read_byte(type);
  BOOST_TEST(type == CopyInResponseMessage);

  int size;
  context.output_buffer->read_int(size);
  BOOST_TEST( 9 == size);

  BOOST_TEST(context.state->state() == In);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyInResponseState)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyInResponseState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_REQUIRE_NO_THROW(context.state->write(context));

  BOOST_TEST(context.state->state() == In);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyOutResponse)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyOutResponseState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_REQUIRE_NO_THROW(context.state->write(context));

  context.output_buffer->first();

  PGMessageType type;
  context.output_buffer->read_byte(type);
  BOOST_TEST(type == CopyOutResponseMessage);

  int size;
  context.output_buffer->read_int(size);
  BOOST_TEST( 9 == size);

  BOOST_TEST(context.state->state() == Out);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyOutResponseState)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyOutResponseState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_REQUIRE_NO_THROW(context.state->write(context));

  BOOST_TEST(context.state->state() == Out);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyBothResponse)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyBothResponseState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_REQUIRE_NO_THROW(context.state->write(context));

  context.output_buffer->first();

  PGMessageType type;
  context.output_buffer->read_byte(type);
  BOOST_TEST(type == CopyBothResponseMessage);

  int size;
  context.output_buffer->read_int(size);
  BOOST_TEST( 9 == size);

  BOOST_TEST(context.state->state() == Both);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyBothResponseState)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyBothResponseState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_REQUIRE_NO_THROW(context.state->write(context));

  BOOST_TEST(context.state->state() == Both);
}
