#define BOOST_TEST_MODULE TestPGProtoCopy
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <proto-buffer.hxx>
#include <pgsql-proto.hxx>
#include <pgproto-copy.hxx>
#include <pgmessage.hxx>

BOOST_AUTO_TEST_CASE(TestPGProtoCopySetup)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataInState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();
  PGProtoCopy * copy_protocol;
  BOOST_REQUIRE_NO_THROW(copy_protocol = new PGProtoCopy(context));
  delete copy_protocol;
}

BOOST_AUTO_TEST_CASE(TestPGProtCopySetupFail)
{
  PGProtoCopyContext context;
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();

  /* Leaks reference to PGProtoCopy instance, but doesn't matter here */
  BOOST_CHECK_THROW(new PGProtoCopy(context), CopyProtocolFailure);
}
