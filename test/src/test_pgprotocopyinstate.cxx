#define BOOST_TEST_MODULE TestPGProtoCopyInState
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <proto-buffer.hxx>
#include <pgsql-proto.hxx>
#include <pgproto-copy.hxx>
#include <pgmessage.hxx>

BOOST_AUTO_TEST_CASE(TestPGProtCopyInStateReadNoInputBuffer)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!");
  std::string msg2;
  PGMessageCopyData copy_data_msg;

  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataInState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_CHECK_THROW(context.state->read(context), std::exception);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyInStateReadNoInputDataBuffer)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!");
  std::string msg2;
  PGMessageCopyData copy_data_msg;

  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataInState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_CHECK_THROW(context.state->read(context), std::exception);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyInStateWrite)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataInState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_TEST(context.state->write(context) == 0);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyInStateReadCopyData)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!\n");
  std::string msg2;
  PGMessageCopyData copy_data_msg;
  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataInState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();

  copy_data_msg.writeTo(context.input_buffer);
  context.input_buffer->first();

  BOOST_REQUIRE_NO_THROW(context.state->read(context));

  {
    context.input_data_buffer->first();
    char * dummy = new char[context.input_data_buffer->getSize() + 1];
    context.input_data_buffer->read_buffer((void*)dummy, context.input_data_buffer->getSize());
    dummy[context.input_data_buffer->getSize()] = '\0';
    msg2 = std::string(dummy);
    //delete dummy;
  }

  BOOST_TEST(msg1 == msg2);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyInStateReadCopyDataState)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!\n");
  std::string msg2;
  PGMessageCopyData copy_data_msg;
  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataInState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();

  copy_data_msg.writeTo(context.input_buffer);
  context.input_buffer->first();

  BOOST_REQUIRE_NO_THROW(context.state->read(context));
  BOOST_TEST(context.state->state() == In);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyInStateReadCopyDone)
{
  PGMessageCopyDone copy_done_msg;

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataInState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();

  copy_done_msg.writeTo(context.input_buffer);
  context.input_buffer->first();
  BOOST_REQUIRE_NO_THROW(context.state->read(context));

  BOOST_TEST(context.state->state() == Done);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyInStateReadCopyDoneState)
{
  PGMessageCopyDone copy_done_msg;

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataInState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();

  copy_done_msg.writeTo(context.input_buffer);
  context.input_buffer->first();
  BOOST_REQUIRE_NO_THROW(context.state->read(context));

  BOOST_TEST(context.state->state() == Done);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyInStateReadCopyFail)
{
  std::string msg1("Eine sehr bedrohliche Fehlermeldung!\n");
  std::string msg2;
  PGMessageCopyFail copy_fail_msg;
  copy_fail_msg.setMessage(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataInState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();

  copy_fail_msg.writeTo(context.input_buffer);
  context.input_buffer->first();
  BOOST_REQUIRE_NO_THROW(context.state->read(context));

  {
    context.input_data_buffer->first();
    char * dummy = new char[context.input_data_buffer->getSize() + 1];
    context.input_data_buffer->read_buffer((void*)dummy, context.input_data_buffer->getSize());
    dummy[context.input_data_buffer->getSize()] = '\0';
    msg2 = std::string(dummy);
    //delete dummy;
  }

  BOOST_TEST(msg1 == msg2);
  BOOST_TEST(context.state->state() == Fail);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyInStateReadCopyFailState)
{
  std::string msg1("Eine sehr bedrohliche Fehlermeldung!\n");
  std::string msg2;
  PGMessageCopyFail copy_fail_msg;
  copy_fail_msg.setMessage(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataInState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();

  copy_fail_msg.writeTo(context.input_buffer);
  context.input_buffer->first();
  BOOST_REQUIRE_NO_THROW(context.state->read(context));

  BOOST_TEST(context.state->state() == Fail);
}
