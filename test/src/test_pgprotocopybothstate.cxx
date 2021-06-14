#define BOOST_TEST_MODULE TestPGProtoCopyBothState
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <proto-buffer.hxx>
#include <pgsql-proto.hxx>
#include <pgproto-copy.hxx>
#include <pgmessage.hxx>

using namespace pgbckctl;

BOOST_AUTO_TEST_CASE(TestPGProtCopyBothStateReadNoInputBuffer)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!");
  std::string msg2;
  PGMessageCopyData copy_data_msg;

  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_CHECK_THROW(context.state->read(context), std::exception);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyBothStateReadNoInputDataBuffer)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!");
  std::string msg2;
  PGMessageCopyData copy_data_msg;

  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_CHECK_THROW(context.state->read(context), std::exception);
}


BOOST_AUTO_TEST_CASE(TestPGProtCopyInStateReadCopyData)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!\n");
  std::string msg2;
  PGMessageCopyData copy_data_msg;
  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
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

BOOST_AUTO_TEST_CASE(TestPGProtCopyBothStateReadCopyDataState)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!\n");
  std::string msg2;
  PGMessageCopyData copy_data_msg;
  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();

  copy_data_msg.writeTo(context.input_buffer);
  context.input_buffer->first();

  BOOST_REQUIRE_NO_THROW(context.state->read(context));
  BOOST_TEST(context.state->state() == Both);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyBothStateReadCopyDone)
{
  PGMessageCopyDone copy_done_msg;

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();

  copy_done_msg.writeTo(context.input_buffer);
  context.input_buffer->first();
  BOOST_REQUIRE_NO_THROW(context.state->read(context));

  BOOST_TEST(context.state->state() == Out);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyBothStateReadCopyDoneState)
{
  PGMessageCopyDone copy_done_msg;

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();

  copy_done_msg.writeTo(context.input_buffer);
  context.input_buffer->first();
  BOOST_REQUIRE_NO_THROW(context.state->read(context));

  BOOST_TEST(context.state->state() == Out);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyBothStateReadCopyFail)
{
  std::string msg1("Eine sehr bedrohliche Fehlermeldung!\n");
  std::string msg2;
  PGMessageCopyFail copy_fail_msg;
  copy_fail_msg.setMessage(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
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

BOOST_AUTO_TEST_CASE(TestPGProtCopyBothStateReadCopyFailState)
{
  std::string msg1("Eine sehr bedrohliche Fehlermeldung!\n");
  std::string msg2;
  PGMessageCopyFail copy_fail_msg;
  copy_fail_msg.setMessage(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.input_buffer = std::make_shared<ProtocolBuffer>();
  context.input_data_buffer = std::make_shared<ProtocolBuffer>();

  copy_fail_msg.writeTo(context.input_buffer);
  context.input_buffer->first();
  BOOST_REQUIRE_NO_THROW(context.state->read(context));

  BOOST_TEST(context.state->state() == Fail);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyBothStateWriteNoOutputBuffer)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!");
  std::string msg2;
  PGMessageCopyData copy_data_msg;

  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_data_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_CHECK_THROW(context.state->write(context), std::exception);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyBothStateWriteNoOutputDataBuffer)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!");
  std::string msg2;
  PGMessageCopyData copy_data_msg;

  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_CHECK_THROW(context.state->write(context), std::exception);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyBothStateWriteCopyData)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!");
  std::string msg2;
  PGMessageCopyData copy_data_msg;

  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();
  context.output_data_buffer = std::make_shared<ProtocolBuffer>();

  context.output_data_buffer->allocate(msg1.size());
  context.output_data_buffer->write_buffer((void*)msg1.c_str(), msg1.size());
  context.output_data_buffer->first();

  BOOST_REQUIRE_NO_THROW(context.state->write(context));

  context.output_buffer->first();
  PGMessageType type;
  context.output_buffer->read_byte(type);
  BOOST_TEST(type == CopyDataMessage);

  int size;
  context.output_buffer->read_int(size);
  BOOST_TEST(msg1.size() + 4 == size);

  {
    char * dummy = new char[size - 3];
    context.output_buffer->read_buffer((void*)dummy, size - 4);
    dummy[size - 4] = '\0';
    msg2 = std::string(dummy);
    delete dummy;
  }

  BOOST_TEST(msg1 == msg2);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyOutStateWriteCopyDataState)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!");
  std::string msg2;
  PGMessageCopyData copy_data_msg;

  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();
  context.output_data_buffer = std::make_shared<ProtocolBuffer>();

  context.output_data_buffer->allocate(msg1.size());
  context.output_data_buffer->write_buffer((void*)msg1.c_str(), msg1.size());
  context.output_data_buffer->first();

  BOOST_REQUIRE_NO_THROW(context.state->write(context));

  BOOST_TEST(context.state->state() == Both);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyBothStateWriteCopyDone)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();
  context.output_data_buffer = std::make_shared<ProtocolBuffer>();

  context.output_data_buffer->allocate(0);

  BOOST_REQUIRE_NO_THROW(context.state->write(context));

  context.output_buffer->first();
  PGMessageType type;
  context.output_buffer->read_byte(type);
  BOOST_TEST(type == CopyDoneMessage);

  int size;
  context.output_buffer->read_int(size);
  BOOST_TEST(4 == size);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyOutStateWriteCopyDoneState)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();
  context.output_data_buffer = std::make_shared<ProtocolBuffer>();

  context.output_data_buffer->allocate(0);

  BOOST_REQUIRE_NO_THROW(context.state->write(context));

  BOOST_TEST(context.state->state() == In);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyBothStateState)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataBothState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();
  context.output_data_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_TEST(context.state->state() == Both);
}
