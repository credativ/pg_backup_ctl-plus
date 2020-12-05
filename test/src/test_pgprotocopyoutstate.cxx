#define BOOST_TEST_MODULE TestPGProtoCopyOutState
#include <boost/test/unit_test.hpp>
#include <common.hxx>
#include <proto-buffer.hxx>
#include <pgsql-proto.hxx>
#include <pgproto-copy.hxx>
#include <pgmessage.hxx>

BOOST_AUTO_TEST_CASE(TestPGProtCopyOutStateWriteNoOutputBuffer)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!");
  std::string msg2;
  PGMessageCopyData copy_data_msg;

  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataOutState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_data_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_CHECK_THROW(context.state->write(context), std::exception);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyOutStateWriteNoOutputDataBuffer)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!");
  std::string msg2;
  PGMessageCopyData copy_data_msg;

  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataOutState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_CHECK_THROW(context.state->write(context), std::exception);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyOutStateWriteCopyData)
{
  std::string msg1("Eine Inhaltlich nicht falsche Nachricht!");
  std::string msg2;
  PGMessageCopyData copy_data_msg;

  copy_data_msg.setData(msg1);

  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataOutState>();
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
  context.state = std::make_shared<PGProtoCopyDataOutState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();
  context.output_data_buffer = std::make_shared<ProtocolBuffer>();

  context.output_data_buffer->allocate(msg1.size());
  context.output_data_buffer->write_buffer((void*)msg1.c_str(), msg1.size());
  context.output_data_buffer->first();

  BOOST_REQUIRE_NO_THROW(context.state->write(context));

  BOOST_TEST(context.state->state() == Out);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyOutStateWriteCopyDone)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataOutState>();
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
  context.state = std::make_shared<PGProtoCopyDataOutState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();
  context.output_data_buffer = std::make_shared<ProtocolBuffer>();

  context.output_data_buffer->allocate(0);

  BOOST_REQUIRE_NO_THROW(context.state->write(context));

  BOOST_TEST(context.state->state() == Done);
}

BOOST_AUTO_TEST_CASE(TestPGProtCopyOutStateState)
{
  PGProtoCopyContext context;
  context.state = std::make_shared<PGProtoCopyDataOutState>();
  context.formats = std::make_shared<PGProtoCopyFormat>(1, true);
  context.output_buffer = std::make_shared<ProtocolBuffer>();
  context.output_data_buffer = std::make_shared<ProtocolBuffer>();

  BOOST_TEST(context.state->state() == Out);
}
