#include <sstream>
#include <ostream>
#include <pgproto-copy.hxx>
#include <pgsql-proto.hxx>
#include <pgmessage.hxx>



/* *****************************************************************************
 * class PGProtoCopyState
 * *****************************************************************************/
size_t PGProtoCopyState::read(PGProtoCopyContext &context){
  return 0;
}

size_t PGProtoCopyState::write(PGProtoCopyContext &context){
  return 0;
}

/* *****************************************************************************
 * class PGProtoCopyInState
 * *****************************************************************************/
size_t PGProtoCopyResponseState::writeCopyResponse(PGProtoCopyContext &context){

#define formats context.formats
#define output_buffer context.output_buffer

  char overall_format = formats->getFormat();
  size_t size = 8;
  size += formats->count() * 2;

  output_buffer->allocate(size);
  output_buffer->write_byte(type());
  output_buffer->write_int(size - 1);
  output_buffer->write_byte(overall_format);
  output_buffer->write_short(formats->count());

  for (uint32_t i = 0; i < formats->count(); i++ ) {
      output_buffer->write_short(formats->get(i));
  }

  return size;
}

std::shared_ptr<PGProtoCopyState> PGProtoCopyInResponseState::nextState(){
  return std::make_shared<PGProtoCopyDataInState>();
}

PGMessageType PGProtoCopyInResponseState::type(){
  return CopyInResponseMessage;
}

std::shared_ptr<PGProtoCopyState> PGProtoCopyOutResponseState::nextState(){
  return std::make_shared<PGProtoCopyDataOutState>();
}

PGMessageType PGProtoCopyOutResponseState::type(){
  return CopyOutResponseMessage;
}

std::shared_ptr<PGProtoCopyState> PGProtoCopyBothResponseState::nextState(){
  return std::make_shared<PGProtoCopyDataBothState>();
}

PGMessageType PGProtoCopyBothResponseState::type(){
  return CopyBothResponseMessage;
}

size_t PGProtoCopyResponseState::write(PGProtoCopyContext &context){

#define formats context.formats
#define output_buffer context.output_buffer

  if (output_buffer == nullptr) {
    throw CopyProtocolFailure("Undefined output-data-buffer.");
  }

  if (formats == nullptr) {
    throw CopyProtocolFailure("Undefined formats.");
  }
  context.state = nextState();
  return writeCopyResponse(context);
}

PGProtoCopyStateType PGProtoCopyResponseState::state() {
  return PGProtoCopyStateType::Init;
}

/* *****************************************************************************
 * class PGProtoCopyInState
 * *****************************************************************************/

size_t PGProtoCopyDataInState::readCopyData(PGProtoCopyContext &context){
#define input_buffer context.input_buffer
#define input_data_buffer context.input_data_buffer

  int size;

  input_buffer->read_int((int&)size);
  size -= 4;
  input_data_buffer->allocate(size);
  input_buffer->read_buffer(input_data_buffer->ptr(), size);

  return size;
}

size_t PGProtoCopyDataInState::readCopyFail(PGProtoCopyContext &context){

#define input_buffer context.input_buffer
#define input_data_buffer context.input_data_buffer

  int size;

  input_buffer->read_int((int&)size);
  size -= 4;
  input_data_buffer->allocate(size);
  input_buffer->read_buffer(input_data_buffer->ptr(), size);

  return size;
}

size_t PGProtoCopyDataInState::readCopyDone(PGProtoCopyContext &context){
  return 0;
}

size_t PGProtoCopyDataInState::read(PGProtoCopyContext &context) {

#define input_buffer context.input_buffer
#define input_data_buffer context.input_data_buffer


  if (input_buffer == nullptr) {
    context.state = std::make_shared<PGProtoCopyFailState>();
    throw CopyProtocolFailure("Undefined input-buffer.");
  }

  if(input_buffer->getSize() == 0) {
    return 0;
  }

  if (input_data_buffer == nullptr) {
    context.state = std::make_shared<PGProtoCopyFailState>();
    throw CopyProtocolFailure("Undefined input-data-buffer.");
  }

  PGMessageType message_type;
  input_buffer->read_byte(message_type);

  if (message_type == CopyDataMessage) {
    return readCopyData(context);
  }

  if (message_type == CopyDoneMessage) {
    context.state = std::make_shared<PGProtoCopyDoneState>();
    return readCopyDone(context);
  }

  if (message_type == CopyFailMessage) {
    context.state = std::make_shared<PGProtoCopyFailState>();
    return readCopyFail(context);
  }

  throw CopyProtocolFailure("Unexpected MessageType.");
  return 0;
}

PGProtoCopyStateType PGProtoCopyDataInState::state() {
  return PGProtoCopyStateType::In;
}

/* *****************************************************************************
 * class PGProtoCopyOutState
 * *****************************************************************************/

size_t PGProtoCopyDataOutState::writeCopyData(PGProtoCopyContext &context){

#define output_buffer context.output_buffer
#define output_data_buffer context.output_data_buffer

  int size = 5;
  size += output_data_buffer->getSize();

  output_buffer->allocate(size);
  output_buffer->write_byte(CopyDataMessage);
  output_buffer->write_int(size - 1);
  output_buffer->write_buffer(output_data_buffer->ptr(), output_data_buffer->getSize());

  return size;
}

size_t PGProtoCopyDataOutState::writeCopyDone(PGProtoCopyContext &context){
  #define output_buffer context.output_buffer

  output_buffer->allocate(5);
  output_buffer->write_byte(CopyDoneMessage);
  output_buffer->write_int(4);

  return 5;
}

size_t PGProtoCopyDataOutState::write(PGProtoCopyContext &context) {

#define output_buffer context.output_buffer
#define output_data_buffer context.output_data_buffer

  if (output_data_buffer == nullptr) {
    context.state = std::make_shared<PGProtoCopyFailState>();
    throw CopyProtocolFailure("Undefined output-buffer.");
  }

  if (output_buffer == nullptr) {
    context.state = std::make_shared<PGProtoCopyFailState>();
    throw CopyProtocolFailure("Undefined output-data-buffer.");
  }

  if(output_data_buffer->getSize() == 0) {
    context.state = std::make_shared<PGProtoCopyDoneState>();
    return writeCopyDone(context);
  }

  return writeCopyData(context);
}


PGProtoCopyStateType PGProtoCopyDataOutState::state() {
  return PGProtoCopyStateType::Out;
}

/* *****************************************************************************
 * class PGProtoCopyBothState
 * *****************************************************************************/

size_t PGProtoCopyDataBothState::write(PGProtoCopyContext &context) {

#define output_buffer context.output_buffer
#define output_data_buffer context.output_data_buffer

  if (output_data_buffer == nullptr) {
    context.state = std::make_shared<PGProtoCopyFailState>();
    throw CopyProtocolFailure("Undefined output-buffer.");
  }

  if (output_buffer == nullptr) {
    context.state = std::make_shared<PGProtoCopyFailState>();
    throw CopyProtocolFailure("Undefined output-data-buffer.");
  }

  if(output_data_buffer->getSize() == 0) {
    context.state = std::make_shared<PGProtoCopyDataInState>();
    return writeCopyDone(context);
  }

  return writeCopyData(context);
}

size_t PGProtoCopyDataBothState::read(PGProtoCopyContext &context) {

#define input_buffer context.input_buffer
#define input_data_buffer context.input_data_buffer

  if (input_buffer == nullptr) {
    context.state = std::make_shared<PGProtoCopyFailState>();
    throw CopyProtocolFailure("Undefined input-buffer.");
  }

  if(input_buffer->getSize() == 0) {
    return 0;
  }

  if (input_data_buffer == nullptr) {
    context.state = std::make_shared<PGProtoCopyFailState>();
    throw CopyProtocolFailure("Undefined input-data-buffer.");
  }

  PGMessageType message_type;
  input_buffer->read_byte(message_type);

  if (message_type == CopyDataMessage) {
    return readCopyData(context);
  }

  if (message_type == CopyDoneMessage) {
    context.state = std::make_shared<PGProtoCopyDataOutState>();
    return readCopyDone(context);
  }

  if (message_type == CopyFailMessage) {
    context.state = std::make_shared<PGProtoCopyFailState>();
    return readCopyFail(context);
  }

  return 0;
}

PGProtoCopyStateType PGProtoCopyDataBothState::state() {
  return PGProtoCopyStateType::Both;
}

/* *****************************************************************************
 * class PGProtoCopyDoneState
 * *****************************************************************************/

PGProtoCopyStateType PGProtoCopyDoneState::state() {
  return PGProtoCopyStateType::Done;
}

/* *****************************************************************************
 * class PGProtoCopyFailState
 * *****************************************************************************/

PGProtoCopyStateType PGProtoCopyFailState::state() {
  return PGProtoCopyStateType::Fail;
}
