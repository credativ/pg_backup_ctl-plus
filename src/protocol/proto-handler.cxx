#include <ostream>
#include <pgproto-commands.hxx>

using namespace credativ;
using namespace credativ::pgprotocol;

ProtocolCommandHandler::ProtocolCommandHandler(std::shared_ptr<PGProtoCmdDescr> descr,
                                               std::shared_ptr<RuntimeConfiguration> rtc) {

  /* Die hard if any of the descriptors are undefined! */

  if (descr == nullptr)
    throw PGProtoCmdFailure("protocol command handler: undefined command descriptor");

  if (rtc == nullptr) {
    throw PGProtoCmdFailure("protocol command handler: undefined reference to runtime configuration");
  }

  cmdDescr = descr;
  runtime_configuration = rtc;

}

ProtocolCommandHandler::~ProtocolCommandHandler() {}

std::shared_ptr<PGProtoStreamingCommand> ProtocolCommandHandler::getExecutable(std::shared_ptr<WorkerSHM> worker_shm) {

  std::shared_ptr<PGProtoStreamingCommand> cmd = nullptr;

  switch (cmdDescr->tag) {

  case IDENTIFY_SYSTEM:
    {
      cmd = std::make_shared<PGProtoIdentifySystem>(cmdDescr, runtime_configuration, worker_shm);
      break;
    }

  case LIST_BASEBACKUPS:
    {
      cmd = std::make_shared<PGProtoListBasebackups>(cmdDescr, runtime_configuration, worker_shm);
      break;
    }

  default:

    throw PGProtoCmdFailure("unknown streaming protocol command");
    break;

  }

  return cmd;

}
