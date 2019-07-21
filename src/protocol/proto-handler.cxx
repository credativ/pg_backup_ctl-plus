#include <ostream>
#include <pgsql-proto.hxx>

using namespace credativ;
using namespace credativ::pgprotocol;

ProtocolCommandHandler::ProtocolCommandHandler(PGProtoCmdDescr descr) {

  tag = descr.tag;

}

ProtocolCommandHandler::~ProtocolCommandHandler() {}
