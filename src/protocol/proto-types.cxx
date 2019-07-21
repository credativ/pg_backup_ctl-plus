#include <ostream>
#include <pgsql-proto.hxx>

using namespace credativ;
using namespace credativ::pgprotocol;

void PGProtoCmdDescr::setCommandTag(ProtocolCommandTag const& tag) {

  this->tag = tag;

}
