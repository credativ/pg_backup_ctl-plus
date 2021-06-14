#include <proto-catalog.hxx>

namespace pgbckctl {

  namespace pgprotocol {

    /*************************************************************************
     * Handles command definitions to process
     * queries in the streaming protocol parser.
     *************************************************************************/

    /**
     * Command descriptor populated by the streaming protocol parser.
     */
    class PGProtoCmdDescr {
    public:

      ProtocolCommandTag tag = INVALID_COMMAND;

      /**
       * tli holds a parsed timeline ID from the PGProtoStreamingParser.
       */
      unsigned int tli = 0;

      void setCommandTag(ProtocolCommandTag const& tag);

    };

  }

}
