#ifndef __HAVE_PGPROTO_PARSER__
#define __HAVE_PGPROTO_PARSER__

#include <pgproto-commands.hxx>

namespace credativ {

  namespace pgprotocol {

    /**
     * The PostgreSQL streaming backup protocol command
     * parser.
     */
    class PostgreSQLStreamingParser {
    private:

      /**
       * Internal reference to runtime configuration instance.
       */
      std::shared_ptr<RuntimeConfiguration> runtime_configuration = nullptr;

      /**
       * Reference to the command handler object.
       *
       * Only initialized after having called parse().
       */
      std::shared_ptr<ProtocolCommandHandler> command_handler = nullptr;

    public:

      PostgreSQLStreamingParser(std::shared_ptr<RuntimeConfiguration> rtc);
      virtual ~PostgreSQLStreamingParser();

      /**
       * Parse the specified string and return
       * an protocol command handler instance.
       *
       * Can return a nullptr in case parsing exits because
       * cmdstr was empty.
       *
       * Throws a PGProtoCmdFailure exception in case a parsing
       * error occured.
       */
      virtual std::shared_ptr<ProtocolCommandHandler> parse(std::string cmdstr);

    };

  }

}

#endif
