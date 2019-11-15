#ifndef __HAVE_PGPROTO_PARSER__
#define __HAVE_PGPROTO_PARSER__

#include <proto-catalog.hxx>
#include <pgproto-commands.hxx>

namespace credativ {

  namespace pgprotocol {

    typedef std::queue<std::shared_ptr<PGProtoCmdDescr>> PGProtoParsedCmdQueue;
    typedef std::queue<std::shared_ptr<ProtocolCommandHandler>> PGProtoCommandExecutionQueue;

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
      PGProtoCommandExecutionQueue cmd_exec_queue;

    public:

      PostgreSQLStreamingParser(std::shared_ptr<RuntimeConfiguration> rtc);
      virtual ~PostgreSQLStreamingParser();

      /**
       * Parse the specified query string and return
       * a command execution queue.
       *
       * Can return an empty queue in case no valid query string was parsed.
       *
       * Throws a PGProtoCmdFailure exception in case a parsing
       * error occured.
       */
      virtual PGProtoCommandExecutionQueue parse(std::shared_ptr<PGProtoCatalogHandler> catalogHandler,
                                                 std::string cmdstr);

      /**
       * Clears internal command execution queues.
       */
      virtual void reset();

    };

  }

}

#endif
