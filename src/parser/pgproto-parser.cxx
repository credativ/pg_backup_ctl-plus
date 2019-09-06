#include <iostream>
#include <boost/tokenizer.hpp>
#include <boost/log/trivial.hpp>

/* required for string case insensitive comparison */
#include <boost/algorithm/string/predicate.hpp>

/*
 * For the boost builtin parser.
 */
#include <boost/config/warning_disable.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/spirit/include/phoenix_core.hpp>
#include <boost/spirit/include/phoenix_operator.hpp>
#include <boost/spirit/include/phoenix_object.hpp>
#include <boost/spirit/repository/include/qi_kwd.hpp>
#include <boost/spirit/repository/include/qi_keywords.hpp>
#include <boost/spirit/include/qi_repeat.hpp>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/fusion/include/io.hpp>
#include <boost/fusion/adapted/std_pair.hpp>
#include <boost/bind.hpp>
#include <complex>
#include <functional>

#include <rtconfig.hxx>
#include <pgsql-proto.hxx>
#include <pgproto-parser.hxx>

/**
 *
 * This is the PostgreSQL compatible Streaming Protocol
 * parser, implementing the Streaming Replication command
 * syntax.
 *
 */
namespace credativ {

  namespace pgprotocol {

    namespace qi      = boost::spirit::qi;
    namespace ascii   = boost::spirit::ascii;
    namespace phoenix = boost::phoenix;
    namespace fusion  = boost::fusion;

    template<typename Iterator>

    struct PGProtoStreamingParser
      : qi::grammar<Iterator, ascii::space_type> {

    private:

      /*
       * Current streaming Protocol command descriptor. This points
       * to the current active cmd instance.
       */
      std::shared_ptr<PGProtoCmdDescr> cmd;

      /**
       * Queued command instances, filled by the parser.
       */
      std::queue<std::shared_ptr<PGProtoCmdDescr>> cmd_queue;

      void newCommand(ProtocolCommandTag const& tag) {

        cmd = std::make_shared<PGProtoCmdDescr>();

        BOOST_LOG_TRIVIAL(debug) << "PGProtoStreamingParser new command" ;

        /* Record last element for current parser iteration */
        cmd_queue.push(cmd);
        cmd->setCommandTag(tag);

      }

      /**
       * Internal runtime confguration handle.
       *
       * Initialized through constructor.
       */
      std::shared_ptr<RuntimeConfiguration> runtime_configuration = nullptr;

      /*
       * Rule return declarations
       */
      qi::rule<Iterator, ascii::space_type> start;
      qi::rule<Iterator, ascii::space_type> cmd_identify_system;
      qi::rule<Iterator, ascii::space_type> cmd_list_basebackups;

    public:

      /**
       * Returns a pointer to a PGProtoCmdDescr after having
       * parsed a command string successfully. Since the parser can iterate over
       * a multi statement query string, we queue up the command options, so
       * that the caller should call getCommand() until a nullptr returns.
       * This indicates the end of the parsing queue.
       */
      std::shared_ptr<PGProtoCmdDescr> getCommand() {

        std::shared_ptr<PGProtoCmdDescr> ptr = nullptr;

        if (cmd_queue.empty())
          return ptr;

        ptr = cmd_queue.front();
        cmd_queue.pop();

        return ptr;

      }

      PGProtoStreamingParser(std::shared_ptr<RuntimeConfiguration> rtc)
        : PGProtoStreamingParser::base_type(start, "PostgreSQL replication command") {

        using qi::_val;
        using qi::_1;
        using qi::_2;
        using qi::lit;
        using qi::lexeme;
        using qi::char_;
        using qi::uint_;
        using qi::eps;
        using qi::graph;
        using qi::no_case;
        using qi::no_skip;
        using qi::repeat;

        /*
         * Basic error handling requires this.
         */
        using qi::on_error;
        using qi::fail;
        using phoenix::construct;
        using phoenix::val;

        runtime_configuration = rtc;

        /*
         * LIST_BASEBACKUPS command
         *
         * Protocol command extension
         */
        cmd_list_basebackups = no_case[ lexeme[ lit("LIST_BASEBACKUPS") ] ]
          [ boost::bind(&PGProtoStreamingParser::newCommand, this, LIST_BASEBACKUPS) ];

        /*
         * IDENTIFY_SYSTEM command
         */
        cmd_identify_system = no_case[ lexeme[ lit("IDENTIFY_SYSTEM") ] ]
          [ boost::bind(&PGProtoStreamingParser::newCommand, this, IDENTIFY_SYSTEM) ];

        start %= eps >> (
                         cmd_identify_system

                         |

                         cmd_list_basebackups

                         ) >> lit(";")
                     >> -(start);

        /*
         * error handling
         */
        on_error<fail>(start,
                       std::cerr
                       << val("Error! Expecting ")
                       << qi::_4
                       << val(" here: \"")
                       << construct<std::string>(qi::_3, qi::_2)
                       << val("\" ")
                       << std::endl
                       );

        /* Command captions */

        start.name("command");
        cmd_identify_system.name("IDENTIFY_SYSTEM");
        cmd_list_basebackups.name("LIST_BASEBACKUPS");

      }

    };

  }

}

using namespace credativ;
using namespace credativ::pgprotocol;

PostgreSQLStreamingParser::PostgreSQLStreamingParser(std::shared_ptr<RuntimeConfiguration> rtc) {

  runtime_configuration = rtc;

}

PostgreSQLStreamingParser::~PostgreSQLStreamingParser() {}

PGProtoCommandExecutionQueue PostgreSQLStreamingParser::parse(std::string cmdstr) {

  using boost::spirit::ascii::space;
  typedef std::string::iterator iterator_type;
  typedef credativ::pgprotocol::PGProtoStreamingParser<iterator_type> PGProtoStreamingParser;

  /*
   * Dispose previous command handler...
   */
  reset();

  /*
   * Empty strings are effectively a no-op.
   */
  if (cmdstr.length() == 0)
    return cmd_exec_queue;

  /* Prepare the parsing steps */

  PGProtoStreamingParser myparser(runtime_configuration);

  std::string::iterator iter = cmdstr.begin();
  std::string::iterator end  = cmdstr.end();

  bool parse_result = phrase_parse(iter, end, myparser, space);

  if (parse_result && iter == end) {

    std::shared_ptr<PGProtoCmdDescr> cmd = nullptr;

    while ( (cmd = myparser.getCommand()) != nullptr) {

      std::shared_ptr<ProtocolCommandHandler> command_handler
        = std::make_shared<ProtocolCommandHandler>(cmd, runtime_configuration);

      /* Save in command execution queue */
      cmd_exec_queue.push(command_handler);

    }

  } else {

    throw PGProtoCmdFailure("aborted due to parser error");
  }

  return cmd_exec_queue;

}

void PostgreSQLStreamingParser::reset() {

  cmd_exec_queue = PGProtoCommandExecutionQueue();

}
