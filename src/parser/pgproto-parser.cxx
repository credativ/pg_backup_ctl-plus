#include <iostream>
#include <boost/tokenizer.hpp>

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
       * Streaming Protocol command descriptor.
       */
      PGProtoCmdDescr cmd;

    public:

      /*
       * Rule return declarations
       */
      qi::rule<Iterator, ascii::space_type> start;
      qi::rule<Iterator, ascii::space_type> cmd_identify_system;

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

        cmd_identify_system = no_case[ lexeme[ lit("IDENTIFY_SYSTEM") ] ]
          [ boost::bind(&PGProtoCmdDescr::setCommandTag, &cmd, IDENTIFY_SYSTEM) ];

        start %= eps >> (
                         cmd_identify_system
                         );

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

      }

    };

  }

}
