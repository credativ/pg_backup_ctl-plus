#include <BackupCatalog.hxx>
#include <catalog.hxx>
#include <commands.hxx>
#include <parser.hxx>

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

using namespace boost::filesystem;

/*
 * This is the core parser implementation for credativ::boostparser
 *
 * Internal parser implementation
 * uses boost::spirit. Make them private to the
 * parser module within their own namespace.
 */
namespace credativ {
  namespace boostparser {

    namespace qi      = boost::spirit::qi;
    namespace ascii   = boost::spirit::ascii;
    namespace phoenix = boost::phoenix;
    namespace fusion  = boost::fusion;

    template<typename Iterator>
    struct PGBackupCtlBoostParser
      : qi::grammar<Iterator, ascii::space_type> {

    private:

      credativ::CatalogDescr cmd;

    public:

      credativ::CatalogDescr getCommand() { return this->cmd; }

      PGBackupCtlBoostParser(shared_ptr<RuntimeConfiguration> rtc)
        : PGBackupCtlBoostParser::base_type(start, "pg_backup_ctl command") {

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

        /*
         * Make sure configuration environment is known
         * to the parser's internal catalog descriptor.
         */
        this->cmd.assignRuntimeConfiguration(rtc);

        /*
         * Parser rule definitions
         */
        start %= eps >> (
                         /* APPLY syntax */
                         (
                          cmd_apply
                          )
                         |
                         /* SET syntax */
                         (
                          cmd_set
                          )
                         |
                         /* SHOW syntax */
                         (
                          cmd_show
                          )
                         |
                         /* RESET syntax */
                         (
                          cmd_reset
                          )
                         |
                         /* EXEC syntax */
                         (
                          no_case[lexeme[ lit("EXEC") ]]
                          [boost::bind(&CatalogDescr::setCommandTag, &cmd, EXEC_COMMAND)]
                          >> executable
                          [boost::bind(&CatalogDescr::setExecString, &cmd, ::_1)]
                          )
                         |

                         /* CREATE command syntax start */
                         (
                          cmd_create >> (
                                         cmd_create_archive
                                         | cmd_create_backup_profile
                                         | cmd_create_connection
                                         | cmd_create_retention
                                         )
                          )

                         /* LIST command syntax start */
                         | (
                            cmd_list >> (
                                         cmd_list_archive
                                         | cmd_list_backup
                                         | cmd_list_connection
                                         | cmd_list_backup_list
                                         | cmd_list_retention
                                         )
                            )

                         /* ALTER command */
                         | (
                            cmd_alter >> (
                                          cmd_alter_archive
                                          | cmd_alter_backup_profile
                                          )
                            )

                         /*
                          * DROP command syntax start
                          */
                         | (
                            cmd_drop >> (
                                         /*
                                          * DROP ARCHIVE <name> command
                                          */
                                         ( cmd_drop_archive >> identifier
                                           [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ] )

                                         /*
                                          * DROP BACKUP PROFILE
                                          */
                                         | cmd_drop_backup_profile

                                         /*
                                          * DROP STREAMING CONNECTION
                                          */
                                         | cmd_drop_connection

                                         /* DROP RETENTION POLICY */
                                         | cmd_drop_retention

                                         /* DROP BASEBACKUP */
                                         | cmd_drop_basebackup )
                            )

                         /*
                          * VERIFY ARCHIVE <name> command
                          */
                         | ( cmd_verify_archive >> identifier
                             [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ]
                             >> -verify_check_connection
                             [ boost::bind(&CatalogDescr::setVerifyOption, &cmd, VERIFY_DATABASE_CONNECTION) ] )

                         /*
                          * START command
                          */
                         | (
                            cmd_start_command >> (
                                                  /*
                                                   * START BASEBACKUP FOR ARCHIVE <name> command
                                                   */
                                                  ( cmd_start_basebackup
                                                    >> identifier
                                                    [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ]
                                                    >> -(with_profile) >> -(force_systemid_update) )
                                                  | ( cmd_start_launcher )
                                                  | ( cmd_start_streaming )
                                                  | ( cmd_start_recovery_stream )
                                                  )
                            )

                         /*
                          * STOP command
                          */
                         | (
                            cmd_stop_command >> ( ( cmd_stop_streaming >> identifier
                                                    [boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ] ) )
                            )

                         /*
                          * PIN command
                          */
                         | (
                            cmd_pin_basebackup
                            )

                         /*
                          * UNPIN command
                          */
                         | (
                            cmd_unpin_basebackup
                            )
                         ); /* start rule end */

        /*
         * PIN { basebackup ID | OLDEST | NEWEST | +n }
         */
        cmd_pin_basebackup = no_case[ lexeme[ lit("PIN") ] ]
          [ boost::bind(&CatalogDescr::setCommandTag,
                        &cmd, PIN_BASEBACKUP) ]>> ( number_ID
                                                    [ boost::bind(&CatalogDescr::makePinDescr,
                                                                  &cmd, ACTION_ID, ::_1) ]
                                                    | no_case[ lexeme[ lit("OLDEST") ]  ]
                                                    [ boost::bind(&CatalogDescr::makePinDescr,
                                                                  &cmd, ACTION_OLDEST) ]
                                                    | no_case[ lexeme[ lit("NEWEST") ] ]
                                                    [ boost::bind(&CatalogDescr::makePinDescr,
                                                                  &cmd, ACTION_NEWEST) ]
                                                    | ( lexeme[ lit("+") ] >> number_ID
                                                        [ boost::bind(&CatalogDescr::makePinDescr,
                                                                      &cmd, ACTION_COUNT, ::_1)]
                                                        )
                                                    )
                                               >> no_case[ lexeme[ lit("IN") ] ]
                                               >> no_case[ lexeme[ lit("ARCHIVE") ] ]
                                               >> identifier [ boost::bind(&CatalogDescr::setIdent,
                                                                           &cmd,
                                                                           ::_1) ] ;

        /*
         * UNPIN { basebackup ID | OLDEST | NEWEST | PINNED | +n }
         */
        cmd_unpin_basebackup = no_case[ lexeme[ lit("UNPIN") ] ]
          [ boost::bind(&CatalogDescr::setCommandTag,
                        &cmd, UNPIN_BASEBACKUP) ] >> ( number_ID
                                                       [ boost::bind(&CatalogDescr::makePinDescr, &cmd, ACTION_ID, ::_1) ]
                                                       | no_case[ lexeme[ lit("OLDEST") ]  ]
                                                       [ boost::bind(&CatalogDescr::makePinDescr,
                                                                     &cmd,
                                                                     ACTION_OLDEST) ]
                                                       | no_case[ lexeme[ lit("NEWEST") ] ]
                                                       [ boost::bind(&CatalogDescr::makePinDescr,
                                                                     &cmd,
                                                                     ACTION_NEWEST) ]
                                                       | no_case[ lexeme[ lit("PINNED") ] ]
                                                       [ boost::bind(&CatalogDescr::makePinDescr,
                                                                     &cmd,
                                                                     ACTION_PINNED) ]
                                                       | ( lexeme[ lit("+") ] >> number_ID
                                                           [ boost::bind(&CatalogDescr::makePinDescr,
                                                                         &cmd, ACTION_COUNT, ::_1)])
                                                       )
                                                  >> no_case[ lexeme[ lit("IN") ] ]
                                                  >> no_case[ lexeme[ lit("ARCHIVE") ] ]
                                                  >> identifier [ boost::bind(&CatalogDescr::setIdent,
                                                                              &cmd,
                                                                              ::_1) ];

        /* parses ID */
        number_ID = +char_("0-9");

        /*
         * ALTER ARCHIVE <name> command
         */
        cmd_alter_archive_opt = identifier
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ]
          >> ( no_case[ lexeme[ lit("SET") ] ]
              ^ ( directory
                  [ boost::bind(&CatalogDescr::setDirectory, &cmd, ::_1) ] )
              ^ ( ( hostname
                    [ boost::bind(&CatalogDescr::setHostname, &cmd, ::_1) ]
                    ^ database
                    [ boost::bind(&CatalogDescr::setDbName, &cmd, ::_1) ]
                    ^ username
                    [ boost::bind(&CatalogDescr::setUsername, &cmd, ::_1) ]
                    ^ portnumber
                    [ boost::bind(&CatalogDescr::setPort, &cmd, ::_1) ] )
                  |
                  ( no_case[ lexeme[ lit("DSN") ]] > -lit("=") > dsn_connection_string
                    [ boost::bind(&CatalogDescr::setDSN, &cmd, ::_1) ] ) ) );

        /*
         * START LAUNCHER command
         */
        cmd_start_launcher = no_case[lexeme[ lit("LAUNCHER") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, START_LAUNCHER) ]
          ^ no_case[lexeme[ lit("NODETACH") ]]
          [ boost::bind(&CatalogDescr::setJobDetachMode, &cmd, false) ];

        /*
         * APPLY command
         */
        cmd_apply = no_case[lexeme[ lit("APPLY") ] ]
          >> cmd_apply_retention;

        /*
         * APPLY RETENTION POLICY <identifier> TO ARCHIVE <identifier>
         */
        cmd_apply_retention = no_case[ lexeme[ lit("RETENTION") ]]
          >> no_case[ lexeme[ lit("POLICY") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, APPLY_RETENTION_POLICY) ]
          >> identifier
          [ boost::bind(&CatalogDescr::setRetentionName, &cmd, ::_1) ]
          >> no_case[ lexeme[ lit("TO") ]]
          >> no_case[ lexeme[ lit("ARCHIVE") ]]
          >> identifier
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ];

        /* SET <class.variable name> TO <variable value> */
        cmd_set = no_case[ lexeme[ lit("SET") ]]
          >> no_case[ lexeme[ lit("VARIABLE") ] ]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, SET_VARIABLE) ]
          >> cmd_set_variable;

        cmd_set_variable = variable_name
          [ boost::bind(&CatalogDescr::setVariableName, &cmd, ::_1) ]
          >> no_case[ lexeme[ lit("=") ] ]
          >> variable_value;

        variable_name = repeat(1, boost::spirit::inf)[char_("0-9A-Za-z_")]
          >> char_(".")
          >> repeat(1, boost::spirit::inf)[char_("A-Za-z_")];

        variable_value = ( ( no_case[ lexeme [ lit("on") ] ]
                             [ boost::bind(&CatalogDescr::setVariableValueBool, &cmd, true) ]
                             |
                             no_case[ lexeme[ lit("off") ] ]
                             [ boost::bind(&CatalogDescr::setVariableValueBool, &cmd, false) ]
                             |
                             no_case[ lexeme[ lit("true") ] ]
                             [ boost::bind(&CatalogDescr::setVariableValueBool, &cmd, true) ]
                             |
                             no_case[ lexeme[ lit("false") ] ]
                             [ boost::bind(&CatalogDescr::setVariableValueBool, &cmd, false) ] )
                           | ( variable_value_string
                               [ boost::bind(&CatalogDescr::setVariableValueString, &cmd, ::_1) ] )
                           | ( number_ID
                               [ boost::bind(&CatalogDescr::setVariableValueInteger, &cmd, ::_1) ] ) );

        variable_value_string = +(char_("A-Za-z"));

        /* RESET <runtime variable> */
        cmd_reset = no_case[ lexeme[ lit("RESET") ] ]
          >> no_case[ lexeme[ lit("VARIABLE") ] ]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, RESET_VARIABLE) ]
          >> variable_name
          [ boost::bind(&CatalogDescr::setVariableName, &cmd, ::_1) ];

        /* SHOW ( VARIABLES | WORKERS | <runtime variable> ) */
        cmd_show = no_case[lexeme[ lit("SHOW") ]]
          >> show_command_type;

        show_command_type
          = ( no_case[lexeme[ lit("WORKERS") ]]
              [boost::bind(&CatalogDescr::setCommandTag, &cmd, SHOW_WORKERS)] )

          |

          ( no_case[lexeme[ lit("VARIABLES") ]]
            [ boost::bind(&CatalogDescr::setCommandTag, &cmd, SHOW_VARIABLES) ] )

          | ( no_case[ lexeme[ lit("VARIABLE") ] ]
              [ boost::bind(&CatalogDescr::setCommandTag, &cmd, SHOW_VARIABLE) ]
              >> variable_name
              [ boost::bind(&CatalogDescr::setVariableName, &cmd, ::_1) ] );

        /* START RECOVERY command */

        cmd_start_recovery_stream = no_case[ lexeme[ lit("RECOVERY") ] ]
          [ boost::bind(&CatalogDescr::makeRecoveryStreamDescr, &cmd) ]
          >> no_case[ lexeme[ lit("STREAM") ] ]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, START_RECOVERY_STREAM_FOR_ARCHIVE) ]
          >> no_case[ lexeme[ lit("FOR") ] ]
          >> no_case[ lexeme[ lit("ARCHIVE") ] ]
          >> identifier
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ]
          >> -(no_case[ lexeme[ lit("PORT") ] ] >> number_ID
               [ boost::bind(&CatalogDescr::setRecoveryStreamPort, &cmd, ::_1) ])
          >> -(stream_listen_on)
          >> -( no_case[ lexeme[ lit("NODETACH") ] ]
                [ boost::bind(&CatalogDescr::setJobDetachMode, &cmd, false) ] );

        stream_listen_on = no_case[ lexeme[ lit("LISTEN_ON") ] ]
          >> ip_address_list;

        ip_address_list = lexeme[ lit("(") ]
          >> ip_address
          [ boost::bind(&CatalogDescr::setRecoveryStreamAddr, &cmd, ::_1) ]
          >> -(ip_address_item)
          >> lexeme[ lit(")") ];

        ip_address_item = lexeme[ lit(",") ]
          >> ip_address
          [ boost::bind(&CatalogDescr::setRecoveryStreamAddr, &cmd, ::_1) ]
          >> -(ip_address_item);

        ip_address = +char_("0-9a-zA-Z.:");

        /*
         * START STREAMING FOR ARCHIVE command
         */
        cmd_start_streaming = no_case[lexeme[ lit("STREAMING") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, START_STREAMING_FOR_ARCHIVE) ]
          >> no_case[ lexeme[ lit("FOR ARCHIVE") ] ]
          >> identifier
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ]
          >> -( no_case[lexeme[ lit("RESTART") ]]
               [ boost::bind(&CatalogDescr::setStreamingForceXLOGPositionRestart, &cmd, true) ] )
          >> -( no_case[lexeme[ lit("NODETACH") ]]
               [ boost::bind(&CatalogDescr::setJobDetachMode, &cmd, false) ] );

        /*
         * CREATE, DROP, ALTER, START and LIST tokens...
         */
        cmd_create = no_case[lexeme[ lit("CREATE") ]] ;

        cmd_drop = no_case[lexeme[ lit("DROP") ]];

        cmd_list = no_case[lexeme[ lit("LIST") ]];

        cmd_alter = no_case[lexeme[ lit("ALTER") ]];

        cmd_start_command = no_case[lexeme[ lit("START") ]];

        cmd_stop_command = no_case[lexeme[ lit("STOP") ]];

        verify_check_connection = no_case[lexeme[ lit("CONNECTION") ]];

        /*
         * LIST RETENTION { POLICIES | POLICY <identifier> }
         */
        cmd_list_retention = no_case[ lexeme[ lit("RETENTION") ]]
          >> ( ( no_case[ lexeme[ lit("POLICIES") ] ]
                [ boost::bind(&CatalogDescr::setCommandTag, &cmd, LIST_RETENTION_POLICIES) ] )
              |
              ( no_case[ lexeme[ lit("POLICY") ] ]
                [ boost::bind(&CatalogDescr::setCommandTag, &cmd, LIST_RETENTION_POLICY) ]
                >> identifier
                [ boost::bind(&CatalogDescr::setRetentionName, &cmd, ::_1) ] ) );

        /*
         * LIST CONNECTION FOR ARCHIVE <archive name > command
         */
        cmd_list_connection = no_case[ lexeme[ lit("CONNECTION") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, LIST_CONNECTION) ]
          >> no_case[ lexeme[ lit("FOR ARCHIVE") ]]
          >> identifier
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ];

        /*
         * LIST BASEBACKUPS IN ARCHIVE <identifier>
         */
        cmd_list_backup_list = no_case[ lexeme[ lit("BASEBACKUPS") ] ]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, LIST_BACKUP_LIST) ]
          >> no_case[ lexeme[ lit("IN") ]] > no_case[ lexeme[ lit("ARCHIVE") ] ]
          >> identifier
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ]
          >> -(no_case[ lexeme[ lit("VERBOSE") ] ])
          [ boost::bind(&CatalogDescr::setPrintVerbose, &cmd, true) ];

        /*
         * LIST BACKUP CATALOG [<backup>] ...
         *             | PROFILE [ <profile> ] ... command
         */
        cmd_list_backup = no_case[ lexeme[ lit("BACKUP") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, LIST_BACKUP_CATALOG) ]
          >> ( ( no_case[ lexeme[ lit("CATALOG") ] ]
                 >> identifier
                 [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ] )
               | ( no_case[lexeme [ lit("PROFILE") ] ]
                   [ boost::bind(&CatalogDescr::setCommandTag, &cmd, LIST_BACKUP_PROFILE) ]
                   >> -(identifier)
                   [ boost::bind(&CatalogDescr::setProfileName, &cmd, ::_1) ]
                   [  boost::bind(&CatalogDescr::setCommandTag, &cmd, LIST_BACKUP_PROFILE_DETAIL) ] ) );


        /*
         * LIST ARCHIVE [<name>] command
         */
        cmd_list_archive = no_case[ lexeme[ lit("ARCHIVE") ] ]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, LIST_ARCHIVE) ]
          >> -(identifier)
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ];

        /*
         * CREATE BACKUP PROFILE <name> command
         */
        cmd_create_backup_profile = no_case[lexeme [ lit("BACKUP") ]]
          >> no_case[lexeme [ lit("PROFILE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, CREATE_BACKUP_PROFILE) ]
          >> identifier
          [ boost::bind(&CatalogDescr::setProfileName, &cmd, ::_1) ]
          >> backup_profile_opts;

        backup_profile_opts =
          -(profile_compression_option)
          >> -(profile_max_rate_option
              [ boost::bind(&CatalogDescr::setProfileMaxRate, &cmd, ::_1) ])
          >> -(profile_backup_label_option)
          >> -(profile_wal_option)
          >> -(profile_checkpoint_option)
          >> -(profile_wait_for_wal_option)
          >> -(profile_noverify_checksums_option);

        /*
         * CREATE RETENTION POLICY <identifier>
         */
        cmd_create_retention =
          no_case[ lexeme[ lit("RETENTION") ] ]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, CREATE_RETENTION_POLICY) ]
          >> no_case[ lexeme[ lit("POLICY") ] ]
          >> identifier
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ]
          >> ( retention_keep_action
               | retention_drop_action
               | retention_cleanup_basebackups
               /*
                * CREATE RETENTION POLICY ... CLEANUP is a DROP action
                *
                * NOTE: Even though the parser set a specific value here, the cleanup
                *       retention policy implementation doesn't depend on it. But the
                *       catalog representation requires a valid string value here, so
                *       use "cleanup" as a placeholder here.
                */
               [ boost::bind(&CatalogDescr::makeRetentionRule, &cmd, RETENTION_CLEANUP, string("cleanup")) ] );

        retention_keep_action =
          no_case[ lexeme[ lit("KEEP") ] ]
          [ boost::bind(&CatalogDescr::setRetentionAction, &cmd, RETENTION_ACTION_KEEP) ]
          >> ( ( retention_rule_with_label
                 [ boost::bind(&CatalogDescr::setRetentionActionModifier, &cmd, RETENTION_MODIFIER_LABEL) ] )
               |
               retention_rule_newer_datetime
               |
               ( retention_rule_num_basebackups
                 [ boost::bind(&CatalogDescr::setRetentionActionModifier, &cmd, RETENTION_MODIFIER_NUM) ] )
               );

        retention_drop_action =
          no_case[ lexeme[ lit("DROP") ] ]
          [ boost::bind(&CatalogDescr::setRetentionAction, &cmd, RETENTION_ACTION_DROP) ]
          >> ( ( retention_rule_with_label
                 [ boost::bind(&CatalogDescr::setRetentionActionModifier, &cmd, RETENTION_MODIFIER_LABEL) ] )
               |
               retention_rule_older_datetime
               |
               retention_rule_num_basebackups
               );

        retention_cleanup_basebackups = no_case[lexeme[ lit("CLEANUP") ]];

        retention_rule_num_basebackups = lexeme[ lit("+") ] >> number_ID
          [ boost::bind(&CatalogDescr::setRetentionActionModifier, &cmd, RETENTION_MODIFIER_NUM) ]
          [ boost::bind(&CatalogDescr::makeRuleFromParserState, &cmd, ::_1) ];

        retention_rule_with_label =
          no_case[ lexeme[ lit("WITH") ] ]
          >> no_case[ lexeme[ lit("LABEL") ] ]
          [ boost::bind(&CatalogDescr::setRetentionActionModifier, &cmd, RETENTION_MODIFIER_LABEL) ]
          >> regexp_expression
          [ boost::bind(&CatalogDescr::makeRuleFromParserState, &cmd, ::_1) ];

        retention_rule_older_datetime =
          no_case[ lexeme[ lit("OLDER") ] ] >> ( no_case[ lexeme[ lit("THAN") ] ]
                                                 /* boost::bind() cannot bind an overloaded method easily, so
                                                  * just create a policy with an empty rule. Use retentionIntervalExprFromParserState()
                                                  * later to compile the correct rule. */
                                                 [ boost::bind(&CatalogDescr::setRetentionActionModifier,
                                                               &cmd, RETENTION_MODIFIER_OLDER_DATETIME) ] )
                                            >> ( retention_datetime_spec );

        retention_rule_newer_datetime =
          no_case[ lexeme[ lit("NEWER") ] ] >> ( no_case[ lexeme[ lit("THAN") ] ]
                                                 /* boost::bind() cannot bind an overloaded method easily, so
                                                  * just create a policy with an empty rule. Use addRetentionIntervalExpr()
                                                  * later to compile the correct rule. */
                                                 [ boost::bind(&CatalogDescr::setRetentionActionModifier,
                                                               &cmd, RETENTION_MODIFIER_NEWER_DATETIME) ] )
                                            >> ( retention_datetime_spec );

        retention_datetime_spec =
          -(retention_datetime_years
            [ boost::bind(&CatalogDescr::retentionIntervalExprFromParserState, &cmd, ::_1, "years") ])
          ^ (retention_datetime_months
             [ boost::bind(&CatalogDescr::retentionIntervalExprFromParserState, &cmd, ::_1, "months") ])
          ^ (retention_datetime_days
             [ boost::bind(&CatalogDescr::retentionIntervalExprFromParserState, &cmd, ::_1, "days") ])
          ^ (retention_datetime_hours
             [ boost::bind(&CatalogDescr::retentionIntervalExprFromParserState, &cmd, ::_1, "hours") ])
          ^ (retention_datetime_minutes
             [ boost::bind(&CatalogDescr::retentionIntervalExprFromParserState, &cmd, ::_1, "minutes") ]);

        retention_datetime_years = +(char_("0-9")) >> no_case[ lexeme[ lit("YEARS") ] ];
        retention_datetime_months = +(char_("0-9")) >> no_case[ lexeme[ lit("MONTHS") ] ];
        retention_datetime_days = +(char_("0-9")) >> no_case[ lexeme[ lit("DAYS") ] ];
        retention_datetime_hours = +(char_("0-9")) >> no_case[ lexeme[ lit("HOURS") ] ];
        retention_datetime_minutes = +(char_("0-9")) >> no_case[ lexeme[ lit("MINUTES") ] ];

        regexp_expression =
          +char_("+-_A-Za-z0-9.[{}()\\*+?|^$");

        /*
         * CREATE STREAMING CONNECTION FOR ARCHIVE <name> command
         */
        cmd_create_connection =
          no_case[ lexeme[ lit("STREAMING") ]]
          [ boost::bind(&CatalogDescr::setConnectionType, &cmd, ConnectionDescr::CONNECTION_TYPE_STREAMER) ]
          >> no_case[ lexeme[ lit("CONNECTION") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, CREATE_CONNECTION) ]
          >> no_case[ lexeme[ lit("FOR ARCHIVE") ]]
          >> ( identifier
              [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ]
              ^ ( ( hostname
                    [ boost::bind(&CatalogDescr::setHostname, &cmd, ::_1) ]
                    ^ database
                    [ boost::bind(&CatalogDescr::setDbName, &cmd, ::_1) ]
                    ^ username
                    [ boost::bind(&CatalogDescr::setUsername, &cmd, ::_1) ]
                    ^ portnumber
                    [ boost::bind(&CatalogDescr::setPort, &cmd, ::_1) ] )
                  |
                  ( no_case[ lexeme[ lit("DSN") ]] > -lit("=") > dsn_connection_string
                    [ boost::bind(&CatalogDescr::setDSN, &cmd, ::_1) ] ) ) );

        /*
         * CREATE ARCHIVE <name> command
         */
        cmd_create_archive = no_case[lexeme [ lit("ARCHIVE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, CREATE_ARCHIVE) ]

          >> identifier
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ]

          >> ( no_case[ lexeme[ lit("PARAMS") ] ]

               >> directory
               [ boost::bind(&CatalogDescr::setDirectory, &cmd, ::_1) ]
               >> ( ( hostname
                     [ boost::bind(&CatalogDescr::setHostname, &cmd, ::_1) ]
                      >> database
                      [ boost::bind(&CatalogDescr::setDbName, &cmd, ::_1) ]
                      >> username
                      [ boost::bind(&CatalogDescr::setUsername, &cmd, ::_1) ]
                      >> portnumber
                      [ boost::bind(&CatalogDescr::setPort, &cmd, ::_1) ] )
                    /* alternative DSN syntax */
                    | ( no_case[ lexeme[ lit("DSN") ] ]
                        >> -lit("=")
                        >> dsn_connection_string
                        [ boost::bind(&CatalogDescr::setDSN, &cmd, ::_1)] )
                    ) /* hostname / DSN alternative */
               );

        dsn_connection_string = '"' >> no_skip[+(char_ - ('"'))] >> '"';

        cmd_verify_archive = no_case[lexeme[ lit("VERIFY") ]]
          >> no_case[lexeme [ lit("ARCHIVE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, VERIFY_ARCHIVE) ];

        /*
         * DROP BASEBACKUP <ID> FROM ARCHIVE <identifier>
         */
        cmd_drop_basebackup = no_case[ lexeme[ lit("BASEBACKUP") ] ]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, DROP_BASEBACKUP) ]
          >> number_ID
          [ boost::bind(&CatalogDescr::setBasebackupID, &cmd, ::_1) ]
          >> no_case[ lexeme[ lit("FROM") ] ] >> no_case[ lexeme[ lit("ARCHIVE") ] ]
          >> identifier
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ];

        /*
         * DROP RETENTION POLICY <identifier>
         */
        cmd_drop_retention = no_case[ lexeme[ lit("RETENTION") ] ]
          >> no_case[ lexeme[ lit("POLICY") ] ]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, DROP_RETENTION_POLICY) ]
          >> identifier
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ];

        /*
         * DROP STREAMING CONNECTION FROM ARCHIVE <archive name>
         */
        cmd_drop_connection = no_case[ lexeme[ lit("STREAMING") ]]
          [ boost::bind(&CatalogDescr::setConnectionType, &cmd, ConnectionDescr::CONNECTION_TYPE_STREAMER) ]
          >> no_case[ lexeme[ lit("CONNECTION") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, DROP_CONNECTION) ]
          >> no_case[ lexeme[ lit("FROM ARCHIVE") ]]
          >> identifier
          [ boost::bind(&CatalogDescr::setIdent, &cmd, ::_1) ];

        cmd_drop_backup_profile = no_case[lexeme[ lit("BACKUP") ]]
          >> no_case[lexeme[ lit("PROFILE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, DROP_BACKUP_PROFILE) ]
          >> identifier
          [ boost::bind(&CatalogDescr::setProfileName, &cmd, ::_1) ];

        cmd_drop_archive = no_case[lexeme[ lit("ARCHIVE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, DROP_ARCHIVE) ];

        cmd_alter_archive = no_case[lexeme[ lit("ARCHIVE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, ALTER_ARCHIVE) ]
          >> cmd_alter_archive_opt;

        cmd_start_basebackup = no_case[lexeme[ lit("BASEBACKUP") ]]
          >> no_case[lexeme[ lit("FOR") ]] >> no_case[lexeme[ lit("ARCHIVE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, START_BASEBACKUP) ];

        cmd_stop_streaming = no_case[lexeme[ lit("STREAMING") ]]
          >> no_case[lexeme[ lit("FOR") ]] >> no_case[lexeme[ lit("ARCHIVE") ]]
          [ boost::bind(&CatalogDescr::setCommandTag, &cmd, STOP_STREAMING_FOR_ARCHIVE) ];

        /*
         * Property clauses
         */
        property_string = lexeme [ +(graph - (",;[]{} ")) ];
        hostname = no_case[lexeme[ lit("PGHOST") ]] > -lit("=") > property_string;
        database = no_case[lexeme[ lit("PGDATABASE") ]] > -lit("=") > property_string;
        username = no_case[lexeme[ lit("PGUSER") ]] > -lit("=") > property_string;
        directory = no_case[lexeme[ lit("DIRECTORY") ]] > -lit("=") > directory_string;
        portnumber = no_case[lexeme[ lit("PGPORT") ]] > -lit("=") > +(char_("0-9"));

        /*
         * Rule to read in COMPRESSION=<BACKUP_COMPRESSION_TYPE>
         */
        profile_compression_option =
          no_case[lexeme[ lit("COMPRESSION") ]]
          >> -lit("=")
          >> (no_case[lexeme[ lit("GZIP") ]]
              [ boost::bind(&CatalogDescr::setProfileCompressType, &cmd, BACKUP_COMPRESS_TYPE_GZIP) ]
              | no_case[lexeme[ lit("NONE") ]]
              [ boost::bind(&CatalogDescr::setProfileCompressType, &cmd, BACKUP_COMPRESS_TYPE_NONE) ]
              | no_case[lexeme[ lit("ZSTD") ]]
              [ boost::bind(&CatalogDescr::setProfileCompressType, &cmd, BACKUP_COMPRESS_TYPE_ZSTD) ]
              | no_case[lexeme[ lit("PBZIP") ]]
              [ boost::bind(&CatalogDescr ::setProfileCompressType, &cmd, BACKUP_COMPRESS_TYPE_PBZIP)]
              | no_case[lexeme[ lit("PLAIN") ]]
              [ boost::bind(&CatalogDescr ::setProfileCompressType, &cmd, BACKUP_COMPRESS_TYPE_PLAIN)]);

        /*
         * CREATE BACKUP PROFILE ...  MAX_RATE=<kbps>
         */
        profile_max_rate_option = no_case[lexeme[ lit("MAX_RATE") ]]
          >> -lit("=")
          >> +(char_("0-9"));

        /*
         * CREATE BACKUP PROFILE ... LABEL="<label>"
         *
         * We want to allow file and directory names as a
         * label.
         */
        profile_backup_label_option = no_case[lexeme[ lit("LABEL") ]]
          >> -lit("=")
          >> directory_string
          [boost::bind(&CatalogDescr::setProfileBackupLabel, &cmd, ::_1)];

        /*
         * CREATE BACKUP PROFILE ... WAL=<INCLUDED|EXCLUDED>
         */
        profile_wal_option = no_case[lexeme[ lit("WAL") ]]
          >> -lit("=")
          >> (no_case[lexeme[ lit("INCLUDED") ]]
              [ boost::bind(&CatalogDescr::setProfileWALIncluded, &cmd, true) ]
              |
              no_case[lexeme[ lit("EXCLUDED") ]]
              [ boost::bind(&CatalogDescr::setProfileWALIncluded, &cmd, false) ]
              );

        /*
         * CREATE BACKUP PROFILE ... CHECKPOINT=FAST|DELAYED
         */
        profile_checkpoint_option = no_case[lexeme[ lit("CHECKPOINT") ]]
          >> -lit("=")
          >> (no_case[lexeme[ lit("FAST") ]]
              [ boost::bind(&CatalogDescr::setProfileCheckpointMode, &cmd, true) ]
              | no_case[lexeme[ lit("DELAYED") ]]
              [ boost::bind(&CatalogDescr::setProfileCheckpointMode, &cmd, false) ]
              );

        /*
         * CREATE BACKUP PROFILE ... WAIT_FOR_WAL=TRUE|FALSE
         */
        profile_wait_for_wal_option = no_case[lexeme[ lit("WAIT_FOR_WAL") ]]
          >> -lit("=")
          >> (no_case[lexeme[ lit("TRUE") ]]
              [ boost::bind(&CatalogDescr::setProfileWaitForWAL, &cmd, true) ]
              | no_case[lexeme[ lit("FALSE") ]]
              [ boost::bind(&CatalogDescr::setProfileWaitForWAL, &cmd, false) ]
              );

        /*
         * CREATE BACKUP PROFILE ... NOVERIFY
         */
        profile_noverify_checksums_option = no_case[lexeme[ lit("NOVERIFY") ]]
          >> -lit("=")
          >> (no_case[lexeme[ lit("TRUE") ]]
              [ boost::bind(&CatalogDescr::setProfileNoVerify, &cmd, true) ]
              | no_case[lexeme[ lit("FALSE") ]]
              [ boost::bind(&CatalogDescr::setProfileNoVerify, &cmd, false) ]
              );

        /*
         * We try to support both, quoted and unquoted identifiers. With quoted
         * identifiers, we disallow any embedded double quotes, too.
         */
        identifier = ( lexeme [ '"' >> +(char_("a-zA-Z0-9")) >> '"' ]
                       | lexeme [ +(char_("a-zA-Z0-9")) ] );

        /* We enforce quoting for path strings */
        directory_string = no_skip[ '"' >> +(char_ - ('"') ) >> '"' ];

        /* A binary name (executable without full path */
        executable = +(char_("a-zA-Z0-9_-+/"));

        /* PROFILE property */
        with_profile = no_case[lexeme [ lit("PROFILE") ]] >> identifier
          [ boost::bind(&CatalogDescr::setProfileName, &cmd, ::_1) ];

        /* handle FORCE_SYSTEMID_UPDATE option */
        force_systemid_update = no_case[ lexeme [ lit("FORCE_SYSTEMID_UPDATE") ] ]
          [ boost::bind(&CatalogDescr::setForceSystemIDUpdate, &cmd, true) ];

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

        start.name("command");
        cmd_apply.name("APPLY");
        cmd_apply_retention.name("RETENTION POLICY");
        cmd_create.name("CREATE");
        cmd_drop.name("DROP");
        cmd_list.name("LIST");
        cmd_alter.name("ALTER");
        cmd_start_command.name("START");
        cmd_start_launcher.name("LAUNCHER");
        cmd_start_streaming.name("STREAMING");
        cmd_start_recovery_stream.name("START RECOVERY STREAM FOR ARCHIVE");
        cmd_stop_command.name("STOP");
        cmd_stop_streaming.name("STREAMING FOR ARCHIVE");
        cmd_show.name("SHOW");
        cmd_set.name("SET");
        cmd_set_variable.name("VARIABLE");
        cmd_reset.name("RESET");
        cmd_create_archive.name("CREATE ARCHIVE");
        cmd_create_backup_profile.name("CREATE BACKUP PROFILE");
        cmd_create_connection.name("CREATE STREAMING CONNECTION");
        cmd_create_retention.name("CREATE RETENTION POLICY");
        cmd_verify_archive.name("VERIFY ARCHIVE");
        cmd_drop_archive.name("DROP ARCHIVE");
        cmd_drop_backup_profile.name("DROP BACKUP_PROFILE");
        cmd_drop_connection.name("DROP STREAMING CONNECTION");
        cmd_drop_retention.name("DROP RETENTION POLICY");
        cmd_alter_archive.name("ALTER ARCHIVE");
        cmd_alter_archive_opt.name("ALTER ARCHIVE options");
        cmd_start_basebackup.name("START BASEBACKUP");
        cmd_list_archive.name("LIST ARCHIVE");
        cmd_list_backup.name("LIST BASEBACKUPS");
        cmd_list_backup_list.name("LIST BACKUPS");
        cmd_list_connection.name("LIST CONNECTION");
        cmd_list_retention.name("LIST RETENTION");
        retention_keep_action.name("KEEP");
        retention_drop_action.name("DROP");
        retention_rule_older_datetime.name("OLDER THAN");
        retention_rule_older_datetime.name("NEWER THAN");
        retention_datetime_spec.name("[nnn YEARS] [nn MONTHS] [nnn DAYS] [nn HOURS] [nn MINUTES]");
        retention_rule_num_basebackups.name("nnn");
        retention_cleanup_basebackups.name("CLEANUP");
        identifier.name("object identifier");
        executable.name("executable name");
        hostname.name("ip or hostname");
        profile_compression_option.name("COMPRESSION=GZIP|NONE");
        profile_max_rate_option.name("MAX_RATE=maximum transfer rate in KB/s");
        profile_wal_option.name("WAL=INCLUDED|EXCLUDED");
        profile_backup_label_option.name("LABEL=label string");
        profile_checkpoint_option.name("CHECKPOINT=FAST|DELAYED");
        profile_wait_for_wal_option.name("WAIT_FOR_WAL=TRUE|FALSE");
        show_command_type.name("WORKERS");
        database.name("database identifier");
        username.name("username identifier");
        portnumber.name("port number");
        directory_string.name("directory path");
        directory.name("DIRECTORY=path");
        dsn_connection_string.name("DSN=connection parameters key value pair");
        backup_profile_opts.name("backup profile parameters");
        with_profile.name("backup profile name");
        verify_check_connection.name("CONNECTION");
        profile_noverify_checksums_option.name("NOVERIFY");
        retention_rule_with_label.name("WITH LABEL");
        regexp_expression.name("<regular expression>");
        force_systemid_update.name("FORCE_SYSTEMID_UPDATE");
        variable_name.name("<variable name>");
        variable_value.name("<variable value>");
        cmd_drop_basebackup.name("BASEBACKUP");
        ip_address.name("<IP>");
        ip_address_list.name("(<IP>[, <IP>, ...])");
        ip_address_item.name(", <IP>");
        stream_listen_on.name("LISTEN_ON");
      }

      /*
       * Rule return declarations.
       */
      qi::rule<Iterator, ascii::space_type> start;
      qi::rule<Iterator, ascii::space_type> cmd_create, cmd_drop, cmd_list, cmd_alter;
      qi::rule<Iterator, ascii::space_type> cmd_create_archive,
                          cmd_verify_archive,
                          cmd_drop_archive,
                          cmd_drop_connection,
                          cmd_alter_archive,
                          cmd_start_command,
                          cmd_stop_command,
                          cmd_alter_archive_opt,
                          cmd_start_basebackup,
                          cmd_start_launcher,
                          cmd_start_streaming,
                          cmd_start_recovery_stream,
                          cmd_stop_streaming,
                          cmd_list_archive,
                          cmd_list_connection,
                          cmd_create_backup_profile,
                          cmd_list_backup,
                          cmd_list_retention,
                          cmd_pin_basebackup,
                          cmd_unpin_basebackup,
                          cmd_list_backup_list,
                          cmd_drop_backup_profile,
                          cmd_drop_retention,
                          cmd_drop_basebackup,
                          cmd_alter_backup_profile,
                          cmd_create_connection,
                          cmd_create_retention,
                          cmd_show,
                          cmd_set,
                          cmd_set_variable,
                          cmd_reset,
                          cmd_apply,
                          cmd_apply_retention,
                          verify_check_connection,
                          show_command_type,
                          profile_noverify_checksums_option,
                          backup_profile_opts,
                          retention_keep_action,
                          retention_drop_action,
                          retention_rule_older_datetime,
                          retention_rule_newer_datetime,
                          retention_datetime_spec,
                          retention_cleanup_basebackups,
                          force_systemid_update,
                          stream_listen_on,
                          ip_address_list,
                          ip_address_item;

      qi::rule<Iterator, std::string(), ascii::space_type> identifier;
      qi::rule<Iterator, std::string(), ascii::space_type> hostname,
                          database,
                          directory,
                          dsn_connection_string,
                          username,
                          portnumber,
                          profile_wal_option,
                          profile_wait_for_wal_option,
                          profile_checkpoint_option,
                          profile_max_rate_option,
                          profile_compression_option,
                          profile_backup_label_option,
                          with_profile,
                          executable,
                          retention_rule_with_label,
                          retention_rule_num_basebackups,
                          regexp_expression,
                          ip_address;
      qi::rule<Iterator, std::string(), ascii::space_type> property_string,
                          directory_string,
                          number_ID,
                          variable_name,
                          variable_value,
                          variable_value_string;
      qi::rule<Iterator, std::string(), ascii::space_type> retention_datetime_years,
                          retention_datetime_months,
                          retention_datetime_days,
                          retention_datetime_hours,
                          retention_datetime_minutes;

    };

  }
}

PGBackupCtlCommand::PGBackupCtlCommand(CatalogTag tag) {
  this->catalogDescr = make_shared<CatalogDescr>();
  this->catalogDescr->tag = tag;
}

PGBackupCtlCommand::PGBackupCtlCommand(CatalogDescr descr) {
  this->catalogDescr = std::make_shared<CatalogDescr>(descr);
}

PGBackupCtlCommand::~PGBackupCtlCommand() {}

std::string PGBackupCtlCommand::archive_name() {

  if (this->catalogDescr != nullptr) {

    if (this->catalogDescr->tag != EMPTY_DESCR)
      return this->catalogDescr->archive_name;

  }

  return "";

}

CatalogTag PGBackupCtlCommand::getCommandTag() {

  if (this->catalogDescr != nullptr)
    return this->catalogDescr->tag;
  else
    return EMPTY_DESCR;

}

void PGBackupCtlCommand::setWorkerID(int worker_id) {

  this->worker_id = worker_id;

}

void PGBackupCtlCommand::assignSigStopHandler(JobSignalHandler *handler) {

  if (handler == nullptr)
    throw CPGBackupCtlFailure("attempt to assign uninitialized signal handler");

  this->stopHandler = handler;
}

void PGBackupCtlCommand::assignSigIntHandler(JobSignalHandler *handler) {

  if (handler == nullptr)
    throw CPGBackupCtlFailure("attempt to assign uninitialized signal handler");

  this->intHandler = handler;
}

CatalogTag PGBackupCtlCommand::execute(std::string catalogDir) {

  shared_ptr<CatalogDescr> descr = nullptr;
  CatalogTag result = EMPTY_DESCR;

  /*
   * We only accept a CatalogDescr
   * which can be transformed into an
   * executable descriptor.
   */
  if (this->catalogDescr->tag == EMPTY_DESCR) {
    throw CPGBackupCtlFailure("catalog descriptor is not executable");
  }

  /*
   * First at all we need to create a catalog descriptor
   * which will then support initializing the backup catalog.
   */
  descr = this->getExecutableDescr();

  if (descr == nullptr) {
    throw CPGBackupCtlFailure("cannot execute uninitialized descriptor handle");
  }

  result = descr->tag;

  /*
   * Now establish the catalog instance.
   */
  shared_ptr<BackupCatalog> catalog
    = make_shared<BackupCatalog>(catalogDir);

  try {

    BaseCatalogCommand *execCmd;

    /*
     * Must cast to derived class.
     */
    execCmd = dynamic_cast<BaseCatalogCommand*>(descr.get());

    /*
     * Assign signal handlers, if present.
     */
    if (this->stopHandler != nullptr)
      execCmd->assignSigStopHandler(this->stopHandler);

    if (this->intHandler != nullptr)
      execCmd->assignSigIntHandler(this->intHandler);

    /* Also assign runtime configuration */
    execCmd->assignRuntimeConfiguration(this->runtime_config);

    execCmd->setWorkerID(worker_id);
    execCmd->setCatalog(catalog);
    execCmd->execute(false);

    /*
     * And we're done...
     */
    catalog->close();

  } catch (exception &e) {
    /*
     * Don't suppress any exceptions from here, but
     * make sure, we close the catalog safely.
     */
    if (catalog->available())
      catalog->close();
    throw CCatalogIssue(e.what());
  }

  return result;
}

shared_ptr<CatalogDescr> PGBackupCtlCommand::getExecutableDescr() {

  /*
   * Based on the assigned catalog descriptor we
   * form an executable descriptor with all derived properties.
   */
  shared_ptr<BaseCatalogCommand> result(nullptr);

  switch(this->catalogDescr->tag) {

  case CREATE_ARCHIVE: {
    result = make_shared<CreateArchiveCatalogCommand>(this->catalogDescr);
    break;
  }

  case DROP_ARCHIVE: {
    result = make_shared<DropArchiveCatalogCommand>(this->catalogDescr);
    break;
  }

  case ALTER_ARCHIVE: {
    result = make_shared<AlterArchiveCatalogCommand>(this->catalogDescr);
    break;
  }

  case VERIFY_ARCHIVE: {
    result = make_shared<VerifyArchiveCatalogCommand>(this->catalogDescr);
    break;
  }

  case START_BASEBACKUP: {
    result = make_shared<StartBasebackupCatalogCommand>(this->catalogDescr);
    break;
  }

  case START_LAUNCHER: {
    result = make_shared<StartLauncherCatalogCommand>(this->catalogDescr);
    break;
  }

  case LIST_BACKUP_CATALOG: {
    result = make_shared<ListBackupCatalogCommand>(this->catalogDescr);
    break;
  }

  case LIST_BACKUP_LIST: {
    result = make_shared<ListBackupListCommand>(this->catalogDescr);
    break;
  }

  case LIST_ARCHIVE: {

    shared_ptr<ListArchiveCatalogCommand> listCmd = std::make_shared<ListArchiveCatalogCommand>(this->catalogDescr);

    /*
     * If an archive name is specified, we request
     * a detail view of this archive.
     */
    if (this->catalogDescr->archive_name != "") {
      listCmd->setOutputMode(ARCHIVE_DETAIL_LIST);
    } else {
      listCmd->setOutputMode(ARCHIVE_LIST);
    }

    result = listCmd;
    break;
  }

  case CREATE_BACKUP_PROFILE: {
    result = make_shared<CreateBackupProfileCatalogCommand>(this->catalogDescr);
    break;
  }

  case LIST_BACKUP_PROFILE:
  case LIST_BACKUP_PROFILE_DETAIL:
    result = make_shared<ListBackupProfileCatalogCommand>(this->catalogDescr);
    break;

  case DROP_BACKUP_PROFILE:
    result = make_shared<DropBackupProfileCatalogCommand>(this->catalogDescr);
    break;

  case CREATE_CONNECTION:
    result = make_shared<CreateConnectionCatalogCommand>(this->catalogDescr);
    break;

  case LIST_CONNECTION:
    result = make_shared<ListConnectionCatalogCommand>(this->catalogDescr);
    break;

  case DROP_CONNECTION:
    result = make_shared<DropConnectionCatalogCommand>(this->catalogDescr);
    break;

  case START_STREAMING_FOR_ARCHIVE:
    result = make_shared<StartStreamingForArchiveCommand>(this->catalogDescr);
    break;

  case STOP_STREAMING_FOR_ARCHIVE:
    result = make_shared<StopStreamingForArchiveCommandHandle>(this->catalogDescr);
    break;

  case EXEC_COMMAND:
    result = make_shared<ExecCommandCatalogCommand>(this->catalogDescr);
    break;

  case SHOW_WORKERS:
    result = make_shared<ShowWorkersCommandHandle>(this->catalogDescr);
    break;

  case SHOW_VARIABLES:
    result = make_shared<ShowVariablesCatalogCommand>(this->catalogDescr);
    break;

  case SHOW_VARIABLE:
    result = make_shared<ShowVariableCatalogCommand>(this->catalogDescr);
    break;

  case PIN_BASEBACKUP:
  case UNPIN_BASEBACKUP:
    result = make_shared<PinCatalogCommand>(this->catalogDescr);
    break;

  case CREATE_RETENTION_POLICY:
    result = make_shared<CreateRetentionPolicyCommand>(this->catalogDescr);
    break;

  case LIST_RETENTION_POLICIES:
    result = make_shared<ListRetentionPoliciesCommand>(this->catalogDescr);
    break;

  case LIST_RETENTION_POLICY:
    result = make_shared<ListRetentionPolicyCommand>(this->catalogDescr);
    break;

  case DROP_RETENTION_POLICY:
    result = make_shared<DropRetentionPolicyCommand>(this->catalogDescr);
    break;

  case APPLY_RETENTION_POLICY:
    result = make_shared<ApplyRetentionPolicyCommand>(this->catalogDescr);
    break;

  case SET_VARIABLE:
    result = make_shared<SetVariableCatalogCommand>(this->catalogDescr);
    break;

  case RESET_VARIABLE:
    result = make_shared<ResetVariableCatalogCommand>(this->catalogDescr);
    break;

  case DROP_BASEBACKUP:
    result = make_shared<DropBasebackupCatalogCommand>(this->catalogDescr);
    break;

  case START_RECOVERY_STREAM_FOR_ARCHIVE:
    result = make_shared<StartRecoveryArchiveCommand>(this->catalogDescr);
    break;

  default:
    /* no-op, but we return nullptr ! */
    break;
  }


  return result;
}

PGBackupCtlParser::PGBackupCtlParser() {

  /* Create a dummy runtime environment here. */
  //this->command = make_shared<PGBackupCtlCommand>(EMPTY_DESCR);
  this->runtime_config = RuntimeVariableEnvironment::createRuntimeConfiguration();
  //this->command->assignRuntimeConfiguration(this->runtime_config);

}

PGBackupCtlParser::PGBackupCtlParser(path sourceFile,
                                     shared_ptr<RuntimeConfiguration> rtc)
  : PGBackupCtlParser(rtc) {

  this->sourceFile = sourceFile;
  //this->command = make_shared<PGBackupCtlCommand>(EMPTY_DESCR);
  //this->command->assignRuntimeConfiguration(this->runtime_config);

}

PGBackupCtlParser::PGBackupCtlParser(path sourceFile) {

  this->sourceFile = sourceFile;
  //this->command = make_shared<PGBackupCtlCommand>(EMPTY_DESCR);
  this->runtime_config = RuntimeVariableEnvironment::createRuntimeConfiguration();
  //this->command->assignRuntimeConfiguration(this->runtime_config);

}

PGBackupCtlParser::PGBackupCtlParser(shared_ptr<RuntimeConfiguration> rtc)
  : RuntimeVariableEnvironment(rtc) {

  // this->command = make_shared<PGBackupCtlCommand>(EMPTY_DESCR);
  // this->command->assignRuntimeConfiguration(this->runtime_config);

}

PGBackupCtlParser::~PGBackupCtlParser() {}

shared_ptr<PGBackupCtlCommand> PGBackupCtlParser::getCommand() {
  return this->command;
}

void PGBackupCtlParser::parseLine(std::string in) {

  using boost::spirit::ascii::space;
  typedef std::string::iterator iterator_type;
  typedef credativ::boostparser::PGBackupCtlBoostParser<iterator_type> PGBackupCtlBoostParser;

  /*
   * establish internal boost parser instance.
   */
  PGBackupCtlBoostParser myparser(this->runtime_config);

  std::string::iterator iter = in.begin();
  std::string::iterator end  = in.end();

  bool parse_result = phrase_parse(iter, end, myparser, space);

  if (parse_result && iter == end) {

    CatalogDescr cmd = myparser.getCommand();
    this->command = make_shared<PGBackupCtlCommand>(cmd);
    this->command->assignRuntimeConfiguration(this->runtime_config);

  }
  else
    throw CParserIssue("aborted due to parser error");

}

void PGBackupCtlParser::parseFile() {

  std::ifstream fileHandle;
  std::stringstream fs;
  bool compressed = false;
  std::string line;
  std::ostringstream cmdStr;

  /*
   * Check state of the source file. Throws
   * an exception in case something is wrong.
   */
  status(this->sourceFile);

  /*
   * Use the internal openFile() method
   * from CPGBackupCtlBase.
   */
  this->openFile(fileHandle,
                 fs,
                 this->sourceFile,
                 &compressed);

  /*
   * Read input into a single command string:
   * The parser doesn't handle carriage returns et al.
   */
  while (std::getline(fs, line)) {
    if (cmdStr.tellp() > 0)
      cmdStr << " ";
    cmdStr << line;
  }

  cout << cmdStr.str() << endl;
  this->parseLine(cmdStr.str());
}
