/*
 * Tab completion module for pg_backup_ctl++ interactive shell.
 *
 * NOTE: the mix of std::string C++ patterns and ordinary C
 *       string handling is a result of bad behavior
 *       of readline in conjunction with const char*. Thus,
 *       parts of this code which have to deal with readline
 *       rely heavily on C strings.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <vector>
#include <readline/readline.h>
#include <boost/algorithm/string.hpp>
/* required for string case insensitive comparison */
#include <boost/algorithm/string/predicate.hpp>

#include <BackupCatalog.hxx>
#include <rtconfig.hxx>

#ifdef __DEBUG__
#include <iostream>
#endif

/* charakters that are breaking words into pieces */
#define WORD_BREAKS "\t\n@$><=;|&{ "

/* Catalog handle used for queries to perform completion actions */
std::shared_ptr<BackupCatalog> compl_catalog_handle = nullptr;

/* A reference to the global runtime configuration.
 * In opposite to compl_catalog_handle above, this is *not*
 * initialized here! */
std::shared_ptr<RuntimeConfiguration> compl_runtime_cfg = nullptr;

/*
 * Completion tags, used to identify completion token type.
 */
typedef enum {

              COMPL_KEYWORD,
              COMPL_IDENTIFIER,
              COMPL_END,
              COMPL_EOL

} CompletionWordType;

/*
 * Completion action, callback, array et al.
 */
typedef enum {

              COMPL_STATIC_ARRAY,
              COMPL_FUNC_SQL

} CompletionAction;

/*
 * Completion token definition.
 */
typedef struct _completion_word completion_word;

/* Completion list constructor callback */
typedef completion_word* (* completion_callback)(completion_word *&,
                                                 completion_word *);

typedef struct _completion_word {

  std::string name;          /* completion string passed to readline. */
  CompletionWordType type;   /* type of this completion word */
  CompletionAction action;   /* action, either a static array or completion callback */

  completion_word *next_completions; /* Might be NULL */

  completion_callback cb;              /* Might be NULL */

} completion_word;

/******************************************************************************
 * Completion calback functions for catalog objects completion.
 *****************************************************************************/

completion_word *compl_identifier(std::string sql,
                                  completion_word *& compl_list,
                                  completion_word *next_compl) {

  std::shared_ptr<std::vector<std::string>> vlist;
  completion_word item;

  /* NOTE: can throw! */
  compl_catalog_handle->open_ro();
  vlist = compl_catalog_handle->SQL(sql);
  compl_catalog_handle->close();

  if (vlist->size() > 0) {

    int counter = 0;

    compl_list = new completion_word[vlist->size() + 1];

    for(auto &string_item : *(vlist.get())) {

      item.name = string_item;
      item.type = COMPL_IDENTIFIER;
      item.action = COMPL_STATIC_ARRAY;
      item.next_completions = next_compl;
      item.cb = NULL;

      compl_list[counter] = item;
      counter++;

    }

    /*
     * Last element indicates end of completion list.
     */
    compl_list[vlist->size()].name = "";
    compl_list[vlist->size()].type = COMPL_EOL;
    compl_list[vlist->size()].action = COMPL_STATIC_ARRAY;
    compl_list[vlist->size()].next_completions = NULL;
    compl_list[vlist->size()].cb = NULL;

    return compl_list;

  } else {

    /*
     * We *must* add a COMPL_EOL item to indicate
     * the end of the list!
     */
    compl_list = new completion_word[1];

    item.name = "";
    item.type = COMPL_EOL;
    item.action = COMPL_STATIC_ARRAY;
    item.next_completions = NULL;
    item.cb = NULL;

    compl_list[0] = item;
    return compl_list;

  }

  return compl_list;

}

/**
 * Completion list for runtime variables.
 */
completion_word *compl_variable(completion_word *& compl_list,
                                completion_word *next_compl) {

  int counter = 0;

  /*
   * In case runtime is not properly initialized, just
   * return an empty completion list (with a dummy
   * COMPL_EOL of course).
   */
  if (compl_runtime_cfg == nullptr) {

    compl_list = new completion_word[1];

    compl_list[0].name = "";
    compl_list[0].type = COMPL_EOL;
    compl_list[0].action = COMPL_STATIC_ARRAY;
    compl_list[0].next_completions = NULL;
    compl_list[0].cb = NULL;

    return compl_list;

  }

  /*
   * The same if the list is empty
   */
  if (compl_runtime_cfg->count_variables() == 0) {

    compl_list = new completion_word[1];

    compl_list[0].name = "";
    compl_list[0].type = COMPL_EOL;
    compl_list[0].action = COMPL_STATIC_ARRAY;
    compl_list[0].next_completions = NULL;
    compl_list[0].cb = NULL;

    return compl_list;

  }

  compl_list = new completion_word[compl_runtime_cfg->count_variables() + 1];

  /*
   * Loop through the variables list.
   */
  auto it_start = compl_runtime_cfg->begin();
  auto it_end   = compl_runtime_cfg->end();

  for(; it_start != it_end; ++it_start) {

    std::shared_ptr<credativ::ConfigVariable> var = it_start->second;

    compl_list[counter].name = var->getName();
    compl_list[counter].type = COMPL_IDENTIFIER;
    compl_list[counter].action = COMPL_STATIC_ARRAY;
    compl_list[counter].next_completions = next_compl;
    compl_list[counter].cb = NULL;

    counter++;

  }

  /*
   * Finally we need the end-of-list placeholder.
   */
  compl_list[counter].name = "";
  compl_list[counter].type = COMPL_EOL;
  compl_list[counter].action = COMPL_STATIC_ARRAY;
  compl_list[counter].next_completions = NULL;
  compl_list[counter].cb = NULL;

  return compl_list;
}

/**
 * Completion list of archive identifiers.
 */
completion_word *compl_archive_identifier(completion_word *& compl_list,
                                          completion_word *next_compl) {

  ostringstream sql;

  sql << "SELECT name FROM archive ORDER BY name ASC;";
  return compl_identifier(sql.str(), compl_list, next_compl);

}

/*
 * Completion list of retention identifiers.
 */
completion_word *compl_retention_identifier(completion_word *& compl_list,
                                            completion_word *next_compl) {

  ostringstream sql;

  sql << "SELECT name FROM retention ORDER BY name ASC;";
  return compl_identifier(sql.str(), compl_list, next_compl);

}

/******************************************************************************
 * Completion definitions.
 *****************************************************************************/

completion_word param_pgport_completion[] = { { "PGPORT", COMPL_END, COMPL_STATIC_ARRAY, NULL, NULL },
                                              { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word param_pguser_completion[] = { { "PGUSER",
                                                COMPL_KEYWORD, COMPL_STATIC_ARRAY, param_pgport_completion, NULL },
                                              { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word param_pgdatabase_completion[] = { { "PGDATABASE" ,
                                                    COMPL_KEYWORD, COMPL_STATIC_ARRAY, param_pguser_completion, NULL },
                                                  { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word param_start_completion[]
= { { "DSN", COMPL_END, COMPL_STATIC_ARRAY, NULL,  NULL },
    { "PGHOST", COMPL_KEYWORD, COMPL_STATIC_ARRAY, param_pgdatabase_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL,  NULL } };

completion_word create_archive_dir_completion[] = { { "DIRECTORY", COMPL_KEYWORD,
                                                      COMPL_STATIC_ARRAY, param_start_completion, NULL },
                                                    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_archive_params_completion[]
= { { "PARAMS", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_archive_dir_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_archive_ident_completion[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, create_archive_params_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } /* marks end of list */ };

completion_word list_archive_verbose_compl[]
= { { "VERBOSE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word list_archive_ident_completion[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, list_archive_verbose_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word list_connection_archive_completion[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_FUNC_SQL, NULL,
      (completion_callback) compl_archive_identifier },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word list_connection_for_completion[]
= { { "FOR", COMPL_KEYWORD, COMPL_STATIC_ARRAY, list_connection_archive_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_connection_completion[]
= { { "CONNECTION", COMPL_KEYWORD, COMPL_STATIC_ARRAY, list_connection_for_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

/*
 * Forwarded declaration for param completion list of CREATE BACKUP PROFILE
 * Please note that the initialization of those completion tokens
 * are done during runtime in init_readline() !
 */
completion_word create_bck_prof_param_full[8] ;

completion_word create_bck_prof_noverify_setting[]
= { { "TRUE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 7, NULL },
    { "FALSE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 7, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_bck_prof_wfw_setting[]
= { { "TRUE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 6, NULL },
    { "FALSE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 6, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_bck_prof_chkpt_setting[]
= { { "FAST", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 5, NULL },
    { "DELAYED", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 5, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_bck_prof_wal_setting[]
= { { "INCLUDED", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 4, NULL },
    { "EXCLUDED", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 4, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_bck_prof_label_string[]
= { { "<label string>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 3, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_bck_prof_max_rate[]
= { { "<max rate in bytes>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 2, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_bck_prof_compr_type[]
= { { "GZIP", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 1, NULL },
    { "NONE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 1, NULL },
    { "ZSTD", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 1, NULL },
    { "PBZIP", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 1, NULL },
    { "PLAIN", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_param_full + 1, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_bck_prof_ident_completion[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, create_bck_prof_param_full, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_backup_profile_completion[]
= { { "PROFILE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_ident_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word retention_regex_compl[]
= { { "<regular expression>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word retention_with_compl[]
= { { "LABEL", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_regex_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word retention_minutes_compl[]
= { { "MINUTES", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word retention_nn_minutes_compl[]
= { { "[0-9]*", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, retention_minutes_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word retention_hours_compl[]
= { { "HOURS", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_nn_minutes_compl, NULL },
    { "MINUTES", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word retention_nn_hours_compl[]
= { { "[0-9]*", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, retention_hours_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word retention_days_compl[]
= { { "DAYS", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_nn_hours_compl, NULL },
    { "HOURS", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_nn_minutes_compl, NULL },
    { "MINUTES", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word retention_nn_days_compl[]
= { { "[0-9]*", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, retention_days_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word retention_months_compl[]
= { { "MONTHS", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_nn_days_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word retention_nn_months_compl[]
= { { "[0-9]*", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, retention_months_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word retention_year_compl[]
= { { "YEARS", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_nn_months_compl, NULL },
    { "DAYS", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_nn_hours_compl, NULL },
    { "HOURS", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_nn_minutes_compl, NULL },
    { "MONTHS", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_nn_days_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word retention_nn_year_compl[]
= { { "[0-9]*", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, retention_year_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word retention_than_compl[]
= { { "THAN", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_nn_year_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word retention_rule_compl[]
= { { "WITH", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_with_compl, NULL },
    { "OLDER", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_than_compl, NULL },
    { "NEWER", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_than_compl, NULL },
    { "+<number of basebackups>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_retention_rule_compl[]
= { { "KEEP", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_rule_compl, NULL },
    { "DROP", COMPL_KEYWORD, COMPL_STATIC_ARRAY, retention_rule_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_retention_ident[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, create_retention_rule_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_retention_completion[]
= { { "POLICY", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_retention_ident, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word create_completion[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_archive_ident_completion, NULL  },
    { "STREAMING", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_connection_completion, NULL },
    { "BACKUP", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_backup_profile_completion, NULL },
    { "RETENTION", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_retention_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } /* marks end of list */ };

completion_word list_backup_completion[]
= { { "PROFILE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, list_archive_ident_completion, NULL },
    { "CATALOG", COMPL_KEYWORD, COMPL_STATIC_ARRAY, list_archive_ident_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word list_backup_verbose_compl[]
= { { "VERBOSE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word list_backup_archive_completion[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_FUNC_SQL, list_backup_verbose_compl,
      (completion_callback)compl_archive_identifier },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word list_backup_list_completion[]
= { { "IN", COMPL_KEYWORD, COMPL_STATIC_ARRAY, list_backup_archive_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word list_retention_completion[]
= { { "POLICIES", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "POLICY", COMPL_KEYWORD, COMPL_FUNC_SQL, NULL,
      (completion_callback) compl_retention_identifier },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word list_completion[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_FUNC_SQL, NULL,
      (completion_callback) (compl_archive_identifier) },
    { "BACKUP", COMPL_KEYWORD, COMPL_STATIC_ARRAY, list_backup_completion, NULL },
    { "BASEBACKUPS", COMPL_KEYWORD, COMPL_STATIC_ARRAY, list_backup_list_completion, NULL },
    { "CONNECTION", COMPL_KEYWORD, COMPL_STATIC_ARRAY, list_connection_for_completion, NULL },
    { "RETENTION", COMPL_KEYWORD, COMPL_STATIC_ARRAY, list_retention_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } /* marks end of list */ };

completion_word start_basebackup_opt_force_sysid_upd[]
= { { "FORCE_SYSTEMID_UPDATE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word start_basebackup_profile_ident[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, start_basebackup_opt_force_sysid_upd, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word start_basebackup_profile[]
= { { "PROFILE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, start_basebackup_profile_ident, NULL },
    { "FORCE_SYSTEMID_UPDATE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word start_basebackup_ident_completion[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, start_basebackup_profile, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word start_basebackup_archive[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, start_basebackup_ident_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word start_basebackup_for[]
= { { "FOR", COMPL_KEYWORD, COMPL_STATIC_ARRAY, start_basebackup_archive, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word start_streaming_option_detach[]
= { { "NODETACH", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word start_streaming_option[]
= { { "RESTART", COMPL_KEYWORD, COMPL_STATIC_ARRAY, start_streaming_option_detach, NULL },
    { "NODETACH", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word start_streaming_ident_completion[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, start_streaming_option, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word start_streaming_archive[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, start_streaming_ident_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word start_streaming_for[]
= { { "FOR", COMPL_KEYWORD, COMPL_STATIC_ARRAY, start_streaming_archive, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word recovery_stream_ip_list_end[]
= { { ")", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word recovery_stream_ip_address[]
= { { "<ip address>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, recovery_stream_ip_list_end, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word recovery_stream_ip_list_start[]
= { { "(", COMPL_KEYWORD, COMPL_STATIC_ARRAY, recovery_stream_ip_address, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word recovery_stream_listen_on[]
= { { "LISTEN_ON", COMPL_KEYWORD, COMPL_STATIC_ARRAY, recovery_stream_ip_list_start, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } } ;

completion_word recovery_stream_port[]
= { { "<port number>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, recovery_stream_listen_on, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word recovery_stream_listen_port[]
= { { "LISTEN_ON", COMPL_KEYWORD, COMPL_STATIC_ARRAY, recovery_stream_ip_list_start, NULL },
    { "PORT", COMPL_KEYWORD, COMPL_STATIC_ARRAY, recovery_stream_port, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word recovery_stream_for_archive[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_FUNC_SQL, recovery_stream_listen_port,
      (completion_callback) compl_archive_identifier },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word start_recovery_stream[]
= { { "FOR" , COMPL_KEYWORD, COMPL_STATIC_ARRAY, recovery_stream_for_archive, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word start_recovery_compl[]
= { { "STREAM" , COMPL_KEYWORD, COMPL_STATIC_ARRAY, start_recovery_stream, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word start_completion[]
= { { "BASEBACKUP", COMPL_KEYWORD, COMPL_STATIC_ARRAY, start_basebackup_for, NULL },
    { "STREAMING", COMPL_KEYWORD, COMPL_STATIC_ARRAY, start_streaming_for, NULL },
    { "LAUNCHER", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "RECOVERY", COMPL_KEYWORD, COMPL_STATIC_ARRAY, start_recovery_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word verify_archive_options[]
= { { "CONNECTION", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word verify_archive_ident_completion[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, verify_archive_options, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word verify_archive_completion[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, verify_archive_ident_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word drop_connection_archive_completion[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, list_archive_ident_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word drop_connection_from_completion[]
= { { "FROM", COMPL_KEYWORD, COMPL_STATIC_ARRAY, drop_connection_archive_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word drop_connection_completion[]
= { { "CONNECTION", COMPL_KEYWORD, COMPL_STATIC_ARRAY, drop_connection_from_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word drop_profile_ident_completion[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word drop_profile_completion[]
= { { "PROFILE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, drop_profile_ident_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word drop_basebackup_ident_compl[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word drop_basebackup_archive_compl[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, drop_basebackup_ident_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word drop_basebackup_from_compl[]
= { { "FROM", COMPL_KEYWORD, COMPL_STATIC_ARRAY, drop_basebackup_archive_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word drop_basebackup_completion[]
= { { "<ID>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, drop_basebackup_from_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word drop_retention_ident_compl[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word drop_retention_policy_compl[]
= { { "POLICY", COMPL_KEYWORD, COMPL_STATIC_ARRAY, drop_retention_ident_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word drop_completion[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, list_archive_ident_completion, NULL },
    { "STREAMING", COMPL_KEYWORD, COMPL_STATIC_ARRAY, drop_connection_completion, NULL },
    { "BACKUP", COMPL_KEYWORD, COMPL_STATIC_ARRAY, drop_profile_completion, NULL },
    { "BASEBACKUP", COMPL_KEYWORD, COMPL_STATIC_ARRAY, drop_basebackup_completion, NULL },
    { "RETENTION", COMPL_KEYWORD, COMPL_STATIC_ARRAY, drop_retention_policy_compl, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word alter_archive_set_completion[]
= { { "SET", COMPL_KEYWORD, COMPL_STATIC_ARRAY, param_start_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word alter_archive_ident_completion[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, alter_archive_set_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word alter_completion[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, alter_archive_ident_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word show_completion_variable[]
= { { "<variable>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word show_completion[]
= { { "WORKERS", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "VARIABLES", COMPL_KEYWORD, COMPL_STATIC_ARRAY, NULL, NULL },
    { "VARIABLE", COMPL_KEYWORD, COMPL_FUNC_SQL, NULL,
      (completion_callback) compl_variable },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word stop_completion_archive[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_FUNC_SQL, NULL,
      (completion_callback) compl_archive_identifier },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word stop_completion_streaming_for[]
= { { "FOR", COMPL_KEYWORD, COMPL_STATIC_ARRAY, stop_completion_archive, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word stop_completion[]
= { { "STREAMING" , COMPL_KEYWORD, COMPL_STATIC_ARRAY, stop_completion_streaming_for, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word pin_completion_ident[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word pin_completion_archive[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_FUNC_SQL, NULL,
      (completion_callback) compl_archive_identifier },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word pin_completion_in[]
= { { "IN", COMPL_KEYWORD, COMPL_STATIC_ARRAY, pin_completion_archive, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word unpin_completion[]
= { { "+", COMPL_KEYWORD, COMPL_STATIC_ARRAY, pin_completion_in, NULL },
    { "NEWEST", COMPL_KEYWORD, COMPL_STATIC_ARRAY, pin_completion_in, NULL },
    { "OLDEST", COMPL_KEYWORD, COMPL_STATIC_ARRAY, pin_completion_in, NULL },
    { "<BASEBACKUP ID>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, pin_completion_in, NULL },
    { "PINNED", COMPL_KEYWORD, COMPL_STATIC_ARRAY, pin_completion_in, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word pin_completion[]
= { { "+", COMPL_KEYWORD, COMPL_STATIC_ARRAY, pin_completion_in, NULL },
    { "NEWEST", COMPL_KEYWORD, COMPL_STATIC_ARRAY, pin_completion_in, NULL },
    { "OLDEST", COMPL_KEYWORD, COMPL_STATIC_ARRAY, pin_completion_in, NULL },
    { "<BASEBACKUP ID>", COMPL_KEYWORD, COMPL_STATIC_ARRAY, pin_completion_in, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word apply_retention_archive_name[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word apply_retention_archive[]
= { { "ARCHIVE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, apply_retention_archive_name, NULL } ,
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word apply_retention_to_archive[]
= { { "TO", COMPL_KEYWORD, COMPL_STATIC_ARRAY, apply_retention_archive, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word apply_retention_name[]
= { { "<identifier>", COMPL_IDENTIFIER, COMPL_STATIC_ARRAY, apply_retention_to_archive, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word apply_retention_completion[]
= { { "POLICY", COMPL_KEYWORD, COMPL_STATIC_ARRAY, apply_retention_name, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word apply_completion[]
= { { "RETENTION", COMPL_KEYWORD, COMPL_STATIC_ARRAY, apply_retention_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word set_variable_assignment[]
= { { "=", COMPL_END, COMPL_STATIC_ARRAY, NULL, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word set_completion[]
= { { "VARIABLE", COMPL_KEYWORD, COMPL_FUNC_SQL, set_variable_assignment,
      (completion_callback) compl_variable },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word reset_completion[]
= { { "VARIABLE", COMPL_KEYWORD, COMPL_FUNC_SQL, NULL,
      (completion_callback) compl_variable },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } };

completion_word start_keyword[]
= { { "CREATE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_completion, NULL },
    { "START", COMPL_KEYWORD, COMPL_STATIC_ARRAY, start_completion, NULL },
    { "LIST", COMPL_KEYWORD, COMPL_STATIC_ARRAY, list_completion, NULL },
    { "VERIFY", COMPL_KEYWORD, COMPL_STATIC_ARRAY, verify_archive_completion, NULL },
    { "DROP", COMPL_KEYWORD, COMPL_STATIC_ARRAY, drop_completion, NULL },
    { "ALTER", COMPL_KEYWORD, COMPL_STATIC_ARRAY, alter_completion, NULL },
    { "SHOW", COMPL_KEYWORD, COMPL_STATIC_ARRAY, show_completion, NULL },
    { "STOP", COMPL_KEYWORD, COMPL_STATIC_ARRAY, stop_completion, NULL },
    { "PIN", COMPL_KEYWORD, COMPL_STATIC_ARRAY, pin_completion, NULL },
    { "UNPIN", COMPL_KEYWORD, COMPL_STATIC_ARRAY, unpin_completion, NULL },
    { "APPLY", COMPL_KEYWORD, COMPL_STATIC_ARRAY, apply_completion, NULL },
    { "SET", COMPL_KEYWORD, COMPL_STATIC_ARRAY, set_completion, NULL },
    { "RESET", COMPL_KEYWORD, COMPL_STATIC_ARRAY, reset_completion, NULL },
    { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } /* marks end of list */ };

/* global buffer to hold completed input for readline callbacks */
std::vector<completion_word> completed_keywords = {};

/* Forwarded declarations */
char **keyword_completion(const char *input, int start, int end);
char *keyword_generator(const char *input, int state);

void recognize_previous_words(std::vector<std::string> previous_words) {

  std::string last_word;

  /*
   * if previous_word is empty, there's nothing to do.
   */
  if (previous_words.size() <= 0)
    return;

  /*
   * Loop through previous_words, examing
   * completion order and corresponding candidates.
   *
   * NOTE: previous_words also contains the last
   *       *uncompleted* input, so leave it out from
   *       examination.
   */
  for (unsigned int i = 0; i < previous_words.size() - 1; i++) {

    std::string current_word = previous_words[i];
    completion_word *candidates = NULL;
    CompletionAction completion_action = COMPL_STATIC_ARRAY;

    /* nothing to compare */
    if (current_word.length() <= 0)
      continue;

    /*
     * Iff the list of completed_keywords is empty, start
     * with the start_keyword array. Stop if we find a match.
     */
    if (completed_keywords.size() <= 0)
      candidates = start_keyword;
    else {

      completion_word cw = completed_keywords.back();

      if (cw.action == COMPL_STATIC_ARRAY) {

        candidates = cw.next_completions;

      } else if (cw.action == COMPL_FUNC_SQL) {

        cw.cb(candidates, cw.next_completions);

        /* We need to free the allocated array afterwards */
        completion_action = cw.action;

      }

    }

    if (candidates == NULL)
        break;

    for (int j = 0;;j++) {

      completion_word c_word;

      c_word = candidates[j];

      /* End of list ? */
      if (c_word.type == COMPL_EOL)
        break;

      /* No completion alternatives avail ? */
      if (c_word.type == COMPL_END)
        continue;

      if (boost::iequals(current_word, std::string(c_word.name))) {

        /* push recognized completion to vector */
        completed_keywords.push_back(c_word);

        /* inner loop done, next previous word */
        break;

      }

      /*
       * We didn't match a keyword, examine wether
       * we expect an identifier for completion.
       */
      if (c_word.type == COMPL_IDENTIFIER) {
        /* name content is not interesting, since it can
         * contain anything user defined */
        completed_keywords.push_back(c_word);

        break;
      }
    }

    if (completion_action == COMPL_FUNC_SQL)
      delete[] candidates;

  }

  return;

}

char **keyword_completion(const char *input, int start, int end) {

  std::string current_input_buf = rl_line_buffer;
  std::vector<std::string> previous_words = {};

  /*
   * Tell readline that we've finished and don't want
   * to fall back to default path completion
   */
  rl_attempted_completion_over = 1;

  /*
   * Break current input into completed words. Note that this
   * also contains the uncompleted last input. We also
   * replace the whole content of previous_words.
   */
  boost::split(previous_words,
               current_input_buf,
               boost::is_any_of(WORD_BREAKS));

  /*
   * Now recognize completed words. This is done from the start
   * or last production rule. We put this into the global
   * completed_words vector.
   *
   * NOTE: completed_keywords will contain only the last
   *       completed words, *NOT* the current one. That's
   *       why we force the previous_words vector
   *       having two elements at least!
   */
  completed_keywords.clear();

  if (previous_words.size() > 1) {
    recognize_previous_words(previous_words);
  }

  /*
   * Return matches.
   */
  return rl_completion_matches(input, keyword_generator);

}

void init_readline(std::string catalog_name,
                   std::shared_ptr<RuntimeConfiguration> rtc) {

  /*
   * Full completion tokens for parameter list for CREATE BACKUP PROFILE
   * Since we need to specify the parameters in a proper order, we
   * change the offset into this completion array to the right position
   * (see the detail in e.g. create_bck_prof_compr_type[]). Problem is, that
   * we just can't forward a static array initialization, so we initialize
   * the elements here!
   *
   * IMPORTANT: If you change the number of elements here, make sure you
   *            match them in the forward declaration above!
   */
  completion_word create_bck_prof_w0
    = { "COMPRESSION", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_compr_type, NULL };
  completion_word create_bck_prof_w1
    = { "MAX_RATE", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_max_rate, NULL };
  completion_word create_bck_prof_w2
    = { "LABEL", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_label_string, NULL };
  completion_word create_bck_prof_w3
    = { "WAL", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_wal_setting, NULL };
  completion_word create_bck_prof_w4
    = { "CHECKPOINT", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_chkpt_setting, NULL };
  completion_word create_bck_prof_w5
    = { "WAIT_FOR_WAL", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_wfw_setting, NULL };
  completion_word create_bck_prof_w6
    = { "NOVERIFY", COMPL_KEYWORD, COMPL_STATIC_ARRAY, create_bck_prof_noverify_setting, NULL };
  completion_word create_bck_prof_w7
    = { "", COMPL_EOL, COMPL_STATIC_ARRAY, NULL, NULL } ;

  create_bck_prof_param_full[0] = create_bck_prof_w0;
  create_bck_prof_param_full[1] = create_bck_prof_w1;
  create_bck_prof_param_full[2] = create_bck_prof_w2;
  create_bck_prof_param_full[3] = create_bck_prof_w3;
  create_bck_prof_param_full[4] = create_bck_prof_w4;
  create_bck_prof_param_full[5] = create_bck_prof_w5;
  create_bck_prof_param_full[6] = create_bck_prof_w6;
  create_bck_prof_param_full[7] = create_bck_prof_w7;

  /*
   * Initialize catalog handle for completion queries, iff
   * necessary.
   */
  if (compl_catalog_handle == nullptr) {
    compl_catalog_handle = std::make_shared<BackupCatalog>(catalog_name);
  }

  if (compl_runtime_cfg == nullptr) {
    compl_runtime_cfg = rtc;
  }

  /* XXX: what about specific append settings?
   *
   * rl_completion_append_character = '\0'
   * rl_completion_suppress_append = 1;
   */

  /*
   * Setup readline callbacks
   */
  rl_attempted_completion_function = keyword_completion;

  /*
   * Set word breaks.
   */
  rl_basic_word_break_characters = WORD_BREAKS;
}

void step_readline() {

  completed_keywords.clear();

}

static inline char *
_evaluate_keyword(completion_word *lookup_table,
                  const char *input,
                  int *index,
                  int len) {

  char * result = NULL;
  const char *name;

  /* nothing to do if lookup table is undefined */
  if (lookup_table == NULL)
    return result;

  /* Sanity check: same with index */
  if (index == NULL)
    return result;

  /*
   * Take care here, since we rely on index being
   * resettet to zero when entering this function. Index
   * is then incremented during the keyword lookup of the
   * specified lookup table.
   */
  while (lookup_table[(*index)].type != COMPL_EOL) {

    /* NOTE: lookup keyword, but also increment index for next round */
    name = lookup_table[(*index)++].name.c_str();

    if (strncasecmp(name, input, len) == 0) {
      return strdup(name);
    }

  }

  /* only in case no match found, should be NULL */
  return result;
}

char *
keyword_generator(const char *input, int state) {

  static int list_index, len;
  char *result = NULL;

  if (!state) {
    list_index = 0;
    len = strlen(input);
  }

  if (!completed_keywords.empty()) {

    completion_word cw = completed_keywords.back();
    completion_word *next;

    if (cw.action == COMPL_STATIC_ARRAY) {

      result = _evaluate_keyword(cw.next_completions,
                                 input, &list_index, len);

    } else if (cw.action == COMPL_FUNC_SQL) {

      /* call SQL completion callback */
      cw.cb(next, cw.next_completions);
      result = _evaluate_keyword(next,
                                 input, &list_index, len);

      /* _evaluate_keyword makes a duplicate of the returned
       * character string, so it should be safe to free it */
      delete[] next;

    }

  } else {

    result = _evaluate_keyword(start_keyword,
                               input, &list_index, len);

  }

  return result;

}
