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

#ifdef __DEBUG__
#include <iostream>
#endif

/* charakters that are breaking words into pieces */
#define WORD_BREAKS "\t\n@$><=;|&{() "

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
 * Completion token definition.
 */
typedef struct _completion_word completion_word;

typedef struct _completion_word {

  const char *name;
  CompletionWordType type;
  completion_word *next_completions;

} completion_word;

/******************************************************************************
 * Completion definitions.
 *****************************************************************************/

completion_word param_pgport_completion[] = { { "PGPORT", COMPL_END, NULL },
                                              { "", COMPL_EOL, NULL } };

completion_word param_pguser_completion[] = { { "PGUSER", COMPL_KEYWORD, param_pgport_completion },
                                              { "", COMPL_EOL, NULL } };

completion_word param_pgdatabase_completion[] = { { "PGDATABASE" , COMPL_KEYWORD, param_pguser_completion },
                                                  { "", COMPL_EOL, NULL } };

completion_word param_start_completion[] = { { "DSN", COMPL_END, NULL },
                                             { "PGHOST", COMPL_KEYWORD, param_pgdatabase_completion },
                                             { "", COMPL_EOL, NULL } };

completion_word create_archive_dir_completion[] = { { "DIRECTORY", COMPL_KEYWORD, param_start_completion },
                                                    { "", COMPL_EOL, NULL } };

completion_word create_archive_params_completion[] = { { "PARAMS", COMPL_KEYWORD, create_archive_dir_completion },
                                                       { "", COMPL_EOL, NULL } };

completion_word create_archive_ident_completion[] = { { "<identifier>", COMPL_IDENTIFIER, create_archive_params_completion },
                                                      { "", COMPL_EOL, NULL } /* marks end of list */ };

completion_word list_archive_ident_completion[] = { { "<identifier>", COMPL_IDENTIFIER, NULL },
                                                    { "", COMPL_EOL, NULL } };

completion_word list_connection_archive_completion[] = { { "ARCHIVE", COMPL_KEYWORD, list_archive_ident_completion },
                                                         { "", COMPL_EOL, NULL } };

completion_word list_connection_for_completion[] = { { "FOR", COMPL_KEYWORD, list_connection_archive_completion },
                                                     { "", COMPL_EOL, NULL } };

completion_word create_connection_completion[] = { { "CONNECTION", COMPL_KEYWORD, list_connection_for_completion},
                                                   { "", COMPL_EOL, NULL } };

/*
 * Forwarded declaration for param completion list of CREATE BACKUP PROFILE
 * Please note that the initialization of those completion tokens
 * are done during runtime in init_readline() !
 */
completion_word create_bck_prof_param_full[7] ;

completion_word create_bck_prof_wfw_setting[] = { { "TRUE", COMPL_KEYWORD, NULL },
                                                  { "FALSE", COMPL_KEYWORD, NULL },
                                                  { "", COMPL_EOL, NULL } };

completion_word create_bck_prof_chkpt_setting[] = { { "FAST", COMPL_KEYWORD, create_bck_prof_param_full + 5 },
                                                    { "DELAYED", COMPL_KEYWORD, create_bck_prof_param_full + 5 },
                                                    { "", COMPL_EOL, NULL } };

completion_word create_bck_prof_wal_setting[] = { { "INCLUDED", COMPL_KEYWORD, create_bck_prof_param_full + 4 },
                                                  { "EXCLUDED", COMPL_KEYWORD, create_bck_prof_param_full + 4 },
                                                  { "", COMPL_EOL, NULL } };

completion_word create_bck_prof_label_string[] = { { "<label string>", COMPL_IDENTIFIER, create_bck_prof_param_full + 3 },
                                                   { "", COMPL_EOL, NULL } };

completion_word create_bck_prof_max_rate[] = { { "<max rate in bytes>", COMPL_IDENTIFIER, create_bck_prof_param_full + 2 },
                                               { "", COMPL_EOL, NULL } };

completion_word create_bck_prof_compr_type[] = { { "GZIP", COMPL_KEYWORD, create_bck_prof_param_full + 1 },
                                                 { "NONE", COMPL_KEYWORD, create_bck_prof_param_full + 1 },
                                                 { "ZSTD", COMPL_KEYWORD, create_bck_prof_param_full + 1 },
                                                 { "PBZIP", COMPL_KEYWORD, create_bck_prof_param_full + 1 },
                                                 { "", COMPL_EOL, NULL } };

completion_word create_bck_prof_ident_completion[] = { { "<identifier>", COMPL_IDENTIFIER, create_bck_prof_param_full },
                                                       { "", COMPL_EOL, NULL } };

completion_word create_backup_profile_completion[] = { { "PROFILE", COMPL_KEYWORD, create_bck_prof_ident_completion },
                                                       { "", COMPL_EOL, NULL } };

completion_word create_completion[] = { { "ARCHIVE", COMPL_KEYWORD, create_archive_ident_completion },
                                        { "STREAMING", COMPL_KEYWORD, create_connection_completion },
                                        { "BACKUP", COMPL_KEYWORD, create_backup_profile_completion },
                                        { "", COMPL_EOL, NULL } /* marks end of list */ };

completion_word list_backup_completion[] = { { "PROFILE", COMPL_KEYWORD, list_archive_ident_completion },
                                             { "CATALOG", COMPL_KEYWORD, list_archive_ident_completion },
                                             { "", COMPL_EOL, NULL } };

completion_word list_completion[] = { { "ARCHIVE", COMPL_KEYWORD, list_archive_ident_completion },
                                      { "BACKUP", COMPL_KEYWORD, list_backup_completion },
                                      { "CONNECTION", COMPL_KEYWORD, list_connection_for_completion },
                                      { "", COMPL_EOL, NULL } /* marks end of list */ };

completion_word start_basebackup_profile_ident[] = { { "<identifier>", COMPL_IDENTIFIER, NULL },
                                                     { "", COMPL_EOL, NULL } };

completion_word start_basebackup_profile[] = { { "PROFILE", COMPL_KEYWORD, start_basebackup_profile_ident },
                                               { "", COMPL_EOL, NULL } };

completion_word start_basebackup_ident_completion[] = { { "<identifier>", COMPL_IDENTIFIER, start_basebackup_profile },
                                                        { "", COMPL_EOL, NULL } };

completion_word start_basebackup_archive[] = { { "ARCHIVE", COMPL_KEYWORD, start_basebackup_ident_completion },
                                               { "", COMPL_EOL, NULL } };

completion_word start_basebackup_for[] = { { "FOR", COMPL_KEYWORD, start_basebackup_archive },
                                           { "", COMPL_EOL, NULL } };

completion_word start_streaming_option_detach[] = { { "NODETACH", COMPL_KEYWORD, NULL },
                                                     { "", COMPL_EOL, NULL } };

completion_word start_streaming_option[] = { { "RESTART", COMPL_KEYWORD, start_streaming_option_detach },
                                             { "NODETACH", COMPL_KEYWORD, NULL },
                                             { "", COMPL_EOL, NULL } };

completion_word start_streaming_ident_completion[] = { { "<identifier>", COMPL_IDENTIFIER, start_streaming_option },
                                                        { "", COMPL_EOL, NULL } };

completion_word start_streaming_archive[] = { { "ARCHIVE", COMPL_KEYWORD, start_streaming_ident_completion },
                                               { "", COMPL_EOL, NULL } };

completion_word start_streaming_for[] = { { "FOR", COMPL_KEYWORD, start_streaming_archive },
                                          { "", COMPL_EOL, NULL } };

completion_word start_completion[] = { { "BASEBACKUP", COMPL_KEYWORD, start_basebackup_for },
                                       { "STREAMING", COMPL_KEYWORD, start_streaming_for },
                                       { "", COMPL_EOL, NULL } };

completion_word verify_archive_completion[] = { { "ARCHIVE", COMPL_KEYWORD, list_archive_ident_completion },
                                                { "", COMPL_EOL, NULL } };

completion_word drop_connection_archive_completion[] = { { "ARCHIVE", COMPL_KEYWORD, list_archive_ident_completion },
                                                         { "", COMPL_EOL, NULL } };

completion_word drop_connection_from_completion[] = { { "FROM", COMPL_KEYWORD, drop_connection_archive_completion },
                                                    { "", COMPL_EOL, NULL } };

completion_word drop_connection_completion[] = { { "CONNECTION", COMPL_KEYWORD, drop_connection_from_completion },
                                                 { "", COMPL_EOL, NULL } };

completion_word drop_profile_ident_completion[] = { { "<identifier>", COMPL_IDENTIFIER, NULL },
                                                    { "", COMPL_EOL, NULL } };

completion_word drop_profile_completion[] = { { "PROFILE", COMPL_KEYWORD, drop_profile_ident_completion },
                                              { "", COMPL_EOL, NULL } };

completion_word drop_completion[] = { { "ARCHIVE", COMPL_KEYWORD, list_archive_ident_completion },
                                      { "STREAMING", COMPL_KEYWORD, drop_connection_completion },
                                      { "BACKUP", COMPL_KEYWORD, drop_profile_completion },
                                      { "", COMPL_EOL, NULL } };

completion_word alter_archive_set_completion[] = { { "SET", COMPL_KEYWORD, param_start_completion },
                                                   { "", COMPL_EOL, NULL } };

completion_word alter_archive_ident_completion[] = { { "<identifier>", COMPL_IDENTIFIER, alter_archive_set_completion },
                                                     { "", COMPL_EOL, NULL } };

completion_word alter_completion[] = { { "ARCHIVE", COMPL_KEYWORD, alter_archive_ident_completion },
                                       { "", COMPL_EOL, NULL } };

completion_word show_completion[] = { { "WORKERS", COMPL_KEYWORD, NULL },
                                      { "", COMPL_EOL, NULL } };

completion_word start_keyword[] = { { "CREATE", COMPL_KEYWORD, create_completion },
                                    { "START", COMPL_KEYWORD, start_completion },
                                    { "LIST", COMPL_KEYWORD, list_completion },
                                    { "VERIFY", COMPL_KEYWORD, verify_archive_completion },
                                    { "DROP", COMPL_KEYWORD, drop_completion },
                                    { "ALTER", COMPL_KEYWORD, alter_completion },
                                    { "SHOW", COMPL_KEYWORD, show_completion },
                                    { "", COMPL_EOL, NULL } /* marks end of list */ };

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
    completion_word *candidates;

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

      candidates = completed_keywords.back().next_completions;

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
  boost::split(previous_words, current_input_buf, boost::is_any_of(WORD_BREAKS));

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

void init_readline() {

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
  completion_word create_bck_prof_w0 = { "COMPRESSION", COMPL_KEYWORD, create_bck_prof_compr_type };
  completion_word create_bck_prof_w1 = { "MAX_RATE", COMPL_KEYWORD, create_bck_prof_max_rate };
  completion_word create_bck_prof_w2 = { "LABEL", COMPL_KEYWORD, create_bck_prof_label_string };
  completion_word create_bck_prof_w3 = { "WAL", COMPL_KEYWORD, create_bck_prof_wal_setting };
  completion_word create_bck_prof_w4 = { "CHECKPOINT", COMPL_KEYWORD, create_bck_prof_chkpt_setting};
  completion_word create_bck_prof_w5 = { "WAIT_FOR_WAL", COMPL_KEYWORD, create_bck_prof_wfw_setting };
  completion_word create_bck_prof_w6 = { "", COMPL_EOL, NULL } ;

  create_bck_prof_param_full[0] = create_bck_prof_w0;
  create_bck_prof_param_full[1] = create_bck_prof_w1;
  create_bck_prof_param_full[2] = create_bck_prof_w2;
  create_bck_prof_param_full[3] = create_bck_prof_w3;
  create_bck_prof_param_full[4] = create_bck_prof_w4;
  create_bck_prof_param_full[5] = create_bck_prof_w5;
  create_bck_prof_param_full[6] = create_bck_prof_w6;

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
    name = lookup_table[(*index)++].name;

    if (strncasecmp(name, input, len) == 0) {
      return strdup(name);
    }

  }

  /* only in case no match found */
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

    result = _evaluate_keyword(completed_keywords.back().next_completions,
                               input, &list_index, len);

  } else {

    result = _evaluate_keyword(start_keyword,
                               input, &list_index, len);

  }

  return result;
}
