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
#define WORD_BREAKS		"\t\n@$><=;|&{() "

std::string inputString("One!Two,Three:Four");
std::string delimiters("|,:");

typedef enum {
  COMPL_KEYWORD,
  COMPL_IDENTIFIER,
  COMPL_END
} CompletionWordType;

typedef struct _completion_word completion_word;

typedef struct _completion_word {

  const char *name;
  CompletionWordType type;
  completion_word *next_completions;

} completion_word;

completion_word param_pgport_completion[] = { { "PGPORT", COMPL_KEYWORD, NULL },
                                              { "", COMPL_END, NULL } };

completion_word param_pguser_completion[] = { { "PGUSER", COMPL_KEYWORD, param_pgport_completion },
                                              { "", COMPL_END, NULL } };

completion_word param_pgdatabase_completion[] = { { "PGDATABASE" , COMPL_KEYWORD, param_pguser_completion },
                                                  { "", COMPL_END, NULL } };

completion_word param_start_completion[] = { { "DSN", COMPL_END, NULL },
                                             { "PGHOST", COMPL_KEYWORD, param_pgdatabase_completion },
                                             { "", COMPL_END, NULL } };

completion_word create_archive_dir_completion[] = { { "DIRECTORY", COMPL_KEYWORD, param_start_completion },
                                                    { "", COMPL_END, NULL } };

completion_word create_archive_params_completion[] = { { "PARAMS", COMPL_KEYWORD, create_archive_dir_completion },
                                                       { "", COMPL_END, NULL } };

completion_word create_archive_ident_completion[] = { { "<identifier>", COMPL_IDENTIFIER, create_archive_params_completion },
                                                      { "", COMPL_END, NULL } /* marks end of list */ };

completion_word list_archive_ident_completion[] = { { "<identifier>", COMPL_END, NULL },
                                                    { "", COMPL_END, NULL } };

completion_word create_completion[] = { { "ARCHIVE", COMPL_KEYWORD, create_archive_ident_completion },
                                        { "STREAMING CONNECTION", COMPL_KEYWORD, NULL },
                                        { "BACKUP PROFILE", COMPL_KEYWORD, NULL },
                                        { "", COMPL_END, NULL } /* marks end of list */ };

completion_word list_completion[] = { { "ARCHIVE", COMPL_KEYWORD, list_archive_ident_completion },
                                      { "BACKUP PROFILE", COMPL_KEYWORD, NULL},
                                      { "CONNECTION", COMPL_KEYWORD, NULL },
                                      { "", COMPL_END, NULL } /* marks end of list */ };

completion_word start_keyword[] = { { "CREATE", COMPL_KEYWORD, create_completion },
                                    { "START", COMPL_KEYWORD, NULL },
                                    { "LIST", COMPL_KEYWORD, list_completion },
                                    { "VERIFY", COMPL_KEYWORD, NULL },
                                    { "DROP", COMPL_KEYWORD, NULL },
                                    { "", COMPL_END, NULL } /* marks end of list */ };

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
  else
    last_word = previous_words[ previous_words.size() - 2 ];

  /*
   * Look if we have already dispatched words in
   * the global completed words vector. If true,
   * take the production from the last one, if not, simply
   * dispatch from the start_keyword array.
   */
  if (completed_keywords.size() > 0) {

    /* previous word holds keyword list */
    completion_word c_word = completed_keywords.back();

    if ( (c_word.type != COMPL_END)
         || (c_word.next_completions != NULL) ) {

      for(int i = 0;;i++) {
        /* is this one a completed candidate? */
        completion_word candidate = c_word.next_completions[i];

        /*
         * Terminate, if we reach COMPL_END.
         */
        if (candidate.type == COMPL_END)
          break;

        if (boost::iequals(std::string(candidate.name), last_word)) {
          /*
           * Yeha, add it.
           */
          //          std::cout << "NEW COMPL " << candidate.name << std::endl;
          completed_keywords.push_back(candidate);
        }

        /*
         * If current production rules imply a user submitted
         * identifier, followed by post completion rules, treat them
         * special. Don't look at the string itself, it doesn't
         * contain anything interesting.
         */
        if (candidate.type == COMPL_IDENTIFIER) {
          completed_keywords.push_back(candidate);
        }
      }
    }

  } else {

    /*
     * No completed previous words already, start
     * over with the start_keywords list.
     */
    for(int i = 0;;i++) {
      /* first completion target */
      completion_word candidate = start_keyword[i];

      if ( (candidate.type == COMPL_END)
           || (candidate.next_completions == NULL) )
        break;

      if (boost::iequals(std::string(candidate.name), last_word)) {
        /*
         * Yeha, add it.
         */
        //        std::cout << "NEW COMPL " << candidate.name << std::endl;
        completed_keywords.push_back(candidate);
      }
    }

  }
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

  // for (auto &i : previous_words) {
  //   std::cout << " PREV " << i << std::endl;
  // }

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

  while ((name = lookup_table[(*index)++].name)) {

    if (lookup_table[(*index)].type == COMPL_IDENTIFIER) {
      return strdup(name);
    }

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

  if (!completed_keywords.empty()
      && (completed_keywords.size() >= 1) ) {

    result = _evaluate_keyword(completed_keywords.back().next_completions,
                               input, &list_index, len);

  } else {

          result = _evaluate_keyword(start_keyword,
                                     input, &list_index, len);

  }


  return result;
}
