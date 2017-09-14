/*
 * Helper functions/callbacks for readline support.
 *
 * See tab_completion.cxx for details.
 */
char **keyword_completion(const char *input, int start, int end);

/*
 * Initializes readline machinery.
 */
void init_readline();


/*
 * Resets readline machinery after command
 * completion.
 */
void step_readline();
