#include <rtconfig.hxx>

/*
 * Helper functions/callbacks for readline support.
 *
 * See tab_completion.cxx for details.
 */
char **keyword_completion(const char *input, int start, int end);

/*
 * Initializes readline machinery.
 */
void init_readline(std::string catalog_name,
                   std::shared_ptr<pgbckctl::RuntimeConfiguration> rtc);


/*
 * Resets readline machinery after command
 * completion.
 */
void step_readline();
