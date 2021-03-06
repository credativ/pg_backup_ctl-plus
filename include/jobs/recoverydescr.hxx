#ifndef __HAVE_RECOVERYDESCR_HXX__
#define __HAVE_RECOVERYDESCR_HXX__

#include <boost/filesystem.hpp>
#include <string>
#include <vector>

namespace pgbckctl {

  /**
   * pg_backup_ctl SSL Context.
   */
  class StreamSSLContext {
    boost::filesystem::path srv_file;
    boost::filesystem::path client_file;
  };

  /**
   * Recovery Stream descriptor.
   */
  class RecoveryStreamDescr {
  public:

    static const int DEFAULT_RECOVERY_STREAM_PORT = 7432;

    /**
     * Port number to listen on.
     */
    unsigned int port = 7432;

    /**
     * List of IP addresses to listen on.
     */
    std::vector<std::string> listen_on;

    /**
     * Archive this descriptor is attached to, -1
     * means not initialized yet.
     */
    int archive_id = -1;

    /**
     * The worker id this recovery stream was registered to.
     */
    int worker_id = -1;

    /**
     * Catalog this streaming instance is attached to.
     * This usually means the path to the SQLite database.
     */
    std::string catalog_name = "";

    /**
     * Wether to use SSL certificates. Defaults to TRUE
     *
     * NOTE: SSL currently *not* implemented.
     */
    bool use_ssl = true;

    /**
     * Backup Stream SSL context
     */
    StreamSSLContext ssl_context;

    /**
     * Server version as a string.
     */
    std::string version;

  };
}

#endif
