#ifndef __HAVE_RECOVERYDESCR_HXX__
#define __HAVE_RECOVERYDESCR_HXX__

#include <boost/filesystem.hpp>
#include <string>
#include <vector>

namespace credativ {

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

    /**
     * Port number to listen on.
     */
    unsigned int port;

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
     * Wether to use SSL certificates. Defaults to TRUE
     *
     * NOTE: non-SSL currently *not* implemented.
     */
    bool use_ssl = true;

    /**
     * Backup Stream SSL context
     */
    StreamSSLContext ssl_context;


  };
}

#endif
