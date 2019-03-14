#ifndef __HAVE_PGBCKCTL_SERVER__
#define __HAVE_PGBCKCTL_SERVER__

#include <common.hxx>
#include <descr.hxx>

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
    int port;

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

  /**
   * pg_backup_ctl++ streaming server implementation.
   *
   * Based on boost::asio
   */
  class PGBackupCtlStreamingServer {
  private:
  protected:
  public:

    /*
     * C'tor
     */
    PGBackupCtlStreamingServer(RecoveryStreamDescr streamDescr);

    /*
     * D'tor
     */
    virtual ~PGBackupCtlStreamingServer();
  };

}

#endif
