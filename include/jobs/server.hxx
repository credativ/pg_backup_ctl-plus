#ifndef __HAVE_PGBCKCTL_SERVER__
#define __HAVE_PGBCKCTL_SERVER__

/*
 * NOTE:
 *
 * We stay here on our own without
 * any includes from pgbckctl-common, since that will
 * slurp in PostgreSQL def's which aren't compatible
 * with what boost::asio has in mind, thus the definitions
 * are kept private in server.cxx.
 *
 * (for example, the definitions of arpa/inet.h clashes
 * with the one from PG's server/port.h).
 *
 * XXX: I really think above should be fixed, since the
 *      extern declarations collide (C vs. C++ binding
 *      from inet_net_ntop())
 */

#include <recoverydescr.hxx>

namespace credativ {

  /* Forward class declarations */
  class PGBackupCtlStreamingServer;
  class PGProtoStreamingServer;

  /*
   * TCP server API exceptions
   */
  class TCPServerFailure : public std::exception {
  protected:

    std::string errstr;

  public:

    TCPServerFailure(const char *err) throw() : errstr() {
      errstr = err;
    }

    TCPServerFailure(std::string err) throw() : errstr() {
      errstr = err;
    }

    virtual ~TCPServerFailure() throw() {}

    const char *what() const throw() {
      return errstr.c_str();
    }

  };

  /**
   * Public implementation interface for
   * the pg_backup_ctl++ streaming server.
   */
  class StreamingServer {
  protected:
    std::shared_ptr<PGBackupCtlStreamingServer> instance = nullptr;
  public:

    StreamingServer(std::shared_ptr<RecoveryStreamDescr> streamDescr);
    virtual ~StreamingServer();

    virtual void run();

  };
}

#endif
