#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/write.hpp>
#include <boost/array.hpp>
#include <boost/bind.hpp>
#include <iostream>

extern "C" {
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <iostream>
}

#include <bgrndroletype.hxx>
#include <pgsql-proto.hxx>
#include <server.hxx>

using namespace credativ;

/*
 * Import external global variable to tell
 * in which worker state this module was called.
 */
extern BackgroundJobType _pgbckctl_job_type;

/**
 * pg_backup_ctl++ streaming server implementation.
 *
 * Based on boost::asio, heavily on example
 *
 * https://www.boost.org/doc/libs/1_53_0/doc/html/boost_asio/example/fork/process_per_connection.cpp
 */
namespace credativ {

  namespace ba = boost::asio;
  namespace ip = boost::asio::ip;

  class PGBackupCtlStreamingServer {
  private:

    /*
     * Internal boost::asio handles.
     */
    ba::io_service *ios     = nullptr;
    ba::signal_set *sset    = nullptr;
    ip::tcp::acceptor *acpt = nullptr;
    ip::tcp::socket   *soc  = nullptr;

    /* XXX: should be replaced by std::array */
    boost::array<char, 1024> data_;

    void start_signal_wait() {
      this->sset->async_wait(boost::bind(&PGBackupCtlStreamingServer::handle_signal_wait,
                                         this));
    }

    void handle_signal_wait() {

      pid_t pid;

      /*
       * Only the parent process should check for this signal. We can determine
       * whether we are in the parent by checking if the acceptor is still open.
       */
      if (this->acpt->is_open()) {

        /* Reap completed child processes so that we don't end up with zombies. */
        int status = 0;
        while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {

#ifdef __DEBUG__
          std::cerr << "child with pid exited" << std::endl;
#endif

        }

        this->start_signal_wait();
      }
    }

    void start_accept() {
      this->acpt->async_accept(*(this->soc),
                               boost::bind(&PGBackupCtlStreamingServer::handle_accept,
                                           this,
                                           _1));
    }

    void handle_accept(const boost::system::error_code& ec) {
      if (!ec)
        {
          // Inform the io_service that we are about to fork. The io_service cleans
          // up any internal resources, such as threads, that may interfere with
          // forking.
          this->ios->notify_fork(boost::asio::io_service::fork_prepare);

          if (fork() == 0)
            {
              /*
               * This is a worker subchild, tell global worker state about
               * this to prevent cleanup if important worker resources on exit.
               */
              _pgbckctl_job_type = BACKGROUND_WORKER_CHILD;

              // Inform the io_service that the fork is finished and that this is the
              // child process. The io_service uses this opportunity to create any
              // internal file descriptors that must be private to the new process.
              this->ios->notify_fork(boost::asio::io_service::fork_child);

              // The child won't be accepting new connections, so we can close the
              // acceptor. It remains open in the parent.
              this->acpt->close();

              // The child process is not interested in processing the SIGCHLD signal.
              this->sset->cancel();

              start_read();
            }
          else
            {
              // Inform the io_service that the fork is finished (or failed) and that
              // this is the parent process. The io_service uses this opportunity to
              // recreate any internal resources that were cleaned up during
              // preparation for the fork.
              this->ios->notify_fork(boost::asio::io_service::fork_parent);

              this->soc->close();
              start_accept();
            }
        }
      else
        {
          std::cerr << "Accept error: " << ec.message() << std::endl;
          start_accept();
        }
    }

    void start_read() {
      this->soc->async_read_some(boost::asio::buffer(data_),
                                 boost::bind(&PGBackupCtlStreamingServer::handle_read,
                                             this, _1, _2));
    }

    void handle_read(const boost::system::error_code& ec, std::size_t length) {

      if(data_[0] == pgprotocol::AuthenticationMessageType)
        std::cerr << "PG PROTO message AUTH" << std::endl;

      if  (data_[0] == 'Q')
        exit(0);

      if (!ec)
        start_write(length);
    }

    void start_write(std::size_t length) {
      boost::asio::async_write(*(this->soc), boost::asio::buffer(data_, length),
                               boost::bind(&PGBackupCtlStreamingServer::handle_write, this, _1));
    }

    void handle_write(const boost::system::error_code& ec) {
      if (!ec)
        start_read();
    }

  protected:

  public:

    /*
     * C'tor
     */
    PGBackupCtlStreamingServer(std::shared_ptr<RecoveryStreamDescr> streamDescr);

    /*
     * D'tor
     */
    virtual ~PGBackupCtlStreamingServer();

    /*
     * Run the io service.
     */
    virtual void run();
  };

}

StreamingServer::StreamingServer(std::shared_ptr<RecoveryStreamDescr> streamDescr) {

  this->instance = std::make_shared<PGBackupCtlStreamingServer>(streamDescr);

}

void StreamingServer::run() {

  this->instance->run();

}

StreamingServer::~StreamingServer() {}

PGBackupCtlStreamingServer::PGBackupCtlStreamingServer(std::shared_ptr<RecoveryStreamDescr> streamDescr) {

  /*
   * We need a valid stream descriptor!
   */
  if (streamDescr == nullptr) {
    throw TCPServerFailure("could not initialized TCP server instance: invalid stream descriptor");
  }

  /*
   * Create io_service handler
   */
  this->ios = new ba::io_service();

  /*
   * Create signal set
   */
  this->sset = new ba::signal_set(*(this->ios), SIGCHLD);

  /*
   * Create acceptor handle
   */
  this->acpt = new ip::tcp::acceptor(*(this->ios),
                                     ip::tcp::endpoint(ip::tcp::v6(),
                                                       streamDescr->port));

  /*
   * Instantiate the socket
   */
  this->soc = new ip::tcp::socket(*(this->ios));

  start_signal_wait();
  start_accept();

}

PGBackupCtlStreamingServer::~PGBackupCtlStreamingServer() {

  if (this->soc != nullptr)
    delete this->soc;

  if (this->acpt != nullptr)
    delete this->acpt;

  if (this->sset != nullptr)
    delete this->sset;

  if (this->ios != nullptr)
    delete this->ios;

}


void PGBackupCtlStreamingServer::run() {

  this->ios->run();

}
