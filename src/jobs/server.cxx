#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/array.hpp>
#include <boost/bind.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/log/trivial.hpp>
#include <iostream>
#include <map>

extern "C" {
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <iostream>
}

#include <common.hxx>
#include <rtconfig.hxx>
#include <bgrndroletype.hxx>
#include <pgsql-proto.hxx>
#include <proto-catalog.hxx>
#include <proto-buffer.hxx>
#include <pgproto-parser.hxx>
#include <pgproto-commands.hxx>
#include <shm.hxx>
#include <server.hxx>

#define SOCKET_P(ptr) (*((ptr)->soc))

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

  /*
   * Base class for streaming server implementation.
   */
  class PGBackupCtlStreamingServer {
  private:
  protected:

    /**
     * Runtime configuration private to this server
     * instance.
     */
    std::shared_ptr<RuntimeVariableEnvironment> server_env = std::make_shared<RuntimeVariableEnvironment>();

    /**
     * The worker child ID we are registered at.
     */
    int child_id = -1;

    /*
     * The parent worker ID we are belonging to.
     */
    int worker_id = -1;

    /**
     * Shared memory segment for background workers.
     *
     * We need this to reflect basebackup usage and statistics.
     */
    std::shared_ptr<WorkerSHM> worker_shm = std::make_shared<WorkerSHM>();

    /*
     * Internal boost::asio handles.
     */
    ba::io_service *ios     = nullptr;
    ba::signal_set *sset    = nullptr;
    ba::signal_set *sset_exit = nullptr;
    ip::tcp::acceptor *acpt = nullptr;
    ip::tcp::socket   *soc  = nullptr;

    /* XXX: should be replaced by std::array */
    boost::array<char, 1024> data_;

    /*
     * Recovery handle.
     */
    std::shared_ptr<RecoveryStreamDescr> streamDescr = nullptr;

    void start_signal_wait() {

      sset->async_wait(boost::bind(&PGBackupCtlStreamingServer::handle_signal_wait,
                                   this));
      sset_exit->async_wait(boost::bind(&boost::asio::io_service::stop, this->ios));

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

          if (worker_shm->get_shmid() > 0  && worker_id >= 0) {

            /*
             * XXX: This is tricky. We can't use mutex locks here in the
             *      signal handler, so do the deletion directly. This is not
             *      really safe. Consider employing a separate reaper thread!
             */
            worker_shm->free_child_by_pid(worker_id, pid);

          }

        }

        this->start_signal_wait();

      }

    }

    void start_accept() {
      this->acpt->async_accept(SOCKET_P(this),
                               boost::bind(&PGBackupCtlStreamingServer::handle_accept,
                                           this,
                                           _1));
    }

    void handle_accept(const boost::system::error_code& ec) {
      if (!ec)
        {
          /*
           * Inform the io_service that we are about to fork. The io_service cleans
           * up any internal resources, such as threads, that may interfere with
           * forking.
           */
          ios->notify_fork(boost::asio::io_service::fork_prepare);

          if (fork() == 0)
            {
              /*
               * This is a worker subchild, tell global worker state about
               * this to prevent cleanup if important worker resources on exit.
               */
              _pgbckctl_job_type = BACKGROUND_WORKER_CHILD;
              ios->notify_fork(boost::asio::io_service::fork_child);

              /*
               * Inform the io_service that the fork is finished and that this is the
               * child process. The io_service uses this opportunity to create any
               * internal file descriptors that must be private to the new process.
               * this->ios->notify_fork(boost::asio::io_service::fork_child);
               */

              /*
               * The child won't be accepting new connections, so we can close the
               * acceptor. It remains open in the parent.
               */
              acpt->close();

              /* The child process is not interested in processing the SIGCHLD signal. */
              sset->cancel();

              /*
               * Everything in shape, attach to parent shared memory slot
               * and save its childs information there.
               */
              worker_shm->attach(streamDescr->catalog_name, false);

              try {

                {
                  shm_worker_area worker_info;
                  sub_worker_info child_info;

                  child_info.pid = ::getpid();

                  /* we use a scoped lock */
                  boost::interprocess::scoped_lock<boost::interprocess::interprocess_mutex>(*(worker_shm->check_and_get_mutex()));

                  worker_shm->write(streamDescr->worker_id, child_id, child_info);

                }

                /*
                 * Save child id within runtime configuration, so it can easily
                 * referenced by subsequent command handlers.
                 */
                server_env->getRuntimeConfiguration()->create("recovery_instance.child_id",
                                                              child_id,
                                                              child_id);

                BOOST_LOG_TRIVIAL(debug) << "registered worker id="
                                         << worker_id << ", child_id="
                                         << child_id;

                /*
                 * Setup connection.
                 */
                initial_read();

              } catch(TCPServerFailure &failure) {

                BOOST_LOG_TRIVIAL(fatal) << failure.what();
                soc->close();
                ios->stop();

              }

              catch (SHMFailure &failure) {

                BOOST_LOG_TRIVIAL(fatal) << failure.what();
                soc->close();
                ios->stop();

              }

            }

          else

            {
              /*
               * Inform the io_service that the fork is finished (or failed) and that
               * this is the parent process. The io_service uses this opportunity to
               * recreate any internal resources that were cleaned up during
               * preparation for the fork.
               */
              ios->notify_fork(boost::asio::io_service::fork_parent);

              soc->close();
              start_accept();

            }

        }
      else
        {
          BOOST_LOG_TRIVIAL(fatal) << "Accept error: " << ec.message();
          start_accept();
        }
    }

    virtual void initial_read() = 0;
    virtual void start_read_header() = 0;
    virtual void start_read_msg() = 0;
    virtual void start_write(std::size_t length) = 0;


  public:

    PGBackupCtlStreamingServer() {}

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

  class PGProtoStreamingServer : public PGBackupCtlStreamingServer {
  private:

    /**
     * The internal query descriptor initialized
     * by the streaming replication parser.
     */
    std::shared_ptr<pgprotocol::PGProtoCmdDescr> cmd = nullptr;

    /*
     * Internal state machine indicator.
     */
    PostgreSQLProtocolState state = PGPROTO_STARTUP;

    /**
     * A handler instance to manage catalog database access. This also means
     * that this is the workhorse for various Streaming Replication commands.
     */
    shared_ptr<PGProtoCatalogHandler> catalogHandler = nullptr;

    /**
     * Current message header. Defines the current
     * message processing context. Initialized by the pgproto_header_in()
     * completion handler.
     */
    pgprotocol::pg_protocol_msg_header msghdr;

    /**
     * Internal memory buffers, gets re-allocated
     * for any new incoming or outgoing message.
     */
    ProtocolBuffer write_buffer;
    ProtocolBuffer read_header_buffer;
    ProtocolBuffer read_body_buffer;

    /**
     * Clear contents of protocol error stack.
     */
    void clearErrorStack();

    /*
     * Protocol error stack. Flushed by _send_error() or _send_notice().
     */
    pgprotocol::ProtocolErrorStack errm;

    /**
     * Last processed command tag. Set after processing a query.
     */
    pgprotocol::PGProtoCmdTag last_cmd_tag = pgprotocol::UNKNOWN_CMD;

    /**
     * Rows processed by last_cmd_tag.
     */
    unsigned int processed_rows = 0;

    /**
     * Disables streaming API commands. This is done
     * by process_startup_guc() during startup in case no
     * special basebackup name was selected for connection.
     */
    bool disable_streaming_commands = false;

    /**
     * Reset query state affine properties to defaults.
     */
    virtual void resetQueryState();

    /*
     * Handles the startup header.
     */
    void _startup_header();

    /**
     * Requests a basebackup from the catalog.
     * In case the basebackup exists, this will add the
     * basebackup into the worker shared memory, so that concurrent
     * caller recognized its usage here. Might throw a
     * CCatalogIssue in case there are catalog problems.
     */
    void process_startup_guc();

    /**
     * Extracts and process the query string from
     * a QueryMessage.
     */
    std::string _process_query_execute(size_t qsize);

    /**
     * Prepare to process a QueryMessage from client.
     * Returns the query string length extracted from
     * the message header.
     */
    int _process_query_start(int hdr_len);

    /**
     * Read and convert startup GUCs into a local
     * hashmap.
     */
    std::map<std::string, std::string> startup_gucs;
    void _read_startup_gucs();

    /**
     * Sends an error response to the client.
     */
    void _send_error();

    /**
     * Sends a notice response to the client.
     */
    void _send_notice();

    /**
     * Sends a CommandComplete Response.
     */
    void _send_command_complete(pgprotocol::PGProtoCmdTag tag,
                                unsigned int rowcount);

    /**
     * Sends a AuthenticationOK response.
     */
    void _send_AuthenticationOK();

    /**
     * Sends a ReadyForQuery response to the client.
     */
    void _send_ReadyForQuery();

    /**
     * Sends a ParameterStatus message to the client.
     */
    void _send_ParameterStatus();

    /**
     *
     */
    void _send_BackendKey();

    /**
     * Serializes key/value pairs (server parameters)
     * into the specified memory buffer
     */
    void _parameter_to_buffer(ProtocolBuffer &dest,
                              std::string key,
                              std::string val);

    /**
     * Read the message header from the current buffer
     * content.
     */
    pgprotocol::PGMessageType _read_message_header();

  protected:

    /*
     * Start reading a new protocol message.
     */
    virtual void start_read_header();
    virtual void start_read_msg();
    virtual void initial_read();
    virtual void initial_read_body();
    virtual void start_write(size_t length);

    /**
     * Handles startup message body.
     */
    virtual void startup_msg_body(const boost::system::error_code& ec,
                                  std::size_t len);

    /**
     * Handles incoming message body.
     */
    virtual void pgproto_msg_in(const boost::system::error_code& ec,
                                std::size_t len);

    /**
     * Handles incoming message header.
     */
    virtual void pgproto_header_in(const boost::system::error_code& ec,
                                   std::size_t len);

    /**
     * Handler for incoming startup message
     */
    virtual void startup_msg_in(const boost::system::error_code& ec,
                                 std::size_t len);

      /*
     * Handles outgoing message.
     */
    virtual void pgproto_msg_out(const boost::system::error_code &ec);

    /**
     * Process protocol messages.
     */
    virtual bool process_message();

    /**
     * Stacks an protocol error or notice message with the given severity
     * on the protocol stack. Calling this method just stacks
     * the message onto the message stack, see _send_error() or _send_notice
     * to empy the stack and sent the message finally over the wire.
     */
    virtual void msg(pgprotocol::PGErrorSeverity severity,
                     std::string msg,
                     bool translatable = true);

    virtual void set_sqlstate(std::string state);

  public:

    PGProtoStreamingServer(std::shared_ptr<RecoveryStreamDescr> streamDescr);
    virtual ~PGProtoStreamingServer();

    /*
     * Run the io service.
     */
    virtual void run();

  };

}

StreamingServer::StreamingServer(std::shared_ptr<RecoveryStreamDescr> streamDescr) {

  this->instance = std::make_shared<PGProtoStreamingServer>(streamDescr);

}

void StreamingServer::run() {

  BOOST_LOG_TRIVIAL(debug) << "DEBUG: run StreamingServer";
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

  this->streamDescr = streamDescr;

  /*
   * Create io_service handler
   */
  this->ios = new ba::io_service();

  /*
   * Create signal set
   */
  this->sset = new ba::signal_set(*(this->ios), SIGCHLD);
  this->sset_exit = new ba::signal_set(*(this->ios), SIGTERM, SIGINT);

  /*
   * Create acceptor handle
   */
  if (streamDescr->listen_on.size() == 1) {

    this->acpt = new ip::tcp::acceptor(*(this->ios),
                                       ip::tcp::endpoint(ip::address::from_string(streamDescr->listen_on[0]),
                                                         streamDescr->port));
  } else {

    this->acpt = new ip::tcp::acceptor(*(this->ios),
                                       ip::tcp::endpoint(ip::tcp::v6(),
                                                         streamDescr->port));
  }

  /*
   * Instantiate the socket
   */
  this->soc = new ip::tcp::socket(*(this->ios));

  /* Create and assign runtime configuration */

  server_env->assignRuntimeConfiguration(RuntimeVariableEnvironment::createRuntimeConfiguration());
  BOOST_LOG_TRIVIAL(debug) << "created runtime configuration";

}

PGBackupCtlStreamingServer::~PGBackupCtlStreamingServer() {

  if (this->soc != nullptr)
    delete this->soc;

  if (this->acpt != nullptr)
    delete this->acpt;

  if (this->sset != nullptr)
    delete this->sset;

  if (this->sset_exit != nullptr)
    delete this->sset_exit;

  if (this->ios != nullptr)
    delete this->ios;

}


void PGBackupCtlStreamingServer::run() {

  BOOST_LOG_TRIVIAL(debug) << "DEBUG: running PGBackupCtlStreamingServer";

  start_signal_wait();
  start_accept();

  this->ios->run();

}

/* ****************************************************************************
 * Implementation PGProtoStreamingServer
 * (Implements PostgreSQL Streaming protocol)
 * ****************************************************************************/

#define INITIAL_STARTUP_BUFFER_SIZE 8

PGProtoStreamingServer::PGProtoStreamingServer(std::shared_ptr<RecoveryStreamDescr> streamDescr)
  : PGBackupCtlStreamingServer(streamDescr) {

  /*
   * Initialize command handler
   */
  cmd = std::make_shared<pgprotocol::PGProtoCmdDescr>();

}

PGProtoStreamingServer::~PGProtoStreamingServer() {}

void PGProtoStreamingServer::run() {

  std::shared_ptr<RuntimeConfiguration> runtime_configuration
    = server_env->getRuntimeConfiguration();

  /*
   * Attach to worker shared memory area.
   */
  worker_shm->attach(streamDescr->catalog_name, false);

  /* initialization stuff */
  worker_id = streamDescr->worker_id;

  /*
   * Create global runtime parameters.
   */
  runtime_configuration->create("recovery_instance.archive_id",
                                streamDescr->archive_id,
                                -1);
  runtime_configuration->create("recovery_instance.use_ssl",
                                streamDescr->use_ssl,
                                false);

  runtime_configuration->create("recovery_instance.catalog_name",
                                streamDescr->catalog_name,
                                "");

  runtime_configuration->create("recovery_instance.worker_id",
                                worker_id,
                                worker_id);

  /*
   * Handler for catalog database access.
   */
  catalogHandler = make_shared<PGProtoCatalogHandler>(streamDescr->catalog_name);

  /* Internal startup buffer */
  this->read_header_buffer.allocate(INITIAL_STARTUP_BUFFER_SIZE);

  start_signal_wait();
  start_accept();

  this->ios->run();

}

void PGProtoStreamingServer::_parameter_to_buffer(ProtocolBuffer &dest,
                                                  std::string key,
                                                  std::string val) {

  std::string temp_str;

  /* Do nothing in case key is empty */
  if (key.length() == 0)
    return;

  /*
   * Allocate room for the string. We need
   * two extra bytes for the null byte(s)!
   */
  dest.allocate(key.length() + val.length() + 2);

  /*
   * Write key and its null byte
   */
  dest.write_buffer(key.c_str(), key.length());
  dest.write_byte('\0');

  /*
   * Write value and its null byte
   */
  dest.write_buffer(val.c_str(), val.length());
  dest.write_byte('\0');

  /* ... and done */
}

void PGProtoStreamingServer::_read_startup_gucs() {

  std::ostringstream sbuf;
  std::string key = "";
  std::string val = "";
  bool is_key = true;
  boost::asio::mutable_buffer temp_buf(this->read_body_buffer.ptr(),
                                       this->read_body_buffer.getSize());
  const char *start_byte = boost::asio::buffer_cast<const char *>(temp_buf);

  for (size_t i = 0; i < (boost::asio::buffer_size(temp_buf) - 1); i++) {

    const char *current_byte = (boost::asio::buffer_cast<const char *>(temp_buf)) + i;

    if ((current_byte != start_byte) && *current_byte == '\0') {

      /*
       * Copy last offset up to current position without null byte into either
       * key or value string
       */
      std::copy(start_byte, current_byte, std::ostream_iterator<char>(sbuf));

      /*
       * Save string as key?
       */
      if (is_key) {

        key = sbuf.str();
        sbuf.str("");
        sbuf.clear();
        is_key = false;

        /*
         * Convert key into lower case.
         */
        boost::algorithm::to_lower(key);

      } else {

        val = sbuf.str();
        sbuf.str("");
        sbuf.clear();

        is_key = true;

      }

      if (is_key) {

        startup_gucs.insert( std::pair<std::string, std::string>(key, val) );

      }

      /* Next byte offset, but skip current null byte */
      start_byte = current_byte + 1;

    }

  }

#ifdef __DEBUG__
  for (auto &item : startup_gucs) {

    BOOST_LOG_TRIVIAL(debug) << "startup GUC: \""
                             << item.first
                             << "\"=\"" << item.second << "\"";

  }
#endif

}

void PGProtoStreamingServer::_startup_header() {

  int msglen = 0;
  int protocolVersion;

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO startup buffer size: "
                           << this->read_header_buffer.getSize();

  /*
   * Read the message bytes. First four bytes is the startup
   * length indicator followed by the protocol version.
   */
  read_header_buffer.read_int(msglen);
  read_header_buffer.read_int(protocolVersion);

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO len " << msglen;
  BOOST_LOG_TRIVIAL(debug) << "PG PROTO ver " << PG_PROTOCOL_MAJOR(protocolVersion);

    /*
   * XXX: honor use_ssl, whether we are forced to SSL.
   */
  if (PG_PROTOCOL_MAJOR(protocolVersion) == 1234) {

    pgprotocol::PGMessageType nossl = pgprotocol::NoSSLMessage;

    BOOST_LOG_TRIVIAL(debug) << "PG PROTO SSL negotiation";

    /*
     * Send NO SSL negotiation to client.
     */
    this->state = PGPROTO_STARTUP_SSL_NO;

    /* No SSL currently, tell the client
     * we are currently not handling SSL requests */
    write_buffer.assign(&nossl, sizeof(pgprotocol::PGMessageType));

    /* we're done at this point, send the NoSSL Message */
    return;

  } else if (PG_PROTOCOL_MAJOR(protocolVersion) == 3) {

    BOOST_LOG_TRIVIAL(debug) << "PG PROTO version 3, setting up connection";

    this->read_body_buffer.allocate(msglen - MESSAGE_HDR_LENGTH_SIZE
                                    - sizeof(protocolVersion));
    msglen = read_body_buffer.getSize();

    /*
     * We must read all remaining bytes.
     */
    this->state = PGPROTO_READ_STARTUP_GUC;

  }

}

void PGProtoStreamingServer::_send_AuthenticationOK() {

  pgprotocol::pg_protocol_auth authreq;

  authreq.hdr.type = pgprotocol::AuthenticationMessage;
  authreq.hdr.length = 8;
  authreq.auth_type = 0;

  write_buffer.allocate(MESSAGE_HDR_SIZE + sizeof(authreq.auth_type));
  write_buffer.write_byte(authreq.hdr.type);
  write_buffer.write_int(authreq.hdr.length);
  write_buffer.write_int(authreq.auth_type);

  this->state = PGPROTO_AUTH;

}

void PGProtoStreamingServer::_send_BackendKey() {

  pgprotocol::pg_protocol_backendkey keydata;

  keydata.pid = ::getpid();
  keydata.key = 1234;

  this->write_buffer.allocate(MESSAGE_HDR_SIZE + sizeof(keydata.pid)
                              + sizeof(keydata.key));

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO backend key buf size " << this->write_buffer.getSize();

  write_buffer.write_byte(keydata.hdr.type);
  write_buffer.write_int(keydata.hdr.length);
  write_buffer.write_int(keydata.pid);
  write_buffer.write_int(keydata.key);

  this->state = PGPROTO_READY_FOR_QUERY;

}

/*
 * _process_query_execute() is the main entry point for query processing.
 *
 * This method is called once a PostgreSQL message is dispatched as a
 * query(Q) protocol message, the query is extracted and passed over to
 * the PostgreSQLStreamingParser which is responsible to parse the query.
 *
 * If the query is valid, the parser would have generated an executable
 * command object instance (derived from ProtocolCommandHandler),
 * which will finally execute the corresponding actions.
 *
 * Since the PostgreSQLStreamingParser can parse and execute multi statement
 * command strings (separated by ';'), we must do the execution within a
 * loop.
 *
 * Please note that an executable command object (a descendant of class
 * PGProtoStreamingCommand) never does I/O itself. Instead, it calls
 * the protocol interface method PGProtoStreamingCommand::step(), which
 * fills the specified ProtocolBuffer and leaves the I/O to the response
 * handler itself. This is in normal cases the StreamingServer instance.
 */
std::string PGProtoStreamingServer::_process_query_execute(size_t qsize) {

  char *qbuf;
  std::string query_string = "";
  std::string error_string = "";

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO query string length: " << qsize;

  /**
   * Make sure we allocate an reasonable sized
   * buffer for the input query. If we have a buffer
   * larger than PGPROTO_MAX_QUERY_SIZE we error out.
   */
  if (qsize > PGPROTO_MAX_QUERY_SIZE) {

    msg(pgprotocol::PG_ERR_ERROR, "could not process invalid query string");
    msg(pgprotocol::PG_ERR_DETAIL, "query exceeds maximum query length");
    set_sqlstate("42601");
    _send_error();

    state = PGPROTO_ERROR_AFTER_QUERY;

  }

  /*
   * Extract query string from buffer.
   */
  qbuf = new char[qsize + 1];
  memset(qbuf, 0, qsize + 1);

  read_body_buffer.read_buffer(qbuf, qsize);

  /**
   * Extract the query length from the buffer and convert it
   * to a std::string. Then call the parser to handle the query.
   */
  query_string = std::string(qbuf);

  delete[] qbuf;

  BOOST_LOG_TRIVIAL(debug) << "query " << query_string;

  try {

    /* Parse the query */
    pgprotocol::PostgreSQLStreamingParser pgparser(server_env->getRuntimeConfiguration());
    std::shared_ptr<pgprotocol::ProtocolCommandHandler> handler = nullptr;
    std::shared_ptr<pgprotocol::PGProtoStreamingCommand> cmd = nullptr;
    pgprotocol::PGProtoCommandExecutionQueue execQueue;

    /*
     * The PostgreSQLStreamingParser::parse() method will
     * materialize a command execution queue with executable
     * command handlers.
     */
    execQueue = pgparser.parse(catalogHandler,
                               query_string);

    BOOST_LOG_TRIVIAL(debug) << "parser exited successfully";

    /* We're not bothered with a PGProtoCmdFailure, so
     * proceed and execute the successfully parsed command.
     */
    if (!execQueue.empty()) {

      while (!execQueue.empty()) {

        int step;

        handler = execQueue.front();
        execQueue.pop();

        BOOST_LOG_TRIVIAL(debug) << "executing command";

        cmd = handler->getExecutable(worker_shm);

        /*
         * If authentication procedure has sucessfully passed, we
         * send some NOTICE message indicating special startup settings.
         */
        if (cmd->needsArchive() && disable_streaming_commands) {

          error_string = "connected to catalog only, streaming API commands disabled";
          state = PGPROTO_ERROR_AFTER_QUERY;
          break;

        }

        cmd->execute();

        while((step = cmd->step(write_buffer)) != -1) {

          state = PGPROTO_PROCESS_QUERY_IN_PROGRESS;

          BOOST_LOG_TRIVIAL(debug) << "PG PROTO sent query message buffer "
                                   << write_buffer.getSize();

          start_write(write_buffer.getSize());

          BOOST_LOG_TRIVIAL(debug) << "stepping protocol message "
                                   << step;

        }

        /* Query processing done, prepare a command complete message */
        _send_command_complete(pgprotocol::SELECT_CMD, 0);
        start_write(write_buffer.getSize());

      }

    } else {

      error_string = "empty command execution queue, nothing to do";
      state = PGPROTO_ERROR_AFTER_QUERY;

    }

  } catch(pgprotocol::PGProtoCmdFailure &failure) {

    /* protocol specific exception handling */

    BOOST_LOG_TRIVIAL(fatal) << failure.what();
    state = PGPROTO_ERROR_AFTER_QUERY;
    error_string = failure.what();

  } catch(CPGBackupCtlFailure &failure) {

    BOOST_LOG_TRIVIAL(fatal) << failure.what();
    state = PGPROTO_ERROR_AFTER_QUERY;
    error_string = failure.what();

  } catch (std::exception &e) {

    /* generic exception handling */

    BOOST_LOG_TRIVIAL(fatal) << e.what();
    error_string = e.what();
    state = PGPROTO_ERROR_AFTER_QUERY;

  }

  /*
   * If an error condition was met, prepare
   * the error response and exit.
   */
  if (state == PGPROTO_ERROR_AFTER_QUERY) {

    msg(pgprotocol::PG_ERR_ERROR, error_string);
    msg(pgprotocol::PG_ERR_DETAIL, "query was: " + query_string);
    set_sqlstate("42601");
    _send_error();
    start_write(write_buffer.getSize());

    state = PGPROTO_ERROR_AFTER_QUERY;

  }

  return query_string;
}

int PGProtoStreamingServer::_process_query_start(int hdr_len) {

  /*
   * Read message body length.
   */
  int query_str_len = 0;

  query_str_len = hdr_len - MESSAGE_HDR_LENGTH_SIZE;

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO query string len: " << query_str_len;

  /*
   * Check query string length. We don't process
   * anything which doesn't make sense here...
   */

  if (query_str_len <= 0) {

    state = PGPROTO_ERROR_AFTER_QUERY;

  }

  /*
   * If an error condition was met, prepare
   * the error response and exit.
   */
  if (state == PGPROTO_ERROR_AFTER_QUERY) {

    msg(pgprotocol::PG_ERR_ERROR, "could not process invalid query string");
    set_sqlstate("42601");
    _send_error();

    return query_str_len;
  }

  state = PGPROTO_PROCESS_QUERY_EXECUTE;
  return query_str_len;

}

void PGProtoStreamingServer::_send_ParameterStatus() {

  pgprotocol::pg_protocol_param_status status;

  /*
   * Private memory buffer to build
   * result string.
   *
   * NOTE:
   *
   * The key=value pairs are encoding with null
   * terminated string into the buffer.
   */
  ProtocolBuffer temp_buf;

  /*
   * Send server_version
   */
  this->_parameter_to_buffer(temp_buf, "server_version",
                             this->streamDescr->version);

  // this->_parameter_to_buffer(temp_buf, "integer_datetimes",
  //                            "on");

  status.hdr.type = pgprotocol::ParameterStatusMessage;

  /* Build key value pairs */

  /*
   * Send our server version.
   */
  status.data_ptr = temp_buf.ptr();
  status.hdr.length = MESSAGE_HDR_LENGTH_SIZE + temp_buf.getSize();

  write_buffer.allocate(MESSAGE_HDR_SIZE + MESSAGE_DATA_LENGTH(status));

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO parameter buffer data length: " << status.hdr.length;

  write_buffer.write_byte(status.hdr.type);
  write_buffer.write_int(status.hdr.length);
  write_buffer.write_buffer(status.data_ptr,
                            temp_buf.getSize());

  this->state = PGPROTO_SEND_BACKEND_KEY;

}

void PGProtoStreamingServer::clearErrorStack() {

  if (this->errm.empty())
    return;

  while (!this->errm.empty())
    this->errm.pop();

}

void PGProtoStreamingServer::msg(pgprotocol::PGErrorSeverity severity,
                                 std::string msg,
                                 bool translatable) {

  pgprotocol::PGErrorResponseType errtype;

  if (translatable) {
    errtype = 'S';
  } else {
    errtype = 'V';
  }
  switch(severity) {

  case pgprotocol::PG_ERR_ERROR:
    {
      this->errm.push(errtype, "ERROR");
      this->errm.push('M', msg);

      break;
    }

  case pgprotocol::PG_ERR_FATAL:
    {
      this->errm.push(errtype, "FATAL");
      this->errm.push('M', msg);

      break;
    }
  case pgprotocol::PG_ERR_WARNING:
    {
      this->errm.push(errtype, "WARNING");
      this->errm.push('M', msg);

      break;
    }

  case pgprotocol::PG_ERR_DETAIL:
    {
      this->errm.push('D', msg);

      break;
    }
  case pgprotocol::PG_ERR_NOTICE:
    {
      this->errm.push(errtype, "NOTICE");
      this->errm.push('M', msg);

      break;
    }

  default:
    break;

  }

}

void PGProtoStreamingServer::set_sqlstate(std::string state) {

  errm.push('C', state);

}

void PGProtoStreamingServer::_send_command_complete(pgprotocol::PGProtoCmdTag tag,
                                                    unsigned int rowcount) {
  pgprotocol::pg_protocol_msg_header hdr;
  std::ostringstream message_buffer;

  hdr.type = pgprotocol::CommandCompleteMessage;

  /*
   * Generate command tag
   */
  switch(tag) {
  case pgprotocol::INSERT_CMD:
    {
      message_buffer << "INSERT 0 " << rowcount;
      break;
    }
  case pgprotocol::UPDATE_CMD:
    {
      message_buffer << "UPDATE " << rowcount;
      break;
    }
  case pgprotocol::DELETE_CMD:
    {
      message_buffer << "DELETE " << rowcount;
      break;
    }
  case pgprotocol::MOVE_CMD:
    {
      message_buffer << "MOVE " << rowcount;
      break;
    }
  case pgprotocol::FETCH_CMD:
    {
      message_buffer << "FETCH " << rowcount;
      break;
    }
  case pgprotocol::SELECT_CMD:
    {
      message_buffer << "SELECT " << rowcount;
      break;
    }
  case pgprotocol::COPY_CMD:
    {
      message_buffer << "COPY " << rowcount;
      break;
    }
  case pgprotocol::UNKNOWN_CMD:
    {
      message_buffer << "UNKNOWN " << rowcount;
      break;
    }
  }

  /*
   * NOTE: We need a extra byte at the end of the command tag!
   */
  hdr.length = MESSAGE_HDR_LENGTH_SIZE + message_buffer.str().length() + 1;

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO command completion tag " << message_buffer.str();
  BOOST_LOG_TRIVIAL(debug) << "PG PROTO message buffer length " << hdr.length;

  write_buffer.allocate(hdr.length + MESSAGE_HDR_BYTE);
  write_buffer.write_byte(hdr.type);
  write_buffer.write_int(hdr.length);
  write_buffer.write_buffer(message_buffer.str().c_str(), message_buffer.str().length());
  write_buffer.write_byte('\0');

  state = PGPROTO_COMMAND_COMPLETE;
}

void PGProtoStreamingServer::_send_error() {

  /*
   * Slurp in error stack.
   */
  size_t errm_size = 0;
  errm.toBuffer(write_buffer,
                errm_size);

  BOOST_LOG_TRIVIAL(debug) << "toBuffer() buffer size " << write_buffer.getSize();
  this->state = PGPROTO_ERROR_CONDITION;

}

void PGProtoStreamingServer::_send_notice() {

  /*
   * Slurp in error stack.
   */
  size_t errm_size = 0;
  errm.toBuffer(write_buffer,
                errm_size,
                false);

  BOOST_LOG_TRIVIAL(debug) << "toBuffer() buffer size " << write_buffer.getSize();
  this->state = PGPROTO_NOTICE_CONDITION;

}


void PGProtoStreamingServer::_send_ReadyForQuery() {

  pgprotocol::pg_protocol_ready_for_query rfq;

  rfq.hdr.type = pgprotocol::ReadyForQueryMessage;
  rfq.hdr.length = 5;
  rfq.tx_state = 'I'; /* idle in transaction */

  write_buffer.allocate(MESSAGE_HDR_SIZE + sizeof(rfq.tx_state));
  write_buffer.write_byte(rfq.hdr.type);
  write_buffer.write_int(rfq.hdr.length);
  write_buffer.write_byte(rfq.tx_state);

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO ReadyForQuery";

  this->state = PGPROTO_READY_FOR_QUERY_WAIT;

}

void PGProtoStreamingServer::startup_msg_in(const boost::system::error_code& ec,
                                            std::size_t len) {

  if (ec) {
    throw TCPServerFailure("error reading socket for startup message");
  }

  /**
   * Startup message expected, delegate the legwork
   * to _startup which prepares the necessary steps.
   */
  _startup_header();

  /*
   * In case _startup_header() decided to decline a SSL request,
   * we must sent a NoSSLMessage back to the client. If SSL
   * was acknowledged, we must setup SSL.
   *
   * Iff _startup_header() encountered a plain (unencrypted) connection
   * attempt, we can just proceed and read up the upcoming
   * startup gucs.
   */
  if ( (state == PGPROTO_STARTUP_SSL_OK)
        || (state == PGPROTO_STARTUP_SSL_NO) ) {

    start_write(write_buffer.getSize());

    /* Client responds with a startup message again. */
    read_header_buffer.allocate(INITIAL_STARTUP_BUFFER_SIZE);

    /* Call initial_read again to re-read final startup
     * messages to establish SSL/NoSSL */
    initial_read();
    _startup_header();

  }

  if (state == PGPROTO_READ_STARTUP_GUC)  {

    /* Read remaining startup GUCs */
    BOOST_LOG_TRIVIAL(debug) << "PG PROTO startup: read startup contents";

    /* _startup_header() has prepared the receive buffers for
     * the remaining bytes, so we can just proceed and
     * read the remaining bytes */
    initial_read_body();

  }

  // else {
  //   throw TCPServerFailure("unexpected protocol state during connection establishing");
  // }

}

void PGProtoStreamingServer::pgproto_header_in(const boost::system::error_code& ec,
                                               std::size_t len) {
  if (!ec) {

    BOOST_LOG_TRIVIAL(debug) << "PG PROTO incoming msg header completion handler, transferred "
                             << len << " bytes";

    /*
     * Read message header contents.
     */
    _read_message_header();

    /*
     * Schedule body read with message body length
     * if necessary, else process the plain message header.
     */
    if (MESSAGE_HDR_DATA_LENGTH(msghdr) > 0) {

      read_body_buffer.allocate(MESSAGE_HDR_DATA_LENGTH(msghdr));
      start_read_msg();

    } else {

      switch (msghdr.type) {

      case pgprotocol::CancelMessage:

        {

          BOOST_LOG_TRIVIAL(debug) << "PG PROTO termination message, exiting";

          soc->close();
          ios->stop();

          std::exit(0);

          /* Equivalent to TerminationMessage */
          break;

        }

      }

    }

  }

}

void PGProtoStreamingServer::process_startup_guc() {

  /*
   * Check if a specific value for the "dbname" parameter was set.
   *
   * Currently we allow placeholders (aka reserved) values for specific
   * kind of basebackups to connect to:
   *
   * - "latest": Means the latest create basebackup in the list, if any
   * - "current": the same as above.
   * - "oldest": the oldest available basebackup, if any
   * - "catalog": allows archive related commands, but no streaming API supported
   *              commands
   */

  std::map<std::string, std::string>::iterator guc_iter;

  guc_iter = startup_gucs.find("database");

  if (guc_iter == startup_gucs.end()) {

    BOOST_LOG_TRIVIAL(debug) << "no database selected, only archive commands available";
    disable_streaming_commands = true;

  } else {

    if (guc_iter->second == "catalog") {

      BOOST_LOG_TRIVIAL(debug) << "catalog database selected, only archive commands available";
      disable_streaming_commands = true;

    } else {

      shared_ptr<RuntimeConfiguration> rtconfig = server_env->getRuntimeConfiguration();
      int archive_id = -1;
      string basebackup_fqfn = guc_iter->second;
      string catalog_name;

      BOOST_LOG_TRIVIAL(debug) << "basebackup "
                               << "\""
                               << basebackup_fqfn
                               << "\" requested";
      /*
       * Special ident name, see comments above...
       *
       * Save the backup identifier within the runtime configuration
       * environment.
       *
       * Please note that we have three reserved identifier here:
       *
       * - newest/latest: this is the latest basebackup, if any
       * - oldest: the oldest basebackup currently present in the catalog
       *
       * The catalog handler will process those special identifier later
       * and replace the corret FQFN into recovery_instance.basebackup_path.
       * So any callers relying on a static basebackup_path here are
       * wrong!
       */
      rtconfig->create("recovery_instance.basebackup_path",
                       basebackup_fqfn,
                       basebackup_fqfn);

      /*
       * We want to have the backup id we are connected to. This is
       * registered within our child slot in the worker shared memory
       * to create a lock on it. Concurrent archive changes will then
       * recognize that this basebackup is currently in use.
       *
       * To accomplish this, we have a PGProtoCatalogHandler within
       * our server context (creating during the startup phase in the run()
       * method, which does all the catalog access. This will
       * register the requested base backup in our child shared memory
       * structure.
       *
       * worker_id (ID of our background parent process) and our child_id
       * should already been registered via handle_accept().
       *
       * Our child is already serving a specific archive, it's ID
       * can be found in the runtime_configuration object, like the catalog
       * name we're using.
       */
      rtconfig->get("recovery_instance.archive_id")->getValue(archive_id);

      /*
       * PGProtoCatalogHandler::attach() throws a CCatalogIssue
       * in case the basebackup_fqfn couldn't processed properly.
       */
      catalogHandler->attach(basebackup_fqfn,
                             archive_id,
                             worker_id,
                             child_id,
                             worker_shm);

    }

  }

}

void PGProtoStreamingServer::resetQueryState() {

  processed_rows = 0;
  last_cmd_tag = pgprotocol::UNKNOWN_CMD;
  clearErrorStack();

}

void PGProtoStreamingServer::startup_msg_body(const boost::system::error_code& ec,
                                         std::size_t len) {

  _read_startup_gucs();

  /*
   * Before proceeding, we are going to check if the
   * client has submitted basebackup name to connect to.
   *
   * Within the streaming server context here, this means
   * a specific basebackup target was requested.
   */
  try {

    process_startup_guc();

  } catch (CCatalogIssue &ci) {

    std::string error_str = "requested basebackup does not exist";
    msg(pgprotocol::PG_ERR_FATAL, error_str);
    set_sqlstate("");
    _send_error();
    start_write(write_buffer.getSize());

    /* Throw here, since we must force our worker to exit hard */
    throw TCPServerFailure(error_str);

  }

  /*
   * After having read the startup GUCs we enter the
   * authentication routines.
   */
  _send_AuthenticationOK();
  start_write(write_buffer.getSize());

  if (disable_streaming_commands) {

    msg(pgprotocol::PG_ERR_NOTICE,
        "streaming API commands disabled");
    _send_notice();
    start_write(write_buffer.getSize());

  }

  /*
   * Sent over ParameterStatus Messages.
   */
  _send_ParameterStatus();
  start_write(write_buffer.getSize());

  /*
   * Sennt the backend cancellation key....
   */
  _send_BackendKey();
  start_write(write_buffer.getSize());

  /*
   * ReadyForQuery marks the last message here. We're exiting
   * here and caller has the chance to enter main processing loop.
   */
  _send_ReadyForQuery();
  start_write(write_buffer.getSize());

  /*
   * ReadForQuery means the end of the initial
   * startup processing. The follow up start_read()
   * will change the incoming message handler to
   * pgproto_msg_in(), where all remaining protocol
   * messages will be dispatched.
   */
  read_header_buffer.allocate(MESSAGE_HDR_SIZE);
  start_read_header();

}

void PGProtoStreamingServer::pgproto_msg_in(const boost::system::error_code& ec,
                                            std::size_t len) {

  if (ec) {
    BOOST_LOG_TRIVIAL(debug) << "error reading from socket";
  }

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO incoming message handler complete, transferred "
                           << len << " bytes";

  process_message();

}

bool PGProtoStreamingServer::process_message() {

  using namespace pgprotocol;

  bool result = true;

  switch( msghdr.type ) {

  case DescribeMessage:
    break;

  case FlushMessage:
    break;

  case ExecuteMessage:
    break;

  case QueryMessage:
    {
      int query_str_len;

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO processing QueryMessage";

      query_str_len = _process_query_start(msghdr.length);

      if (state == PGPROTO_ERROR_AFTER_QUERY) {

        start_write(write_buffer.getSize());
        break;

      }

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO read query string";

      /*
       * remaining bytes are the query string.
       */
      // read_body_buffer.allocate(query_str_len);
      // start_read_msg();

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO execute query";

      /*
       * _process_query_start() might have problems to calculate
       * the correct query length, check...
       */
      _process_query_execute(query_str_len);

      if (state == PGPROTO_ERROR_AFTER_QUERY) {

        /* Finalize the request */
        _send_ReadyForQuery();
        start_write(write_buffer.getSize());

        break;

      }

      /* Finalize the request */
      _send_ReadyForQuery();
      start_write(write_buffer.getSize());

      /* Reset stateful query properties */
      resetQueryState();

      break;

    }

  case FunctionCallMessage:
    break;

  case CancelMessage:
    break;

  case ParseMessageType:
    break;

  case 'p':
    /* Covers SASLInitialResponseMessage, SASLResponse. */
    break;

  case CopyFailMessage:
    break;

  case '\0':
    break;

  default:
    {
      std::ostringstream oss;

      oss << "message type not recognized: " << msghdr.type;
      throw TCPServerFailure(oss.str());

    }
  }

  /* Back to business... */
  read_header_buffer.allocate(MESSAGE_HDR_SIZE);
  start_read_header();

  return result;

}

pgprotocol::PGMessageType PGProtoStreamingServer::_read_message_header() {

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO extracting message header values";

  if (read_header_buffer.getSize() < MESSAGE_HDR_SIZE) {
    throw TCPServerFailure("short read on message buffer size");
  }

  if (read_header_buffer.pos() > 0) {
    std::ostringstream oss;

    oss << "cannot extract message header from buffer, pos: " << read_header_buffer.pos();
    throw TCPServerFailure(oss.str());

  }

  read_header_buffer.read_byte(msghdr.type);

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO message type: \""
                           << msghdr.type << "\"";

  read_header_buffer.read_int(msghdr.length);

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO message length (including self): "
                           << msghdr.length;

  return msghdr.type;

}

void PGProtoStreamingServer::pgproto_msg_out(const boost::system::error_code &ec) {

  if (ec) {
    throw TCPServerFailure("error on writing to server socket");
  }

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO outgoing message done";

}

void PGProtoStreamingServer::start_write(size_t length) {

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO start_write with " << length << " bytes";

  boost::asio::async_write(SOCKET_P(this), boost::asio::buffer(this->write_buffer.ptr(),
                                                               this->write_buffer.getSize()),
                           boost::asio::transfer_exactly(write_buffer.getSize()),
                           boost::bind(&PGProtoStreamingServer::pgproto_msg_out,
                                       this, _1));

}

void PGProtoStreamingServer::initial_read() {

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO initial_read with " << read_header_buffer.getSize() << " bytes";

  boost::asio::async_read(SOCKET_P(this), boost::asio::buffer(this->read_header_buffer.ptr(),
                                                              this->read_header_buffer.getSize()),
                          boost::asio::transfer_exactly(read_header_buffer.getSize()),
                          boost::bind(&PGProtoStreamingServer::startup_msg_in,
                                      this, _1, _2));

}

void PGProtoStreamingServer::initial_read_body() {

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO initial_read with " << read_body_buffer.getSize() << " bytes";

  boost::asio::async_read(SOCKET_P(this), boost::asio::buffer(this->read_body_buffer.ptr(),
                                                              this->read_body_buffer.getSize()),
                          boost::asio::transfer_exactly(read_body_buffer.getSize()),
                          boost::bind(&PGProtoStreamingServer::startup_msg_body,
                                      this, _1, _2));

}

void PGProtoStreamingServer::start_read_msg() {

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO start_read_msg with " << read_body_buffer.getSize() << " bytes";

  boost::asio::async_read(SOCKET_P(this), boost::asio::buffer(this->read_body_buffer.ptr(),
                                                              this->read_body_buffer.getSize()),
                          boost::asio::transfer_exactly(read_body_buffer.getSize()),
                          boost::bind(&PGProtoStreamingServer::pgproto_msg_in,
                                      this, _1, _2));

}

void PGProtoStreamingServer::start_read_header() {

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO start_read_header with " << read_header_buffer.getSize() << " bytes";

  boost::asio::async_read(SOCKET_P(this), boost::asio::buffer(this->read_header_buffer.ptr(),
                                                              this->read_header_buffer.getSize()),
                          boost::asio::transfer_exactly(read_header_buffer.getSize()),
                          boost::bind(&PGProtoStreamingServer::pgproto_header_in,
                                      this, _1, _2));

}
