#include <boost/asio/io_service.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <boost/array.hpp>
#include <boost/bind.hpp>
#include <boost/log/trivial.hpp>
#include <iostream>
#include <map>

extern "C" {
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <iostream>
}

#include <bgrndroletype.hxx>
#include <pgsql-proto.hxx>
#include <proto-buffer.hxx>
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
        while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {}

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

              start_read();
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

    virtual void start_read() {

      this->soc->async_read_some(boost::asio::buffer(data_),
                                 boost::bind(&PGBackupCtlStreamingServer::handle_read,
                                             this, _1, _2));

    }

    /*
     * Process incoming data message and performs a response.
     */
    virtual void handle_read(const boost::system::error_code& ec, std::size_t length) {

      if (!ec)
        start_write(length);

    }

    virtual void start_write(std::size_t length) {
      boost::asio::async_write(SOCKET_P(this), boost::asio::buffer(data_, length),
                               boost::bind(&PGBackupCtlStreamingServer::handle_write, this, _1));
    }

    virtual void handle_write(const boost::system::error_code& ec) {
      if (!ec)
        start_read();
    }

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
     * Internal memory buffer, gets re-allocated
     * for any new incoming message.
     */
    ProtocolBuffer buf;

    /**
     * Clear contents of protocol error stack.
     */
    void clearErrorStack();

    /*
     * Protocol error stack. Flushed by _send_error().
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
     * Guts of protocol startup.
     */
    void _startup();

    /**
     * Extracts and process the query string from
     * a QueryMessage.
     */
    std::string _process_query_execute(size_t qsize);

    /**
     * Prepare to process a QueryMessage from client.
     */
    void _process_query_start();

    /**
     * Read and convert startup GUCs into a local
     * hashmap.
     */
    std::map<std::string, std::string> startup_gucs;
    void _read_startup_gucs();

    /**
     * Sends a error response to the client.
     */
    void _send_error();

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

  protected:

    /*
     * Start reading a new protocol message.
     */
    virtual void start_read();

    virtual void start_write(size_t length);

    /**
     * Handle an incoming message.
     */
    virtual void handle_read(const boost::system::error_code& ec, std::size_t length);

    /**
     * Handle an outgoing message.
     */
    virtual void handle_write(const boost::system::error_code &ec);

    /*
     * Stacks an error message with the given severity
     * on the protocol stack
     */
    virtual void error_msg(pgprotocol::PGErrorSeverity severity,
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
  this->acpt = new ip::tcp::acceptor(*(this->ios),
                                     ip::tcp::endpoint(ip::tcp::v6(),
                                                       streamDescr->port));

  /*
   * Instantiate the socket
   */
  this->soc = new ip::tcp::socket(*(this->ios));

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

  BOOST_LOG_TRIVIAL(debug) << "DEBUG: run PGBackupCtlStreamingServer";

  start_signal_wait();
  start_accept();

  this->ios->run();

}

/* ****************************************************************************
 * Implementation PGProtoStreamingServer
 * (Implements PostgreSQL Streaming protocol)
 * ****************************************************************************/

PGProtoStreamingServer::PGProtoStreamingServer(std::shared_ptr<RecoveryStreamDescr> streamDescr)
  : PGBackupCtlStreamingServer(streamDescr) {

  /*
   * Initialize command handler
   */
  cmd = std::make_shared<pgprotocol::PGProtoCmdDescr>();

}

PGProtoStreamingServer::~PGProtoStreamingServer() {}

#define INITIAL_STARTUP_BUFFER_SIZE 8

void PGProtoStreamingServer::run() {

  BOOST_LOG_TRIVIAL(debug) << "DEBUG: run PGProtoStreamingServer";

  /* Internal startup buffer */
  this->buf.allocate(INITIAL_STARTUP_BUFFER_SIZE);

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
  boost::asio::mutable_buffer temp_buf(this->buf.ptr(),
                                       this->buf.getSize());
  const char *start_byte = boost::asio::buffer_cast<const char *>(temp_buf);

  for (size_t i = 1; i < (boost::asio::buffer_size(temp_buf) - 1); i++) {

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
        val = "";
        is_key = false;

        std::cerr << "GUC KEY: " << key << std::endl;

      } else {

        val = sbuf.str();
        sbuf.str("");
        sbuf.clear();

        std::cerr << "GUC VALUE: " << val << std::endl;
        startup_gucs.insert( std::pair<std::string, std::string>(key, val) );

        val = "";
        key = "";

        is_key = true;

      }

      /* Next byte offset */
      start_byte = (boost::asio::buffer_cast<const char *>(temp_buf)) + i;

    }

  }

}

void PGProtoStreamingServer::handle_read(const boost::system::error_code& ec, std::size_t length) {

  if (ec) {
    throw TCPServerFailure("error reading on server socket");
  }

  switch (this->state) {

  case PGPROTO_STARTUP:
    {
      /* expected startup message */
      this->_startup();

      /*
       * Recheck state machine.
       *
       * Iff _startup() has got an SSL negotiation
       * request, we must sent our intention wether
       * we want to use SSL or not. _startup() will have changed
       * the state machine in this case, so recheck.
       */
      if ( (this->state == PGPROTO_STARTUP_SSL_OK)
           || (this->state == PGPROTO_STARTUP_SSL_NO) ) {

           start_write(buf.getSize());

      } else if (this->state == PGPROTO_READ_STARTUP_GUC) {

        BOOST_LOG_TRIVIAL(debug) << "PG PROTO READ STARTUP GUC, buf size=" << this->buf.getSize();

        /* remaining bytes contain client GUCs, so read on */
        start_read();

      } else {
        throw TCPServerFailure("unexpected protocol state during startup");
      }

      break;
    }

  case PGPROTO_STARTUP_SSL_OK:
  case PGPROTO_STARTUP_SSL_NO:
    {
      /*
       * Handle SSL Negotiation,
       * receive startup buffers again
       */
      this->_startup();

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO reading remaining startup bytes";

      /* Read remaining bytes (GUCs) */
      start_read();
      break;
    }

  case PGPROTO_READ_STARTUP_GUC:
    {
      BOOST_LOG_TRIVIAL(debug) << "PG PROTO handle read GUCS, size " << this->buf.getSize();

      /* Handle startup GUCs sent by client */
      this->_read_startup_gucs();

      /*
       * We try to authenticate now and sent an authentication
       * message back to the client. This will also set the
       * next state to the internal protocol state machine.
       */

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO  send auth OK";

      this->_send_AuthenticationOK();
      start_write(buf.getSize());

      break;
    }

  case PGPROTO_PROCESS_QUERY_EXECUTE:
    {
      std::string query_string = "";

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO process query EXECUTE";

      /* read the query string and prepare execution */
      query_string = _process_query_execute(length);

      if (state == PGPROTO_ERROR_AFTER_QUERY) {

        BOOST_LOG_TRIVIAL(error) << "error processing query";
        start_write(buf.getSize());
        break;

      }

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO process query string: "
                               << query_string;

      break;
    }

  case PGPROTO_PROCESS_QUERY_START:
    {


      BOOST_LOG_TRIVIAL(debug) << "PG PROTO process query START";

      /* prepare to read query string */
      _process_query_start();

      start_read();
      break;
    }

  default:
    throw TCPServerFailure("unexpected internal protocol state");
  }

}

void PGProtoStreamingServer::handle_write(const boost::system::error_code& ec) {

  if (ec) {
    throw TCPServerFailure("error on writing to server socket");
  }

  switch (this->state) {
  case PGPROTO_STARTUP_SSL_NO:
  case PGPROTO_STARTUP_SSL_OK:
    {
      /*
       * SSL negotiation sent, read startup gucs now
       * from client. This means we must do the startup
       * procedure again now. Adjust the receive buffer
       * to receive the message length and protocolVersion
       * before going on reading. The client will then send
       * additional bytes, with its database connection parameters.
       *
       * We need the message length to know how large those
       * connection string is.
       */
      this->buf.allocate((size_t) 8);
      start_read();
      break;
    }
  case PGPROTO_AUTH:
    {
      /*
       * Server has sent an AuthenticationMessage to our client.
       */
      BOOST_LOG_TRIVIAL(debug) << "PG PROTO sent auth OK";

      /* Sent Parameter status messages to client */
      this->_send_ParameterStatus();
      start_write(this->buf.getSize());

      break;
    }
  case PGPROTO_COMMAND_COMPLETE:
    {
      BOOST_LOG_TRIVIAL(debug) << "PG PROTO command complete";

      this->_send_command_complete(this->last_cmd_tag, this->processed_rows);
      start_write(this->buf.getSize());
      break;

    }
  case PGPROTO_SEND_BACKEND_KEY:
    {

      this->_send_BackendKey();
      start_write(this->buf.getSize());

      break;
    }

  case PGPROTO_READY_FOR_QUERY:
    {

      /* We're ready now, send a ReadyForQuery message */
      this->_send_ReadyForQuery();
      start_write(this->buf.getSize());

      break;
    }

  case PGPROTO_READY_FOR_QUERY_WAIT:
    {
      /* Write handler just completed a ReadyForQuery message */
      BOOST_LOG_TRIVIAL(debug) << "PG PROTO ready for query completed";

      // error_msg(pgprotocol::PG_ERR_ERROR, "not yet implemented, exiting");
      // set_sqlstate("08004");
      // _send_error();

      // this->start_write(buf.getSize());
      buf.allocate(MESSAGE_HDR_SIZE);
      state = PGPROTO_PROCESS_QUERY_START;
      start_read();

      break;
    }

  case PGPROTO_ERROR_CONDITION:
    {
      BOOST_LOG_TRIVIAL(debug) << "PG PROTO error condition processed";

      buf.allocate(MESSAGE_HDR_SIZE);
      start_read();

      break;
    }
  case PGPROTO_ERROR_AFTER_QUERY:
    {
      BOOST_LOG_TRIVIAL(debug) << "PG PROTO error processing query handled";

      _send_ReadyForQuery();
      start_write(this->buf.getSize());

      break;
    }
  default:

    throw TCPServerFailure("unexpected PostgreSQL protocol state");
  }

}

void PGProtoStreamingServer::_startup() {

  int msglen;
  int protocolVersion;

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO startup buffer size: "
                           << this->buf.getSize();

  /*
   * Read the message bytes. First four bytes is the startup
   * length indicator followed by the protocol version.
   */
  buf.read_int(msglen);
  buf.read_int(protocolVersion);

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO len " << msglen;
  BOOST_LOG_TRIVIAL(debug) << "PG PROTO ver " << PG_PROTOCOL_MAJOR(protocolVersion);

  if (PG_PROTOCOL_MAJOR(protocolVersion) == 1234) {

    pgprotocol::PGMessageType nossl = pgprotocol::NoSSLMessage;

    BOOST_LOG_TRIVIAL(debug) << "PG PROTO SSL negotiation";

    /*
     * Send NO SSL negotiation to client.
     */
    this->state = PGPROTO_STARTUP_SSL_NO;

    /* No SSL currently, tell the client
     * we are currently not handling SSL requests */
    buf.assign(&nossl, sizeof(pgprotocol::PGMessageType));

  } else if (PG_PROTOCOL_MAJOR(protocolVersion) == 3) {

    BOOST_LOG_TRIVIAL(debug) << "PG PROTO version 3, setting up connection";

    this->buf.allocate(msglen - MESSAGE_HDR_LENGTH_SIZE);

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

  buf.allocate(MESSAGE_HDR_SIZE + sizeof(authreq.auth_type));
  buf.write_byte(authreq.hdr.type);
  buf.write_int(authreq.hdr.length);
  buf.write_int(authreq.auth_type);

  this->state = PGPROTO_AUTH;
}

void PGProtoStreamingServer::_send_BackendKey() {

  pgprotocol::pg_protocol_backendkey keydata;

  keydata.pid = ::getpid();
  keydata.key = 1234;

  this->buf.allocate(MESSAGE_HDR_SIZE + sizeof(keydata.pid)
                     + sizeof(keydata.key));

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO backend key buf size " << this->buf.getSize();

  buf.write_byte(keydata.hdr.type);
  buf.write_int(keydata.hdr.length);
  buf.write_int(keydata.pid);
  buf.write_int(keydata.key);

  this->state = PGPROTO_READY_FOR_QUERY;

}

std::string PGProtoStreamingServer::_process_query_execute(size_t qsize) {

  char *qbuf;
  std::string query_string = "";

  /*
   * Extract query string from buffer.
   */
  qbuf = new char[qsize + 1];
  memset(qbuf, 0, qsize + 1);

  buf.read_buffer(qbuf, qsize);

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO query string length: " << qsize;
  query_string = std::string(qbuf);

  delete[] qbuf;

  state = PGPROTO_ERROR_AFTER_QUERY;

  /*
   * If an error condition was met, prepare
   * the error response and exit.
   */
  if (state == PGPROTO_ERROR_AFTER_QUERY) {

    error_msg(pgprotocol::PG_ERR_ERROR, "could not process invalid query string");
    set_sqlstate("42601");
    _send_error();

    state = PGPROTO_ERROR_AFTER_QUERY;

  }

  return query_string;
}

void PGProtoStreamingServer::_process_query_start() {

  unsigned char msg_type = '\0';

  /*
   * Determine message type.
   */
  buf.read_byte(msg_type);

  switch((pgprotocol::PGMessageType)msg_type) {

  case pgprotocol::QueryMessage:
    {
      /*
       * Read message body length.
       */
      int msg_len       = 0;
      int query_str_len = 0;

      buf.read_int(msg_len);
      query_str_len = msg_len - MESSAGE_HDR_LENGTH_SIZE;

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO query string len: " << query_str_len;

      /*
       * Check query string length. We don't process
       * anything which doesn't make sense here...
       */

      if (query_str_len <= 0) {
        state = PGPROTO_ERROR_AFTER_QUERY;
        break;
      }

      buf.allocate(query_str_len);

      /*
       * NOTE: fall through and set appropiate state machine
       *       at the end of this method.
       */
      break;
    }

  default:
    {
      /*
       * If reached here, we can't handle the received
       * message type. Respond with an ErrorMessage.
       */
      state = PGPROTO_ERROR_AFTER_QUERY;
      break;
    }

  }; /* switch ... case */

  /*
   * If an error condition was met, prepare
   * the error response and exit.
   */
  if (state == PGPROTO_ERROR_AFTER_QUERY) {

    error_msg(pgprotocol::PG_ERR_ERROR, "could not process invalid query string");
    set_sqlstate("42601");
    _send_error();

    return;
  }

  state = PGPROTO_PROCESS_QUERY_EXECUTE;

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

  buf.allocate(MESSAGE_HDR_SIZE + MESSAGE_DATA_LENGTH(status));

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO parameter buffer data length: " << status.hdr.length;

  buf.write_byte(status.hdr.type);
  buf.write_int(status.hdr.length);
  buf.write_buffer(status.data_ptr,
                   temp_buf.getSize());

  this->state = PGPROTO_SEND_BACKEND_KEY;

}

void PGProtoStreamingServer::clearErrorStack() {

  if (this->errm.empty())
    return;

  while (!this->errm.empty())
    this->errm.pop();

}

void PGProtoStreamingServer::error_msg(pgprotocol::PGErrorSeverity severity,
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

  case pgprotocol::PG_ERR_WARNING:
    {
      this->errm.push(errtype, "WARNING");
      this->errm.push('M', msg);

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

  hdr.length = MESSAGE_HDR_SIZE + message_buffer.str().length();

  buf.allocate(hdr.length + MESSAGE_HDR_BYTE);
  buf.write_byte(hdr.type);
  buf.write_int(hdr.length);
  buf.write_buffer(message_buffer.str().c_str(), message_buffer.str().length());

  state = PGPROTO_COMMAND_COMPLETE;
}

void PGProtoStreamingServer::_send_error() {

  /*
   * Slurp in error stack.
   */
  size_t errm_size = 0;
  errm.toBuffer(buf,
                errm_size);

  BOOST_LOG_TRIVIAL(debug) << "toBuffer() buffer size " << buf.getSize();
  this->state = PGPROTO_ERROR_CONDITION;

}

void PGProtoStreamingServer::_send_ReadyForQuery() {

  pgprotocol::pg_protocol_ready_for_query rfq;

  rfq.hdr.type = pgprotocol::ReadyForQueryMessage;
  rfq.hdr.length = 5;
  rfq.tx_state = 'I'; /* idle in transaction */

  buf.allocate(MESSAGE_HDR_SIZE + sizeof(rfq.tx_state));
  buf.write_byte(rfq.hdr.type);
  buf.write_int(rfq.hdr.length);
  buf.write_byte(rfq.tx_state);

  this->state = PGPROTO_READY_FOR_QUERY_WAIT;

}

void PGProtoStreamingServer::start_write(size_t length) {

  BOOST_LOG_TRIVIAL(debug) << "PG PROTO start_write with " << length << " bytes";

  this->soc->async_write_some(boost::asio::buffer(this->buf.ptr(), length),
                              boost::bind(&PGProtoStreamingServer::handle_write,
                                          this, _1));

}

void PGProtoStreamingServer::start_read() {

  this->soc->async_read_some(boost::asio::buffer(this->buf.ptr(), this->buf.getSize()),
                             boost::bind(&PGProtoStreamingServer::handle_read,
                                         this, _1, _2));

}
