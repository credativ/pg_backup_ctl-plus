#ifndef __HAVE_PGIOSOCKETCONTEXT_HXX_
#define __HAVE_PGIOSOCKETCONTEXT_HXX_

#include <boost/asio/ip/tcp.hpp>
#include <exectx.hxx>

namespace pgbckctl {

  namespace pgprotocol {

    /**
     * PGSocketIOException
     *
     * Thrown if an I/O error occured on the current network socket.
     */
    class PGSocketIOFailure : public ExecutableContextFailure {
    private:
      boost::system::error_code error_code;
    public:
      PGSocketIOFailure(const char *errstr, const boost::system::error_code ec) throw()
        : ExecutableContextFailure(errstr)
      {
        this->error_code = ec;
      };

      PGSocketIOFailure(std::string errstr, const boost::system::error_code ec) throw()
        : ExecutableContextFailure(errstr)
      {
        this->error_code = ec;
      };
    };

    /**
     * An abstract interface for executable contexts providing access to
     * network I/O, suitable to be used by PostgreSQL protocol implementations.
     *
     * Private callbacks for handling incoming/outgoing messages must be initialized
     * by the I/O context descendant.
     *
     * A PGSocketIOContextInterface provides a full implementation of
     * read/write actions for the following parts of the PostgreSQL protocol:
     *
     * - initial_read(): Reads header of startup message
     *                   This is the first operation on a fresh IO Context instance.
     *                   The startup message header is read into the read_header_buffer
     *                   ProtocolBuffer.
     *
     * - initial_read_body(): Reads the startup message body. The content of the startup
     *                        message is stored in the read_body_buffer ProtocolBuffer.
     *
     * - start_read_header(): This needs to be called first to read new message
     *                        from the socket. The message header is then placed into
     *                        the read_buffer_header ProtocolBuffer.
     *
     * - start_read_body(): Reads the protocol message body into the read_body_buffer
     *                      ProtocolBuffer
     *
     * - start_write(): Write the content of the write_buffer ProtocolBuffer to
     *                  the socket.
     *
     * The message handling is controlled by abstract callback handlers which are called
     * by the I/O methods above, as follows:
     *
     * - initial_read()      -> startup_msg_in()
     * - initial_read_body() -> startup_msg_body()
     * - start_read_header() -> pgproto_header_in()
     * - start_read_msg()    -> pgproto_msg_in()
     * - start_write()       -> pgproto_msg_out()
     *
     * Those callbacks implement specific protocol message processing, though not every
     * callback needs a specific implementation for every use case. For example,
     * COPY subprotocol doesn't need startup message processing, thus handlers
     * implementing that functionality just need to provide an empty callback
     * in this case.
     */
    class PGSocketIOContextInterface : public ExecutableContext {
    protected:

      ExecutableContextName name = EXECUTABLE_CONTEXT_SOCKET_IO;
      boost::asio::ip::tcp::socket *soc = nullptr;

      /**
       * Internal callback handler for outgoing protocol messages
       */
      virtual void pgproto_msg_out(const boost::system::error_code &ec) = 0;

      /**
       * Internal callback handler for handling incoming protocol message
       * headers.
       */
      virtual void pgproto_header_in(const boost::system::error_code& ec,
                                     std::size_t len) = 0;

      /**
       * Internal callback handler for incoming protocol message bodies.
       */
      virtual void pgproto_msg_in(const boost::system::error_code& ec,
                                  std::size_t len) = 0;

      /**
       * Internal callback handler for startup message headers.
       */
      virtual void startup_msg_in(const boost::system::error_code& ec,
                                  std::size_t len) = 0;

      /**
       * Internal callback handler for protocol startup message headers.
       */
      virtual void startup_msg_body(const boost::system::error_code& ec,
                                    std::size_t len) = 0;

    public:

      /*
       * I/O buffers for protocol communication.
       */
      ProtocolBuffer write_buffer;
      ProtocolBuffer read_header_buffer;
      ProtocolBuffer read_body_buffer;

      PGSocketIOContextInterface();
      PGSocketIOContextInterface(boost::asio::ip::tcp::socket *soc);
      virtual ~PGSocketIOContextInterface();

      /**
       * Returns the internal pointer to a socket instance used by an
       * I/O context object.
       *
       * NOTE: since the socket instance is maintained by the caller, care
       *       must be taken, since the pointer could already be invalid.
       *       An I/O context doesn't ensure that a returned socket pointer
       *       remains valid throughout its lifetime.
       */
      virtual boost::asio::ip::tcp::socket *socket();

      /*
       * Routines for socket I/O
       */

      /**
       * Writes the associated protocol buffer contents to the internal socket.
       *
       * Calls the callback method pgproto_msg_out(), which needs to be
       * implemented by descendants.
       */
      virtual void start_write();

      /**
       * Starts reading the postgresql protocol message header
       * from the current socket.
       *
       * If succesful, the protocol header contents are available via
       * read_header_buffer ProtocolBuffer instance associated with
       * this I/O context.
       *
       * This method calls the abstract callback method
       * pgproto_header_in(), which can handle the protocol
       * message header accordingly.
       */
      virtual void start_read_header();

      /**
       * Starts reading a postgresql protocol message body from
       * the current socket.
       *
       * If successful, the protocol message body contents are
       * available via the read_msg_buffer ProtocolBuffer instance associated
       * with this I/O context.
       *
       * start_read_msg() calls the callback method pgproto_msg_in().
       */
      virtual void start_read_msg();

      /**
       * First read attempt on a socket interface, used for a PostgreSQL
       * protocol implementation. initial_read() reads the Startup Message
       * header received from the client and initiates the connection startup.
       */
      virtual void initial_read();

      /**
       * Reads startup message body from the current socket.
       */
      virtual void initial_read_body();

    };

  }
}

#endif
