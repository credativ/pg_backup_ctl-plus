#ifndef __HAVE_COPY_PROTOCOL_CONTEXT_
#define __HAVE_COPY_PROTOCOL_CONTEXT_

#include <pgiosocketcontext.hxx>

namespace pgbckctl {

  namespace pgprotocol {
		/**
     * A specific executable context, describing the context
     * for protocol commands using COPY BOTH protocol actions.
     */
    class CopyContext : public PGSocketIOContextInterface {
    protected:

      /* Identifier for this context */
      ExecutableContextName name = EXECUTABLE_CONTEXT_COPY;
      /**
       * Internal callback handler for outgoing protocol messages
       */
      void pgproto_msg_out(const boost::system::error_code &ec);

      /**
       * Internal callback handler for handling incoming protocol message
       * headers.
       */
      void pgproto_header_in(const boost::system::error_code& ec,
                                     std::size_t len);

      /**
       * Internal callback handler for incoming protocol message bodies.
       */
      void pgproto_msg_in(const boost::system::error_code& ec,
                                  std::size_t len);

      /**
       * Internal callback handler for startup message headers.
       */
      void startup_msg_in(const boost::system::error_code& ec,
                                  std::size_t len);

      /**
       * Internal callback handler for protocol startup message headers.
       */
      void startup_msg_body(const boost::system::error_code& ec,
                                    std::size_t len);
    public:

      CopyContext();
      virtual ~CopyContext();

    };
	}
}

#endif