#include <copyprotocolcontext.hxx>

using namespace pgbckctl::pgprotocol;

CopyContext::CopyContext() {}

CopyContext::~CopyContext() {}
/**
* Internal callback handler for outgoing protocol messages
*/
void CopyContext::pgproto_msg_out(const boost::system::error_code &ec) {

}

/**
* Internal callback handler for handling incoming protocol message
* headers.
*/
void CopyContext::pgproto_header_in(const boost::system::error_code& ec,
                              std::size_t len) {

}

/**
* Internal callback handler for incoming protocol message bodies.
*/
void CopyContext::pgproto_msg_in(const boost::system::error_code& ec,
                          std::size_t len) {

};

/**
* Internal callback handler for startup message headers.
*/
void CopyContext::startup_msg_in(const boost::system::error_code& ec,
                          std::size_t len) {

}

/**
* Internal callback handler for protocol startup message headers.
*/
void CopyContext::startup_msg_body(const boost::system::error_code& ec,
                            std::size_t len) {

}
