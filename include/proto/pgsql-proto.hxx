#ifndef __HAVE_PGSQL_PROTO_HXX__
#define __HAVE_PGSQL_PROTO_HXX__

#define MESSAGE_DATA_LENGTH(hdr) ((hdr).length - sizeof(unsigned int))

namespace credativ {

  namespace pgprotocol {

    typedef char PGMessageType;

    const PGMessageType AuthenticationMessageType = 'R';

    struct pg_protocol_msg_header {
      PGMessageType type;
      unsigned int length;
    };

  }

}

#endif
