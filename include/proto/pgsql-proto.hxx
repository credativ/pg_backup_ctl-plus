#ifndef __HAVE_PGSQL_PROTO_HXX__
#define __HAVE_PGSQL_PROTO_HXX__

namespace credativ {

  namespace pgprotocol {

    typedef char PGMessageType;

    const PGMessageType AuthenticationMessageType = 'R';

  }

}

#endif
