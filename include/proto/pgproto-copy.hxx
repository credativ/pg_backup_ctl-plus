#ifndef __HAVE_PG_PROTO_COPY__
#define __HAVE_PG_PROTO_COPY__

#include <string>
#include <pgsql-proto.hxx>
#include <pgbckctl_exception.hxx>

using namespace credativ;
using namespace credativ::pgprotocol;

namespace credativ {

  /**
   * Copy subprotocol exception class.
   */
  class CopyProtocolFailure : public CPGBackupCtlFailure {
  public:

    CopyProtocolFailure(const char *errString) throw()
      : CPGBackupCtlFailure(errString) {}

    CopyProtocolFailure(std::string errString) throw()
      : CPGBackupCtlFailure(errString) {}

    virtual ~CopyProtocolFailure() throw() {}

    const char *what() const throw() {
      return errstr.c_str();
    }

  };

  /**
   * Base class for PostgreSQL COPY sub-protocol
   * implementations.
   */
  class PGProtoCopy {
  protected:

    ProtocolBuffer *buf = nullptr;

  public:

    PGProtoCopy(ProtocolBuffer *buf);
    virtual ~PGProtoCopy();

    virtual void begin() = 0;
    virtual void end() = 0;

  };

  class PGProtoCopyIn : public PGProtoCopy {
  public:

    PGProtoCopyIn(ProtocolBuffer *buf);
    virtual ~PGProtoCopyIn();

    virtual void begin();
    virtual void end();

  };

}

#endif
