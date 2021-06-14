#ifndef __HAVE_EXEC_CONTEXT_
#define __HAVE_EXEC_CONTEXT_

#include <pgbckctl_exception.hxx>
#include <proto-buffer.hxx>
#include <exectx.hxx>

namespace pgbckctl {

  namespace pgprotocol {

    class ExecutableContext;

    /**
     * ExecutableContextException
     *
     * Usually thrown if setting up of an ErrorContext
     * has failed.
     */
    class ExecutableContextFailure : public CPGBackupCtlFailure {
    public:
      ExecutableContextFailure(const char *errstr) throw() : CPGBackupCtlFailure(errstr) {};
      ExecutableContextFailure(std::string errstr) throw() : CPGBackupCtlFailure(errstr) {};
    };

    /**
     * Defines executable context identifiers.
     *
     * A protocol command must define an executable context
     * which it wants to use within a PGProtoStreamingServer instance.
     *
     * The identifier is then used by the executableContext() factory
     * method of a PGProtoStreamingServer instance to create the
     * requested context to successfully execute the protocol command.
     */
    typedef enum {
      EXECUTABLE_CONTEXT_DEFAULT,
      EXECUTABLE_CONTEXT_COPY,
      EXECUTABLE_CONTEXT_SOCKET_IO,
      EXECUTABLE_CONTEXT_ERROR
    } ExecutableContextName;

    /**
     * ExecutableContext base class.
     *
     * This implements a default context for protocol commands,
     * identified by EXECUTABLE_CONTEXT_DEFAULT.
     *
     * Specific executable contexts should inherit from this
     * base class.
     */
    class ExecutableContext {
    protected:

      /** Identifier for this context */
      ExecutableContextName name = EXECUTABLE_CONTEXT_DEFAULT;
    public:

      ExecutableContext();
      virtual ~ExecutableContext();

      /**
       * Returns the identifier of this context.
       */
      virtual ExecutableContextName getName();

      /**
       * Factory method.
       */
      static std::shared_ptr<ExecutableContext> create(ExecutableContextName name);

    };

  }

}

#endif
