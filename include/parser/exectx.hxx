#ifndef __HAVE_EXEC_CONTEXT_
#define __HAVE_EXEC_CONTEXT_

namespace credativ {

  namespace pgprotocol {

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
      EXECUTABLE_CONTEXT_COPY_BOTH,
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

    /**
     * A specific executable context, describing the context
     * for protocol commands using COPY BOTH protocol actions.
     */
    class CopyBothExecutableContext : public ExecutableContext {
    protected:

      /* Identifier for this context */
      ExecutableContextName name = EXECUTABLE_CONTEXT_COPY_BOTH;

    public:

      CopyBothExecutableContext();
      virtual ~CopyBothExecutableContext();

    };
  }

}

#endif
