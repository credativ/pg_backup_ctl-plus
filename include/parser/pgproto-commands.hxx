#ifndef __HAVE_PGPROTO_COMMANDS__
#define __HAVE_PGPROTO_COMMANDS__

#include <memory>
#include <boost/filesystem.hpp>

#include <rtconfig.hxx>
#include <descr.hxx>
#include <exectx.hxx>
#include <shm.hxx>
#include <pgsql-proto.hxx>
#include <proto-descr.hxx>
#include <proto-catalog.hxx>

namespace pgbckctl {

  namespace pgprotocol {

    /*
     * Forwarded declarations.
     */
    class PGProtoStreamingCommand;
    class PGProtoListBasebackups;
    class PGProtoIdentifySystem;

    /**
     * Command execution failures.
     */
    class PGProtoCmdFailure : public std::exception {
    protected:

      std::string errstr;

    public:

      PGProtoCmdFailure(const char *err) throw() : errstr() {
        errstr = err;
      }

      PGProtoCmdFailure(std::string err) throw() : errstr() {
        errstr = err;
      }

      virtual ~PGProtoCmdFailure() throw() {}

      const char *what() const throw() {
        return errstr.c_str();
      }

    };

    /**
     * A ProtocolCommandHandler is instantiated from
     * a PGProtoCmdDescr and returns an executable
     * command object representing the properties
     * specified by the reference PGProtoCmdDescr.
     */
    class ProtocolCommandHandler {
    protected:

      /**
       * Reference to the command descriptor a command
       * handler was instantiated from.
       */
      std::shared_ptr<PGProtoCmdDescr> cmdDescr = nullptr;

      /**
       * The runtime configuration reference attached
       * to the command.
       */
      std::shared_ptr<RuntimeConfiguration> runtime_configuration = nullptr;

      /**
       * Reference to global catalog access handler.
       */
      std::shared_ptr<PGProtoCatalogHandler> catalogHandler = nullptr;

    public:

      ProtocolCommandHandler(std::shared_ptr<PGProtoCmdDescr> descr,
                             std::shared_ptr<PGProtoCatalogHandler> catalogHandler,
                             std::shared_ptr<RuntimeConfiguration> rtc);
      virtual ~ProtocolCommandHandler();

      virtual std::shared_ptr<PGProtoStreamingCommand> getExecutable(std::shared_ptr<WorkerSHM> worker_shm);

    };

    /**
     * Base class for implementing PostgreSQL Streaming
     * commands.
     */
    class PGProtoStreamingCommand : public PGProtoBufferAggregator {
    protected:

      /**
       * Executable context this command should run within.
       */
      ExecutableContextName executable_context_name = EXECUTABLE_CONTEXT_DEFAULT;

      /**
       * A reference to the worker shared memory handle.
       * Initialized in case a command needs shared memory access.
       */
      std::shared_ptr<WorkerSHM> worker_shm = nullptr;

      /**
       * Reference to the command handle describing this
       * command instance.
       */
      std::shared_ptr<PGProtoCmdDescr> command_handle = nullptr;

      /**
       * Catalog handler for database access routines.
       */
      std::shared_ptr<PGProtoCatalogHandler> catalogHandler = nullptr;

      /**
       * Handle to runtime configuration settings.
       */
      std::shared_ptr<RuntimeConfiguration> runtime_configuration = nullptr;

      /**
       * Internal backup catalog handle.
       *
       * This is usually initialized by calling openCatalog(), otherwise
       * the catalog is undefined.
       *
       * Please note that the recovery stream descriptor should
       * carry a full qualified database name.
       */
      std::shared_ptr<BackupCatalog> catalog = nullptr;

      /**
       * A result set suitable to form a
       * PostgreSQL wire-compatible set to be sent
       * over the wire. Usually initialized by derived classes
       * after calling execute().
       */
      std::shared_ptr<PGProtoResultSet> resultSet = nullptr;

      /**
       * Indicates whether this command instance needs
       * direct archive access. This means that the caller needs
       * to set the basebackup identifier to this command instance,
       * otherwise the command refuses to work.
       */
      bool needs_archive_access = false;

      /**
       * Helper routine to prepare and open the backup catalog.
       *
       * The readwrite arguments configures whether the catalog
       * database will accept write transactions, in which case
       * the caller have to pass 'true'.
       */
      void openCatalog(bool readwrite = false);

      /* Command tag identifier */
      std::string command_tag = "UNKNOWN";

    public:

      PGProtoStreamingCommand(std::shared_ptr<PGProtoCmdDescr> descr,
                              std::shared_ptr<PGProtoCatalogHandler> catalogHandler,
                              std::shared_ptr<RuntimeConfiguration> rtc,
                              std::shared_ptr<WorkerSHM> worker_shm);
      virtual ~PGProtoStreamingCommand();

      /**
       * Returns the identifier of the executable context required
       * by a streaming command.
       */
      virtual ExecutableContextName getExecutableContextName();

      /**
       * Abstract method, implementing the leg work of this command.
       */
      virtual void execute(std::shared_ptr<ExecutableContext> context) = 0;

      /**
       * Protocol execution steps.
       */
      virtual int step(ProtocolBuffer &buffer);

      /**
       * Returns true if the command handle required backup/archive access.
       */
      virtual bool needsArchive();

      /**
       * The command tag this command implements.
       */
      virtual std::string tag();

    };

    /**
     * Implements the IDENTIFY_SYSTEM streaming
     * command.
     */
    class PGProtoIdentifySystem : public PGProtoStreamingCommand {
    public:

      PGProtoIdentifySystem(std::shared_ptr<PGProtoCmdDescr> descr,
                            std::shared_ptr<PGProtoCatalogHandler> catalogHandler,
                            std::shared_ptr<RuntimeConfiguration> rtc,
                            std::shared_ptr<WorkerSHM> worker_shm);

      virtual ~PGProtoIdentifySystem();

      /**
       * Does the legwork for IDENTIFY_SYSTEM commands.
       */
      virtual void execute(std::shared_ptr<ExecutableContext> context);

      /**
       * Resets internal protocol steps.
       */
      virtual void reset();
    };

    /**
     * Implements the TIMELINE_HISTORY streaming command.
     */
    class PGProtoTimelineHistory : public PGProtoStreamingCommand {
    public:

      PGProtoTimelineHistory(std::shared_ptr<PGProtoCmdDescr> descr,
                             std::shared_ptr<PGProtoCatalogHandler> catalogHandler,
                             std::shared_ptr<RuntimeConfiguration> rtc,
                             std::shared_ptr<WorkerSHM> worker_shm);

      virtual ~PGProtoTimelineHistory();

      /**
       * Executes the leg work for TIMELINE_HISTORY commands.
       */
      virtual void execute(std::shared_ptr<ExecutableContext> context);

      /**
       * Resets internal protocol steps.
       */
      virtual void reset();

    };

    /**
     * Implements the pg_backup_ctl++ specific
     * LIST_BASEBACKUPS streaming command.
     */
    class PGProtoListBasebackups : public PGProtoStreamingCommand {
    private:

      /**
       * Pointer to the archive descriptor we're currently
       * operating on. This is initialized by execute().
       */
      std::shared_ptr<CatalogDescr> archive_descr = nullptr;

      /**
       * Helper routine, queries the backup catalog for a list
       * of valid basebackups.
       *
       * This routine prepares a complete PGProtoResultSet suitable
       * to be sent over the wire.
       */
      void prepareListOfBackups();

    public:

      PGProtoListBasebackups(std::shared_ptr<PGProtoCmdDescr> descr,
                             std::shared_ptr<PGProtoCatalogHandler> catalogHandler,
                             std::shared_ptr<RuntimeConfiguration> rtc,
                             std::shared_ptr<WorkerSHM> worker_shm);

      virtual ~PGProtoListBasebackups();

      /**
       * Executes the leg work for LIST_BASEBACKUPS commands.
       */
      virtual void execute(std::shared_ptr<ExecutableContext> context);

      /**
       * Resets the internal protocol steps.
       */
      virtual void reset();
    };

  }
}

#endif
