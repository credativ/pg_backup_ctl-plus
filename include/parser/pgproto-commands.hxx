#ifndef __HAVE_PGPROTO_COMMANDS__
#define __HAVE_PGPROTO_COMMANDS__

#include <memory>
#include <boost/filesystem.hpp>

#include <rtconfig.hxx>
#include <pgsql-proto.hxx>
#include <BackupCatalog.hxx>

namespace credativ {

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

    public:

      ProtocolCommandHandler(std::shared_ptr<PGProtoCmdDescr> descr,
                             std::shared_ptr<RuntimeConfiguration> rtc);
      virtual ~ProtocolCommandHandler();

      virtual std::shared_ptr<PGProtoStreamingCommand> getExecutable();

    };

    /**
     * Base class for implementing PostgreSQL Streaming
     * commands.
     */
    class PGProtoStreamingCommand : public PGProtoBufferAggregator {
    protected:

      /**
       * Reference to the command handle describing this
       * command instance.
       */
      std::shared_ptr<PGProtoCmdDescr> command_handle = nullptr;

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
       * over the wire. Usually initialized by derived classes.
       */
      std::shared_ptr<PGProtoResultSet> resultSet = nullptr;

      /**
       * Helper routine to prepare and open the backup catalog.
       *
       * The readwrite arguments configures wether the catalog
       * database will accept write transactions, in which case
       * the caller have to pass 'true'.
       */
      void openCatalog(bool readwrite = false);

      /* Command tag identifier */
      std::string command_tag = "UNKNOWN";

    public:

      PGProtoStreamingCommand(std::shared_ptr<PGProtoCmdDescr> descr,
                              std::shared_ptr<RuntimeConfiguration> rtc);
      virtual ~PGProtoStreamingCommand();

      virtual void execute() = 0;

      virtual std::string tag();
    };

    /**
     * Implements the IDENTIFY_SYSTEM streaming
     * command.
     */
    class PGProtoIdentifySystem : public PGProtoStreamingCommand {
    public:

      PGProtoIdentifySystem(std::shared_ptr<PGProtoCmdDescr> descr,
                            std::shared_ptr<RuntimeConfiguration> rtc);
      virtual ~PGProtoIdentifySystem();

      virtual void execute();

      virtual int step(ProtocolBuffer &buffer);

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
                             std::shared_ptr<RuntimeConfiguration> rtc);
      virtual ~PGProtoListBasebackups();

      virtual void execute();

      virtual int step(ProtocolBuffer &buffer);

      virtual void reset();
    };

  }
}

#endif
