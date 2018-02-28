#ifndef __PGBACKUPCTL_PARSER__
#define __PGBACKUPCTL_PARSER__

#include <BackupCatalog.hxx>
#include <common.hxx>
#include <parser.hxx>
#include <signalhandler.hxx>

#include <iostream>
#include <unordered_map>
#include <boost/heap/binomial_heap.hpp>
#include <boost/filesystem.hpp>

using namespace credativ;

namespace credativ {

  /*
   * Base archive exception.
   */
  class CParserIssue : public CPGBackupCtlFailure {
  public:
    CParserIssue(const char *errstr) throw() : CPGBackupCtlFailure(errstr) {};
    CParserIssue(string errstr) throw() : CPGBackupCtlFailure(errstr) {};
  };

  /*
   * Encapsulates command properties.
   */
  class PGBackupCtlCommand {
  private:
    /*
     * Catalog descriptor wraps internal
     * command information. This descriptor
     * is the template for building up the
     * executable descriptor objects dispatched by
     * getExecutableDescr().
     *
     * See commands.cxx for details.
     */
    std::shared_ptr<CatalogDescr> catalogDescr = nullptr;

    /**
     * Internal references to signal handler instances. Currently
     * supported singal handlers are:
     *
     * - Stop signal (TERM, INT) handlers
     */
    JobSignalHandler *stopHandler = nullptr;
    JobSignalHandler *intHandler  = nullptr;
  public:

    PGBackupCtlCommand(CatalogTag tag);
    PGBackupCtlCommand(CatalogDescr descr);
    virtual ~PGBackupCtlCommand();

    /*
     * Create an executable catalog descr based on the current
     * state of command properties.
     */

    virtual shared_ptr<CatalogDescr> getExecutableDescr();

    /*
     * executes the command handle.
     */
    virtual CatalogTag execute(std::string catalogDir);

    /**
     * Assigns a stop signal handler to a command instance.
     * This should be used to react on SIGTERM signals.
     */
    virtual void assignSigStopHandler(JobSignalHandler *handler);

    /*
     * Assigns an interruption signal handler to a command instance.
     * This should be used to react on SIGINT signals
     */
    virtual void assignSigIntHandler(JobSignalHandler *handler);

    /**
     * Returns the command tag of this handler.
     *
     * EMPTY_DESCR is returned in case this handler
     * doesn't hold a valid command handle yet.
     */
    virtual CatalogTag getCommandTag();

    /**
     * Returns the archive name of a handler instance, if
     * attached to a archive. If no archive is attached, an empty string
     * is returned.
     *
     * An empty string is also returned, of the catalog descriptor is EMPTY_DESCR
     * or not yet initialized.
     */
    virtual std::string archive_name();
  };

  class PGBackupCtlParser : CPGBackupCtlBase {
  protected:

    boost::filesystem::path sourceFile;
    std::shared_ptr<PGBackupCtlCommand> command;

  public:

    /*
     * Public c'tors.
     */
    PGBackupCtlParser();
    PGBackupCtlParser(boost::filesystem::path sourceFile);
    virtual ~PGBackupCtlParser();

    /*
     * Public access methods
     */
    virtual void parseFile();
    virtual void parseLine(std::string line);

    virtual shared_ptr<PGBackupCtlCommand> getCommand();

  };

}

#endif
