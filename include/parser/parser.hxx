#ifndef __PGBACKUPCTL_PARSER__
#define __PGBACKUPCTL_PARSER__

#include <BackupCatalog.hxx>
#include <common.hxx>
#include <parser.hxx>

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
     * Optional initialized CatalogDescr, needed
     * by certain CmdTags...
     */
    std::shared_ptr<CatalogDescr> catalogDescr;
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
    virtual void execute(std::string catalogDir)
      throw(CPGBackupCtlFailure);
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
    virtual void parseFile() throw (CParserIssue);
    virtual void parseLine(std::string line) throw (CParserIssue);

    virtual shared_ptr<PGBackupCtlCommand> getCommand();

  };

}

#endif
