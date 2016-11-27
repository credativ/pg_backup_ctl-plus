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

  typedef enum {
    EMPTY_CMD,
    CREATE_CMD,
    DROP_CMD,
    LIST_CMD,
    ALTER_CMD,
    VERIFY_CMD,
    CLEANUP_CMD } CmdToken;

  typedef enum {
    PROPERTY_EMPTY,
    PROPERTY_ARCHIVE_START,
    PROPERTY_ARCHIVE_NAME,
    PROPERTY_ARCHIVE_DIRECTORY,
    PROPERTY_ARCHIVE_PGHOST,
    PROPERTY_ARCHIVE_PGDATABASE,
    PROPERTY_ARCHIVE_PGUSER,
    PROPERTY_ARCHIVE_PGPORT,
    PROPERTY_VERIFY_START,
    PROPERTY_VERIFY_ARCHIVE_NAME,
    PROPERTY_BASEBACKUP_START,
    PROPERTY_BASEBACKUP_ARCHIVE_NAME,
  } CmdPropertyToken;

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
    CmdToken tag;
    CmdPropertyToken propTag;

    std::unordered_map<std::string, std::string> properties;

    PGBackupCtlCommand(CmdToken tag);
    virtual ~PGBackupCtlCommand();

    virtual bool propertyMissing(std::string key);
    virtual int getPropertyInt(std::string key)
      throw(CParserIssue);
    virtual std::string getPropertyString(std::string key)
      throw(CParserIssue);

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

    virtual void saveCommandProperty(std::string key, std::string value);

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
