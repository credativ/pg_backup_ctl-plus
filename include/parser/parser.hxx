#ifndef __PGBACKUPCTL_PARSER__
#define __PGBACKUPCTL_PARSER__

#include <BackupCatalog.hxx>
#include <common.hxx>
#include <parser.hxx>

#include <iostream>
#include <boost/heap/binomial_heap.hpp>
#include <boost/filesystem.hpp>

using namespace credativ;

namespace credativ {

  typedef enum { 
    EMPTY_CMD,
    CREATE_CMD,
    DROP_CMD,
    LIST_CMD,
    CLEANUP_CMD } CmdToken;

  typedef enum {
    PROPERTY_EMPTY,
    PROPERTY_ARCHIVE_START,
    PROPERTY_ARCHIVE_NAME,
    PROPERTY_ARCHIVE_DIRECTORY,
    PROPERTY_ARCHIVE_PGHOST,
    PROPERTY_ARCHIVE_PGDATABASE,
    PROPERTY_ARCHIVE_PGUSER,
    PROPERTY_BASEBACKUP_START,
    PROPERTY_BASEBACKUP_ARCHIVE_NAME} CmdPropertyToken;
    

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

    PGBackupCtlCommand(CmdToken tag);
    virtual ~PGBackupCtlCommand();
  };

  class PGBackupCtlParser : CPGBackupCtlBase {
  protected:
    boost::filesystem::path sourceFile;
    std::shared_ptr<PGBackupCtlCommand> command;

    void parseArchiveCommand(boost::tokenizer<boost::char_separator<char>> tokens);
  public:

    /*
     * Public c'tor. Reads in the specified file into
     * a private string stream.
     */
    PGBackupCtlParser(std::stringstream& in);
    PGBackupCtlParser(std::string in);
    PGBackupCtlParser(boost::filesystem::path sourceFile);
    virtual ~PGBackupCtlParser();

    /*
     * Public access methods
     */
    virtual void parseFile();
    virtual void parseLine(std::string line);

  };

}

#endif
