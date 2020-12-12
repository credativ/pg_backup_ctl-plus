#ifndef __HAVE_OUTPUT_HXX__
#define __HAVE_OUTPUT_HXX__

#include <boost/property_tree/ptree.hpp>

#include <descr.hxx>
#include <rtconfig.hxx>
#include <BackupCatalog.hxx>

using namespace credativ;

namespace credativ {

  /**
   * Format options descriptor.
   *
   * This is just an ancestor implementation of
   * our RuntimeConfiguration class.
   */
  class OutputFormatConfiguration : public RuntimeConfiguration {
  public:

    OutputFormatConfiguration() {};
    virtual ~OutputFormatConfiguration() {};

  };

  /**
   * This is an abstract base class for output formatting
   *
   * Handles descriptor instances and converts them to the
   * output format implemented by its descendants.
   *
   * Transforming basebackup, catalog and other objects information
   * into a string representation we'd like to print to users
   * is tricky: they usually involve multiple different information sites,
   * e.g catalog information for basebackups et al. This requires
   * formatter instances to do catalog lookups itself.
   *
   * Because of the reason mentioned above, each formatter instance gets
   * a BackupCatalog shared pointer instance during creation, so it is able
   * to perform lookups and gather all the information we'd like to print
   * for a given object.
   *
   * Additionally each formatter needs to know for which archive he needs
   * to operator on, thus we also require a catalog descriptor instance
   * which references the archive. This doesn't necessarily reference
   * a *real* archive (think of retention rules which aren't belonging
   * to an archive).
   */
  class OutputFormatter {
  protected:

    /* Internal reference to BackupCatalog */
    std::shared_ptr<BackupCatalog> catalog = nullptr;

    /* Internal reference to archive descriptor */
    std::shared_ptr<CatalogDescr> catalog_descr = nullptr;

    /* Internal reference to output format options */
    std::shared_ptr<OutputFormatConfiguration> config = nullptr;

  public:

    OutputFormatter(std::shared_ptr<OutputFormatConfiguration> config) {};
    OutputFormatter(std::shared_ptr<OutputFormatConfiguration> config,
                    std::shared_ptr<BackupCatalog> catalog,
                    std::shared_ptr<CatalogDescr> catalog_descr);
    virtual ~OutputFormatter();

    /*
     * These are abstract output methods, required to be implemented
     * by descendants.
     */
    virtual void nodeAs(std::vector<std::shared_ptr<BaseBackupDescr>> &list,
                        std::ostringstream &output) = 0;
    virtual void nodeAs(std::shared_ptr<RetentionDescr> retentionDescr,
                        std::ostringstream &output) = 0;
    virtual void nodeAs(std::vector<shm_worker_area> &slots,
                        std::ostringstream &output) = 0;
    virtual void nodeAs(std::vector<std::shared_ptr<ConnectionDescr>> connections,
                        std::ostringstream &output) = 0;
    virtual void nodeAs(std::vector<std::shared_ptr<RetentionDescr>> &retentionList,
                        std::ostringstream &output) = 0;
    virtual void nodeAs(std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> list,
                        std::ostringstream &output) = 0;
    virtual void nodeAs(std::shared_ptr<RuntimeConfiguration> rtc,
                        std::ostringstream &output) = 0;
    virtual void nodeAs(std::shared_ptr<ConfigVariable> var,
                        std::ostringstream &output) = 0;

    /**
     * Static factory method, returns an instance of
     * OutputFormatter for the specified output format.
     */
    static std::shared_ptr<OutputFormatter> formatter(std::shared_ptr<OutputFormatConfiguration> config,
                                                      std::shared_ptr<BackupCatalog> catalog,
                                                      std::shared_ptr<CatalogDescr> catalog_descr,
                                                      OutputFormatType type);

    static std::shared_ptr<OutputFormatter> formatter(std::shared_ptr<OutputFormatConfiguration> config,
                                                      std::shared_ptr<BackupCatalog> catalog,
                                                      OutputFormatType type);

  };

  class ConsoleOutputFormatter : public OutputFormatter {
  private:

    /**
     * Internal method to list backups non-verbose
     */
    void listBackups(std::vector<std::shared_ptr<BaseBackupDescr>> &list,
                     std::ostringstream &output);

    /**
     * Internal method to list backups verbose
     */
    void listBackupsVerbose(std::vector<std::shared_ptr<BaseBackupDescr>> &list,
                            std::ostringstream &output);

    /**
     * Output archive information in full or filtered mode.
     */
    void listArchiveList(std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> descr,
                         std::ostringstream &output);

    /**
     * Output archive information in detail mode.
     */
    void listArchiveDetail(std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> descr,
                           std::ostringstream &output);

  public:

    ConsoleOutputFormatter(std::shared_ptr<OutputFormatConfiguration> config,
                           std::shared_ptr<BackupCatalog> catalog,
                           std::shared_ptr<CatalogDescr> catalog_descr);
    virtual ~ConsoleOutputFormatter();

    virtual void nodeAs(std::vector<std::shared_ptr<BaseBackupDescr>> &list,
                        std::ostringstream &output);
    virtual void nodeAs(std::shared_ptr<RetentionDescr> retentionDescr,
                        std::ostringstream &output);
    virtual void nodeAs(std::vector<shm_worker_area> &slots,
                        std::ostringstream &output);
    virtual void nodeAs(std::vector<std::shared_ptr<ConnectionDescr>> connections,
                        std::ostringstream &output);
    virtual void nodeAs(std::vector<std::shared_ptr<RetentionDescr>> &retentionList,
                        std::ostringstream &output);
    virtual void nodeAs(std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> list,
                        std::ostringstream &output);
    virtual void nodeAs(std::shared_ptr<RuntimeConfiguration> rtc,
                        std::ostringstream &output);
    virtual void nodeAs(std::shared_ptr<ConfigVariable> var,
                        std::ostringstream &output);

  };

  class JsonOutputFormatter : public OutputFormatter {
  private:

    /**
     * Internal method to convert a RetentionDescr instance
     * to a json ptree representation
     */
    boost::property_tree::ptree toPtree(std::shared_ptr<RetentionDescr> retentionDescr);

    /**
     * Internal method to list backups verbose
     */
    void listBackupsVerbose(std::vector<std::shared_ptr<BaseBackupDescr>> &list,
                            std::ostringstream &output);

    /**
     * Internal method to list backups non-verbose
     */
    void listBackups(std::vector<std::shared_ptr<BaseBackupDescr>> &list,
                     std::ostringstream &output);

    /**
     * Output archive information in full or filtered mode.
     */
    void listArchiveList(std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> descr,
                         std::ostringstream &output);

    /**
     * Output archive information in detail mode.
     */
    void listArchiveDetail(std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> descr,
                           std::ostringstream &output);

  public:

    JsonOutputFormatter(std::shared_ptr<OutputFormatConfiguration> config,
                        std::shared_ptr<BackupCatalog> catalog,
                        std::shared_ptr<CatalogDescr> catalog_descr);
    virtual ~JsonOutputFormatter();

    virtual void nodeAs(std::vector<std::shared_ptr<BaseBackupDescr>> &list,
                        std::ostringstream &output);
    virtual void nodeAs(std::shared_ptr<RetentionDescr> retentionDescr,
                        std::ostringstream &output);
    virtual void nodeAs(std::vector<shm_worker_area> &slots,
                            std::ostringstream &output);
    virtual void nodeAs(std::vector<std::shared_ptr<ConnectionDescr>> connections,
                        std::ostringstream &output);
    virtual void nodeAs(std::vector<std::shared_ptr<RetentionDescr>> &retentionList,
                        std::ostringstream &output);
    virtual void nodeAs(std::shared_ptr<std::list<std::shared_ptr<CatalogDescr>>> list,
                        std::ostringstream &output);
    virtual void nodeAs(std::shared_ptr<RuntimeConfiguration> rtc,
                        std::ostringstream &output);
    virtual void nodeAs(std::shared_ptr<ConfigVariable> var,
                        std::ostringstream &output);


  };

}

#endif
