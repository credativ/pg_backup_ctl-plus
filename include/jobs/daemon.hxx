#include <common.hxx>
#include <BackupCatalog.hxx>

#define DAEMON_RUN 0
#define DAEMON_TERM_NORMAL 1
#define DAEMON_TERM_EMERGENCY 2
#define DAEMON_STATUS_UPDATE 3


namespace credativ {
  namespace daemon {

    struct job_info {

      /*
       * PID from fork(). 0 is the background
       * process, others launcher processes.
       */
      pid_t pid;

      /*
       * Background process detaches from
       * parent.
       */
      bool detach = false;

      /*
       * Catalog/Command descriptor.
       */
      std::shared_ptr<CatalogDescr> catalogDescr = nullptr;

    };

    /*
     * Launcher errors are mapped to
     * LauncherFailure exceptions.
     */
    class LauncherFailure : public credativ::CPGBackupCtlFailure {
    public:
      LauncherFailure(const char *errstr) throw() : CPGBackupCtlFailure(errstr) {};
      LauncherFailure(std::string errstr) throw() : CPGBackupCtlFailure(errstr) {};
    };

    pid_t launch(job_info& info);

  }
}
