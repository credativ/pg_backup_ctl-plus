#ifndef __PIPED_ARCHIVE__
#define __PIPED_ARCHIVE__

#include <fs-archive.hxx>
#include <jobhandles.hxx>

namespace credativ {

  /**
   * A special archive file which just
   * represents a pipe communicating with
   * a subprocess doing the actual legwork.
   *
   * When opened, this class forks and uses
   * the specified processTitle() to execute and open
   * a pipe for communication. The goal is to write to STDOUT
   * and consume this input from STDOUT on the other side(TM).
   *
   * There's also a reverse channel opened for read().
   */
  class ArchivePipedProcess : public BackupFile {
  protected:

    /**
     * PID of piped executable.
     */
    pid_t pid = -1;

    /**
     * Internal job descriptor, controls
     * settings for the background job.
     */
    job_info jobDescr;

    /*
     * Set to true in case the pipe is open
     * and fully initialized.
     */
    bool opened = false;

  public:
    ArchivePipedProcess(path pathHandle);
    ArchivePipedProcess(path pathHandle, string executable, vector<string> execArgs);
    virtual ~ArchivePipedProcess();

    virtual void open();
    virtual size_t write(const char *buf, size_t len);
    virtual bool isOpen();
  };


}

#endif
