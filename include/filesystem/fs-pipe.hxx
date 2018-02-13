#ifndef __PIPED_ARCHIVE__
#define __PIPED_ARCHIVE__

#include <sys/types.h>
#include <sys/wait.h>

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
    FILE *fpipe_handle = NULL;

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

    /**
     * Sets the mode for piped operations.
     *
     * Currently unused.
     */
    std::string mode = "";

  public:
    ArchivePipedProcess(path pathHandle);
    ArchivePipedProcess(path pathHandle, string executable, vector<string> execArgs);
    virtual ~ArchivePipedProcess();

    virtual void open();
    virtual size_t write(const char *buf, size_t len);
    virtual size_t read(char *buf, size_t len);
    virtual bool isOpen();
    virtual void fsync();
    virtual void rename(path& newname);
    virtual void remove();
    virtual void close();
    virtual off_t lseek(off_t offset, int whence);
    virtual void setOpenMode(std::string mode);

    /*
     * Special setter/getter methods
     */
    virtual void pushExecArgument(std::string arg);
    virtual void setExecutable(path executable,
                               bool error_if_not_exists = false);

    /**
     * Determines wether the underlying pipe
     * was opened in read or write-only mode.
     */
    virtual bool writeable();

  };


}

#endif
