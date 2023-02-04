#ifndef __HAVE_BASEBACKUPMSG_HXX
#define __HAVE_BASEBACKUPMSG_HXX

/* PostgreSQL client API */
extern "C" {
#include <libpq-fe.h>
}

#include <common.hxx>
#include <proto-buffer.hxx>

namespace pgbckctl {

  /**
   * BaseBackup Message Types received via the streaming
   * replication protocol.
   */
  typedef enum {

    BBMSG_TYPE_UNKNOWN,
    BBMSG_TYPE_ARCHIVE_START,
    BBMSG_TYPE_MANIFEST_START,
    BBMSG_TYPE_DATA,
    BBMSG_TYPE_PROGRESS

  } BaseBackupMsgType;

  /**
   * BaseBackupMessage
   *
   * Starting with PostgreSQL 15, the BASE_BACKUP streaming command
   * encodes various message types within CopyOutResponse payloads.
   *
   * This class is the base class for decoding and representing the contents
   * of these payloads and access its properties.
   */
  class BaseBackupMessage {
  protected:
    std::shared_ptr<ProtocolBuffer> msg = nullptr;
    BaseBackupMsgType kind = BBMSG_TYPE_UNKNOWN;

    /* internal initialization routine, needs to be implemented by each
     * descendant */
    virtual void read() = 0;

    /* operator assign method */
    virtual void assign(ProtocolBuffer &srcbuffer);
    virtual void assign(std::shared_ptr<ProtocolBuffer> &srcbuffer);

  public:

    /**
     * Constructs a new BaseBackupMessage descendant from the specified ProtocolBuffer.
     *
     * @param msgbuf A shared pointer to a ProtocolBuffer instance. The buffer managed
     * by this shared pointer is not copied, we just assign the pointer itself.
     */
    explicit BaseBackupMessage(std::shared_ptr<ProtocolBuffer> msgbuf);

    /**
     * Constructor to instantiate a descendant BaseBackupMessage
     *
     * @param buffer Buffer to copy. The contents of this buffer are copied into
     * an internal ProtocolBuffer.
     * @param sz Size of buffer
     */
    explicit BaseBackupMessage(char *buffer, size_t sz);
    virtual ~BaseBackupMessage();

    /**
     * @ref data()
     * @return A pointer into the data payload of this message.
     * @note Please note that the pointer is only valid as long as the message
     * buffer exists. The caller either needs to copy the data or needs to make sure
     * that the msgbuf object lives in the same context.
     */
    char *data();

    /**
     * Returns the size of the buffer pointed to by the pointer returned by @ref data()
     * @return
     */
    virtual size_t dataSize();

    /**
     * Returns a reference to the internal protocol buffer.
     *
     * @return shared pointer reference to internal buffer.
     */
    virtual std::shared_ptr<ProtocolBuffer> buffer();

    /**
     * Factory method to instantiate a corresponding kind of BaseBackupMessage.
     *
     * Returns a valid ancestor of BaseBackupMessage instance representing the kind
     * of message. Throws in case of unknown message kind.
     */
    static std::shared_ptr<BaseBackupMessage> message(char *buffer, size_t sz);

    /**
     * Returns kind of the message
     */
    virtual BaseBackupMsgType msgType();

  };

  /**
   * Archive or manifest data message
   */
  class BaseBackupDataMsg : public BaseBackupMessage {
  private:
    void read_internal();
  protected:
    void read() override;
  public:

    /**
     * Constructs a new BaseBackupDataMsg from the specified ProtocolBuffer.
     *
     * @param msgbuf A shared pointer to a ProtocolBuffer instance. The buffer managed
     * by this shared pointer is not copied, we just assign the pointer itself.
     */
    explicit BaseBackupDataMsg(std::shared_ptr<ProtocolBuffer> msgbuf);

    /**
     * Constructs a new BaseBackupDataMsg from the specified buffer.
     *
     * @param buffer Buffer to copy. The contents of this buffer are copied into
     * an internal ProtocolBuffer.
     * @param sz Size of buffer
     */
    explicit BaseBackupDataMsg(char *buffer, size_t sz);
    ~BaseBackupDataMsg() override;

  };

  class BaseBackupArchiveStartMsg : public BaseBackupMessage {
  private:

    /* Archive name */
    std::string archive_name = "";

    /* Tablespace location */
    std::string tblspc_location = "";

    /*
     * Read character byte for byte from memory buffer until
     * a nullbyte is reached, rise rince repeat unless all
     * string properties are correctly initialized.
     */
    void readStringPropertiesFromMsg();
    void read_internal();

  protected:

    /*
     * Read archive name and full tablespace from the message
     * and initialize properties accordingly
     */
    void read() override;

  public:

    /**
     * Constructs a new BaseBackupArchiveDataMsg from the specified ProtocolBuffer.
     *
     * @param msgbuf A shared pointer to a ProtocolBuffer instance. The buffer managed
     * by this shared pointer is not copied, we just assign the pointer itself.
     */
    explicit BaseBackupArchiveStartMsg(std::shared_ptr<ProtocolBuffer> msgbuf);

    /**
     * Constructs a new BaseBackupArchiveStartMsg from the specified buffer.
     *
     * @param buffer Buffer to copy. The contents of this buffer are copied into
     * an internal ProtocolBuffer.
     * @param sz Size of buffer
     */
    explicit BaseBackupArchiveStartMsg(char *buffer, size_t sz);

    ~BaseBackupArchiveStartMsg() override;

    /**
     * @return Archive name of this message object
     */
    virtual std::string getArchiveName();


    /**
     * @return The location of the tablespace or empty string.
     *
     * @note An empty tablespace location usually indicates that
     * this BaseBackupArchiveStartMsg belongs to the default tablespace.
     */
    virtual std::string getLocation();

  };

  class BaseBackupManifestStartMsg : public BaseBackupMessage {
  private:
    void read_internal();
  protected:

    /* A manifest starting messages doesn't have any particular
     * payload, so read() is basically a no-op here.
     */
    void read() override;

  public:

    /**
     * Constructs a new BaseBackupManifestStartMsg from the specified ProtocolBuffer.
     *
     * @param msgbuf A shared pointer to a ProtocolBuffer instance. The buffer managed
     * by this shared pointer is not copied, we just assign the pointer itself.
     */
    explicit BaseBackupManifestStartMsg(std::shared_ptr<ProtocolBuffer> msgbuf);

    /**
     * Constructs a new BaseBackupManifestStartMsg from the specified buffer.
     *
     * @param buffer Buffer to copy. The contents of this buffer are copied into
     * an internal ProtocolBuffer.
     * @param sz Size of buffer
     */
    explicit BaseBackupManifestStartMsg(char *buffer, size_t sz);
    ~BaseBackupManifestStartMsg() override;

  };

  class BaseBackupProgressMsg : public BaseBackupMessage {
  private:

    /* Number of bytes encapsulated by progress message */
    int64_t val = 0;

    /**
     * Implements guts of overridden method read()
     */
    void read_internal();

  protected:

    /* Initializes properties from the internal memory buffer */
    void read() override;

  public:

    /**
     * Constructs a new BaseBackupProgressMsg from the specified ProtocolBuffer.
     *
     * @param msgbuf A shared pointer to a ProtocolBuffer instance. The buffer managed
     * by this shared pointer is not copied, we just assign the pointer itself.
     */
    explicit BaseBackupProgressMsg(std::shared_ptr<ProtocolBuffer> msgbuf);

    /**
     * Constructs a new BaseBackupProgressMsg from the specified buffer.
     *
     * @param buffer Buffer to copy. The contents of this buffer are copied into
     * an internal ProtocolBuffer.
     * @param sz Size of buffer
     */
    explicit BaseBackupProgressMsg(char *buffer, size_t sz);
    ~BaseBackupProgressMsg() override;

    /**
     * Returns the current bytes retrieved within the current archive.
     */
     int64_t getProgressBytes();

  };

}

#endif
