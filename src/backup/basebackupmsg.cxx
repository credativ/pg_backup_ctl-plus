#include <basebackupmsg.hxx>
#include <stream.hxx>

using namespace pgbckctl;

/* ************************************************************************************************
 * Implementation of BaseBackupMessage
 * ********************************************************************************************** */

BaseBackupMessage::BaseBackupMessage(std::shared_ptr<ProtocolBuffer> msgbuf) {

  /* Established connection required */
  if (msgbuf == nullptr) {
    throw StreamingFailure("cannot create basebackup message from undefined buffer");
  }

  if (msgbuf->getSize() <= 0) {
    throw StreamingFailure("cannot build basebackup message object from empty buffer");
  }

  msg = msgbuf;
  
}

BaseBackupMessage::BaseBackupMessage(char *buffer, size_t sz) {

  /* sanity checks */
  if (buffer == nullptr) {
    throw StreamingFailure("cannot create basebackup message from undefined buffer");
  }

  if (sz <= 0) {
    throw StreamingFailure("cannot build basebackup message object from empty buffer");
  }

  this->msg = std::make_shared<ProtocolBuffer>();
  this->msg->assign(buffer, sz);

}

BaseBackupMessage::~BaseBackupMessage() noexcept {}

BaseBackupMsgType BaseBackupMessage::msgType() {
  return kind;
}

void BaseBackupMessage::assign(std::shared_ptr<ProtocolBuffer> &srcbuffer) {

  /*
   * We just copy the shared pointer and are done.
   * This should increment the reference counter accordingly, so we're
   * safe if the caller context decrements it. We can just use direct
   * assignment here.
   */
  this->msg = srcbuffer;

}

void BaseBackupMessage::assign(ProtocolBuffer &srcbuffer) {

  /*
   * There is no LHS with a shared_ptr type declaration here,
   * so use assign() directly
   */
  this->msg->assign(srcbuffer.ptr(), srcbuffer.getSize());

}

size_t BaseBackupMessage::dataSize() {
  return msg->getSize() - 1;
}

std::shared_ptr<ProtocolBuffer> BaseBackupMessage::buffer() {
  return msg;
}

char * BaseBackupMessage::data() {

  /* first byte is kind */
  return msg->ptr() + 1;

}

std::shared_ptr<BaseBackupMessage>
BaseBackupMessage::message(char *buffer,
                           size_t sz) {

  std::shared_ptr<BaseBackupMessage> result = nullptr;

  if (buffer == nullptr) {
    throw StreamingFailure("could not instantiate message object from undefined "
                           "message");
  }

  char kind = buffer[0];

  switch(kind) {
    case 'n': {

      /* Starting of a new archive */

      result = std::make_shared<BaseBackupArchiveStartMsg>(buffer, sz);
      break;

    }

    case 'm': {

      /* Starting of manifest */
      result = std::make_shared<BaseBackupManifestStartMsg>(buffer, sz);
      break;

    }

    case 'd': {

      /* Data message, either manifest or archive data */
      result = std::make_shared<BaseBackupDataMsg>(buffer, sz);
      break;

    }

    case 'p': {

      /* Progress status message */
      result = std::make_shared<BaseBackupProgressMsg>(buffer, sz);
      break;

    }

    default: {

      std::stringstream oss;
      oss << "invalid message kind: \"" << kind << "\"";
      throw StreamingFailure(oss.str());
      break; /* cosmetic */

    }
  }

  return result;

}

/* ************************************************************************************************
 * Implementation of BaseBackupDataMsg
 * ********************************************************************************************** */

BaseBackupDataMsg::BaseBackupDataMsg(std::shared_ptr<ProtocolBuffer> msgbuf)
  : BaseBackupMessage(msgbuf) {

  this->kind = BBMSG_TYPE_DATA;
  read_internal();

}

BaseBackupDataMsg::BaseBackupDataMsg(char *buffer, size_t sz) : BaseBackupMessage(buffer, sz) {

  this->kind = BBMSG_TYPE_DATA;
  read_internal();

}

BaseBackupDataMsg::~BaseBackupDataMsg() noexcept {}

void BaseBackupDataMsg::read_internal() {   /* noop */ }

void BaseBackupDataMsg::read() {

  read_internal();

}

/* ************************************************************************************************
 * Implementation of BaseBackupArchiveStartMsg
 * ********************************************************************************************** */

BaseBackupArchiveStartMsg::BaseBackupArchiveStartMsg(std::shared_ptr<ProtocolBuffer> msgbuf)
  : BaseBackupMessage(msgbuf) {

  this->kind = BBMSG_TYPE_ARCHIVE_START;
  read_internal();

}

BaseBackupArchiveStartMsg::BaseBackupArchiveStartMsg(char *buffer, size_t sz)
        : BaseBackupMessage(buffer, sz) {

  kind = BBMSG_TYPE_ARCHIVE_START;
  read_internal();

}

BaseBackupArchiveStartMsg::~BaseBackupArchiveStartMsg() noexcept {}

void BaseBackupArchiveStartMsg::read_internal() {
  this->readStringPropertiesFromMsg();
}

void BaseBackupArchiveStartMsg::read() {
  read_internal();
}

std::string BaseBackupArchiveStartMsg::getArchiveName() {
  return this->archive_name;
}

std::string BaseBackupArchiveStartMsg::getLocation() {
  return this->tblspc_location;
}

void BaseBackupArchiveStartMsg::readStringPropertiesFromMsg() {

  /* First byte is the message byte */
  size_t pos = 1;

  /*
   * The message payload is encoded currently like this:
   *
   * First string is the archive name
   * \0
   * Tablespace location path.
   *
   * The latter can be empty, so we must be prepared to read two nullbyte
   * sequences in a row.
   */
  bool got_archive_name = false;

  while(pos <= this->msg->getSize()) {

    char b;
    this->msg->read(&b, 1, pos++);

    if (got_archive_name && (b == '\0')) {

      /*
       * If we have read the archive name already,
       * this indicates end of message
       */
      break;

    } else if (!got_archive_name && (b == '\0')) {

      /* Archive name input bytes end */
      got_archive_name = true;
      continue;

    }

    if (got_archive_name)
      this->tblspc_location += b;
    else
      this->archive_name += b;

  }

}

/* ************************************************************************************************
 * Implementation of BaseBackupManifestStartMsg
 * ********************************************************************************************** */

BaseBackupManifestStartMsg::BaseBackupManifestStartMsg(std::shared_ptr<ProtocolBuffer> msgbuf)
  : BaseBackupMessage(msgbuf) {

  this->kind = BBMSG_TYPE_MANIFEST_START;
  read_internal();

}

BaseBackupManifestStartMsg::~BaseBackupManifestStartMsg() noexcept {}

void BaseBackupManifestStartMsg::read_internal() {}

void BaseBackupManifestStartMsg::read() {
  read_internal();
}

BaseBackupManifestStartMsg::BaseBackupManifestStartMsg(char *buffer, size_t sz)
  : BaseBackupMessage(buffer, sz) {

  kind = BBMSG_TYPE_MANIFEST_START;

}

/* ************************************************************************************************
 * Implementation of BaseBackupProgressMsg
 * ********************************************************************************************** */

BaseBackupProgressMsg::BaseBackupProgressMsg(std::shared_ptr<ProtocolBuffer> msgbuf)
  : BaseBackupMessage(msgbuf) {

  this->kind = BBMSG_TYPE_PROGRESS;
  read_internal();

}

BaseBackupProgressMsg::BaseBackupProgressMsg(char *buffer, size_t sz)
        : BaseBackupMessage(buffer, sz) {

  kind = BBMSG_TYPE_PROGRESS;
  read_internal();

}

BaseBackupProgressMsg::~BaseBackupProgressMsg() noexcept {}

void BaseBackupProgressMsg::read_internal() {

  /* First byte was already checked by constructor, so proceed
   * at the right offset to extract the current progress position.
   */
  this->msg->read(&(this->val), sizeof(int64_t), 1);

}

void BaseBackupProgressMsg::read() {
  read_internal();
}

int64_t BaseBackupProgressMsg::getProgressBytes() {
  return this->val;
}

