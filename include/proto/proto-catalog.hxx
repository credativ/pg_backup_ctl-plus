#ifndef __HAVE_PROTO_CATALOG_HXX__
#define __HAVE_PROTO_CATALOG_HXX__

#include <memory>
#include <pgbckctl_exception.hxx>
#include <descr.hxx>
#include <shm.hxx>
#include <BackupCatalog.hxx>
#include <pgsql-proto.hxx>

namespace credativ {

  /**
   * A catalog handler instance encapsulates various
   * actions performed by PostgreSQL streaming API commands.
   *
   * An instance of PGProtoCatalogHandler usually is "attached"
   * to a specific basebackup, thus attach() is required to perform
   * basebackup specific actions.
   */
  class PGProtoCatalogHandler {
  private:

    /**
     * Internal catalog handle.
     */
    std::unique_ptr<BackupCatalog> catalog = nullptr;

    /**
     * The basebackup a PGProtoCatalogHandler is connected
     * to.
     */
    std::shared_ptr<BaseBackupDescr> attached_basebackup = nullptr;

  public:

    PGProtoCatalogHandler(std::string catalog_name,
                          std::string basebackup_fqfn,
                          int archive_id,
                          int worker_id,
                          int child_id,
                          std::shared_ptr<WorkerSHM> shm);

    PGProtoCatalogHandler(std::string catalog_name);

    virtual ~PGProtoCatalogHandler();

    /**
     * Internally attach a PGProtoCatalogHandler to the
     * specified full qualified basebackup name.
     *
     * The return shared pointer is also internally referenced, but
     * since it is shared is stays valid even after having called
     * detach().
     *
     * The worker_id and child_id arguments are required to register
     * the specified basebackup into shared memory, so that any
     * concurrent user could recognize possible conflicts.
     *
     * shm must be an attached WorkerSHM handle.
     *
     * It is also safe to call attach() without an explicit detach() before.
     *
     */
    virtual std::shared_ptr<BaseBackupDescr> attach(std::string basebackup_fqfn,
                                                    int archive_id,
                                                    int worker_id,
                                                    int child_id,
                                                    std::shared_ptr<WorkerSHM> shm);

    /**
     * Returns "true" in case a PGProtoCatalogHandler is attached
     * to a basebackup via attach().
     */
    virtual bool isAttached();

    /**
     * Detaches the internal basebackup reference.
     */
    void detach();

    /**
     * Materializes a protocol level result set to
     * answer a IDENTIFY_SYSTEM streaming API query.
     *
     * set holds the PGProtoResult set afterwards, suitable to
     * be sent over the wire.
     */
    virtual void queryIdentifySystem(std::shared_ptr<pgprotocol::PGProtoResultSet> set);

  };

}

#endif
