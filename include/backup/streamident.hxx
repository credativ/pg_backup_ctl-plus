#ifndef __HAVE_STREAMIDENT_HXX_
#define __HAVE_STREAMIDENT_HXX_

/* for XLogRecPtr */
#include <postgres.hxx>

namespace credativ {

  /**
   * Represents an identified streaming connection.
   */
  class StreamIdentification : public PushableCols {
  public:

    static constexpr const char * STREAM_PROGRESS_IDENTIFIED = "IDENTIFIED";
    static constexpr const char * STREAM_PROGRESS_STREAMING = "STREAMING";
    static constexpr const char * STREAM_PROGRESS_SHUTDOWN = "SHUTDOWN";
    static constexpr const char * STREAM_PROGRESS_FAILED = "FAILED";
    static constexpr const char * STREAM_PROGRESS_TIMELINE_SWITCH = "TIMELINE_SWITCH";

    unsigned long long id = -1; /* internal catalog stream id */
    int archive_id = -1; /* used to reflect assigned archive */
    std::string stype;
    std::string slot_name;
    std::string systemid;
    unsigned int timeline;
    std::string xlogpos;
    std::string dbname;
    std::string status;
    std::string create_date;

    /**
     * Runtime variable wal_segment_size, transports
     * the configured wal_segment_size during streaming
     * operation.
     *
     * Usually this gets initialized by instantiating
     * a PGStream object and establish a streaming connnection
     * (e.g. PGStream::connect()).
     */
    unsigned long long wal_segment_size = -1;

    /*
     * Tells the stream to restart from the server XLOG
     * position without consulting the catalog. Only used
     * during runtime.
     */
    bool force_xlogpos_restart = false;

    /*
     * Runtime streaming properties. Those usually
     * get instrumented for example by a WALStreamerProcess
     * instance.
     */
    int write_pos_start_offset = 0; /* starting offset into current XLogSegment */
    XLogRecPtr flush_position = InvalidXLogRecPtr;
    XLogRecPtr write_position = InvalidXLogRecPtr;
    XLogRecPtr apply_position = InvalidXLogRecPtr;
    XLogRecPtr server_position = InvalidXLogRecPtr;
    XLogRecPtr last_reported_flush_position = InvalidXLogRecPtr;

    /*
     * Additional properties, those aren't necessarily
     * initialized. Use them with care.
     */
    std::string archive_name = "";

    StreamIdentification();
    ~StreamIdentification();

    /*
     * Physical replication slot, if any
     */
    std::shared_ptr<PhysicalReplicationSlot> slot = nullptr;

    /*
     * Set properties back to default.
     */
    void reset();

    /*
     * Returns the decoded XLogRecPtr from xlogpos
     */
    XLogRecPtr xlogposDecoded();
    std::string xlogposEncoded();

    /**
     * Updates the internal write position segment
     * to XLOG segment start boundary.
     *
     * Please note that calling this method is only legit if you have
     * set the write_position and WAL segment size (which
     * might be hard coded if compiled against PostgreSQL < 11).
     */
    int updateStartSegmentWriteOffset();

  };

}

#endif
