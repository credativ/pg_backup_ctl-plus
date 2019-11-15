#include <sstream>
#include <boost/log/trivial.hpp>
#include <pgsql-proto.hxx>
#include <proto-descr.hxx>
#include <server.hxx>

using namespace credativ;
using namespace credativ::pgprotocol;

/* ****************************************************************************
 * PGProtoResultSet implementation
 * ***************************************************************************/

PGProtoResultSet::PGProtoResultSet() {}

PGProtoResultSet::~PGProtoResultSet() {}

int PGProtoResultSet::calculateRowDescrSize() {

  return MESSAGE_HDR_LENGTH_SIZE + sizeof(short) + row_descr_size;

}

int PGProtoResultSet::descriptor(ProtocolBuffer &buffer) {

  /* reset rows iterator to start offset */
  row_iterator = data_descr.row_values.begin();

  return prepareSend(buffer, PGPROTO_ROW_DESCR_MESSAGE);

}

int PGProtoResultSet::data(ProtocolBuffer &buffer) {

  /*
   * NOTE: calling descriptor() before data() should have
   *       positioned the internal iterator to the first data row.
   */
  return prepareSend(buffer, PGPROTO_DATA_DESCR_MESSAGE);

}

int PGProtoResultSet::prepareSend(ProtocolBuffer &buffer,
                                  int type) {

  int message_size = 0;

  switch(type) {

  case PGPROTO_DATA_DESCR_MESSAGE:
    {
      /*
       * Check if we are positioned on the last element.
       * If true, message_size will return 0, indicating the end of
       * data row messages.
       */
      if (row_iterator == data_descr.row_values.end()) {

        BOOST_LOG_TRIVIAL(debug) << "result set iterator reached end of data rows";
        break;

      }

      message_size = MESSAGE_HDR_SIZE + sizeof(short)
        + row_iterator->row_size;
      buffer.allocate(message_size);

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO write data row size "
                               << message_size
                               << ", buffer size "
                               << buffer.getSize();

      /*
       * Prepare message header.
       */
      buffer.write_byte(DescribeMessage);
      buffer.write_int(message_size - MESSAGE_HDR_BYTE);

      /* Number of columns */
      buffer.write_short(row_iterator->fieldCount());

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO data row message has "
                               << row_iterator->values.size()
                               << " columns";

      /*
       * NOTE: on the very first call to data(), we will
       *       find the iterator positioned on the very first
       *       data row, if descriptor() was called before.
       *
       *       Just advance the iterator unless we have seen
       *       the last data row.
       */

      /* Loop through the column values list */
      for (auto &colval : row_iterator->values) {

        BOOST_LOG_TRIVIAL(debug) << "PG PROTO write col data len "
                                 << colval.length << " bytes";

        buffer.write_int(colval.length);
        buffer.write_buffer(colval.data.c_str(), colval.length);

      }

      /*
       * Position the data row iterator on the next data row.
       */
      row_iterator++;

      break;
    }

  case PGPROTO_ROW_DESCR_MESSAGE:
    {

      message_size = calculateRowDescrSize();
      buffer.allocate(message_size + MESSAGE_HDR_BYTE);

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO buffer allocated "
                               << buffer.getSize() << " bytes";

      /*
       * Prepare the message header.
       */
      buffer.write_byte(RowDescriptionMessage);
      buffer.write_int(message_size);
      buffer.write_short(row_descr.fieldCount());

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO row descriptor has "
                               << row_descr.count
                               << " fields";

      /* The header is prepared now, write the message contents */
      for (auto &it : row_descr.column_list) {

        buffer.write_buffer(it.name.c_str(), it.name.length());
        buffer.write_byte('\0');
        buffer.write_int(it.tableoid);
        buffer.write_short(it.attnum);
        buffer.write_int(it.typeoid);
        buffer.write_short(it.typelen);
        buffer.write_int(it.typemod);
        buffer.write_short(it.format);

      }

      BOOST_LOG_TRIVIAL(debug) << "PG PROTO row descriptor buffer pos " << buffer.pos();
      break;
    }
  }

  return message_size;

}

void PGProtoResultSet::clear() {

  row_descr_size = 0;

  data_descr.row_values.clear();
  row_descr.column_list.clear();
  row_descr.count = 0;

}

void PGProtoResultSet::addColumn(std::string colname,
                                 int tableoid,
                                 short attnum,
                                 int typeoid,
                                 short typelen,
                                 int typemod,
                                 short format) {

  PGProtoColumnDescr coldef;

  coldef.name = colname;
  coldef.tableoid = tableoid;
  coldef.attnum   = attnum;
  coldef.typeoid  = typeoid;
  coldef.typemod  = typemod;
  coldef.format   = format;

  row_descr_size += coldef.name.length() + 1; /* NULL byte */
  row_descr_size += sizeof(tableoid);
  row_descr_size += sizeof(attnum);
  row_descr_size += sizeof(typeoid);
  row_descr_size += sizeof(typelen);
  row_descr_size += sizeof(typemod);
  row_descr_size += sizeof(format);

  row_descr.column_list.push_back(coldef);

}

void PGProtoResultSet::addRow(std::vector<PGProtoColumnDataDescr> column_values) {

  PGProtoColumns columns;

  /*
   * Sanity check, number of column values should be identical to
   * number of column descriptors.
   */
  if (row_descr.fieldCount() != column_values.size()) {

    std::ostringstream oss;

    oss << "number of colums("
        << column_values.size()
        << ") do not match number in row  descriptor("
        << row_descr.fieldCount()
        << ")";

    throw TCPServerFailure(oss.str());

  }

  /*
   * Push column values to internal rows list
   * and calculate new total row size.
   */
  for (auto &col : column_values) {

    columns.values.push_back(col);

    /* The row size includes bytes of the column value _and_
     * the 4 byte length value! */
    columns.row_size += (sizeof(col.length) + col.length);

  }

  /* increase row counter */
  row_descr.count++;

  /* save columns to internal list */
  data_descr.row_values.push_back(columns);

}

unsigned int PGProtoResultSet::rowCount() {

  return row_descr.count;

}

/* ****************************************************************************
 * PGProtoCmdDescr implementation
 * ***************************************************************************/

void PGProtoCmdDescr::setCommandTag(ProtocolCommandTag const& tag) {

  this->tag = tag;

}

