package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Long256Impl;
import io.questdb.client.std.Uuid;

/**
 * Reading every supported wire type from a {@link QwpColumnBatch}.
 * <p>
 * The batch exposes per-cell typed accessors and a {@code getColumnWireType(col)}
 * helper so you can dispatch generically when the query's column set isn't known
 * at compile time (e.g., a generic query runner).
 * <p>
 * Assumes a table containing a representative set of columns, for example:
 * <pre>
 *   CREATE TABLE demo (
 *       b BOOLEAN, bt BYTE, sh SHORT, ch CHAR,
 *       i INT, l LONG, f FLOAT, d DOUBLE,
 *       dt DATE, ts TIMESTAMP,
 *       s STRING, v VARCHAR, sy SYMBOL,
 *       u UUID, l256 LONG256,
 *       g GEOHASH(20b),
 *       d64 DECIMAL(18,2)
 *   );
 * </pre>
 */
public class TypedResultExample {

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {
            try (Query q = db.borrowQuery()) {
                q.sql("SELECT * FROM demo LIMIT 5").handler(new QwpColumnBatchHandler() {
                    // Sinks live at the handler level so they're reused across every
                    // (row, col) that needs a UUID or LONG256/DECIMAL256 decode.
                    final Long256Impl long256Sink = new Long256Impl();
                    final Uuid uuidSink = new Uuid();

                    @Override
                    public void onBatch(QwpColumnBatch batch) {
                        int cols = batch.getColumnCount();
                        int rows = batch.getRowCount();
                        for (int row = 0; row < rows; row++) {
                            StringBuilder line = new StringBuilder();
                            for (int col = 0; col < cols; col++) {
                                if (col > 0) line.append(" | ");
                                line.append(batch.getColumnName(col)).append('=');
                                if (batch.isNull(col, row)) {
                                    line.append("NULL");
                                } else {
                                    appendCell(line, batch, col, row, uuidSink, long256Sink);
                                }
                            }
                            System.out.println(line);
                        }
                    }

                    @Override
                    public void onEnd(long totalRows) {
                    }

                    @Override
                    public void onError(byte status, String message) {
                    }
                }).submit().await();
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }
        }
    }

    /**
     * Appends a typed value to the builder using the column's wire type to pick
     * the right accessor. The set of wire type codes is in {@link QwpConstants}.
     * {@code uuidSink} and {@code long256Sink} are reusable sinks owned by the
     * caller; passing them in keeps UUID / LONG256 / DECIMAL256 decoding
     * allocation-free across all cells in the query.
     */
    private static void appendCell(StringBuilder out, QwpColumnBatch batch, int col, int row,
                                   Uuid uuidSink, Long256Impl long256Sink) {
        byte type = batch.getColumnWireType(col);
        switch (type) {
            case QwpConstants.TYPE_BOOLEAN:
                out.append(batch.getBoolValue(col, row));
                break;
            case QwpConstants.TYPE_BYTE:
                out.append(batch.getByteValue(col, row));
                break;
            case QwpConstants.TYPE_SHORT:
                out.append(batch.getShortValue(col, row));
                break;
            case QwpConstants.TYPE_CHAR:
                out.append(batch.getCharValue(col, row));
                break;
            case QwpConstants.TYPE_INT:
            case QwpConstants.TYPE_IPv4:
                out.append(batch.getIntValue(col, row));
                break;
            case QwpConstants.TYPE_LONG:
            case QwpConstants.TYPE_DATE:
            case QwpConstants.TYPE_TIMESTAMP:
            case QwpConstants.TYPE_TIMESTAMP_NANOS:
            case QwpConstants.TYPE_DECIMAL64:
                out.append(batch.getLongValue(col, row));
                break;
            case QwpConstants.TYPE_FLOAT:
                out.append(batch.getFloatValue(col, row));
                break;
            case QwpConstants.TYPE_DOUBLE:
                out.append(batch.getDoubleValue(col, row));
                break;
            case QwpConstants.TYPE_SYMBOL:
            case QwpConstants.TYPE_VARCHAR:
                out.append(batch.getString(col, row));
                break;
            case QwpConstants.TYPE_UUID:
                batch.getUuid(col, row, uuidSink);
                out.append(String.format("%016x-%016x", uuidSink.getHi(), uuidSink.getLo()));
                break;
            case QwpConstants.TYPE_LONG256:
            case QwpConstants.TYPE_DECIMAL256:
                batch.getLong256(col, row, long256Sink);
                out.append(String.format("0x%016x%016x%016x%016x",
                        long256Sink.getLong3(),
                        long256Sink.getLong2(),
                        long256Sink.getLong1(),
                        long256Sink.getLong0()));
                break;
            case QwpConstants.TYPE_GEOHASH:
                out.append("geohash(").append(batch.getGeohashPrecisionBits(col))
                        .append("b)=0x").append(Long.toHexString(batch.getGeohashValue(col, row)));
                break;
            case QwpConstants.TYPE_DECIMAL128:
                out.append("decimal(")
                        .append(batch.getDecimal128Low(col, row)).append(',')
                        .append(batch.getDecimal128High(col, row)).append(')');
                break;
            default:
                out.append("(type 0x").append(Integer.toHexString(type & 0xFF)).append(")");
                break;
        }
    }
}
