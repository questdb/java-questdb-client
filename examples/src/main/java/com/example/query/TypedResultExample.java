package com.example.query;

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;

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

    public static void main(String[] args) {
        try (QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000)) {
            client.connect();
            client.execute("SELECT * FROM demo LIMIT 5", new QwpColumnBatchHandler() {
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
                                appendCell(line, batch, col, row);
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
                    System.err.println("query error: " + message);
                }
            });
        }
    }

    /**
     * Appends a typed value to the builder using the column's wire type to pick
     * the right accessor. The set of wire type codes is in {@link QwpConstants}.
     */
    private static void appendCell(StringBuilder out, QwpColumnBatch batch, int col, int row) {
        byte type = batch.getColumnWireType(col);
        if (type == QwpConstants.TYPE_BOOLEAN) {
            out.append(((Boolean) batch.getValue(col, row)).booleanValue());
        } else if (type == QwpConstants.TYPE_BYTE
                || type == QwpConstants.TYPE_SHORT
                || type == QwpConstants.TYPE_CHAR
                || type == QwpConstants.TYPE_INT
                || type == QwpConstants.TYPE_LONG
                || type == QwpConstants.TYPE_DATE
                || type == QwpConstants.TYPE_TIMESTAMP
                || type == QwpConstants.TYPE_TIMESTAMP_NANOS
                || type == QwpConstants.TYPE_DECIMAL64) {
            out.append(batch.getLong(col, row));
        } else if (type == QwpConstants.TYPE_FLOAT) {
            out.append(batch.getFloat(col, row));
        } else if (type == QwpConstants.TYPE_DOUBLE) {
            out.append(batch.getDouble(col, row));
        } else if (type == QwpConstants.TYPE_STRING || type == QwpConstants.TYPE_SYMBOL) {
            out.append(batch.getString(col, row));
        } else if (type == QwpConstants.TYPE_VARCHAR) {
            out.append(new String(batch.getVarchar(col, row)));
        } else if (type == QwpConstants.TYPE_UUID) {
            long[] parts = batch.getLongArray(col, row); // [lo, hi]
            out.append(String.format("%016x-%016x", parts[1], parts[0]));
        } else if (type == QwpConstants.TYPE_LONG256) {
            long[] parts = batch.getLongArray(col, row); // 4 longs LSB-first
            out.append(String.format("0x%016x%016x%016x%016x",
                    parts[3], parts[2], parts[1], parts[0]));
        } else if (type == QwpConstants.TYPE_GEOHASH) {
            out.append("geohash(").append(batch.getGeohashPrecisionBits(col))
                    .append("b)=0x").append(Long.toHexString(batch.getLong(col, row)));
        } else if (type == QwpConstants.TYPE_DECIMAL128 || type == QwpConstants.TYPE_DECIMAL256) {
            long[] parts = batch.getLongArray(col, row);
            out.append("decimal(");
            for (int i = 0; i < parts.length; i++) {
                if (i > 0) out.append(',');
                out.append(parts[i]);
            }
            out.append(')');
        } else {
            out.append("(type 0x").append(Integer.toHexString(type & 0xFF)).append(")");
        }
    }
}
