package com.example.query;

import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.std.str.StringSink;

/**
 * Streaming CSV export of a query result.
 * <p>
 * Combines {@link QwpColumnBatch#forEachRow} for the per-row loop with the
 * zero-allocation string sink ({@link QwpColumnBatch#getString(int, int,
 * io.questdb.client.std.str.CharSink) batch.getString(col, row, sink)}) so the
 * UTF-8 bytes for STRING / VARCHAR / SYMBOL columns transcode straight into a
 * {@link StringSink} without intermediate {@link String} allocations.
 * <p>
 * Note: this example does not handle CSV quoting -- a real exporter must
 * escape commas, quotes, and newlines in string values. The point here is the
 * shape of the loop, not the CSV format.
 * <p>
 * Assumes a table exists:
 * <pre>
 *   CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG)
 *       TIMESTAMP(ts) PARTITION BY DAY WAL;
 * </pre>
 */
public class CsvExportExample {

    public static void main(String[] args) throws InterruptedException {
        // One sink reused across every row -- clear() resets the position to 0
        // without releasing the backing array.
        final StringSink line = new StringSink();
        final long[] rowsWritten = {0};

        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {
            // Header row.
            System.out.println("ts,sym,price,qty");

            try {
                db.executeSql(
                        "SELECT ts, sym, price, qty FROM trades",
                        new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                batch.forEachRow(row -> {
                                    line.clear();

                                    // ts (TIMESTAMP -> microseconds since epoch).
                                    if (!row.isNull(0)) line.put(row.getLongValue(0));
                                    line.put(',');

                                    // sym (SYMBOL) -- transcodes UTF-8 directly into the sink.
                                    row.getString(1, line);
                                    line.put(',');

                                    // price (DOUBLE).
                                    if (!row.isNull(2)) line.put(row.getDoubleValue(2));
                                    line.put(',');

                                    // qty (LONG).
                                    if (!row.isNull(3)) line.put(row.getLongValue(3));

                                    System.out.println(line);
                                    rowsWritten[0]++;
                                });
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                System.err.println("exported " + rowsWritten[0] + " rows");
                            }

                            @Override
                            public void onError(byte status, String message) {
                                System.err.println("query failed: status=" + status + " msg=" + message);
                            }
                        }
                ).await();
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }
        }
    }
}
