package com.example.query;

import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Minimal QWP egress query example.
 * <p>
 * Opens a pooled {@link QuestDB} handle over QWP (WebSocket), runs a SELECT
 * through {@code db.executeSql(...)}, and prints each row as the batches
 * arrive. {@code Completion.await()} blocks until the query finishes and
 * rethrows any server error as a {@link QueryException}.
 * <p>
 * Iterates rows via {@link QwpColumnBatch#forEachRow}, which hands a reusable
 * row-pinned view to the lambda. Single-arg accessors keep the read path
 * compact; the underlying batch is still column-major and the {@code (col, row)}
 * primitives remain available on {@code batch} for callers that prefer them.
 * <p>
 * Assumes a table exists:
 * <pre>
 *   CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG)
 *       TIMESTAMP(ts) PARTITION BY DAY WAL;
 * </pre>
 */
public class BasicQueryExample {

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {
            try {
                db.executeSql(
                        "SELECT ts, sym, price, qty FROM trades WHERE sym = 'AAPL' LIMIT 1000",
                        new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                // The RowView handed to the lambda is reusable and pinned to the
                                // current row; copy values out before the callback returns if you
                                // need to retain them past the surrounding onBatch call.
                                batch.forEachRow(row -> {
                                    long timestamp = row.getLongValue(0);  // TIMESTAMP -> microseconds since epoch
                                    String symbol = row.getSymbol(1);      // SYMBOL -> String
                                    double price = row.getDoubleValue(2);  // DOUBLE
                                    long qty = row.getLongValue(3);        // LONG

                                    System.out.printf(
                                            "ts=%d sym=%s price=%.4f qty=%d%n",
                                            timestamp, symbol, price, qty
                                    );
                                });
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                System.out.println("query finished");
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
