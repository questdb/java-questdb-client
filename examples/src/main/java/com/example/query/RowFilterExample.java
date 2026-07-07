package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Per-row filtering with {@code RowView}.
 * <p>
 * Most of the value of a row-pinned facade shows up when the consumer is
 * deciding what to do with each row based on a predicate over several columns.
 * The lambda reads cleanly because the row index is implicit, and the same
 * reusable view is handed back across every iteration -- zero allocations
 * inside the loop.
 * <p>
 * Assumes the {@code trades} table the ingest examples write:
 * <pre>
 *   CREATE TABLE trades (
 *       symbol SYMBOL, side SYMBOL, price DOUBLE, amount DOUBLE, timestamp TIMESTAMP
 *   ) TIMESTAMP(timestamp) PARTITION BY DAY WAL;
 * </pre>
 */
public class RowFilterExample {

    public static void main(String[] args) throws InterruptedException {
        final double threshold = 100.0;
        final long[] kept = {0};
        final long[] skipped = {0};

        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {

            try (Query q = db.borrowQuery()) {
                q.sql(
                        "SELECT timestamp, symbol, price, amount FROM trades")
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                batch.forEachRow(row -> {
                                    // Skip NULL prices and rows below threshold without ever
                                    // materialising the full result set.
                                    if (row.isNull(2) || row.getDoubleValue(2) < threshold) {
                                        skipped[0]++;
                                        return;
                                    }
                                    kept[0]++;
                                    System.out.printf(
                                            "timestamp=%d symbol=%s price=%.2f amount=%.5f%n",
                                            row.getLongValue(0),
                                            row.getString(1),
                                            row.getDoubleValue(2),
                                            row.getDoubleValue(3)
                                    );
                                });
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                System.out.printf("done: kept=%d skipped=%d%n", kept[0], skipped[0]);
                            }

                            @Override
                            public void onError(byte status, String message) {
                            }
                        }
                ).submit().await();
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }
        }
    }
}
