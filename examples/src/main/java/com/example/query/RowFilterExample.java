package com.example.query;

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;

/**
 * Per-row filtering with {@code RowView}.
 * <p>
 * Most of the value of a row-pinned facade shows up when the consumer is
 * deciding what to do with each row based on a predicate over several columns.
 * The lambda reads cleanly because the row index is implicit, and the same
 * reusable view is handed back across every iteration -- zero allocations
 * inside the loop.
 * <p>
 * Assumes a table exists:
 * <pre>
 *   CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG)
 *       TIMESTAMP(ts) PARTITION BY DAY WAL;
 * </pre>
 */
public class RowFilterExample {

    public static void main(String[] args) {
        final double threshold = 100.0;
        final long[] kept = {0};
        final long[] skipped = {0};

        try (QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000)) {
            client.connect();

            client.execute(
                    "SELECT ts, sym, price, qty FROM trades",
                    new QwpColumnBatchHandler() {
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
                                        "ts=%d sym=%s price=%.2f qty=%d%n",
                                        row.getLongValue(0),
                                        row.getString(1),
                                        row.getDoubleValue(2),
                                        row.getLongValue(3)
                                );
                            });
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            System.out.printf("done: kept=%d skipped=%d%n", kept[0], skipped[0]);
                        }

                        @Override
                        public void onError(byte status, String message) {
                            System.err.println("query failed: status=" + status + " msg=" + message);
                        }
                    }
            );
        }
    }
}
