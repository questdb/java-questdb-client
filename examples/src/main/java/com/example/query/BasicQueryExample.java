package com.example.query;

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;

/**
 * Minimal QWP egress query example.
 * <p>
 * Connects to a QuestDB server over the /read/v1 WebSocket endpoint,
 * runs a SELECT query, and prints each row as the batches arrive.
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

    public static void main(String[] args) {
        try (QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000)) {
            client.connect();

            client.execute(
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
            );
        }
    }
}
