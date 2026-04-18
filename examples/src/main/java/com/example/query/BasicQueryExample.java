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
                            // The batch is a column-major view of one RESULT_BATCH frame.
                            // It is valid only for the duration of this callback — copy out
                            // anything you need to retain after the method returns.
                            for (int row = 0; row < batch.getRowCount(); row++) {
                                long timestamp = batch.getLong(0, row);       // TIMESTAMP → microseconds since epoch
                                String symbol = batch.getString(1, row);      // SYMBOL → String
                                double price = batch.getDouble(2, row);       // DOUBLE
                                long qty = batch.getLong(3, row);             // LONG

                                System.out.printf(
                                        "ts=%d sym=%s price=%.4f qty=%d%n",
                                        timestamp, symbol, price, qty
                                );
                            }
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
