package com.example.query;

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;

/**
 * Querying with the low-level {@link QwpQueryClient} primitive.
 * <p>
 * Most applications should use the pooled {@link io.questdb.client.QuestDB}
 * facade instead (see {@link BasicQueryExample} and the other examples in this
 * package): it keeps connections warm, is thread-safe, and exposes the very
 * same result-handler API. Drop down to {@code QwpQueryClient} only when you
 * want a single, explicitly-managed egress connection without a pool -- a
 * one-off script, or the building block for a pool of your own.
 * <p>
 * The primitive is neither pooled nor thread-safe: open it, {@code connect()}
 * once, run queries on the calling thread (each {@code execute(...)} blocks
 * until the query finishes), then {@code close()} tears the WebSocket down.
 * For the raw-pointer / SIMD read path over the same result batches (through
 * the pooled facade), see {@link RawAddressScanExample}.
 * <p>
 * Assumes the {@code trades} table the ingest examples write:
 * <pre>
 *   CREATE TABLE trades (
 *       symbol SYMBOL, side SYMBOL, price DOUBLE, amount DOUBLE, timestamp TIMESTAMP
 *   ) TIMESTAMP(timestamp) PARTITION BY DAY WAL;
 * </pre>
 */
public class QwpQueryClientExample {

    public static void main(String[] args) {
        try (QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000)) {
            client.connect();

            client.execute(
                    "SELECT timestamp, symbol, price, amount FROM trades WHERE symbol = 'ETH-USD' LIMIT 1000",
                    new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            // The RowView handed to the lambda is reusable and pinned to the
                            // current row; copy values out before the callback returns if you
                            // need to retain them past the surrounding onBatch call.
                            batch.forEachRow(row -> System.out.printf(
                                    "timestamp=%d symbol=%s price=%.4f amount=%.5f%n",
                                    row.getLongValue(0),   // TIMESTAMP -> microseconds since epoch
                                    row.getSymbol(1),      // SYMBOL -> String
                                    row.getDoubleValue(2), // DOUBLE
                                    row.getDoubleValue(3)  // DOUBLE
                            ));
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            System.out.println("query finished: " + totalRows + " rows");
                        }

                        @Override
                        public void onError(byte status, String message) {
                            System.err.printf("query failed: status=0x%02X %s%n", status & 0xFF, message);
                        }
                    }
            );
        }
    }
}
