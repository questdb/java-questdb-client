package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Streaming over a large result set.
 * <p>
 * For result sets that don't fit in memory, the column-batch consumer is the
 * right entry point: each {@code onBatch} callback sees one {@code RESULT_BATCH}
 * frame (up to a few thousand rows), letting you process the data incrementally
 * without the whole result set ever being materialised in the client.
 * <p>
 * The server streams continuously until the cursor is exhausted; batches are
 * delivered to the handler on the pool's query I/O thread as the WebSocket
 * yields frames, while the submitting thread blocks in {@code await()}. Pacing
 * naturally follows TCP back-pressure -- if the consumer is slower than the
 * network, the kernel send buffer fills and the server parks between batches.
 * For explicit byte-level flow control on top of that, see
 * {@link CreditFlowControlExample}.
 */
public class LargeResultStreamingExample {

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {

            // Running totals we accumulate across all batches without ever holding
            // the whole result set in memory.
            final long[] rowsSeen = {0};
            final long[] batchCount = {0};
            final double[] priceSum = {0.0};

            try (Query q = db.borrowQuery()) {
                q.sql(
                        "SELECT timestamp, price FROM trades WHERE timestamp > dateadd('d', -7, now())")
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                batchCount[0]++;
                                int rows = batch.getRowCount();
                                for (int r = 0; r < rows; r++) {
                                    priceSum[0] += batch.getDoubleValue(1, r);
                                }
                                rowsSeen[0] += rows;

                                // Per-batch progress marker for long-running queries.
                                if (batchCount[0] % 10 == 0) {
                                    System.out.println("received " + rowsSeen[0] + " rows so far");
                                }
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                System.out.printf(
                                        "done: rows=%d batches=%d priceSum=%.2f%n",
                                        rowsSeen[0], batchCount[0], priceSum[0]
                                );
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
