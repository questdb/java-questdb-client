package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

import java.util.Arrays;

/**
 * Reading array columns from a result set.
 * <p>
 * {@link QwpColumnBatch#getDoubleArrayElements(int, int)} returns a flattened
 * {@code double[]} in row-major order; {@link QwpColumnBatch#getArrayNDims(int, int)}
 * reports the dimensionality so you can interpret that flat layout. This is the
 * egress counterpart to {@link com.example.sender.WsArrayExample} (ingest).
 * <p>
 * Alternatively, extract individual elements in SQL (e.g. {@code SELECT
 * bids[1][1] FROM market_data}) and read them as scalar doubles.
 * <p>
 * Assumes a table with a {@code DOUBLE[]} column, e.g. the one
 * {@code WsArrayExample} writes:
 * <pre>
 *   CREATE TABLE book (levels DOUBLE[], timestamp TIMESTAMP)
 *       TIMESTAMP(timestamp) PARTITION BY DAY WAL;
 * </pre>
 */
public class ArrayResultExample {

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {
            try (Query q = db.borrowQuery()) {
                q.sql(
                        "SELECT levels FROM book LIMIT 10")
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                for (int row = 0; row < batch.getRowCount(); row++) {
                                    if (batch.isNull(0, row)) {
                                        System.out.println("levels: null");
                                        continue;
                                    }
                                    int nDims = batch.getArrayNDims(0, row);
                                    double[] flat = batch.getDoubleArrayElements(0, row);
                                    System.out.printf("levels: nDims=%d values=%s%n",
                                            nDims, Arrays.toString(flat));
                                }
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                System.out.println("done: " + totalRows + " rows");
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
