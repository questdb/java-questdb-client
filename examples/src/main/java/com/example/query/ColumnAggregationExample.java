package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.ColumnView;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Single-column aggregate (min / max / sum / count) using {@code ColumnView}.
 * <p>
 * Aggregates are textbook column-first work: the inner loop touches one
 * column's values in tight succession. Pinning the column with
 * {@link QwpColumnBatch#column(int)} resolves the column layout once per batch;
 * the loop body is then a pure per-row read.
 * <p>
 * Assumes the {@code trades} table the ingest examples write:
 * <pre>
 *   CREATE TABLE trades (
 *       symbol SYMBOL, side SYMBOL, price DOUBLE, amount DOUBLE, timestamp TIMESTAMP
 *   ) TIMESTAMP(timestamp) PARTITION BY DAY WAL;
 * </pre>
 */
public class ColumnAggregationExample {

    public static void main(String[] args) throws InterruptedException {
        final double[] min = {Double.POSITIVE_INFINITY};
        final double[] max = {Double.NEGATIVE_INFINITY};
        final double[] sum = {0.0};
        final long[] count = {0};

        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {
            try (Query q = db.borrowQuery()) {
                q.sql(
                        "SELECT price FROM trades WHERE symbol = 'ETH-USD'")
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                ColumnView prices = batch.column(0);
                                int rows = batch.getRowCount();
                                for (int r = 0; r < rows; r++) {
                                    if (prices.isNull(r)) continue;
                                    double p = prices.getDoubleValue(r);
                                    if (p < min[0]) min[0] = p;
                                    if (p > max[0]) max[0] = p;
                                    sum[0] += p;
                                    count[0]++;
                                }
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                if (count[0] == 0) {
                                    System.out.println("no rows");
                                    return;
                                }
                                System.out.printf(
                                        "count=%d min=%.4f max=%.4f avg=%.4f%n",
                                        count[0], min[0], max[0], sum[0] / count[0]
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
