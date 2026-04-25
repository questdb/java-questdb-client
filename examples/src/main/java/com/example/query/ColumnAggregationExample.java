package com.example.query;

import io.questdb.client.cutlass.qwp.client.ColumnView;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;

/**
 * Single-column aggregate (min / max / sum / count) using {@code ColumnView}.
 * <p>
 * Aggregates are textbook column-first work: the inner loop touches one
 * column's values in tight succession. Pinning the column with
 * {@link QwpColumnBatch#column(int)} resolves the column layout once per batch;
 * the loop body is then a pure per-row read.
 * <p>
 * Assumes a table exists:
 * <pre>
 *   CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG)
 *       TIMESTAMP(ts) PARTITION BY DAY WAL;
 * </pre>
 */
public class ColumnAggregationExample {

    public static void main(String[] args) {
        final double[] min = {Double.POSITIVE_INFINITY};
        final double[] max = {Double.NEGATIVE_INFINITY};
        final double[] sum = {0.0};
        final long[] count = {0};

        try (QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000)) {
            client.connect();

            client.execute(
                    "SELECT price FROM trades WHERE sym = 'AAPL'",
                    new QwpColumnBatchHandler() {
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
                            System.err.println("query failed: status=" + status + " msg=" + message);
                        }
                    }
            );
        }
    }
}
