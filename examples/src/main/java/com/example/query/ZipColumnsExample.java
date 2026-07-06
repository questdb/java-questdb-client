package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.ColumnView;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Zip-style scan over two pinned columns.
 * <p>
 * Multi-column work that doesn't need every column -- compute notional
 * (price * amount), correlate two series, build a derived column -- is well
 * served by holding two {@link ColumnView} instances side-by-side.
 * {@link QwpColumnBatch#column(int)} caches one view per column index, so a
 * second call with a different column does not invalidate the first; both
 * views stay valid for the lifetime of the surrounding {@code onBatch}.
 * <p>
 * Assumes the {@code trades} table the ingest examples write:
 * <pre>
 *   CREATE TABLE trades (
 *       symbol SYMBOL, side SYMBOL, price DOUBLE, amount DOUBLE, timestamp TIMESTAMP
 *   ) TIMESTAMP(timestamp) PARTITION BY DAY WAL;
 * </pre>
 */
public class ZipColumnsExample {

    public static void main(String[] args) throws InterruptedException {
        final double[] notional = {0.0};
        final long[] rows = {0};

        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {

            try (Query q = db.borrowQuery()) {
                q.sql(
                        "SELECT price, amount FROM trades WHERE symbol = 'ETH-USD'")
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                ColumnView prices = batch.column(0);
                                ColumnView amounts = batch.column(1);
                                int n = batch.getRowCount();
                                rows[0] += n;
                                for (int r = 0; r < n; r++) {
                                    // NULL handling: if either side is NULL, skip the contribution.
                                    if (prices.isNull(r) || amounts.isNull(r)) continue;
                                    notional[0] += prices.getDoubleValue(r) * amounts.getDoubleValue(r);
                                }
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                System.out.printf("rows=%d notional=%.2f%n", rows[0], notional[0]);
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
