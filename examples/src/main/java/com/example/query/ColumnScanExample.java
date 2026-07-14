package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.ColumnView;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Column-first iteration over a QWP egress result.
 * <p>
 * The wire format is column-major, so analytical scans -- summing a price
 * column, finding a min/max, building a histogram -- are most efficient when
 * the loop pins one column and walks rows. {@link QwpColumnBatch#column(int)}
 * returns a {@link ColumnView} flyweight that resolves the column layout once
 * and exposes per-row accessors that take only the row index.
 * <p>
 * For SIMD or JNI consumers, the same view exposes the raw native pointers
 * underlying the batch: {@link ColumnView#valuesAddr()} for the packed values,
 * {@link ColumnView#nullBitmapAddr()} for the null bitmap, and
 * {@link ColumnView#bytesPerValue()} for the fixed-width stride. Two
 * {@code ColumnView} instances over different columns can be held side-by-side
 * (the batch caches one per column index), so a zip-style pass over two
 * columns is a single inner loop.
 * <p>
 * Assumes the {@code trades} table the ingest examples write:
 * <pre>
 *   CREATE TABLE trades (
 *       symbol SYMBOL, side SYMBOL, price DOUBLE, amount DOUBLE, timestamp TIMESTAMP
 *   ) TIMESTAMP(timestamp) PARTITION BY DAY WAL;
 * </pre>
 */
public class ColumnScanExample {

    public static void main(String[] args) throws InterruptedException {
        final double[] sumPrice = {0.0};
        final double[] sumAmount = {0.0};
        final long[] rowCount = {0};
        final long[] nonNullPrice = {0};

        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {
            try (Query q = db.borrowQuery()) {
                q.sql(
                        "SELECT price, amount FROM trades WHERE symbol = 'ETH-USD'")
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                // Pin both columns once. The batch caches one ColumnView per
                                // column index, so prices and amounts remain valid simultaneously.
                                ColumnView prices = batch.column(0);
                                ColumnView amounts = batch.column(1);

                                int rows = batch.getRowCount();
                                rowCount[0] += rows;
                                nonNullPrice[0] += prices.nonNullCount();

                                // Typed column-first scan: one layout lookup, then per-row reads.
                                for (int r = 0; r < rows; r++) {
                                    if (!prices.isNull(r)) {
                                        sumPrice[0] += prices.getDoubleValue(r);
                                    }
                                    sumAmount[0] += amounts.getDoubleValue(r);
                                }

                                // The same view exposes raw addresses for SIMD/JNI consumers:
                                //   long base = prices.valuesAddr();
                                //   int stride = prices.bytesPerValue();      // 8 for DOUBLE
                                //   long bitmap = prices.nullBitmapAddr();    // 0 if no nulls
                                //   int count = prices.nonNullCount();
                                // The bytes follow the QWP wire format documented in
                                // QwpColumnBatch#valuesAddr(int).
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                System.out.printf(
                                        "rows=%d nonNullPrice=%d sumPrice=%.2f sumAmount=%.5f%n",
                                        rowCount[0], nonNullPrice[0], sumPrice[0], sumAmount[0]
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
