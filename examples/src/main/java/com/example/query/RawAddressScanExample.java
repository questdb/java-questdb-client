package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.ColumnView;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.std.Unsafe;

/**
 * Raw-address (SIMD-friendly) scan over a fixed-width column.
 * <p>
 * Runs through the pooled {@link QuestDB} facade like the other query examples;
 * the raw {@link ColumnView} pointer surface used here lives on the
 * {@link QwpColumnBatch} and is available regardless of how the query was
 * submitted. For the low-level {@link io.questdb.client.cutlass.qwp.client.QwpQueryClient}
 * primitive itself, see {@link QwpQueryClientExample}.
 * <p>
 * For consumers who want to vectorise -- via the JDK Vector API, JNI to a
 * native kernel, or hand-rolled {@code Unsafe} loops -- {@link ColumnView}
 * exposes the underlying native pointers directly:
 * <ul>
 *   <li>{@link ColumnView#valuesAddr()} -- base of the dense, non-null values.</li>
 *   <li>{@link ColumnView#bytesPerValue()} -- per-value stride for fixed-width
 *       types (8 for DOUBLE / LONG, 4 for INT / FLOAT, etc.); 0 for BOOLEAN
 *       (bit-packed) and -1 for variable-width types.</li>
 *   <li>{@link ColumnView#nonNullCount()} -- number of values packed in the
 *       dense array.</li>
 *   <li>{@link ColumnView#nullBitmapAddr()} -- the per-row null bitmap, or 0
 *       when the column has no nulls in this batch (in which case dense index
 *       equals row index).</li>
 * </ul>
 * The bytes follow the QWP wire format documented on
 * {@link QwpColumnBatch#valuesAddr(int)}. Treat this surface as an expert API:
 * it ties the consumer to the wire layout, which may shift across versions.
 * <p>
 * This example walks the dense values array directly with {@code Unsafe}, then
 * cross-checks the result against the typed {@link ColumnView#getDoubleValue}
 * accessor. In a real SIMD consumer you'd hand {@code valuesAddr} and
 * {@code nonNullCount} to a vector kernel instead.
 * <p>
 * Assumes the {@code trades} table the ingest examples write:
 * <pre>
 *   CREATE TABLE trades (
 *       symbol SYMBOL, side SYMBOL, price DOUBLE, amount DOUBLE, timestamp TIMESTAMP
 *   ) TIMESTAMP(timestamp) PARTITION BY DAY WAL;
 * </pre>
 */
public class RawAddressScanExample {

    public static void main(String[] args) throws InterruptedException {
        final double[] rawSum = {0.0};
        final double[] typedSum = {0.0};
        final long[] rawCount = {0};

        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {
            try (Query q = db.borrowQuery()) {
                q.sql(
                        "SELECT price FROM trades WHERE symbol = 'ETH-USD'")
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                ColumnView prices = batch.column(0);

                                long base = prices.valuesAddr();
                                int stride = prices.bytesPerValue();   // 8 for DOUBLE
                                int dense = prices.nonNullCount();

                                // Raw inner loop: pure pointer arithmetic, no per-row
                                // ObjList lookup or null check. The dense values array
                                // skips NULL rows entirely, so the loop trip count is
                                // nonNullCount, not rowCount.
                                for (int i = 0; i < dense; i++) {
                                    rawSum[0] += Unsafe.getUnsafe().getDouble(base + (long) stride * i);
                                }
                                rawCount[0] += dense;

                                // Cross-check: same result via the typed accessor.
                                int rows = batch.getRowCount();
                                for (int r = 0; r < rows; r++) {
                                    if (!prices.isNull(r)) {
                                        typedSum[0] += prices.getDoubleValue(r);
                                    }
                                }
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                System.out.printf(
                                        "rawCount=%d rawSum=%.6f typedSum=%.6f match=%b%n",
                                        rawCount[0], rawSum[0], typedSum[0], rawSum[0] == typedSum[0]
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
