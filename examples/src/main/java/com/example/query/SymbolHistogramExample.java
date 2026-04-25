package com.example.query;

import io.questdb.client.cutlass.qwp.client.ColumnView;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Per-symbol row counter that exploits the SYMBOL dictionary cache.
 * <p>
 * SYMBOL columns ship as a per-batch (or connection-scoped) dictionary plus
 * per-row dict ids. {@link ColumnView#getSymbolId(int)} returns the id without
 * touching the dictionary heap, so the inner loop is a pure {@code int} read
 * per row. The materialised String for each id is resolved lazily once per
 * dict entry, regardless of how many rows reference it.
 * <p>
 * Pattern: key the histogram on the dict id (an {@code int}), then resolve
 * names once at the end.
 * <p>
 * Assumes a table exists:
 * <pre>
 *   CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG)
 *       TIMESTAMP(ts) PARTITION BY DAY WAL;
 * </pre>
 */
public class SymbolHistogramExample {

    public static void main(String[] args) {
        // The dict ids are batch-scoped, so we resolve to String per batch and
        // accumulate against the resolved name across the whole query.
        final Map<String, Long> counts = new HashMap<>();

        try (QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000)) {
            client.connect();

            client.execute(
                    "SELECT sym FROM trades",
                    new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            ColumnView syms = batch.column(0);
                            int rows = batch.getRowCount();

                            // Per-batch counter keyed on dict id -- only K HashMap
                            // operations, where K is the number of distinct symbols
                            // in this batch (typically << rows).
                            int dictSize = syms.symbolDictSize();
                            long[] perDict = new long[dictSize];
                            long nullCount = 0;

                            for (int r = 0; r < rows; r++) {
                                int id = syms.getSymbolId(r);
                                if (id < 0) {
                                    nullCount++;
                                } else {
                                    perDict[id]++;
                                }
                            }

                            // Resolve dict id -> String once per entry, then merge
                            // into the global histogram.
                            for (int id = 0; id < dictSize; id++) {
                                if (perDict[id] == 0) continue;
                                // getSymbolForId hits the per-dict String cache, so the
                                // same instance is reused across rows in this batch.
                                String name = batch.getSymbolForId(0, id);
                                counts.merge(name, perDict[id], Long::sum);
                            }
                            if (nullCount > 0) {
                                counts.merge("(null)", nullCount, Long::sum);
                            }
                        }

                        @Override
                        public void onEnd(long totalRows) {
                            // Sort descending by count for readability.
                            counts.entrySet().stream()
                                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                                    .collect(LinkedHashMap::new,
                                            (m, e) -> m.put(e.getKey(), e.getValue()),
                                            Map::putAll)
                                    .forEach((sym, n) -> System.out.printf("%-12s %d%n", sym, (int) n));
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
