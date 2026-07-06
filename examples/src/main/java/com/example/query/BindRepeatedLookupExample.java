package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

import java.util.Arrays;
import java.util.List;

/**
 * The repeated-lookup / factory-cache-reuse pattern.
 * <p>
 * Run the same SQL text many times with different bind values. The server
 * compiles the factory on the first call and caches it keyed on the SQL
 * text; every subsequent call with identical text hits the cache. Typed
 * binds (not string interpolation) are what make this work -- the moment
 * you splice values into the SQL text, each call becomes a new cache key.
 * <p>
 * This pattern shows up in dashboards polling a fixed query per-entity,
 * detail-page lookups by id, and any "one query shape, many parameter
 * sets" workload. The cache stays warm for the lifetime of the query-
 * execution plan cache on the server side.
 * <p>
 * Assumes the {@code trades} table the ingest examples write:
 * <pre>
 *   CREATE TABLE trades (
 *       symbol SYMBOL, side SYMBOL, price DOUBLE, amount DOUBLE, timestamp TIMESTAMP
 *   ) TIMESTAMP(timestamp) PARTITION BY DAY WAL;
 * </pre>
 */
public class BindRepeatedLookupExample {

    public static void main(String[] args) throws InterruptedException {
        List<String> instruments = Arrays.asList("ETH-USD", "BTC-USD", "SOL-USD", "ADA-USD", "XRP-USD");

        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;");
             Query q = db.borrowQuery()) {

            // SAME SQL TEXT across every iteration. Only the bind values differ.
            String sql = "SELECT timestamp, price, amount FROM trades "
                    + "WHERE symbol = $1 AND timestamp >= $2 ORDER BY timestamp DESC LIMIT 10";

            long since = 1_700_000_000_000_000L; // micros since epoch

            for (String symbol : instruments) {
                System.out.println("latest trades for " + symbol);
                long[] rowCount = {0};
                try {
                    q.sql(sql)
                            .binds(binds -> binds
                                    .setVarchar(0, symbol)
                                    .setTimestampMicros(1, since))
                            .handler(new QwpColumnBatchHandler() {
                                @Override
                                public void onBatch(QwpColumnBatch batch) {
                                    rowCount[0] += batch.getRowCount();
                                    batch.forEachRow(row -> System.out.printf(
                                            "  timestamp=%d price=%.4f amount=%.5f%n",
                                            row.getLongValue(0),
                                            row.getDoubleValue(1),
                                            row.getDoubleValue(2)
                                    ));
                                }

                                @Override
                                public void onEnd(long totalRows) {
                                }

                                @Override
                                public void onError(byte status, String message) {
                                }
                            })
                            .submit()
                            .await();
                } catch (QueryException e) {
                    System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
                }
                System.out.println("  (" + rowCount[0] + " rows)");
            }
        }
    }
}
