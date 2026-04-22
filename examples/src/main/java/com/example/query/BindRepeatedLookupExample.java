package com.example.query;

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;

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
 * Assumes a table exists:
 * <pre>
 *   CREATE TABLE trades (
 *       ts    TIMESTAMP,
 *       sym   SYMBOL,
 *       price DOUBLE,
 *       qty   LONG
 *   ) TIMESTAMP(ts) PARTITION BY DAY WAL;
 * </pre>
 */
public class BindRepeatedLookupExample {

    public static void main(String[] args) {
        List<String> instruments = Arrays.asList("AAPL", "MSFT", "GOOG", "AMZN", "META");

        try (QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000)) {
            client.connect();

            // SAME SQL TEXT across every iteration. Only the bind values differ.
            String sql = "SELECT ts, price, qty FROM trades "
                    + "WHERE sym = $1 AND ts >= $2 ORDER BY ts DESC LIMIT 10";

            long since = 1_700_000_000_000_000L; // micros since epoch

            for (String symbol : instruments) {
                System.out.println("latest trades for " + symbol);
                long[] rowCount = {0};
                client.execute(
                        sql,
                        binds -> binds
                                .setVarchar(0, symbol)
                                .setTimestampMicros(1, since),
                        new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                rowCount[0] += batch.getRowCount();
                                batch.forEachRow(row -> System.out.printf(
                                        "  ts=%d price=%.4f qty=%d%n",
                                        row.getLongValue(0),
                                        row.getDoubleValue(1),
                                        row.getLongValue(2)
                                ));
                            }

                            @Override
                            public void onEnd(long totalRows) {
                            }

                            @Override
                            public void onError(byte status, String message) {
                                System.err.println("  query failed: " + message);
                            }
                        }
                );
                System.out.println("  (" + rowCount[0] + " rows)");
            }
        }
    }
}
