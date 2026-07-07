package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Demonstrates typed bind parameters.
 * <p>
 * Placeholders in the SQL ({@code $1, $2, ...}) are filled by a lambda that
 * receives a {@code QwpBindValues} sink. Values go over the wire as typed
 * binary payloads, not interpolated strings -- no manual escaping, correct
 * handling of UUID / DECIMAL / TIMESTAMP_NANOS, and the server's SQL-text-
 * keyed factory cache hits on every repeated call because the SQL text is
 * identical.
 * <p>
 * Assumes the {@code trades} table the ingest examples write:
 * <pre>
 *   CREATE TABLE trades (
 *       symbol SYMBOL, side SYMBOL, price DOUBLE, amount DOUBLE, timestamp TIMESTAMP
 *   ) TIMESTAMP(timestamp) PARTITION BY DAY WAL;
 * </pre>
 */
public class BindParametersExample {

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;");
             Query q = db.borrowQuery()) {

            String sql = "SELECT timestamp, symbol, price, amount FROM trades "
                    + "WHERE symbol = $1 AND price >= $2 AND timestamp >= $3 LIMIT 1000";

            // Same SQL, three different parameter sets. Each call reuses the
            // compiled factory on the server side because the text is identical.
            String[] symbols = {"ETH-USD", "BTC-USD", "SOL-USD"};
            for (String symbol : symbols) {
                System.out.println("fetching trades for " + symbol);
                try {
                    q.sql(sql)
                            .binds(binds -> binds
                                    .setVarchar(0, symbol)
                                    .setDouble(1, 100.0)
                                    .setTimestampMicros(2, 1_700_000_000_000_000L))
                            .handler(new PrintingHandler())
                            .submit()
                            .await();
                } catch (QueryException e) {
                    System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
                }
            }
        }
    }

    private static final class PrintingHandler implements QwpColumnBatchHandler {
        @Override
        public void onBatch(QwpColumnBatch batch) {
            batch.forEachRow(row -> System.out.printf(
                    "timestamp=%d symbol=%s price=%.4f amount=%.5f%n",
                    row.getLongValue(0),
                    row.getSymbol(1),
                    row.getDoubleValue(2),
                    row.getDoubleValue(3)
            ));
        }

        @Override
        public void onEnd(long totalRows) {
            System.out.println("batch done, total rows = " + totalRows);
        }

        @Override
        public void onError(byte status, String message) {
        }
    }
}
