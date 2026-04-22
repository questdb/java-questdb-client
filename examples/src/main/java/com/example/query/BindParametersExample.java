package com.example.query;

import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;

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
 * Assumes a table exists:
 * <pre>
 *   CREATE TABLE trades (ts TIMESTAMP, sym SYMBOL, price DOUBLE, qty LONG)
 *       TIMESTAMP(ts) PARTITION BY DAY WAL;
 * </pre>
 */
public class BindParametersExample {

    public static void main(String[] args) {
        try (QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000)) {
            client.connect();

            String sql = "SELECT ts, sym, price, qty FROM trades "
                    + "WHERE sym = $1 AND price >= $2 AND ts >= $3 LIMIT 1000";

            // Same SQL, three different parameter sets. Each call reuses the
            // compiled factory on the server side because the text is identical.
            String[] symbols = {"AAPL", "MSFT", "GOOG"};
            for (String symbol : symbols) {
                System.out.println("fetching trades for " + symbol);
                client.execute(
                        sql,
                        binds -> binds
                                .setVarchar(0, symbol)
                                .setDouble(1, 100.0)
                                .setTimestampMicros(2, 1_700_000_000_000_000L),
                        new PrintingHandler()
                );
            }
        }
    }

    private static final class PrintingHandler implements QwpColumnBatchHandler {
        @Override
        public void onBatch(QwpColumnBatch batch) {
            batch.forEachRow(row -> System.out.printf(
                    "ts=%d sym=%s price=%.4f qty=%d%n",
                    row.getLongValue(0),
                    row.getSymbol(1),
                    row.getDoubleValue(2),
                    row.getLongValue(3)
            ));
        }

        @Override
        public void onEnd(long totalRows) {
            System.out.println("batch done, total rows = " + totalRows);
        }

        @Override
        public void onError(byte status, String message) {
            System.err.println("query failed: status=" + status + " msg=" + message);
        }
    }
}
