package com.example;

import io.questdb.client.Completion;
import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * The recommended entry point: the pooled {@link QuestDB} handle.
 * <p>
 * {@code QuestDB.connect(...)} owns connection pools for <b>both</b> directions
 * over QWP (the QuestDB WebSocket protocol): a {@link Sender} pool for ingest
 * and a query-client pool for egress. One {@code ws}/{@code wss} connect string
 * configures both -- each side reads the keys it owns and ignores the rest.
 * <p>
 * Construct one {@code QuestDB} per deployment, share it across threads, and
 * close it at shutdown. This example does the whole round trip on one thread:
 * borrow a {@link Sender} to ingest a couple of rows, then run a {@code SELECT}
 * to read them back.
 * <p>
 * For ingest-only and query-only variants, see
 * {@code com.example.sender.WsExample} and
 * {@code com.example.query.BasicQueryExample}.
 */
public class QuestDBExample {

    public static void main(String[] args) throws InterruptedException {
        // One handle for the whole deployment. close() (try-with-resources)
        // shuts the pools down and disconnects every underlying client.
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;");
             Query q = db.borrowQuery()) {

            // Ingest: borrow a Sender, write rows, close() flushes it and
            // returns it to the pool. The WebSocket is NOT torn down here --
            // a real disconnect only happens at db.close().
            try (Sender sender = db.borrowSender()) {
                sender.table("trades")
                        .symbol("symbol", "ETH-USD")
                        .symbol("side", "sell")
                        .doubleColumn("price", 2615.54)
                        .doubleColumn("amount", 0.00044)
                        .atNow();
                sender.table("trades")
                        .symbol("symbol", "BTC-USD")
                        .symbol("side", "sell")
                        .doubleColumn("price", 39269.98)
                        .doubleColumn("amount", 0.001)
                        .atNow();
            }

            // Query: one-shot SELECT. Borrow a Query handle from the egress
            // pool, submit the SQL, and await the returned Completion.
            // (Freshly ingested rows land in the WAL asynchronously, so an
            // immediate read-back may not see them yet -- that is expected.)
            Completion c = q.sql(
                    "SELECT symbol, side, price, amount FROM trades "
                            + "WHERE symbol = 'ETH-USD' LIMIT 10")
                    .handler(new PrintingHandler())
                    .submit();
            try {
                c.await(); // blocks until onEnd / onError has run
            } catch (QueryException e) {
                // Server reported an error (parse failure, schema mismatch, ...).
                System.err.printf("query failed: status=0x%02X %s%n",
                        e.getStatus() & 0xFF, e.getMessage());
            }
        }
    }

    /**
     * Prints each row. The {@code QwpColumnBatch} is valid only during the
     * {@code onBatch} callback -- copy values out if you need them later.
     */
    private static final class PrintingHandler implements QwpColumnBatchHandler {
        @Override
        public void onBatch(QwpColumnBatch batch) {
            batch.forEachRow(row -> System.out.printf(
                    "symbol=%s side=%s price=%.4f amount=%.5f%n",
                    row.getSymbol(0),
                    row.getSymbol(1),
                    row.getDoubleValue(2),
                    row.getDoubleValue(3)));
        }

        @Override
        public void onEnd(long totalRows) {
            System.out.println("done: " + totalRows + " rows");
        }

        @Override
        public void onError(byte status, String message) {
        }
    }
}
