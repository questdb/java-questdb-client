package com.example.query;

import io.questdb.client.Completion;
import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Running several queries in flight at once from a single thread.
 * <p>
 * A single {@link Query} handle is single-flight: it runs one query at a time.
 * To hold multiple queries in flight from one thread, borrow one handle per
 * concurrent query via {@link QuestDB#borrowQuery()}: each handle leases its
 * own pooled client (one WebSocket + I/O thread), so their submits run in
 * parallel. The pool's {@code query_pool_max} caps how many run at once --
 * extra borrows block on the acquire timeout until a client frees up. Close
 * every handle (try-with-resources) to return the clients to the pool.
 */
public class MultiInFlightQueryExample {

    public static void main(String[] args) throws InterruptedException {
        // Size the query pool to the concurrency we want.
        try (QuestDB db = QuestDB.builder()
                .fromConfig("ws::addr=localhost:9000;")
                .queryPoolSize(2)
                .build()) {

            // Two independent queries, each on its own borrowed Query handle.
            try (Query q1 = db.borrowQuery();
                 Query q2 = db.borrowQuery()) {
                q1.sql("SELECT count() FROM trades WHERE symbol = 'ETH-USD'")
                        .handler(new PrintingHandler("ETH-USD"));
                q2.sql("SELECT count() FROM trades WHERE symbol = 'BTC-USD'")
                        .handler(new PrintingHandler("BTC-USD"));

                // Submit both before awaiting either -- they run concurrently.
                Completion c1 = q1.submit();
                Completion c2 = q2.submit();

                awaitQuietly("ETH-USD", c1);
                awaitQuietly("BTC-USD", c2);
            }
        }
    }

    private static void awaitQuietly(String label, Completion c) throws InterruptedException {
        try {
            c.await();
        } catch (QueryException e) {
            System.err.printf("%s query failed: status=0x%02X %s%n", label, e.getStatus() & 0xFF, e.getMessage());
        }
    }

    private static final class PrintingHandler implements QwpColumnBatchHandler {
        private final String label;

        PrintingHandler(String label) {
            this.label = label;
        }

        @Override
        public void onBatch(QwpColumnBatch batch) {
            batch.forEachRow(row -> System.out.println(label + " count = " + row.getLongValue(0)));
        }

        @Override
        public void onEnd(long totalRows) {
        }

        @Override
        public void onError(byte status, String message) {
        }
    }
}
