package com.example.query;

import io.questdb.client.Completion;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.Query;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Running several queries in flight at once from a single thread.
 * <p>
 * {@link QuestDB#query()} returns the same per-thread {@link Query} instance
 * every call, so it runs one query at a time. To hold multiple queries in
 * flight from one thread, call {@link QuestDB#newQuery()} for each: every call
 * allocates a fresh {@code Query}, and each {@code submit()} acquires its own
 * worker from the query pool. The pool's {@code query_pool_max} caps how many
 * run in parallel -- extra submits block on the acquire timeout until a worker
 * frees up.
 */
public class MultiInFlightQueryExample {

    public static void main(String[] args) throws InterruptedException {
        // Size the query pool to the concurrency we want.
        try (QuestDB db = QuestDB.builder()
                .fromConfig("ws::addr=localhost:9000;")
                .queryPoolSize(2)
                .build()) {

            // Two independent queries, each on its own fresh Query handle.
            Query q1 = db.newQuery()
                    .sql("SELECT count() FROM trades WHERE symbol = 'ETH-USD'")
                    .handler(new PrintingHandler("ETH-USD"));
            Query q2 = db.newQuery()
                    .sql("SELECT count() FROM trades WHERE symbol = 'BTC-USD'")
                    .handler(new PrintingHandler("BTC-USD"));

            // Submit both before awaiting either -- they run concurrently.
            Completion c1 = q1.submit();
            Completion c2 = q2.submit();

            awaitQuietly(c1);
            awaitQuietly(c2);
        }
    }

    private static void awaitQuietly(Completion c) throws InterruptedException {
        try {
            c.await();
        } catch (QueryException e) {
            System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
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
            System.err.printf("%s error: 0x%02X %s%n", label, status & 0xFF, message);
        }
    }
}
