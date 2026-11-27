package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

import java.util.concurrent.TimeUnit;

/**
 * Giving a query a timeout.
 * <p>
 * {@link Query#timeout(long, TimeUnit)} bounds the whole query, measured from
 * {@code submit()}; {@code query_timeout_ms} in the connection string sets a
 * default for every query. When the timeout expires the query is stopped --
 * by the server when it supports per-query timeouts, otherwise by a cancel the
 * client sends -- and {@code await()} throws a {@link QueryException} whose
 * {@link QueryException#isTimeout()} is {@code true}. The pooled connection
 * stays open, so the same handle goes on to run the next query on it.
 * <p>
 * Compare {@link QueryCancellationExample}: {@code await(timeout)} only bounds
 * how long the caller waits, the query keeps running until cancelled.
 */
public class QueryTimeoutExample {

    public static void main(String[] args) throws InterruptedException {
        // query_timeout_ms: default timeout for every query of this handle's pool.
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;query_timeout_ms=30000;");
             Query q = db.borrowQuery()) {

            q.sql("SELECT * FROM trades ORDER BY price")
                    .handler(new PrintingHandler())
                    // Tighter than the default, for this handle only.
                    .timeout(5, TimeUnit.SECONDS);
            try {
                q.submit().await();
                System.out.println("finished within the timeout");
            } catch (QueryException e) {
                if (!e.isTimeout()) {
                    throw e;
                }
                System.out.println("timed out: " + e.getMessage());
            }

            // The connection survived the timeout; run the next query on it.
            q.sql("SELECT count() FROM trades").timeout(0, TimeUnit.SECONDS).submit().await();
        }
    }

    private static final class PrintingHandler implements QwpColumnBatchHandler {
        @Override
        public void onBatch(QwpColumnBatch batch) {
            // Process rows... kept minimal here.
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
