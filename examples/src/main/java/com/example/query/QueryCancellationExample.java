package com.example.query;

import io.questdb.client.Completion;
import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

import java.util.concurrent.TimeUnit;

/**
 * Bounding a query with a deadline, and cancelling it.
 * <p>
 * {@link Completion#await(long, java.util.concurrent.TimeUnit)} returns
 * {@code false} instead of blocking forever -- the query keeps running. Call
 * {@link Completion#cancel()} to ask the server to stop; the handler then sees
 * {@code onError} with the cancel status ({@code 0x0A}), and a subsequent
 * blocking {@link Completion#await()} throws {@link QueryException} carrying
 * that status. If the query finished before the cancel landed, {@code await()}
 * returns normally -- {@code cancel()} is a no-op on an already-terminal query.
 */
public class QueryCancellationExample {

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;");
             Query q = db.borrowQuery()) {

            Completion c = q.sql(
                    "SELECT * FROM trades ORDER BY price")
                    .handler(new PrintingHandler())
                    .submit();

            // Give it 5 seconds; if it's still running, cancel and drain. await(...)
            // also rethrows a server error that lands before the deadline, so the
            // whole flow is wrapped once.
            try {
                if (!c.await(5, TimeUnit.SECONDS)) {
                    System.out.println("still running after 5s -- cancelling");
                    c.cancel();
                    try {
                        c.await();
                    } catch (QueryException cancelled) {
                        System.out.printf("cancelled: status=0x%02X%n", cancelled.getStatus() & 0xFF);
                    }
                } else {
                    System.out.println("finished within the deadline");
                }
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }
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
