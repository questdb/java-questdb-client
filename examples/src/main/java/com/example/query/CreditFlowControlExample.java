package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Byte-budgeted flow control for large streaming queries.
 * <p>
 * When a consumer is slow (throttled network, expensive per-row processing,
 * transient back-pressure in a downstream sink), you can put the stream under
 * explicit flow control by setting the {@code initial_credit} key in the
 * connect string. The server then streams at most that many bytes of
 * result payload before pausing; the client's I/O thread auto-replenishes by
 * the size of each batch as soon as the user's {@code onBatch} handler returns.
 * <p>
 * Benefits:
 * <ul>
 *   <li>Bounded client-side memory -- the client never holds more than
 *       roughly {@code initial_credit} bytes of unread result data, even if
 *       the handler pauses, sleeps, or does blocking I/O.</li>
 *   <li>Bounded server-side outstanding work -- the server parks its cursor
 *       when credit is exhausted instead of blasting bytes into a kernel
 *       buffer that a slow consumer can't drain.</li>
 *   <li>Back-pressure propagation that's explicit on the wire rather than
 *       relying solely on TCP window behaviour -- useful across proxies,
 *       load balancers, or WebSocket gateways that may or may not propagate
 *       TCP-level back-pressure cleanly.</li>
 * </ul>
 * <p>
 * Defaults ({@code initial_credit} unset, or {@code 0}): the stream
 * is unbounded; no CREDIT frames are emitted. This is the Phase-1 behaviour
 * and is the right choice for local / fast-network consumers that can keep
 * up with whatever the server emits.
 * <p>
 * Sizing guidance: make {@code initial_credit} at least a few times the batch
 * size (server emits batches up to a few hundred KB). A value like
 * {@code 262144} (256 KiB) is a reasonable starting point -- large enough
 * that the server can send a couple of batches ahead, small enough to cap
 * memory tightly on slow consumers.
 */
public class CreditFlowControlExample {

    public static void main(String[] args) throws InterruptedException {
        // 256 KiB send-ahead budget (initial_credit=262144). Server pauses once
        // it has streamed that many bytes of result payload; client auto-replenishes
        // by the byte-size of each batch once the handler releases it.
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;initial_credit=262144;")) {
            final long[] rowsSeen = {0};
            try (Query q = db.borrowQuery()) {
                q.sql(
                        "SELECT timestamp, price FROM trades WHERE timestamp > dateadd('d', -30, now())")
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                // Process the batch. If you block here (e.g. writing to a
                                // slow downstream), the server naturally parks until this
                                // returns and the credit-replenish frame catches up.
                                rowsSeen[0] += batch.getRowCount();
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                System.out.printf("done: rows=%d%n", rowsSeen[0]);
                            }

                            @Override
                            public void onError(byte status, String message) {
                            }
                        }
                ).submit().await();
            } catch (QueryException e) {
                System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
            }
        }
    }
}
