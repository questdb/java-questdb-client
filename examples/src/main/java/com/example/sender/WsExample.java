package com.example.sender;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;

/**
 * Ingest over QWP (WebSocket) through the pooled {@link QuestDB} facade.
 * <p>
 * The pooled {@link Sender} is the recommended way to ingest: one
 * {@code QuestDB} handle, created once and shared across threads, owns a pool
 * of senders that stay connected between uses. Take one with
 * {@link QuestDB#borrowSender()} -- a lease per use: {@code close()} flushes
 * pending rows and returns the sender to the pool (it does NOT disconnect).
 * Borrow one per producer thread, or once per batch, and close it when done.
 * <p>
 * A {@code Sender} is not thread-safe, so no two threads may share one -- the
 * pool hands each thread its own. Contrast with the low-level ILP examples in
 * this package ({@link TcpExample}, {@link HttpExample}, ...), which open a
 * dedicated {@code Sender.fromConfig(...)} connection per use over HTTP/TCP.
 */
public class WsExample {

    public static void main(String[] args) {
        try (QuestDB db = QuestDB.connect("ws::addr=localhost:9000;")) {

            // Borrowed: lease per use. try-with-resources close() flushes and
            // returns the sender to the pool for reuse.
            try (Sender sender = db.borrowSender()) {
                sender.table("trades")
                        .symbol("symbol", "ETH-USD")
                        .symbol("side", "sell")
                        .doubleColumn("price", 2615.54)
                        .doubleColumn("amount", 0.00044)
                        .atNow();
            }

            // Borrow one sender and reuse it for the whole batch. close()
            // (try-with-resources) flushes pending rows and returns the sender
            // to the pool -- the WebSocket is NOT torn down here.
            try (Sender sender = db.borrowSender()) {
                for (int i = 0; i < 1_000; i++) {
                    sender.table("trades")
                            .symbol("symbol", "BTC-USD")
                            .symbol("side", "buy")
                            .doubleColumn("price", 39269.98 + i)
                            .doubleColumn("amount", 0.001)
                            .atNow();
                }
                // Hand the buffered rows to the send engine. Auto-flush would
                // also do this on the row/time thresholds; an explicit flush()
                // makes the batch boundary deterministic. close() flushes too,
                // so this call is optional here.
                sender.flush();
            }

            // db.close() (try-with-resources) drains and disconnects every
            // pooled sender.
        }
    }
}
