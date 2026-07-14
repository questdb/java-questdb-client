package com.example.sender;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;

/**
 * Advanced pooled ingest: pool sizing, auto-flush, and acknowledgement waits.
 * <p>
 * Uses {@link QuestDB#builder()} to size the sender pool and set the
 * housekeeping timeouts, and carries the ingest auto-flush thresholds in the
 * connect string. After writing, it waits for the server to acknowledge the
 * batch with {@link Sender#flushAndGetSequence()} +
 * {@link Sender#awaitAckedFsn(long, long)} -- the QWP frame-sequence machinery
 * that lets you confirm durability before moving on.
 * <p>
 * For crash-durable buffering, add {@code sf_dir=/path} to the connect string
 * (store-and-forward): the send engine backs unacknowledged rows with
 * memory-mapped files that survive a producer restart. This is the pooled/QWP
 * counterpart to {@link HttpAdvancedExample}'s HTTP retry/auto-flush tuning.
 */
public class WsAdvancedExample {

    public static void main(String[] args) {
        try (QuestDB db = QuestDB.builder()
                // auto_flush_* are ingest-side connect-string keys: hand rows to
                // the send engine after 5000 rows or 100ms, whichever comes first.
                .fromConfig("ws::addr=localhost:9000;auto_flush_rows=5000;auto_flush_interval=100;")
                // Keep 2 senders warm, grow to 8 under bursty load.
                .senderPoolMin(2)
                .senderPoolMax(8)
                // Wait up to 10s for a free sender when the pool is at max.
                .acquireTimeoutMillis(10_000)
                // Reap senders idle for 60s (down to min); recycle any older
                // than 30 min the next time they go idle.
                .idleTimeoutMillis(60_000)
                .maxLifetimeMillis(30 * 60_000L)
                .build()) {

            try (Sender sender = db.borrowSender()) {
                for (int i = 0; i < 3_000; i++) {
                    sender.table("trades")
                            .symbol("symbol", "BTC-USD")
                            .doubleColumn("price", 39_000.0 + i)
                            .doubleColumn("amount", 0.001)
                            .atNow();
                }

                // Hand everything to the send engine and take the highest frame
                // sequence number (FSN) this flush published...
                long fsn = sender.flushAndGetSequence();
                // ...then block until the server has acknowledged up to it, or
                // 10s elapses. Over QWP the ack is real; on the legacy ILP
                // transports these are no-ops (fsn == -1, awaitAckedFsn returns
                // immediately).
                if (fsn >= 0 && !sender.awaitAckedFsn(fsn, 10_000)) {
                    System.err.println("timed out waiting for server ack of fsn=" + fsn);
                } else {
                    System.out.println("server acknowledged up to fsn=" + fsn);
                }
            }
        }
    }
}
