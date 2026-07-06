package com.example.sender;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;

/**
 * Crash-durable ingest with store-and-forward ({@code sf_dir}).
 * <p>
 * Ingestion is asynchronous in every mode: {@code flush()} hands rows to a
 * background send engine that delivers them and tracks the server's
 * acknowledgements. {@code sf_dir} changes <b>where the engine keeps
 * unacknowledged rows</b>:
 * <ul>
 *   <li><b>Memory mode</b> (default, no {@code sf_dir}): unacked rows live in
 *       RAM. The engine reconnects across transient server outages and replays
 *       them, but they die with the sender <i>process</i>.</li>
 *   <li><b>Store-and-forward</b> ({@code sf_dir} set): the engine backs its
 *       buffer with memory-mapped files, so unacked rows survive a producer
 *       <i>process restart</i> -- on the next startup it recovers the tail from
 *       disk and replays it once the server is reachable.</li>
 * </ul>
 * {@code request_durable_ack=on} (Enterprise + replication) makes the ack wait
 * for the durable upload to object storage rather than the ordinary WAL commit.
 */
public class WsStoreAndForwardExample {

    public static void main(String[] args) {
        try (QuestDB db = QuestDB.connect(
                "ws::addr=localhost:9000;"
                        + "sf_dir=/var/lib/questdb/sf;"
                        + "request_durable_ack=on;")) {

            try (Sender sender = db.borrowSender()) {
                sender.table("trades")
                        .symbol("symbol", "ETH-USD")
                        .doubleColumn("price", 2615.54)
                        .atNow();

                // Confirm durability before moving on: flush, take the highest
                // published frame sequence number (FSN), then block until the
                // server acknowledges up to it. With request_durable_ack=on the
                // ack advances only after the durable object-storage upload.
                long fsn = sender.flushAndGetSequence();
                if (fsn >= 0 && !sender.awaitAckedFsn(fsn, 30_000)) {
                    System.err.println("timed out waiting for durable ack of fsn=" + fsn);
                }
            }
        }

        // Ephemeral / autoscaled producers on a shared volume: give each
        // instance a distinct sender_id so their sf directories don't collide,
        // and drain_orphans=on so a survivor replays a crashed peer's rows:
        //
        //   ws::addr=db:9000;sf_dir=/shared/qdb-sf;sender_id=ingest-a;drain_orphans=on;
    }
}
