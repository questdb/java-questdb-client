package com.example.sender;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;

/**
 * Multi-endpoint ingest failover (Enterprise).
 * <p>
 * List several hosts in {@code addr}, comma-separated. The sender tries them in
 * order and settles on whichever is the current primary -- ingestion always
 * writes to the primary, and replicas reject a write connection. Failing over
 * to a different host therefore helps once that host becomes the primary (for
 * example after the old primary goes down and a replica is promoted).
 * <p>
 * If the connection to the primary drops, the sender reconnects on its own with
 * exponential backoff and resumes where it left off. Whether unacknowledged
 * rows survive a <i>sender</i> failure depends on
 * {@link WsStoreAndForwardExample store-and-forward}. Multi-host failover
 * requires QuestDB Enterprise (the replicas come from Enterprise replication).
 */
public class WsFailoverExample {

    public static void main(String[] args) {
        try (QuestDB db = QuestDB.connect(
                "ws::addr=db-primary:9000,db-replica-1:9000,db-replica-2:9000;"
                        + "reconnect_max_duration_millis=300000;"    // keep retrying one outage up to 5 min
                        + "reconnect_initial_backoff_millis=100;"    // wait before the first retry
                        + "reconnect_max_backoff_millis=5000;"       // cap the backoff
                        + "initial_connect_retry=on;")) {            // retry the very first connect too

            try (Sender sender = db.borrowSender()) {
                sender.table("trades")
                        .symbol("symbol", "ETH-USD")
                        .doubleColumn("price", 2615.54)
                        .atNow();
                sender.flush();
            }
        }
    }
}
