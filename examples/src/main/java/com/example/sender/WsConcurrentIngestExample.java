package com.example.sender;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;

/**
 * Sharing one {@link QuestDB} handle across many producer threads.
 * <p>
 * This is what the pool is for. A single {@code QuestDB} is thread-safe: create
 * it once, hand the same instance to every thread, and let each take a
 * {@link Sender} from the pool. A {@code Sender} itself is <b>not</b>
 * thread-safe, so no two threads may touch the same one -- the pool hands each
 * thread its own.
 * <p>
 * Each producer thread takes its own sender with {@link QuestDB#borrowSender()}
 * and reuses it for the batch (see {@link WsExample}); close() flushes and
 * returns it to the pool. We size the sender pool to the producer count so no
 * thread ever waits on the acquire timeout.
 */
public class WsConcurrentIngestExample {

    private static final int PRODUCERS = 4;
    private static final int ROWS_PER_PRODUCER = 250;

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.builder()
                .fromConfig("ws::addr=localhost:9000;")
                .senderPoolSize(PRODUCERS)  // one warm sender per producer thread
                .build()) {

            Thread[] producers = new Thread[PRODUCERS];
            for (int p = 0; p < PRODUCERS; p++) {
                final int producerId = p;
                producers[p] = new Thread(() -> ingestBatch(db, producerId), "producer-" + p);
                producers[p].start();
            }
            for (Thread producer : producers) {
                producer.join();
            }
            System.out.printf("ingested %d rows across %d threads%n",
                    PRODUCERS * ROWS_PER_PRODUCER, PRODUCERS);
            // db.close() (try-with-resources) drains and disconnects every
            // pooled sender.
        }
    }

    private static void ingestBatch(QuestDB db, int producerId) {
        // Each thread borrows its own sender and reuses it for the batch;
        // close() (try-with-resources) flushes and returns it to the pool.
        try (Sender sender = db.borrowSender()) {
            String symbol = "SYM-" + producerId;
            for (int i = 0; i < ROWS_PER_PRODUCER; i++) {
                sender.table("trades")
                        .symbol("symbol", symbol)
                        .longColumn("producer", producerId)
                        .doubleColumn("price", 100.0 + i)
                        .atNow();
            }
            sender.flush();
        }
    }
}
