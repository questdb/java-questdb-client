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
 * These dedicated producer threads use {@link QuestDB#sender()} (thread-affine):
 * the first call on a thread pins a sender to it, and the tight loop reuses that
 * same instance with zero borrow overhead. Short-lived or event-loop callers
 * would use {@link QuestDB#borrowSender()} instead (see {@link WsExample}). We
 * size the sender pool to the producer count so no thread ever waits on the
 * acquire timeout.
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
            // pooled sender, including the ones pinned to the producer threads.
        }
    }

    private static void ingestBatch(QuestDB db, int producerId) {
        // First sender() call on this thread pins a sender; the loop reuses it.
        Sender sender = db.sender();
        String symbol = "SYM-" + producerId;
        for (int i = 0; i < ROWS_PER_PRODUCER; i++) {
            sender.table("trades")
                    .symbol("symbol", symbol)
                    .longColumn("producer", producerId)
                    .doubleColumn("price", 100.0 + i)
                    .atNow();
        }
        sender.flush();
        // These threads are dedicated and about to exit, so db.close() reaps
        // their pinned senders. On a thread borrowed from a foreign pool (a
        // Netty event loop, a servlet container) call db.releaseSender() here
        // before handing the thread back.
    }
}
