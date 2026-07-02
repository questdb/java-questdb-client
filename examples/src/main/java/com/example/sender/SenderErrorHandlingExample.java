package com.example.sender;

import io.questdb.client.Sender;

/**
 * Structured ingestion-error reports via {@code SenderErrorHandler}.
 * <p>
 * WebSocket ingestion uses an asynchronous error model: batch rejections
 * (schema, parse, write, security errors) are delivered out of band, NOT thrown
 * from {@code flush()}. The pooled {@link io.questdb.client.QuestDB} facade does
 * not yet expose this callback, so to receive structured {@code SenderError}
 * reports you drop down to the low-level {@link Sender#builder(CharSequence)}
 * and register an {@code errorHandler(...)} -- this example is intentionally
 * NOT using the pool.
 * <p>
 * The handler runs on a dedicated dispatcher thread (never the I/O or producer
 * thread). It is for dead-lettering, alerting, and metrics. When a rejection
 * latches a {@code HALT} policy, the <i>next</i> producer-thread API call throws
 * {@code LineSenderServerException} -- that is where retry/abort logic belongs.
 */
public class SenderErrorHandlingExample {

    public static void main(String[] args) {
        try (Sender sender = Sender.builder("ws::addr=localhost:9000;")
                .errorHandler(error -> System.err.printf(
                        "rejected: category=%s policy=%s table=%s fsn=[%d..%d] msg=%s%n",
                        error.getCategory(),
                        error.getAppliedPolicy(),
                        error.getTableName(),
                        error.getFromFsn(),
                        error.getToFsn(),
                        error.getServerMessage()))
                .build()) {

            sender.table("trades")
                    .symbol("symbol", "ETH-USD")
                    .doubleColumn("price", 2615.54)
                    .atNow();

            // flush() returns once rows are handed to the send engine; a
            // rejection surfaces later on the errorHandler above, not here.
            sender.flush();
        }
    }
}
