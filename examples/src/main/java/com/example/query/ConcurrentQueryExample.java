package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Sharing one {@link QuestDB} handle across many query threads.
 * <p>
 * The query side of the pool is thread-safe the same way ingest is (see
 * {@link com.example.sender.WsConcurrentIngestExample}): create one
 * {@code QuestDB}, hand the same instance to every thread, and let each call
 * {@link QuestDB#borrowQuery()}. Every borrow leases that thread its own
 * {@link io.questdb.client.Query} handle (one WebSocket + I/O thread) for the
 * lifetime of the borrow -- no external synchronization, one in-flight query
 * per handle. Each borrow leases a client from the query pool, so
 * {@code queryPoolSize} caps how many queries run in parallel; extra borrows
 * block on the acquire timeout until a client frees up. Close the handle
 * (try-with-resources) to return the client to the pool.
 * <p>
 * For several in-flight queries from a <em>single</em> thread, borrow one
 * handle per concurrent query -- see {@link MultiInFlightQueryExample}.
 */
public class ConcurrentQueryExample {

    private static final String[] SYMBOLS = {"ETH-USD", "BTC-USD", "SOL-USD", "ADA-USD"};

    public static void main(String[] args) throws InterruptedException {
        try (QuestDB db = QuestDB.builder()
                .fromConfig("ws::addr=localhost:9000;")
                .queryPoolSize(SYMBOLS.length)
                .build()) {

            Thread[] readers = new Thread[SYMBOLS.length];
            for (int i = 0; i < SYMBOLS.length; i++) {
                final String symbol = SYMBOLS[i];
                readers[i] = new Thread(() -> countTrades(db, symbol), "reader-" + symbol);
                readers[i].start();
            }
            for (Thread reader : readers) {
                reader.join();
            }
        }
    }

    private static void countTrades(QuestDB db, final String symbol) {
        try (Query q = db.borrowQuery()) {
            q.sql("SELECT count() FROM trades WHERE symbol = $1")
                    .binds(binds -> binds.setVarchar(0, symbol))
                    .handler(new QwpColumnBatchHandler() {
                        @Override
                        public void onBatch(QwpColumnBatch batch) {
                            batch.forEachRow(row -> System.out.println(symbol + " count = " + row.getLongValue(0)));
                        }

                        @Override
                        public void onEnd(long totalRows) {
                        }

                        @Override
                        public void onError(byte status, String message) {
                        }
                    })
                    .submit()
                    .await();
        } catch (QueryException e) {
            System.err.printf("%s query failed: status=0x%02X %s%n", symbol, e.getStatus() & 0xFF, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
