package com.example.query;

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
 * {@link QuestDB#query()}. Every call returns that thread's own cached
 * {@link io.questdb.client.Query} instance -- no external synchronization, no
 * per-query allocation, one in-flight query per thread. Each {@code submit()}
 * acquires a worker from the query pool, so {@code queryPoolSize} caps how
 * many queries run in parallel; extra submits block on the acquire timeout
 * until a worker frees up.
 * <p>
 * For several in-flight queries from a <em>single</em> thread, see
 * {@link MultiInFlightQueryExample} ({@link QuestDB#newQuery()}).
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
        try {
            db.query()
                    .sql("SELECT count() FROM trades WHERE symbol = $1")
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
                            System.err.printf("%s error: 0x%02X %s%n", symbol, status & 0xFF, message);
                        }
                    })
                    .submit()
                    .await();
        } catch (QueryException e) {
            System.err.printf("query failed: status=0x%02X %s%n", e.getStatus() & 0xFF, e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
