package com.example.query;

import io.questdb.client.Query;
import io.questdb.client.QueryException;
import io.questdb.client.QuestDB;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

/**
 * Opting into zstd compression of {@code RESULT_BATCH} frames.
 * <p>
 * QWP can compress the body of each result batch with zstd. Compression is
 * negotiated once at WebSocket upgrade time via the
 * {@code X-QWP-Accept-Encoding} header; once agreed, every batch on the
 * connection ships compressed (unless the compressed form would be larger
 * than the raw form, in which case the server sends that specific batch
 * raw).
 * <p>
 * Supported {@code compression=} values:
 * <ul>
 *   <li>{@code auto} (default) -- advertise {@code zstd,raw}. The server
 *       uses zstd when it supports it and falls back to raw otherwise.
 *       Good default for most apps.</li>
 *   <li>{@code zstd} -- same advertisement as {@code auto}. Makes intent
 *       explicit in the connection string.</li>
 *   <li>{@code raw} -- skip compression entirely. No {@code X-QWP-Accept-Encoding}
 *       header is sent; the server ships batches uncompressed.</li>
 * </ul>
 * <p>
 * {@code compression_level=N} is a hint passed to the server; the server
 * clamps it to {@code [1, 9]} because zstd levels 10 and above drop below
 * ~20 MB/s compress speed and would pin a worker thread under a heavy load.
 * The default is 1 -- minimal server-side CPU; compression ratio gain at
 * higher levels is usually small for typical columnar payloads, so raise
 * the level only when you measure a real improvement and have the headroom.
 * <p>
 * When to turn it on:
 * <ul>
 *   <li>Cross-region, cellular, or otherwise bandwidth-constrained clients.</li>
 *   <li>Highly repetitive data: rotating symbols, low-cardinality enums,
 *       adjacent timestamps. Expect 3-10x shrinkage.</li>
 *   <li>You'd rather spend server CPU than wire bandwidth.</li>
 * </ul>
 * When to leave it off ({@code compression=raw}):
 * <ul>
 *   <li>Co-located client + server (localhost, same rack) -- the NIC is not
 *       the bottleneck; zstd eats CPU you could spend parsing results.</li>
 *   <li>Pre-compressed or encrypted-looking payloads -- BINARY columns
 *       holding images, compressed JSON, etc. zstd won't shrink them and
 *       you pay the CPU for nothing.</li>
 * </ul>
 */
public final class CompressionExample {

    private CompressionExample() {
    }

    public static void main(String[] args) throws InterruptedException {
        // compression=zstd at a mid-tier level for a typical WAN consumer.
        try (QuestDB db = QuestDB.connect(
                "ws::addr=127.0.0.1:9000;compression=zstd;compression_level=3;")) {

            long[] rows = {0};

            try (Query q = db.borrowQuery()) {
                q.sql(
                        "SELECT symbol, price, timestamp "
                                + "FROM trades WHERE timestamp IN yesterday()")
                        .handler(new QwpColumnBatchHandler() {
                            @Override
                            public void onBatch(QwpColumnBatch batch) {
                                // The decoder hands us a view over the decompressed
                                // bytes; the rest of the row-consuming code is
                                // identical whether or not compression is on.
                                rows[0] += batch.getRowCount();
                            }

                            @Override
                            public void onEnd(long totalRows) {
                                System.out.printf("done: %d rows%n", totalRows);
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
