/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.client.test.example;

import io.questdb.client.Completion;
import io.questdb.client.QuestDB;
import io.questdb.client.Query;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatchHandler;

import java.util.concurrent.TimeUnit;

/**
 * Examples for the {@link QuestDB} facade -- the high-level handle that
 * pools both Senders (ingest) and query clients (egress).
 * <p>
 * Create one {@code QuestDB} per deployment, share it across threads, and
 * close it at shutdown. Borrows and releases are zero-allocation at steady
 * state; the per-thread {@link Query} handle is cached in a ThreadLocal.
 */
public class QuestDBExamples {

    public static void main(String[] args) throws Exception {
        // 1. Connect with a single configuration string for the whole cluster.
        //    Both sides run over QWP/WebSocket, so one ws:: string configures
        //    ingest and egress; list every node in one addr server list.
        try (QuestDB db = QuestDB.connect("ws::addr=node1:9000,node2:9000,node3:9000;")) {
            ingestWithBorrowedSender(db);
            queryOneShot(db);
            queryWithBinds(db);
            cancelExample(db);
        }

        // 2. Authenticated connect: token auth becomes a Bearer Authorization
        //    header on both the ingest and egress WebSocket upgrades.
        try (QuestDB db = QuestDB.connect(
                "wss::addr=db.questdb.cloud:9000;token=YOUR_TOKEN_HERE;")) {
            // ... use db ...
            try (Query q = db.borrowQuery()) {
                q.sql("SELECT 1").handler(new PrintingHandler()).submit().await();
            }
        }

        // 3. Custom pool sizing and timeouts via the builder. One cluster config
        //    (a single addr server list) drives both pools; use the builder to
        //    override pool/timeout defaults.
        try (QuestDB db = QuestDB.builder()
                .fromConfig("ws::addr=node1.cluster:9000,node2.cluster:9000;")
                .senderPoolSize(8)
                .queryPoolSize(4)
                .acquireTimeoutMillis(10_000)
                .build()) {
            // ... use db ...
            try (Query q = db.borrowQuery()) {
                q.sql("SELECT 1").handler(new PrintingHandler()).submit().await();
            }
        }
    }

    /**
     * Cancel mid-query: the handler observes {@code onError} with the cancel
     * status, and {@code await()} throws {@code QueryException} with that
     * status. If the query completed before cancel landed, {@code await()}
     * returns normally; either way the Completion reaches a terminal state.
     */
    static void cancelExample(QuestDB db) {
        try (Query q = db.borrowQuery()) {
            Completion c = q.sql("SELECT * FROM big_table ORDER BY ts")
                    .handler(new PrintingHandler())
                    .submit();
            // ... some condition decides to abort ...
            c.cancel();
            try {
                c.await();
            } catch (Exception cancelled) {
                // expected when cancel won the race
            }
        }
    }

    /**
     * Borrowed Sender: leases one from the pool, flushes pending rows on
     * close(), returns to the pool. Use this for short-lived or event-loop
     * callers where pinning a Sender to a thread is not appropriate.
     */
    static void ingestWithBorrowedSender(QuestDB db) {
        try (Sender s = db.borrowSender()) {
            s.table("trades")
                    .symbol("symbol", "BTC-USD")
                    .doubleColumn("price", 42_500.50)
                    .longColumn("size", 100)
                    .atNow();
            // close() flushes -- no need to call flush() yourself.
        }
    }

    /**
     * One-shot query, no bind parameters. Borrow a {@link Query} handle,
     * submit, await, and close it (try-with-resources). {@code submit()}
     * returns a {@link Completion} you can {@code await()} synchronously, time
     * out on, or cancel.
     */
    static void queryOneShot(QuestDB db) throws InterruptedException {
        try (Query q = db.borrowQuery()) {
            q.sql("SELECT price FROM trades WHERE symbol = 'BTC-USD' LIMIT 10")
                    .handler(new PrintingHandler())
                    .submit()
                    .await();
        }
    }

    /**
     * Query with bind parameters. Borrow a {@link Query} handle, then set SQL,
     * binds (via QwpBindSetter), and handler.
     * <p>
     * The same SQL text reuses the server's compiled-factory cache -- bind
     * values supply the per-call inputs. Interpolating values into the SQL
     * string defeats that cache.
     */
    static void queryWithBinds(QuestDB db) throws InterruptedException {
        try (Query q = db.borrowQuery()) {
            q.sql("SELECT price FROM trades WHERE symbol = $1 LIMIT $2")
                    .binds(binds -> {
                        binds.setVarchar(0, "BTC-USD");
                        binds.setLong(1, 10L);
                    })
                    .handler(new PrintingHandler());
            Completion c = q.submit();
            // Optional timeout: returns false if the query is still in flight.
            if (!c.await(5, TimeUnit.SECONDS)) {
                c.cancel();
                c.await();
            }
        }
    }

    /**
     * Minimal handler: prints each row's first column. Real applications
     * implement a stateful handler that aggregates batches; the same
     * instance can be reused across submits for zero allocation.
     */
    private static final class PrintingHandler implements QwpColumnBatchHandler {
        @Override
        public void onBatch(QwpColumnBatch batch) {
            for (int r = 0; r < batch.getRowCount(); r++) {
                System.out.println(batch.getDoubleValue(0, r));
            }
        }

        @Override
        public void onEnd(long totalRows) {
            System.out.println("done: " + totalRows + " rows");
        }

        @Override
        public void onError(byte status, String message) {
            System.err.println("egress error: status=" + status + ", message=" + message);
        }
    }
}
