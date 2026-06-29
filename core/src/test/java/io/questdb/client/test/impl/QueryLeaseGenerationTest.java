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

package io.questdb.client.test.impl;

import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.impl.QueryClientPool;
import io.questdb.client.impl.QueryWorker;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;

/**
 * Regression tests for M1: a stale {@code Query} lease (held after close, or a
 * cached {@code Completion}) must not disturb a later borrow of the same
 * worker. The reused per-worker {@code QueryImpl} alone cannot distinguish a
 * stale handle from a live one -- the fix stamps each borrow with a monotonic
 * generation under the pool lock and validates it on close/cancel/release.
 * <p>
 * These exercise the package-private internals by reflection (the same
 * white-box style as the other tests in this package). They construct workers
 * with a non-connected {@code newPlainText} client and never start the worker
 * thread, so no network or I/O thread is involved.
 */
public class QueryLeaseGenerationTest {

    /**
     * A stale {@code Completion.cancel()} (its lease long since released and the
     * worker re-borrowed) must NOT reach the worker's client -- otherwise it
     * would cancel whatever query the current borrower is running. We observe
     * "reached the client" via the client's pending-cancel latch, which
     * {@code QwpQueryClient.cancel()} sets first thing.
     */
    @Test
    public void testStaleCancelDoesNotReachClient() throws Exception {
        Class<?> workerClass = Class.forName("io.questdb.client.impl.QueryWorker");
        Class<?> queryImplClass = Class.forName("io.questdb.client.impl.QueryImpl");
        Method bump = workerClass.getDeclaredMethod("bumpGeneration");
        bump.setAccessible(true);
        Field queryF = workerClass.getDeclaredField("query");
        queryF.setAccessible(true);
        Field doneF = queryImplClass.getDeclaredField("done");
        doneF.setAccessible(true);
        Method cancel = queryImplClass.getDeclaredMethod("cancel", long.class);
        cancel.setAccessible(true);

        // Live lease: generation 1 (one acquire), query in flight -> cancel(1)
        // must reach the client.
        try (QwpQueryClient live = QwpQueryClient.newPlainText("localhost", 9000)) {
            QueryWorker w = new QueryWorker(live, null, 0);
            bump.invoke(w); // generation -> 1 (acquire stamp)
            Object impl = queryF.get(w);
            doneF.setBoolean(impl, false); // pretend a submit is in flight
            cancel.invoke(impl, 1L);
            Assert.assertTrue("cancel() on the live lease must reach the client",
                    live.isPendingCancelForTest());
        }

        // Stale lease: the worker was borrowed (gen 1), released and re-borrowed
        // (gen now 3). A cancel from the old lease (gen 1) must be dropped, even
        // though the current query is in flight.
        try (QwpQueryClient reused = QwpQueryClient.newPlainText("localhost", 9000)) {
            QueryWorker w = new QueryWorker(reused, null, 0);
            bump.invoke(w); // -> 1 (first acquire)
            bump.invoke(w); // -> 2 (release)
            bump.invoke(w); // -> 3 (second acquire by a new borrower)
            Object impl = queryF.get(w);
            doneF.setBoolean(impl, false); // the new borrower's query is in flight
            cancel.invoke(impl, 1L); // stale lease cancels
            Assert.assertFalse("a stale lease's cancel() must NOT reach the client and "
                            + "cancel a different borrower's in-flight query",
                    reused.isPendingCancelForTest());
        }
    }

    /**
     * The pool-wide blast radius of M1: a stale (duplicate / post-reborrow)
     * release must never enqueue a worker that a live borrower owns, otherwise
     * the worker sits in {@code available} twice and is handed to two borrowers
     * at once. The generation captured at borrow time, re-checked under the pool
     * lock, makes this impossible.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testStaleReleaseDoesNotEnqueueWorkerTwice() throws Exception {
        Class<?> poolClass = Class.forName("io.questdb.client.impl.QueryClientPool");
        Method release = poolClass.getDeclaredMethod("release", QueryWorker.class, long.class);
        release.setAccessible(true);
        Field availableF = poolClass.getDeclaredField("available");
        availableF.setAccessible(true);
        Method bump = QueryWorker.class.getDeclaredMethod("bumpGeneration");
        bump.setAccessible(true);
        Method generation = QueryWorker.class.getDeclaredMethod("generation");
        generation.setAccessible(true);

        QueryClientPool pool = new QueryClientPool(
                "ws::addr=localhost:9000;",
                /*minSize*/ 0, /*maxSize*/ 2,
                /*acquireTimeoutMillis*/ 1_000L,
                /*idleTimeoutMillis*/ Long.MAX_VALUE,
                /*maxLifetimeMillis*/ Long.MAX_VALUE);
        QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000);
        try {
            ArrayDeque<QueryWorker> available = (ArrayDeque<QueryWorker>) availableF.get(pool);
            QueryWorker w = new QueryWorker(client, pool, 0);

            // acquire #1 stamps generation 1; the lease (A) captures 1.
            bump.invoke(w);
            Assert.assertEquals(1L, generation.invoke(w));

            // close A -> release(w, 1): matches, enqueues once.
            release.invoke(pool, w, 1L);
            Assert.assertEquals("valid release must enqueue the worker once", 1, available.size());

            // close A again (duplicate, e.g. explicit close + try-with-resources)
            // -> release(w, 1): generation already bumped to 2, so it is dropped.
            release.invoke(pool, w, 1L);
            Assert.assertEquals("duplicate release of the same lease must be dropped",
                    1, available.size());

            // acquire #2 hands the worker to a new borrower (B): pull it out and
            // stamp generation 3.
            available.pollFirst();
            bump.invoke(w);
            Assert.assertEquals(3L, generation.invoke(w));

            // A stray close from the long-dead lease A -> release(w, 1): dropped,
            // so B's worker is NOT re-enqueued while B still owns it.
            release.invoke(pool, w, 1L);
            Assert.assertEquals("a post-reborrow stale release must NOT enqueue the "
                            + "worker while another borrower owns it",
                    0, available.size());

            // B's own close -> release(w, 3): matches, enqueues legitimately.
            release.invoke(pool, w, 3L);
            Assert.assertEquals("the current borrower's release must still work",
                    1, available.size());
        } finally {
            client.close();
            pool.close();
        }
    }
}
