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
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

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
        TestUtils.assertMemoryLeak(() -> {
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

            // cancel(gen) validates the generation under the pool lock, so the
            // worker needs a real pool to lock on (the worker thread is never
            // started, so no network or I/O thread is involved).
            QueryClientPool pool = new QueryClientPool(
                    "ws::addr=localhost:9000;",
                    /*minSize*/ 0, /*maxSize*/ 2,
                    /*acquireTimeoutMillis*/ 1_000L,
                    /*idleTimeoutMillis*/ Long.MAX_VALUE,
                    /*maxLifetimeMillis*/ Long.MAX_VALUE);
            try {
                // Live lease: generation 1 (one acquire), query in flight -> cancel(1)
                // must reach the client.
                try (QwpQueryClient live = QwpQueryClient.newPlainText("localhost", 9000)) {
                    QueryWorker w = new QueryWorker(live, pool, 0);
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
                    QueryWorker w = new QueryWorker(reused, pool, 0);
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
            } finally {
                pool.close();
            }
        });
    }

    /**
     * The TOCTOU the locked cancel closes: a cross-thread watchdog calls
     * {@code cancel(gen)} while its lease is live, but the lease goes stale (the
     * worker is released and re-borrowed) before the wire cancel fires. The
     * cancel must re-validate the generation atomically with the cancel, under
     * the pool lock, or it would abort the new borrower's query.
     * <p>
     * Driven deterministically: the test thread holds the pool lock, so the
     * watchdog's cancel parks inside the pool's generation re-check. We then
     * advance the generation (release + re-borrow) under the lock and release
     * it. The parked cancel must observe the new generation and drop. An
     * unlocked check-then-cancel would not park, would pass its check at the
     * still-live generation, and would fire the wire cancel.
     */
    @Test
    public void testConcurrentCancelDoesNotReachClientAfterReborrow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Method bump = QueryWorker.class.getDeclaredMethod("bumpGeneration");
            bump.setAccessible(true);
            Field queryF = QueryWorker.class.getDeclaredField("query");
            queryF.setAccessible(true);
            Class<?> queryImplClass = Class.forName("io.questdb.client.impl.QueryImpl");
            Field doneF = queryImplClass.getDeclaredField("done");
            doneF.setAccessible(true);
            Method cancel = queryImplClass.getDeclaredMethod("cancel", long.class);
            cancel.setAccessible(true);
            Field poolLockF = QueryClientPool.class.getDeclaredField("lock");
            poolLockF.setAccessible(true);

            QueryClientPool pool = new QueryClientPool(
                    "ws::addr=localhost:9000;",
                    /*minSize*/ 0, /*maxSize*/ 2,
                    /*acquireTimeoutMillis*/ 1_000L,
                    /*idleTimeoutMillis*/ Long.MAX_VALUE,
                    /*maxLifetimeMillis*/ Long.MAX_VALUE);
            QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000);
            try {
                final QueryWorker w = new QueryWorker(client, pool, 0);
                bump.invoke(w); // generation -> 1; the watchdog's lease captured 1
                final Object impl = queryF.get(w);
                doneF.setBoolean(impl, false); // a query is in flight

                ReentrantLock poolLock = (ReentrantLock) poolLockF.get(pool);
                final CountDownLatch atCancel = new CountDownLatch(1);
                final CountDownLatch cancelReturned = new CountDownLatch(1);
                final AtomicReference<Throwable> err = new AtomicReference<>();

                // Hold the pool lock so the watchdog's cancel cannot finish its
                // generation re-check + wire cancel until we let go.
                poolLock.lock();
                Thread watchdog = new Thread(() -> {
                    atCancel.countDown();
                    try {
                        cancel.invoke(impl, 1L); // lease generation captured at borrow = 1
                    } catch (Throwable t) {
                        err.set(t);
                    } finally {
                        cancelReturned.countDown();
                    }
                }, "watchdog-cancel");
                watchdog.start();
                Assert.assertTrue("watchdog must start", atCancel.await(5, TimeUnit.SECONDS));

                // With the locked cancel, cancel() parks on the pool lock and cannot
                // return while we hold it. An unlocked check-then-cancel would have
                // already fired the wire cancel and returned.
                Assert.assertFalse("cancel() must re-check the generation under the pool "
                                + "lock, so it cannot complete while the lock is held",
                        cancelReturned.await(200, TimeUnit.MILLISECONDS));

                // The lease goes stale underneath the parked cancel: released (-> 2)
                // and re-borrowed by a new owner (-> 3).
                bump.invoke(w);
                bump.invoke(w);
                poolLock.unlock();

                Assert.assertTrue("cancel() must return once the pool lock is free",
                        cancelReturned.await(5, TimeUnit.SECONDS));
                if (err.get() != null) {
                    throw new AssertionError("cancel() threw", err.get());
                }
                Assert.assertFalse("a cancel whose lease went stale while parked on the pool "
                                + "lock must NOT reach the client and abort the new borrower's query",
                        client.isPendingCancelForTest());
            } finally {
                client.close();
                pool.close();
            }
        });
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
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }
}
