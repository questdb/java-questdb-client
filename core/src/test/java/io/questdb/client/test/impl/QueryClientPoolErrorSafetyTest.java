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

import io.questdb.client.QueryException;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.impl.QueryClientPool;
import io.questdb.client.impl.QueryWorker;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

// Error-safety of the three QueryClientPool creation paths the teardown-hardening
// fix widened from catch (RuntimeException) to catch (Throwable). The native
// build/connect path runs under -ea and can throw an Error (AssertionError,
// OutOfMemoryError); the old catches let that Error skip cleanup.
//
// QwpQueryClient is a concrete class with no fake seam, so these tests inject an
// Error at the real connect step via the public connectHook constructor.
// fromConfig()
// still runs for real, committing the NATIVE_DEFAULT scratch the cleanup must
// reclaim, so the memory assertions are meaningful.
public class QueryClientPoolErrorSafetyTest {

    // ws config that fromConfig() parses without opening a socket; the injected
    // connectHook replaces connect(), so the port is never dialled.
    private static final String CFG = "ws::addr=127.0.0.1:9000;";

    // Site: acquire() inner catch around client.connect(). createUnlocked() must
    // close the half-built client when connect throws an Error, otherwise the
    // field-initialised QwpBindValues scratch (NATIVE_DEFAULT) leaks.
    // RED: catch (RuntimeException) -> Error skips client.close() -> leak.
    // GREEN: catch (Throwable) -> client.close() runs -> no leak.
    @Test(timeout = 30_000)
    public void acquireDoesNotLeakNativeScratchOnErrorFromConnect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QueryClientPool pool = newPool(CFG, 0, 1, 250, alwaysThrow());
            try {
                long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                try {
                    pool.acquire();
                    Assert.fail("expected acquire() to propagate the injected Error");
                } catch (Throwable expected) {
                    // wrapped or raw -- the leak check is the discriminator
                }
                long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                Assert.assertEquals(
                        "acquire() leaked NATIVE_DEFAULT scratch on an Error from connect()",
                        baseline, after);
            } finally {
                pool.close();
            }
        });
    }

    // Site: acquire() outer catch around createUnlocked()/start(). An Error must
    // still run inFlightCreations--, otherwise the reserved slot is leaked and
    // (maxSize == 1) the pool is wedged forever.
    // RED: catch (RuntimeException) -> inFlightCreations stuck at 1.
    // GREEN: catch (Throwable) -> inFlightCreations restored to 0.
    @Test(timeout = 30_000)
    public void acquireRestoresInFlightCreationsOnErrorFromConnect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QueryClientPool pool = newPool(CFG, 0, 1, 250, alwaysThrow());
            try {
                try {
                    pool.acquire();
                    Assert.fail("expected acquire() to propagate the injected Error");
                } catch (Throwable expected) {
                    // expected
                }

                Assert.assertEquals(
                        "acquire() leaked an in-flight creation slot on an Error from connect()",
                        0, inFlightCreations(pool));

                // Corollary: capacity is usable again -- the next acquire() must
                // reach the creation path (and fail there) rather than time out.
                try {
                    pool.acquire();
                    Assert.fail("expected second acquire() to re-attempt creation");
                } catch (QueryException e) {
                    Assert.assertFalse(
                            "pool wedged: second acquire() timed out -> capacity permanently lost ("
                                    + e.getMessage() + ")",
                            e.getMessage() != null && e.getMessage().contains("timed out"));
                } catch (Throwable injectedAgain) {
                    // also fine: the Error surfaced again from the re-attempt
                }
            } finally {
                pool.close();
            }
        });
    }

    // Site: constructor prewarm outer catch. An Error mid-prewarm must run the
    // cleanup loop that shuts down already-built workers, otherwise the first
    // worker's client (NATIVE_DEFAULT) and I/O thread leak.
    // RED: catch (RuntimeException) -> first worker's client never closed.
    // GREEN: catch (Throwable) -> cleanup loop closes it -> no leak.
    @Test(timeout = 30_000)
    public void preWarmDoesNotLeakNativeScratchOnErrorFromConnect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
            // First connect() succeeds (no-op, leaves the client unconnected but
            // built); the second throws an Error mid-prewarm.
            AtomicInteger calls = new AtomicInteger();
            Consumer<QwpQueryClient> hook = client -> {
                if (calls.incrementAndGet() >= 2) {
                    throw new AssertionError("injected native connect failure");
                }
            };
            try {
                newPool(CFG, 2, 2, 250, hook);
                Assert.fail("expected prewarm to propagate the injected Error");
            } catch (Throwable expected) {
                // expected -- construction aborts
            }
            long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
            Assert.assertEquals(
                    "prewarm leaked NATIVE_DEFAULT scratch of an already-built worker on an Error",
                    baseline, after);
        });
    }

    // Site: acquire() outer catch around createUnlocked()/start(). When start()
    // throws *after* createUnlocked() returned a fully connected client, that
    // client (NATIVE_DEFAULT scratch + socket + I/O thread) must be torn down,
    // otherwise it is stranded -- nothing else references it. This is the
    // start()-throws path connectHook cannot reach: connect() runs (no-op here,
    // committing the scratch) and the failure is injected at thread start.
    // RED: catch does inFlightCreations-- but never created.shutdown() -> leak.
    // GREEN: catch calls created.shutdown() -> client.close() -> no leak.
    @Test(timeout = 30_000)
    public void acquireDoesNotLeakNativeScratchOnErrorFromStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QueryClientPool pool = newPool(CFG, 0, 1, 250, noConnect(), alwaysThrowStart());
            try {
                long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                try {
                    pool.acquire();
                    Assert.fail("expected acquire() to propagate the injected start Error");
                } catch (Throwable expected) {
                    // wrapped or raw -- the leak check is the discriminator
                }
                long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                Assert.assertEquals(
                        "acquire() leaked NATIVE_DEFAULT scratch on an Error from start()",
                        baseline, after);
                // The reservation must also be restored so the pool is not wedged.
                Assert.assertEquals(
                        "acquire() leaked an in-flight creation slot on an Error from start()",
                        0, inFlightCreations(pool));
            } finally {
                pool.close();
            }
        });
    }

    // Site: constructor prewarm. When start() throws after createUnlocked()
    // returned a fully connected client, the just-built worker is not yet in
    // `all`, so the cleanup loop (which only walks `all`) would skip it. Its
    // client (NATIVE_DEFAULT) must still be torn down.
    // RED: cleanup loop walks `all` only -> the worker that failed at start()
    //      is never closed -> leak.
    // GREEN: the pending-worker teardown closes it -> no leak.
    @Test(timeout = 30_000)
    public void preWarmDoesNotLeakNativeScratchOnErrorFromStart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
            // First worker is admitted to `all` (start is a no-op here -- the test
            // never runs queries, and an unstarted thread keeps the later
            // shutdown()'s join() instant); the second throws at start() after its
            // client is fully built. That second worker is the stranded one: it
            // never made it into `all`, so only the new pending-worker teardown
            // closes it. The assertion catches a leak of EITHER worker's scratch.
            AtomicInteger calls = new AtomicInteger();
            Consumer<QueryWorker> startHook = w -> {
                if (calls.incrementAndGet() >= 2) {
                    throw new AssertionError("injected thread-start failure");
                }
                // first worker: leave the dispatch thread unstarted (see above)
            };
            try {
                newPool(CFG, 2, 2, 250, noConnect(), startHook);
                Assert.fail("expected prewarm to propagate the injected start Error");
            } catch (Throwable expected) {
                // expected -- construction aborts
            }
            long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
            Assert.assertEquals(
                    "prewarm leaked NATIVE_DEFAULT scratch of a start()-failed worker",
                    baseline, after);
        });
    }

    // Site: acquire()'s closed-mid-creation branch. When acquire() re-locks
    // after building a fresh worker and finds the pool closed, it must tear the
    // worker down on its own thread -- outside the pool lock (shutdown() joins
    // the dispatch thread for up to SHUTDOWN_JOIN_MILLIS; cancelIfCurrent's
    // contract is that this lock is "held only briefly") -- and still reclaim
    // everything: the client's NATIVE_DEFAULT scratch and the creation slot.
    // RED (teardown dropped in the restructure): the scratch leaks and the
    //      baseline assertion fails.
    // GREEN: shutdown() runs after the lock is released -> no leak, accounting
    //        restored, acquire() surfaces the closed pool.
    @Test(timeout = 30_000)
    public void closedMidCreationTearsDownFreshWorkerWithoutLeak() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountDownLatch inConnect = new CountDownLatch(1);
            CountDownLatch releaseConnect = new CountDownLatch(1);
            Consumer<QwpQueryClient> connectHook = client -> {
                inConnect.countDown();
                try {
                    if (!releaseConnect.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("test never released the parked connect");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted in parked connect", e);
                }
            };
            // No-op startHook: the dispatch thread stays unstarted, so the
            // closed-mid-creation shutdown()'s interrupt+join is instant.
            QueryClientPool pool = newPool(CFG, 0, 1, 10_000, connectHook, w -> {
            });
            try {
                long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                AtomicReference<Throwable> acquireOutcome = new AtomicReference<>();
                Thread acquirer = new Thread(() -> {
                    try {
                        pool.acquire();
                    } catch (Throwable t) {
                        acquireOutcome.set(t);
                    }
                }, "mid-creation-acquirer");
                acquirer.start();
                Assert.assertTrue("acquirer never reached connect()",
                        inConnect.await(10, TimeUnit.SECONDS));

                // Close the pool while the worker build is in flight (the fresh
                // worker never entered `all`, so close()'s snapshot skips it),
                // then let the build finish: acquire() must observe `closed`,
                // tear the worker down on its own thread and throw.
                pool.close();
                releaseConnect.countDown();
                acquirer.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("acquirer did not finish", acquirer.isAlive());

                Assert.assertTrue(
                        "acquire() must surface the closed pool, got: " + acquireOutcome.get(),
                        acquireOutcome.get() instanceof QueryException
                                && String.valueOf(acquireOutcome.get().getMessage()).contains("closed"));
                long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
                Assert.assertEquals(
                        "closed-mid-creation acquire() leaked the fresh worker's NATIVE_DEFAULT scratch",
                        baseline, after);
                Assert.assertEquals("in-flight creation accounting must be restored",
                        0, inFlightCreations(pool));
            } finally {
                pool.close();
            }
        });
    }

    private static Consumer<QwpQueryClient> alwaysThrow() {
        return client -> {
            throw new AssertionError("injected native connect failure");
        };
    }

    // connectHook that connects nothing: fromConfig() has already committed the
    // NATIVE_DEFAULT scratch, so the client is half-built (scratch, no socket)
    // -- exactly the state createUnlocked() returns before start() runs.
    private static Consumer<QwpQueryClient> noConnect() {
        return client -> {
        };
    }

    private static Consumer<QueryWorker> alwaysThrowStart() {
        return worker -> {
            throw new AssertionError("injected thread-start failure");
        };
    }

    private static int inFlightCreations(QueryClientPool pool) {
        return pool.inFlightCreations();
    }

    private static QueryClientPool newPool(
            String cfg, int min, int max, long acquireMs, Consumer<QwpQueryClient> connectHook
    ) {
        return new QueryClientPool(cfg, min, max, acquireMs, Long.MAX_VALUE, Long.MAX_VALUE, connectHook);
    }

    private static QueryClientPool newPool(
            String cfg, int min, int max, long acquireMs,
            Consumer<QwpQueryClient> connectHook, Consumer<QueryWorker> startHook
    ) {
        return new QueryClientPool(cfg, min, max, acquireMs, Long.MAX_VALUE, Long.MAX_VALUE,
                connectHook, startHook);
    }
}
