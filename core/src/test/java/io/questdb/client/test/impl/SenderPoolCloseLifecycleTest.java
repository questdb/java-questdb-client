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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.impl.SenderPool;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

/**
 * C1 regression tests: {@link SenderPool#close()} must NEVER tear down a
 * borrowed delegate. A producer thread may be inside it (mid-append,
 * mid-flush); closing the delegate from the pool thread flushes table buffers
 * that thread is mutating and then frees their native memory under its feet --
 * a use-after-free / SEGV, not an exception. The contract under test:
 * <ul>
 * <li>close() waits boundedly (acquireTimeoutMillis) for outstanding leases;</li>
 * <li>a lease returned during the wait has its delegate torn down exactly once,
 * on the returning borrower's thread, and close() does not return before that
 * teardown completes;</li>
 * <li>a lease still outstanding at the deadline is left alive (leak over
 * SEGV), and its delegate is torn down whenever the lease finally closes;</li>
 * <li>with no leases outstanding, close() does not wait at all.</li>
 * </ul>
 * Mirrors the egress twin, {@code QueryWorker.shutdown()}'s bounded
 * interrupt+join before {@code client.close()}.
 * <p>
 * Sender is an interface, so delegates are faked with a Proxy injected via the
 * package-private senderFactory constructor (same seam as
 * {@link SenderPoolErrorSafetyTest}).
 */
public class SenderPoolCloseLifecycleTest {

    // Non-SF http config: fromConfig is never reached (the factory replaces the
    // build), but the constructor's eager config probe must still parse it.
    private static final String CFG = "http::addr=127.0.0.1:1;protocol_version=2;auto_flush=off;";

    @Test(timeout = 30_000)
    public void closeNeverTearsDownBorrowedDelegateUnderLiveProducer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicInteger delegateCloses = new AtomicInteger();
            AtomicReference<Thread> closerThread = new AtomicReference<>();
            CountDownLatch inAppend = new CountDownLatch(1);
            CountDownLatch releaseAppend = new CountDownLatch(1);
            IntFunction<Sender> factory = slotIndex ->
                    fakeSender(delegateCloses, closerThread, inAppend, releaseAppend);

            try (SenderPool pool = newPool(CFG, 1, 1, /*acquireMs*/ 200, factory)) {
                Sender lease = pool.borrow();

                // T1: park INSIDE a data-plane call on the delegate -- the
                // exact C1 interleaving (mid-longColumn append into native
                // table buffers) -- then close the lease once released.
                Thread producer = new Thread(() -> {
                    lease.longColumn("v", 1L);
                    lease.close();
                }, "c1-producer");
                producer.start();
                Assert.assertTrue("producer never reached the delegate",
                        inAppend.await(10, TimeUnit.SECONDS));

                // T2: pool close with the producer live inside the delegate.
                // Must wait out the lease budget, then return WITHOUT touching
                // the borrowed delegate.
                long startNanos = System.nanoTime();
                pool.close();
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                Assert.assertEquals(
                        "close() tore down a borrowed delegate under a live producer (C1: use-after-free/SEGV)",
                        0, delegateCloses.get());
                Assert.assertTrue(
                        "close() must wait out the lease budget before giving up, waited only " + elapsedMs + "ms",
                        elapsedMs >= 150);

                // The straggler lease comes home: teardown is delegated to the
                // returning borrower's thread, exactly once.
                releaseAppend.countDown();
                producer.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("producer did not finish", producer.isAlive());
                Assert.assertEquals(
                        "the returning lease must tear down its delegate exactly once",
                        1, delegateCloses.get());
                Assert.assertSame(
                        "teardown must run on the returning borrower's thread (single-owner teardown)",
                        producer, closerThread.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void closeWaitsForLeaseReturnAndForItsTeardownToComplete() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicInteger delegateCloses = new AtomicInteger();
            IntFunction<Sender> factory = slotIndex ->
                    fakeSender(delegateCloses, new AtomicReference<>(), null, null);

            // Generous budget: the wait must end when the lease comes home,
            // not when the budget runs out.
            try (SenderPool pool = newPool(CFG, 1, 1, /*acquireMs*/ 10_000, factory)) {
                Sender lease = pool.borrow();
                Thread returner = new Thread(() -> {
                    try {
                        Thread.sleep(150);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    lease.close();
                }, "c1-returner");
                returner.start();

                long startNanos = System.nanoTime();
                pool.close();
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                // pendingLeaseTeardowns contract: when close() returns, the
                // returned lease's delegate teardown has COMPLETED -- not
                // merely started -- so the count is deterministically 1.
                Assert.assertEquals(
                        "close() must not return before the returned lease's delegate teardown completes",
                        1, delegateCloses.get());
                Assert.assertTrue(
                        "close() must return promptly once the lease is home, not burn the full budget; took "
                                + elapsedMs + "ms",
                        elapsedMs < 9_000);
                returner.join(TimeUnit.SECONDS.toMillis(10));
            }
        });
    }

    @Test(timeout = 30_000)
    public void closeIsBoundedEvenWithInfiniteAcquireTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicInteger delegateCloses = new AtomicInteger();
            AtomicReference<Thread> closerThread = new AtomicReference<>();
            IntFunction<Sender> factory = slotIndex ->
                    fakeSender(delegateCloses, closerThread, null, null);

            // Long.MAX_VALUE acquire timeout is a legitimate BORROW policy
            // ("block until a slot frees") -- it must NOT unbound shutdown.
            // close()'s lease-wait is hard-capped at
            // SenderPool.MAX_CLOSE_LEASE_WAIT_MILLIS (5s, mirroring the
            // egress QueryWorker.SHUTDOWN_JOIN_MILLIS); without the cap this
            // test would hang until its 30s timeout.
            try (SenderPool pool = new SenderPool(
                    CFG, 1, 1, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, factory)) {
                Sender lease = pool.borrow();

                long startNanos = System.nanoTime();
                pool.close();
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                Assert.assertTrue(
                        "close() must exhaust the lease-wait cap before giving up, waited only "
                                + elapsedMs + "ms",
                        elapsedMs >= 4_000);
                Assert.assertTrue(
                        "close() must be bounded by the cap even with an infinite acquire timeout, took "
                                + elapsedMs + "ms",
                        elapsedMs < 20_000);
                Assert.assertEquals(
                        "the borrowed delegate must be left alive at the deadline (leak over SEGV)",
                        0, delegateCloses.get());

                // The straggler still reclaims cleanly when it finally returns.
                lease.close();
                Assert.assertEquals("late return must tear down the delegate exactly once",
                        1, delegateCloses.get());
                Assert.assertSame("teardown must run on the returning thread",
                        Thread.currentThread(), closerThread.get());
            }
        });
    }

    @Test(timeout = 30_000)
    public void closeWithNoOutstandingLeasesDoesNotWait() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicInteger delegateCloses = new AtomicInteger();
            AtomicReference<Thread> closerThread = new AtomicReference<>();
            IntFunction<Sender> factory = slotIndex ->
                    fakeSender(delegateCloses, closerThread, null, null);

            try (SenderPool pool = newPool(CFG, 1, 1, /*acquireMs*/ 10_000, factory)) {
                long startNanos = System.nanoTime();
                pool.close();
                long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                Assert.assertEquals("idle delegate must be closed by pool close()",
                        1, delegateCloses.get());
                Assert.assertSame("idle teardown runs on the closing thread",
                        Thread.currentThread(), closerThread.get());
                Assert.assertTrue(
                        "close() with no outstanding leases must not wait, took " + elapsedMs + "ms",
                        elapsedMs < 5_000);
            }
        });
    }

    @Test(timeout = 30_000)
    public void sequentialDoubleCloseOfOneLeaseIsNoOp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicInteger delegateCloses = new AtomicInteger();
            IntFunction<Sender> factory = slotIndex ->
                    fakeSender(delegateCloses, new AtomicReference<>(), null, null);

            try (SenderPool pool = newPool(CFG, 1, 1, 1_000, factory)) {
                Sender lease = pool.borrow();
                lease.close();
                // Duplicate close (e.g. explicit close + try-with-resources):
                // the stale generation must make it a silent no-op -- no
                // throw, no second flush, no delegate close.
                lease.close();
                Assert.assertEquals("pool must still hold the recycled slot", 1, pool.availableSize());
                Assert.assertEquals("no delegate teardown before pool close", 0, delegateCloses.get());
            }
        });
    }

    // Closed-mid-creation regression: when borrow() re-locks after building a
    // fresh delegate and finds the pool closed, it must tear that delegate down
    // OUTSIDE the pool lock (mirroring retireLease). A delegate close() can
    // block for seconds (bounded ack drain, drainer-pool wind-down) or longer
    // (unbounded I/O-thread latch await behind an OS-level connect); held under
    // the lock it would stall close(), giveBack/retireLease and reapIdle.
    // RED (teardown under the lock): the concurrent close() probe below parks
    // behind the borrower's delegate close and the join times out.
    @Test(timeout = 30_000)
    public void closedMidCreationTeardownRunsOutsideThePoolLock() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicInteger delegateCloses = new AtomicInteger();
            AtomicReference<Thread> closerThread = new AtomicReference<>();
            CountDownLatch inCreate = new CountDownLatch(1);
            CountDownLatch releaseCreate = new CountDownLatch(1);
            CountDownLatch inDelegateClose = new CountDownLatch(1);
            CountDownLatch releaseDelegateClose = new CountDownLatch(1);
            IntFunction<Sender> factory = slotIndex -> {
                inCreate.countDown();
                awaitOrFail(releaseCreate, "test never released the parked creation");
                return parkingCloseSender(
                        delegateCloses, closerThread, inDelegateClose, releaseDelegateClose);
            };

            try (SenderPool pool = newPool(CFG, 0, 1, 10_000, factory)) {
                AtomicReference<Throwable> borrowOutcome = new AtomicReference<>();
                Thread borrower = new Thread(() -> {
                    try {
                        pool.borrow();
                    } catch (Throwable t) {
                        borrowOutcome.set(t);
                    }
                }, "mid-creation-borrower");
                borrower.start();
                Assert.assertTrue("borrower never reached the factory",
                        inCreate.await(10, TimeUnit.SECONDS));

                // Close the pool while the creation is in flight: nothing is
                // outstanding (the new slot never entered `all`), so this
                // returns promptly with `closed` raised.
                pool.close();

                // Let the creation finish: the borrower re-locks, observes the
                // closed pool and starts the delegate teardown, which parks.
                releaseCreate.countDown();
                Assert.assertTrue("borrower never reached the delegate teardown",
                        inDelegateClose.await(10, TimeUnit.SECONDS));

                // With the teardown parked mid-close, the pool lock must be
                // FREE: a concurrent lock-taking call (an idempotent re-close)
                // has to complete promptly instead of stalling behind it.
                Thread probe = new Thread(pool::close, "pool-lock-probe");
                probe.start();
                probe.join(TimeUnit.SECONDS.toMillis(5));
                boolean lockFree = !probe.isAlive();
                // Always unpark the teardown, even when the probe failed,
                // so the test fails on the assertion and not its timeout.
                releaseDelegateClose.countDown();
                Assert.assertTrue(
                        "pool lock is held across the closed-mid-creation delegate teardown: "
                                + "a concurrent close() stalled behind it",
                        lockFree);

                borrower.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("borrower did not finish", borrower.isAlive());
                Assert.assertTrue(
                        "borrow() must surface the closed pool, got: " + borrowOutcome.get(),
                        borrowOutcome.get() instanceof LineSenderException
                                && String.valueOf(borrowOutcome.get().getMessage()).contains("closed"));
                Assert.assertEquals("the mid-creation delegate must be torn down exactly once",
                        1, delegateCloses.get());
                Assert.assertSame("teardown must run on the borrower's thread",
                        borrower, closerThread.get());
            }
        });
    }

    // pendingLeaseTeardowns contract for the closed-mid-creation path: once the
    // out-of-lock teardown has started, close() must NOT return before it
    // completes (the delegate may still hold an SF flock / native resources).
    // Uses the production interleaving: markClosing() raises `closed` first
    // (QuestDBImpl.close() does this before SenderPool.close()), the borrower
    // starts its teardown, THEN close() runs and has to wait. SF config so the
    // reserved-slot branch (closingSlots move + reclaimSlot) executes too.
    // RED (pendingLeaseTeardowns not bumped on this path): close() returns
    // while the teardown is still parked and the first assertion fails.
    @Test(timeout = 30_000)
    public void closeWaitsForClosedMidCreationTeardownToComplete() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Unique, non-existent sf_dir: minSize=0 means no pre-warm, so the
            // dir is never created and startup SF recovery is a no-op. The
            // factory replaces createUnlocked(), so localhost:1 is never dialed.
            String sfDir = Paths.get(System.getProperty("java.io.tmpdir"),
                    "qdb-sf-midcreation-" + System.nanoTime()).toString();
            String sfCfg = "ws::addr=localhost:1;sf_dir=" + sfDir + ";";

            AtomicInteger delegateCloses = new AtomicInteger();
            CountDownLatch inCreate = new CountDownLatch(1);
            CountDownLatch releaseCreate = new CountDownLatch(1);
            CountDownLatch inDelegateClose = new CountDownLatch(1);
            CountDownLatch releaseDelegateClose = new CountDownLatch(1);
            IntFunction<Sender> factory = slotIndex -> {
                inCreate.countDown();
                awaitOrFail(releaseCreate, "test never released the parked creation");
                return parkingCloseSender(delegateCloses, new AtomicReference<>(),
                        inDelegateClose, releaseDelegateClose);
            };

            try (SenderPool pool = newPool(sfCfg, 0, 1, 10_000, factory)) {
                AtomicReference<Throwable> borrowOutcome = new AtomicReference<>();
                Thread borrower = new Thread(() -> {
                    try {
                        pool.borrow();
                    } catch (Throwable t) {
                        borrowOutcome.set(t);
                    }
                }, "sf-mid-creation-borrower");
                borrower.start();
                Assert.assertTrue("borrower never reached the factory",
                        inCreate.await(10, TimeUnit.SECONDS));

                // Raise the shutdown signal WITHOUT running close() yet --
                // exactly what QuestDBImpl.close() does via markClosing()
                // before it stops the housekeeper and closes the pool.
                Method markClosing = SenderPool.class.getDeclaredMethod("markClosing");
                markClosing.setAccessible(true);
                markClosing.invoke(pool);

                // The borrower observes `closed` and parks inside the
                // out-of-lock delegate teardown.
                releaseCreate.countDown();
                Assert.assertTrue("borrower never reached the delegate teardown",
                        inDelegateClose.await(10, TimeUnit.SECONDS));

                // close() must wait on the in-flight teardown, not return.
                Thread closer = new Thread(pool::close, "pool-closer");
                closer.start();
                closer.join(300);
                boolean closeWaited = closer.isAlive();
                releaseDelegateClose.countDown();
                Assert.assertTrue(
                        "close() returned while a closed-mid-creation delegate teardown "
                                + "was still in flight (pendingLeaseTeardowns not accounted)",
                        closeWaited);

                // Once the teardown completes, close() must return promptly
                // (well before its 5s lease-wait cap) and the teardown must
                // have run exactly once.
                closer.join(TimeUnit.SECONDS.toMillis(4));
                Assert.assertFalse("close() did not return after the teardown completed",
                        closer.isAlive());
                Assert.assertEquals("the mid-creation delegate must be torn down exactly once",
                        1, delegateCloses.get());

                borrower.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("borrower did not finish", borrower.isAlive());
                Assert.assertTrue(
                        "borrow() must surface the closed pool, got: " + borrowOutcome.get(),
                        borrowOutcome.get() instanceof LineSenderException
                                && String.valueOf(borrowOutcome.get().getMessage()).contains("closed"));
            }
        });
    }

    private static void awaitOrFail(CountDownLatch latch, String message) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException(message);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(message, e);
        }
    }

    /**
     * Like {@link #fakeSender}, but {@code close()} signals {@code inClose} and
     * parks on {@code releaseClose} before counting -- a teardown frozen
     * mid-close, so tests can probe what the pool does while it runs.
     */
    private static Sender parkingCloseSender(
            AtomicInteger closes,
            AtomicReference<Thread> closerThread,
            CountDownLatch inClose,
            CountDownLatch releaseClose
    ) {
        return (Sender) Proxy.newProxyInstance(
                Sender.class.getClassLoader(),
                new Class[]{Sender.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "close":
                            closerThread.set(Thread.currentThread());
                            inClose.countDown();
                            awaitOrFail(releaseClose, "test never released the parked close");
                            closes.incrementAndGet();
                            return null;
                        case "toString":
                            return "ParkingCloseFakeSender";
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            Class<?> rt = method.getReturnType();
                            if (rt == boolean.class) return false;
                            if (rt == byte.class) return (byte) 0;
                            if (rt == short.class) return (short) 0;
                            if (rt == int.class) return 0;
                            if (rt == long.class) return 0L;
                            if (rt == float.class) return 0f;
                            if (rt == double.class) return 0d;
                            if (rt == char.class) return (char) 0;
                            if (rt == void.class) return null;
                            if (rt.isInstance(proxy)) return proxy;
                            return null;
                    }
                });
    }

    private static SenderPool newPool(
            String cfg, int min, int max, long acquireMs, IntFunction<Sender> senderFactory
    ) {
        return new SenderPool(cfg, min, max, acquireMs, Long.MAX_VALUE, Long.MAX_VALUE, senderFactory);
    }

    /**
     * Proxy-backed fake Sender. {@code close()} bumps {@code closes} and
     * records the closing thread. When {@code inAppend}/{@code releaseAppend}
     * are non-null, {@code longColumn} signals {@code inAppend} and then parks
     * on {@code releaseAppend} -- a producer frozen inside the delegate.
     */
    private static Sender fakeSender(
            AtomicInteger closes,
            AtomicReference<Thread> closerThread,
            CountDownLatch inAppend,
            CountDownLatch releaseAppend
    ) {
        return (Sender) Proxy.newProxyInstance(
                Sender.class.getClassLoader(),
                new Class[]{Sender.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "close":
                            closes.incrementAndGet();
                            closerThread.set(Thread.currentThread());
                            return null;
                        case "flush":
                            return null;
                        case "longColumn":
                            if (inAppend != null) {
                                inAppend.countDown();
                                if (!releaseAppend.await(10, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException("test never released the parked append");
                                }
                            }
                            return proxy;
                        case "toString":
                            return "FakeSender";
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            Class<?> rt = method.getReturnType();
                            if (rt == boolean.class) return false;
                            if (rt == byte.class) return (byte) 0;
                            if (rt == short.class) return (short) 0;
                            if (rt == int.class) return 0;
                            if (rt == long.class) return 0L;
                            if (rt == float.class) return 0f;
                            if (rt == double.class) return 0d;
                            if (rt == char.class) return (char) 0;
                            if (rt == void.class) return null;
                            // fluent builder methods return Sender
                            if (rt.isInstance(proxy)) return proxy;
                            return null;
                    }
                });
    }
}
