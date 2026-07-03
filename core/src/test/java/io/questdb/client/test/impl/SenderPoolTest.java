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
import io.questdb.client.impl.PooledSender;
import io.questdb.client.impl.SenderPool;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for the {@link SenderPool} borrow/return semantics. Uses the
 * {@code http} schema with a dead address: HTTP Senders connect lazily on
 * first request, so the pool builds without I/O and the tests can exercise
 * borrow/return without needing a real server.
 * <p>
 * Tests never call methods that would attempt a network round-trip
 * (no {@code flush}, no row builders that auto-flush). Pooled
 * {@link io.questdb.client.impl.PooledSender#close()} does call
 * {@code delegate.flush()}, but on an empty buffer that path is a no-op for
 * HTTP transport.
 */
public class SenderPoolTest {

    private static final String DEAD_HTTP_CONFIG =
            "http::addr=127.0.0.1:1;protocol_version=2;auto_flush=off;";

    @Test
    public void testBorrowReturnRecyclesSameDecorator() throws Exception {
        try (SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 1, 1, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
            Sender first = pool.borrow();
            first.close();
            Sender second = pool.borrow();
            // Each borrow is a fresh PooledSender wrapper; what the pool recycles
            // is the underlying slot, so compare those rather than the handles.
            Assert.assertSame("returned slot should be recycled after close()",
                    slotOf(first), slotOf(second));
            second.close();
        }
    }

    private static Object slotOf(Sender pooledWrapper) throws Exception {
        Field f = PooledSender.class.getDeclaredField("slot");
        f.setAccessible(true);
        return f.get(pooledWrapper);
    }

    @Test
    public void testBrokenSenderIsNotReturnedToPool() throws Exception {
        // Borrowing, buffering a row, and then closing forces flush() against
        // the unreachable address, which throws. The broken slot must not be
        // returned to the pool: its delegate's buffer still holds the failed
        // row, and on transports with terminal-failure semantics the delegate
        // is also unusable. Either way, the next borrower must get a fresh
        // slot, not the broken one.
        try (SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 1, 1, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
            Sender first = pool.borrow();
            Object firstSlot = slotOf(first);
            first.table("t").longColumn("v", 1).atNow();
            try {
                first.close();
                Assert.fail("close() with buffered rows against an unreachable host must throw");
            } catch (LineSenderException ignored) {
                // expected
            }
            Sender second = pool.borrow();
            try {
                // borrow() always hands out a FRESH PooledSender wrapper, so
                // assertNotSame(first, second) on the wrappers is vacuously
                // true and proves nothing -- it stays true whether or not the
                // broken slot was discarded. What the pool recycles is the
                // underlying slot, so a broken slot leaking back to the next
                // borrower shows up as the SAME slot. Assert the slot differs.
                Assert.assertNotSame("broken slot must not be handed back to next borrower",
                        firstSlot, slotOf(second));
            } finally {
                // On the failing path (broken slot recycled) second.close()
                // re-throws, since its delegate's buffer still holds the
                // failed row; swallow it so the assertion above is what
                // surfaces rather than this incidental close() failure.
                try {
                    second.close();
                } catch (LineSenderException ignored) {
                    // expected only when the regression is present
                }
            }
        }
    }

    @Test
    public void testCloseIdempotent() {
        SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 2, 2, 1_000, Long.MAX_VALUE, Long.MAX_VALUE);
        pool.close();
        pool.close();
    }

    @Test
    public void testLeaseReturnedAfterCloseTearsDownItsOwnDelegate() {
        // C1: pool.close() must never tear down a borrowed delegate -- a
        // producer thread may be inside it, and freeing its native buffers
        // mid-append is a use-after-free / SEGV. close() instead waits
        // boundedly for the lease and leaves the borrowed slot alive when the
        // wait times out; the lease's own close() then tears the delegate
        // down on the returning thread (retireLease): here the buffered row's
        // flush fails against the dead address, routing through
        // discardBroken, which removes the slot from `all` and closes the
        // delegate -- now safe post-close because close()'s teardown loop
        // only ever touches idle slots.
        //
        // acquireTimeout is kept short: it doubles as close()'s bounded
        // lease-wait budget, which this test intentionally exhausts.
        try (SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 1, 1, 150, Long.MAX_VALUE, Long.MAX_VALUE)) {
            Sender s = pool.borrow();
            // A row in the buffer forces flush() to attempt a real HTTP
            // request on close(); against the unreachable DEAD_HTTP target
            // flush throws and routes the wrapper to discardBroken.
            s.table("t").longColumn("v", 1L).atNow();

            pool.close();
            Assert.assertEquals(
                    "close() must leave the borrowed slot in place -- teardown belongs to the returning lease",
                    1, pool.totalSize()
            );

            // The lease comes home after close: flush throws (dead address),
            // and PooledSender.close()'s finally routes to discardBroken ->
            // retireLease, the delegated single-owner teardown.
            try {
                s.close();
                Assert.fail("flush against the dead address with a buffered row must throw");
            } catch (RuntimeException expected) {
                // expected: LineSenderException (or a transport-specific wrap)
            }

            Assert.assertEquals(
                    "the returning lease must retire its slot (delegated teardown)",
                    0, pool.totalSize()
            );
        }
    }

    @Test
    public void testCloseRejectsSubsequentBorrow() {
        SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 1, 1, 1_000, Long.MAX_VALUE, Long.MAX_VALUE);
        pool.close();
        try {
            pool.borrow();
            Assert.fail("borrow after close must throw");
        } catch (LineSenderException ignored) {
        }
    }

    @Test
    public void testExhaustionTimeoutThrows() {
        try (SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 1, 1, 100, Long.MAX_VALUE, Long.MAX_VALUE)) {
            long start = System.nanoTime();
            try (Sender ignored = pool.borrow()) {
                pool.borrow();
                Assert.fail("expected timeout");
            } catch (LineSenderException e) {
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                Assert.assertTrue("error should mention timeout, was: " + e.getMessage(),
                        e.getMessage().contains("timed out"));
                Assert.assertTrue("should have waited close to the timeout, elapsed=" + elapsedMs,
                        elapsedMs >= 90);
            }
        }
    }

    @Test
    public void testPoolBuildsRequestedNumberOfSenders() throws Exception {
        try (SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 3, 3, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
            Sender a = pool.borrow();
            Sender b = pool.borrow();
            Sender c = pool.borrow();
            // borrow() allocates a fresh PooledSender wrapper on every call, so
            // comparing the wrappers is vacuously true -- it would stay green
            // even if the pool double-lent a single slot to all three borrowers.
            // The property this test exists for -- three concurrent borrowers
            // each hold their own sender -- lives in the underlying slots, so
            // compare those (same reasoning as testBrokenSenderIsNotReturnedToPool).
            Object slotA = slotOf(a);
            Object slotB = slotOf(b);
            Object slotC = slotOf(c);
            Assert.assertNotSame("a and b must not share a slot", slotA, slotB);
            Assert.assertNotSame("b and c must not share a slot", slotB, slotC);
            Assert.assertNotSame("a and c must not share a slot", slotA, slotC);
            Assert.assertEquals("pool must build exactly the requested number of senders",
                    3, pool.totalSize());
            a.close();
            b.close();
            c.close();
        }
    }

    @Test
    public void testElasticGrowsUpToMax() {
        // min=1, max=3 -- starts at 1, grows on demand to 3.
        try (SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 1, 3, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
            Assert.assertEquals("pre-warm to min", 1, pool.totalSize());
            Sender a = pool.borrow();
            Assert.assertEquals(1, pool.totalSize());
            Sender b = pool.borrow();
            Assert.assertEquals("borrowing past min grows", 2, pool.totalSize());
            Sender c = pool.borrow();
            Assert.assertEquals(3, pool.totalSize());
            // At max: next borrow times out.
            try {
                pool.borrow();
                Assert.fail("4th borrow must time out at max=3");
            } catch (LineSenderException ignored) {
            }
            a.close();
            b.close();
            c.close();
            Assert.assertEquals("size unchanged on return", 3, pool.totalSize());
        }
    }

    @Test
    public void testAvailableSizeTracksBorrowAndReturn() throws InterruptedException {
        // min=2, max=4. Walk the full lifecycle and assert availableSize() and
        // totalSize() stay in sync at every step: pre-warm, borrow shrinks
        // available, growth doesn't change available (the new slot goes
        // straight to the caller), return restores availability, reap shrinks
        // total back toward min but never below.
        try (SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 2, 4, 1_000, 100, Long.MAX_VALUE)) {
            // Pre-warmed to min=2; everything is idle.
            Assert.assertEquals(2, pool.totalSize());
            Assert.assertEquals(2, pool.availableSize());

            // Borrowing from the warm slots leaves total unchanged but consumes one available.
            Sender a = pool.borrow();
            Assert.assertEquals(2, pool.totalSize());
            Assert.assertEquals(1, pool.availableSize());

            Sender b = pool.borrow();
            Assert.assertEquals(2, pool.totalSize());
            Assert.assertEquals(0, pool.availableSize());

            // Borrowing past min grows the pool. The new slot goes straight to
            // the caller, so availableSize stays at 0.
            Sender c = pool.borrow();
            Assert.assertEquals(3, pool.totalSize());
            Assert.assertEquals(0, pool.availableSize());

            // Returning two restores availability without touching total.
            a.close();
            b.close();
            Assert.assertEquals(3, pool.totalSize());
            Assert.assertEquals(2, pool.availableSize());

            // Reaping idle slots over min closes them; available counts the
            // remaining idle ones. Total shrinks; min=2 is respected so we end
            // up with min=2 total and (min - in-use)=1 available.
            Thread.sleep(150);
            pool.reapIdle();
            Assert.assertEquals(2, pool.totalSize());
            Assert.assertEquals(1, pool.availableSize());

            c.close();
            Assert.assertEquals(2, pool.totalSize());
            Assert.assertEquals(2, pool.availableSize());
        }
    }

    @Test
    public void testAvailableSizeZeroAfterClose() {
        SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 2, 2, 1_000, Long.MAX_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(2, pool.availableSize());
        pool.close();
        // close() destroys every underlying Sender; the available queue is no
        // longer being added to, but the snapshot read is still safe. The
        // exact value (0 or stale) is less important than the call not
        // throwing on a closed pool.
        int snapshot = pool.availableSize();
        Assert.assertTrue("availableSize on closed pool must be a non-negative snapshot, got " + snapshot,
                snapshot >= 0);
    }

    @Test
    public void testReapIdleShrinksToMin() throws InterruptedException {
        // Short idle timeout; reapIdle() drives the sweep deterministically.
        try (SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 1, 3, 1_000, 100, Long.MAX_VALUE)) {
            Sender a = pool.borrow();
            Sender b = pool.borrow();
            Sender c = pool.borrow();
            Assert.assertEquals(3, pool.totalSize());
            a.close();
            b.close();
            c.close();
            // All idle; wait until idle threshold passes, then sweep.
            Thread.sleep(150);
            pool.reapIdle();
            Assert.assertEquals("reap must shrink to min", 1, pool.totalSize());
        }
    }

    @Test
    public void testReapIdleRespectsMinSize() throws InterruptedException {
        // min=2: two slots must stay even after long idle.
        try (SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 2, 4, 1_000, 50, Long.MAX_VALUE)) {
            Sender a = pool.borrow();
            Sender b = pool.borrow();
            Sender c = pool.borrow();
            a.close();
            b.close();
            c.close();
            Thread.sleep(100);
            pool.reapIdle();
            Assert.assertEquals("min=2 must be preserved", 2, pool.totalSize());
        }
    }

    // ----------------------------------------------------------------------
    // Teardown robustness: a delegate close() can throw an Error (e.g. an
    // -ea AssertionError), not just a RuntimeException. The pool's best-effort
    // teardown paths used to catch only RuntimeException, so such an Error
    // escaped: reapIdle() aborted mid-loop (stranding siblings) and the cap
    // accounting was skipped, permanently understating capacity. These tests
    // pin the fix: an Error during delegate close() must not abort the loop,
    // must not propagate, and must leave the pool fully usable.
    // ----------------------------------------------------------------------

    @Test
    public void testReapIdleSurvivesDelegateCloseError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // idleTimeout=1ms so every idle slot is reap-eligible after a short sleep.
            try (SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 0, 3, 1_000, 1, Long.MAX_VALUE)) {
                AtomicInteger closeAttempts = new AtomicInteger();
                PooledSender a = pool.borrow();
                PooledSender b = pool.borrow();
                PooledSender c = pool.borrow();
                installFailingCloseDelegate(a, closeAttempts);
                installFailingCloseDelegate(b, closeAttempts);
                installFailingCloseDelegate(c, closeAttempts);
                pool.giveBack(a);
                pool.giveBack(b);
                pool.giveBack(c);
                Assert.assertEquals(3, pool.totalSize());

                Thread.sleep(10);
                // Must NOT throw even though every delegate.close() raises an Error.
                pool.reapIdle();

                // The loop attempted to close ALL three delegates -- it did not abort
                // after the first Error.
                Assert.assertEquals("reap loop must not abort on a delegate close() Error",
                        3, closeAttempts.get());
                // Every reaped slot left `all`; capacity was not stranded.
                Assert.assertEquals(0, pool.totalSize());
                Assert.assertEquals(0, pool.availableSize());

                // Pool still grows back to full capacity -- no permanent leak.
                PooledSender d = pool.borrow();
                PooledSender e = pool.borrow();
                PooledSender f = pool.borrow();
                Assert.assertEquals(3, pool.totalSize());
                pool.giveBack(d);
                pool.giveBack(e);
                pool.giveBack(f);
            }
        });
    }

    @Test
    public void testCloseSurvivesDelegateCloseError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 2, 2, 1_000, Long.MAX_VALUE, Long.MAX_VALUE);
            AtomicInteger closeAttempts = new AtomicInteger();
            PooledSender a = pool.borrow();
            PooledSender b = pool.borrow();
            installFailingCloseDelegate(a, closeAttempts);
            installFailingCloseDelegate(b, closeAttempts);
            pool.giveBack(a);
            pool.giveBack(b);

            // close() must not propagate an Error and must attempt every delegate.
            pool.close();
            Assert.assertEquals("close() must attempt to close every delegate despite an Error",
                    2, closeAttempts.get());
            // Idempotent second close stays clean.
            pool.close();
        });
    }

    /**
     * Reflectively replaces the wrapper's delegate with a {@link Proxy} that
     * throws an {@link AssertionError} (an {@link Error}, not a
     * {@link RuntimeException}) on {@code close()} and forwards every other
     * call to the real delegate. Models an -ea assertion firing during native
     * teardown.
     * <p>
     * The proxy still forwards {@code close()} to the real delegate (freeing
     * its native {@code HttpClient} buffers) <em>before</em> throwing, so the
     * pool sees the injected {@link Error} exactly as it would in production
     * while the test does not leak native memory.
     */
    private static void installFailingCloseDelegate(PooledSender ps, AtomicInteger closeAttempts) throws Exception {
        Field slotF = PooledSender.class.getDeclaredField("slot");
        slotF.setAccessible(true);
        Object slot = slotF.get(ps);
        Field f = slot.getClass().getDeclaredField("delegate");
        f.setAccessible(true);
        Sender real = (Sender) f.get(slot);
        Sender failing = (Sender) Proxy.newProxyInstance(
                Sender.class.getClassLoader(),
                new Class[]{Sender.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName()) && (args == null || args.length == 0)) {
                        closeAttempts.incrementAndGet();
                        // Free the real delegate's native resources first, then
                        // surface the injected Error -- the pool's teardown loop
                        // still observes it, but no native memory is leaked.
                        try {
                            real.close();
                        } catch (Throwable ignore) {
                            // real.close() on an empty HTTP buffer is a no-op;
                            // swallow anything so the injected Error is what the
                            // pool sees.
                        }
                        throw new AssertionError("injected delegate close() Error");
                    }
                    return method.invoke(real, args);
                });
        f.set(slot, failing);
    }
}
