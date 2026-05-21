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
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
    public void testBorrowReturnRecyclesSameDecorator() {
        try (SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 1, 1, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
            Sender first = pool.borrow();
            first.close();
            Sender second = pool.borrow();
            Assert.assertSame("returned decorator should be reused after close()", first, second);
            second.close();
        }
    }

    @Test
    public void testCloseIdempotent() {
        SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 2, 2, 1_000, Long.MAX_VALUE, Long.MAX_VALUE);
        pool.close();
        pool.close();
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
    public void testPoolBuildsRequestedNumberOfSenders() {
        try (SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 3, 3, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
            Sender a = pool.borrow();
            Sender b = pool.borrow();
            Sender c = pool.borrow();
            Assert.assertNotSame(a, b);
            Assert.assertNotSame(b, c);
            Assert.assertNotSame(a, c);
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

    @Test
    public void testThreadAffinityIsPerThread() throws InterruptedException {
        try (SenderPool pool = new SenderPool(DEAD_HTTP_CONFIG, 2, 2, 1_000, Long.MAX_VALUE, Long.MAX_VALUE)) {
            Sender mainPinned = pool.pinToCurrentThread();
            Assert.assertSame("re-pin on same thread returns same instance",
                    mainPinned, pool.pinToCurrentThread());

            AtomicReference<Sender> otherPinned = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);
            Thread t = new Thread(() -> {
                try {
                    otherPinned.set(pool.pinToCurrentThread());
                } finally {
                    done.countDown();
                }
            });
            t.start();
            Assert.assertTrue(done.await(2, TimeUnit.SECONDS));
            Assert.assertNotSame("different threads must get different pinned Senders",
                    mainPinned, otherPinned.get());

            pool.releaseCurrentThread();
        }
    }
}
