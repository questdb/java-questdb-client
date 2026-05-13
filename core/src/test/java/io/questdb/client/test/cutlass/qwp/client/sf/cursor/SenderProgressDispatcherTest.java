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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderProgressDispatcher;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SenderProgressDispatcherTest {

    @Test
    public void testCloseDeliversLatestPendingValue() throws Exception {
        // After close(), a value that was offered while the dispatcher was
        // busy should still be delivered before the loop exits (best-effort
        // within the drain deadline).
        CountDownLatch unblock = new CountDownLatch(1);
        AtomicLong lastDelivered = new AtomicLong(-1);
        CountDownLatch firstDelivered = new CountDownLatch(1);
        SenderProgressDispatcher d = new SenderProgressDispatcher(
                fsn -> {
                    if (lastDelivered.get() == -1) {
                        lastDelivered.set(fsn);
                        firstDelivered.countDown();
                        try {
                            unblock.await();
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                    } else {
                        lastDelivered.set(fsn);
                    }
                },
                4);
        Assert.assertTrue(d.offer(10));
        Assert.assertTrue("first delivery should start",
                firstDelivered.await(2, TimeUnit.SECONDS));
        Assert.assertTrue(d.offer(20));
        Assert.assertTrue(d.offer(30));
        // Close while the handler is still blocked on the first call.
        Thread closer = new Thread(() -> {
            unblock.countDown();
            d.close();
        });
        closer.start();
        closer.join(TimeUnit.SECONDS.toMillis(2));
        Assert.assertFalse("close must complete", closer.isAlive());
        Assert.assertEquals("dispatcher must drain to highest watermark",
                30L, lastDelivered.get());
    }

    @Test
    public void testCloseIsIdempotent() {
        SenderProgressDispatcher d = new SenderProgressDispatcher(fsn -> { /* no-op */ }, 4);
        d.offer(1);
        d.close();
        d.close();
        d.close();
    }

    @Test
    public void testConstructorRejectsBadCapacity() {
        try {
            new SenderProgressDispatcher(fsn -> { /* no-op */ }, 0).close();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("capacity"));
        }
        try {
            new SenderProgressDispatcher(fsn -> { /* no-op */ }, -1).close();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }

    @Test
    public void testConstructorRejectsNullHandler() {
        try {
            new SenderProgressDispatcher(null, 4).close();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("handler"));
        }
    }

    @Test
    public void testDeliveryIsStrictlyMonotonic() throws Exception {
        // The dispatcher must skip any value <= the last value it already
        // delivered, so re-offering a stale watermark must not cause a
        // duplicate delivery to the handler.
        List<Long> received = new ArrayList<>();
        try (SenderProgressDispatcher d = new SenderProgressDispatcher(fsn -> {
            synchronized (received) {
                received.add(fsn);
            }
        }, 16)) {
            d.offer(5);
            waitForDelivered(d, 1);
            // Re-offers of equal-or-lower values must not produce additional
            // deliveries.
            d.offer(5);
            d.offer(3);
            d.offer(1);
            // Brief grace period for any erroneous re-delivery to happen.
            TimeUnit.MILLISECONDS.sleep(50);
            Assert.assertEquals("only the first 5 should have delivered",
                    1L, d.getTotalDelivered());
            d.offer(7);
            waitForDelivered(d, 2);
            synchronized (received) {
                long previous = -1;
                for (long fsn : received) {
                    Assert.assertTrue("values must be strictly increasing; "
                                    + previous + " then " + fsn,
                            fsn > previous);
                    previous = fsn;
                }
                Assert.assertEquals(2, received.size());
                Assert.assertEquals(5L, (long) received.get(0));
                Assert.assertEquals(7L, (long) received.get(1));
            }
        }
    }

    @Test
    public void testDroppedNotificationsCounter() throws Exception {
        // Slow handler — releases once the test allows it. Watermarks
        // offered while the handler is busy collapse onto the latest, which
        // should surface as totalOffered > totalDelivered.
        CountDownLatch unblock = new CountDownLatch(1);
        CountDownLatch firstDelivered = new CountDownLatch(1);
        AtomicInteger delivered = new AtomicInteger();
        try (SenderProgressDispatcher d = new SenderProgressDispatcher(fsn -> {
            firstDelivered.countDown();
            try {
                unblock.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            delivered.incrementAndGet();
        }, 4)) {
            Assert.assertTrue(d.offer(1));
            Assert.assertTrue("handler must enter before pile-up",
                    firstDelivered.await(2, TimeUnit.SECONDS));
            for (int i = 2; i <= 10; i++) {
                Assert.assertTrue("offer must not block", d.offer(i));
            }
            // 10 offered, 1 in-flight, 0 delivered yet. After unblocking,
            // exactly one more delivery happens (the latest watermark, 10),
            // so 10 - 2 = 8 advances are subsumed.
            Assert.assertTrue("offered watermarks must accumulate as drops",
                    d.getDroppedNotifications() >= 8L);
            unblock.countDown();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (delivered.get() < 2 && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(10);
            }
            Assert.assertEquals(2, delivered.get());
            Assert.assertEquals(2L, d.getTotalDelivered());
        }
    }

    @Test
    public void testHandlerThrowDoesNotKillDispatcher() throws Exception {
        // A handler that throws on the first call must not poison the
        // dispatcher; subsequent offers must still deliver.
        AtomicInteger thrown = new AtomicInteger();
        CountDownLatch reachedThird = new CountDownLatch(1);
        try (SenderProgressDispatcher d = new SenderProgressDispatcher(fsn -> {
            if (thrown.incrementAndGet() == 1) {
                throw new RuntimeException("simulated handler bug");
            }
            if (fsn == 3) {
                reachedThird.countDown();
            }
        }, 4)) {
            Assert.assertTrue(d.offer(1));
            // Ensure the throwing call has happened before queuing the next
            // values, otherwise overwrite-collapse may swallow them.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (thrown.get() < 1 && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(10);
            }
            Assert.assertTrue(d.offer(2));
            Assert.assertTrue(d.offer(3));
            Assert.assertTrue("dispatcher must keep delivering after a throw",
                    reachedThird.await(2, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testLazyStartOnFirstOffer() throws Exception {
        // No thread should exist before the first offer; workloads with zero
        // acks pay zero thread cost.
        try (SenderProgressDispatcher d = new SenderProgressDispatcher(fsn -> { /* no-op */ },
                16, "lazy-start-progress-test")) {
            Assert.assertNull("dispatcher thread must not exist before first offer",
                    findThreadByName("lazy-start-progress-test"));
            Assert.assertTrue(d.offer(0));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            Thread spawned;
            while ((spawned = findThreadByName("lazy-start-progress-test")) == null
                    && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(10);
            }
            Assert.assertNotNull("dispatcher thread must exist after first offer", spawned);
            Assert.assertTrue("dispatcher must be a daemon", spawned.isDaemon());
        }
    }

    @Test
    public void testOfferAfterCloseReturnsFalse() {
        SenderProgressDispatcher d = new SenderProgressDispatcher(fsn -> { /* no-op */ }, 4);
        d.close();
        Assert.assertFalse(d.offer(1));
        Assert.assertEquals(0L, d.getDroppedNotifications());
        Assert.assertEquals(0L, d.getTotalDelivered());
    }

    @Test
    public void testTotalDeliveredCounter() throws Exception {
        CountDownLatch reachedLast = new CountDownLatch(1);
        try (SenderProgressDispatcher d = new SenderProgressDispatcher(fsn -> {
            if (fsn == 3) {
                reachedLast.countDown();
            }
        }, 4)) {
            d.offer(1);
            // Wait for delivery before issuing the next, to avoid 1 and 2
            // collapsing into a single delivery.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (d.getTotalDelivered() < 1 && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(10);
            }
            d.offer(2);
            while (d.getTotalDelivered() < 2 && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(10);
            }
            d.offer(3);
            Assert.assertTrue(reachedLast.await(2, TimeUnit.SECONDS));
            Assert.assertEquals(3L, d.getTotalDelivered());
            Assert.assertEquals(0L, d.getDroppedNotifications());
        }
    }

    private static void waitForDelivered(SenderProgressDispatcher d, long target) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (d.getTotalDelivered() < target && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(5);
        }
        Assert.assertTrue("expected totalDelivered >= " + target + ", got " + d.getTotalDelivered(),
                d.getTotalDelivered() >= target);
    }

    private static Thread findThreadByName(String name) {
        Thread[] all = new Thread[Thread.activeCount() * 2 + 16];
        int n = Thread.enumerate(all);
        for (int i = 0; i < n; i++) {
            Thread t = all[i];
            if (t != null && name.equals(t.getName())) {
                return t;
            }
        }
        return null;
    }
}
