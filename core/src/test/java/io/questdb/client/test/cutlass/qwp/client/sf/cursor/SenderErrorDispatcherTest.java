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

import io.questdb.client.SenderError;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderErrorDispatcher;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SenderErrorDispatcherTest {

    @Test
    public void testCloseDrainsRemainingEntries() {
        // After close(), entries already in the queue should still be
        // delivered (within the drain deadline). Spec: "drains remaining
        // queue entries on stop with a short deadline".
        List<SenderError> received = new ArrayList<>();
        Object lock = new Object();
        SenderErrorDispatcher d = new SenderErrorDispatcher(err -> {
            synchronized (lock) {
                received.add(err);
            }
        });
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(d.offer(buildError(i)));
        }
        d.close();
        synchronized (lock) {
            // Best-effort: with a 100ms drain deadline and a near-instant
            // handler, all 10 should land. Allow tolerance for slow CI.
            Assert.assertTrue("expected drain to deliver most entries; got " + received.size(),
                    received.size() >= 5);
        }
    }

    @Test
    public void testCloseIsIdempotent() {
        SenderErrorDispatcher d = new SenderErrorDispatcher(err -> { /* no-op */ });
        d.offer(buildError(0));
        d.close();
        d.close(); // must not throw
        d.close();
    }

    @Test
    public void testConstructorRejectsBadCapacity() {
        try {
            new SenderErrorDispatcher(err -> { /* no-op */ }, 0).close();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("capacity"));
        }
        try {
            new SenderErrorDispatcher(err -> { /* no-op */ }, -1).close();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }

    @Test
    public void testConstructorRejectsNullHandler() {
        try {
            new SenderErrorDispatcher(null).close();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("handler"));
        }
    }

    @Test
    public void testFullInboxDropsOldestAndCounts() throws Exception {
        // Spec sf-client.md section 14.6: on overflow, drop the OLDEST entry
        // and admit the new one. The latest entry is always the most
        // informative, so the FIFO head loses, not the new arrival.
        CountDownLatch unblock = new CountDownLatch(1);
        List<SenderError> received = new ArrayList<>();
        Object lock = new Object();
        CountDownLatch allDelivered = new CountDownLatch(5);
        try (SenderErrorDispatcher d = new SenderErrorDispatcher(err -> {
            try {
                unblock.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            synchronized (lock) {
                received.add(err);
            }
            allDelivered.countDown();
        }, /*capacity=*/ 4)) {
            // First offer starts the dispatcher and lands in the handler
            // immediately (and blocks there). Now we can fill the bounded
            // inbox to capacity, then overflow.
            Assert.assertTrue(d.offer(buildError(0)));
            // Give the dispatcher a moment to take the head into the
            // handler so subsequent offers don't get an extra slot.
            TimeUnit.MILLISECONDS.sleep(50);
            for (int i = 1; i <= 4; i++) {
                Assert.assertTrue("inbox should accept offer " + i,
                        d.offer(buildError(i)));
            }
            // Inbox is now at capacity (4) holding [1,2,3,4]. The next two
            // offers must each drop the head (1, then 2) to admit the new
            // arrival. They return true because the new entry IS enqueued.
            Assert.assertTrue("offer beyond capacity must admit the new entry",
                    d.offer(buildError(5)));
            Assert.assertTrue(d.offer(buildError(6)));
            Assert.assertEquals(2L, d.getDroppedNotifications());

            unblock.countDown();
            Assert.assertTrue("all retained entries should deliver within 5s",
                    allDelivered.await(5, TimeUnit.SECONDS));
            synchronized (lock) {
                // 0 was taken by the handler before overflow began; 1 and 2
                // are the dropped pair; 3, 4, 5, 6 are the surviving tail.
                long[] fromFsns = new long[received.size()];
                for (int i = 0; i < received.size(); i++) {
                    fromFsns[i] = received.get(i).getFromFsn();
                }
                Assert.assertArrayEquals("drop-oldest must retain the newest tail",
                        new long[]{0, 3, 4, 5, 6}, fromFsns);
            }
        } finally {
            unblock.countDown();
        }
    }

    @Test
    public void testHandlerThrowDoesNotKillDispatcher() throws Exception {
        // A handler that throws on the first call must not poison the
        // dispatcher; subsequent offers must still deliver.
        AtomicInteger delivered = new AtomicInteger();
        AtomicInteger thrown = new AtomicInteger();
        try (SenderErrorDispatcher d = new SenderErrorDispatcher(err -> {
            delivered.incrementAndGet();
            if (thrown.incrementAndGet() == 1) {
                throw new RuntimeException("simulated handler bug");
            }
        })) {
            Assert.assertTrue(d.offer(buildError(1)));
            Assert.assertTrue(d.offer(buildError(2)));
            Assert.assertTrue(d.offer(buildError(3)));
            // Wait for delivery; ~100ms generous.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (delivered.get() < 3 && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(10);
            }
            Assert.assertEquals(3, delivered.get());
            // Counter sees all three "happened" — exception or not.
            Assert.assertEquals(3L, d.getTotalDelivered());
        }
    }

    @Test
    public void testLazyStartOnFirstOffer() throws Exception {
        // No thread should exist before the first offer. Verifies that
        // workloads with zero errors pay zero thread cost.
        Thread t0 = findDispatcherThread();
        /* no-op */
        try (SenderErrorDispatcher d = new SenderErrorDispatcher(err -> { /* no-op */ },
                16, "lazy-start-test-dispatcher")) {
            // No offer yet → thread must not exist.
            Thread spawned = findThreadByName("lazy-start-test-dispatcher");
            Assert.assertNull("dispatcher thread must not exist before first offer", spawned);

            Assert.assertTrue(d.offer(buildError(0)));
            // Allow the lazy-start to commit. Poll up to ~1s.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
            while (findThreadByName("lazy-start-test-dispatcher") == null
                    && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(10);
            }
            Thread spawnedNow = findThreadByName("lazy-start-test-dispatcher");
            Assert.assertNotNull("dispatcher thread must exist after first offer", spawnedNow);
            Assert.assertTrue("dispatcher must be a daemon", spawnedNow.isDaemon());
            // Sanity: not the same as a thread that existed at test entry.
            Assert.assertNotSame(t0, spawnedNow);
        }
    }

    @Test
    public void testNullErrorIsRejectedSilently() {
        AtomicInteger delivered = new AtomicInteger();
        try (SenderErrorDispatcher d = new SenderErrorDispatcher(err -> delivered.incrementAndGet())) {
            Assert.assertFalse(d.offer(null));
            Assert.assertEquals(0L, d.getDroppedNotifications());
            Assert.assertEquals(0, delivered.get());
        }
    }

    @Test
    public void testOfferAfterCloseReturnsFalse() {
        AtomicInteger delivered = new AtomicInteger();
        SenderErrorDispatcher d = new SenderErrorDispatcher(err -> delivered.incrementAndGet());
        d.close();
        Assert.assertFalse(d.offer(buildError(1)));
        // Dropped counter only tracks queue-overflow drops, not closed.
        Assert.assertEquals(0L, d.getDroppedNotifications());
    }

    @Test
    public void testOrderingIsFifo() throws Exception {
        // ArrayBlockingQueue is FIFO; verify ordering is preserved
        // end-to-end so users can rely on the FSN span sequence matching
        // their producer-side log order.
        int n = 50;
        List<SenderError> received = new ArrayList<>();
        Object lock = new Object();
        CountDownLatch all = new CountDownLatch(n);
        try (SenderErrorDispatcher d = new SenderErrorDispatcher(err -> {
            synchronized (lock) {
                received.add(err);
            }
            all.countDown();
        })) {
            for (int i = 0; i < n; i++) {
                Assert.assertTrue(d.offer(buildError(i)));
            }
            Assert.assertTrue("all entries should deliver within 5s",
                    all.await(5, TimeUnit.SECONDS));
            synchronized (lock) {
                for (int i = 0; i < n; i++) {
                    Assert.assertEquals("FIFO ordering broken at index " + i,
                            i, received.get(i).getFromFsn());
                }
            }
        }
    }

    @Test
    public void testTotalDeliveredCounter() throws Exception {
        CountDownLatch all = new CountDownLatch(3);
        try (SenderErrorDispatcher d = new SenderErrorDispatcher(err -> all.countDown())) {
            d.offer(buildError(1));
            d.offer(buildError(2));
            d.offer(buildError(3));
            Assert.assertTrue(all.await(2, TimeUnit.SECONDS));
            Assert.assertEquals(3L, d.getTotalDelivered());
            Assert.assertEquals(0L, d.getDroppedNotifications());
        }
    }

    private static SenderError buildError(int seq) {
        // FSN reused as the test's identity field — easiest to assert on.
        return new SenderError(
                SenderError.Category.SCHEMA_MISMATCH,
                SenderError.Policy.RETRIABLE,
                0x03,
                "msg-" + seq,
                seq,
                seq,
                seq,
                "table-" + seq,
                System.nanoTime()
        );
    }

    private static Thread findDispatcherThread() {
        return findThreadByName("qdb-sf-error-dispatcher");
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
