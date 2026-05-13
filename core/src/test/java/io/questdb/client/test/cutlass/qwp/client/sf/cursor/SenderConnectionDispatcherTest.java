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

import io.questdb.client.SenderConnectionEvent;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderConnectionDispatcher;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SenderConnectionDispatcherTest {

    @Test
    public void testCloseDrainsRemainingEntries() {
        // After close(), entries already in the queue should still be
        // delivered within the drain deadline. Mirrors SenderErrorDispatcher
        // contract: shutdown is bounded but does not silently drop.
        List<SenderConnectionEvent> received = new ArrayList<>();
        Object lock = new Object();
        SenderConnectionDispatcher d = new SenderConnectionDispatcher(ev -> {
            synchronized (lock) {
                received.add(ev);
            }
        });
        for (int i = 0; i < 10; i++) {
            Assert.assertTrue(d.offer(buildEvent(i)));
        }
        d.close();
        synchronized (lock) {
            Assert.assertTrue("expected drain to deliver most entries; got " + received.size(),
                    received.size() >= 5);
        }
    }

    @Test
    public void testCloseIsIdempotent() {
        SenderConnectionDispatcher d = new SenderConnectionDispatcher(ev -> { /* no-op */ });
        d.offer(buildEvent(0));
        d.close();
        d.close();
        d.close();
    }

    @Test
    public void testConstructorRejectsBadCapacity() {
        try {
            new SenderConnectionDispatcher(ev -> { /* no-op */ }, 0).close();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("capacity"));
        }
        try {
            new SenderConnectionDispatcher(ev -> { /* no-op */ }, -1).close();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }

    @Test
    public void testConstructorRejectsNullListener() {
        try {
            new SenderConnectionDispatcher(null).close();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("listener"));
        }
    }

    @Test
    public void testFullInboxDropsOldestAndCounts() throws Exception {
        // Inherits sf-client.md section 14.6: on overflow, drop the OLDEST
        // entry and admit the new one. Later connection events carry the
        // freshest state, so dropping the head loses the least information.
        CountDownLatch unblock = new CountDownLatch(1);
        List<SenderConnectionEvent> received = new ArrayList<>();
        Object lock = new Object();
        CountDownLatch allDelivered = new CountDownLatch(5);
        try (SenderConnectionDispatcher d = new SenderConnectionDispatcher(ev -> {
            try {
                unblock.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            synchronized (lock) {
                received.add(ev);
            }
            allDelivered.countDown();
        }, /*capacity=*/ 4)) {
            // First offer starts the dispatcher and lands in the listener
            // immediately (and blocks there), freeing one slot. Now we can
            // fill the bounded inbox to capacity (4), then overflow.
            Assert.assertTrue(d.offer(buildEvent(0)));
            TimeUnit.MILLISECONDS.sleep(50);
            for (int i = 1; i <= 4; i++) {
                Assert.assertTrue("inbox should accept offer " + i,
                        d.offer(buildEvent(i)));
            }
            // Inbox holds [1,2,3,4]. The next two offers each drop the head
            // (1, then 2) to admit the new arrival, returning true.
            Assert.assertTrue("offer beyond capacity must admit the new entry",
                    d.offer(buildEvent(5)));
            Assert.assertTrue(d.offer(buildEvent(6)));
            Assert.assertEquals(2L, d.getDroppedNotifications());

            unblock.countDown();
            Assert.assertTrue("all retained entries should deliver within 5s",
                    allDelivered.await(5, TimeUnit.SECONDS));
            synchronized (lock) {
                long[] attempts = new long[received.size()];
                for (int i = 0; i < received.size(); i++) {
                    attempts[i] = received.get(i).getAttemptNumber();
                }
                Assert.assertArrayEquals("drop-oldest must retain the newest tail",
                        new long[]{0, 3, 4, 5, 6}, attempts);
            }
        } finally {
            unblock.countDown();
        }
    }

    @Test
    public void testListenerThrowDoesNotKillDispatcher() throws Exception {
        // A listener that throws on the first call must not poison the
        // dispatcher; subsequent offers must still deliver.
        AtomicInteger delivered = new AtomicInteger();
        AtomicInteger thrown = new AtomicInteger();
        try (SenderConnectionDispatcher d = new SenderConnectionDispatcher(ev -> {
            delivered.incrementAndGet();
            if (thrown.incrementAndGet() == 1) {
                throw new RuntimeException("simulated listener bug");
            }
        })) {
            Assert.assertTrue(d.offer(buildEvent(1)));
            Assert.assertTrue(d.offer(buildEvent(2)));
            Assert.assertTrue(d.offer(buildEvent(3)));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (delivered.get() < 3 && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.sleep(10);
            }
            Assert.assertEquals(3, delivered.get());
            Assert.assertEquals(3L, d.getTotalDelivered());
        }
    }

    @Test
    public void testNullEventIsRejectedSilently() {
        AtomicInteger delivered = new AtomicInteger();
        try (SenderConnectionDispatcher d = new SenderConnectionDispatcher(ev -> delivered.incrementAndGet())) {
            Assert.assertFalse(d.offer(null));
            Assert.assertEquals(0L, d.getDroppedNotifications());
            Assert.assertEquals(0, delivered.get());
        }
    }

    @Test
    public void testOfferAfterCloseReturnsFalse() {
        AtomicInteger delivered = new AtomicInteger();
        SenderConnectionDispatcher d = new SenderConnectionDispatcher(ev -> delivered.incrementAndGet());
        d.close();
        Assert.assertFalse(d.offer(buildEvent(1)));
        Assert.assertEquals(0L, d.getDroppedNotifications());
    }

    @Test
    public void testOrderingIsFifo() throws Exception {
        // ArrayBlockingQueue is FIFO; verify ordering is preserved end-to-end
        // so users can rely on event sequence matching the I/O thread's
        // observation order.
        int n = 50;
        List<SenderConnectionEvent> received = new ArrayList<>();
        Object lock = new Object();
        CountDownLatch all = new CountDownLatch(n);
        try (SenderConnectionDispatcher d = new SenderConnectionDispatcher(ev -> {
            synchronized (lock) {
                received.add(ev);
            }
            all.countDown();
        })) {
            for (int i = 0; i < n; i++) {
                Assert.assertTrue(d.offer(buildEvent(i)));
            }
            Assert.assertTrue("all entries should deliver within 5s",
                    all.await(5, TimeUnit.SECONDS));
            synchronized (lock) {
                for (int i = 0; i < n; i++) {
                    Assert.assertEquals("FIFO ordering broken at index " + i,
                            i, received.get(i).getAttemptNumber());
                }
            }
        }
    }

    @Test
    public void testSetListenerSwapTakesEffect() throws Exception {
        // Default-constructed dispatcher routes to the no-op listener. After
        // setListener installs a custom one, subsequent offers go to the new
        // listener immediately -- no need to tear down the dispatcher thread.
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();
        CountDownLatch firstLatch = new CountDownLatch(1);
        CountDownLatch secondLatch = new CountDownLatch(1);
        try (SenderConnectionDispatcher d = new SenderConnectionDispatcher(ev -> {
            first.incrementAndGet();
            firstLatch.countDown();
        })) {
            Assert.assertTrue(d.offer(buildEvent(1)));
            Assert.assertTrue(firstLatch.await(2, TimeUnit.SECONDS));
            Assert.assertEquals(1, first.get());
            d.setListener(ev -> {
                second.incrementAndGet();
                secondLatch.countDown();
            });
            Assert.assertTrue(d.offer(buildEvent(2)));
            Assert.assertTrue(secondLatch.await(2, TimeUnit.SECONDS));
            Assert.assertEquals(1, second.get());
            Assert.assertEquals("first listener must not see post-swap event",
                    1, first.get());
        }
    }

    @Test
    public void testTotalDeliveredCounter() throws Exception {
        CountDownLatch all = new CountDownLatch(3);
        try (SenderConnectionDispatcher d = new SenderConnectionDispatcher(ev -> all.countDown())) {
            d.offer(buildEvent(1));
            d.offer(buildEvent(2));
            d.offer(buildEvent(3));
            Assert.assertTrue(all.await(2, TimeUnit.SECONDS));
            Assert.assertEquals(3L, d.getTotalDelivered());
            Assert.assertEquals(0L, d.getDroppedNotifications());
        }
    }

    private static SenderConnectionEvent buildEvent(int seq) {
        // attemptNumber doubles as the test's identity field: easiest to
        // assert on for FIFO ordering.
        return new SenderConnectionEvent(
                SenderConnectionEvent.Kind.ENDPOINT_ATTEMPT_FAILED,
                "host-" + seq,
                9000 + seq,
                null,
                SenderConnectionEvent.NO_PORT,
                seq,
                1L,
                null,
                System.currentTimeMillis()
        );
    }
}
