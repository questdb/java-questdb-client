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

import io.questdb.client.cutlass.qwp.client.sf.cursor.AckWaiters;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * {@link AckWaiters} alone, no sender: every {@code awaitAckedFsn} / {@code drain}
 * call that has to wait registers and deregisters once, so steady-state
 * add + remove must not allocate; and a signal must reach every registered
 * waiter and none of the deregistered ones.
 */
public class AckWaitersTest {
    private static final int ITERATIONS = 10_000;
    private static final int WARMUP = 20_000;

    @Test(timeout = 60_000L)
    public void testAddRemoveDoesNotAllocateWithOneWaiter() {
        AckWaiters w = new AckWaiters();
        assertAddRemoveAllocatesNothing(w);
    }

    @Test(timeout = 60_000L)
    public void testAddRemoveDoesNotAllocateWithThreeWaiters() throws Exception {
        AckWaiters w = new AckWaiters();
        CountDownLatch registered = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        Thread[] others = new Thread[2];
        for (int i = 0; i < others.length; i++) {
            others[i] = new Thread(() -> {
                Thread me = Thread.currentThread();
                w.add(me);
                registered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    w.remove(me);
                }
            }, "ack-waiters-bystander-" + i);
            others[i].start();
        }
        try {
            Assert.assertTrue(registered.await(5, TimeUnit.SECONDS));
            // two waiters stay registered the whole time; we are the third
            assertAddRemoveAllocatesNothing(w);
        } finally {
            release.countDown();
            for (Thread t : others) {
                t.join(5_000);
            }
        }
    }

    /**
     * Five waiters: more than the initial capacity, so this also covers
     * growth. Every one of them must be unparked by a single signal.
     */
    @Test(timeout = 60_000L)
    public void testSignalUnparksEveryRegisteredWaiter() throws Exception {
        AckWaiters w = new AckWaiters();
        AtomicBoolean go = new AtomicBoolean();
        AtomicInteger woken = new AtomicInteger();
        CountDownLatch registered = new CountDownLatch(5);
        Thread[] waiters = new Thread[5];
        for (int i = 0; i < waiters.length; i++) {
            waiters[i] = new Thread(() -> {
                Thread me = Thread.currentThread();
                w.add(me);
                registered.countDown();
                try {
                    while (!go.get()) {
                        LockSupport.park(w);
                    }
                    woken.incrementAndGet();
                } finally {
                    w.remove(me);
                }
            }, "ack-waiter-" + i);
            waiters[i].start();
        }
        Assert.assertTrue(registered.await(5, TimeUnit.SECONDS));
        for (Thread t : waiters) {
            awaitParked(t);
        }
        go.set(true);
        w.signal();
        for (Thread t : waiters) {
            t.join(10_000);
            Assert.assertFalse(t.getName() + " still parked after signal()", t.isAlive());
        }
        Assert.assertEquals(waiters.length, woken.get());
    }

    /**
     * A waiter that deregistered before the signal must not be handed a
     * permit: its next park has to actually wait. The remaining registered
     * waiter proves the signal was delivered.
     */
    @Test(timeout = 60_000L)
    public void testSignalSkipsRemovedWaiter() throws Exception {
        AckWaiters w = new AckWaiters();
        AtomicBoolean go = new AtomicBoolean();
        CountDownLatch bothRegistered = new CountDownLatch(2);
        CountDownLatch removedOne = new CountDownLatch(1);
        CountDownLatch signalled = new CountDownLatch(1);
        long[] departedParkedNanos = new long[1];

        Thread departed = new Thread(() -> {
            Thread me = Thread.currentThread();
            w.add(me);
            bothRegistered.countDown();
            w.remove(me);
            removedOne.countDown();
            try {
                signalled.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            long t0 = System.nanoTime();
            LockSupport.parkNanos(w, TimeUnit.MILLISECONDS.toNanos(300));
            departedParkedNanos[0] = System.nanoTime() - t0;
        }, "ack-waiter-departed");
        Thread staying = new Thread(() -> {
            Thread me = Thread.currentThread();
            w.add(me);
            bothRegistered.countDown();
            try {
                while (!go.get()) {
                    LockSupport.park(w);
                }
            } finally {
                w.remove(me);
            }
        }, "ack-waiter-staying");
        departed.start();
        staying.start();

        Assert.assertTrue(bothRegistered.await(5, TimeUnit.SECONDS));
        Assert.assertTrue(removedOne.await(5, TimeUnit.SECONDS));
        awaitParked(staying);
        go.set(true);
        w.signal();
        signalled.countDown();
        staying.join(10_000);
        Assert.assertFalse("registered waiter not woken", staying.isAlive());
        departed.join(10_000);
        Assert.assertFalse(departed.isAlive());
        // a leaked permit would make parkNanos return in microseconds
        Assert.assertTrue("removed waiter was unparked: parked only " + departedParkedNanos[0] / 1_000_000L + " ms",
                departedParkedNanos[0] >= TimeUnit.MILLISECONDS.toNanos(100));
    }

    private static void assertAddRemoveAllocatesNothing(AckWaiters w) {
        ThreadMXBean plain = ManagementFactory.getThreadMXBean();
        Assume.assumeTrue("com.sun.management.ThreadMXBean required", plain instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean mx = (com.sun.management.ThreadMXBean) plain;
        Assume.assumeTrue("thread allocation accounting required", mx.isThreadAllocatedMemorySupported());
        if (!mx.isThreadAllocatedMemoryEnabled()) {
            mx.setThreadAllocatedMemoryEnabled(true);
        }
        Thread me = Thread.currentThread();
        long tid = me.getId();
        for (int i = 0; i < WARMUP; i++) {
            w.add(me);
            w.remove(me);
        }
        long before = mx.getThreadAllocatedBytes(tid);
        for (int i = 0; i < ITERATIONS; i++) {
            w.add(me);
            w.remove(me);
        }
        long bytes = mx.getThreadAllocatedBytes(tid) - before;
        // getThreadAllocatedBytes itself may allocate a few dozen bytes on
        // JDK 8; anything per iteration shows up as kilobytes.
        System.out.println("AckWaiters add+remove: " + bytes + " B over " + ITERATIONS + " iterations");
        Assert.assertEquals("bytes per add+remove", 0L, bytes / ITERATIONS);
        Assert.assertTrue("add+remove allocated " + bytes + " B over " + ITERATIONS + " iterations", bytes < 1024);
    }

    private static void awaitParked(Thread t) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (t.getState() != Thread.State.WAITING && t.getState() != Thread.State.TIMED_WAITING) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(t.getName() + " never parked: " + t.getState());
            }
            Thread.sleep(1);
        }
    }
}
