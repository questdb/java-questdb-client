/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\__,_|\___||___/\__|____/|____/
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

import io.questdb.client.cutlass.qwp.client.sf.cursor.ProgressNotifier;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProgressNotifierTest {

    @Test
    public void testNoSignalReturnsFalseWithinTheCap() {
        ProgressNotifier n = new ProgressNotifier();
        long t0 = System.nanoTime();
        assertFalse(n.awaitChange(n.generation(), 2_000_000_000L));
        long elapsed = System.nanoTime() - t0;
        assertTrue("parked: " + elapsed, elapsed >= 5_000_000L);
        assertTrue("capped: " + elapsed, elapsed < 1_000_000_000L);
    }

    @Test
    public void testSecondWaiterStillWakes() throws Exception {
        ProgressNotifier n = new ProgressNotifier();
        long gen = n.generation();
        AtomicInteger woke = new AtomicInteger();
        Runnable waiter = () -> {
            if (awaitChanged(n, gen, 5_000_000_000L)) {
                woke.incrementAndGet();
            }
        };
        Thread a = new Thread(waiter);
        Thread b = new Thread(waiter);
        a.start();
        b.start();
        sleepQuietly(100);
        n.signal();
        a.join(2000);
        b.join(2000);
        assertFalse(a.isAlive());
        assertFalse(b.isAlive());
        assertEquals(2, woke.get());
    }

    @Test
    public void testSignalBeforeAwaitReturnsWithoutParking() {
        ProgressNotifier n = new ProgressNotifier();
        long gen = n.generation();
        n.signal();
        long t0 = System.nanoTime();
        assertTrue(n.awaitChange(gen, 2_000_000_000L));
        assertTrue(System.nanoTime() - t0 < 500_000_000L);
        assertEquals(1L, n.awaitCount());
    }

    @Test
    public void testSignalFromAnotherThreadWakesTheWaiter() throws Exception {
        ProgressNotifier n = new ProgressNotifier();
        long gen = n.generation();
        Thread t = new Thread(() -> {
            sleepQuietly(50);
            n.signal();
        });
        t.start();
        long t0 = System.nanoTime();
        assertTrue(awaitChanged(n, gen, 5_000_000_000L));
        assertTrue(System.nanoTime() - t0 < 2_000_000_000L);
        t.join();
    }

    @Test
    public void testZeroBudgetDoesNotPark() {
        ProgressNotifier n = new ProgressNotifier();
        long t0 = System.nanoTime();
        assertFalse(n.awaitChange(n.generation(), 0L));
        assertTrue(System.nanoTime() - t0 < 100_000_000L);
    }

    private static boolean awaitChanged(ProgressNotifier n, long gen, long timeoutNanos) {
        long deadline = System.nanoTime() + timeoutNanos;
        while (true) {
            long remaining = deadline - System.nanoTime();
            if (n.awaitChange(gen, remaining)) {
                return true;
            }
            if (remaining <= 0) {
                return false;
            }
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
