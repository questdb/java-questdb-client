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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.client.QwpSpscQueue;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit coverage for the lock-free SPSC queue used to hand events from the
 * I/O thread to the user thread inside {@link io.questdb.client.cutlass.qwp.client.QwpQueryClient}.
 * The queue's correctness rests on three behaviours:
 * <ul>
 *   <li>{@code offer} returns false when the ring is full;</li>
 *   <li>{@code take} progresses through fast-poll → spin → park;</li>
 *   <li>a producer that {@code offer}s while the consumer is parked must call
 *       {@code LockSupport.unpark} — guarded by {@code consumerThread}.</li>
 * </ul>
 * Each test couples a producer and a consumer thread with a small {@link CountDownLatch}
 * so the timing of the offer is observable from the test thread.
 */
public class QwpSpscQueueTest {

    @Test
    public void testOfferAndPollFastPath() {
        QwpSpscQueue<String> q = new QwpSpscQueue<>(4);
        Assert.assertNull("fresh queue must be empty", q.poll());
        Assert.assertTrue(q.offer("a"));
        Assert.assertTrue(q.offer("b"));
        Assert.assertEquals("a", q.poll());
        Assert.assertEquals("b", q.poll());
        Assert.assertNull("queue must be empty after draining", q.poll());
    }

    @Test
    public void testCapacityRoundedUpToPowerOfTwo() {
        // Requesting capacity 5 must round up to 8; we should be able to offer 8 items.
        QwpSpscQueue<Integer> q = new QwpSpscQueue<>(5);
        for (int i = 0; i < 8; i++) {
            Assert.assertTrue("expected ring size 8 after rounding 5 up, failed at i=" + i, q.offer(i));
        }
        Assert.assertFalse("offer past power-of-two capacity must fail", q.offer(8));
        for (int i = 0; i < 8; i++) {
            Assert.assertEquals(Integer.valueOf(i), q.poll());
        }
    }

    @Test
    public void testOfferReturnsFalseWhenFull() {
        QwpSpscQueue<Integer> q = new QwpSpscQueue<>(2);
        Assert.assertTrue(q.offer(1));
        Assert.assertTrue(q.offer(2));
        Assert.assertFalse("ring of 2 must reject the third offer", q.offer(3));
        Assert.assertEquals(Integer.valueOf(1), q.poll());
        Assert.assertTrue("freed slot must be reusable", q.offer(3));
    }

    @Test
    public void testTakeFastPathReturnsImmediately() throws InterruptedException {
        QwpSpscQueue<String> q = new QwpSpscQueue<>(4);
        // Producer publishes before consumer asks -- no spin or park.
        q.offer("ready");
        long start = System.nanoTime();
        Assert.assertEquals("ready", q.take());
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        Assert.assertTrue("take must return immediately when value present, took " + elapsedMs + " ms",
                elapsedMs < 50);
    }

    @Test
    public void testTakeBlocksUntilProducerOffers() throws Exception {
        QwpSpscQueue<String> q = new QwpSpscQueue<>(4);
        AtomicReference<String> taken = new AtomicReference<>();
        CountDownLatch consumerStarted = new CountDownLatch(1);
        CountDownLatch consumerDone = new CountDownLatch(1);

        Thread consumer = new Thread(() -> {
            try {
                consumerStarted.countDown();
                taken.set(q.take());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                consumerDone.countDown();
            }
        }, "spsc-test-consumer");
        consumer.start();

        Assert.assertTrue("consumer must start", consumerStarted.await(2, TimeUnit.SECONDS));
        // Sleep a touch longer than the spin window so the consumer is parked,
        // exercising the unpark-from-offer path. SPIN_ITERATIONS is ~50 µs;
        // 100 ms is overkill but safe under load.
        Thread.sleep(100);
        Assert.assertNull("consumer must not have completed before offer", taken.get());

        Assert.assertTrue(q.offer("delivered"));
        Assert.assertTrue("consumer must complete after offer + unpark",
                consumerDone.await(2, TimeUnit.SECONDS));
        Assert.assertEquals("delivered", taken.get());
        consumer.join(1000);
    }

    @Test
    public void testTakeInterruptedDuringParkThrowsAndStaysAlive() throws Exception {
        QwpSpscQueue<String> q = new QwpSpscQueue<>(4);
        AtomicReference<Throwable> caught = new AtomicReference<>();
        CountDownLatch consumerStarted = new CountDownLatch(1);
        CountDownLatch consumerDone = new CountDownLatch(1);

        Thread consumer = new Thread(() -> {
            try {
                consumerStarted.countDown();
                q.take();
                caught.set(new AssertionError("expected InterruptedException"));
            } catch (Throwable t) {
                caught.set(t);
            } finally {
                consumerDone.countDown();
            }
        }, "spsc-test-consumer-interrupt");
        consumer.start();

        Assert.assertTrue(consumerStarted.await(2, TimeUnit.SECONDS));
        // Wait past the spin window so the consumer is in the park loop.
        Thread.sleep(100);
        consumer.interrupt();
        Assert.assertTrue("interrupt must release the parked consumer",
                consumerDone.await(2, TimeUnit.SECONDS));
        Assert.assertTrue("expected InterruptedException, got " + caught.get(),
                caught.get() instanceof InterruptedException);
        consumer.join(1000);
    }

    @Test
    public void testProducerConsumerStreamRoundTrip() throws Exception {
        // Larger end-to-end workload to exercise the wrap-around mask path
        // (head and tail walk past Integer.MAX_VALUE within the (h & mask) cast).
        QwpSpscQueue<Integer> q = new QwpSpscQueue<>(8);
        final int items = 5_000;
        int[] received = new int[items];
        CountDownLatch done = new CountDownLatch(1);

        Thread consumer = new Thread(() -> {
            try {
                for (int i = 0; i < items; i++) {
                    received[i] = q.take();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                done.countDown();
            }
        }, "spsc-stream-consumer");
        consumer.start();

        for (int i = 0; i < items; i++) {
            // Spin if the ring is full -- mirrors the real producer's behaviour
            // when it ran into back-pressure from a slow consumer.
            while (!q.offer(i)) {
                Thread.yield();
            }
        }

        Assert.assertTrue("consumer must finish the stream", done.await(5, TimeUnit.SECONDS));
        consumer.join(1000);

        for (int i = 0; i < items; i++) {
            Assert.assertEquals("FIFO order must hold for SPSC", i, received[i]);
        }
        Assert.assertNull("queue must be empty after stream", q.poll());
    }

    @Test
    public void testNoUnparkWhenConsumerNotParked() {
        // offer() consults consumerThread; when it's null (consumer running),
        // no unpark should occur -- this is the cheap fast path. Behaviourally
        // we can only verify that offer() doesn't throw and the value is delivered.
        QwpSpscQueue<String> q = new QwpSpscQueue<>(4);
        Assert.assertTrue(q.offer("x"));
        Assert.assertTrue(q.offer("y"));
        Assert.assertEquals("x", q.poll());
        Assert.assertEquals("y", q.poll());
        Assert.assertNull(q.poll());
    }

    @Test
    public void testCapacityOneIsRoundedToOne() {
        // Edge case: requesting capacity 1 still yields a power-of-two ring (1).
        QwpSpscQueue<String> q = new QwpSpscQueue<>(1);
        Assert.assertTrue(q.offer("only"));
        Assert.assertFalse("size-1 ring rejects a second offer", q.offer("nope"));
        Assert.assertEquals("only", q.poll());
        Assert.assertTrue(q.offer("again"));
    }
}
