/*******************************************************************************
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

import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentBudget;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * Several {@link SegmentManager}s charging one {@link SegmentBudget}: what a
 * sender pool does so that {@code sf_max_total_bytes} caps the pool as a whole
 * instead of each pooled sender.
 */
public class SegmentManagerSharedBudgetTest {

    private static final int FRAME_PAYLOAD = 64;
    private static final long POLL_NANOS = 200_000L;
    // Exactly two frames per segment, so the rotation arithmetic below is exact.
    private static final long SEGMENT_SIZE = MmapSegment.HEADER_SIZE
            + 2 * (MmapSegment.FRAME_HEADER_SIZE + FRAME_PAYLOAD);

    @Test
    public void testConcurrentManagersStayWithinBudgetAndReleaseEveryCharge() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Producers on four managers race for one budget while acks trim behind
            // them. tryCharge must never let the budget exceed its capacity by more
            // than the minimum working sets the managers grant unconditionally, and
            // once every ring is deregistered nothing may remain charged.
            final int ringCount = 4;
            final SegmentBudget budget = new SegmentBudget(6 * SEGMENT_SIZE);
            final long bound = budget.getCapacityBytes() + ringCount * 2 * SEGMENT_SIZE;
            final SegmentRing[] rings = new SegmentRing[ringCount];
            final SegmentManager[] managers = new SegmentManager[ringCount];
            final Thread[] producers = new Thread[ringCount];
            final AtomicBoolean stop = new AtomicBoolean();
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            try {
                for (int i = 0; i < ringCount; i++) {
                    rings[i] = newRing();
                    managers[i] = new SegmentManager(SEGMENT_SIZE, POLL_NANOS, budget);
                    managers[i].start();
                    managers[i].register(rings[i], null);
                }
                for (int i = 0; i < ringCount; i++) {
                    final SegmentRing ring = rings[i];
                    final int lag = i;
                    producers[i] = new Thread(() -> {
                        long buf = Unsafe.malloc(FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
                        try {
                            while (!stop.get()) {
                                long fsn = ring.appendOrFsn(buf, FRAME_PAYLOAD);
                                if (fsn >= 0) {
                                    // Ack with a per-ring lag so the rings trim, and
                                    // therefore release, at different rates.
                                    if (fsn > lag) {
                                        ring.acknowledge(fsn - lag);
                                    }
                                } else {
                                    Assert.assertEquals(SegmentRing.BACKPRESSURE_NO_SPARE, fsn);
                                    Thread.yield();
                                }
                            }
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        } finally {
                            Unsafe.free(buf, FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
                        }
                    });
                    producers[i].start();
                }
                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
                while (System.nanoTime() < deadline && failure.get() == null) {
                    long charged = budget.getSegmentBytes();
                    if (charged > bound) {
                        Assert.fail("budget overshot its capacity by more than the minimum working sets: "
                                + charged + " > " + bound);
                    }
                }
            } finally {
                stop.set(true);
                for (Thread producer : producers) {
                    if (producer != null) {
                        producer.join();
                    }
                }
                for (int i = 0; i < ringCount; i++) {
                    if (managers[i] != null && rings[i] != null) {
                        managers[i].deregister(rings[i]);
                        if (!managers[i].awaitRingQuiescence(rings[i])) {
                            failure.compareAndSet(null, new AssertionError("ring " + i + " did not quiesce"));
                        }
                    }
                }
                for (int i = 0; i < ringCount; i++) {
                    if (managers[i] != null) {
                        managers[i].close();
                    }
                    if (rings[i] != null) {
                        rings[i].close();
                    }
                }
            }
            if (failure.get() != null) {
                throw new AssertionError("producer or teardown failed", failure.get());
            }
            Assert.assertEquals("every charge must be released once all rings are deregistered",
                    0, budget.getSegmentBytes());
        });
    }

    @Test
    public void testRingsOfDifferentManagersShareOneBudget() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SegmentBudget budget = new SegmentBudget(4 * SEGMENT_SIZE);
            long buf = Unsafe.malloc(FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing busy = newRing();
                 SegmentRing sibling = newRing();
                 SegmentManager busyManager = new SegmentManager(SEGMENT_SIZE, POLL_NANOS, budget);
                 SegmentManager siblingManager = new SegmentManager(SEGMENT_SIZE, POLL_NANOS, budget)) {
                busyManager.start();
                siblingManager.start();

                // The busy ring grows to the whole budget: three segments of frames
                // plus the active one -- eight frames -- and no further spare.
                busyManager.register(busy, null);
                appendFrames(busy, buf, 8);
                assertCapped(busy, buf);
                Assert.assertEquals(4 * SEGMENT_SIZE, busy.totalSegmentBytes());
                Assert.assertEquals(4 * SEGMENT_SIZE, budget.getSegmentBytes());

                // A ring on ANOTHER manager finds the budget spent. It still gets its
                // minimum working set -- the active segment plus one spare, four
                // frames -- but nothing beyond: a private budget of the same size
                // would have let it grow to four segments of its own.
                siblingManager.register(sibling, null);
                appendFrames(sibling, buf, 4);
                assertCapped(sibling, buf);
                Assert.assertEquals(2 * SEGMENT_SIZE, sibling.totalSegmentBytes());
                Assert.assertEquals("the budget may be exceeded only by the sibling's minimum working set",
                        6 * SEGMENT_SIZE, budget.getSegmentBytes());

                // Once the busy ring gives its segments back, the sibling grows into
                // the room they leave -- up to the whole budget, and no further.
                busyManager.deregister(busy);
                Assert.assertTrue(busyManager.awaitRingQuiescence(busy));
                Assert.assertTrue("the sibling must get a spare once the budget has room",
                        waitFor(() -> !sibling.needsHotSpare()));
                Assert.assertEquals(3 * SEGMENT_SIZE, budget.getSegmentBytes());
                appendFrames(sibling, buf, 4);
                assertCapped(sibling, buf);
                Assert.assertEquals(4 * SEGMENT_SIZE, sibling.totalSegmentBytes());
                Assert.assertEquals(4 * SEGMENT_SIZE, budget.getSegmentBytes());

                siblingManager.deregister(sibling);
                Assert.assertTrue(siblingManager.awaitRingQuiescence(sibling));
                Assert.assertEquals(0, budget.getSegmentBytes());
            } finally {
                Unsafe.free(buf, FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    // Appends `frames` frames, waiting out the moments a rotation finds the
    // spare not yet provisioned. Every frame is expected to fit eventually.
    private static void appendFrames(SegmentRing ring, long buf, int frames) throws InterruptedException {
        for (int i = 0; i < frames; i++) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            long fsn;
            while ((fsn = ring.appendOrFsn(buf, FRAME_PAYLOAD)) == SegmentRing.BACKPRESSURE_NO_SPARE) {
                Assert.assertTrue("no spare arrived for frame " + i, System.nanoTime() < deadline);
                Thread.sleep(1);
            }
            Assert.assertTrue("append failed: " + fsn, fsn >= 0);
        }
    }

    private static void assertCapped(SegmentRing ring, long buf) throws InterruptedException {
        // Ample time for a manager worker to (wrongly) provision past the budget.
        Thread.sleep(100);
        Assert.assertTrue("the budget must refuse this ring another spare", ring.needsHotSpare());
        Assert.assertEquals(SegmentRing.BACKPRESSURE_NO_SPARE, ring.appendOrFsn(buf, FRAME_PAYLOAD));
    }

    private static SegmentRing newRing() {
        return new SegmentRing(MmapSegment.createInMemory(0, SEGMENT_SIZE), SEGMENT_SIZE);
    }

    private static boolean waitFor(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            Thread.sleep(1);
        }
        return true;
    }
}
