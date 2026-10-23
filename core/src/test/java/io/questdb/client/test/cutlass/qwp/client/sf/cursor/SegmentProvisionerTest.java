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

import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentBudget;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentProvisioner;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The producer creates the segment it rotates into, on its own thread, at
 * rotation: the budget cap and the liveness floor are applied there, a failed
 * create backpressures the producer and is retried on a timer, and nothing
 * about any of it involves the manager worker.
 */
public class SegmentProvisionerTest {
    private static final int FRAME_PAYLOAD = 64;
    // Exactly two frames per segment.
    private static final long SEGMENT_SIZE = MmapSegment.HEADER_SIZE
            + 2 * (MmapSegment.FRAME_HEADER_SIZE + FRAME_PAYLOAD);

    /** A stopped manager: no worker exists, so only the producer can create segments. */
    @Test
    public void testRotationCreatesSegmentOnProducerThreadWithinCap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SegmentBudget budget = new SegmentBudget(3 * SEGMENT_SIZE);
            long buf = Unsafe.malloc(FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(MmapSegment.createInMemory(0, SEGMENT_SIZE), SEGMENT_SIZE);
                 SegmentManager manager = new SegmentManager(SEGMENT_SIZE, budget)) {
                manager.register(ring, null);
                Assert.assertEquals(SEGMENT_SIZE, budget.getSegmentBytes());
                Assert.assertEquals(0L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals(1L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals("third frame rotates into a segment the producer created",
                        2L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals(2L, ring.getActive().baseSeq());
                Assert.assertEquals(2 * SEGMENT_SIZE, budget.getSegmentBytes());
                Assert.assertEquals(3L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals(4L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals(3 * SEGMENT_SIZE, budget.getSegmentBytes());
                Assert.assertEquals(5L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                // The fourth segment would exceed the cap; the ring is above the floor.
                Assert.assertEquals(SegmentRing.BACKPRESSURE_NO_SPARE, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals(3 * SEGMENT_SIZE, budget.getSegmentBytes());
                // One driven trim pass frees the acked first segment; the retry fits.
                Assert.assertTrue(ring.acknowledge(1L));
                manager.serviceRingForTesting(ring);
                Assert.assertEquals(2 * SEGMENT_SIZE, budget.getSegmentBytes());
                Assert.assertEquals(6L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals(3 * SEGMENT_SIZE, budget.getSegmentBytes());
                manager.deregister(ring);
                Assert.assertEquals(0, budget.getSegmentBytes());
            } finally {
                Unsafe.free(buf, FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * A ring holding only its active segment is charged past the cap: what
     * fills the budget then is nothing this ring's ACKs can shrink.
     */
    @Test
    public void testRingBelowLivenessFloorIsChargedPastCap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SegmentBudget budget = new SegmentBudget(SEGMENT_SIZE + SEGMENT_SIZE / 2);
            long buf = Unsafe.malloc(FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(MmapSegment.createInMemory(0, SEGMENT_SIZE), SEGMENT_SIZE);
                 SegmentManager manager = new SegmentManager(SEGMENT_SIZE, budget)) {
                manager.register(ring, null);
                Assert.assertEquals(0L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals(1L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals("one active segment is below the floor: rotate past the cap",
                        2L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals(2 * SEGMENT_SIZE, budget.getSegmentBytes());
                Assert.assertEquals(3L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals("two segments meet the floor: the cap holds",
                        SegmentRing.BACKPRESSURE_NO_SPARE, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                manager.deregister(ring);
                Assert.assertEquals(0, budget.getSegmentBytes());
            } finally {
                Unsafe.free(buf, FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * A failing create backpressures the producer, releases its charge, and
     * is retried on the timer rather than on every 50 µs poll.
     */
    @Test
    public void testFailedCreateIsRetriedOnTimerNotPerPoll() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            File dir = java.nio.file.Files.createTempDirectory("qdb-provisioner-fail").toFile();
            AtomicLong attempts = new AtomicLong();
            FilesFacade facade = new DelegatingFilesFacade() {
                @Override
                public boolean allocate(int fd, long size) {
                    attempts.incrementAndGet();
                    return false;
                }
            };
            String path = new File(dir, "sf-0000000000000000.sfa").getPath();
            SegmentBudget budget = new SegmentBudget(8 * SEGMENT_SIZE);
            long buf = Unsafe.malloc(FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(MmapSegment.create(path, 0, SEGMENT_SIZE), SEGMENT_SIZE);
                 SegmentManager manager = new SegmentManager(SEGMENT_SIZE, budget, facade, System::nanoTime)) {
                manager.register(ring, dir.getPath());
                Assert.assertEquals(0L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals(1L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                long end = System.nanoTime() + 300_000_000L;
                while (System.nanoTime() < end) {
                    Assert.assertEquals(SegmentRing.BACKPRESSURE_NO_SPARE, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                    Assert.assertEquals("a failed create must release its charge", SEGMENT_SIZE, budget.getSegmentBytes());
                    java.util.concurrent.locks.LockSupport.parkNanos(50_000L);
                }
                long n = attempts.get();
                Assert.assertTrue("expected about 30 create attempts in 300 ms, got " + n, n >= 5 && n < 100);
                Assert.assertEquals("no stray segment file may survive a failed create",
                        1, dir.list().length);
                manager.deregister(ring);
            } finally {
                Unsafe.free(buf, FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
                File[] files = dir.listFiles();
                if (files != null) {
                    for (File f : files) {
                        f.delete();
                    }
                }
                dir.delete();
            }
        });
    }

    /** Disk mode: the file is generation-named past what the slot already holds, with the final base. */
    @Test
    public void testDiskRotationNamesFilePastExistingGenerations() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String dir = Paths.get(System.getProperty("java.io.tmpdir"), "qdb-provisioner-" + System.nanoTime()).toString();
            Assert.assertEquals(0, Files.mkdir(dir, Files.DIR_MODE_DEFAULT));
            long buf = Unsafe.malloc(FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(MmapSegment.create(dir + "/sf-0000000000000007.sfa", 0, SEGMENT_SIZE), SEGMENT_SIZE);
                 SegmentManager manager = new SegmentManager(SEGMENT_SIZE)) {
                manager.register(ring, dir);
                Assert.assertEquals(0L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals(1L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals(2L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals(dir + "/sf-0000000000000008.sfa", ring.getActive().path());
                Assert.assertEquals(2L, ring.getActive().baseSeq());
                Assert.assertTrue(Files.exists(dir + "/sf-0000000000000008.sfa"));
                manager.deregister(ring);
            } finally {
                Unsafe.free(buf, FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
                for (File f : new File(dir).listFiles()) {
                    f.delete();
                }
                Files.remove(dir);
            }
        });
    }
}
