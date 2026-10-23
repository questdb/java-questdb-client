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
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The two ways a ring gets its spare after a refusal: the pass whose own trim
 * frees the budget re-provisions at once, and a failed create is retried on
 * the timer with no further event. The second guards the timer against
 * deletion -- without it a transient allocation failure wedges the producer.
 */
public class SegmentManagerProvisionRetryTest {
    private static final int FRAME_PAYLOAD = 64;
    // Exactly two frames per segment.
    private static final long SEGMENT_SIZE = MmapSegment.HEADER_SIZE
            + 2 * (MmapSegment.FRAME_HEADER_SIZE + FRAME_PAYLOAD);

    @Test
    public void testPassWhoseTrimFreesBudgetInstallsSpareInSamePass() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SegmentBudget budget = new SegmentBudget(2 * SEGMENT_SIZE);
            long buf = Unsafe.malloc(FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(MmapSegment.createInMemory(0, SEGMENT_SIZE), SEGMENT_SIZE);
                 SegmentManager manager = new SegmentManager(SEGMENT_SIZE, budget)) {
                // Stopped manager: every pass is driven by hand, so no timer can
                // disguise a stall.
                manager.register(ring, null);
                manager.serviceRingForTesting(ring);
                Assert.assertFalse(ring.needsHotSpare());
                Assert.assertEquals(2 * SEGMENT_SIZE, budget.getSegmentBytes());

                // Third frame rotates into the spare and seals the first segment.
                Assert.assertEquals(0L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals(1L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertEquals(2L, ring.appendOrFsn(buf, FRAME_PAYLOAD));
                Assert.assertTrue(ring.needsHotSpare());

                // One pass: refused on the cap, then trims the acked segment.
                Assert.assertTrue(ring.acknowledge(1L));
                manager.serviceRingForTesting(ring);
                Assert.assertFalse("the pass whose trim freed the budget must install the spare",
                        ring.needsHotSpare());
                Assert.assertEquals(2 * SEGMENT_SIZE, budget.getSegmentBytes());

                manager.deregister(ring);
                Assert.assertEquals(0, budget.getSegmentBytes());
            } finally {
                Unsafe.free(buf, FRAME_PAYLOAD, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFailedCreateIsRetriedWithNoFurtherEvent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            File dir = Files.createTempDirectory("qdb-segmgr-provision-retry").toFile();
            AtomicBoolean failNextAllocate = new AtomicBoolean(true);
            FilesFacade facade = new DelegatingFilesFacade() {
                @Override
                public boolean allocate(int fd, long size) {
                    return !failNextAllocate.compareAndSet(true, false) && super.allocate(fd, size);
                }
            };
            String path = new File(dir, "sf-0000000000000000.sfa").getPath();
            try (SegmentRing ring = new SegmentRing(MmapSegment.create(path, 0, SEGMENT_SIZE), SEGMENT_SIZE);
                 SegmentManager manager = new SegmentManager(SEGMENT_SIZE, 8 * SEGMENT_SIZE, facade)) {
                manager.start();
                // The ring holds only its active segment: no ACK, no rotation and
                // no pin move will ever wake the worker. Only the timer can.
                manager.register(ring, dir.getPath());
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (ring.needsHotSpare()) {
                    Assert.assertTrue("a failed create must be retried with no further event",
                            System.nanoTime() < deadline);
                    Thread.sleep(1);
                }
                Assert.assertFalse("the first create must have been the failed one", failNextAllocate.get());
                manager.deregister(ring);
                Assert.assertTrue(manager.awaitRingQuiescence(ring));
            } finally {
                // Best effort: a cleanup assertion here would mask the real failure.
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
}
