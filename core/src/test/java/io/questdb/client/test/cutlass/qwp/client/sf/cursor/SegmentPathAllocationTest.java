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
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SegmentPathAllocationTest {
    private static final int OBJECT_SAMPLES = 1024;
    private static final int OBJECT_WARMUP = 10_000;
    private static final int PAYLOAD = 64;
    private static final int ROTATIONS = 256;
    private static final int ROTATION_WARMUP = 64;
    private static final long SIZE = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + PAYLOAD;
    private static volatile MmapSegment observed;

    @Test(timeout = 30_000L)
    public void testRotationsDoNotAllocatePaths() throws Exception {
        Assume.assumeTrue(ManagementFactory.getThreadMXBean() instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean mx = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        Assume.assumeTrue(mx.isThreadAllocatedMemorySupported());
        mx.setThreadAllocatedMemoryEnabled(true);
        TestUtils.assertMemoryLeak(() -> {
            long tid = Thread.currentThread().getId();
            long objectBytes = 0;
            for (int i = 0; i < OBJECT_WARMUP + OBJECT_SAMPLES; i++) {
                long before = mx.getThreadAllocatedBytes(tid);
                MmapSegment segment = MmapSegment.createInMemory(0, SIZE);
                observed = segment; // Keep the object from being scalar-replaced.
                long bytes = mx.getThreadAllocatedBytes(tid) - before;
                if (i >= OBJECT_WARMUP) {
                    objectBytes += bytes;
                }
                segment.close();
                observed = null;
            }
            objectBytes /= OBJECT_SAMPLES;
            Assert.assertTrue("segment object must be counted", objectBytes > 0);
            String dir = TestUtils.createTmpDir("qdb-path-allocation-");
            long buf = Unsafe.malloc(PAYLOAD, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(MmapSegment.create(dir + "/sf-initial.sfa", 0, SIZE), SIZE);
                 SegmentManager manager = new SegmentManager(SIZE)) {
                manager.register(ring, dir);
                // Exclude sealed-list growth from the path allocation measurement.
                ring.getSealedSegments().checkCapacity(512);
                Assert.assertEquals(0L, ring.appendOrFsn(buf, PAYLOAD));
                MmapSegment first = null;
                long allocated = 0;
                for (int i = 1; i <= ROTATION_WARMUP + ROTATIONS; i++) {
                    long before = mx.getThreadAllocatedBytes(tid);
                    long fsn = ring.appendOrFsn(buf, PAYLOAD);
                    long bytes = mx.getThreadAllocatedBytes(tid) - before;
                    Assert.assertEquals(i, fsn);
                    if (i > ROTATION_WARMUP) {
                        allocated += bytes;
                    }
                    if (i == 1) {
                        first = ring.getActive();
                    }
                }
                // JDK 8 may charge a few hundred one-off runtime bytes. Any
                // extra object per rotation exceeds this allowance over 256 rotations.
                Assert.assertTrue(allocated >= objectBytes * ROTATIONS);
                Assert.assertTrue("unexpected rotation allocation: " + allocated,
                        allocated - objectBytes * ROTATIONS < 1024);
                // Check old filenames after subsequent rotations overwrite the native buffer.
                Assert.assertEquals(dir + "/sf-0000000000000000.sfa", first.path());
                Assert.assertEquals(dir + "/sf-000000000000013f.sfa", ring.getActive().path());
                Assert.assertEquals(320L, ring.getActive().baseSeq());
                manager.deregister(ring);
            } finally {
                Unsafe.free(buf, PAYLOAD, MemoryTag.NATIVE_DEFAULT);
                TestUtils.removeTmpDir(dir);
            }
        });
    }

    @Test
    public void testLongUnicodePathAndFailedNativeDirectorySync() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String root = TestUtils.createTmpDir("qdb-path-unicode-");
            StringBuilder part = new StringBuilder();
            for (int i = 0; i < 64; i++) {
                part.append('a');
            }
            String parent = root + "/" + part;
            Assert.assertEquals(0, Files.mkdir(parent, Files.DIR_MODE_DEFAULT));
            part.setLength(0);
            for (int i = 0; i < 14; i++) {
                part.append("žluť😀");
            }
            String dir = parent + "/" + part;
            Assert.assertEquals(0, Files.mkdir(dir, Files.DIR_MODE_DEFAULT));
            AtomicBoolean failSync = new AtomicBoolean(true);
            AtomicInteger syncCalls = new AtomicInteger();
            Thread producer = Thread.currentThread();
            DelegatingFilesFacade ff = new DelegatingFilesFacade() {
                @Override
                public boolean allocate(int fd, long bytes) {
                    Assert.assertSame(producer, Thread.currentThread());
                    return super.allocate(fd, bytes);
                }
                @Override
                public int fsyncDir(long ptr) {
                    Assert.assertEquals(dir, Files.utf8ToString(ptr));
                    syncCalls.incrementAndGet();
                    return failSync.get() ? -1 : Files.fsyncDir(ptr);
                }
            };
            SegmentBudget budget = new SegmentBudget(4 * SIZE);
            long buf = Unsafe.malloc(PAYLOAD, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(MmapSegment.create(dir + "/sf-0000000000000007.sfa", 0, SIZE), SIZE);
                 SegmentManager manager = new SegmentManager(SIZE, budget, ff, System::nanoTime)) {
                manager.register(ring, dir);
                Assert.assertEquals(0L, ring.appendOrFsn(buf, PAYLOAD));
                Assert.assertEquals(SegmentRing.BACKPRESSURE_NO_SPARE, ring.appendOrFsn(buf, PAYLOAD));
                Assert.assertEquals(SIZE, budget.getSegmentBytes());
                Assert.assertEquals("failed directory barrier removes created file", 1, new File(dir).list().length);
                failSync.set(false);
                Thread.sleep(20);
                Assert.assertEquals(1L, ring.appendOrFsn(buf, PAYLOAD));
                Assert.assertEquals(dir + "/sf-0000000000000009.sfa", ring.getActive().path());
                Assert.assertEquals(2, syncCalls.get());
                Assert.assertEquals(2 * SIZE, budget.getSegmentBytes());
                manager.deregister(ring);
                Assert.assertEquals(0L, budget.getSegmentBytes());
            } finally {
                Unsafe.free(buf, PAYLOAD, MemoryTag.NATIVE_DEFAULT);
                TestUtils.removeTmpDir(root);
            }
        });
    }
}
