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
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.DelegatingFilesFacade;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link SegmentRing#acknowledge} runs on the I/O thread for every server
 * ACK. In disk store-and-forward mode the producer's rotation and the
 * manager's trim staging both hold the ring monitor across a manifest
 * fsync, so the ACK path must never take that monitor: while it waits, the
 * I/O thread neither sends nor receives. The trim wake it used to derive
 * from the monitored {@code firstTrimmable()} must still fire.
 */
public class SegmentRingAckOffMonitorTest {
    private static final int PAYLOAD = 64;
    private static final long SEGMENT_SIZE = MmapSegment.HEADER_SIZE + 4 * (MmapSegment.FRAME_HEADER_SIZE + PAYLOAD);
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-sf-ack-monitor-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDir(tmpDir);
    }

    @Test(timeout = 20_000L)
    public void testAcknowledgeDoesNotWaitForManifestFsyncHeldUnderRingMonitor() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Legacy slot with one frame (seq 0): recovery resumes it as the
            // active segment and creates a manifest that goes through ff.
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa");
            StallingFsyncFacade ff = new StallingFsyncFacade();
            SegmentRing ring = SegmentRing.openExisting(ff, tmpDir, SEGMENT_SIZE);
            Assert.assertNotNull("legacy slot must recover", ring);
            long payload = Unsafe.malloc(PAYLOAD, MemoryTag.NATIVE_DEFAULT);
            try {
                ring.installHotSpare(MmapSegment.create(tmpDir + "/sf-0000000000000004.sfa", 4L, SEGMENT_SIZE));
                // Fill the active: seqs 1..3. The next append rotates.
                for (int i = 0; i < 3; i++) {
                    Assert.assertTrue(ring.appendOrFsn(payload, PAYLOAD) >= 0);
                }
                ff.stall = true;
                Thread producer = new Thread(() -> Assert.assertEquals(4L, ring.appendOrFsn(payload, PAYLOAD)), "producer");
                producer.start();
                Assert.assertTrue("rotation must reach the manifest fsync", ff.inFsync.await(5, TimeUnit.SECONDS));
                // The rotation now holds the ring monitor inside the stalled fsync.
                long start = System.nanoTime();
                Assert.assertTrue(ring.acknowledge(1));
                long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                producer.join();
                ff.stall = false;
                Assert.assertTrue("acknowledge waited " + tookMs + " ms behind the manifest fsync", tookMs < 50);

                // The sealed segment holds seqs 0..3. The ACK that completes it
                // must still wake the manager; the one before it must not.
                Assert.assertNotNull(ring.firstSealed());
                AtomicInteger wakes = new AtomicInteger();
                ring.setManagerWakeup(wakes::incrementAndGet);
                Assert.assertTrue(ring.acknowledge(2));
                Assert.assertEquals("ACK inside the oldest sealed segment must not wake", 0, wakes.get());
                Assert.assertTrue(ring.acknowledge(3));
                Assert.assertEquals("ACK completing the oldest sealed segment must wake", 1, wakes.get());
                Assert.assertNotNull(ring.firstTrimmable());
            } finally {
                Unsafe.free(payload, PAYLOAD, MemoryTag.NATIVE_DEFAULT);
                ring.close();
            }
        });
    }

    private static void writeSegmentWithFrames(String path) {
        long buf = Unsafe.malloc(PAYLOAD, MemoryTag.NATIVE_DEFAULT);
        try {
            MmapSegment segment = MmapSegment.create(path, 0L, SEGMENT_SIZE);
            try {
                for (int b = 0; b < PAYLOAD; b++) {
                    Unsafe.getUnsafe().putByte(buf + b, (byte) b);
                }
                Assert.assertTrue("test frame must fit", segment.tryAppend(buf, PAYLOAD) >= 0);
            } finally {
                segment.close();
            }
        } finally {
            Unsafe.free(buf, PAYLOAD, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static final class StallingFsyncFacade extends DelegatingFilesFacade {
        private final CountDownLatch inFsync = new CountDownLatch(1);
        private volatile boolean stall;

        @Override
        public int fsync(int fd) {
            if (stall) {
                inFsync.countDown();
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return super.fsync(fd);
        }
    }
}
