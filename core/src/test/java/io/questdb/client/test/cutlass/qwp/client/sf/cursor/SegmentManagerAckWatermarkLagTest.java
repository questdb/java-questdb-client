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

import io.questdb.client.cutlass.qwp.client.sf.cursor.AckWatermark;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentBudget;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The ack watermark is written by the I/O thread on every ACK, so an ACK that
 * lands inside the active segment is visible in the mmap the moment
 * {@link CursorSendEngine#acknowledge} returns and costs the segment manager
 * nothing: no wake, no timed pass, no state to re-arm.
 */
public class SegmentManagerAckWatermarkLagTest {
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-wm-lag-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        long find = Files.findFirst(tmpDir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(tmpDir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(tmpDir);
    }

    /**
     * The manager is never started, so nothing but the acknowledging thread
     * can have written the watermark.
     */
    @Test(timeout = 20_000L)
    public void testAckWritesWatermarkBeforeReturning() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segmentSize = MmapSegment.HEADER_SIZE + 64 * (MmapSegment.FRAME_HEADER_SIZE + 32L);
            long payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try (SegmentManager manager = new SegmentManager(segmentSize, new SegmentBudget(segmentSize * 8));
                 CursorSendEngine engine = new CursorSendEngine(tmpDir, segmentSize, manager);
                 AckWatermark probe = AckWatermark.open(tmpDir)) {
                Assert.assertNotNull(probe);
                for (int i = 0; i < 10; i++) {
                    engine.appendBlocking(payload, 32);
                }
                Assert.assertTrue(engine.acknowledge(9L));
                Assert.assertEquals(9L, probe.read());
                Assert.assertFalse("re-ack must be a no-op", engine.acknowledge(9L));
                Assert.assertFalse("clamped at publishedFsn, already acked", engine.acknowledge(100L));
                Assert.assertEquals(9L, probe.read());
            } finally {
                Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test(timeout = 30_000L)
    public void testStreamingAcksInsideActiveSegmentNeverWakeWorker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int acks = 20_000;
            // Every frame lands in the active segment below the hot-spare
            // high-water mark: no rotation, no trim, nothing for the worker.
            long segmentSize = MmapSegment.HEADER_SIZE + (acks * 2L + 64) * (MmapSegment.FRAME_HEADER_SIZE + 32L);
            MmapSegment initial = MmapSegment.create(tmpDir + "/sf-0000000000000000.sfa", 0L, segmentSize);
            SegmentRing ring = new SegmentRing(initial, segmentSize);
            AckWatermark watermark = AckWatermark.open(tmpDir);
            Assert.assertNotNull(watermark);
            long payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            AtomicLong passes = new AtomicLong();
            try (SegmentManager manager = new SegmentManager(segmentSize, new SegmentBudget(segmentSize * 8))) {
                manager.setBeforeTrimSyncHook(passes::incrementAndGet);
                manager.register(ring, tmpDir, watermark);
                manager.start();
                for (int i = 0; i < acks; i++) {
                    Assert.assertTrue(ring.appendOrFsn(payload, 32) >= 0);
                }
                Thread.sleep(50); // let the register wake settle
                passes.set(0);
                for (int seq = 0; seq < acks; seq++) {
                    ring.acknowledge(seq);
                }
                Thread.sleep(100);
                Assert.assertEquals("worker ran for ACKs inside the active segment", 0L, passes.get());
            } finally {
                Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
                watermark.close();
                ring.close();
            }
        });
    }
}
