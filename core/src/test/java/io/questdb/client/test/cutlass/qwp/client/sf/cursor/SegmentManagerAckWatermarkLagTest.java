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
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
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
import java.util.concurrent.TimeUnit;

/**
 * ACKs that land inside the active segment never complete a sealed segment, so
 * nothing but the watermark cadence brings the manager back. The on-disk
 * watermark must still catch up on its own: after a crash, recovery replays
 * everything above it as duplicates. Regression for the event-driven worker.
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

    @Test(timeout = 20_000L)
    public void testAcksInsideActiveSegmentReachWatermarkWithoutOtherActivity() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segmentSize = MmapSegment.HEADER_SIZE + 64 * (MmapSegment.FRAME_HEADER_SIZE + 32L);
            MmapSegment initial = MmapSegment.create(tmpDir + "/sf-0000000000000000.sfa", 0L, segmentSize);
            SegmentRing ring = new SegmentRing(initial, segmentSize);
            AckWatermark watermark = AckWatermark.open(tmpDir);
            Assert.assertNotNull(watermark);
            long payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try (SegmentManager manager = new SegmentManager(segmentSize, segmentSize * 8)) {
                manager.register(ring, tmpDir, watermark);
                manager.start();
                // Three rounds: the first ACK after a fully persisted watermark
                // must wake a worker that is parked without a timeout, every time.
                for (int round = 0; round < 3; round++) {
                    for (int i = 0; i < 10; i++) {
                        Assert.assertTrue(ring.appendOrFsn(payload, 32) >= 0);
                    }
                    long acked = round * 10 + 9;
                    ring.acknowledge(acked);
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while (watermark.read() != acked && System.nanoTime() < deadline) {
                        Thread.sleep(1);
                    }
                    Assert.assertEquals("watermark did not catch up with ACKs in round " + round,
                            acked, watermark.read());
                }
            } finally {
                Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
                watermark.close();
                ring.close();
            }
        });
    }
}
