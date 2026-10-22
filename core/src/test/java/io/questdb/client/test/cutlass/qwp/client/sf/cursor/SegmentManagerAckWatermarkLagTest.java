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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ACKs that land inside the active segment never complete a sealed segment, so
 * nothing but the watermark cadence brings the manager back. The on-disk
 * watermark must still catch up on its own: after a crash, recovery replays
 * everything above it as duplicates. Regression for the event-driven worker.
 * <p>
 * The converse bound: while ACKs keep streaming in, the worker must run on the
 * {@link SegmentManager#WATERMARK_LAG_NANOS} cadence, not once per ACK, and
 * must go back to an untimed park once the stream stops.
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
            try (SegmentManager manager = new SegmentManager(segmentSize, new SegmentBudget(segmentSize * 8))) {
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

    /**
     * The round-0 case above with the startup race removed: register seeds the
     * ring's persisted-ACK mark so that the very first ACK wakes the worker.
     * Wait until the worker is parked without a timeout before acking, so the
     * register wake cannot service the ACK by accident.
     */
    @Test(timeout = 20_000L)
    public void testFirstAckAfterRegisterWakesParkedWorker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segmentSize = MmapSegment.HEADER_SIZE + 64 * (MmapSegment.FRAME_HEADER_SIZE + 32L);
            MmapSegment initial = MmapSegment.create(tmpDir + "/sf-0000000000000000.sfa", 0L, segmentSize);
            SegmentRing ring = new SegmentRing(initial, segmentSize);
            AckWatermark watermark = AckWatermark.open(tmpDir);
            Assert.assertNotNull(watermark);
            long payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try (SegmentManager manager = new SegmentManager(segmentSize, new SegmentBudget(segmentSize * 8))) {
                manager.register(ring, tmpDir, watermark);
                manager.start();
                Thread worker = manager.getWorkerThreadForTesting();
                Assert.assertNotNull(worker);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (worker.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
                    Thread.sleep(1);
                }
                Assert.assertEquals("worker must park without a timeout once the register wake is serviced",
                        Thread.State.WAITING, worker.getState());
                for (int i = 0; i < 10; i++) {
                    Assert.assertTrue(ring.appendOrFsn(payload, 32) >= 0);
                }
                ring.acknowledge(9L);
                deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (watermark.read() != 9L && System.nanoTime() < deadline) {
                    Thread.sleep(1);
                }
                Assert.assertEquals("the first ACK after register did not wake the parked worker",
                        9L, watermark.read());
            } finally {
                Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
                watermark.close();
                ring.close();
            }
        });
    }

    @Test(timeout = 30_000L)
    public void testStreamingAcksWakeWorkerOnWatermarkCadenceNotPerAck() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int acks = 20_000;
            final long spacingNanos = 20_000L; // 50k ACK/s
            // Segment large enough that every frame lands in the active
            // segment below the hot-spare high-water mark: no rotation, no
            // trim, nothing but ACKs reaches the worker.
            long segmentSize = MmapSegment.HEADER_SIZE + (acks * 2L + 64) * (MmapSegment.FRAME_HEADER_SIZE + 32L);
            MmapSegment initial = MmapSegment.create(tmpDir + "/sf-0000000000000000.sfa", 0L, segmentSize);
            SegmentRing ring = new SegmentRing(initial, segmentSize);
            AckWatermark watermark = AckWatermark.open(tmpDir);
            Assert.assertNotNull(watermark);
            long payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            AtomicLong passes = new AtomicLong();
            try (SegmentManager manager = new SegmentManager(segmentSize, new SegmentBudget(segmentSize * 8))) {
                // Runs once per serviceRing pass, i.e. once per worker wake or
                // timed park expiry for this ring.
                manager.setBeforeTrimSyncHook(passes::incrementAndGet);
                manager.register(ring, tmpDir, watermark);
                manager.start();
                for (int i = 0; i < acks; i++) {
                    Assert.assertTrue(ring.appendOrFsn(payload, 32) >= 0);
                }
                Thread.sleep(50); // let the register wake settle
                passes.set(0);

                long start = System.nanoTime();
                long next = start;
                for (int seq = 0; seq < acks; ) {
                    if (System.nanoTime() - next >= 0) {
                        ring.acknowledge(seq++);
                        next += spacingNanos;
                    }
                }
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (watermark.read() != acks - 1 && System.nanoTime() < deadline) {
                    Thread.sleep(1);
                }
                Assert.assertEquals("watermark did not catch up after the ACK stream", acks - 1, watermark.read());
                long streamPasses = passes.get();
                // One wake for the first ACK, then at most one pass per
                // WATERMARK_LAG_NANOS; x2 leaves room for scheduler jitter.
                long bound = elapsedMillis * 2 + 16;
                Assert.assertTrue("worker ran " + streamPasses + " passes for " + acks + " ACKs over "
                        + elapsedMillis + " ms; bound " + bound, streamPasses <= bound);

                // Once the stream stops the worker re-arms the ACK wake and
                // parks without a timeout: no further passes while idle.
                Thread.sleep(100);
                long idlePasses = passes.get() - streamPasses;
                Assert.assertTrue("worker kept ticking while idle: " + idlePasses + " passes", idlePasses <= 2);
            } finally {
                Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
                watermark.close();
                ring.close();
            }
        });
    }
}
