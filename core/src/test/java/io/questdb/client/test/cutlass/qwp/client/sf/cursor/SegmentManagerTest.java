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
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class SegmentManagerTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-segmgr-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, 0755));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        long find = Files.findFirst(tmpDir);
        if (find != 0) {
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

    @Test
    public void testManagerProvisionsSpareWithinPollingTick() throws Exception {
        long segSize = MmapSegment.HEADER_SIZE
                + 4 * (MmapSegment.FRAME_HEADER_SIZE + 32);
        MmapSegment seg0 = MmapSegment.create(tmpDir + "/0000000000000000.sfa", 0, segSize);
        try (SegmentRing ring = new SegmentRing(seg0, segSize);
             SegmentManager mgr = new SegmentManager(segSize, 200_000L /* 0.2ms */)) {
            mgr.start();
            mgr.register(ring, tmpDir);

            // Wait for the manager to install a spare. Should happen within ~ms.
            assertTrue("manager should install hot spare within 2 seconds",
                    waitFor(() -> !ring.needsHotSpare(), 2000));
        }
    }

    @Test
    public void testProducerCanRotateAcrossManySegmentsWithoutBackpressure() throws Exception {
        long segSize = MmapSegment.HEADER_SIZE
                + 4 * (MmapSegment.FRAME_HEADER_SIZE + 32);
        MmapSegment seg0 = MmapSegment.create(tmpDir + "/0000000000000000.sfa", 0, segSize);
        long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try (SegmentRing ring = new SegmentRing(seg0, segSize);
             SegmentManager mgr = new SegmentManager(segSize, 200_000L)) {
            mgr.start();
            mgr.register(ring, tmpDir);

            for (int i = 0; i < 32; i++) {
                Unsafe.getUnsafe().putInt(buf, i);
                long fsn;
                long deadline = System.nanoTime() + 5_000_000_000L; // 5 seconds
                while (true) {
                    fsn = ring.appendOrFsn(buf, 32);
                    if (fsn >= 0) break;
                    if (fsn == SegmentRing.PAYLOAD_TOO_LARGE) {
                        throw new AssertionError("payload too large at i=" + i);
                    }
                    // BACKPRESSURE_NO_SPARE — wait for the manager to catch up.
                    if (System.nanoTime() > deadline) {
                        throw new AssertionError(
                                "stuck waiting for spare at i=" + i + ", needsSpare=" + ring.needsHotSpare());
                    }
                    Thread.onSpinWait();
                }
                assertEquals(i, fsn);
            }
        } finally {
            Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testManagerTrimsAckedSegmentFiles() throws Exception {
        long segSize = MmapSegment.HEADER_SIZE
                + 2 * (MmapSegment.FRAME_HEADER_SIZE + 32);
        String seg0Path = tmpDir + "/0000000000000000.sfa";
        MmapSegment seg0 = MmapSegment.create(seg0Path, 0, segSize);
        long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
        try (SegmentRing ring = new SegmentRing(seg0, segSize);
             SegmentManager mgr = new SegmentManager(segSize, 200_000L)) {
            mgr.start();
            mgr.register(ring, tmpDir);

            // Fill seg0 (2 frames) and force rotation by appending a third.
            for (int i = 0; i < 2; i++) ring.appendOrFsn(buf, 32);
            // Wait for the spare for seg1 to land.
            assertTrue(waitFor(() -> !ring.needsHotSpare(), 2000));
            ring.appendOrFsn(buf, 32);                 // FSN 2, rotates active to seg1

            assertTrue("seg0 should still exist before ack", Files.exists(seg0Path));

            // ACK every frame in seg0; manager should remove the file.
            ring.acknowledge(1);
            assertTrue("manager should unlink seg0 within 2 seconds",
                    waitFor(() -> !Files.exists(seg0Path), 2000));
        } finally {
            Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMaxTotalBytesCapBlocksProvisioningUntilTrimFrees() throws Exception {
        long segSize = MmapSegment.HEADER_SIZE
                + 2 * (MmapSegment.FRAME_HEADER_SIZE + 64);
        // Cap = exactly 2 manager-provisioned segments. The engine's initial
        // active is "free" per the cap's documented approximation.
        long cap = 2 * segSize;
        MmapSegment seg0 = MmapSegment.create(tmpDir + "/0000000000000000.sfa", 0, segSize);
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try (SegmentRing ring = new SegmentRing(seg0, segSize);
             SegmentManager mgr = new SegmentManager(segSize, 200_000L, cap)) {
            mgr.start();
            mgr.register(ring, tmpDir);

            // Manager provisions spare 1 → counter = 1*segSize.
            assertTrue(waitFor(() -> !ring.needsHotSpare(), 2000));
            // Fill initial (becomes sealed), rotate to spare 1.
            ring.appendOrFsn(buf, 64);
            ring.appendOrFsn(buf, 64);
            ring.appendOrFsn(buf, 64); // forces rotation
            // Manager provisions spare 2 → counter = 2*segSize. At cap.
            assertTrue(waitFor(() -> !ring.needsHotSpare(), 2000));
            // Fill spare 1 (becomes sealed), rotate to spare 2.
            ring.appendOrFsn(buf, 64);
            ring.appendOrFsn(buf, 64); // forces rotation again
            // Manager would provision spare 3 → would be 3*segSize > cap. Refused.
            // The ring should sit in needsHotSpare=true indefinitely.
            // Verify: after ample time, still no spare.
            Thread.sleep(150);
            assertTrue("manager must respect cap and not provision spare 3", ring.needsHotSpare());
            // Producer's appendOrFsn must report backpressure.
            ring.appendOrFsn(buf, 64); // fills the second-to-last slot of spare 2
            ring.appendOrFsn(buf, 64); // fills the last slot, spare 2 now full
            assertEquals(SegmentRing.BACKPRESSURE_NO_SPARE, ring.appendOrFsn(buf, 64));

            // Now ACK enough frames to make the oldest sealed segment trimmable.
            // The initial held FSN 0..1 (2 frames). ACK frame 1 → initial trims.
            ring.acknowledge(1L);
            // The manager should trim → totalBytes drops by 1*segSize → headroom
            // for one more spare → spare 3 gets installed.
            assertTrue("manager must provision a spare once trim freed space",
                    waitFor(() -> !ring.needsHotSpare(), 2000));
            // And the once-stuck producer's append now succeeds.
            assertNotEquals(SegmentRing.BACKPRESSURE_NO_SPARE,
                    ring.appendOrFsn(buf, 64));
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testCloseStopsWorkerAndIsIdempotent() throws Exception {
        SegmentManager mgr = new SegmentManager(8192, 200_000L);
        mgr.start();
        // Give the worker a moment to exist.
        Thread.sleep(50);
        mgr.close();
        // Second close must not throw or hang.
        mgr.close();
    }

    @Test
    public void testMultipleRingsServedByOneManager() throws Exception {
        long segSize = MmapSegment.HEADER_SIZE
                + 4 * (MmapSegment.FRAME_HEADER_SIZE + 16);
        // Three rings, each with their own subdir.
        String dirA = tmpDir + "/A"; Files.mkdir(dirA, 0755);
        String dirB = tmpDir + "/B"; Files.mkdir(dirB, 0755);
        String dirC = tmpDir + "/C"; Files.mkdir(dirC, 0755);
        SegmentRing ringA = new SegmentRing(MmapSegment.create(dirA + "/0000000000000000.sfa", 0, segSize), segSize);
        SegmentRing ringB = new SegmentRing(MmapSegment.create(dirB + "/0000000000000000.sfa", 0, segSize), segSize);
        SegmentRing ringC = new SegmentRing(MmapSegment.create(dirC + "/0000000000000000.sfa", 0, segSize), segSize);
        try (SegmentManager mgr = new SegmentManager(segSize, 200_000L)) {
            mgr.start();
            mgr.register(ringA, dirA);
            mgr.register(ringB, dirB);
            mgr.register(ringC, dirC);

            assertTrue("ringA spare", waitFor(() -> !ringA.needsHotSpare(), 2000));
            assertTrue("ringB spare", waitFor(() -> !ringB.needsHotSpare(), 2000));
            assertTrue("ringC spare", waitFor(() -> !ringC.needsHotSpare(), 2000));

            // Deregister B. After deregister, B's spare-installation pipeline
            // halts — but B still owns whatever spare the manager already gave it.
            mgr.deregister(ringB);
        } finally {
            ringA.close();
            ringB.close();
            ringC.close();
            Files.remove(dirA);
            Files.remove(dirB);
            Files.remove(dirC);
        }
    }

    private static boolean waitFor(BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return true;
            Thread.sleep(5);
        }
        return cond.getAsBoolean();
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean();
    }
}
