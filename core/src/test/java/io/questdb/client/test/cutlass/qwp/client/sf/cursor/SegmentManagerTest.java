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
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class SegmentManagerTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-segmgr-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
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

    @Test
    public void testAckBatchDoesNotDelaySiblingSpareProvisioning() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int sealedCount = 128;
            long segSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1L;
            long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            CountDownLatch busyDrained = new CountDownLatch(1);
            CountDownLatch siblingInstall = new CountDownLatch(1);
            AtomicInteger trimPass = new AtomicInteger();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            try (SegmentRing busyRing = new SegmentRing(MmapSegment.createInMemory(0, segSize), segSize);
                 SegmentRing siblingRing = new SegmentRing(MmapSegment.createInMemory(0, segSize), segSize);
                 SegmentManager manager = new SegmentManager(segSize)) {
                for (int i = 0; i < sealedCount; i++) {
                    assertEquals(i, busyRing.appendOrFsn(buf, 1));
                    QueueSegmentSource.install(busyRing, MmapSegment.createInMemory(busyRing.nextSeqHint(), segSize));
                }
                assertEquals(sealedCount, busyRing.appendOrFsn(buf, 1));
                QueueSegmentSource.install(busyRing, MmapSegment.createInMemory(busyRing.nextSeqHint(), segSize));
                busyRing.acknowledge(sealedCount - 1L);

                manager.register(busyRing, null);
                manager.register(siblingRing, null);
                manager.setBeforeTrimSyncHook(() -> {
                    int pass = trimPass.incrementAndGet();
                    if (pass == 2) {
                        // The sibling's turn, right after the busy ring's first quantum.
                        try {
                            assertEquals("the busy ring must stop at the 64-segment quantum before sibling service",
                                    64, busyRing.getSealedSegments().size());
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        } finally {
                            siblingInstall.countDown();
                        }
                    }
                    if (pass == 4) {
                        try {
                            assertEquals("the next fair pass must fully drain the busy ring",
                                    0, busyRing.getSealedSegments().size());
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        } finally {
                            busyDrained.countDown();
                        }
                    }
                });
                manager.start();
                assertTrue("manager did not reach the sibling ring",
                        siblingInstall.await(10, TimeUnit.SECONDS));
                assertTrue("manager parked instead of immediately rescheduling the remaining ACK batch",
                        busyDrained.await(10, TimeUnit.SECONDS));
                if (failure.get() != null) {
                    throw new AssertionError("ACK batch fairness witness failed", failure.get());
                }
            } finally {
                Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testProducerCanRotateAcrossManySegmentsWithoutBackpressure() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 32);
            MmapSegment seg0 = MmapSegment.create(tmpDir + "/0000000000000000.sfa", 0, segSize);
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(seg0, segSize);
                 SegmentManager mgr = new SegmentManager(segSize)) {
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
                                    "stuck waiting for a segment at i=" + i);
                        }
                        io.questdb.client.std.Compat.onSpinWait();
                    }
                    assertEquals(i, fsn);
                }
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testManagerTrimsAckedSegmentFiles() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 32);
            String seg0Path = tmpDir + "/0000000000000000.sfa";
            MmapSegment seg0 = MmapSegment.create(seg0Path, 0, segSize);
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try (AckWatermark watermark = AckWatermark.open(tmpDir);
                 SegmentRing ring = new SegmentRing(seg0, segSize);
                 SegmentManager mgr = new SegmentManager(segSize)) {
                assertTrue("watermark must open", watermark != null);
                mgr.start();
                mgr.register(ring, tmpDir, watermark);

                // Fill seg0 (2 frames) and force rotation by appending a third.
                for (int i = 0; i < 2; i++) ring.appendOrFsn(buf, 32);
                ring.appendOrFsn(buf, 32);                 // FSN 2, rotates active to seg1

                assertTrue("seg0 should still exist before ack", Files.exists(seg0Path));

                // ACK every frame in seg0; manager should remove the file.
                ring.acknowledge(1);
                assertTrue("manager should unlink seg0 within 2 seconds",
                        waitFor(() -> !Files.exists(seg0Path), 2000));
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * SegmentManager creates every rotation spare with manifestRequired
     * stamped from birth (unlike the engine's own initial segment, which is
     * created without the flag and stamped separately only after its
     * manifest is durable -- see CursorSendEngine's fresh-start branch). By
     * the time a rotation can happen the manifest this slot needs already
     * exists, so there is no crash window to protect against here; this
     * pins the byte manager actually writes rather than just the argument it
     * passes, the same way testHeaderShapeMatchesTheDocumentedLayout pins
     * MmapSegment's own header bytes rather than restating a constant.
     */
    @Test
    public void testRotationSpareCarriesManifestRequiredFlagOnDisk() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 32);
            MmapSegment seg0 = MmapSegment.create(tmpDir + "/0000000000000000.sfa", 0, segSize);
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(seg0, segSize);
                 SegmentManager mgr = new SegmentManager(segSize)) {
                mgr.start();
                mgr.register(ring, tmpDir);

                // Fill seg0 (2 frames) and force rotation by appending a third.
                for (int i = 0; i < 2; i++) ring.appendOrFsn(buf, 32);
                ring.appendOrFsn(buf, 32); // FSN 2, rotates active to the spare

                MmapSegment spare = ring.getActive();
                assertNotEquals("rotation must have installed a new active segment", seg0, spare);
                byte[] bytes = java.nio.file.Files.readAllBytes(Paths.get(spare.path()));
                assertEquals("spare must carry the current on-disk VERSION",
                        MmapSegment.VERSION, bytes[4]);
                assertEquals("spare must be created with the manifestRequired flag "
                                + "already stamped in its header",
                        MmapSegment.MANIFEST_REQUIRED_FLAG, bytes[5]);
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMaxTotalBytesCapBlocksProvisioningUntilTrimFrees() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 64);
            // Cap = 3 segments total. The ring's initial active counts toward
            // the cap (counted at register-time), so this leaves headroom for
            // exactly 2 rotations before backpressure kicks in.
            long cap = 3 * segSize;
            MmapSegment seg0 = MmapSegment.create(tmpDir + "/0000000000000000.sfa", 0, segSize);
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try (AckWatermark watermark = AckWatermark.open(tmpDir);
                 SegmentRing ring = new SegmentRing(seg0, segSize);
                 SegmentManager mgr = new SegmentManager(segSize, new SegmentBudget(cap))) {
                assertTrue("watermark must open", watermark != null);
                mgr.start();
                // register seeds totalBytes = 1*segSize (initial active).
                mgr.register(ring, tmpDir, watermark);

                // Fill initial (becomes sealed), rotate into segment 2 → 2*segSize.
                ring.appendOrFsn(buf, 64);
                ring.appendOrFsn(buf, 64);
                assertEquals(2L, ring.appendOrFsn(buf, 64)); // rotation 1
                assertEquals(2 * segSize, mgr.getTotalBytesForTesting());
                // Fill segment 2, rotate into segment 3 → 3*segSize. At cap.
                ring.appendOrFsn(buf, 64);
                assertEquals(4L, ring.appendOrFsn(buf, 64)); // rotation 2
                assertEquals(3 * segSize, mgr.getTotalBytesForTesting());
                // Fill segment 3. The next rotation would make 4*segSize > cap:
                // refused, and the producer is backpressured.
                ring.appendOrFsn(buf, 64);
                assertEquals(SegmentRing.BACKPRESSURE_NO_SPARE, ring.appendOrFsn(buf, 64));
                assertEquals(3 * segSize, mgr.getTotalBytesForTesting());

                // ACK the initial segment's frames (FSN 0..1): the manager trims it
                // and the once-stuck producer's rotation now fits under the cap.
                ring.acknowledge(1L);
                assertTrue("the rotation must succeed once trim freed space",
                        waitFor(() -> ring.appendOrFsn(buf, 64) >= 0, 2000));
                assertEquals(3 * segSize, mgr.getTotalBytesForTesting());
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCloseStopsWorkerAndIsIdempotent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SegmentManager mgr = new SegmentManager(8192);
            mgr.start();
            // Give the worker a moment to exist.
            Thread.sleep(50);
            mgr.close();
            // Second close must not throw or hang.
            mgr.close();
        });
    }

    @Test
    public void testMultipleRingsServedByOneManager() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 16);
            // Three rings, each with their own subdir.
            String dirA = tmpDir + "/A"; Files.mkdir(dirA, Files.DIR_MODE_DEFAULT);
            String dirB = tmpDir + "/B"; Files.mkdir(dirB, Files.DIR_MODE_DEFAULT);
            String dirC = tmpDir + "/C"; Files.mkdir(dirC, Files.DIR_MODE_DEFAULT);
            try (
                    SegmentRing ringA = new SegmentRing(MmapSegment.create(dirA + "/0000000000000000.sfa", 0, segSize), segSize);
                    SegmentRing ringB = new SegmentRing(MmapSegment.create(dirB + "/0000000000000000.sfa", 0, segSize), segSize);
                    SegmentRing ringC = new SegmentRing(MmapSegment.create(dirC + "/0000000000000000.sfa", 0, segSize), segSize);
                    SegmentManager mgr = new SegmentManager(segSize)
            ) {
                mgr.start();
                mgr.register(ringA, dirA);
                mgr.register(ringB, dirB);
                mgr.register(ringC, dirC);

                // Deregister B; A and C stay registered until close.
                mgr.deregister(ringB);
            } finally {
                Files.remove(dirA);
                Files.remove(dirB);
                Files.remove(dirC);
            }
        });
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
