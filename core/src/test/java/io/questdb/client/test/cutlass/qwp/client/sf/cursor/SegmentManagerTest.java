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
    public void testManagerProvisionsSpareWithinPollingTick() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testMaxTotalBytesCapBlocksProvisioningUntilTrimFrees() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 64);
            // Cap = 3 segments total. The ring's initial active counts toward
            // the cap (counted at register-time), so this leaves headroom for
            // exactly 2 manager-provisioned spares before backpressure kicks in.
            long cap = 3 * segSize;
            MmapSegment seg0 = MmapSegment.create(tmpDir + "/0000000000000000.sfa", 0, segSize);
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(seg0, segSize);
                 SegmentManager mgr = new SegmentManager(segSize, 200_000L, cap)) {
                mgr.start();
                // register seeds totalBytes = 1*segSize (initial active).
                mgr.register(ring, tmpDir);

                // Manager provisions spare 1 → totalBytes = 2*segSize.
                assertTrue(waitFor(() -> !ring.needsHotSpare(), 2000));
                // Fill initial (becomes sealed), rotate to spare 1.
                ring.appendOrFsn(buf, 64);
                ring.appendOrFsn(buf, 64);
                ring.appendOrFsn(buf, 64); // forces rotation
                // Manager provisions spare 2 → totalBytes = 3*segSize. At cap.
                assertTrue(waitFor(() -> !ring.needsHotSpare(), 2000));
                // Fill spare 1 (becomes sealed), rotate to spare 2.
                ring.appendOrFsn(buf, 64);
                ring.appendOrFsn(buf, 64); // forces rotation again
                // Manager would provision spare 3 → would be 4*segSize > cap. Refused.
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
        });
    }

    @Test
    public void testFirstSpareLandsBeforeFirstPoll() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // pollNanos is intentionally long enough that the 5s park can be
            // ruled out as the mechanism by which the first spare arrives.
            // register() unparks the worker after publishing the new ring,
            // so the worker re-iterates and provisions the spare even when
            // its first loop snapshot ran before register() acquired `lock`.
            // The spare must therefore land within seconds of register(),
            // not minutes -- the 5s park is never reached.
            //
            // The append below is incidental to the contract under test; it
            // does NOT cross the SegmentRing high-water mark for this 4-frame
            // segment (HEADER_SIZE 24 + FRAME_HEADER_SIZE 8 + 16 = 48 vs
            // signalAtBytes = (120 >> 2) * 3 = 90), so no producer-side wakeup
            // fires. The rotation/high-water wakeup paths are covered by
            // testRotationWakeupTriggersImmediateSparePrep, and the
            // deterministic register-after-park case is covered by
            // testRegisterAfterWorkerParkedWakesWorker.
            long pollNanos = 5_000_000_000L; // 5 seconds
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 16);
            MmapSegment seg0 = MmapSegment.create(tmpDir + "/0000000000000000.sfa", 0, segSize);
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(seg0, segSize);
                 SegmentManager mgr = new SegmentManager(segSize, pollNanos)) {
                mgr.start();
                mgr.register(ring, tmpDir);
                long t0 = System.nanoTime();
                ring.appendOrFsn(buf, 16);
                // 2s is the budget used across the other manager tests; it
                // tolerates the slower file-open + truncate + mmap that runs
                // under JaCoCo coverage instrumentation while still being
                // orders of magnitude below the 5s poll interval.
                assertTrue("first spare must land without waiting for the 5s poll tick",
                        waitFor(() -> !ring.needsHotSpare(), 2000));
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
                assertTrue("spare arrived in " + elapsedMs + "ms -- should be <<5000ms",
                        elapsedMs < 4000);
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRegisterAfterWorkerParkedWakesWorker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Deterministic version of testFirstSpareLandsBeforeFirstPoll:
            // sleep between start() and register() long enough for the worker
            // to definitely complete its first (empty) iteration and enter
            // parkNanos. Without register()'s wakeWorker() the spare would
            // not land for the full 5s poll interval; with it the spare lands
            // promptly because register() unparks the worker out of its park.
            // No append at all, so no producer-side wakeup can mask a missing
            // register-side wakeup.
            long pollNanos = 5_000_000_000L; // 5 seconds
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 16);
            MmapSegment seg0 = MmapSegment.create(tmpDir + "/0000000000000000.sfa", 0, segSize);
            try (SegmentRing ring = new SegmentRing(seg0, segSize);
                 SegmentManager mgr = new SegmentManager(segSize, pollNanos)) {
                mgr.start();
                // Give the worker plenty of time to enter workerLoop, snapshot
                // an empty rings list, and reach parkNanos. 250ms is far more
                // than the OS scheduling + thread startup cost on any sane
                // CI runner, and still well below the 5s poll interval.
                Thread.sleep(250);
                long t0 = System.nanoTime();
                mgr.register(ring, tmpDir);
                assertTrue("register must wake a worker that has already parked",
                        waitFor(() -> !ring.needsHotSpare(), 2000));
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000L;
                assertTrue("spare arrived in " + elapsedMs + "ms -- should be <<5000ms",
                        elapsedMs < 4000);
            }
        });
    }

    @Test
    public void testRotationWakeupTriggersImmediateSparePrep() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Segment small enough that one frame fills it; verifies that the
            // post-rotation wakeup runs before the next 5s poll.
            long pollNanos = 5_000_000_000L;
            long segSize = MmapSegment.HEADER_SIZE
                    + (MmapSegment.FRAME_HEADER_SIZE + 16);
            MmapSegment seg0 = MmapSegment.create(tmpDir + "/0000000000000000.sfa", 0, segSize);
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try (SegmentRing ring = new SegmentRing(seg0, segSize);
                 SegmentManager mgr = new SegmentManager(segSize, pollNanos)) {
                mgr.start();
                mgr.register(ring, tmpDir);
                // First spare via high-water signal on the very first append.
                // 2000ms budget: same wakeup-vs-poll rationale as
                // testProducerWakeupBeatsThePollInterval -- still 2.5x below
                // the 5s poll, but tolerant of CI scheduling jitter.
                ring.appendOrFsn(buf, 16);
                assertTrue(waitFor(() -> !ring.needsHotSpare(), 2_000));
                // Now active is full → next append rotates → consumes the spare →
                // hotSpare goes back to null → rotation-time wakeup runs →
                // manager promptly provisions the next spare.
                long beforeRotate = System.nanoTime();
                long fsn = ring.appendOrFsn(buf, 16);
                assertEquals(1, fsn);
                assertTrue("rotation-time wakeup must trigger spare 2 well before 5s poll",
                        waitFor(() -> !ring.needsHotSpare(), 2_000));
                long elapsedMs = (System.nanoTime() - beforeRotate) / 1_000_000L;
                assertTrue("spare 2 arrived in " + elapsedMs + "ms — should be <<5000ms",
                        elapsedMs < 4_000);
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCloseStopsWorkerAndIsIdempotent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            SegmentManager mgr = new SegmentManager(8192, 200_000L);
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
                    SegmentManager mgr = new SegmentManager(segSize, 200_000L)
            ) {
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
