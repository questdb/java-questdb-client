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
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegmentException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SfSanitizedResidueException;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Misc;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SegmentRingTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-ring-" + System.nanoTime()).toString();
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
    public void testAppendAssignsMonotonicFsnsAndPublishesThem() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                MmapSegment seg = MmapSegment.create(tmpDir + "/0.sfa", 0, 64 * 1024);
                try (SegmentRing ring = new SegmentRing(seg, 64 * 1024)) {
                    assertEquals(0, ring.nextSeqHint());
                    assertEquals(-1, ring.publishedFsn());
                    fillPattern(buf, 32, 1);
                    long fsn0 = ring.appendOrFsn(buf, 32);
                    assertEquals(0, fsn0);
                    assertEquals(0, ring.publishedFsn());
                    long fsn1 = ring.appendOrFsn(buf, 32);
                    assertEquals(1, fsn1);
                    assertEquals(1, ring.publishedFsn());
                }
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRotationConsumesHotSpare() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Sized so exactly two 100-byte payloads fit, forcing rotation on the third.
            long segSize = MmapSegment.HEADER_SIZE
                    + 2 * (MmapSegment.FRAME_HEADER_SIZE + 100);
            long buf = Unsafe.malloc(100, MemoryTag.NATIVE_DEFAULT);
            try {
                MmapSegment seg0 = MmapSegment.create(tmpDir + "/seg0.sfa", 0, segSize);
                try (SegmentRing ring = new SegmentRing(seg0, segSize)) {
                    fillPattern(buf, 100, 0);
                    assertEquals(0, ring.appendOrFsn(buf, 100));
                    assertEquals(1, ring.appendOrFsn(buf, 100));
                    // Active is now full. Without a spare, append must report backpressure.
                    assertEquals(SegmentRing.BACKPRESSURE_NO_SPARE,
                            ring.appendOrFsn(buf, 100));
                    assertTrue("ring should be asking for a spare", ring.needsHotSpare());

                    // Manager installs a fresh spare with the right baseSeq.
                    MmapSegment spare = MmapSegment.create(tmpDir + "/seg1.sfa",
                            ring.nextSeqHint(), segSize);
                    ring.installHotSpare(spare);

                    // Now the same append succeeds, and FSN keeps incrementing across
                    // segment boundaries (no reset to 0 in the new segment).
                    // Two prior successful appends were 0 and 1; the failed append
                    // didn't burn an FSN, so this one is FSN 2.
                    assertEquals(2, ring.appendOrFsn(buf, 100));
                    assertEquals(2, ring.publishedFsn());
                    // After the rotation succeeded, ring should ask for the next spare.
                    assertTrue(ring.needsHotSpare());
                }
            } finally {
                Unsafe.free(buf, 100, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRotationRebasesSpareToCorrectFsnRegardlessOfManagerGuess() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The segment manager's pre-creation baseSeq is provisional — the ring
            // pins the real value via MmapSegment.rebaseSeq() at rotation time.
            // Verify that even if the spare comes in with a wildly wrong baseSeq,
            // rotation succeeds and the resulting FSN sequence is contiguous.
            long segSize = MmapSegment.HEADER_SIZE
                    + (MmapSegment.FRAME_HEADER_SIZE + 64);
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                MmapSegment seg0 = MmapSegment.create(tmpDir + "/wseg0.sfa", 0, segSize);
                try (SegmentRing ring = new SegmentRing(seg0, segSize)) {
                    fillPattern(buf, 64, 0);
                    assertEquals(0, ring.appendOrFsn(buf, 64));    // active full
                    // Manager guessed baseSeq=999 long before the active filled.
                    MmapSegment lateSpare = MmapSegment.create(tmpDir + "/lateseg.sfa", 999, segSize);
                    ring.installHotSpare(lateSpare);
                    // Rotation must rebase the spare to baseSeq=1 (the actual nextSeq).
                    assertEquals(1, ring.appendOrFsn(buf, 64));
                    assertEquals(1, ring.publishedFsn());
                    assertEquals(1, lateSpare.baseSeq());
                }
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRotationRearmsHighWaterBackupWakeup() throws Exception {
        // The producer-thread flag that gates the high-water backup wakeup is
        // per-active. Rotation must reset it so the new active can fire the
        // backup again if the rotation-time wakeup didn't get a fresh spare
        // installed in time. Pre-fix the flag stayed sticky-true after the
        // first set, so on every active past the first the backup branch was
        // a dead path.
        TestUtils.assertMemoryLeak(() -> {
            // 4 100-byte frames per segment. signalAtBytes is 75% of segSize,
            // and HEADER (24) + 3 frames (3*108 = 324) lands publishedOffset
            // at 348, just past the 342-byte threshold.
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 100);
            long buf = Unsafe.malloc(100, MemoryTag.NATIVE_DEFAULT);
            try {
                MmapSegment seg0 = MmapSegment.create(tmpDir + "/wseg0.sfa", 0, segSize);
                try (SegmentRing ring = new SegmentRing(seg0, segSize)) {
                    int[] wakeups = {0};
                    ring.setManagerWakeup(() -> wakeups[0]++);
                    fillPattern(buf, 100, 0);

                    // First active: two frames stay below high-water.
                    assertEquals(0, ring.appendOrFsn(buf, 100));
                    assertEquals(1, ring.appendOrFsn(buf, 100));
                    assertEquals("no wakeup before high-water", 0, wakeups[0]);
                    // Third frame crosses 75%: backup branch fires once.
                    assertEquals(2, ring.appendOrFsn(buf, 100));
                    assertEquals("backup signal fires on high-water crossing", 1, wakeups[0]);
                    // Fourth frame fills the active. Still same active, so the
                    // backup branch must coalesce and not fire again.
                    assertEquals(3, ring.appendOrFsn(buf, 100));
                    assertEquals("backup signal coalesces within an active", 1, wakeups[0]);
                    // Active is full and there is still no spare.
                    assertEquals(SegmentRing.BACKPRESSURE_NO_SPARE,
                            ring.appendOrFsn(buf, 100));
                    assertEquals("backpressure does not fire wakeup", 1, wakeups[0]);

                    // Install spare, then retry. The retry triggers rotation,
                    // which fires the wakeup unconditionally.
                    ring.installHotSpare(MmapSegment.create(
                            tmpDir + "/wseg1.sfa", ring.nextSeqHint(), segSize));
                    assertEquals(4, ring.appendOrFsn(buf, 100));
                    assertEquals("rotation fires wakeup", 2, wakeups[0]);

                    // New active. Two frames keep publishedOffset below the
                    // high-water mark (the rotated frame counts as the first).
                    assertEquals(5, ring.appendOrFsn(buf, 100));
                    assertEquals(2, wakeups[0]);
                    // Third frame on the new active crosses 75% again. Without
                    // rotation re-arming the per-active flag, this assertion
                    // catches the regression: pre-fix wakeups stayed at 2.
                    assertEquals(6, ring.appendOrFsn(buf, 100));
                    assertEquals("backup signal re-arms on the new active", 3, wakeups[0]);
                }
            } finally {
                Unsafe.free(buf, 100, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testAcknowledgeAndDrainTrimsOldestFirstUntilUnackedFound() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Three small segments worth of frames; ack progressively, drain.
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 16);
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                MmapSegment seg0 = MmapSegment.create(tmpDir + "/t0.sfa", 0, segSize);
                try (SegmentRing ring = new SegmentRing(seg0, segSize)) {
                    fillPattern(buf, 16, 0);
                    // Fill seg0 (FSN 0..3).
                    for (int i = 0; i < 4; i++) ring.appendOrFsn(buf, 16);
                    // Spare for seg1 (FSN 4..7).
                    ring.installHotSpare(MmapSegment.create(tmpDir + "/t1.sfa", 4, segSize));
                    for (int i = 0; i < 4; i++) ring.appendOrFsn(buf, 16);
                    // Spare for seg2 (FSN 8..11).
                    ring.installHotSpare(MmapSegment.create(tmpDir + "/t2.sfa", 8, segSize));
                    for (int i = 0; i < 4; i++) ring.appendOrFsn(buf, 16);

                    // No acks yet — nothing to trim.
                    assertNull(ring.drainTrimmable());

                    // ACK halfway into seg0 — still not enough to trim it (need
                    // every frame in the segment to be acked).
                    ring.acknowledge(2);
                    assertNull(ring.drainTrimmable());

                    // ACK exactly the last frame of seg0 — now it can be trimmed.
                    ring.acknowledge(3);
                    ObjList<MmapSegment> drained = ring.drainTrimmable();
                    assertNotNull(drained);
                    assertEquals(1, drained.size());
                    assertEquals(0, drained.get(0).baseSeq());
                    drained.get(0).close();

                    // ACK a value spanning seg1 and into seg2 — only seg1 is fully
                    // acked; seg2 has unacked frames so trim must stop after seg1.
                    ring.acknowledge(9);
                    drained = ring.drainTrimmable();
                    assertNotNull(drained);
                    assertEquals(1, drained.size());
                    assertEquals(4, drained.get(0).baseSeq());
                    drained.get(0).close();

                    // No further trimmable segments.
                    assertNull(ring.drainTrimmable());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOpenExistingReturnsNullOnEmptyDir() throws Exception {
        TestUtils.assertMemoryLeak(() -> assertNull("nothing in dir → null ring", SegmentRing.openExisting(tmpDir, 8192)));
    }

    @Test
    public void testOpenExistingRecoversActivePlusSealed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 16);
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                // Write three segments with FSN ranges 0..3, 4..7, 8..9 (last
                // partially full so the recovered ring has appendable room).
                MmapSegment s0 = MmapSegment.create(tmpDir + "/r0.sfa", 0, segSize);
                for (int i = 0; i < 4; i++) s0.tryAppend(buf, 16);
                s0.close();

                MmapSegment s1 = MmapSegment.create(tmpDir + "/r1.sfa", 4, segSize);
                for (int i = 0; i < 4; i++) s1.tryAppend(buf, 16);
                s1.close();

                MmapSegment s2 = MmapSegment.create(tmpDir + "/r2.sfa", 8, segSize);
                s2.tryAppend(buf, 16);
                s2.tryAppend(buf, 16);
                s2.close();

                try (SegmentRing recovered = SegmentRing.openExisting(tmpDir, segSize)) {
                    assertNotNull(recovered);
                    // Active is the highest-baseSeq segment (s2) with 2 frames.
                    assertEquals(8, recovered.getActive().baseSeq());
                    assertEquals(2, recovered.getActive().frameCount());
                    // Two sealed segments, oldest first.
                    assertEquals(2, recovered.getSealedSegments().size());
                    assertEquals(0, recovered.getSealedSegments().get(0).baseSeq());
                    assertEquals(4, recovered.getSealedSegments().get(1).baseSeq());
                    // nextSeq must continue past the recovered frames.
                    assertEquals(10, recovered.nextSeqHint());
                    // Further appends land into the active and assign FSN 10.
                    assertEquals(10, recovered.appendOrFsn(buf, 16));
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testResealedTornRecoveredSegmentDoesNotBrickNextRecovery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // End-to-end two-crash regression:
            //   crash #1 -> torn active with residue up to the file end;
            //   recovery #1 -> resume in it, fill it, rotate (reseals the
            //   recovered file with a tail gap the resumed appends never
            //   overwrote);
            //   crash #2 -> recovery #2 used to throw "corrupt torn tail in
            //   sealed SF segment" on EVERY startup, because nothing ever
            //   sanitized crash #1's residue and the sealed-suffix check
            //   correctly refuses non-zero sealed tails. Recovery #1 now
            //   sanitizes the segment it resumes as active before any
            //   append, so recovery #2 must succeed and see the full chain.
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 16)
                    + 12; // reseal gap: a 5th 24-byte frame can never fit
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                // Session 1 "crashes": two good frames, then garbage from the
                // torn write all the way to the file end.
                try (MmapSegment s0 = MmapSegment.create(tmpDir + "/r0.sfa", 0, segSize)) {
                    s0.tryAppend(buf, 16);
                    s0.tryAppend(buf, 16);
                    long addr = s0.address();
                    for (long off = s0.publishedOffset(); off + 4 <= segSize; off += 4) {
                        Unsafe.getUnsafe().putInt(addr + off, 0xCAFEBABE);
                    }
                    s0.msync();
                }
                // Recovery #1 + session 2: resume in the torn segment, fill
                // it, rotate into a spare. The recovered file is resealed
                // with its 12-byte gap intact.
                try (SegmentRing ring = SegmentRing.openExisting(tmpDir, segSize)) {
                    assertNotNull(ring);
                    assertEquals(2, ring.nextSeqHint());
                    assertEquals(2, ring.appendOrFsn(buf, 16));
                    assertEquals(3, ring.appendOrFsn(buf, 16));
                    ring.installHotSpare(MmapSegment.create(tmpDir + "/r1.sfa", 4, segSize));
                    assertEquals("append must rotate into the spare",
                            4, ring.appendOrFsn(buf, 16));
                    assertEquals(1, ring.getSealedSegments().size());
                }
                // Recovery #2 ("crash" #2): must not brick on the resealed
                // recovered segment.
                try (SegmentRing ring = SegmentRing.openExisting(tmpDir, segSize)) {
                    assertNotNull("second recovery must survive the resealed segment", ring);
                    assertEquals(1, ring.getSealedSegments().size());
                    assertEquals(0, ring.getSealedSegments().get(0).baseSeq());
                    assertEquals(4, ring.getSealedSegments().get(0).frameCount());
                    assertEquals(4, ring.getActive().baseSeq());
                    assertEquals(1, ring.getActive().frameCount());
                    assertEquals(5, ring.nextSeqHint());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMidFileTearInSealedMemberFailsClosedWithoutMutation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Bit-rot mid-file in a SEALED chain member: the scan stops at
            // the first bad CRC, so frames past the tear -- individually
            // valid CRCs, real unacked payloads, the ONLY surviving copy --
            // are classified as torn-tail residue. Recovery must fail closed
            // on the FSN gap the lost frames create BEFORE any sanitization
            // decision, leaving every byte on disk for operator extraction,
            // and must keep doing so identically on retry.
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 16);
            String m0Path = tmpDir + "/m0.sfa";
            String m1Path = tmpDir + "/m1.sfa";
            String m2Path = tmpDir + "/m2.sfa";
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                fillPattern(buf, 16, 3);
                MmapSegment s0 = MmapSegment.create(m0Path, 0, segSize);
                for (int i = 0; i < 4; i++) s0.tryAppend(buf, 16);
                s0.close();
                MmapSegment s1 = MmapSegment.create(m1Path, 4, segSize);
                for (int i = 0; i < 4; i++) s1.tryAppend(buf, 16);
                s1.close();
                MmapSegment s2 = MmapSegment.create(m2Path, 8, segSize);
                s2.tryAppend(buf, 16);
                s2.close();
                // First recovery migrates the legacy chain, creates the
                // manifest and validates clean: sealed m0+m1, active m2.
                try (SegmentRing ring = SegmentRing.openExisting(tmpDir, segSize)) {
                    assertNotNull(ring);
                    assertEquals(2, ring.getSealedSegments().size());
                }
                // Bit-rot strikes sealed m0 mid-file: clobber the CRC of its
                // SECOND frame. Frames 2..3 keep valid CRCs but are
                // unreachable to the sequential scan.
                long frame1Offset = MmapSegment.HEADER_SIZE
                        + MmapSegment.FRAME_HEADER_SIZE + 16;
                int fd = Files.openRW(m0Path);
                assertTrue("openRW must succeed", fd >= 0);
                long bad = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putInt(bad, 0xDEADBEEF);
                    assertEquals(4L, Files.write(fd, bad, 4, frame1Offset));
                    Files.fsync(fd);
                } finally {
                    Unsafe.free(bad, 4, MemoryTag.NATIVE_DEFAULT);
                    Files.close(fd);
                }
                byte[] m0Before = readFileBytes(m0Path);
                byte[] m1Before = readFileBytes(m1Path);
                byte[] m2Before = readFileBytes(m2Path);
                for (int attempt = 0; attempt < 2; attempt++) {
                    try {
                        Misc.free(SegmentRing.openExisting(tmpDir, segSize));
                        throw new AssertionError(
                                "mid-file tear in a sealed member must fail recovery");
                    } catch (MmapSegmentException expected) {
                        assertTrue(expected.getMessage(),
                                expected.getMessage().contains("FSN gap"));
                    }
                    assertArrayEquals("attempt #" + attempt
                                    + ": failed recovery must not mutate the torn sealed member",
                            m0Before, readFileBytes(m0Path));
                    assertArrayEquals(m1Before, readFileBytes(m1Path));
                    assertArrayEquals(m2Before, readFileBytes(m2Path));
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testProvenDeadSealedResidueSanitizedThenHealsAfterOneRestart() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Legacy pre-sanitization poison: a sealed member whose frame
            // accounting is COMPLETE (contiguity + boundaries prove no frame
            // lost) but whose suffix gap carries residue from a pre-fix
            // client. The residue is provably dead bytes, so recovery zeroes
            // it durably, still fails closed on first sight to surface the
            // incident, and the restart proves the chain clean.
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 16)
                    + 12; // sealed suffix gap that can never fit a frame
            String m0Path = tmpDir + "/p0.sfa";
            String m1Path = tmpDir + "/p1.sfa";
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                fillPattern(buf, 16, 5);
                MmapSegment s0 = MmapSegment.create(m0Path, 0, segSize);
                for (int i = 0; i < 4; i++) s0.tryAppend(buf, 16);
                long gapStart = s0.publishedOffset();
                s0.close();
                MmapSegment s1 = MmapSegment.create(m1Path, 4, segSize);
                s1.tryAppend(buf, 16);
                s1.close();
                // First recovery creates the manifest over the clean chain.
                try (SegmentRing ring = SegmentRing.openExisting(tmpDir, segSize)) {
                    assertNotNull(ring);
                    assertEquals(1, ring.getSealedSegments().size());
                }
                // Poison the sealed suffix gap the way a pre-fix client's
                // reseal-after-recovery did.
                int fd = Files.openRW(m0Path);
                assertTrue("openRW must succeed", fd >= 0);
                long junk = Unsafe.malloc(12, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < 3; i++) {
                        Unsafe.getUnsafe().putInt(junk + i * 4L, 0xCAFEBABE);
                    }
                    assertEquals(12L, Files.write(fd, junk, 12, gapStart));
                    Files.fsync(fd);
                } finally {
                    Unsafe.free(junk, 12, MemoryTag.NATIVE_DEFAULT);
                    Files.close(fd);
                }
                // Recovery #1: proven-dead residue is sanitized, incident
                // still fails closed on first sight.
                try {
                    Misc.free(SegmentRing.openExisting(tmpDir, segSize));
                    throw new AssertionError("poisoned sealed suffix must fail closed on first sight");
                } catch (MmapSegmentException expected) {
                    assertTrue("first-sight throw must be the healed-residue refinement so "
                                    + "unattended callers can retry: " + expected.getClass().getName(),
                            expected instanceof SfSanitizedResidueException);
                    assertTrue(expected.getMessage(),
                            expected.getMessage().contains("corrupt torn tail in sealed SF segment"));
                }
                // The heal: residue durably zeroed, all frames intact.
                try (MmapSegment seg = MmapSegment.openExisting(m0Path)) {
                    assertEquals("frames must be untouched", 4L, seg.frameCount());
                    assertEquals("proven-dead residue must be zeroed on disk",
                            0L, seg.tornTailBytes());
                }
                // Recovery #2: the restart proves the chain clean.
                try (SegmentRing ring = SegmentRing.openExisting(tmpDir, segSize)) {
                    assertNotNull("restart after the sanitizing fail-closed throw must succeed", ring);
                    assertEquals(1, ring.getSealedSegments().size());
                    assertEquals(4, ring.getSealedSegments().get(0).frameCount());
                    assertEquals(4, ring.getActive().baseSeq());
                    assertEquals(1, ring.getActive().frameCount());
                    assertEquals(5, ring.nextSeqHint());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * The legacy-migration twin of
     * {@link #testProvenDeadSealedResidueSanitizedThenHealsAfterOneRestart}:
     * the SAME proven-dead sealed residue, but already on disk BEFORE the
     * first recovery ever runs — a pre-manifest slot written by a pre-fix
     * client. Legacy slots predate the fail-closed sealed-suffix contract,
     * so migration must zero the residue SILENTLY (no first-sight throw)
     * and hand the manifest era a chain that already satisfies the
     * all-zero sealed-suffix invariant — the next restart comes up clean
     * instead of tripping a spurious {@link SfSanitizedResidueException}
     * over bytes migration should have healed.
     */
    @Test
    public void testLegacyMigrationSilentlySanitizesProvenDeadSealedResidue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 16)
                    + 12; // sealed suffix gap that can never fit a frame
            String m0Path = tmpDir + "/q0.sfa";
            String m1Path = tmpDir + "/q1.sfa";
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                fillPattern(buf, 16, 6);
                MmapSegment s0 = MmapSegment.create(m0Path, 0, segSize);
                for (int i = 0; i < 4; i++) s0.tryAppend(buf, 16);
                long gapStart = s0.publishedOffset();
                s0.close();
                MmapSegment s1 = MmapSegment.create(m1Path, 4, segSize);
                s1.tryAppend(buf, 16);
                s1.close();
                // Poison the sealed suffix gap BEFORE any recovery: no
                // manifest exists yet, so this is a legacy slot carrying
                // pre-fix-client residue into migration.
                int fd = Files.openRW(m0Path);
                assertTrue("openRW must succeed", fd >= 0);
                long junk = Unsafe.malloc(12, MemoryTag.NATIVE_DEFAULT);
                try {
                    for (int i = 0; i < 3; i++) {
                        Unsafe.getUnsafe().putInt(junk + i * 4L, 0xCAFEBABE);
                    }
                    assertEquals(12L, Files.write(fd, junk, 12, gapStart));
                    Files.fsync(fd);
                } finally {
                    Unsafe.free(junk, 12, MemoryTag.NATIVE_DEFAULT);
                    Files.close(fd);
                }
                // Migration: contiguity proves the residue dead, so the
                // legacy chain recovers in ONE pass — silently.
                try (SegmentRing ring = SegmentRing.openExisting(tmpDir, segSize)) {
                    assertNotNull("legacy migration must not fail closed over "
                            + "proven-dead sealed residue", ring);
                    assertEquals(1, ring.getSealedSegments().size());
                    assertEquals(4, ring.getSealedSegments().get(0).frameCount());
                    assertEquals(4, ring.getActive().baseSeq());
                }
                // The silent heal: residue durably zeroed, frames intact,
                // slot migrated to the manifest era.
                try (MmapSegment seg = MmapSegment.openExisting(m0Path)) {
                    assertEquals("frames must be untouched", 4L, seg.frameCount());
                    assertEquals("legacy migration must durably zero the dead residue",
                            0L, seg.tornTailBytes());
                }
                assertTrue("legacy migration must create the manifest",
                        Files.exists(tmpDir + "/sf-manifest.bin"));
                // The manifest-era invariant holds: the restart's fail-closed
                // sealed-suffix pass finds nothing to surface. A
                // SfSanitizedResidueException here means migration skipped
                // the sanitize and deferred the incident to production's
                // next restart.
                try (SegmentRing ring = SegmentRing.openExisting(tmpDir, segSize)) {
                    assertNotNull("restart over the migrated chain must come up clean", ring);
                    assertEquals(1, ring.getSealedSegments().size());
                    assertEquals(5, ring.nextSeqHint());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRingRecoverySanitizesResumedActiveTornTail() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The funnel guarantee: whatever segment recovery selects as the
            // resumed active has its torn residue durably zeroed before the
            // ring is exposed -- even if the session then crashes without a
            // single append, the reseal/resurrection hazards are gone.
            String path = tmpDir + "/a0.sfa";
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                fillPattern(buf, 16, 9);
                try (MmapSegment s0 = MmapSegment.create(path, 0, 4096)) {
                    s0.tryAppend(buf, 16);
                    s0.tryAppend(buf, 16);
                    long addr = s0.address();
                    for (long off = s0.publishedOffset(); off + 4 <= 4096; off += 4) {
                        Unsafe.getUnsafe().putInt(addr + off, 0xCAFEBABE);
                    }
                    s0.msync();
                }
                try (SegmentRing ring = SegmentRing.openExisting(tmpDir, 4096)) {
                    assertNotNull(ring);
                    assertEquals("the observation must survive for diagnostics",
                            true, ring.getActive().tornTailBytes() > 0);
                }
                // No appends happened; the zeroing must already be durable.
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals(2L, seg.frameCount());
                    assertEquals("ring recovery must have sanitized the resumed active",
                            0L, seg.tornTailBytes());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOpenExistingDetectsFsnGap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 16);
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                MmapSegment s0 = MmapSegment.create(tmpDir + "/g0.sfa", 0, segSize);
                for (int i = 0; i < 4; i++) s0.tryAppend(buf, 16);
                s0.close();

                // Gap: should be baseSeq=4 next, but we use 100 — simulating
                // a segment file that was deleted out from under us.
                MmapSegment s2 = MmapSegment.create(tmpDir + "/g2.sfa", 100, segSize);
                s2.tryAppend(buf, 16);
                s2.close();

                try {
                    Misc.free(SegmentRing.openExisting(tmpDir, segSize));
                    throw new AssertionError("expected FSN gap to be detected");
                } catch (MmapSegmentException expected) {
                    assertTrue(expected.getMessage(),
                            expected.getMessage().contains("FSN gap"));
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOpenExistingSkipsBadMagicFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + (MmapSegment.FRAME_HEADER_SIZE + 16);
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                // One good segment.
                MmapSegment s0 = MmapSegment.create(tmpDir + "/good.sfa", 0, segSize);
                s0.tryAppend(buf, 16);
                s0.close();
                // One stray .sfa with no proper header — must be ignored.
                int fd = Files.openCleanRW(tmpDir + "/stray.sfa");
                long hdr = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putLong(hdr, 0xBADBADBADBADBADBL);
                    Files.write(fd, hdr, 8, 0);
                    Files.fsync(fd);
                } finally {
                    Files.close(fd);
                    Unsafe.free(hdr, 8, MemoryTag.NATIVE_DEFAULT);
                }

                try (SegmentRing recovered = SegmentRing.openExisting(tmpDir, segSize)) {
                    assertNotNull(recovered);
                    assertEquals(0, recovered.getActive().baseSeq());
                    assertEquals(0, recovered.getSealedSegments().size());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testAcknowledgeIsMonotonic() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Contract: acknowledge() advances ackedFsn but never lets it
            // regress AND never lets it run past publishedFsn (defense-in-
            // depth against malformed server NACKs). To exercise the
            // monotonicity logic we must first publish enough frames to
            // give the cursor headroom; otherwise every ack would be
            // clamped to -1 (nothing published) and the monotonicity check
            // would test the clamp instead of the regression rule.
            long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                MmapSegment seg = MmapSegment.create(tmpDir + "/m.sfa", 0, 8192);
                try (SegmentRing ring = new SegmentRing(seg, 8192)) {
                    // Publish 201 frames so FSNs 0..200 exist on the ring.
                    for (int i = 0; i <= 200; i++) {
                        Unsafe.getUnsafe().putLong(buf, i);
                        long fsn = ring.appendOrFsn(buf, 8);
                        assertEquals(i, fsn);
                    }
                    assertEquals(200L, ring.publishedFsn());

                    ring.acknowledge(100);
                    assertEquals(100, ring.ackedFsn());
                    ring.acknowledge(50);   // regression — ignored
                    assertEquals(100, ring.ackedFsn());
                    ring.acknowledge(200);
                    assertEquals(200, ring.ackedFsn());
                }
            } finally {
                Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testNextSealedAfterWalksThousandsOfSegmentsInLinearOperations() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Regression for both "sealed snapshot grew unexpectedly large"
            // and a later quadratic list traversal at every segment boundary.
            // The cursor I/O loop used to copy the entire sealed list into a
            // fixed-size array (initial 16, grown once to 32) on every advance.
            // Under load — producer outpacing the WS sender, no maxTotalBytes
            // cap — sealed segments accumulate well past 32 and the I/O thread
            // would crash. Walk via nextSealedAfter must work no matter how
            // many sealed segments are in the list.
            final int sealedCount = 200; // comfortably exceeds the old 32-slot cap
            // One frame per segment keeps the test fast; rotation forces seal.
            long segSize = MmapSegment.HEADER_SIZE
                    + (MmapSegment.FRAME_HEADER_SIZE + 16);
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                MmapSegment seg0 = MmapSegment.create(tmpDir + "/seg-0000.sfa", 0, segSize);
                try (SegmentRing ring = new SegmentRing(seg0, segSize)) {
                    fillPattern(buf, 16, 0);
                    // (sealedCount + 1) iterations puts exactly sealedCount segments
                    // into the sealed list: the first iteration just fills the
                    // initial active (no rotation yet); iterations 2..N each rotate
                    // the previous active onto the sealed list before appending.
                    for (int i = 0; i <= sealedCount; i++) {
                        long fsn = ring.appendOrFsn(buf, 16);
                        assertEquals("first append after rotation produces fsn=" + i, i, fsn);
                        // Active is now full; install a spare so the next append rotates.
                        MmapSegment spare = MmapSegment.create(
                                tmpDir + "/seg-" + String.format("%04d", i + 1) + ".sfa",
                                ring.nextSeqHint(), segSize);
                        ring.installHotSpare(spare);
                    }
                    // After the loop we have `sealedCount` sealed segments and one
                    // active (containing nothing yet — its base = sealedCount).
                    // Now walk: oldest sealed, then nextSealedAfter() repeatedly.
                    SegmentRing.resetNextSealedComparisons();
                    MmapSegment cursor = ring.firstSealed();
                    assertNotNull(cursor);
                    for (int i = 1; i < sealedCount / 2; i++) {
                        cursor = ring.nextSealedAfter(cursor);
                        assertNotNull(cursor);
                    }
                    long halfWalkOperations = SegmentRing.getNextSealedComparisons();

                    SegmentRing.resetNextSealedComparisons();
                    cursor = ring.firstSealed();
                    assertNotNull(cursor);
                    assertEquals(0, cursor.baseSeq());
                    int visited = 1;
                    long prevBase = cursor.baseSeq();
                    while (true) {
                        MmapSegment next = ring.nextSealedAfter(cursor);
                        if (next == null) break;
                        assertTrue("baseSeq must strictly increase: prev=" + prevBase
                                        + " next=" + next.baseSeq(),
                                next.baseSeq() > prevBase);
                        prevBase = next.baseSeq();
                        cursor = next;
                        visited++;
                    }
                    assertEquals("must visit every sealed segment", sealedCount, visited);
                    // The loop terminated when walking past the last sealed returned null.
                    long comparisons = SegmentRing.getNextSealedComparisons();
                    assertTrue("walk inspected " + comparisons + " entries for " + sealedCount
                                    + " segments; successor traversal must remain O(N)",
                            comparisons <= 2L * sealedCount);
                    assertTrue("doubling the walk grew operations from " + halfWalkOperations
                                    + " to " + comparisons + "; expected linear scaling",
                            comparisons <= 2L * halfWalkOperations + 2L);
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testNextSealedAfterStillReturnsCorrectlyWhenCursorWasTrimmed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Bug class: I/O thread is mid-walk; trim removes the segment
            // referenced by `cursor` between iterations. The next call must
            // return the segment whose baseSeq is just above cursor.baseSeq()
            // — not crash, not skip ahead, not loop forever. baseSeq comparison
            // (rather than identity) is what makes this safe.
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 16);
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                MmapSegment seg0 = MmapSegment.create(tmpDir + "/t-0.sfa", 0, segSize);
                try (SegmentRing ring = new SegmentRing(seg0, segSize)) {
                    fillPattern(buf, 16, 0);
                    // Build sealed: [seg0, seg1, seg2, seg3]; active = seg4.
                    for (int i = 0; i < 4; i++) {
                        ring.appendOrFsn(buf, 16);
                        ring.installHotSpare(MmapSegment.create(
                                tmpDir + "/t-" + (i + 1) + ".sfa", ring.nextSeqHint(), segSize));
                    }
                    MmapSegment seg0Snapshot = ring.firstSealed();
                    assertNotNull(seg0Snapshot);
                    assertEquals(0, seg0Snapshot.baseSeq());
                    // Simulate trim: ack everything in seg0 and seg1, drain.
                    ring.acknowledge(1);
                    ObjList<MmapSegment> trimmed = ring.drainTrimmable();
                    assertNotNull(trimmed);
                    assertEquals(2, trimmed.size());
                    for (int i = 0; i < trimmed.size(); i++) trimmed.get(i).close();
                    // I/O thread was holding seg0Snapshot; nextSealedAfter must
                    // still return seg2 (baseSeq=2), not crash, not return seg0Snapshot itself.
                    MmapSegment next = ring.nextSealedAfter(seg0Snapshot);
                    assertNotNull(next);
                    assertEquals(2L, next.baseSeq());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test(timeout = 30_000L)
    public void testNextSealedAfterSurvivesConcurrentRotationAndHeadTrim() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 16);
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                MmapSegment seg0 = MmapSegment.create(tmpDir + "/race-0.sfa", 0, segSize);
                try (SegmentRing ring = new SegmentRing(seg0, segSize)) {
                    fillPattern(buf, 16, 0);
                    for (int i = 0; i < 3; i++) {
                        assertEquals(i, ring.appendOrFsn(buf, 16));
                        ring.installHotSpare(MmapSegment.create(
                                tmpDir + "/race-" + (i + 1) + ".sfa",
                                ring.nextSeqHint(), segSize));
                    }
                    MmapSegment cursor = ring.firstSealed();
                    assertNotNull(cursor);
                    ring.acknowledge(1);

                    CountDownLatch ready = new CountDownLatch(2);
                    CountDownLatch start = new CountDownLatch(1);
                    AtomicReference<Throwable> failure = new AtomicReference<>();
                    Thread rotate = new Thread(() -> {
                        ready.countDown();
                        try {
                            start.await();
                            assertEquals(3L, ring.appendOrFsn(buf, 16));
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        }
                    }, "segment-rotate");
                    Thread trim = new Thread(() -> {
                        ready.countDown();
                        try {
                            start.await();
                            ObjList<MmapSegment> drained = ring.drainTrimmable();
                            assertNotNull(drained);
                            assertEquals(2, drained.size());
                            for (int i = 0; i < drained.size(); i++) {
                                drained.get(i).close();
                            }
                        } catch (Throwable t) {
                            failure.compareAndSet(null, t);
                        }
                    }, "segment-trim");

                    synchronized (ring) {
                        rotate.start();
                        trim.start();
                        ready.await();
                        start.countDown();
                    }
                    rotate.join();
                    trim.join();
                    assertNull("concurrent mutation failed: " + failure.get(), failure.get());

                    MmapSegment next = ring.nextSealedAfter(cursor);
                    assertNotNull(next);
                    assertEquals("trim fallback must skip both removed successors", 2L, next.baseSeq());
                    assertEquals("rotation must leave the former active sealed", 1, ring.getSealedSegments().size());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Open-time sort regression: at the documented {@code sf_max_total_bytes
     * / sf_max_bytes} ceiling (~16K segments), an O(N²) sort over the
     * recovered segments delays the I/O thread. This test guards the sort
     * with a deterministic comparison count.
     * <p>
     * Constructs N=2048 valid one-frame segments with names assigned in
     * lexicographic order — the exact pattern {@code readdir} produces on
     * many filesystems (and the worst case for a naive first-element pivot).
     * Recovers, asserts contiguous baseSeq ordering and total frame count,
     * and bounds sort comparisons independently of CI timing or filesystem
     * throughput.
     */
    @Test
    public void testLargeSegmentCountRecoveryIsLogLinear() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int n = 2048;
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < 16; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) i);
                }
                // Lexicographic 5-digit zero-padded prefix → readdir on most
                // filesystems returns entries in ascending baseSeq order, the
                // worst case for naive quicksort pivots.
                for (int i = 0; i < n; i++) {
                    String name = String.format("sf-%05d.sfa", i);
                    long segSize = MmapSegment.HEADER_SIZE
                            + MmapSegment.FRAME_HEADER_SIZE + 16;
                    try (MmapSegment seg = MmapSegment.create(tmpDir + "/" + name, i, segSize)) {
                        assertTrue("setup append " + i, seg.tryAppend(buf, 16) >= 0);
                    }
                }

                SegmentRing.resetSortComparisons();
                try (SegmentRing ring = SegmentRing.openExisting(tmpDir,
                        MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 16)) {
                    long comparisons = SegmentRing.getSortComparisons();
                    assertNotNull("recovery must produce a ring", ring);
                    // After recovery, the ring's nextSeqHint is one past the
                    // last frame on disk. With one frame per segment numbered
                    // 0..n-1, that's exactly n.
                    assertEquals("recovered ring must see all " + n + " frames in order",
                            n, ring.nextSeqHint());
                    // publishedFsn = n - 1 (last frame visible).
                    assertEquals(n - 1, ring.publishedFsn());
                    assertEquals("full valid chain must retain every segment",
                            n - 1, ring.getSealedSegments().size());
                    // O(N log N) quicksort with good pivots does ~2-3 * N * log2(N)
                    // comparisons; the partition-pass + median-of-three counter we
                    // increment per recursive frame upper-bounds this at roughly
                    // 3 N log2(N). The naive O(N²) regression on already-sorted
                    // input would do ~N(N-1)/2 -- 2.1M at N=2048 vs the ~67k a
                    // healthy sort produces. The 5x N log2(N) bound below sits
                    // about 30x below the O(N²) value and 1.5x above the
                    // expected count, so it fires on a real regression without
                    // flapping on harmless implementation drift. Comparison
                    // counts are deterministic across CI hardware, unlike the
                    // wall-clock bound this assertion used to carry.
                    long bound = 5L * n * (long) (Math.log(n) / Math.log(2));
                    assertTrue("sort took " + comparisons + " comparisons (expected < " + bound
                                    + " = 5 * N * log2(N) for N=" + n + "); "
                                    + "regression suggests the segment sort is back to O(N^2)",
                            comparisons < bound);
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Adversarial-order companion to
     * {@link #testLargeSegmentCountRecoveryIsLogLinear}: that test covers the
     * already-sorted readdir order median-of-three was chosen for; this one
     * covers the orders median-of-three does NOT defend. On the
     * pre-introsort quicksort, exact simulation at N=16384 measured ~22.6M
     * comparisons for organ-pipe, ~134M (full N²/2) for mass-duplicate
     * baseSeqs and ~50.7M for Musser's median-of-three-killer permutation.
     * The heapsort fallback caps the worst of these at ~3.8·N·log₂(N)
     * (~860K), so the 8·N·log₂(N) bound below keeps >2x headroom against
     * harmless implementation drift while sitting ~26x under the mildest
     * quadratic blow-up. Also pins unsigned key ordering: baseSeqs with the
     * sign bit set must sort above {@code Long.MAX_VALUE}, not below zero.
     * <p>
     * A healthy client cannot produce these orders (recovery validation
     * rejects duplicate or non-contiguous baseSeqs moments after the sort),
     * but corrupted-yet-parseable headers or operator file copies feed the
     * sort BEFORE validation runs, so the sort itself must stay log-linear.
     * Sorts in-memory segments directly: staging 16K adversarial header
     * files on disk per pattern would dominate the test's runtime without
     * adding coverage.
     */
    @Test
    public void testAdversarialSegmentOrdersSortLogLinear() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int n = 16384;
            final long bound = 8L * n * (long) (Math.log(n) / Math.log(2));

            // Organ pipe: 0,1,...,n/2-1,n/2-1,...,1,0.
            long[] organPipe = new long[n];
            for (int i = 0; i < n / 2; i++) {
                organPipe[i] = i;
                organPipe[n - 1 - i] = i;
            }
            assertAdversarialSortWithinBound("organ-pipe", organPipe, bound);

            long[] allDuplicates = new long[n];
            for (int i = 0; i < n; i++) {
                allDuplicates[i] = 42;
            }
            assertAdversarialSortWithinBound("all-duplicates", allDuplicates, bound);

            long[] fewDistinct = new long[n];
            for (int i = 0; i < n; i++) {
                fewDistinct[i] = i % 4;
            }
            assertAdversarialSortWithinBound("few-distinct", fewDistinct, bound);

            // Musser's median-of-three killer permutation of 1..n.
            long[] med3Killer = new long[n];
            int half = n / 2;
            for (int i = 1; i <= half; i++) {
                if (i % 2 == 1) {
                    med3Killer[i - 1] = i;
                    med3Killer[i] = half + i;
                }
                med3Killer[half + i - 1] = 2L * i;
            }
            assertAdversarialSortWithinBound("median-of-three-killer", med3Killer, bound);

            // High-bit keys interleaved with small ones: exercises the
            // unsigned comparison contract alongside the comparison bound.
            long[] unsignedMix = new long[n];
            for (int i = 0; i < n; i++) {
                unsignedMix[i] = (i % 2 == 0) ? (0x8000000000000000L | i) : i;
            }
            assertAdversarialSortWithinBound("unsigned-mix", unsignedMix, bound);
        });
    }

    private static void assertAdversarialSortWithinBound(String label, long[] baseSeqs, long bound) {
        final long segSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1;
        ObjList<MmapSegment> list = new ObjList<>();
        try {
            for (long baseSeq : baseSeqs) {
                list.add(MmapSegment.createInMemory(baseSeq, segSize));
            }
            SegmentRing.resetSortComparisons();
            SegmentRing.sortByBaseSeqForTest(list);
            long comparisons = SegmentRing.getSortComparisons();
            for (int i = 1, size = list.size(); i < size; i++) {
                assertTrue(label + ": unsigned baseSeq order violated at index " + i,
                        Long.compareUnsigned(list.get(i - 1).baseSeq(), list.get(i).baseSeq()) <= 0);
            }
            assertTrue(label + " sort took " + comparisons + " comparisons (expected < " + bound
                            + " = 8 * N * log2(N) for N=" + baseSeqs.length
                            + "); regression suggests the introsort heapsort fallback stopped engaging",
                    comparisons < bound);
        } finally {
            for (int i = 0, size = list.size(); i < size; i++) {
                list.get(i).close();
            }
        }
    }

    @Test
    public void testRemovingAcknowledgedPrefixMovesLinearReferences() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1L;
            long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                long previousMoves = -1;
                for (int sealedCount = 64; sealedCount <= 256; sealedCount *= 2) {
                    MmapSegment initial = MmapSegment.createInMemory(0, segSize);
                    try (SegmentRing ring = new SegmentRing(initial, segSize)) {
                        for (int i = 0; i < 2 * sealedCount; i++) {
                            assertEquals(i, ring.appendOrFsn(buf, 1));
                            ring.installHotSpare(MmapSegment.createInMemory(ring.nextSeqHint(), segSize));
                        }
                        assertEquals(2L * sealedCount, ring.appendOrFsn(buf, 1));
                        ring.acknowledge(sealedCount - 1L);
                        SegmentRing.resetTrimMovedReferences();
                        MmapSegment segment;
                        int removed = 0;
                        while ((segment = ring.firstTrimmable()) != null) {
                            assertTrue(ring.removeTrimmable(segment));
                            segment.close();
                            removed++;
                        }
                        assertEquals(sealedCount, removed);
                        assertEquals("unacknowledged suffix must remain live", sealedCount,
                                ring.getSealedSegments().size());
                        assertEquals(sealedCount, ring.firstSealed().baseSeq());
                        long moves = SegmentRing.getTrimMovedReferences();
                        assertTrue("removing " + sealedCount + " entries moved " + moves
                                        + " references; expected amortized linear work",
                                moves <= 2L * sealedCount);
                        if (previousMoves >= 0) {
                            assertTrue("doubling the acknowledged prefix grew moved references from "
                                            + previousMoves + " to " + moves,
                                    moves <= 2L * previousMoves + sealedCount);
                        }
                        previousMoves = moves;
                    }
                }
            } finally {
                Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testMaxBytesPerSegmentReturnsConfiguredValue() throws Exception {
        // Direct constructor path: the value passed in must round-trip through
        // the accessor. SegmentManager seeds its totalBytes accounting from this
        // (per-ring contribution = active + spare + sealed * maxBytesPerSegment),
        // so a stale or rounded-down readback would silently mis-size the cap.
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 32);
            MmapSegment seg = MmapSegment.create(tmpDir + "/seg.sfa", 0, segSize);
            try (SegmentRing ring = new SegmentRing(seg, segSize)) {
                assertEquals(segSize, ring.maxBytesPerSegment());
            }
        });
    }

    @Test
    public void testMaxBytesPerSegmentSurvivesOpenExisting() throws Exception {
        // Recovery path: openExisting builds the ring from on-disk segments and
        // forwards the supplied maxBytesPerSegment into the constructor. The
        // accessor must report what the caller passed, not the file size of any
        // particular segment (those can legitimately differ when the operator
        // shrinks segment-size-bytes between sessions).
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 32);
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                try (MmapSegment seg = MmapSegment.create(tmpDir + "/sf-00000.sfa", 0, segSize)) {
                    fillPattern(buf, 32, 0);
                    assertTrue(seg.tryAppend(buf, 32) >= 0);
                }

                try (SegmentRing recovered = SegmentRing.openExisting(tmpDir, segSize)) {
                    assertNotNull(recovered);
                    assertEquals(segSize, recovered.maxBytesPerSegment());
                }
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Pins findSegmentContaining across the full boundary matrix so the
     * O(log N) lookup rewrite is provably semantics-preserving: empty ring,
     * miss below the head segment, exact segment base, mid-segment, last FSN
     * in a segment, tail sealed segment, active-segment hits, miss above the
     * published range, and misses below a trimmed live head (sealedHead > 0).
     * Three frames per segment give every sealed segment a distinct base,
     * mid and last FSN.
     */
    @Test
    public void testFindSegmentContainingBoundaryMatrix() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 3 * (MmapSegment.FRAME_HEADER_SIZE + 8);
            long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
            try {
                fillPattern(buf, 8, 0);
                MmapSegment initial = MmapSegment.createInMemory(0, segSize);
                try (SegmentRing ring = new SegmentRing(initial, segSize)) {
                    // Empty ring: no frame published anywhere, every lookup misses.
                    assertNull(ring.findSegmentContaining(-1));
                    assertNull(ring.findSegmentContaining(0));

                    for (int i = 0; i < 12; i++) {
                        if (ring.needsHotSpare()) {
                            ring.installHotSpare(
                                    MmapSegment.createInMemory(ring.nextSeqHint(), segSize));
                        }
                        assertEquals(i, ring.appendOrFsn(buf, 8));
                    }
                    // Sealed: [0-2], [3-5], [6-8]; active: [9-11].
                    for (long fsn = 0; fsn < 12; fsn++) {
                        MmapSegment seg = ring.findSegmentContaining(fsn);
                        assertNotNull("fsn " + fsn, seg);
                        assertEquals("fsn " + fsn, fsn / 3 * 3, seg.baseSeq());
                    }
                    assertSame(ring.getActive(), ring.findSegmentContaining(9));
                    assertSame(ring.getActive(), ring.findSegmentContaining(11));
                    assertNull(ring.findSegmentContaining(-1));
                    assertNull(ring.findSegmentContaining(12));
                    assertNull(ring.findSegmentContaining(Long.MIN_VALUE));
                    assertNull(ring.findSegmentContaining(Long.MAX_VALUE));

                    // Trim the two fully-ACK'd oldest sealed segments: their
                    // FSNs must now miss while the surviving window (with a
                    // non-zero sealedHead) still resolves every live FSN.
                    ring.acknowledge(5);
                    MmapSegment trimmable;
                    int removed = 0;
                    while ((trimmable = ring.firstTrimmable()) != null) {
                        assertTrue(ring.removeTrimmable(trimmable));
                        trimmable.close();
                        removed++;
                    }
                    assertEquals(2, removed);
                    for (long fsn = 0; fsn < 6; fsn++) {
                        assertNull("trimmed fsn " + fsn, ring.findSegmentContaining(fsn));
                    }
                    for (long fsn = 6; fsn < 12; fsn++) {
                        MmapSegment seg = ring.findSegmentContaining(fsn);
                        assertNotNull("fsn " + fsn, seg);
                        assertEquals("fsn " + fsn, fsn / 3 * 3, seg.baseSeq());
                    }
                }
            } finally {
                Unsafe.free(buf, 8, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    /**
     * Same lookup pinned over one-frame segments, where every FSN is both a
     * segment base and a segment's last FSN: 33 sealed segments (odd live
     * count), then a 17-segment trim leaving an even 16-segment window with
     * sealedHead well inside the backing list. Complements the three-frame
     * matrix by exercising both live-window parities and a larger list.
     */
    @Test
    public void testFindSegmentContainingSingleFrameSegments() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1;
            long buf = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                MmapSegment initial = MmapSegment.createInMemory(0, segSize);
                try (SegmentRing ring = new SegmentRing(initial, segSize)) {
                    for (int i = 0; i < 34; i++) {
                        assertEquals(i, ring.appendOrFsn(buf, 1));
                        ring.installHotSpare(
                                MmapSegment.createInMemory(ring.nextSeqHint(), segSize));
                    }
                    // Sealed: FSNs 0..32, one frame each; active: [33].
                    for (long fsn = 0; fsn <= 33; fsn++) {
                        MmapSegment seg = ring.findSegmentContaining(fsn);
                        assertNotNull("fsn " + fsn, seg);
                        assertEquals("fsn " + fsn, fsn, seg.baseSeq());
                    }
                    assertNull(ring.findSegmentContaining(-1));
                    assertNull(ring.findSegmentContaining(34));

                    ring.acknowledge(16);
                    MmapSegment trimmable;
                    int removed = 0;
                    while ((trimmable = ring.firstTrimmable()) != null) {
                        assertTrue(ring.removeTrimmable(trimmable));
                        trimmable.close();
                        removed++;
                    }
                    assertEquals(17, removed);
                    for (long fsn = 0; fsn < 17; fsn++) {
                        assertNull("trimmed fsn " + fsn, ring.findSegmentContaining(fsn));
                    }
                    for (long fsn = 17; fsn <= 33; fsn++) {
                        MmapSegment seg = ring.findSegmentContaining(fsn);
                        assertNotNull("fsn " + fsn, seg);
                        assertEquals("fsn " + fsn, fsn, seg.baseSeq());
                    }
                }
            } finally {
                Unsafe.free(buf, 1, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static byte[] readFileBytes(String path) throws java.io.IOException {
        return java.nio.file.Files.readAllBytes(Paths.get(path));
    }

    private static void fillPattern(long addr, int len, int seed) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(addr + i, (byte) (seed * 31 + i + 17));
        }
    }
}
