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
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Unsafe;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SegmentRingTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-ring-" + System.nanoTime()).toString();
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
    public void testAppendAssignsMonotonicFsnsAndPublishesThem() {
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
    }

    @Test
    public void testRotationConsumesHotSpare() {
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
    }

    @Test
    public void testRotationRebasesSpareToCorrectFsnRegardlessOfManagerGuess() {
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
    }

    @Test
    public void testAcknowledgeAndDrainTrimsOldestFirstUntilUnackedFound() {
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
    }

    @Test
    public void testOpenExistingReturnsNullOnEmptyDir() {
        assertEquals("nothing in dir → null ring",
                null, SegmentRing.openExisting(tmpDir, 8192));
    }

    @Test
    public void testOpenExistingRecoversActivePlusSealed() {
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
    }

    @Test
    public void testOpenExistingDetectsFsnGap() {
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
                SegmentRing.openExisting(tmpDir, segSize);
                throw new AssertionError("expected FSN gap to be detected");
            } catch (MmapSegmentException expected) {
                assertTrue(expected.getMessage(),
                        expected.getMessage().contains("FSN gap"));
            }
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testOpenExistingSkipsBadMagicFile() {
        long segSize = MmapSegment.HEADER_SIZE
                + (MmapSegment.FRAME_HEADER_SIZE + 16);
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            // One good segment.
            MmapSegment s0 = MmapSegment.create(tmpDir + "/good.sfa", 0, segSize);
            s0.tryAppend(buf, 16);
            s0.close();
            // One stray .sfa with no proper header — must be ignored.
            int fd = Files.openCleanRW(tmpDir + "/stray.sfa", 64);
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
    }

    @Test
    public void testAcknowledgeIsMonotonic() {
        long buf = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            MmapSegment seg = MmapSegment.create(tmpDir + "/m.sfa", 0, 8192);
            try (SegmentRing ring = new SegmentRing(seg, 8192)) {
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
    }

    @Test
    public void testNextSealedAfterWalksThousandsOfSegmentsWithoutOverflow() {
        // Regression for "sealed snapshot grew unexpectedly large".
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
                MmapSegment cursor = ring.firstSealed();
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
                // Walking past the last sealed → null (caller falls through to active).
                assertNull(ring.nextSealedAfter(cursor));
            }
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testNextSealedAfterStillReturnsCorrectlyWhenCursorWasTrimmed() {
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
    }

    private static void fillPattern(long addr, int len, int seed) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(addr + i, (byte) (seed * 31 + i + 17));
        }
    }
}
