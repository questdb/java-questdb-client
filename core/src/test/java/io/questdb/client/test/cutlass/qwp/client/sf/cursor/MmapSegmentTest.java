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
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MmapSegmentTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-mmap-seg-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDir(tmpDir);
    }

    @Test
    public void testBarrierChecksMsyncAndFsyncFailures() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                FaultyFilesFacade success = new FaultyFilesFacade();
                try (MmapSegment segment = MmapSegment.create(
                        success, tmpDir + "/barrier-ok.sfa", 0L, 4096L)) {
                    fillPattern(payload, 32, 1);
                    assertTrue(segment.tryAppend(payload, 32) >= 0L);
                    assertFalse(segment.isPublishedDurable());
                    assertEquals(segment.publishedOffset(), segment.syncPublished());
                    assertTrue(segment.isPublishedDurable());
                    assertEquals(1, success.msyncCalls);
                    assertEquals(1, success.fsyncCalls);
                }

                FaultyFilesFacade msyncFailure = new FaultyFilesFacade();
                msyncFailure.failOnMsync = true;
                try (MmapSegment segment = MmapSegment.create(
                        msyncFailure, tmpDir + "/barrier-msync-fail.sfa", 0L, 4096L)) {
                    assertTrue(segment.tryAppend(payload, 32) >= 0L);
                    try {
                        segment.syncPublished();
                        fail("expected msync failure");
                    } catch (MmapSegmentException expected) {
                        assertTrue(expected.getMessage().contains("sync segment data"));
                    }
                    assertFalse(segment.isPublishedDurable());
                    assertEquals(1, msyncFailure.msyncCalls);
                    assertEquals(0, msyncFailure.fsyncCalls);
                }

                FaultyFilesFacade fsyncFailure = new FaultyFilesFacade();
                fsyncFailure.failOnFsync = true;
                try (MmapSegment segment = MmapSegment.create(
                        fsyncFailure, tmpDir + "/barrier-fsync-fail.sfa", 0L, 4096L)) {
                    assertTrue(segment.tryAppend(payload, 32) >= 0L);
                    try {
                        segment.syncPublished();
                        fail("expected fsync failure");
                    } catch (MmapSegmentException expected) {
                        assertTrue(expected.getMessage().contains("sync segment file"));
                    }
                    assertFalse(segment.isPublishedDurable());
                    assertEquals(1, fsyncFailure.msyncCalls);
                    assertEquals(1, fsyncFailure.fsyncCalls);
                }
            } finally {
                Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCapacityRemainingAccountsForFrameEnvelope() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/seg-cap.sfa";
            long size = MmapSegment.HEADER_SIZE
                    + MmapSegment.FRAME_HEADER_SIZE + 50
                    + MmapSegment.FRAME_HEADER_SIZE + 50;
            long buf = Unsafe.malloc(50, MemoryTag.NATIVE_DEFAULT);
            try {
                try (MmapSegment seg = MmapSegment.create(path, 0L, size)) {
                    // Initial: room for two 50-byte payloads (each with an 8-byte envelope).
                    long firstCap = seg.capacityRemaining();
                    assertTrue(firstCap >= 50);
                    // After one append, exactly one more 50-byte payload fits.
                    seg.tryAppend(buf, 50);
                    assertTrue(seg.capacityRemaining() >= 50);
                    seg.tryAppend(buf, 50);
                    assertEquals(0, seg.capacityRemaining());
                }
            } finally {
                Unsafe.free(buf, 50, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCompatibilityMsyncKeepsLegacyCallPattern() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                FaultyFilesFacade filesFacade = new FaultyFilesFacade();
                try (MmapSegment segment = MmapSegment.create(
                        filesFacade, tmpDir + "/compat-msync.sfa", 0L, 4096L)) {
                    assertTrue(segment.tryAppend(payload, 32) >= 0L);
                    segment.msync();
                    segment.msync();
                    assertEquals(2, filesFacade.msyncCalls);
                    assertEquals(0, filesFacade.fsyncCalls);
                    assertFalse(segment.isPublishedDurable());
                }
            } finally {
                Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCreateAppendCloseReopenScansAllFrames() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/seg-create.sfa";
            long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
            try {
                // Append 100 distinct payloads of 32 bytes each.
                try (MmapSegment seg = MmapSegment.create(path, 42L, 64 * 1024)) {
                    assertEquals(42L, seg.baseSeq());
                    assertEquals(MmapSegment.HEADER_SIZE, seg.publishedOffset());
                    for (int i = 0; i < 100; i++) {
                        fillPattern(buf, 32, i);
                        long offset = seg.tryAppend(buf, 32);
                        assertNotEquals("frame " + i + " should fit", -1L, offset);
                    }
                    long expectedEnd = MmapSegment.HEADER_SIZE
                            + 100L * (MmapSegment.FRAME_HEADER_SIZE + 32);
                    assertEquals(expectedEnd, seg.publishedOffset());
                }

                // Re-open: scan must land at exactly the same offset.
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals(42L, seg.baseSeq());
                    long expectedEnd = MmapSegment.HEADER_SIZE
                            + 100L * (MmapSegment.FRAME_HEADER_SIZE + 32);
                    assertEquals(expectedEnd, seg.publishedOffset());
                }
            } finally {
                Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testCreateFailsCleanlyWhenAllocateReturnsFalse() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/seg-enospc.sfa";
            long sizeBytes = MmapSegment.HEADER_SIZE
                    + MmapSegment.FRAME_HEADER_SIZE + 64;
            FaultyFilesFacade ff = new FaultyFilesFacade();
            ff.failOnAllocate = true;
            try {
                MmapSegment.create(ff, path, 0L, sizeBytes).close();
                fail("expected MmapSegmentException from failed pre-allocation");
            } catch (MmapSegmentException expected) {
                assertTrue(expected.getMessage(),
                        expected.getMessage().contains("pre-allocation failed"));
            }
            assertEquals("exclusive create must run exactly once", 1, ff.openRWExclusiveCalls);
            assertEquals("allocate must run exactly once", 1, ff.allocateCalls);
            assertEquals("fd must be closed on allocate failure", 1, ff.closeCalls);
            assertEquals("file must be removed on allocate failure", 1, ff.removeCalls);
            assertFalse("partial file must not survive failed allocate",
                    Files.exists(path));
        });
    }

    @Test
    public void testCreateFailsCleanlyWhenExclusiveOpenReturnsMinusOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/seg-noopen.sfa";
            long sizeBytes = MmapSegment.HEADER_SIZE
                    + MmapSegment.FRAME_HEADER_SIZE + 64;
            FaultyFilesFacade ff = new FaultyFilesFacade();
            ff.failOnOpenRWExclusive = true;
            try {
                MmapSegment.create(ff, path, 0L, sizeBytes).close();
                fail("expected MmapSegmentException from openRWExclusive returning -1");
            } catch (MmapSegmentException expected) {
                assertTrue(expected.getMessage(),
                        expected.getMessage().contains("exclusive create failed"));
            }
            assertEquals("exclusive create must run exactly once", 1, ff.openRWExclusiveCalls);
            assertEquals("allocate must not run after exclusive-create failure",
                    0, ff.allocateCalls);
            assertEquals("close must not be called when no fd was opened",
                    0, ff.closeCalls);
            // With O_EXCL semantics a create failure can mean "path already
            // exists and belongs to another lifecycle" -- create() must NOT
            // unlink a file it never owned.
            assertEquals("remove must not be called when exclusive create failed",
                    0, ff.removeCalls);
            assertFalse("no file should exist when exclusive create failed",
                    Files.exists(path));
        });
    }

    @Test
    public void testCreateRepeatedAllocateFailuresDoNotAccumulateOrphans() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long sizeBytes = MmapSegment.HEADER_SIZE
                    + MmapSegment.FRAME_HEADER_SIZE + 64;
            FaultyFilesFacade ff = new FaultyFilesFacade();
            ff.failOnAllocate = true;
            int attempts = 50;
            for (int i = 0; i < attempts; i++) {
                try {
                    MmapSegment.create(ff, tmpDir + "/seg-" + i + ".sfa", 0L, sizeBytes).close();
                    fail("expected MmapSegmentException on iteration " + i);
                } catch (MmapSegmentException ignored) {
                    // expected
                }
            }
            long find = Files.findFirst(tmpDir);
            int survivors = 0;
            if (find > 0) {
                try {
                    int rc = 1;
                    while (rc > 0) {
                        String name = Files.utf8ToString(Files.findName(find));
                        if (name != null && !".".equals(name) && !"..".equals(name)) {
                            survivors++;
                        }
                        rc = Files.findNext(find);
                    }
                } finally {
                    Files.findClose(find);
                }
            }
            assertEquals("no orphan files may survive repeated allocate failures",
                    0, survivors);
            assertEquals(attempts, ff.openRWExclusiveCalls);
            assertEquals(attempts, ff.allocateCalls);
            assertEquals(attempts, ff.closeCalls);
            assertEquals(attempts, ff.removeCalls);
        });
    }

    @Test
    public void testFirstFrameCrcCorruptionFlagsTornTailAndPreservesFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Existing torn-tail tests cover the case where N >= 1 valid
            // frames are followed by garbage. None cover frame[0] itself
            // being corrupt — yet a single bit-flip on the CRC of frame[0]
            // at rest (bit-rot, partial-page-write at crash) is the
            // worst-case data-loss trigger: the recovery scan bails at
            // HEADER_SIZE and frameCount drops to 0, even though valid frames
            // still sit on disk past the corrupt header.
            //
            // Contract: tornTailBytes() must be non-zero (because non-zero
            // bytes exist past the last good frame), and openExisting
            // must NOT delete the file. SegmentRing relies on the
            // tornTailBytes signal to distinguish "empty hot-spare" from
            // "valid data behind a corrupt frame[0]" and quarantine the
            // latter.
            String path = tmpDir + "/seg-frame0-corrupt.sfa";
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                // Write three legitimate frames so there's something the
                // recovery path could lose.
                try (MmapSegment seg = MmapSegment.create(path, 0L, 4096)) {
                    for (int i = 0; i < 3; i++) {
                        fillPattern(buf, 32, i);
                        seg.tryAppend(buf, 32);
                    }
                    assertEquals(3L, seg.frameCount());
                    seg.msync();
                }

                // Flip a bit in the CRC of frame[0]. Frame[0]'s CRC sits at
                // offset HEADER_SIZE in the file (FRAME_HEADER_SIZE layout
                // is u32 crc | u32 payloadLen). Overwriting all 4 bytes
                // with 0xDEADBEEF is statistically guaranteed to mismatch
                // any real CRC.
                int fd = Files.openRW(path);
                assertTrue("openRW must succeed", fd >= 0);
                long badCrcBuf = Unsafe.malloc(4, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().putInt(badCrcBuf, 0xDEADBEEF);
                    Files.write(fd, badCrcBuf, 4, MmapSegment.HEADER_SIZE);
                } finally {
                    Unsafe.free(badCrcBuf, 4, MemoryTag.NATIVE_DEFAULT);
                    Files.close(fd);
                }
                assertTrue("file must still exist after CRC clobber",
                        Files.exists(path));

                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals("recovery scan must bail at the corrupt frame[0]",
                            0L, seg.frameCount());
                    assertEquals("publishedOffset must rewind to the header end",
                            MmapSegment.HEADER_SIZE, seg.publishedOffset());
                    assertTrue(
                            "tornTailBytes must signal non-zero so SegmentRing "
                                    + "can distinguish a corrupt-data segment from an empty "
                                    + "hot-spare leftover; got " + seg.tornTailBytes(),
                            seg.tornTailBytes() > 0L);
                }
                // A second open must see the SAME evidence: openExisting is
                // observe-only, so the valid-CRC frames past the corrupt
                // frame[0] -- the only copy of that data -- stay on disk for
                // operator extraction (chain-level recovery of such a member
                // fails closed without mutating it).
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals(0L, seg.frameCount());
                    assertEquals("observe-only recovery must preserve the residue",
                            4096L - MmapSegment.HEADER_SIZE, seg.tornTailBytes());
                }
                assertTrue("openExisting must not unlink the corrupt file",
                        Files.exists(path));
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testAllZeroCorruptFrameHeaderDoesNotHideNonZeroSuffix() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/seg-zero-frame-header.sfa";
            long payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                fillPattern(payload, 32, 0);
                try (MmapSegment seg = MmapSegment.create(path, 0L, 4096)) {
                    assertTrue(seg.tryAppend(payload, 32) >= 0L);
                    assertTrue(seg.tryAppend(payload, 32) >= 0L);
                    seg.msync();
                }

                // Zero both the CRC and length of frame[0]. Looking only at
                // this 8-byte header would misclassify the segment as a clean
                // empty spare even though its payload and frame[1] remain.
                int fd = Files.openRW(path);
                assertTrue("openRW must succeed", fd >= 0);
                long zeroHeader = Unsafe.malloc(MmapSegment.FRAME_HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
                try {
                    Unsafe.getUnsafe().setMemory(zeroHeader, MmapSegment.FRAME_HEADER_SIZE, (byte) 0);
                    assertEquals(MmapSegment.FRAME_HEADER_SIZE,
                            Files.write(fd, zeroHeader, MmapSegment.FRAME_HEADER_SIZE, MmapSegment.HEADER_SIZE));
                    Files.fsync(fd);
                } finally {
                    Unsafe.free(zeroHeader, MmapSegment.FRAME_HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
                    Files.close(fd);
                }

                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals(0L, seg.frameCount());
                    assertEquals(MmapSegment.HEADER_SIZE, seg.publishedOffset());
                    assertEquals("non-zero bytes later in the suffix must preserve corruption evidence",
                            4096L - MmapSegment.HEADER_SIZE, seg.tornTailBytes());
                }
            } finally {
                Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFullSegmentRejectsFurtherAppends() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/seg-full.sfa";
            // Just enough room for header + exactly one 100-byte payload.
            long sizeBytes = MmapSegment.HEADER_SIZE
                    + MmapSegment.FRAME_HEADER_SIZE + 100;
            long buf = Unsafe.malloc(100, MemoryTag.NATIVE_DEFAULT);
            try {
                try (MmapSegment seg = MmapSegment.create(path, 0L, sizeBytes)) {
                    fillPattern(buf, 100, 0);
                    long ok = seg.tryAppend(buf, 100);
                    assertEquals("first append should fit at offset HEADER_SIZE",
                            MmapSegment.HEADER_SIZE, ok);
                    assertTrue("segment should now be full", seg.isFull());
                    assertEquals("a second append must be rejected",
                            -1L, seg.tryAppend(buf, 100));
                    assertEquals("an even-1-byte append must be rejected",
                            -1L, seg.tryAppend(buf, 1));
                }
            } finally {
                Unsafe.free(buf, 100, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testOpenExistingRejectsCorruptHeader() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/seg-bad-magic.sfa";
            // Build a file with the right size but the wrong magic.
            int fd = Files.openCleanRW(path);
            long bufHdr = Unsafe.malloc(MmapSegment.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putInt(bufHdr, 0xBAD0FACE);
                for (int i = 4; i < MmapSegment.HEADER_SIZE; i++) {
                    Unsafe.getUnsafe().putByte(bufHdr + i, (byte) 0);
                }
                assertEquals(MmapSegment.HEADER_SIZE,
                        Files.write(fd, bufHdr, MmapSegment.HEADER_SIZE, 0));
                Files.fsync(fd);
                Files.close(fd);
            } finally {
                Unsafe.free(bufHdr, MmapSegment.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
            }

            try {
                MmapSegment.openExisting(path).close();
                fail("openExisting should reject bad magic");
            } catch (MmapSegmentException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("bad magic"));
            }
        });
    }

    @Test
    public void testRecoveryDoesNotFlagCleanPartialFill() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Counterpart to the torn-tail test: a writer that wrote N valid
            // frames and stopped (clean) leaves an all-zero tail. Recovery must
            // NOT cry wolf — tornTailBytes should be 0 so log noise stays
            // proportional to actual incidents.
            String path = tmpDir + "/seg-clean-tail.sfa";
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                try (MmapSegment seg = MmapSegment.create(path, 0L, 4096)) {
                    for (int i = 0; i < 3; i++) {
                        fillPattern(buf, 16, i);
                        seg.tryAppend(buf, 16);
                    }
                    seg.msync();
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals("clean partial fill must report zero torn tail",
                            0L, seg.tornTailBytes());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRecoveryDoesNotFlagFreshUnusedSegment() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // A manager-allocated hot-spare that the writer never touched: the
            // file has just the header and an all-zero body. Recovery must not
            // emit a torn-tail signal here either.
            String path = tmpDir + "/seg-fresh.sfa";
            try (MmapSegment seg = MmapSegment.create(path, 42L, 4096)) {
                seg.msync();
            }
            try (MmapSegment seg = MmapSegment.openExisting(path)) {
                assertEquals("fresh-but-unused segment must report zero torn tail",
                        0L, seg.tornTailBytes());
            }
        });
    }

    @Test
    public void testOpenExistingObservesTornTailWithoutMutation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // openExisting must be a pure observer: a torn tail can hide
            // unreachable valid-CRC frames past a mid-file tear -- the only
            // copy of real payloads -- and whether they may be destroyed is
            // a chain-level decision (SegmentRing sanitizes the segment it
            // resumes as active, plus proven-dead sealed residue, and fails
            // closed preserving the bytes otherwise). Repeated opens must
            // keep reporting the same observation over identical bytes.
            String path = tmpDir + "/seg-observe.sfa";
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            long lastGood;
            try {
                try (MmapSegment seg = MmapSegment.create(path, 0L, 4096)) {
                    for (int i = 0; i < 3; i++) {
                        fillPattern(buf, 16, i);
                        seg.tryAppend(buf, 16);
                    }
                    lastGood = seg.publishedOffset();
                    long addr = seg.address();
                    for (long off = lastGood; off + 4 <= 4096; off += 4) {
                        Unsafe.getUnsafe().putInt(addr + off, 0xCAFEBABE);
                    }
                    seg.msync();
                }
                byte[] before = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path));
                for (int open = 0; open < 2; open++) {
                    try (MmapSegment seg = MmapSegment.openExisting(path)) {
                        assertEquals("open #" + open + " must report the residue",
                                4096L - lastGood, seg.tornTailBytes());
                        assertEquals(3L, seg.frameCount());
                        assertTrue(seg.hasUnsanitizedTornTail());
                    }
                    assertArrayEquals(
                            "observe-only recovery must not change a single byte (open #" + open + ')',
                            before, java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(path)));
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSanitizeTornTailZeroesResidueOnDisk() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Once ring recovery proves the residue is not load-bearing it
            // calls sanitizeTornTail: the open keeps reporting the observed
            // residue (operator signal), but after sanitization a later open
            // of the untouched file must find a clean zero suffix and intact
            // frames.
            String path = tmpDir + "/seg-zeroed.sfa";
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            long lastGood;
            try {
                try (MmapSegment seg = MmapSegment.create(path, 0L, 4096)) {
                    for (int i = 0; i < 3; i++) {
                        fillPattern(buf, 16, i);
                        seg.tryAppend(buf, 16);
                    }
                    lastGood = seg.publishedOffset();
                    long addr = seg.address();
                    for (long off = lastGood; off + 4 <= 4096; off += 4) {
                        Unsafe.getUnsafe().putInt(addr + off, 0xCAFEBABE);
                    }
                    seg.msync();
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals("recovery must report the observed residue",
                            4096L - lastGood, seg.tornTailBytes());
                    assertTrue(seg.hasUnsanitizedTornTail());
                    seg.sanitizeTornTail();
                    assertEquals("the observation must survive sanitization for diagnostics",
                            4096L - lastGood, seg.tornTailBytes());
                    assertFalse(seg.hasUnsanitizedTornTail());
                    // Idempotent: a second call is a no-op, not a re-zero.
                    seg.sanitizeTornTail();
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals("sanitize must have zeroed the residue on disk",
                            0L, seg.tornTailBytes());
                    assertEquals(lastGood, seg.publishedOffset());
                    assertEquals(3L, seg.frameCount());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testResealGapResidueCannotSurviveRecoveryAppends() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Two-crash reseal regression at segment level. Crash #1 leaves
            // residue up to the file end; recovery resumes at lastGood; the
            // resumed writer fills the segment but its last frame stops short
            // of the file end (the remaining gap cannot fit another frame).
            // Pre-fix the stale residue survived in that gap, so the segment
            // -- sealed as-is by rotation -- failed the sealed-suffix-must-
            // be-zero check on the NEXT recovery, permanently failing startup.
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 16)
                    + 12; // reseal gap: a 5th 24-byte frame can never fit
            String path = tmpDir + "/seg-reseal.sfa";
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                // Session 1: two frames, then a crash mid-write near the end
                // -- garbage from the torn frame all the way to the file end.
                try (MmapSegment seg = MmapSegment.create(path, 0L, segSize)) {
                    fillPattern(buf, 16, 0);
                    seg.tryAppend(buf, 16);
                    fillPattern(buf, 16, 1);
                    seg.tryAppend(buf, 16);
                    long addr = seg.address();
                    for (long off = seg.publishedOffset(); off + 4 <= segSize; off += 4) {
                        Unsafe.getUnsafe().putInt(addr + off, 0xCAFEBABE);
                    }
                    seg.msync();
                }
                // Recovery #1 + session 2: fill the segment to its rotation
                // point. The 4th frame ends 12 bytes short of the file end --
                // a region session 2 never overwrites. Sanitize before the
                // appends, exactly as SegmentRing.recover does for the
                // segment it resumes as active.
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals(2L, seg.frameCount());
                    seg.sanitizeTornTail();
                    fillPattern(buf, 16, 2);
                    assertTrue(seg.tryAppend(buf, 16) >= 0);
                    fillPattern(buf, 16, 3);
                    assertTrue(seg.tryAppend(buf, 16) >= 0);
                    assertEquals("next append must not fit (rotation would seal here)",
                            -1L, seg.tryAppend(buf, 16));
                    seg.msync();
                }
                // Recovery #2: the state a sealed segment presents at the next
                // startup. Its suffix must be clean or ring recovery bricks.
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals("all four frames must recover", 4L, seg.frameCount());
                    assertEquals("no residue may survive in the reseal gap: a sealed "
                                    + "segment with a non-zero suffix permanently fails "
                                    + "ring recovery",
                            0L, seg.tornTailBytes());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testDiscardedStaleFrameNotResurrectedByLaterRecovery() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The frame envelope [crc(len|payload)][len][payload] binds
            // neither position nor FSN, so a stale frame with a valid CRC
            // sitting past the tear -- byte-aligned with the resumed writer's
            // frames, natural with fixed-size records -- would be silently
            // re-adopted by the next recovery scan at a recycled FSN.
            // Recovery #1 discarded it; recovery #2 must not bring it back.
            String path = tmpDir + "/seg-stale.sfa";
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            long frameB;
            try {
                try (MmapSegment seg = MmapSegment.create(path, 0L, 4096)) {
                    fillPattern(buf, 16, 0);
                    seg.tryAppend(buf, 16);            // frame A, FSN 0
                    fillPattern(buf, 16, 1);
                    frameB = seg.tryAppend(buf, 16);   // frame B, FSN 1
                    fillPattern(buf, 16, 2);
                    seg.tryAppend(buf, 16);            // frame C, FSN 2
                    // The crash tears frame B only: flip its CRC. C keeps a
                    // valid CRC at a frame-aligned offset past the tear.
                    long addr = seg.address();
                    int crc = Unsafe.getUnsafe().getInt(addr + frameB);
                    Unsafe.getUnsafe().putInt(addr + frameB, crc ^ 0x5A5A5A5A);
                    seg.msync();
                }
                // Recovery #1: B fails CRC, so the scan stops at B; B and C
                // are discarded, and the resume-as-active sanitization (the
                // same call SegmentRing.recover makes) zeroes them on disk.
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals("scan must stop at the torn frame", 1L, seg.frameCount());
                    seg.sanitizeTornTail();
                    // The resumed writer re-issues FSN 1 with a fresh payload
                    // of the old B's exact size -- the byte-aligned case.
                    fillPattern(buf, 16, 7);
                    assertEquals(frameB, seg.tryAppend(buf, 16));
                    seg.msync();
                }
                // Recovery #2: pre-fix the scan walked A, B' and then adopted
                // the STALE C (valid CRC) as a live frame -- data recovery #1
                // had already discarded, resurrected behind the engine's back.
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals("stale frame C must stay discarded, not be "
                            + "resurrected at a recycled FSN", 2L, seg.frameCount());
                    assertEquals(0L, seg.tornTailBytes());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSanitizeTornTailSyncFailureFailsClosed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // The sanitize barrier is load-bearing: if the zeroes cannot be
            // made durable, ring recovery must fail closed rather than expose
            // a segment whose reseal could permanently fail the next startup.
            // openExisting itself runs no barrier -- it never writes. A
            // failed attempt may still leave zeroes in the page cache; that
            // is safe (a retry either sees a clean tail or re-zeroes), so
            // the follow-up open with a healthy facade must succeed.
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                String msyncPath = tmpDir + "/seg-zero-msync-fail.sfa";
                String fsyncPath = tmpDir + "/seg-zero-fsync-fail.sfa";
                for (String path : new String[]{msyncPath, fsyncPath}) {
                    try (MmapSegment seg = MmapSegment.create(path, 0L, 4096)) {
                        fillPattern(buf, 16, 0);
                        seg.tryAppend(buf, 16);
                        long addr = seg.address();
                        Unsafe.getUnsafe().putInt(addr + seg.publishedOffset(), 0xCAFEBABE);
                        Unsafe.getUnsafe().putInt(addr + seg.publishedOffset() + 4, 16);
                        seg.msync();
                    }
                }

                FaultyFilesFacade msyncFailure = new FaultyFilesFacade();
                msyncFailure.failOnMsync = true;
                try (MmapSegment seg = MmapSegment.openExisting(msyncFailure, msyncPath)) {
                    assertEquals("observe-only open must not run the zeroing barrier",
                            0, msyncFailure.msyncCalls);
                    try {
                        seg.sanitizeTornTail();
                        fail("expected sanitize to abort when the zeroed tail cannot be msync'd");
                    } catch (MmapSegmentException expected) {
                        assertTrue(expected.getMessage(),
                                expected.getMessage().contains("zeroed torn tail"));
                    }
                    assertTrue("a failed barrier must leave the residue unsanitized",
                            seg.hasUnsanitizedTornTail());
                }
                assertEquals(1, msyncFailure.msyncCalls);
                assertEquals(0, msyncFailure.fsyncCalls);
                try (MmapSegment seg = MmapSegment.openExisting(msyncPath)) {
                    assertEquals(1L, seg.frameCount());
                }

                FaultyFilesFacade fsyncFailure = new FaultyFilesFacade();
                fsyncFailure.failOnFsync = true;
                try (MmapSegment seg = MmapSegment.openExisting(fsyncFailure, fsyncPath)) {
                    try {
                        seg.sanitizeTornTail();
                        fail("expected sanitize to abort when the zeroed tail cannot be fsync'd");
                    } catch (MmapSegmentException expected) {
                        assertTrue(expected.getMessage(),
                                expected.getMessage().contains("zeroed torn tail"));
                    }
                    assertTrue("a failed barrier must leave the residue unsanitized",
                            seg.hasUnsanitizedTornTail());
                }
                assertEquals(1, fsyncFailure.msyncCalls);
                assertEquals(1, fsyncFailure.fsyncCalls);
                try (MmapSegment seg = MmapSegment.openExisting(fsyncPath)) {
                    assertEquals(1L, seg.frameCount());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testSanitizeTornTailRefusedAfterAppendsResume() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Zeroing [residueStart, EOF) after appends resumed would destroy
            // freshly appended frames, so that ordering is rejected outright:
            // sanitization is a recovery-time-only operation that must run
            // before the segment takes traffic.
            String path = tmpDir + "/seg-sanitize-late.sfa";
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                try (MmapSegment seg = MmapSegment.create(path, 0L, 4096)) {
                    fillPattern(buf, 16, 0);
                    seg.tryAppend(buf, 16);
                    long addr = seg.address();
                    Unsafe.getUnsafe().putInt(addr + seg.publishedOffset(), 0xCAFEBABE);
                    seg.msync();
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertTrue(seg.hasUnsanitizedTornTail());
                    fillPattern(buf, 16, 1);
                    assertTrue(seg.tryAppend(buf, 16) >= 0);
                    try {
                        seg.sanitizeTornTail();
                        fail("sanitize after appends must be refused");
                    } catch (MmapSegmentException expected) {
                        assertTrue(expected.getMessage(),
                                expected.getMessage().contains("before appends resume"));
                    }
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testRecoverySignalsTornTailWithByteCount() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Recovery must distinguish "writer attempted a frame past lastGood
            // and failed" (torn tail — possible corruption / partial write) from
            // a clean partial fill (no incident, just unwritten space).
            // Pre-fix: silent truncation with no diagnostic.
            String path = tmpDir + "/seg-torn-signal.sfa";
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            long lastGood;
            try {
                try (MmapSegment seg = MmapSegment.create(path, 0L, 4096)) {
                    for (int i = 0; i < 3; i++) {
                        fillPattern(buf, 16, i);
                        seg.tryAppend(buf, 16);
                    }
                    lastGood = seg.publishedOffset();
                    // Inject a non-zero attempted-frame signature past the last
                    // valid frame: a CRC and length that don't validate. This
                    // mirrors a partial write or in-place corruption.
                    long addr = seg.address();
                    Unsafe.getUnsafe().putInt(addr + lastGood, 0xCAFEBABE);
                    Unsafe.getUnsafe().putInt(addr + lastGood + 4, 16);
                    seg.msync();
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals("scan must stop at last good frame", lastGood, seg.publishedOffset());
                    assertTrue("torn tail must be reported as nonzero so operators see "
                                    + "silent truncation; got " + seg.tornTailBytes(),
                            seg.tornTailBytes() > 0);
                    assertEquals("torn-tail count must be the byte gap to file end",
                            4096L - lastGood, seg.tornTailBytes());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testTornTailFromNegativeOrOversizedLengthAlsoRecovered() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/seg-bad-len.sfa";
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            long expectedEnd;
            try {
                try (MmapSegment seg = MmapSegment.create(path, 9L, 4096)) {
                    fillPattern(buf, 16, 1);
                    seg.tryAppend(buf, 16);
                    expectedEnd = seg.publishedOffset();
                    long addr = seg.address();
                    // Negative length — defensive scan must reject this.
                    Unsafe.getUnsafe().putInt(addr + expectedEnd, 0);
                    Unsafe.getUnsafe().putInt(addr + expectedEnd + 4, -1);
                    seg.msync();
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals(expectedEnd, seg.publishedOffset());
                }
                // Now an absurdly oversized length that would run past EOF.
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    long addr = seg.address();
                    Unsafe.getUnsafe().putInt(addr + expectedEnd, 0);
                    Unsafe.getUnsafe().putInt(addr + expectedEnd + 4, Integer.MAX_VALUE);
                    seg.msync();
                }
                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals(expectedEnd, seg.publishedOffset());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testTornTailIsRecoveredCleanly() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/seg-torn.sfa";
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            long expectedEnd;
            try {
                try (MmapSegment seg = MmapSegment.create(path, 7L, 64 * 1024)) {
                    for (int i = 0; i < 5; i++) {
                        fillPattern(buf, 16, i);
                        seg.tryAppend(buf, 16);
                    }
                    expectedEnd = seg.publishedOffset();
                    // Now corrupt what would be the start of the next frame:
                    // write a plausible-looking 4-byte length followed by some bytes,
                    // but no matching CRC. Recovery scan should detect this and
                    // stop at expectedEnd (the start of the bad frame).
                    long addr = seg.address();
                    Unsafe.getUnsafe().putInt(addr + expectedEnd, 0xCAFEBABE);   // garbage CRC
                    Unsafe.getUnsafe().putInt(addr + expectedEnd + 4, 32);        // declared length
                    // Don't bother filling the body — CRC mismatch alone defeats it.
                    seg.msync(); // make sure pages flushed before reopen reads them
                }

                try (MmapSegment seg = MmapSegment.openExisting(path)) {
                    assertEquals("scan must stop at the torn frame's start", expectedEnd,
                            seg.publishedOffset());
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void fillPattern(long addr, int len, int seed) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(addr + i, (byte) (seed * 31 + i + 17));
        }
    }

    // Test seam: counts the calls MmapSegment.create makes through the facade
    // and lets each test induce a clean failure at one of the create-time
    // syscalls. Anything not overridden here delegates to FilesFacade.INSTANCE.
    private static final class FaultyFilesFacade implements FilesFacade {
        int allocateCalls;
        int closeCalls;
        boolean failOnAllocate;
        boolean failOnFsync;
        boolean failOnMsync;
        boolean failOnOpenCleanRW;
        boolean failOnOpenRWExclusive;
        int fsyncCalls;
        int msyncCalls;
        int openCleanRWCalls;
        int openRWExclusiveCalls;
        int removeCalls;

        @Override
        public int openRWExclusive(String path) {
            openRWExclusiveCalls++;
            if (failOnOpenRWExclusive) {
                return -1;
            }
            return INSTANCE.openRWExclusive(path);
        }

        @Override
        public int openRWExclusive(long pathPtr) {
            openRWExclusiveCalls++;
            if (failOnOpenRWExclusive) {
                return -1;
            }
            return INSTANCE.openRWExclusive(pathPtr);
        }

        @Override
        public long allocNativePath(String path) {
            return INSTANCE.allocNativePath(path);
        }

        @Override
        public boolean allocate(int fd, long size) {
            allocateCalls++;
            if (failOnAllocate) {
                return false;
            }
            return INSTANCE.allocate(fd, size);
        }

        @Override
        public int close(int fd) {
            closeCalls++;
            return INSTANCE.close(fd);
        }

        @Override
        public boolean exists(String path) {
            return INSTANCE.exists(path);
        }

        @Override
        public void findClose(long findPtr) {
            INSTANCE.findClose(findPtr);
        }

        @Override
        public long findFirst(String dir) {
            return INSTANCE.findFirst(dir);
        }

        @Override
        public long findName(long findPtr) {
            return INSTANCE.findName(findPtr);
        }

        @Override
        public int findNext(long findPtr) {
            return INSTANCE.findNext(findPtr);
        }

        @Override
        public int findType(long findPtr) {
            return INSTANCE.findType(findPtr);
        }

        @Override
        public void freeNativePath(long pathPtr) {
            INSTANCE.freeNativePath(pathPtr);
        }

        @Override
        public int fsync(int fd) {
            fsyncCalls++;
            return failOnFsync ? -1 : INSTANCE.fsync(fd);
        }

        @Override
        public long length(int fd) {
            return INSTANCE.length(fd);
        }

        @Override
        public long length(String path) {
            return INSTANCE.length(path);
        }

        @Override
        public int lock(int fd) {
            return INSTANCE.lock(fd);
        }

        @Override
        public int mkdir(String path, int mode) {
            return INSTANCE.mkdir(path, mode);
        }

        @Override
        public int msync(long addr, long len, boolean async) {
            msyncCalls++;
            return failOnMsync ? -1 : INSTANCE.msync(addr, len, async);
        }

        @Override
        public int openCleanRW(String path) {
            openCleanRWCalls++;
            if (failOnOpenCleanRW) {
                return -1;
            }
            return INSTANCE.openCleanRW(path);
        }

        @Override
        public int openCleanRW(long pathPtr) {
            openCleanRWCalls++;
            if (failOnOpenCleanRW) {
                return -1;
            }
            return INSTANCE.openCleanRW(pathPtr);
        }

        @Override
        public int openRW(String path) {
            return INSTANCE.openRW(path);
        }

        @Override
        public int openRW(long pathPtr) {
            return INSTANCE.openRW(pathPtr);
        }

        @Override
        public long length(long pathPtr) {
            return INSTANCE.length(pathPtr);
        }

        @Override
        public long read(int fd, long addr, long len, long offset) {
            return INSTANCE.read(fd, addr, len, offset);
        }

        @Override
        public boolean remove(String path) {
            removeCalls++;
            return INSTANCE.remove(path);
        }

        @Override
        public boolean remove(long pathPtr) {
            removeCalls++;
            return INSTANCE.remove(pathPtr);
        }

        @Override
        public int rename(String oldPath, String newPath) {
            return INSTANCE.rename(oldPath, newPath);
        }

        @Override
        public boolean truncate(int fd, long size) {
            return INSTANCE.truncate(fd, size);
        }

        @Override
        public long write(int fd, long addr, long len, long offset) {
            return INSTANCE.write(fd, addr, len, offset);
        }
    }
}
