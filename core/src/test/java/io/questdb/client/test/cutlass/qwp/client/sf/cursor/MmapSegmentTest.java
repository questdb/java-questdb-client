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
            assertEquals("openCleanRW must run exactly once", 1, ff.openCleanRWCalls);
            assertEquals("allocate must run exactly once", 1, ff.allocateCalls);
            assertEquals("fd must be closed on allocate failure", 1, ff.closeCalls);
            assertEquals("file must be removed on allocate failure", 1, ff.removeCalls);
            assertFalse("partial file must not survive failed allocate",
                    Files.exists(path));
        });
    }

    @Test
    public void testCreateFailsCleanlyWhenOpenCleanRWReturnsMinusOne() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String path = tmpDir + "/seg-noopen.sfa";
            long sizeBytes = MmapSegment.HEADER_SIZE
                    + MmapSegment.FRAME_HEADER_SIZE + 64;
            FaultyFilesFacade ff = new FaultyFilesFacade();
            ff.failOnOpenCleanRW = true;
            try {
                MmapSegment.create(ff, path, 0L, sizeBytes).close();
                fail("expected MmapSegmentException from openCleanRW returning -1");
            } catch (MmapSegmentException expected) {
                assertTrue(expected.getMessage(),
                        expected.getMessage().contains("openCleanRW failed"));
            }
            assertEquals("openCleanRW must run exactly once", 1, ff.openCleanRWCalls);
            assertEquals("allocate must not run after openCleanRW failure",
                    0, ff.allocateCalls);
            assertEquals("close must not be called when no fd was opened",
                    0, ff.closeCalls);
            assertEquals("remove must not be called when openCleanRW failed",
                    0, ff.removeCalls);
            assertFalse("no file should exist when openCleanRW failed",
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
            assertEquals(attempts, ff.openCleanRWCalls);
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
            // worst-case data-loss trigger: scanFrames bails at HEADER_SIZE
            // and frameCount drops to 0, even though valid frames still
            // sit on disk past the corrupt header.
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
                    assertEquals("scanFrames must bail at the corrupt frame[0]",
                            0L, seg.frameCount());
                    assertEquals("publishedOffset must rewind to the header end",
                            MmapSegment.HEADER_SIZE, seg.publishedOffset());
                    assertTrue(
                            "tornTailBytes must signal non-zero so SegmentRing "
                                    + "can distinguish a corrupt-data segment from an empty "
                                    + "hot-spare leftover; got " + seg.tornTailBytes(),
                            seg.tornTailBytes() > 0L);
                }
                assertTrue("openExisting must not unlink the corrupt file",
                        Files.exists(path));
            } finally {
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
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
        boolean failOnOpenCleanRW;
        int openCleanRWCalls;
        int removeCalls;

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
            return INSTANCE.fsync(fd);
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
