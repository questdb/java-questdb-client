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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Recovery regressions for sparse, short, corrupt, and unreadable segment
 * files. {@link MmapSegment#openExisting} validates bytes through positional
 * reads before mmap, so EOF and I/O failures remain synchronous on every
 * supported HotSpot release and filesystem. Tests exercise the production
 * entry point and assert partial recovery, per-file rejection, and cleanup.
 */
public class MmapSegmentRecoveryFaultTest {

    private static final long SEGMENT_BYTES = 1L << 20;

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-mmap-recover-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDir(tmpDir);
    }

    /**
     * Clean unbacked tail: a single frame ends exactly on a page boundary and
     * everything above it is a sparse hole. Recovery must keep the frame and
     * stop at the boundary, reporting no torn tail (an unwritten hole is not a
     * torn write).
     */
    @Test
    public void testRecoveryKeepsFramesBeforeUnbackedTail() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-unbacked-tail.sfa";
            final long page = Files.PAGE_SIZE;
            // One frame sized so the used region ends exactly on a page
            // boundary: HEADER_SIZE + FRAME_HEADER_SIZE + payload == page.
            final int payloadLen = (int) (page - MmapSegment.HEADER_SIZE - MmapSegment.FRAME_HEADER_SIZE);

            long boundary = writeSegment(path, 7L, new int[]{payloadLen});
            assertEquals("frame must fill exactly one page", page, boundary);
            // Drop the tail blocks, then re-extend logically so [page, SEGMENT_BYTES)
            // is an unbacked hole under the recovery mapping.
            punchSparseTail(path, page);

            try (MmapSegment seg = MmapSegment.openExisting(path)) {
                assertEquals("the frame below the unbacked tail must be recovered", 1L, seg.frameCount());
                assertEquals("scan must stop at the unbacked-page boundary", page, seg.publishedOffset());
                assertEquals("an unwritten hole is not a torn write", 0L, seg.tornTailBytes());
            }
        });
    }

    /**
     * The harder case: a frame whose 8-byte header sits on a backed page but
     * whose payload reaches into the unbacked hole (a torn write leaves a real
     * positive {@code payloadLen} with the payload spanning the boundary). The
     * CRC fold therefore reads across the backed-to-unbacked edge. Positional
     * reads must reject that frame and keep the one below it without exposing
     * the mmap to Java or native CRC code during validation.
     */
    @Test
    public void testRecoverySurvivesPayloadReachingUnbackedPage() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-unbacked-payload.sfa";
            final long page = Files.PAGE_SIZE;
            final long boundary = 2 * page;
            // Frame 2's header ends 8 bytes below the boundary (backed); its
            // payload starts 8 bytes below and runs a full page past -- across
            // the backed->unbacked edge.
            final long frame2Offset = boundary - 16;
            final int payloadLen2 = (int) page;
            final int payloadLen1 = (int) (frame2Offset - MmapSegment.HEADER_SIZE - MmapSegment.FRAME_HEADER_SIZE);

            long used = writeSegment(path, 11L, new int[]{payloadLen1, payloadLen2});
            assertEquals("frame 2's header must end 8 bytes below the page boundary", boundary - 8, frame2Offset + MmapSegment.FRAME_HEADER_SIZE);
            assertTrue("frame 2 payload must reach past the boundary", used > boundary);
            punchSparseTail(path, boundary);

            try (MmapSegment seg = MmapSegment.openExisting(path)) {
                assertEquals("only the frame below the unbacked payload is recoverable", 1L, seg.frameCount());
                assertEquals("scan must stop at the header-backed/payload-unbacked frame",
                        frame2Offset, seg.publishedOffset());
                // Frame 2's header bytes are real (non-zero) and survive the
                // truncate, so the bail-out region is flagged as a torn tail.
                assertTrue("a torn write into the unbacked region must be flagged", seg.tornTailBytes() > 0);
            }
        });
    }

    /**
     * An unbacked page-zero header must produce the per-file
     * {@link MmapSegmentException} that SegmentRing skips, not poison recovery
     * of valid sibling files.
     */
    @Test
    public void testUnbackedHeaderPageIsSkippableNotFatal() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-unbacked-header.sfa";
            writeSegment(path, 3L, new int[]{64});
            // Punch the whole file into a hole -- page 0 (the header) included.
            punchSparseTail(path, 0L);
            try {
                MmapSegment.openExisting(path).close();
                fail("expected MmapSegmentException for an unbacked header page");
            } catch (MmapSegmentException expected) {
                // ok -- SegmentRing's per-file catch skips just this file
                // instead of aborting recovery of the whole slot.
            }
        });
    }

    /**
     * A hard positional-read error rejects only this segment and releases the
     * fd and fixed native recovery buffer on the failure path.
     */
    @Test
    public void testReadErrorRejectsSegmentAndClosesFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-read-error.sfa";
            writeSegment(path, 4L, new int[]{64});
            RecoveryFilesFacade ff = new RecoveryFilesFacade(path, Files.length(path), 0L);
            try {
                MmapSegment.openExisting(ff, path).close();
                fail("expected MmapSegmentException for recovery read error");
            } catch (MmapSegmentException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("read failed"));
            }
            assertEquals("failed recovery must close the segment fd", 1, ff.targetCloseCount());
        });
    }

    @Test
    public void testScanStopsAtShortReadBeforeReportedEof() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-mappasteof-scan.sfa";
            final long page = Files.PAGE_SIZE;
            // One frame that ends exactly on the first page boundary.
            final int payloadLen = (int) (page - MmapSegment.HEADER_SIZE - MmapSegment.FRAME_HEADER_SIZE);
            long boundary = writeSegment(path, 5L, new int[]{payloadLen});
            assertEquals("frame must fill exactly one page", page, boundary);
            // Free every block past the first page: the file is now exactly one
            // (fully backed) page, with nothing beyond it on disk.
            truncateTo(path, page);
            // The first fd-size read reports two pages while pread reaches EOF
            // after one. Recovery must retain the valid frame, re-read the fd
            // size, and map only the real page.
            FilesFacade ff = new RecoveryFilesFacade(path, 2 * page);
            try (MmapSegment seg = MmapSegment.openExisting(ff, path)) {
                assertEquals("the frame below EOF must be recovered", 1L, seg.frameCount());
                assertEquals("scan must stop at EOF", page, seg.publishedOffset());
                assertEquals("a short EOF is not a torn write", 0L, seg.tornTailBytes());
            }
        });
    }

    @Test
    public void testShrinkBelowValidatedPrefixRejectsSegment() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-shrink-after-scan.sfa";
            writeSegment(path, 6L, new int[]{64});
            RecoveryFilesFacade ff = new RecoveryFilesFacade(
                    path,
                    Files.length(path),
                    Long.MAX_VALUE,
                    Long.MAX_VALUE,
                    MmapSegment.HEADER_SIZE
            );
            try {
                MmapSegment.openExisting(ff, path).close();
                fail("expected MmapSegmentException after file shrink");
            } catch (MmapSegmentException expected) {
                assertTrue(expected.getMessage(), expected.getMessage().contains("shrank below recovered data"));
            }
            assertEquals("shrink rejection must close the segment fd", 1, ff.targetCloseCount());
        });
    }

    @Test
    public void testShortReadsAcrossFrameBoundariesRecoverAllFrames() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-short-reads.sfa";
            long expectedOffset = writeSegment(path, 7L, new int[]{1, 17, 64});
            RecoveryFilesFacade ff = new RecoveryFilesFacade(
                    path,
                    Files.length(path),
                    Long.MAX_VALUE,
                    3L
            );
            try (MmapSegment segment = MmapSegment.openExisting(ff, path)) {
                assertEquals(3L, segment.frameCount());
                assertEquals(expectedOffset, segment.publishedOffset());
                assertEquals(0L, segment.tornTailBytes());
            }
        });
    }

    @Test
    public void testSuccessfulRecoveryClosesThroughFacadeExactlyOnce() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-successful-facade-close.sfa";
            writeSegment(path, 12L, new int[]{64});
            RecoveryFilesFacade ff = new RecoveryFilesFacade(path, Files.length(path));
            MmapSegment segment = MmapSegment.openExisting(ff, path);
            assertEquals("successful recovery must not close before segment ownership ends",
                    0, ff.targetCloseCount());
            segment.close();
            segment.close();
            assertEquals("successful recovery must close through its facade exactly once",
                    1, ff.targetCloseCount());
        });
    }

    /**
     * A header short-read produces the per-file exception that SegmentRing
     * skips; recovery never maps the stale reported length.
     */
    @Test
    public void testHeaderShortReadIsSkippable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-mappasteof-header.sfa";
            final long page = Files.PAGE_SIZE;
            writeSegment(path, 9L, new int[]{64});
            // The facade reports a page on the first fd-size read, while the
            // positional header read sees the real empty file.
            truncateTo(path, 0L);
            FilesFacade ff = new RecoveryFilesFacade(path, page);
            try {
                MmapSegment.openExisting(ff, path).close();
                fail("expected MmapSegmentException for a short header read");
            } catch (MmapSegmentException expected) {
                assertTrue(
                        "the fd-length seam must reach the positional header read: " + expected.getMessage(),
                        expected.getMessage().contains("short read of segment header")
                );
            }
        });
    }

    /**
     * Creates a segment at {@code path} and appends one frame per entry in
     * {@code payloadLens} (each filled with non-zero bytes so recovery can tell
     * written data from an unwritten/zeroed hole). Returns the used byte count
     * (the published offset after the last append).
     */
    private static long writeSegment(String path, long baseSeq, int[] payloadLens) {
        int maxLen = 0;
        for (int len : payloadLens) {
            maxLen = Math.max(maxLen, len);
        }
        long buf = Unsafe.malloc(maxLen, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < maxLen; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) (i | 1)); // all non-zero
            }
            try (MmapSegment seg = MmapSegment.create(path, baseSeq, SEGMENT_BYTES)) {
                for (int len : payloadLens) {
                    assertTrue("append must fit", seg.tryAppend(buf, len) >= 0);
                }
                return seg.publishedOffset();
            }
        } finally {
            Unsafe.free(buf, maxLen, MemoryTag.NATIVE_DEFAULT);
        }
    }

    /**
     * Turns {@code [keepBytes, SEGMENT_BYTES)} of the file into an unbacked
     * sparse hole: truncate down to {@code keepBytes} (frees the tail blocks),
     * then back up to {@code SEGMENT_BYTES} (re-extends the logical size without
     * allocating blocks). Recovery maps the full stat length, so the hole is
     * inside the mapping -- reads of it fault on ZFS and zero-fill on ext4.
     */
    private static void punchSparseTail(String path, long keepBytes) {
        int fd = Files.openRW(path);
        assertTrue("openRW failed", fd >= 0);
        try {
            assertTrue("truncate down failed", Files.truncate(fd, keepBytes));
            assertTrue("truncate up failed", Files.truncate(fd, SEGMENT_BYTES));
        } finally {
            Files.close(fd);
        }
    }

    /**
     * Shrinks the file to {@code keepBytes}, freeing every block past it, and
     * leaves it there (no re-extend). Combined with a facade that reports a
     * larger length, the freed region becomes a beyond-EOF part of the mapping
     * that faults on read on any filesystem.
     */
    private static void truncateTo(String path, long keepBytes) {
        int fd = Files.openRW(path);
        assertTrue("openRW failed", fd >= 0);
        try {
            assertTrue("truncate failed", Files.truncate(fd, keepBytes));
        } finally {
            Files.close(fd);
        }
    }

    /**
     * Recovery fault seam. The first fd-size read can report a stale larger
     * value, subsequent reads return the real size, and positional reads may
     * inject a hard error at a selected offset. All unrelated operations
     * delegate to {@link FilesFacade#INSTANCE}.
     */
    private static final class RecoveryFilesFacade implements FilesFacade {
        private final long failReadAtOffset;
        private final long maxReadSize;
        private final long reportedLength;
        private final long shrinkOnSecondLengthTo;
        private final String targetPath;
        private int lengthCallCount;
        private int targetCloseCount;
        private int targetFd = -1;

        RecoveryFilesFacade(String targetPath, long reportedLength) {
            this(targetPath, reportedLength, Long.MAX_VALUE, Long.MAX_VALUE, -1L);
        }

        RecoveryFilesFacade(String targetPath, long reportedLength, long failReadAtOffset) {
            this(targetPath, reportedLength, failReadAtOffset, Long.MAX_VALUE, -1L);
        }

        RecoveryFilesFacade(
                String targetPath,
                long reportedLength,
                long failReadAtOffset,
                long maxReadSize
        ) {
            this(targetPath, reportedLength, failReadAtOffset, maxReadSize, -1L);
        }

        RecoveryFilesFacade(
                String targetPath,
                long reportedLength,
                long failReadAtOffset,
                long maxReadSize,
                long shrinkOnSecondLengthTo
        ) {
            this.targetPath = targetPath;
            this.reportedLength = reportedLength;
            this.failReadAtOffset = failReadAtOffset;
            this.maxReadSize = maxReadSize;
            this.shrinkOnSecondLengthTo = shrinkOnSecondLengthTo;
        }

        @Override
        public boolean allocate(int fd, long size) {
            return INSTANCE.allocate(fd, size);
        }

        @Override
        public long allocNativePath(String path) {
            return INSTANCE.allocNativePath(path);
        }

        @Override
        public int close(int fd) {
            int result = INSTANCE.close(fd);
            if (fd == targetFd) {
                targetCloseCount++;
                targetFd = -1;
            }
            return result;
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
            if (fd == targetFd) {
                if (lengthCallCount++ == 0) {
                    return reportedLength;
                }
                if (shrinkOnSecondLengthTo >= 0L) {
                    assertTrue("injected truncate failed", INSTANCE.truncate(fd, shrinkOnSecondLengthTo));
                }
            }
            return INSTANCE.length(fd);
        }

        @Override
        public long length(long pathPtr) {
            return INSTANCE.length(pathPtr);
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
            return INSTANCE.openCleanRW(path);
        }

        @Override
        public int openCleanRW(long pathPtr) {
            return INSTANCE.openCleanRW(pathPtr);
        }

        @Override
        public int openRW(String path) {
            int fd = INSTANCE.openRW(path);
            if (targetPath.equals(path)) {
                targetFd = fd;
            }
            return fd;
        }

        @Override
        public int openRW(long pathPtr) {
            return INSTANCE.openRW(pathPtr);
        }

        @Override
        public long read(int fd, long addr, long len, long offset) {
            if (fd == targetFd && offset >= failReadAtOffset) {
                return -1L;
            }
            return INSTANCE.read(fd, addr, Math.min(len, maxReadSize), offset);
        }

        @Override
        public boolean remove(String path) {
            return INSTANCE.remove(path);
        }

        @Override
        public boolean remove(long pathPtr) {
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

        int targetCloseCount() {
            return targetCloseCount;
        }

        @Override
        public long write(int fd, long addr, long len, long offset) {
            return INSTANCE.write(fd, addr, len, offset);
        }
    }
}
