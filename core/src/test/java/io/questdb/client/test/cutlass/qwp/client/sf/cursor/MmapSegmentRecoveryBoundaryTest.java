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
 * Regression guard for recovery when part of a persisted {@code .sfa} is not
 * readable data: a sparse hole (a truncate-based pre-allocation that never
 * materialized the tail blocks, as on ZFS), a file shorter than its recorded
 * length (size metadata that survived a crash its data blocks did not, or a
 * truncation under recovery), or a failing read (bad sector).
 * <p>
 * {@link MmapSegment#openExisting} reads its entire recovery scan through
 * {@code pread} into a private buffer — never through the mapping — so each of
 * those cases is an ordinary read result with one deterministic outcome per
 * input: a hole reads back as zeros and fails the frame CRC, a region past
 * real end-of-file gives a short read, and an I/O error gives a failed read.
 * Recovery must treat all three as the boundary of recoverable data — keep
 * every frame below it and either hand back a usable segment or throw the
 * per-file {@link MmapSegmentException} that {@code SegmentRing} skips on —
 * and the outcome must be the same on every filesystem, JDK, and JIT state.
 * (Reading the same regions through the mapping would instead raise SIGBUS,
 * which HotSpot converts to an {@code InternalError} delivered at an imprecise
 * point under a JIT-compiled caller — the JDK 8 CI flake this suite guards
 * against reintroducing.)
 * <p>
 * The suite also guards the fully-readable side of the same machinery: a file
 * larger than the scan's pread window, whose recovery slides the window
 * forward and checks one frame's CRC in chunks spanning several window loads.
 * <p>
 * These tests drive the production entry point ({@code openExisting}), not
 * private scan methods via reflection, so they exercise the real recovery
 * path end to end.
 */
public class MmapSegmentRecoveryBoundaryTest {

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
     * everything above it is a sparse hole. The hole preads back as zeros,
     * which fails the next frame's CRC, so recovery keeps the frame and stops
     * at the boundary, reporting no torn tail (an unwritten hole is not a
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
            // is an unbacked hole inside the file's recorded length.
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
     * CRC check therefore reads bytes on both sides of the backed-to-hole
     * edge: the backed bytes are real, the hole preads as zeros, the CRC
     * mismatches, and recovery rejects that frame while keeping the one
     * below it. The surviving non-zero header bytes at the bail-out position
     * are flagged as a torn tail.
     */
    @Test
    public void testRecoverySurvivesPayloadReachingUnbackedPage() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-unbacked-payload.sfa";
            final long page = Files.PAGE_SIZE;
            final long boundary = 2 * page;
            // Frame 2's header ends 8 bytes below the boundary (backed); its
            // payload starts 8 bytes below and runs a full page past -- across
            // the backed->hole edge.
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
     * The header block (magic/version/baseSeq) is read before the frame scan,
     * so a file whose page 0 is a hole must fail cleanly: the header preads
     * back as zeros, the magic check fails, and {@link MmapSegment#openExisting}
     * throws {@link MmapSegmentException} -- the per-file signal
     * {@code SegmentRing} catches to skip just this {@code .sfa} -- rather
     * than anything that could abort recovery of every sibling segment.
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
     * A file shorter than its recorded length: the facade reports twice the
     * real length, so the scan's pread of the second page comes back short.
     * Recovery must treat the short read as the boundary of recoverable data
     * -- keep the frame below it, report no torn tail (nothing readable was
     * ever written there), and hand back a usable segment. One deterministic
     * outcome on every filesystem; this is the shape a crash leaves when the
     * size metadata survives but the tail's data blocks do not, or a
     * concurrent truncation during recovery.
     */
    @Test
    public void testScanPastRealEofStopsAtBoundaryAnyFilesystem() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-pasteof-scan.sfa";
            final long page = Files.PAGE_SIZE;
            // One frame that ends exactly on the first page boundary.
            final int payloadLen = (int) (page - MmapSegment.HEADER_SIZE - MmapSegment.FRAME_HEADER_SIZE);
            long boundary = writeSegment(path, 5L, new int[]{payloadLen});
            assertEquals("frame must fill exactly one page", page, boundary);
            // Free every block past the first page: the file is now exactly one
            // (fully backed) page, with nothing beyond it on disk.
            truncateTo(path, page);
            // Report twice the real length so the scan preads a second,
            // beyond-EOF page; the read comes back short on any filesystem.
            FilesFacade ff = new RecoverySeamFacade(path, 2 * page, -1L);
            try (MmapSegment seg = MmapSegment.openExisting(ff, path)) {
                assertEquals("the frame below the beyond-EOF page must be recovered", 1L, seg.frameCount());
                assertEquals("scan must stop at the beyond-EOF boundary", page, seg.publishedOffset());
                assertEquals("a beyond-EOF region is not a torn write", 0L, seg.tornTailBytes());
            }
        });
    }

    /**
     * The degenerate shorter-than-recorded case: the file is truncated to
     * empty and the facade reports a full page, so the very first header pread
     * comes back empty. {@link MmapSegment#openExisting} must surface that as
     * a {@link MmapSegmentException} -- the per-file signal {@code SegmentRing}
     * skips on -- so one lost header never aborts recovery of every sibling.
     */
    @Test
    public void testHeaderPastRealEofIsSkippableAnyFilesystem() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-pasteof-header.sfa";
            final long page = Files.PAGE_SIZE;
            writeSegment(path, 9L, new int[]{64});
            // Free every block: the file is now empty, so even page 0 (the
            // header) is beyond real EOF under the reported one-page length.
            truncateTo(path, 0L);
            FilesFacade ff = new RecoverySeamFacade(path, page, -1L);
            try {
                MmapSegment.openExisting(ff, path).close();
                fail("expected MmapSegmentException for a beyond-EOF header page");
            } catch (MmapSegmentException expected) {
                // ok -- SegmentRing's per-file catch skips just this file
                // instead of aborting recovery of the whole slot.
            }
        });
    }

    /**
     * A failing read mid-scan (the bad-sector case): reads below the failure
     * offset succeed, reads at or past it fail. Recovery must treat the
     * failure as the boundary of recoverable data -- keep the frames below
     * it, report no torn tail (the region cannot be probed), and hand back a
     * usable segment rather than aborting the slot.
     */
    @Test
    public void testReadErrorDuringScanEndsRecoveryAtBoundary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-read-error.sfa";
            final long page = Files.PAGE_SIZE;
            final int payloadLen = (int) (page - MmapSegment.HEADER_SIZE - MmapSegment.FRAME_HEADER_SIZE);
            long boundary = writeSegment(path, 13L, new int[]{payloadLen, 64});
            assertTrue("second frame must sit past the first page", boundary > page);
            // Reads below `page` succeed (truncated at the bad region's edge,
            // like a real pread up against a bad sector); reads at or past
            // `page` fail.
            FilesFacade ff = new RecoverySeamFacade(path, -1L, page);
            try (MmapSegment seg = MmapSegment.openExisting(ff, path)) {
                assertEquals("the frame below the unreadable region must be recovered", 1L, seg.frameCount());
                assertEquals("scan must stop at the unreadable region", page, seg.publishedOffset());
                assertEquals("an unreadable region cannot be probed for a torn write", 0L, seg.tornTailBytes());
            }
        });
    }

    /**
     * The multi-window scan: a segment larger than the recovery read window
     * (1 MiB), holding a frame whose CRC span -- the (payloadLen, payload)
     * pair -- is itself larger than the window, followed by a small frame.
     * Verifying the large frame forces the scan to compute its CRC
     * incrementally -- reloading the window several times and feeding each
     * chunk into the running value, which chained {@code Crc32c.update} calls
     * make bit-identical to {@code tryAppend}'s one-pass CRC -- and the small
     * frame is then verified out of a repositioned window. Every byte is
     * readable, so recovery must be total: both frames kept, the cursor at
     * the last appended byte, no torn tail.
     */
    @Test
    public void testScanRecoversFrameLargerThanReadWindow() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-multi-window.sfa";
            // 3 MiB of payload in a 4 MiB segment: checking the large frame's
            // CRC reloads the window four times, and the scan as a whole
            // takes multiple preads. (Sized against
            // MmapSegment.RECOVERY_BUF_BYTES = 1 MiB; keep the payload larger
            // than that constant or this test degrades to a single-window
            // scan.)
            final int largeLen = 3 * (1 << 20);
            final long segmentBytes = 4L * (1 << 20);
            long used = writeSegment(path, 17L, new int[]{largeLen, 64}, segmentBytes);

            try (MmapSegment seg = MmapSegment.openExisting(path)) {
                assertEquals("both frames must be recovered", 2L, seg.frameCount());
                assertEquals("scan must recover up to the last appended byte", used, seg.publishedOffset());
                assertEquals("a fully recovered segment has no torn tail", 0L, seg.tornTailBytes());
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
        return writeSegment(path, baseSeq, payloadLens, SEGMENT_BYTES);
    }

    /**
     * Variant of {@link #writeSegment(String, long, int[])} with an explicit
     * segment size, for tests whose file must exceed the recovery scan's
     * 1 MiB pread window.
     */
    private static long writeSegment(String path, long baseSeq, int[] payloadLens, long segmentBytes) {
        int maxLen = 0;
        for (int len : payloadLens) {
            maxLen = Math.max(maxLen, len);
        }
        long buf = Unsafe.malloc(maxLen, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < maxLen; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) (i | 1)); // all non-zero
            }
            try (MmapSegment seg = MmapSegment.create(path, baseSeq, segmentBytes)) {
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
     * allocating blocks). Recovery preads the full stat length, so the hole is
     * inside the scanned range -- it reads back as zeros on every filesystem.
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
     * larger length, the freed region is past real end-of-file, so recovery's
     * preads of it come back short on any filesystem.
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
     * A {@link FilesFacade} exposing the two recovery seams for one target
     * path; every other call, including calls for any other path, delegates to
     * the production {@link FilesFacade#INSTANCE}.
     * <ul>
     *   <li>{@code reportedLength >= 0}: {@code length(String)} reports this
     *       value instead of the real stat length, so recovery scans past real
     *       end-of-file and its preads come back short (see
     *       {@link FilesFacade#length(String)}).</li>
     *   <li>{@code failReadsFromOffset >= 0}: reads below the offset succeed
     *       but are truncated at its edge (like a pread up against a bad
     *       sector); reads at or past it fail with {@code -1}.</li>
     * </ul>
     */
    private static final class RecoverySeamFacade implements FilesFacade {
        private final long failReadsFromOffset;
        private final long reportedLength;
        private final String targetPath;

        RecoverySeamFacade(String targetPath, long reportedLength, long failReadsFromOffset) {
            this.targetPath = targetPath;
            this.reportedLength = reportedLength;
            this.failReadsFromOffset = failReadsFromOffset;
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
        public long length(long pathPtr) {
            return INSTANCE.length(pathPtr);
        }

        @Override
        public long length(String path) {
            return reportedLength >= 0 && targetPath.equals(path)
                    ? reportedLength
                    : INSTANCE.length(path);
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
            return INSTANCE.openRW(path);
        }

        @Override
        public int openRW(long pathPtr) {
            return INSTANCE.openRW(pathPtr);
        }

        @Override
        public long read(int fd, long addr, long len, long offset) {
            if (failReadsFromOffset >= 0) {
                if (offset >= failReadsFromOffset) {
                    return -1;
                }
                len = Math.min(len, failReadsFromOffset - offset);
            }
            return INSTANCE.read(fd, addr, len, offset);
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

        @Override
        public long write(int fd, long addr, long len, long offset) {
            return INSTANCE.write(fd, addr, len, offset);
        }
    }
}
