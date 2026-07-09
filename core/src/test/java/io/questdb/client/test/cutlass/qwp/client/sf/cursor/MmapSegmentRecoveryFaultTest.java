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
 * Regression guard for the recovery-time SIGBUS hazard in {@link MmapSegment}.
 * <p>
 * On recovery, {@link MmapSegment#openExisting} maps a persisted {@code .sfa}
 * to its stat length and scans frames straight out of the mapping. When a prior
 * session left a sparse segment tail -- a truncate-based pre-allocation that
 * never materialized the tail blocks, as happens on ZFS -- a read of an
 * unbacked page raises the JVM's recoverable
 * {@code InternalError("...unsafe memory access operation...")} (a translated
 * SIGBUS). Recovery must treat that page as the boundary of recoverable data,
 * keep every frame below it, and hand back a usable segment -- not let the
 * error abort recovery of the whole slot (the reported ZFS-CI flake).
 * <p>
 * These tests drive the <b>production entry point</b> ({@code openExisting}),
 * not the private scan methods via reflection. That matters for two reasons:
 * <ul>
 *   <li>It exercises the real recovery path end to end.</li>
 *   <li>On pre-21 JDKs the mmap-fault {@code InternalError} is delivered
 *       imprecisely ("a fault occurred in <i>a recent</i> unsafe memory access
 *       operation in compiled Java code") and escapes a <i>reflective</i>
 *       {@code Method.invoke} frame instead of being caught inside the scan --
 *       so a reflection-based test spuriously fails on the shipping JDK 8/11/17
 *       even though the direct-call production path catches it fine.</li>
 * </ul>
 * The fault-delivery mechanism the fix rests on was verified directly on the
 * shipping/CI Java floor -- JDK 8 (Temurin 1.8.0_492) -- not merely inferred
 * from the adjacent pre-21 LTS releases: the whole class passes there in both
 * interpreter ({@code -Xint}) and JIT modes, HotSpot emits the exact pre-21
 * message above, and a <i>direct</i> {@code try/catch} catches the fault in
 * interpreter, C1, and C2 modes. {@code isMmapAccessFault}'s shared
 * {@code "unsafe memory access operation"} fragment matches that message while
 * the JDK 21+-only needle it replaced does not -- the guard is live on JDK 8.
 * The unbacked tail is produced portably by truncating the file down (dropping
 * the tail blocks) and back up to the mapping size (leaving a sparse hole). A
 * hole-faulting filesystem (ZFS) then faults on the read exactly as in
 * production -- the case the fix must survive rather than fold the CRC through
 * the native, JNI-side {@code Crc32c} where a SIGBUS is uncatchable and aborts
 * the JVM. A hole-zero-filling filesystem (ext4) instead reads the hole back as
 * zeroes, which fails the frame CRC; either way recovery must stop at the same
 * boundary and recover the same frames.
 * <p>
 * <b>Fail-on-revert on any filesystem.</b> The sparse-hole tests above only
 * fault on ZFS: on ext4/xfs the within-EOF hole zero-fills, so the scan stops
 * via the CRC-mismatch / bad-magic branch and they stay green even with the
 * mmap-fault guard reverted -- no regression protection on the ext4/xfs CI
 * runners. The two {@code MapPastEof} tests below close that gap portably.
 * They truncate the file <em>down</em> (freeing the tail blocks) and hand
 * {@code openExisting} a {@link FilesFacade} that reports the original, larger
 * length, so the mapping extends past real end-of-file. A read of a page beyond
 * real EOF raises SIGBUS on <em>every</em> filesystem -- the same catchable
 * {@code InternalError} an unbacked ZFS page raises -- so they exercise the
 * real fault path (and fail on revert) on ext4/xfs too, not only on ZFS.
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
     * CRC fold therefore reads across the backed-to-unbacked edge. Recovery
     * must reject that frame and keep the one below it -- and, crucially, must
     * do so via {@code Unsafe} reads: the native, JNI-side {@code Crc32c} over
     * an unbacked page raises a SIGBUS that HotSpot cannot translate, aborting
     * the whole JVM (verified: an {@code hs_err} in
     * {@code Java_io_questdb_client_std_Crc32c_update}).
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
     * M1 regression: the header block (magic/version/baseSeq) is read before
     * {@code scanFrames}, so an unbacked page 0 faults ahead of the guarded
     * scan. {@link MmapSegment#openExisting} must surface that as a
     * {@link MmapSegmentException} -- the per-file signal {@code SegmentRing}
     * catches to skip just this {@code .sfa} -- and never let the raw
     * {@code InternalError} escape and abort recovery of every sibling segment.
     * <p>
     * Portable across filesystems: on a hole-faulting FS (ZFS) the fault is
     * converted to a {@code MmapSegmentException} in {@code openExisting}'s
     * catch; on a hole-zero-filling FS (ext4) page 0 reads back as zeroes, so
     * the magic check fails and throws {@code MmapSegmentException} directly.
     * Either way the file is skippable, not fatal.
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
     * Portable fail-on-revert guard for the recovery mmap-fault handling on the
     * scan path. Unlike {@link #testRecoveryKeepsFramesBeforeUnbackedTail}
     * (which only faults on ZFS), this maps the file past real EOF via the
     * length-injecting facade, so the scan's read of the beyond-EOF page faults
     * on ext4/xfs too. The fix must <em>recognize</em> that fault and keep
     * recovery safe -- never a JVM abort, never a raw {@code InternalError}
     * escaping into {@code SegmentRing}'s recovery loop. Revert the
     * {@code scanFrames}/{@code openExisting} mmap-fault guard (or fold the CRC
     * back through native {@code Crc32c}) and this errors or aborts the fork.
     * <p>
     * Two handled outcomes are accepted, because which one occurs depends on
     * whether the recovery methods are JIT-compiled at fault time:
     * <ul>
     *   <li><b>Interpreter / C1:</b> {@code scanFrames}'s own
     *       {@code catch (InternalError)} fires, so the frame below the tear is
     *       recovered and a usable segment is returned.</li>
     *   <li><b>C2:</b> once {@code scanFrames} is inlined into
     *       {@code openExisting}, HotSpot delivers the async unsafe-access
     *       {@code InternalError} to {@code openExisting}'s outer
     *       {@code catch (Throwable)} instead of the inlined inner one, which
     *       converts the file to a skippable {@link MmapSegmentException}.
     *       Still fully handled -- {@code SegmentRing} skips just this
     *       {@code .sfa} rather than aborting the slot.</li>
     * </ul>
     * (The C2 delivery imprecision is a property of HotSpot's async
     * unsafe-access fault handling, not of this seam; the seam only makes it
     * reproducible off ZFS.)
     */
    @Test
    public void testScanFaultOnMapPastEofIsHandledAnyFilesystem() throws Exception {
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
            // Report twice the real length so openExisting maps a second,
            // beyond-EOF page; the scan faults reading it on any filesystem.
            FilesFacade ff = new MapPastEofFacade(path, 2 * page);
            try (MmapSegment seg = MmapSegment.openExisting(ff, path)) {
                // Interpreter / C1: graceful partial recovery.
                assertEquals("the frame below the beyond-EOF page must be recovered", 1L, seg.frameCount());
                assertEquals("scan must stop at the beyond-EOF boundary", page, seg.publishedOffset());
                assertEquals("a beyond-EOF page is not a torn write", 0L, seg.tornTailBytes());
            } catch (MmapSegmentException skippedUnderC2) {
                // C2: the inlined fault escaped to openExisting's outer catch and
                // was converted to a per-file skip. Assert it is the recognized
                // mmap fault (not some other data error) so a revert -- which
                // lets a raw InternalError through instead -- still fails here.
                assertTrue(skippedUnderC2.getMessage(),
                        skippedUnderC2.getMessage().contains("unsafe memory access operation"));
            }
        });
    }

    /**
     * Portable fail-on-revert guard for the {@code openExisting} header-block
     * guard. The file is truncated to empty and the facade reports a full page,
     * so the very first header read (magic) lands on a beyond-EOF page and
     * faults on any filesystem. {@code openExisting} must convert that to a
     * {@link MmapSegmentException} -- the per-file signal {@code SegmentRing}
     * skips on -- not let the raw {@code InternalError} escape and abort recovery
     * of every sibling. Revert the header-block conversion and this throws
     * {@code InternalError} instead of {@code MmapSegmentException}.
     */
    @Test
    public void testHeaderFaultOnMapPastEofIsSkippableAnyFilesystem() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-mappasteof-header.sfa";
            final long page = Files.PAGE_SIZE;
            writeSegment(path, 9L, new int[]{64});
            // Free every block: the file is now empty, so even page 0 (the
            // header) is beyond EOF under the reported one-page mapping.
            truncateTo(path, 0L);
            FilesFacade ff = new MapPastEofFacade(path, page);
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
     * A {@link FilesFacade} that reports an inflated stat length for one target
     * path so {@code openExisting} maps that file past end-of-file (see
     * {@link FilesFacade#length(String)}); every other call, including
     * {@code length} for any other path, delegates to the production
     * {@link FilesFacade#INSTANCE}.
     */
    private static final class MapPastEofFacade implements FilesFacade {
        private final long reportedLength;
        private final String targetPath;

        MapPastEofFacade(String targetPath, long reportedLength) {
            this.targetPath = targetPath;
            this.reportedLength = reportedLength;
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
            return targetPath.equals(path) ? reportedLength : INSTANCE.length(path);
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
