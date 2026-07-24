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
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegmentCorruptionException;
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Regression guards for recovery through positioned file reads. Recovery must
 * consume every byte it validates before mmap creation so sparse or unbacked
 * pages never raise SIGBUS during validation. Sparse holes are ordinary zero-filled
 * reads; negative reads, premature EOF, and file-size changes are operational
 * errors that fail recovery closed without returning a live mapping or mutating
 * the segment.
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
            // Drop the tail blocks, then re-extend logically so
            // [page, SEGMENT_BYTES) is an unbacked hole in the persisted file.
            punchSparseTail(path, page);

            try (MmapSegment seg = MmapSegment.openExisting(path)) {
                assertEquals("the frame below the unbacked tail must be recovered", 1L, seg.frameCount());
                assertEquals("scan must stop at the unbacked-page boundary", page, seg.publishedOffset());
                assertEquals("an unwritten hole is not a torn write", 0L, seg.tornTailBytes());
            }
        });
    }

    /**
     * The harder sparse case: a frame header is backed but its payload reaches
     * into a hole. Positioned reads return zeroes for the hole, so CRC
     * validation rejects that frame without dereferencing the mapping.
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
            final int payloadLen1 = (int) (
                    frame2Offset - MmapSegment.HEADER_SIZE - MmapSegment.FRAME_HEADER_SIZE
            );

            long used = writeSegment(path, 11L, new int[]{payloadLen1, payloadLen2});
            assertEquals(
                    "frame 2's header must end 8 bytes below the page boundary",
                    boundary - 8,
                    frame2Offset + MmapSegment.FRAME_HEADER_SIZE
            );
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
     * A sparse page zero header is positively identified as corrupt from the
     * bytes returned by positioned read and is therefore skippable per-file.
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
                fail("expected corruption for a sparse zero header");
            } catch (MmapSegmentCorruptionException expected) {
                // ok -- SegmentRing's narrow corruption catch skips just this
                // file instead of aborting recovery of the whole slot.
            } catch (MmapSegmentException unexpected) {
                fail("expected quarantinable corruption subtype, got " + unexpected);
            }
        });
    }

    @Test
    public void testLargeFrameRecoveryCrossesReadBuffer() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-large-frame.sfa";
            // Put frame 2's length field across the first 64 KiB boundary,
            // then make its payload span several recovery-buffer refills.
            final int firstPayloadLen = 64 * 1024
                    - MmapSegment.HEADER_SIZE - MmapSegment.FRAME_HEADER_SIZE - 5;
            final int largePayloadLen = 3 * 64 * 1024 + 17;
            assertEquals(64 * 1024 - 5,
                    MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + firstPayloadLen);
            long expectedEnd = writeSegment(path, 13L, new int[]{firstPayloadLen, largePayloadLen, 31});
            try (MmapSegment seg = MmapSegment.openExisting(path)) {
                assertEquals(3L, seg.frameCount());
                assertEquals(expectedEnd, seg.publishedOffset());
                assertEquals(0L, seg.tornTailBytes());
            }
        });
    }

    @Test
    public void testReadErrorFailsClosedBeforeMmap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-read-error.sfa";
            writeSegment(path, 17L, new int[]{256});
            RecoveryReadFacade ff = new RecoveryReadFacade();
            ff.failReadWithError = true;
            ff.stopReadsAt = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 32L;
            try {
                MmapSegment.openExisting(ff, path).close();
                fail("expected positioned-read failure");
            } catch (MmapSegmentException expected) {
                assertFalse("operational read errors must not be quarantinable corruption",
                        expected instanceof MmapSegmentCorruptionException);
                assertTrue(expected.getMessage(), expected.getMessage().contains("could not read"));
            }
            assertEquals("mapping must not start after a failed scan", 0, ff.mmapCalls);
            assertEquals("open descriptor must be closed", 1, ff.closeCalls);
            assertTrue("read failure must not mutate the segment", Files.exists(path));
        });
    }

    @Test
    public void testReadErrorAfterDetectedTornBytesStillFailsClosed() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-torn-then-read-error.sfa";
            long lastGood = writeSegment(path, 18L, new int[]{256});

            // Make the failed-frame header non-zero in the first recovery
            // buffer. Recovery must still read the rest of the suffix so an
            // operational error in a later buffer cannot hide behind the
            // already-established torn-tail signal.
            int fd = Files.openRW(path);
            assertTrue("openRW failed", fd >= 0);
            long marker = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(marker, (byte) 1);
                assertEquals(1L, Files.write(fd, marker, 1L, lastGood));
            } finally {
                Unsafe.free(marker, 1, MemoryTag.NATIVE_DEFAULT);
                Files.close(fd);
            }

            RecoveryReadFacade ff = new RecoveryReadFacade();
            ff.failReadWithError = true;
            ff.stopReadsAt = 64L * 1024L;
            try {
                MmapSegment.openExisting(ff, path).close();
                fail("expected later positioned-read failure");
            } catch (MmapSegmentException expected) {
                assertFalse(expected instanceof MmapSegmentCorruptionException);
                assertTrue(expected.getMessage(), expected.getMessage().contains("could not read"));
            }
            assertTrue("fault must occur after recovery consumed the first buffer", ff.readCalls > 1);
            assertEquals("mapping must not start after any suffix read fails", 0, ff.mmapCalls);
            assertEquals("open descriptor must be closed", 1, ff.closeCalls);
            assertTrue("read failure must not mutate the segment", Files.exists(path));
        });
    }

    @Test
    public void testShortReadFailsClosedBeforeMmap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-short-read.sfa";
            writeSegment(path, 19L, new int[]{256});
            RecoveryReadFacade ff = new RecoveryReadFacade();
            ff.stopReadsAt = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 32L;
            try {
                MmapSegment.openExisting(ff, path).close();
                fail("expected premature EOF failure");
            } catch (MmapSegmentException expected) {
                assertFalse("premature EOF must remain an operational failure",
                        expected instanceof MmapSegmentCorruptionException);
                assertTrue(expected.getMessage(), expected.getMessage().contains("short read"));
            }
            assertEquals("mapping must not start after a failed scan", 0, ff.mmapCalls);
            assertEquals("open descriptor must be closed", 1, ff.closeCalls);
            assertTrue("short read must not mutate the segment", Files.exists(path));
        });
    }

    @Test
    public void testShortReadsAreRetried() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-partial-reads.sfa";
            long expectedEnd = writeSegment(path, 23L, new int[]{31, 127, 4097});
            RecoveryReadFacade ff = new RecoveryReadFacade();
            ff.maxReadSize = 1024;
            try (MmapSegment seg = MmapSegment.openExisting(ff, path)) {
                assertEquals(3L, seg.frameCount());
                assertEquals(expectedEnd, seg.publishedOffset());
            }
            assertTrue("recovery must loop over partial positioned reads", ff.readCalls > 1);
            assertEquals(1, ff.mmapCalls);
            assertEquals(1, ff.closeCalls);
        });
    }

    @Test
    public void testSizeChangeFailsClosedBeforeMmap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-size-change.sfa";
            writeSegment(path, 29L, new int[]{64});
            RecoveryReadFacade ff = new RecoveryReadFacade();
            ff.changeLengthAfterScan = true;
            try {
                MmapSegment.openExisting(ff, path).close();
                fail("expected unstable-size failure");
            } catch (MmapSegmentException expected) {
                assertFalse(expected instanceof MmapSegmentCorruptionException);
                assertTrue(expected.getMessage(), expected.getMessage().contains("size changed"));
            }
            assertEquals("mapping must not start for an unstable file", 0, ff.mmapCalls);
            assertEquals("open descriptor must be closed", 1, ff.closeCalls);
            assertTrue("size-race detection must not mutate the segment", Files.exists(path));
        });
    }

    @Test
    public void testSizeChangeWhileMappingFailsClosedAndUnmaps() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-size-change-while-mapping.sfa";
            writeSegment(path, 31L, new int[]{64});
            RecoveryReadFacade ff = new RecoveryReadFacade();
            ff.changeLengthOnMmap = true;
            try {
                MmapSegment.openExisting(ff, path).close();
                fail("expected size change during mmap to fail recovery");
            } catch (MmapSegmentException expected) {
                assertFalse(expected instanceof MmapSegmentCorruptionException);
                assertTrue(expected.getMessage(), expected.getMessage().contains("size changed while mapping"));
            }
            assertEquals("scan should map only after validation", 1, ff.mmapCalls);
            assertEquals("rejected mapping must be released", 1, ff.munmapCalls);
            assertEquals("open descriptor must be closed", 1, ff.closeCalls);
            assertTrue("injected size observation must not mutate the segment", Files.exists(path));
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
            Unsafe.getUnsafe().setMemory(buf, maxLen, (byte) 1);
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
     * allocating blocks). Positioned recovery reads observe the hole as zeroes
     * without dereferencing it through mmap.
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
     * Positioned-read fault seam. Calls not involved in recovery scanning
     * delegate to the production {@link FilesFacade#INSTANCE}.
     */
    private static final class RecoveryReadFacade implements FilesFacade {
        private boolean changeLengthAfterScan;
        private boolean changeLengthOnMmap;
        private int closeCalls;
        private boolean failReadWithError;
        private int lengthCalls;
        private int maxReadSize = Integer.MAX_VALUE;
        private int mmapCalls;
        private int munmapCalls;
        private int readCalls;
        private long stopReadsAt = Long.MAX_VALUE;

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
            long length = INSTANCE.length(fd);
            lengthCalls++;
            if ((changeLengthAfterScan && lengthCalls > 1)
                    || (changeLengthOnMmap && mmapCalls > 0)) {
                return length - 1L;
            }
            return length;
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
        public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
            mmapCalls++;
            return INSTANCE.mmap(fd, len, offset, flags, memoryTag);
        }

        @Override
        public void munmap(long address, long len, int memoryTag) {
            munmapCalls++;
            INSTANCE.munmap(address, len, memoryTag);
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
            readCalls++;
            if (offset >= stopReadsAt) {
                return failReadWithError ? -1L : 0L;
            }
            long delegatedLen = Math.min(len, (long) maxReadSize);
            if (delegatedLen > stopReadsAt - offset) {
                delegatedLen = stopReadsAt - offset;
            }
            return INSTANCE.read(fd, addr, delegatedLen, offset);
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
