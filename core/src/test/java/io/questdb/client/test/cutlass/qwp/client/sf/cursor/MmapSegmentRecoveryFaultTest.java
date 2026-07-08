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
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Regression guard for the recovery-time SIGBUS hazard in {@link MmapSegment}.
 * <p>
 * {@code openExisting} maps a recovered {@code .sfa} to its stat length and
 * {@code scanFrames} / {@code detectTornTail} read the mapping directly. When a
 * prior session left a sparse segment tail -- a truncate-based pre-allocation
 * that never materialized the tail blocks, as happens on ZFS -- a read of an
 * unbacked page raises the JVM's recoverable
 * {@code InternalError("a fault occurred in an unsafe memory access
 * operation")} (a translated SIGBUS). That error is NOT a
 * {@code MmapSegmentException}, so {@code SegmentRing.openExisting}'s per-file
 * skip did not catch it: it aborted recovery of the whole slot, which surfaced
 * (via a drainer/probe) as a spurious "unsafe memory access" failure on ZFS
 * CI runners.
 * <p>
 * The fault only reproduces on a real filesystem whose mmap reads of unwritten
 * regions fault instead of zero-filling (ZFS), so this test induces the very
 * same JVM-recoverable fault deterministically on any filesystem: it maps a
 * valid segment file, then truncates the backing file under the still-larger
 * mapping so the tail page is genuinely beyond EOF (the one case POSIX mmap
 * always faults on). The scan must then stop at that page and keep every frame
 * below it, exactly as it treats a torn tail.
 */
public class MmapSegmentRecoveryFaultTest {

    private static final long SEGMENT_BYTES = 1L << 20;

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-mmap-recover-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) {
            return;
        }
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
    public void testRecoveryScanTreatsUnbackedTailAsBoundary() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final String path = tmpDir + "/seg-unbacked-tail.sfa";
            // One frame sized so the segment's used region ends exactly on a
            // 4 KiB page boundary: HEADER_SIZE + FRAME_HEADER_SIZE + payload.
            // Truncating the backing to that boundary then leaves the NEXT
            // page entirely beyond EOF -- the deterministic mmap-fault case.
            final long pageBoundary = 4096L;
            final int payloadLen = (int) (pageBoundary - MmapSegment.HEADER_SIZE - MmapSegment.FRAME_HEADER_SIZE);
            long boundary;
            long buf = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < payloadLen; i++) {
                    Unsafe.getUnsafe().putByte(buf + i, (byte) (i | 1)); // all non-zero
                }
                try (MmapSegment seg = MmapSegment.create(path, 7L, SEGMENT_BYTES)) {
                    assertEquals(MmapSegment.HEADER_SIZE, seg.tryAppend(buf, payloadLen));
                    boundary = seg.publishedOffset();
                }
            } finally {
                Unsafe.free(buf, payloadLen, MemoryTag.NATIVE_DEFAULT);
            }
            assertEquals("frame must fill exactly one page", pageBoundary, boundary);

            // Map the whole segment, then shrink the backing file under the
            // mapping so [boundary, SEGMENT_BYTES) is unbacked. Reads within
            // [0, boundary) stay valid; a read at/after `boundary` faults with
            // the same recoverable InternalError a sparse ZFS tail produces.
            int fd = Files.openRW(path);
            assertTrue("openRW failed", fd >= 0);
            long addr = Files.mmap(fd, SEGMENT_BYTES, 0, Files.MAP_RW, MemoryTag.MMAP_DEFAULT);
            assertTrue("mmap failed", addr != Files.FAILED_MMAP_ADDRESS);
            try {
                assertTrue("truncate failed", Files.truncate(fd, boundary));

                // scanFrames must keep the frame below the unbacked page and
                // return the boundary rather than propagating the fault.
                Method scanFrames = MmapSegment.class.getDeclaredMethod(
                        "scanFrames", long.class, long.class);
                scanFrames.setAccessible(true);
                long lastGood = (Long) scanFrames.invoke(null, addr, SEGMENT_BYTES);
                assertEquals("scan must stop at the unbacked-page boundary", boundary, lastGood);

                // detectTornTail probes the bail-out region -- itself unbacked
                // here -- and must report clean (0), not a fatal fault.
                Method detectTornTail = MmapSegment.class.getDeclaredMethod(
                        "detectTornTail", long.class, long.class, long.class);
                detectTornTail.setAccessible(true);
                long torn = (Long) detectTornTail.invoke(null, addr, lastGood, SEGMENT_BYTES);
                assertEquals("unbacked tail is unwritten space, not a torn write", 0L, torn);
            } finally {
                Files.munmap(addr, SEGMENT_BYTES, MemoryTag.MMAP_DEFAULT);
                Files.close(fd);
            }
        });
    }
}
