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

import io.questdb.client.cutlass.qwp.client.sf.cursor.AckWatermark;
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AckWatermarkTest {

    private String slotDir;

    @Before
    public void setUp() {
        slotDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-ackwatermark-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(slotDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (slotDir == null) return;
        Files.remove(slotDir + "/" + AckWatermark.FILE_NAME);
        Files.remove(slotDir);
    }

    @Test
    public void testCrossSessionPersistence() throws Exception {
        // Open, write, close (simulates a sender session). Then open
        // again (simulates a recovery in a fresh process) and observe
        // the previously-written value.
        TestUtils.assertMemoryLeak(() -> {
            try (AckWatermark w = AckWatermark.open(slotDir)) {
                assertNotNull(w);
                w.write(12_345L);
            }
            assertTrue("watermark file must persist across close",
                    Files.exists(slotDir + "/" + AckWatermark.FILE_NAME));
            try (AckWatermark w2 = AckWatermark.open(slotDir)) {
                assertNotNull(w2);
                assertEquals("recovered value must match written value",
                        12_345L, w2.read());
            }
        });
    }

    @Test
    public void testFacadeMmapFaultFailsOpenAndClosesFd() throws Exception {
        // The watermark mapping must be reachable from an injected facade so
        // tests can fault-inject mmap. On a rejected mapping, open() must
        // fail cleanly and release the fd through the same facade.
        TestUtils.assertMemoryLeak(() -> {
            MappingFilesFacade ff = new MappingFilesFacade(true);
            assertNull("open must fail when the injected facade rejects mmap",
                    AckWatermark.open(ff, slotDir));
            assertEquals("facade must receive the watermark mmap call", 1, ff.mmapCalls);
            assertEquals("failed open must close the watermark fd via the facade",
                    1, ff.watermarkFdCloseCalls);
            assertEquals("a rejected mapping must not be munmapped", 0, ff.munmapCalls);
        });
    }

    @Test
    public void testFallsBackFromTornPlausibleHighValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (AckWatermark watermark = AckWatermark.open(slotDir)) {
                assertNotNull(watermark);
                watermark.write(255L);
                watermark.write(256L);
                watermark.sync();
            }

            // Model a torn little-endian 255 -> 256 transition: the high byte
            // reached storage while the low byte retained 0xff, yielding the
            // false FSN 511. A recovered ring with publishedFsn=599 cannot
            // reject that value using its segment ceiling.
            long publishedFsn = 599L;
            long corruptFsn = 511L;
            assertTrue(corruptFsn <= publishedFsn);
            String path = slotDir + "/" + AckWatermark.FILE_NAME;
            int fd = Files.openRW(path);
            assertTrue(fd >= 0);
            long corruptByte = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(corruptByte, (byte) 0xff);
                long fsnOffset = AckWatermark.FILE_SIZE == 16 ? 8L : 16L;
                assertEquals(1L, Files.write(fd, corruptByte, 1, fsnOffset));
                assertEquals(0, Files.fsync(fd));
            } finally {
                Files.close(fd);
                Unsafe.free(corruptByte, 1, MemoryTag.NATIVE_DEFAULT);
            }

            try (AckWatermark watermark = AckWatermark.open(slotDir)) {
                assertNotNull(watermark);
                assertEquals("torn latest record must fall back to the older watermark",
                        255L, watermark.read());
            }
        });
    }

    @Test
    public void testFreshFileReadsAsInvalid() throws Exception {
        // open() creates the file zero-filled, so magic is 0 and read()
        // must report INVALID until the first write stamps the magic.
        TestUtils.assertMemoryLeak(() -> {
            try (AckWatermark w = AckWatermark.open(slotDir)) {
                assertNotNull(w);
                assertEquals(AckWatermark.INVALID, w.read());
            }
        });
    }

    @Test
    public void testNegativeFsnRoundTrips() throws Exception {
        // Engine seeds use -1 in the no-prior-history case. A
        // persisted -1 should round-trip so recovery can pick the
        // right seed.
        TestUtils.assertMemoryLeak(() -> {
            try (AckWatermark w = AckWatermark.open(slotDir)) {
                assertNotNull(w);
                w.write(-1L);
                assertEquals(-1L, w.read());
            }
        });
    }

    @Test
    public void testOpenAndCloseRouteMappingThroughFacade() throws Exception {
        // The lifetime mapping and its release must go through the injected
        // FilesFacade, matching MmapSegment, so test facades can observe them.
        TestUtils.assertMemoryLeak(() -> {
            MappingFilesFacade ff = new MappingFilesFacade(false);
            try (AckWatermark w = AckWatermark.open(ff, slotDir)) {
                assertNotNull(w);
                assertEquals("open must mmap through the injected facade", 1, ff.mmapCalls);
                w.write(7L);
                assertEquals(7L, w.read());
            }
            assertEquals("close must munmap through the injected facade", 1, ff.munmapCalls);
            assertEquals("close must release the watermark fd via the facade",
                    1, ff.watermarkFdCloseCalls);
        });
    }

    @Test
    public void testPhysicalReleaseIsIdempotentAcrossTestingSeamAndClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AckWatermark watermark = AckWatermark.open(slotDir);
            assertNotNull(watermark);
            try {
                assertTrue("first test release must relinquish physical storage",
                        watermark.releaseStorageButKeepWritableForTesting());
                assertFalse("repeated test release must not touch physical storage again",
                        watermark.releaseStorageButKeepWritableForTesting());
                watermark.close();
                assertFalse("close after test release must keep physical cleanup idempotent",
                        watermark.releaseStorageButKeepWritableForTesting());
            } finally {
                watermark.close();
            }

            AckWatermark normallyClosed = AckWatermark.open(slotDir);
            assertNotNull(normallyClosed);
            try {
                normallyClosed.close();
                assertFalse("ordinary close must record physical relinquishment",
                        normallyClosed.releaseStorageButKeepWritableForTesting());
            } finally {
                normallyClosed.close();
            }
        });
    }

    @Test
    public void testRemoveOrphanDeletesFile() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (AckWatermark w = AckWatermark.open(slotDir)) {
                assertNotNull(w);
                w.write(42L);
            }
            String path = slotDir + "/" + AckWatermark.FILE_NAME;
            assertTrue("write+close must leave file in place", Files.exists(path));
            AckWatermark.removeOrphan(slotDir);
            assertFalse("removeOrphan must delete the file", Files.exists(path));
            // Idempotent: second remove on missing file must not throw.
            AckWatermark.removeOrphan(slotDir);
        });
    }

    @Test
    public void testRepeatedWriteUpdatesValue() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (AckWatermark w = AckWatermark.open(slotDir)) {
                assertNotNull(w);
                w.write(100L);
                assertEquals(100L, w.read());
                w.write(200L);
                assertEquals(200L, w.read());
                w.write(Long.MAX_VALUE);
                assertEquals(Long.MAX_VALUE, w.read());
            }
        });
    }

    @Test
    public void testSingleSectorTearLeavesPriorRecordRecoverable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (AckWatermark watermark = AckWatermark.open(slotDir)) {
                assertNotNull(watermark);
                watermark.write(100L);
                watermark.write(200L);
                watermark.sync();
            }

            String path = slotDir + "/" + AckWatermark.FILE_NAME;
            int fd = Files.openRW(path);
            assertTrue(fd >= 0);
            int tornBytes = (int) Math.min(512L, Files.length(path));
            long tornSector = Unsafe.malloc(tornBytes, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().setMemory(tornSector, tornBytes, (byte) 0xA5);
                assertEquals(tornBytes, Files.write(fd, tornSector, tornBytes, 0));
                assertEquals(0, Files.fsync(fd));
            } finally {
                Files.close(fd);
                Unsafe.free(tornSector, tornBytes, MemoryTag.NATIVE_DEFAULT);
            }

            try (AckWatermark watermark = AckWatermark.open(slotDir)) {
                assertNotNull(watermark);
                assertEquals("one aligned 512-byte tear must leave the prior record valid",
                        100L, watermark.read());
            }
        });
    }

    @Test
    public void testStaleFileWithWrongSizeIsResetOnOpen() throws Exception {
        // A leftover file with an unexpected size (corruption, partial
        // write from an older format, manual tampering) must not poison
        // recovery. open() detects the wrong size and truncates to the
        // expected layout — read() then reports INVALID until the next
        // write.
        TestUtils.assertMemoryLeak(() -> {
            String path = slotDir + "/" + AckWatermark.FILE_NAME;
            int fd = Files.openCleanRW(path);
            assertTrue(fd >= 0);
            assertTrue(Files.truncate(fd, 4));
            Files.close(fd);
            assertEquals("precondition: file exists at wrong size",
                    4L, Files.length(path));

            try (AckWatermark w = AckWatermark.open(slotDir)) {
                assertNotNull("open must succeed despite wrong-sized stale file", w);
                assertEquals("stale wrong-sized file must read as INVALID",
                        AckWatermark.INVALID, w.read());
                w.write(777L);
                assertEquals(777L, w.read());
            }
            assertEquals("after open+write, file must be the expected size",
                    AckWatermark.FILE_SIZE, Files.length(path));
        });
    }

    @Test
    public void testWriteReadInSameSession() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (AckWatermark w = AckWatermark.open(slotDir)) {
                assertNotNull(w);
                w.write(0L);
                assertEquals(0L, w.read());
            }
        });
    }

    /**
     * Delegating facade that counts mmap/munmap traffic and watermark-fd
     * closes, optionally rejecting the mapping to exercise the open()
     * failure path.
     */
    private static final class MappingFilesFacade implements FilesFacade {
        private final boolean failMmap;
        private int mmapCalls;
        private int munmapCalls;
        private int watermarkFd = -1;
        private int watermarkFdCloseCalls;

        private MappingFilesFacade(boolean failMmap) {
            this.failMmap = failMmap;
        }

        @Override public boolean allocate(int fd, long size) { return INSTANCE.allocate(fd, size); }
        @Override public long allocNativePath(String path) { return INSTANCE.allocNativePath(path); }
        @Override public int close(int fd) {
            if (fd >= 0 && fd == watermarkFd) watermarkFdCloseCalls++;
            return INSTANCE.close(fd);
        }
        @Override public boolean exists(String path) { return INSTANCE.exists(path); }
        @Override public void findClose(long findPtr) { INSTANCE.findClose(findPtr); }
        @Override public long findFirst(String dir) { return INSTANCE.findFirst(dir); }
        @Override public long findName(long findPtr) { return INSTANCE.findName(findPtr); }
        @Override public int findNext(long findPtr) { return INSTANCE.findNext(findPtr); }
        @Override public int findType(long findPtr) { return INSTANCE.findType(findPtr); }
        @Override public void freeNativePath(long pathPtr) { INSTANCE.freeNativePath(pathPtr); }
        @Override public int fsync(int fd) { return INSTANCE.fsync(fd); }
        @Override public long length(int fd) { return INSTANCE.length(fd); }
        @Override public long length(String path) { return INSTANCE.length(path); }
        @Override public long length(long pathPtr) { return INSTANCE.length(pathPtr); }
        @Override public int lock(int fd) { return INSTANCE.lock(fd); }
        @Override public int mkdir(String path, int mode) { return INSTANCE.mkdir(path, mode); }
        @Override public long mmap(int fd, long len, long offset, int flags, int memoryTag) {
            mmapCalls++;
            if (failMmap) return Files.FAILED_MMAP_ADDRESS;
            return INSTANCE.mmap(fd, len, offset, flags, memoryTag);
        }
        @Override public void munmap(long address, long len, int memoryTag) {
            munmapCalls++;
            INSTANCE.munmap(address, len, memoryTag);
        }
        @Override public int openCleanRW(String path) {
            int fd = INSTANCE.openCleanRW(path);
            if (path.endsWith(AckWatermark.FILE_NAME)) watermarkFd = fd;
            return fd;
        }
        @Override public int openCleanRW(long pathPtr) { return INSTANCE.openCleanRW(pathPtr); }
        @Override public int openRW(String path) {
            int fd = INSTANCE.openRW(path);
            if (path.endsWith(AckWatermark.FILE_NAME)) watermarkFd = fd;
            return fd;
        }
        @Override public int openRW(long pathPtr) { return INSTANCE.openRW(pathPtr); }
        @Override public long read(int fd, long addr, long len, long offset) { return INSTANCE.read(fd, addr, len, offset); }
        @Override public boolean remove(String path) { return INSTANCE.remove(path); }
        @Override public boolean remove(long pathPtr) { return INSTANCE.remove(pathPtr); }
        @Override public int rename(String oldPath, String newPath) { return INSTANCE.rename(oldPath, newPath); }
        @Override public boolean truncate(int fd, long size) { return INSTANCE.truncate(fd, size); }
        @Override public long write(int fd, long addr, long len, long offset) { return INSTANCE.write(fd, addr, len, offset); }
    }
}
