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
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Regression test for manifest fsync amplification on the disk-trim path.
 * A single trim pass covering N fully-acked sealed segments must commit the
 * manifest head advance exactly once (one write + one fsync past the LAST
 * batch member before any unlink), not once per trimmed segment. Recovery
 * discards files "stale below head", so the single commit is byte-identical
 * crash recovery — the per-segment commits are pure IO amplification.
 */
public class SegmentManagerManifestFsyncTest {
    private static final String MANIFEST_NAME = "sf-manifest.bin";
    private static final long SEGMENT_SIZE = 64 * 1024;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = TestUtils.createTmpDir("qdb-sf-manifest-fsync-");
    }

    @After
    public void tearDown() {
        TestUtils.removeTmpDir(tmpDir);
    }

    @Test(timeout = 15_000L)
    public void testTrimPassCommitsManifestHeadOncePerBatch() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // A contiguous chain of four 1-frame segments: three sealed
            // (seqs 0..2) plus the active tail (seq 3).
            writeSegmentWithFrames(tmpDir + "/sf-initial.sfa", 0, 1);
            writeSegmentWithFrames(tmpDir + "/sf-0000000000000001.sfa", 1, 1);
            writeSegmentWithFrames(tmpDir + "/sf-0000000000000002.sfa", 2, 1);
            writeSegmentWithFrames(tmpDir + "/sf-0000000000000003.sfa", 3, 1);
            ManifestFsyncCountingFacade ff = new ManifestFsyncCountingFacade(tmpDir);
            AckWatermark watermark = null;
            SegmentRing ring = null;
            try {
                ring = SegmentRing.openExisting(ff, tmpDir, SEGMENT_SIZE);
                Assert.assertNotNull("legacy chain must recover", ring);
                Assert.assertNotNull("recovered ring must expose sealed segments", ring.firstSealed());
                Assert.assertTrue(ring.acknowledge(2));
                watermark = openWatermark(ff, tmpDir);
                Assert.assertNotNull(watermark);
                try (SegmentManager manager = new SegmentManager(
                        SEGMENT_SIZE, TimeUnit.SECONDS.toNanos(60), SEGMENT_SIZE * 8L, ff)) {
                    manager.register(ring, tmpDir, watermark);
                    ff.active = true;
                    manager.start();
                    awaitTrimmed(ring);
                }
                Assert.assertEquals(
                        "a trim pass must commit the manifest head once per batch, not once per segment",
                        1, ff.manifestFsyncCalls.get());
            } finally {
                if (ring != null) ring.close();
                if (watermark != null) watermark.close();
            }
        });
    }

    private static void awaitTrimmed(SegmentRing ring) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (ring.firstSealed() != null || ring.getPendingTrimCount() != 0) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("manager did not trim acknowledged segments");
            }
            io.questdb.client.std.Compat.onSpinWait();
        }
    }

    private static AckWatermark openWatermark(FilesFacade ff, String root) throws Exception {
        Method method = AckWatermark.class.getDeclaredMethod("open", FilesFacade.class, String.class);
        method.setAccessible(true);
        return (AckWatermark) method.invoke(null, ff, root);
    }

    private static void writeSegmentWithFrames(String path, long baseSeq, int frames) {
        long buf = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            MmapSegment segment = MmapSegment.create(path, baseSeq, SEGMENT_SIZE);
            try {
                for (int i = 0; i < frames; i++) {
                    for (int b = 0; b < 64; b++) {
                        Unsafe.getUnsafe().putByte(buf + b, (byte) (i * 31 + b));
                    }
                    Assert.assertTrue("test frame must fit", segment.tryAppend(buf, 64) >= 0);
                }
            } finally {
                segment.close();
            }
        } finally {
            Unsafe.free(buf, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static final class ManifestFsyncCountingFacade implements FilesFacade {
        private final AtomicInteger manifestFsyncCalls = new AtomicInteger();
        private final String manifestPath;
        private volatile boolean active;
        private volatile int manifestFd = -1;

        private ManifestFsyncCountingFacade(String root) {
            this.manifestPath = root + "/" + MANIFEST_NAME;
        }

        @Override public boolean allocate(int fd, long size) { return INSTANCE.allocate(fd, size); }
        @Override public long allocNativePath(String path) { return INSTANCE.allocNativePath(path); }
        @Override public int close(int fd) { return INSTANCE.close(fd); }
        @Override public boolean exists(String path) { return INSTANCE.exists(path); }
        @Override public void findClose(long findPtr) { INSTANCE.findClose(findPtr); }
        @Override public long findFirst(String dir) { return INSTANCE.findFirst(dir); }
        @Override public long findName(long findPtr) { return INSTANCE.findName(findPtr); }
        @Override public int findNext(long findPtr) { return INSTANCE.findNext(findPtr); }
        @Override public int findType(long findPtr) { return INSTANCE.findType(findPtr); }
        @Override public void freeNativePath(long pathPtr) { INSTANCE.freeNativePath(pathPtr); }
        @Override public int fsync(int fd) {
            if (active && fd == manifestFd) {
                manifestFsyncCalls.incrementAndGet();
            }
            return INSTANCE.fsync(fd);
        }
        @Override public int fsyncDir(String dir) { return INSTANCE.fsyncDir(dir); }
        @Override public long length(int fd) { return INSTANCE.length(fd); }
        @Override public long length(String path) { return INSTANCE.length(path); }
        @Override public long length(long pathPtr) { return INSTANCE.length(pathPtr); }
        @Override public int lock(int fd) { return INSTANCE.lock(fd); }
        @Override public int mkdir(String path, int mode) { return INSTANCE.mkdir(path, mode); }
        @Override public int msync(long addr, long len, boolean async) { return INSTANCE.msync(addr, len, async); }
        @Override public int openCleanRW(String path) { return INSTANCE.openCleanRW(path); }
        @Override public int openCleanRW(long pathPtr) { return INSTANCE.openCleanRW(pathPtr); }
        @Override public int openRW(String path) {
            int fd = INSTANCE.openRW(path);
            if (manifestPath.equals(path)) manifestFd = fd;
            return fd;
        }
        @Override public int openRW(long pathPtr) { return INSTANCE.openRW(pathPtr); }
        @Override public int openRWExclusive(String path) {
            int fd = INSTANCE.openRWExclusive(path);
            if (manifestPath.equals(path)) manifestFd = fd;
            return fd;
        }
        @Override public int openRWExclusive(long pathPtr) { return INSTANCE.openRWExclusive(pathPtr); }
        @Override public long read(int fd, long addr, long len, long offset) { return INSTANCE.read(fd, addr, len, offset); }
        @Override public boolean remove(String path) { return INSTANCE.remove(path); }
        @Override public boolean remove(long pathPtr) { return INSTANCE.remove(pathPtr); }
        @Override public int rename(String oldPath, String newPath) { return INSTANCE.rename(oldPath, newPath); }
        @Override public boolean truncate(int fd, long size) { return INSTANCE.truncate(fd, size); }
        @Override public long write(int fd, long addr, long len, long offset) { return INSTANCE.write(fd, addr, len, offset); }
    }
}
