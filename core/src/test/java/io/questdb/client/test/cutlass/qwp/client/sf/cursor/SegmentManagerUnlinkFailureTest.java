/*+*****************************************************************************
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
import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SegmentManagerUnlinkFailureTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-segmgr-unlink-fault-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir != null) {
            removeRecursive(tmpDir);
        }
    }

    @Test
    public void testEnumerationFindNextFailureRefusesGenerationAllocation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segmentSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 32L;
            String dir = tmpDir + "/enumeration";
            Assert.assertEquals(0, Files.mkdir(dir, Files.DIR_MODE_DEFAULT));
            String lowerName = "sf-0000000000000000.sfa";
            String lowerPath = dir + "/" + lowerName;
            String higherPath = dir + "/sf-0000000000000007.sfa";
            MmapSegment lower = MmapSegment.create(lowerPath, 0L, segmentSize);
            lower.close();
            MmapSegment initial = MmapSegment.create(higherPath, 0L, segmentSize);
            SegmentRing ring = new SegmentRing(initial, segmentSize);
            byte[] originalHigher = java.nio.file.Files.readAllBytes(Paths.get(higherPath));
            FailingFilesFacade facade = new FailingFilesFacade(null, dir, lowerName);
            try (SegmentManager manager = new SegmentManager(
                    segmentSize, TimeUnit.SECONDS.toNanos(60), segmentSize * 4, facade)) {
                try {
                    manager.register(ring, dir);
                    Assert.fail("register accepted a partially enumerated SF directory");
                } catch (IllegalStateException expected) {
                    Assert.assertTrue(expected.getMessage().contains("could not fully enumerate"));
                }
                Assert.assertTrue("fault did not occur after the lower generation was observed",
                        facade.partialLowerObserved);
                Assert.assertTrue("failed enumeration cursor was not closed", facade.partialFindClosed);
                Assert.assertEquals("enumeration failure must not allocate or truncate a path",
                        0, facade.openCleanCalls);
                Assert.assertTrue("higher generation disappeared", Files.exists(higherPath));
                Assert.assertArrayEquals("partial enumeration changed the unseen higher generation",
                        originalHigher, java.nio.file.Files.readAllBytes(Paths.get(higherPath)));
            } finally {
                ring.close();
            }
        });
    }

    @Test(timeout = 15_000L)
    public void testFailedUnlinkRetainsBookkeepingAndUsesSuccessorPath() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segmentSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 32L;
            String dir = tmpDir + "/unlink";
            Assert.assertEquals(0, Files.mkdir(dir, Files.DIR_MODE_DEFAULT));
            String failedPath = dir + "/sf-0000000000000000.sfa";
            String activePath = dir + "/sf-0000000000000001.sfa";
            MmapSegment initial = MmapSegment.create(failedPath, 0L, segmentSize);
            SegmentRing ring = new SegmentRing(initial, segmentSize);
            AckWatermark watermark = AckWatermark.open(dir);
            Assert.assertNotNull(watermark);
            watermark.write(-1L);
            long payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            try {
                fill(payload, 32, (byte) 0x11);
                Assert.assertEquals(0L, ring.appendOrFsn(payload, 32));
                ring.installHotSpare(MmapSegment.create(activePath, 1L, segmentSize));
                Assert.assertEquals(1L, ring.appendOrFsn(payload, 32));
                ring.acknowledge(0L);
                byte[] original = java.nio.file.Files.readAllBytes(Paths.get(failedPath));

                FailingFilesFacade facade = new FailingFilesFacade(failedPath, null, null);
                try (SegmentManager manager = new SegmentManager(
                        segmentSize, TimeUnit.SECONDS.toNanos(60), segmentSize * 8, facade)) {
                    manager.register(ring, dir, watermark);
                    manager.start();
                    Assert.assertTrue("manager never attempted the injected unlink",
                            facade.removeAttempted.await(5, TimeUnit.SECONDS));
                    Assert.assertEquals("failed unlink must retain conservative registered bytes",
                            ring.totalSegmentBytes(), readTotalBytes(manager));
                }

                Assert.assertTrue("failed unlink path must remain observable", Files.exists(failedPath));
                Assert.assertTrue("failed unlink changed the acknowledged segment bytes",
                        Arrays.equals(original, java.nio.file.Files.readAllBytes(Paths.get(failedPath))));
                Assert.assertNotNull("failed unlink removed the segment from ring bookkeeping",
                        ring.firstSealed());
                Assert.assertEquals(failedPath, ring.firstSealed().path());
                Assert.assertEquals("failed unlink must remain covered by the durable cumulative watermark",
                        0L, watermark.read());

                fill(payload, 32, (byte) 0x5A);
                Assert.assertEquals("non-DEDUP successor must continue at the next FSN",
                        2L, ring.appendOrFsn(payload, 32));
                String successorPath = ring.getActive().path();
                Assert.assertNotEquals("successor reused the acknowledged path",
                        failedPath, successorPath);
                Assert.assertEquals(dir + "/sf-0000000000000002.sfa", successorPath);
                Assert.assertTrue("successor segment was not created", Files.exists(successorPath));
                Assert.assertTrue("distinct non-DEDUP payload was not written to the successor",
                        containsRun(java.nio.file.Files.readAllBytes(Paths.get(successorPath)),
                                (byte) 0x5A, 32));
                Assert.assertTrue("successor write overwrote the failed-unlink segment",
                        Arrays.equals(original, java.nio.file.Files.readAllBytes(Paths.get(failedPath))));

                try (SegmentManager retryManager = new SegmentManager(
                        segmentSize, TimeUnit.SECONDS.toNanos(60), segmentSize * 8, facade)) {
                    retryManager.register(ring, dir, watermark);
                    retryManager.start();
                    retryManager.wakeWorker();
                    awaitRetryCommit(failedPath, watermark);
                }
                MmapSegment firstSealed = ring.firstSealed();
                Assert.assertTrue("successful retry retained the acknowledged segment",
                        firstSealed == null || !failedPath.equals(firstSealed.path()));
            } finally {
                Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
                ring.close();
                watermark.close();
            }
        });
    }

    private static void awaitRetryCommit(String failedPath, AckWatermark watermark) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (Files.exists(failedPath) || watermark.read() != 0L) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("unlink retry did not remove the file and advance the watermark");
            }
            io.questdb.client.std.Compat.onSpinWait();
        }
    }

    private static boolean containsRun(byte[] bytes, byte value, int length) {
        int run = 0;
        for (byte b : bytes) {
            run = b == value ? run + 1 : 0;
            if (run == length) {
                return true;
            }
        }
        return false;
    }

    private static void fill(long address, int len, byte value) {
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(address + i, value);
        }
    }

    private static long readTotalBytes(SegmentManager manager) throws Exception {
        Field field = SegmentManager.class.getDeclaredField("totalBytes");
        field.setAccessible(true);
        Field lockField = SegmentManager.class.getDeclaredField("lock");
        lockField.setAccessible(true);
        Object lock = lockField.get(manager);
        synchronized (lock) {
            return field.getLong(manager);
        }
    }

    private static void removeRecursive(String dir) {
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        if (!Files.remove(child)) {
                            removeRecursive(child);
                        }
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }

    private static final class FailingFilesFacade implements FilesFacade {
        private static final long PARTIAL_FIND_PTR = Long.MAX_VALUE;
        private final String enumerationFailureDir;
        private final String partialLowerName;
        private final String unlinkFailurePath;
        private final CountDownLatch removeAttempted = new CountDownLatch(1);
        private int openCleanCalls;
        private boolean partialFindClosed;
        private long partialFindNamePtr;
        private boolean partialLowerObserved;
        private int unlinkFailuresRemaining = 1;

        private FailingFilesFacade(
                String unlinkFailurePath,
                String enumerationFailureDir,
                String partialLowerName
        ) {
            this.unlinkFailurePath = unlinkFailurePath;
            this.enumerationFailureDir = enumerationFailureDir;
            this.partialLowerName = partialLowerName;
        }

        @Override
        public boolean allocate(int fd, long size) { return INSTANCE.allocate(fd, size); }
        @Override
        public long allocNativePath(String path) { return INSTANCE.allocNativePath(path); }
        @Override
        public int close(int fd) { return INSTANCE.close(fd); }
        @Override
        public boolean exists(String path) { return INSTANCE.exists(path); }
        @Override
        public void findClose(long findPtr) {
            if (findPtr == PARTIAL_FIND_PTR) {
                INSTANCE.freeNativePath(partialFindNamePtr);
                partialFindNamePtr = 0L;
                partialFindClosed = true;
            } else {
                INSTANCE.findClose(findPtr);
            }
        }
        @Override
        public long findFirst(String dir) {
            if (dir.equals(enumerationFailureDir)) {
                partialFindNamePtr = INSTANCE.allocNativePath(partialLowerName);
                return PARTIAL_FIND_PTR;
            }
            return INSTANCE.findFirst(dir);
        }
        @Override
        public long findName(long findPtr) {
            return findPtr == PARTIAL_FIND_PTR ? partialFindNamePtr : INSTANCE.findName(findPtr);
        }
        @Override
        public int findNext(long findPtr) {
            if (findPtr == PARTIAL_FIND_PTR) {
                partialLowerObserved = true;
                return -1;
            }
            return INSTANCE.findNext(findPtr);
        }
        @Override
        public int findType(long findPtr) { return INSTANCE.findType(findPtr); }
        @Override
        public void freeNativePath(long pathPtr) { INSTANCE.freeNativePath(pathPtr); }
        @Override
        public int fsync(int fd) { return INSTANCE.fsync(fd); }
        @Override
        public long length(int fd) { return INSTANCE.length(fd); }
        @Override
        public long length(String path) { return INSTANCE.length(path); }
        @Override
        public long length(long pathPtr) { return INSTANCE.length(pathPtr); }
        @Override
        public int lock(int fd) { return INSTANCE.lock(fd); }
        @Override
        public int mkdir(String path, int mode) { return INSTANCE.mkdir(path, mode); }
        @Override
        public int openCleanRW(String path) {
            openCleanCalls++;
            return INSTANCE.openCleanRW(path);
        }
        @Override
        public int openCleanRW(long pathPtr) {
            openCleanCalls++;
            return INSTANCE.openCleanRW(pathPtr);
        }
        @Override
        public int openRW(String path) { return INSTANCE.openRW(path); }
        @Override
        public int openRW(long pathPtr) { return INSTANCE.openRW(pathPtr); }
        @Override
        public long read(int fd, long addr, long len, long offset) {
            return INSTANCE.read(fd, addr, len, offset);
        }
        @Override
        public boolean remove(String path) {
            if (path.equals(unlinkFailurePath) && unlinkFailuresRemaining > 0) {
                unlinkFailuresRemaining--;
                removeAttempted.countDown();
                return false;
            }
            return INSTANCE.remove(path);
        }
        @Override
        public boolean remove(long pathPtr) { return INSTANCE.remove(pathPtr); }
        @Override
        public int rename(String oldPath, String newPath) { return INSTANCE.rename(oldPath, newPath); }
        @Override
        public boolean truncate(int fd, long size) { return INSTANCE.truncate(fd, size); }
        @Override
        public long write(int fd, long addr, long len, long offset) {
            return INSTANCE.write(fd, addr, len, offset);
        }
    }
}
