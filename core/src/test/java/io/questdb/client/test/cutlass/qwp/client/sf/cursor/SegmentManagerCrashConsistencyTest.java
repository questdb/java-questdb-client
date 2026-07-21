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
 *******************************************************************************/

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
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SegmentManagerCrashConsistencyTest {

    private static void awaitTrimmed(SegmentRing ring) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (ring.firstSealed() != null || ring.getPendingTrimCount() != 0) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("manager did not trim acknowledged segments");
            }
            io.questdb.client.std.Compat.onSpinWait();
        }
    }

    private static void awaitValue(AtomicInteger value, int expected, String message) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (value.get() < expected) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError(message + " [expected=" + expected + ", actual=" + value.get() + ']');
            }
            io.questdb.client.std.Compat.onSpinWait();
        }
    }

    private static SegmentRing createRing(String root, long segmentSize, long payload, int sealedCount) {
        SegmentRing ring = null;
        boolean success = false;
        try {
            ring = new SegmentRing(MmapSegment.create(root + "/sf-0.sfa", 0, segmentSize), segmentSize);
            Assert.assertEquals(0L, ring.appendOrFsn(payload, 1));
            for (int i = 1; i <= sealedCount; i++) {
                ring.installHotSpare(MmapSegment.create(root + "/sf-" + i + ".sfa", i, segmentSize));
                Assert.assertEquals(i, ring.appendOrFsn(payload, 1));
            }
            ring.installHotSpare(MmapSegment.create(root + "/sf-" + (sealedCount + 1) + ".sfa",
                    sealedCount + 1L, segmentSize));
            Assert.assertTrue(ring.acknowledge(sealedCount - 1L));
            success = true;
            return ring;
        } finally {
            if (!success && ring != null) ring.close();
        }
    }

    private static AckWatermark openWatermark(FilesFacade ff, String root) {
        return AckWatermark.open(ff, root);
    }

    private static void removeRecursive(String dir) {
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(dir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(dir);
    }

    @Test(timeout = 15_000L)
    public void testBackgroundTrimDurabilityOrder() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String root = Paths.get(System.getProperty("java.io.tmpdir"),
                    "qdb-manager-crash-order-" + System.nanoTime()).toString();
            Assert.assertEquals(0, Files.mkdir(root, Files.DIR_MODE_DEFAULT));
            long segmentSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1L;
            long payload = 0;
            OrderingFilesFacade ff = new OrderingFilesFacade(root);
            AckWatermark watermark = null;
            SegmentRing ring = null;
            try {
                payload = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                ring = createRing(root, segmentSize, payload, 2);
                watermark = openWatermark(ff, root);
                Assert.assertNotNull(watermark);
                try (SegmentManager manager = new SegmentManager(
                        segmentSize, TimeUnit.SECONDS.toNanos(60), segmentSize * 8L, ff, ff::ticks)) {
                    manager.register(ring, root, watermark);
                    ff.active = true;
                    manager.start();
                    awaitTrimmed(ring);
                }
                Assert.assertEquals(Arrays.asList("watermark-msync", "watermark-fsync", "dir-fsync",
                        "segment-remove", "segment-remove", "dir-fsync"), ff.events);
            } finally {
                if (ring != null) ring.close();
                if (watermark != null) watermark.close();
                if (payload != 0) Unsafe.free(payload, 1, MemoryTag.NATIVE_DEFAULT);
                removeRecursive(root);
            }
        });
    }

    @Test(timeout = 15_000L)
    public void testBarrierFailuresPreserveCrashSafetyAndRetry() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (int failAt : new int[]{0, 1, 2, 5}) {
                String root = Paths.get(System.getProperty("java.io.tmpdir"),
                        "qdb-manager-crash-fault-" + failAt + "-" + System.nanoTime()).toString();
                Assert.assertEquals(0, Files.mkdir(root, Files.DIR_MODE_DEFAULT));
                long segmentSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1L;
                long payload = 0;
                OrderingFilesFacade ff = new OrderingFilesFacade(root, failAt);
                AckWatermark watermark = null;
                SegmentRing ring = null;
                try {
                    payload = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                    ring = createRing(root, segmentSize, payload, 2);
                    watermark = openWatermark(ff, root);
                    Assert.assertNotNull(watermark);
                    try (SegmentManager manager = new SegmentManager(
                            segmentSize, TimeUnit.SECONDS.toNanos(60), segmentSize * 8L, ff, ff::ticks)) {
                        manager.register(ring, root, watermark);
                        ff.active = true;
                        manager.start();
                        ff.awaitFailure();
                        if (failAt == 5) {
                            Assert.assertNull("post-unlink barrier failure exposed a closed segment",
                                    ring.findSegmentContaining(0L));
                            Assert.assertNull("post-unlink barrier failure kept a closed segment live",
                                    ring.firstSealed());
                            Assert.assertEquals("post-unlink barrier failure lost cleanup ownership",
                                    2, ring.getPendingTrimCount());
                            ff.advance(TimeUnit.SECONDS.toNanos(2));
                            manager.wakeWorker();
                            awaitTrimmed(ring);
                        }
                        manager.close();
                    }
                    if (failAt <= 2) {
                        Assert.assertEquals("unlink started before covering barrier failed", 0, ff.removeCalls.get());
                        Assert.assertNotNull("barrier failure removed ring bookkeeping", ring.firstSealed());
                    } else {
                        Assert.assertNull("post-unlink barrier retry did not commit ring removal", ring.firstSealed());
                        Assert.assertEquals("post-unlink barrier retry retained pending ownership",
                                0, ring.getPendingTrimCount());
                    }
                    Assert.assertFalse("segment deletion began without a durable covering watermark",
                            ff.removeCalls.get() > 0 && !ff.durableWatermark);
                } finally {
                    if (ring != null) ring.close();
                    if (watermark != null) watermark.close();
                    if (payload != 0) Unsafe.free(payload, 1, MemoryTag.NATIVE_DEFAULT);
                    removeRecursive(root);
                }
            }
        });
    }

    @Test(timeout = 15_000L)
    public void testDiskTrimWithoutWatermarkIsPreserved() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String root = Paths.get(System.getProperty("java.io.tmpdir"),
                    "qdb-manager-no-watermark-" + System.nanoTime()).toString();
            Assert.assertEquals(0, Files.mkdir(root, Files.DIR_MODE_DEFAULT));
            long segmentSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1L;
            long payload = 0;
            SegmentRing ring = null;
            try {
                payload = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                ring = createRing(root, segmentSize, payload, 1);
                CountDownLatch servicePass = new CountDownLatch(1);
                try (SegmentManager manager = new SegmentManager(
                        segmentSize, TimeUnit.SECONDS.toNanos(60), segmentSize * 8L)) {
                    manager.setBeforeTrimSyncHook(servicePass::countDown);
                    manager.register(ring, root, null);
                    manager.start();
                    Assert.assertTrue("manager did not reach the trim service pass",
                            servicePass.await(5, TimeUnit.SECONDS));
                }
                Assert.assertNotNull(ring.firstSealed());
                Assert.assertTrue(Files.exists(root + "/sf-0.sfa"));
            } finally {
                if (ring != null) ring.close();
                if (payload != 0) Unsafe.free(payload, 1, MemoryTag.NATIVE_DEFAULT);
                removeRecursive(root);
            }
        });
    }

    @Test(timeout = 15_000L)
    public void testMoreThanOneQuantumBatchesBarriers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String root = Paths.get(System.getProperty("java.io.tmpdir"),
                    "qdb-manager-crash-batch-" + System.nanoTime()).toString();
            Assert.assertEquals(0, Files.mkdir(root, Files.DIR_MODE_DEFAULT));
            long segmentSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1L;
            long payload = 0;
            OrderingFilesFacade ff = new OrderingFilesFacade(root);
            AckWatermark watermark = null;
            SegmentRing ring = null;
            try {
                payload = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                ring = createRing(root, segmentSize, payload, 65);
                watermark = openWatermark(ff, root);
                Assert.assertNotNull(watermark);
                try (SegmentManager manager = new SegmentManager(
                        segmentSize, TimeUnit.SECONDS.toNanos(60), segmentSize * 70L, ff, ff::ticks)) {
                    manager.register(ring, root, watermark);
                    ff.active = true;
                    manager.start();
                    awaitTrimmed(ring);
                }
                Assert.assertEquals(65, ff.removeCalls.get());
                Assert.assertEquals("watermark must sync once per quantum", 2, ff.msyncCalls.get());
                Assert.assertEquals("directory barriers must be twice per quantum", 4, ff.dirSyncCalls.get());
            } finally {
                if (ring != null) ring.close();
                if (watermark != null) watermark.close();
                if (payload != 0) Unsafe.free(payload, 1, MemoryTag.NATIVE_DEFAULT);
                removeRecursive(root);
            }
        });
    }

    @Test(timeout = 15_000L)
    public void testPersistentFailureDoesNotStarveSibling() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String base = Paths.get(System.getProperty("java.io.tmpdir"),
                    "qdb-manager-retry-sibling-" + System.nanoTime()).toString();
            String badRoot = base + "/bad";
            String goodRoot = base + "/good";
            Assert.assertEquals(0, Files.mkdir(base, Files.DIR_MODE_DEFAULT));
            Assert.assertEquals(0, Files.mkdir(badRoot, Files.DIR_MODE_DEFAULT));
            Assert.assertEquals(0, Files.mkdir(goodRoot, Files.DIR_MODE_DEFAULT));
            long segmentSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1L;
            long payload = 0;
            OrderingFilesFacade ff = new OrderingFilesFacade(badRoot, "dir-fsync", 0);
            AckWatermark badWatermark = null;
            AckWatermark goodWatermark = null;
            SegmentRing badRing = null;
            SegmentRing goodRing = null;
            try {
                payload = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                badRing = createRing(badRoot, segmentSize, payload, 1);
                goodRing = createRing(goodRoot, segmentSize, payload, 1);
                badWatermark = openWatermark(ff, badRoot);
                goodWatermark = openWatermark(ff, goodRoot);
                try (SegmentManager manager = new SegmentManager(
                        segmentSize, TimeUnit.SECONDS.toNanos(60), segmentSize * 16L, ff, ff::ticks)) {
                    manager.register(badRing, badRoot, badWatermark);
                    manager.register(goodRing, goodRoot, goodWatermark);
                    ff.active = true;
                    manager.start();
                    awaitValue(ff.failureCalls, 1, "bad sibling failure was not attempted");
                    awaitTrimmed(goodRing);
                    Assert.assertNotNull("failed sibling unexpectedly trimmed", badRing.firstSealed());
                }
            } finally {
                if (badRing != null) badRing.close();
                if (goodRing != null) goodRing.close();
                if (badWatermark != null) badWatermark.close();
                if (goodWatermark != null) goodWatermark.close();
                if (payload != 0) Unsafe.free(payload, 1, MemoryTag.NATIVE_DEFAULT);
                removeRecursive(badRoot);
                removeRecursive(goodRoot);
                removeRecursive(base);
            }
        });
    }

    @Test(timeout = 30_000L)
    public void testPersistentFailuresBackOffAndRecover() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String[] failures = {"watermark-msync", "segment-remove", "post-dir-fsync"};
            for (String failure : failures) {
                String root = Paths.get(System.getProperty("java.io.tmpdir"),
                        "qdb-manager-retry-" + failure + '-' + System.nanoTime()).toString();
                Assert.assertEquals(0, Files.mkdir(root, Files.DIR_MODE_DEFAULT));
                long segmentSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1L;
                long payload = 0;
                OrderingFilesFacade ff = new OrderingFilesFacade(
                        root, failure, "segment-remove".equals(failure) ? 2 : 0);
                if ("watermark-msync".equals(failure)) {
                    ff.ticks.set(Long.MAX_VALUE - 2_000_000L);
                }
                AckWatermark watermark = null;
                SegmentRing ring = null;
                try {
                    payload = Unsafe.malloc(1, MemoryTag.NATIVE_DEFAULT);
                    ring = createRing(root, segmentSize, payload, 2);
                    watermark = openWatermark(ff, root);
                    Assert.assertNotNull(watermark);
                    AtomicInteger logs = new AtomicInteger();
                    AtomicInteger passes = new AtomicInteger();
                    try (SegmentManager manager = new SegmentManager(
                            segmentSize, TimeUnit.SECONDS.toNanos(60), segmentSize * 8L, ff, ff::ticks)) {
                        manager.setBeforeTrimSyncHook(passes::incrementAndGet);
                        manager.setRetryLogHook(logs::incrementAndGet);
                        manager.register(ring, root, watermark);
                        ff.active = true;
                        manager.start();
                        awaitValue(ff.failureCalls, 1, "initial persistent failure was not attempted");
                        awaitValue(logs, 1, "initial failure transition was not logged");
                        if ("segment-remove".equals(failure)) {
                            Assert.assertNull("failed unlink left a closed segment live",
                                    ring.firstSealed());
                            Assert.assertEquals("successful unlink prefix was not committed",
                                    1, ring.getPendingTrimCount());
                            Assert.assertFalse(Files.exists(root + "/sf-0.sfa"));
                            Assert.assertTrue(Files.exists(root + "/sf-1.sfa"));
                        }

                        int operations = ff.operationCalls();
                        for (int i = 0; i < 4; i++) {
                            int nextPass = passes.get() + 1;
                            manager.wakeWorker();
                            awaitValue(passes, nextPass, "deferred retry pass did not run");
                        }
                        Assert.assertEquals("deferred passes performed filesystem work",
                                operations, ff.operationCalls());
                        Assert.assertEquals("deferred passes emitted logs", 1, logs.get());

                        long delay = 4_000_000L;
                        for (int attempt = 2; attempt <= 11; attempt++) {
                            int nextPass = passes.get() + 1;
                            operations = ff.operationCalls();
                            ff.advance(delay - 1);
                            manager.wakeWorker();
                            awaitValue(passes, nextPass, "pre-deadline pass did not run");
                            Assert.assertEquals("pre-deadline pass performed filesystem work",
                                    operations, ff.operationCalls());
                            nextPass = passes.get() + 1;
                            ff.advance(1);
                            manager.wakeWorker();
                            awaitValue(ff.failureCalls, attempt, "retry deadline did not enable attempt");
                            awaitValue(passes, nextPass, "retry pass did not run");
                            Assert.assertEquals("persistent failure log was not throttled", 1, logs.get());
                            delay = Math.min(delay * 2, 1_024_000_000L);
                        }

                        ff.failureEnabled = false;
                        ff.advance(delay);
                        manager.wakeWorker();
                        awaitTrimmed(ring);
                        awaitValue(logs, 2, "recovery transition was not logged");
                        Assert.assertEquals("recovery transition was not logged once", 2, logs.get());
                    }
                    Assert.assertNull(ring.firstSealed());
                } finally {
                    if (ring != null) ring.close();
                    if (watermark != null) watermark.close();
                    if (payload != 0) Unsafe.free(payload, 1, MemoryTag.NATIVE_DEFAULT);
                    removeRecursive(root);
                }
            }
        });
    }

    private static final class OrderingFilesFacade implements FilesFacade {
        private final AtomicInteger dirSyncCalls = new AtomicInteger();
        private final List<String> events = new ArrayList<>();
        private final int failAt;
        private final AtomicInteger failureCalls = new AtomicInteger();
        private final AtomicInteger msyncCalls = new AtomicInteger();
        private final AtomicInteger removeCalls = new AtomicInteger();
        private final String persistentEvent;
        private final int persistentRemoveOrdinal;
        private final String root;
        private final AtomicLong ticks = new AtomicLong();
        private boolean active;
        private boolean durableSegments = true;
        private boolean durableWatermark;
        private int eventIndex;
        private boolean expectPreDirSync = true;
        private volatile boolean failureEnabled;
        private volatile boolean failureObserved;
        private int watermarkFd = -1;

        private OrderingFilesFacade(String root) {
            this(root, -1);
        }

        private OrderingFilesFacade(String root, int failAt) {
            this.root = root;
            this.failAt = failAt;
            this.persistentEvent = null;
            this.persistentRemoveOrdinal = 0;
        }

        private OrderingFilesFacade(String root, String persistentEvent, int persistentRemoveOrdinal) {
            this.root = root;
            this.failAt = -1;
            this.persistentEvent = persistentEvent;
            this.persistentRemoveOrdinal = persistentRemoveOrdinal;
            this.failureEnabled = true;
        }

        private void advance(long nanos) {
            ticks.addAndGet(nanos);
        }

        private void awaitFailure() {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!failureObserved) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("injected barrier failure was not reached");
                }
                io.questdb.client.std.Compat.onSpinWait();
            }
        }

        private boolean fail(String event) {
            events.add("post-dir-fsync".equals(event) ? "dir-fsync" : event);
            boolean persistentMatch = failureEnabled && event.equals(persistentEvent)
                    && (!"segment-remove".equals(event) || persistentRemoveOrdinal <= removeCalls.get());
            boolean failed = eventIndex++ == failAt || persistentMatch;
            if (failed) {
                failureCalls.incrementAndGet();
                failureObserved = true;
            }
            return failed;
        }

        private int operationCalls() {
            return msyncCalls.get() + dirSyncCalls.get() + removeCalls.get();
        }

        private long ticks() {
            return ticks.get();
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
            if (active && fd == watermarkFd && fail("watermark-fsync")) return -1;
            return INSTANCE.fsync(fd);
        }
        @Override public int fsyncDir(String dir) {
            if (active && root.equals(dir)) {
                dirSyncCalls.incrementAndGet();
                String event = expectPreDirSync ? "dir-fsync" : "post-dir-fsync";
                boolean failed = fail(event);
                if (expectPreDirSync) {
                    if (!failed) {
                        durableWatermark = true;
                        expectPreDirSync = false;
                    }
                } else {
                    expectPreDirSync = true;
                    if (!failed) durableSegments = false;
                }
                if (failed) return -1;
            }
            return INSTANCE.fsyncDir(dir);
        }
        @Override public long length(int fd) { return INSTANCE.length(fd); }
        @Override public long length(String path) { return INSTANCE.length(path); }
        @Override public long length(long pathPtr) { return INSTANCE.length(pathPtr); }
        @Override public int lock(int fd) { return INSTANCE.lock(fd); }
        @Override public int mkdir(String path, int mode) { return INSTANCE.mkdir(path, mode); }
        @Override public int msync(long addr, long len, boolean async) {
            if (active) {
                msyncCalls.incrementAndGet();
                if (fail("watermark-msync")) return -1;
            }
            return INSTANCE.msync(addr, len, async);
        }
        @Override public int openCleanRW(String path) {
            int fd = INSTANCE.openCleanRW(path);
            if (path.equals(root + "/" + AckWatermark.FILE_NAME)) watermarkFd = fd;
            return fd;
        }
        @Override public int openCleanRW(long pathPtr) { return INSTANCE.openCleanRW(pathPtr); }
        @Override public int openRW(String path) {
            int fd = INSTANCE.openRW(path);
            if (path.equals(root + "/" + AckWatermark.FILE_NAME)) watermarkFd = fd;
            return fd;
        }
        @Override public int openRW(long pathPtr) { return INSTANCE.openRW(pathPtr); }
        @Override public long read(int fd, long addr, long len, long offset) { return INSTANCE.read(fd, addr, len, offset); }
        @Override public boolean remove(String path) {
            if (active && path.endsWith(".sfa")) {
                removeCalls.incrementAndGet();
                if (fail("segment-remove")) return false;
            }
            return INSTANCE.remove(path);
        }
        @Override public boolean remove(long pathPtr) { return INSTANCE.remove(pathPtr); }
        @Override public int rename(String oldPath, String newPath) { return INSTANCE.rename(oldPath, newPath); }
        @Override public boolean truncate(int fd, long size) { return INSTANCE.truncate(fd, size); }
        @Override public long write(int fd, long addr, long len, long offset) { return INSTANCE.write(fd, addr, len, offset); }
    }
}
