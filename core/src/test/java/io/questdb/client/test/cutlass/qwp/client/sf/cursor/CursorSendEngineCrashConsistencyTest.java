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
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
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
import java.util.concurrent.TimeUnit;

public class CursorSendEngineCrashConsistencyTest {

    @Test
    public void testCloseDurabilityOrderAndPostCleanupSyncFailurePropagation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (int failAt : new int[]{-1, 4}) {
                String root = Paths.get(System.getProperty("java.io.tmpdir"),
                        "qdb-close-crash-" + failAt + "-" + System.nanoTime()).toString();
                String slot = root + "/slot";
                SegmentManager manager = null;
                CursorSendEngine engine = null;
                long payload = 0;
                Throwable failure = null;
                try {
                    Assert.assertEquals(0, Files.mkdir(root, Files.DIR_MODE_DEFAULT));
                    CrashImageFilesFacade ff = new CrashImageFilesFacade(slot, failAt);
                    long segmentSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 32L;
                    manager = new SegmentManager(segmentSize, TimeUnit.SECONDS.toNanos(60),
                            SegmentManager.UNLIMITED_TOTAL_BYTES, ff);
                    payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
                    engine = new CursorSendEngine(slot, segmentSize, manager);
                    Unsafe.getUnsafe().setMemory(payload, 32, (byte) 7);
                    Assert.assertEquals(0L, engine.appendBlocking(payload, 32));
                    Assert.assertTrue(engine.acknowledge(0L));
                    ff.beginClose();
                    try {
                        engine.close();
                        if (failAt >= 0) {
                            Assert.fail("sync failure was swallowed at boundary " + failAt);
                        }
                    } catch (IllegalStateException expected) {
                        Assert.assertTrue("unexpected close failure: " + expected, failAt >= 0);
                    }
                    engine = null;

                    Assert.assertFalse("simulated crash replays acknowledged rows at boundary " + failAt,
                            ff.durableSegments && !ff.durableWatermark);
                    if (failAt == -1) {
                        Assert.assertEquals(Arrays.asList("watermark-msync", "watermark-fsync",
                                        "dir-fsync", "segment-remove", "dir-fsync", "watermark-remove"),
                                ff.events);
                    }
                } catch (Throwable t) {
                    failure = t;
                } finally {
                    failure = closeEngine(failure, engine);
                    failure = closeManager(failure, manager);
                    failure = freePayload(failure, payload);
                    failure = removeRoot(failure, root);
                }
                rethrow(failure);
            }
        });
    }

    @Test
    public void testFinalWatermarkBarrierFailureRetainsSlotUntilRetry() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            for (String barrier : Arrays.asList("watermark-msync", "watermark-fsync", "dir-fsync")) {
                String root = Paths.get(System.getProperty("java.io.tmpdir"),
                        "qdb-close-barrier-" + barrier + "-" + System.nanoTime()).toString();
                String slot = root + "/slot";
                SegmentManager manager = null;
                CursorSendEngine predecessor = null;
                CursorSendEngine successor = null;
                CrashImageFilesFacade ff = null;
                long payload = 0;
                Throwable failure = null;
                try {
                    Assert.assertEquals(0, Files.mkdir(root, Files.DIR_MODE_DEFAULT));
                    ff = new CrashImageFilesFacade(slot, -1);
                    long segmentSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 32L;
                    manager = new SegmentManager(segmentSize, TimeUnit.SECONDS.toNanos(60),
                            SegmentManager.UNLIMITED_TOTAL_BYTES, ff);
                    payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
                    predecessor = new CursorSendEngine(slot, segmentSize, manager);
                    Unsafe.getUnsafe().setMemory(payload, 32, (byte) 7);
                    Assert.assertEquals(0L, predecessor.appendBlocking(payload, 32));
                    Assert.assertTrue(predecessor.acknowledge(0L));

                    ff.failPersistently(barrier);
                    try {
                        predecessor.close();
                        Assert.fail("expected close to report failed final barrier " + barrier);
                    } catch (IllegalStateException expected) {
                        Assert.assertTrue("unexpected close failure: " + expected,
                                expected.getMessage().contains("ack watermark")
                                        || expected.getMessage().contains("slot directory"));
                    }
                    try {
                        successor = new CursorSendEngine(slot, segmentSize, manager);
                        Assert.fail("successor acquired a slot whose final ACK barrier failed: " + barrier);
                    } catch (IllegalStateException expected) {
                        Assert.assertTrue("successor failed for the wrong reason: " + expected,
                                expected.getMessage().contains("slot already in use"));
                    }

                    ff.clearPersistentFailure();
                    awaitCloseCompleted(predecessor);
                    predecessor = null;

                    successor = new CursorSendEngine(slot, segmentSize, manager);
                    Assert.assertEquals("successful retry must leave no ACKed frame to replay",
                            -1L, successor.publishedFsn());
                    successor.close();
                    Assert.assertTrue(successor.isCloseCompleted());
                    successor = null;
                } catch (Throwable t) {
                    failure = t;
                } finally {
                    if (failure != null && ff != null) {
                        // Let a retained predecessor converge before cleanup.
                        // The exact red assertion remains attached to failure.
                        ff.clearPersistentFailure();
                    }
                    failure = closeEngine(failure, successor);
                    failure = closeEngine(failure, predecessor);
                    failure = closeManager(failure, manager);
                    failure = freePayload(failure, payload);
                    failure = removeRoot(failure, root);
                }
                rethrow(failure);
            }
        });
    }

    @Test
    public void testRecoveredSlotRejectsMissingWatermark() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String root = Paths.get(System.getProperty("java.io.tmpdir"),
                    "qdb-missing-watermark-" + System.nanoTime()).toString();
            String slot = root + "/slot";
            SegmentManager manager = null;
            CursorSendEngine predecessor = null;
            CursorSendEngine successor = null;
            long payload = 0;
            Throwable failure = null;
            try {
                Assert.assertEquals(0, Files.mkdir(root, Files.DIR_MODE_DEFAULT));
                CrashImageFilesFacade ff = new CrashImageFilesFacade(slot, -1);
                long segmentSize = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 32L;
                manager = new SegmentManager(segmentSize, TimeUnit.SECONDS.toNanos(60),
                        SegmentManager.UNLIMITED_TOTAL_BYTES, ff);
                payload = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
                predecessor = new CursorSendEngine(slot, segmentSize, manager);
                Unsafe.getUnsafe().setMemory(payload, 32, (byte) 7);
                Assert.assertEquals(0L, predecessor.appendBlocking(payload, 32));
                Assert.assertTrue(predecessor.acknowledge(0L));
                ff.beginClose();
                ff.blockSegmentRemove = true;
                predecessor.close();
                Assert.assertTrue(predecessor.isCloseCompleted());
                predecessor = null;
                Assert.assertTrue("precondition: ACKed segment residue must survive",
                        Files.exists(slot + "/sf-initial.sfa"));

                ff.failWatermarkOpen = true;
                try {
                    successor = new CursorSendEngine(slot, segmentSize, manager);
                    Assert.fail("recovered disk slot started without a usable ACK watermark");
                } catch (IllegalStateException expected) {
                    Assert.assertTrue("successor failed for the wrong reason: " + expected,
                            expected.getMessage().contains("ack watermark"));
                }

                ff.failWatermarkOpen = false;
                ff.blockSegmentRemove = false;
                successor = new CursorSendEngine(slot, segmentSize, manager);
                Assert.assertTrue(successor.wasRecoveredFromDisk());
                Assert.assertTrue("successor exposes predecessor's ACKed frame",
                        successor.ackedFsn() >= successor.publishedFsn());
                successor.close();
                Assert.assertTrue(successor.isCloseCompleted());
                successor = null;
            } catch (Throwable t) {
                failure = t;
            } finally {
                failure = closeEngine(failure, successor);
                failure = closeEngine(failure, predecessor);
                failure = closeManager(failure, manager);
                failure = freePayload(failure, payload);
                failure = removeRoot(failure, root);
            }
            rethrow(failure);
        });
    }

    private static Throwable addCleanupFailure(Throwable failure, Throwable cleanupFailure) {
        if (failure == null) {
            return cleanupFailure;
        }
        if (failure != cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
        }
        return failure;
    }

    private static void awaitCloseCompleted(CursorSendEngine engine) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!engine.isCloseCompleted() && System.nanoTime() < deadline) {
            Thread.yield();
        }
        Assert.assertTrue("close retry did not complete", engine.isCloseCompleted());
    }

    private static Throwable closeEngine(Throwable failure, CursorSendEngine engine) {
        if (engine != null) {
            try {
                engine.close();
            } catch (Throwable cleanupFailure) {
                failure = addCleanupFailure(failure, cleanupFailure);
            }
        }
        return failure;
    }

    private static Throwable closeManager(Throwable failure, SegmentManager manager) {
        if (manager != null) {
            try {
                manager.close();
            } catch (Throwable cleanupFailure) {
                failure = addCleanupFailure(failure, cleanupFailure);
            }
        }
        return failure;
    }

    private static Throwable freePayload(Throwable failure, long payload) {
        if (payload != 0) {
            try {
                Unsafe.free(payload, 32, MemoryTag.NATIVE_DEFAULT);
            } catch (Throwable cleanupFailure) {
                failure = addCleanupFailure(failure, cleanupFailure);
            }
        }
        return failure;
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

    private static Throwable removeRoot(Throwable failure, String root) {
        try {
            removeRecursive(root);
            Assert.assertFalse("test directory was not removed: " + root, Files.exists(root));
        } catch (Throwable cleanupFailure) {
            failure = addCleanupFailure(failure, cleanupFailure);
        }
        return failure;
    }

    private static void rethrow(Throwable failure) {
        if (failure != null) {
            CursorSendEngineCrashConsistencyTest.<RuntimeException>throwUnchecked(failure);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwUnchecked(Throwable failure) throws T {
        throw (T) failure;
    }

    private static final class CrashImageFilesFacade implements FilesFacade {
        private final List<String> events = new ArrayList<>();
        private final int failAt;
        private final String slot;
        private boolean active;
        private boolean blockSegmentRemove;
        private boolean durableSegments = true;
        private boolean durableWatermark;
        private int eventIndex;
        private boolean failWatermarkOpen;
        private String persistentFailure;
        private int watermarkFd = -1;

        private CrashImageFilesFacade(String slot, int failAt) {
            this.slot = slot;
            this.failAt = failAt;
        }

        private void beginClose() {
            active = true;
            events.clear();
            eventIndex = 0;
        }

        private void clearPersistentFailure() {
            persistentFailure = null;
        }

        private boolean fail(String event) {
            events.add(event);
            return event.equals(persistentFailure) || eventIndex++ == failAt;
        }

        private void failPersistently(String event) {
            active = true;
            events.clear();
            eventIndex = 0;
            persistentFailure = event;
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
        public void findClose(long findPtr) { INSTANCE.findClose(findPtr); }
        @Override
        public long findFirst(String dir) { return INSTANCE.findFirst(dir); }
        @Override
        public long findName(long findPtr) { return INSTANCE.findName(findPtr); }
        @Override
        public int findNext(long findPtr) { return INSTANCE.findNext(findPtr); }
        @Override
        public int findType(long findPtr) { return INSTANCE.findType(findPtr); }
        @Override
        public void freeNativePath(long pathPtr) { INSTANCE.freeNativePath(pathPtr); }
        @Override
        public int fsync(int fd) {
            return active && fd == watermarkFd && fail("watermark-fsync") ? -1 : INSTANCE.fsync(fd);
        }
        @Override
        public int fsyncDir(String dir) {
            if (active && slot.equals(dir)) {
                if (fail("dir-fsync")) return -1;
                if (events.contains("segment-remove")) {
                    durableSegments = false;
                } else {
                    durableWatermark = true;
                }
            }
            return INSTANCE.fsyncDir(dir);
        }
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
        public int msync(long addr, long len, boolean async) {
            return active && fail("watermark-msync") ? -1 : INSTANCE.msync(addr, len, async);
        }
        @Override
        public int openCleanRW(String path) {
            if (failWatermarkOpen && path.equals(slot + "/" + AckWatermark.FILE_NAME)) return -1;
            int fd = INSTANCE.openCleanRW(path);
            if (path.equals(slot + "/" + AckWatermark.FILE_NAME)) watermarkFd = fd;
            return fd;
        }
        @Override
        public int openCleanRW(long pathPtr) { return INSTANCE.openCleanRW(pathPtr); }
        @Override
        public int openRW(String path) {
            if (failWatermarkOpen && path.equals(slot + "/" + AckWatermark.FILE_NAME)) return -1;
            int fd = INSTANCE.openRW(path);
            if (path.equals(slot + "/" + AckWatermark.FILE_NAME)) watermarkFd = fd;
            return fd;
        }
        @Override
        public int openRW(long pathPtr) { return INSTANCE.openRW(pathPtr); }
        @Override
        public long read(int fd, long addr, long len, long offset) {
            return INSTANCE.read(fd, addr, len, offset);
        }
        @Override
        public boolean remove(String path) {
            if (active && path.endsWith(".sfa")) {
                if (blockSegmentRemove || fail("segment-remove")) return false;
            } else if (active && path.equals(slot + "/" + AckWatermark.FILE_NAME)) {
                if (fail("watermark-remove")) return false;
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
