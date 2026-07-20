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
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SegmentManagerPeriodicSyncTest {

    @Test
    public void testDeadlineAndFailurePropagation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final long intervalNanos = 100L;
            final long segmentSize = 4096L;
            AtomicLong ticks = new AtomicLong();
            CountingFilesFacade filesFacade = new CountingFilesFacade();
            String dir = TestUtils.createTmpDir("qdb-periodic-manager-");
            long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            SegmentManager manager = null;
            SegmentRing ring = null;
            try {
                MmapSegment active = MmapSegment.create(
                        filesFacade, dir + "/active.sfa", 0L, segmentSize);
                ring = new SegmentRing(active, segmentSize);
                ring.installHotSpare(MmapSegment.create(
                        filesFacade, dir + "/spare.sfa", 1L, segmentSize));
                assertEquals(0L, ring.appendOrFsn(payload, 16));

                manager = new SegmentManager(
                        segmentSize,
                        SegmentManager.DEFAULT_POLL_NANOS,
                        segmentSize * 4L,
                        filesFacade,
                        ticks::get);
                manager.register(ring, dir, null, intervalNanos);

                manager.serviceRingForTesting(ring);
                assertTrue(active.isPublishedDurable());
                assertEquals(1, filesFacade.msyncCalls);
                assertEquals(1, filesFacade.fsyncCalls);

                assertEquals(1L, ring.appendOrFsn(payload, 16));
                ticks.set(intervalNanos - 1L);
                manager.serviceRingForTesting(ring);
                assertEquals("checkpoint ran before its deadline", 1, filesFacade.msyncCalls);
                assertEquals("checkpoint ran before its deadline", 1, filesFacade.fsyncCalls);

                ticks.set(intervalNanos);
                manager.serviceRingForTesting(ring);
                assertEquals(2, filesFacade.msyncCalls);
                assertEquals(2, filesFacade.fsyncCalls);

                assertEquals(2L, ring.appendOrFsn(payload, 16));
                filesFacade.isFsyncFailureEnabled = true;
                ticks.set(intervalNanos * 2L);
                manager.serviceRingForTesting(ring);
                assertEquals(3, filesFacade.msyncCalls);
                assertEquals(3, filesFacade.fsyncCalls);
                try {
                    ring.appendOrFsn(payload, 16);
                    fail("expected manager data-sync failure to reach the producer");
                } catch (MmapSegmentException expected) {
                    assertTrue(expected.getMessage().contains("sync segment file"));
                }
                assertEquals("failed append must not enter the ring", 2L, ring.publishedFsn());
            } finally {
                if (manager != null && ring != null) {
                    manager.deregister(ring);
                }
                if (ring != null) {
                    ring.close();
                }
                if (manager != null) {
                    manager.close();
                }
                Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
                TestUtils.removeTmpDir(dir);
            }
        });
    }

    private static final class CountingFilesFacade implements FilesFacade {
        private boolean isFsyncFailureEnabled;
        private int fsyncCalls;
        private int msyncCalls;

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
            fsyncCalls++;
            return isFsyncFailureEnabled ? -1 : INSTANCE.fsync(fd);
        }

        @Override
        public long length(int fd) {
            return INSTANCE.length(fd);
        }

        @Override
        public long length(String path) {
            return INSTANCE.length(path);
        }

        @Override
        public long length(long pathPtr) {
            return INSTANCE.length(pathPtr);
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
        public int msync(long addr, long len, boolean async) {
            msyncCalls++;
            return INSTANCE.msync(addr, len, async);
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
