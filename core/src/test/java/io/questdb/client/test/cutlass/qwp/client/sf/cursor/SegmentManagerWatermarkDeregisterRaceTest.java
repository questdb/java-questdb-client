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
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Deterministic regression for the stale snapshot path where the manager worker
 * keeps servicing a snapshotted ring entry after
 * {@link SegmentManager#deregister(SegmentRing)} has removed it from the live
 * registry.
 *
 * <p>This is intentionally a crash-capable regression. The worker is stopped
 * at the stale-snapshot window, the test deregisters the ring and releases the
 * watermark mapping, then the worker is allowed to continue. Correct code
 * observes {@code registered=false} and never calls {@code watermark.write()}.
 * Buggy code writes through the stale {@link AckWatermark} object into an
 * unmapped page and should take down the test JVM.
 */
public class SegmentManagerWatermarkDeregisterRaceTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-segmgr-watermark-race-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        rmDirRecursive(tmpDir);
    }

    @Test(timeout = 15_000L)
    public void testStaleWorkerDoesNotWriteThroughUnmappedWatermarkAfterDeregister() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE
                    + 4 * (MmapSegment.FRAME_HEADER_SIZE + 32);
            String slot = tmpDir + "/single-ring";
            assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));

            MmapSegment seg0 = MmapSegment.create(slot + "/sf-initial.sfa", 0L, segSize);
            SegmentRing ring = new SegmentRing(seg0, segSize);
            AckWatermark watermark = AckWatermark.open(slot);
            assertNotNull(watermark);
            SegmentManager mgr = new SegmentManager(segSize, TimeUnit.SECONDS.toNanos(60));
            long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
            boolean managerClosed = false;
            boolean watermarkStorageReleased = false;
            CountDownLatch workerAtStaleSnapshot = new CountDownLatch(1);
            CountDownLatch resumeWorker = new CountDownLatch(1);
            try {
                Unsafe.getUnsafe().putInt(buf, 42);
                assertEquals(0L, ring.appendOrFsn(buf, 32));
                assertTrue("precondition: ack must advance",
                        ring.acknowledge(0L));

                mgr.register(ring, slot, watermark);

                AtomicBoolean fired = new AtomicBoolean();
                AtomicReference<Throwable> hookErr = new AtomicReference<>();
                mgr.setBeforeInstallSyncHook(() -> {
                    if (!fired.compareAndSet(false, true)) return;
                    try {
                        workerAtStaleSnapshot.countDown();
                        if (!resumeWorker.await(10, TimeUnit.SECONDS)) {
                            hookErr.compareAndSet(null,
                                    new AssertionError("timed out waiting for test to release stale worker"));
                        }
                    } catch (Throwable t) {
                        hookErr.compareAndSet(null, t);
                    }
                });

                // The first manager tick snapshots the registered entry, creates
                // a spare, and stops before the install block. The main test
                // thread now performs the production-close sequence's dangerous
                // part: deregister, then release the engine-owned watermark
                // mapping while the stale worker snapshot is still live.
                mgr.start();

                assertTrue("install hook never fired",
                        workerAtStaleSnapshot.await(5, TimeUnit.SECONDS));
                mgr.deregister(ring);
                // Do not call watermark.close(): synchronizing the worker with
                // a latch after close() would give it a happens-before edge to
                // closed=true, masking the original bug's plain-boolean guard.
                // Releasing the mmap/fd directly leaves the object in the stale
                // state that a racing worker is allowed to observe.
                releaseWatermarkStorageButLeaveObjectWritable(watermark);
                watermarkStorageReleased = true;
                resumeWorker.countDown();
                if (hookErr.get() != null) {
                    throw new AssertionError("install hook failed", hookErr.get());
                }
                mgr.close();
                managerClosed = true;
            } finally {
                mgr.setBeforeInstallSyncHook(null);
                if (!watermarkStorageReleased) {
                    resumeWorker.countDown();
                }
                if (!managerClosed) {
                    mgr.close();
                }
                try {
                    ring.close();
                } catch (Throwable ignored) {
                    // best-effort
                }
                if (!watermarkStorageReleased) {
                    try {
                        watermark.close();
                    } catch (Throwable ignored) {
                        // best-effort
                    }
                }
                Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static void releaseWatermarkStorageButLeaveObjectWritable(AckWatermark watermark) throws Exception {
        Field mmapAddressF = AckWatermark.class.getDeclaredField("mmapAddress");
        mmapAddressF.setAccessible(true);
        long mmapAddress = mmapAddressF.getLong(watermark);
        if (mmapAddress != 0L && mmapAddress != Files.FAILED_MMAP_ADDRESS) {
            Files.munmap(mmapAddress, AckWatermark.FILE_SIZE, MemoryTag.MMAP_DEFAULT);
        }

        Field fdF = AckWatermark.class.getDeclaredField("fd");
        fdF.setAccessible(true);
        int fd = fdF.getInt(watermark);
        if (fd >= 0) {
            Files.close(fd);
        }
    }

    private static void rmDirRecursive(String dir) {
        long find = Files.findFirst(dir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        String child = dir + "/" + name;
                        if (!Files.remove(child)) {
                            rmDirRecursive(child);
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
}
