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
import static org.junit.Assert.assertTrue;

/**
 * Deterministic test for {@code SegmentManager.totalBytes} drift on the
 * <b>install</b> path of {@code serviceRing}. The bug: between the worker's
 * "decide to install a spare" and "commit the +segmentSize under lock", a
 * concurrent {@code deregister(ring)} would subtract the ring's bytes (which
 * at that moment don't include the in-flight spare) and the worker would
 * still commit, inflating {@code totalBytes} by one segment per occurrence
 * with no future subtractor.
 *
 * <p>Drives the race with the {@code beforeInstallSyncHook} seam on
 * {@code SegmentManager}, which fires on the worker thread immediately
 * before the install block's {@code synchronized(lock)}. The hook performs
 * the deregister synchronously; when the worker subsequently enters the
 * synchronized block, the stillRegistered re-check sees the entry removed
 * and skips the (otherwise drifting) install + commit.
 *
 * <p>Pre-fix the test ends with {@code totalBytes > 0}; post-fix it ends at
 * exactly {@code 0}. No stress, no concurrency in setup, no spin loops in
 * the assertion path: the hook fires exactly once, on a single worker tick
 * the test triggers via the ring's managerWakeup callback.
 */
public class SegmentManagerTotalBytesRaceTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-segmgr-install-race-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        rmDirRecursive(tmpDir);
    }

    @Test(timeout = 15_000L)
    public void testInstallPathDoesNotCommitAfterDeregister() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // One frame per segment, so the very first append forces the
            // ring into needsHotSpare and wakes the manager.
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            long maxTotal = segSize * 8L;

            // Large pollNanos: the worker parks until explicitly woken. The
            // ring's appendOrFsn fires managerWakeup when it needs a spare,
            // so the producer-driven setup phase still gets prompt service.
            try (SegmentManager mgr = new SegmentManager(
                    segSize, TimeUnit.SECONDS.toNanos(60), maxTotal)) {
                mgr.start();

                String dir = tmpDir + "/single-ring";
                assertEquals(0, Files.mkdir(dir, Files.DIR_MODE_DEFAULT));
                String activePath = dir + "/sf-initial.sfa";
                MmapSegment seg0 = MmapSegment.create(activePath, 0L, segSize);
                SegmentRing ring = new SegmentRing(seg0, segSize);

                long buf = Unsafe.malloc(32, MemoryTag.NATIVE_DEFAULT);
                try {
                    mgr.register(ring, dir);
                    long bytesAfterRegister = readTotalBytes(mgr);
                    assertEquals("register should account for the initial segment",
                            segSize, bytesAfterRegister);

                    // Hook fires on the worker thread immediately before the
                    // install block's synchronized(lock) — i.e. AFTER the
                    // worker has snapshotted observedTotal, dropped the lock,
                    // and finished MmapSegment.create, but BEFORE it tries to
                    // commit +segmentSize. The hook deregisters the ring
                    // synchronously, then the worker enters the lock, the
                    // stillRegistered check sees the entry removed, and skips
                    // the install + commit. Without the guard the worker
                    // would still commit and drift totalBytes by +segSize.
                    CountDownLatch hookDone = new CountDownLatch(1);
                    AtomicBoolean fired = new AtomicBoolean();
                    AtomicReference<Throwable> hookErr = new AtomicReference<>();
                    setBeforeInstallSyncHook(mgr, () -> {
                        if (!fired.compareAndSet(false, true)) return;
                        try {
                            mgr.deregister(ring);
                        } catch (Throwable t) {
                            hookErr.compareAndSet(null, t);
                        } finally {
                            hookDone.countDown();
                        }
                    });

                    // Fill the single-frame active segment. The ring's
                    // needsHotSpare flips to true and managerWakeup fires,
                    // unparking the worker; serviceRing enters the install
                    // path and triggers the hook.
                    ring.appendOrFsn(buf, 32);

                    assertTrue("install hook never fired",
                            hookDone.await(5, TimeUnit.SECONDS));
                    if (hookErr.get() != null) {
                        throw new AssertionError("install hook failed", hookErr.get());
                    }

                    // Wait for the worker to park again. With the entry
                    // deregistered, no further wakeups arrive and a 60 s
                    // pollNanos makes TIMED_WAITING a strong signal that
                    // the current tick (snapshot + serviceRing + iteration
                    // exit) has finished.
                    Thread worker = workerThread(mgr);
                    awaitParked(worker);

                    long observed = readTotalBytes(mgr);
                    assertEquals("totalBytes drifted away from 0. Observed "
                                    + observed + " (segSize=" + segSize + ", "
                                    + "bytesAfterRegister=" + bytesAfterRegister + "). "
                                    + "Positive drift means SegmentManager.serviceRing's "
                                    + "install block committed +segmentSize after deregister "
                                    + "had already subtracted ring.totalSegmentBytes(): no "
                                    + "stillRegistered guard. Fix: gate installHotSpare + "
                                    + "totalBytes += segmentSize on a stillRegistered re-check "
                                    + "under the same lock that covers deregister.",
                            0L, observed);
                } finally {
                    setBeforeInstallSyncHook(mgr, null);
                    Unsafe.free(buf, 32, MemoryTag.NATIVE_DEFAULT);
                    try {
                        ring.close();
                    } catch (Throwable ignored) {
                        // best-effort
                    }
                }
            }
        });
    }

    private static void awaitParked(Thread t) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (true) {
            Thread.State s = t.getState();
            if (s == Thread.State.TIMED_WAITING || s == Thread.State.WAITING) return;
            if (System.nanoTime() > deadline) {
                throw new AssertionError("worker did not park within 5 s; state=" + s);
            }
            Thread.onSpinWait();
        }
    }

    private static long readTotalBytes(SegmentManager mgr) throws Exception {
        Field f = SegmentManager.class.getDeclaredField("totalBytes");
        f.setAccessible(true);
        Field lockF = SegmentManager.class.getDeclaredField("lock");
        lockF.setAccessible(true);
        Object lock = lockF.get(mgr);
        synchronized (lock) {
            return f.getLong(mgr);
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

    private static void setBeforeInstallSyncHook(SegmentManager mgr, Runnable hook) throws Exception {
        Field f = SegmentManager.class.getDeclaredField("beforeInstallSyncHook");
        f.setAccessible(true);
        f.set(mgr, hook);
    }

    private static Thread workerThread(SegmentManager mgr) throws Exception {
        Field f = SegmentManager.class.getDeclaredField("workerThread");
        f.setAccessible(true);
        return (Thread) f.get(mgr);
    }
}
