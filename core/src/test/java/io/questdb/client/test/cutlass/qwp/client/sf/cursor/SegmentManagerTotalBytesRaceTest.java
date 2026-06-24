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
 * <p>Concurrent but non-racy: {@code mgr.register} and
 * {@code setBeforeInstallSyncHook} both run before {@code mgr.start}, so
 * the worker's first iteration deterministically observes the registered
 * ring (with {@code hotSpare == null}, so {@code needsHotSpare()} is true)
 * and the hook. The hook fires exactly once and signals the test via a
 * {@code CountDownLatch}; the test then waits for the worker to park
 * before reading {@code totalBytes}.
 *
 * <p>Pre-fix the test ends with {@code totalBytes > 0}; post-fix it ends
 * at exactly {@code 0}.
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
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            long maxTotal = segSize * 8L;

            try (SegmentManager mgr = new SegmentManager(
                    segSize, TimeUnit.SECONDS.toNanos(60), maxTotal)) {
                String dir = tmpDir + "/single-ring";
                assertEquals(0, Files.mkdir(dir, Files.DIR_MODE_DEFAULT));
                String activePath = dir + "/sf-initial.sfa";
                MmapSegment seg0 = MmapSegment.create(activePath, 0L, segSize);
                SegmentRing ring = new SegmentRing(seg0, segSize);

                try {
                    // Register + read totalBytes BEFORE the worker exists.
                    // No worker thread means readTotalBytes here is the
                    // pure effect of register: exactly one initial segment.
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
                    mgr.setBeforeInstallSyncHook(() -> {
                        if (!fired.compareAndSet(false, true)) return;
                        try {
                            mgr.deregister(ring);
                        } catch (Throwable t) {
                            hookErr.compareAndSet(null, t);
                        } finally {
                            hookDone.countDown();
                        }
                    });

                    // Concurrent but non-racy: register + hook are visible
                    // before the worker exists (Thread.start() establishes
                    // a happens-before edge). hotSpare is null at construction,
                    // so needsHotSpare() returns true on the worker's first
                    // and only relevant iteration -- no producer-side append
                    // needed to trigger the install. The hook then fires
                    // exactly once and hookDone signals the test.
                    mgr.start();

                    assertTrue("install hook never fired",
                            hookDone.await(5, TimeUnit.SECONDS));
                    if (hookErr.get() != null) {
                        throw new AssertionError("install hook failed", hookErr.get());
                    }

                    // Wait for the worker to park again. With the entry
                    // deregistered, the next loop iteration finds rings
                    // empty and parks for the full 60 s pollNanos. That
                    // TIMED_WAITING transition is the test's signal that
                    // the worker has finished serviceRing -- including the
                    // stillRegistered re-check and the (skipped) commit --
                    // so readTotalBytes below observes the final state.
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
                    mgr.setBeforeInstallSyncHook(null);
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
            io.questdb.client.std.Compat.onSpinWait();
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

    private static Thread workerThread(SegmentManager mgr) throws Exception {
        Field f = SegmentManager.class.getDeclaredField("workerThread");
        f.setAccessible(true);
        return (Thread) f.get(mgr);
    }
}
