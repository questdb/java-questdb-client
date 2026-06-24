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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Deterministic test for {@code SegmentManager.totalBytes} drift on the
 * <b>trim</b> path of {@code serviceRing}. The bug: the trim loop subtracts
 * {@code s.sizeBytes()} per drained segment with no {@code stillRegistered}
 * re-check, so if {@code deregister(ring)} fires between the worker's
 * {@code rings} snapshot and the trim block's {@code synchronized(lock)},
 * deregister's {@code totalBytes -= ring.totalSegmentBytes()} already
 * accounts for those sealed segments and the loop double-counts them.
 *
 * <p>Drives the race with the {@code beforeTrimSyncHook} seam on
 * {@code SegmentManager}, which fires on the worker thread immediately
 * before the trim block's {@code synchronized(lock)}. The hook performs the
 * deregister synchronously; when the worker subsequently enters the
 * synchronized block, the stillRegistered re-check sees the entry removed
 * and skips the (otherwise double-counted) subtract.
 *
 * <p>Pre-fix the test ends with {@code totalBytes < 0}; post-fix it ends at
 * exactly {@code 0}. No stress, no concurrency in setup, no spin loops in
 * the assertion path: the hook fires exactly once, on a single woken tick.
 */
public class SegmentManagerTrimDeregisterRaceTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-segmgr-trim-race-" + System.nanoTime()).toString();
        assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        rmDirRecursive(tmpDir);
    }

    @Test(timeout = 15_000L)
    public void testTrimPathDoesNotDoubleSubtractAfterDeregister() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // One frame per segment, so a second append always rotates.
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            long maxTotal = segSize * 8L;

            // Large pollNanos: the worker parks until explicitly woken, so we
            // own the tick boundaries. SegmentRing fires managerWakeup on
            // appendOrFsn whenever a spare is needed, so the producer-driven
            // setup phase still gets prompt service.
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
                    // Fill seg0 (FSN 0). The ring auto-wakes the worker to
                    // provision a spare; wait for that spare so the next
                    // append can rotate.
                    ring.appendOrFsn(buf, 32);
                    awaitSpare(ring, "after seg0 fill");
                    // Rotate: seg0 joins sealedSegments, the spare becomes
                    // active. Worker auto-wakes again to provision the next
                    // spare; whether that completes before we install the
                    // hook does not matter for the test (the hook fires only
                    // on a worker tick we trigger explicitly below).
                    ring.appendOrFsn(buf, 32);
                    awaitSpare(ring, "after rotation");
                    // FSN 0 acked: seg0 (baseSeq=0, lastSeq=0) is now
                    // trimmable. Acknowledge does NOT wake the worker, so the
                    // trim does not happen until we wakeWorker() below.
                    ring.acknowledge(0L);

                    // Snapshot totalBytes before the race. With a spare
                    // installed both before and after rotation, the ring
                    // owns 3 segments; if the second spare failed to install
                    // (e.g. allocation race we did not orchestrate), it owns
                    // 2. Either way the test invariant is "deregister +
                    // trim net to zero", regardless of the starting value.
                    long bytesBeforeRace = readTotalBytes(mgr);
                    assertTrue("expected totalBytes > 0 before race, was " + bytesBeforeRace,
                            bytesBeforeRace > 0L);

                    // Hook fires on the worker thread immediately before the
                    // trim block's synchronized(lock). It deregisters the
                    // ring synchronously, then the worker enters the lock,
                    // calls drainTrimmable() (returns seg0), the
                    // stillRegistered check sees the entry removed, and
                    // skips the subtract that would otherwise double-count
                    // the bytes deregister already accounted for.
                    AtomicBoolean fired = new AtomicBoolean();
                    CountDownLatch hookDone = new CountDownLatch(1);
                    AtomicReference<Throwable> hookErr = new AtomicReference<>();
                    mgr.setBeforeTrimSyncHook(() -> {
                        if (!fired.compareAndSet(false, true)) return;
                        try {
                            mgr.deregister(ring);
                        } catch (Throwable t) {
                            hookErr.compareAndSet(null, t);
                        } finally {
                            hookDone.countDown();
                        }
                    });

                    // Trigger exactly one worker tick — the one that fires
                    // the hook. The worker may run additional ticks if the
                    // hook's deregister itself wakes it, but the fired CAS
                    // makes subsequent invocations no-ops.
                    mgr.wakeWorker();

                    assertTrue("hook never fired", hookDone.await(5, TimeUnit.SECONDS));
                    if (hookErr.get() != null) {
                        throw new AssertionError("hook failed", hookErr.get());
                    }

                    // Wait for the worker to park again, which is our signal
                    // that the trim block following the hook has finished
                    // (drain + maybe-subtract + lock release + iteration
                    // exit). The 60 s pollNanos means TIMED_WAITING is a
                    // strong signal; with the entry deregistered, no further
                    // wakeups arrive.
                    Thread worker = workerThread(mgr);
                    awaitParked(worker);

                    long observed = readTotalBytes(mgr);
                    assertEquals("totalBytes drifted away from 0. Observed "
                                    + observed + " (segSize=" + segSize + ", "
                                    + "bytesBeforeRace=" + bytesBeforeRace + "). "
                                    + "Negative drift means SegmentManager.serviceRing's "
                                    + "trim loop subtracted bytes that deregister had "
                                    + "already subtracted via ring.totalSegmentBytes(): "
                                    + "no stillRegistered guard. Fix: gate "
                                    + "`totalBytes -= sz` on a stillRegistered re-check "
                                    + "under the same lock that covers deregister.",
                            0L, observed);
                    assertFalse("stale SegmentManager snapshot skipped drainTrimmable() "
                                    + "after deregister and left a fully-acked sealed "
                                    + "segment on disk. The registration guard should "
                                    + "protect watermark/accounting only; trim ownership "
                                    + "transfer must still close and unlink " + activePath,
                            Files.exists(activePath));
                } finally {
                    mgr.setBeforeTrimSyncHook(null);
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
            io.questdb.client.std.Compat.onSpinWait();
        }
    }

    private static void awaitSpare(SegmentRing ring, String where) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (ring.needsHotSpare()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("spare never installed " + where);
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
