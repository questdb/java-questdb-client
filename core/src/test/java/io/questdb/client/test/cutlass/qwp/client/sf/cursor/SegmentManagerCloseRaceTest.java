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
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.std.bytes.DirectByteSink;
import io.questdb.client.std.Files;
import io.questdb.client.std.str.DirectUtf8Sink;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Concurrent regression for the {@code SegmentManager} worker race vs
 * ring deregister/close.
 * <p>
 * The manager's worker loop snapshots {@code rings} under a lock, then
 * services each ring outside the lock. If a user thread calls
 * {@code deregister(ring)} + {@code ring.close()} between the snapshot
 * and {@code installHotSpare}, the manager:
 * <ul>
 *   <li>creates a new {@code MmapSegment} (mmap + fd + on-disk file)</li>
 *   <li>calls {@code ring.installHotSpare(spare)} on the closed ring —
 *       which sees {@code hotSpare == null} (just zeroed by close) and
 *       silently accepts the install</li>
 * </ul>
 * The spare's mmap + fd are now permanently leaked: nothing will ever
 * close them because {@code close()} already ran.
 * <p>
 * Detection: after the manager has joined, reflect into each closed
 * ring's {@code hotSpare} field. A non-null value means a spare was
 * installed AFTER {@code close()} zeroed the field — i.e. exactly the
 * leak path. We close any survivors so the test itself doesn't leak.
 */
public class SegmentManagerCloseRaceTest {

    private static final int ITERATIONS = 200;
    private static final long SEGMENT_SIZE = 64 * 1024;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-mgr-close-race-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        cleanupRecursively(tmpDir);
        Files.remove(tmpDir);
    }

    @Test
    public void testManagerDoesNotInstallSpareIntoClosedRing() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Aggressive 1us poll so the worker is almost always running
            // serviceRing — maximizes overlap with concurrent deregister/close.
            SegmentManager manager = new SegmentManager(SEGMENT_SIZE, 1_000L,
                    Long.MAX_VALUE);
            manager.start();

            SegmentRing[] rings = new SegmentRing[ITERATIONS];
            try {
                for (int i = 0; i < ITERATIONS; i++) {
                    String slot = tmpDir + "/slot-" + i;
                    Assert.assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
                    MmapSegment initial = MmapSegment.create(
                            slot + "/sf-initial.sfa", 0L, SEGMENT_SIZE);
                    rings[i] = new SegmentRing(initial, SEGMENT_SIZE);
                    manager.register(rings[i], slot);
                    // Immediately deregister + close. The manager may be mid-
                    // serviceRing for this very ring, having already created a
                    // spare and not yet installed it — that's the race window.
                    manager.deregister(rings[i]);
                    rings[i].close();
                }
            } finally {
                // join the worker so any in-flight serviceRing finishes
                // BEFORE we inspect the rings — otherwise a later install
                // could escape detection.
                manager.close();
            }

            Field hotSpareField = SegmentRing.class.getDeclaredField("hotSpare");
            hotSpareField.setAccessible(true);

            int leaked = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                Object hs = hotSpareField.get(rings[i]);
                if (hs != null) {
                    leaked++;
                    // Don't leak in the test: close the survivor.
                    ((MmapSegment) hs).close();
                }
            }

            Assert.assertEquals(
                    "SegmentManager installed hot spares into closed rings — "
                            + "spare mmap/fd permanently leaked",
                    0, leaked);
        });
    }

    @Test(timeout = 15_000L)
    public void testCloseDoesNotFreePathScratchWhenWorkerStillAlive() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            String slot = tmpDir + "/timeout-slot";
            Assert.assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
            MmapSegment initial = MmapSegment.create(slot + "/sf-initial.sfa", 0L, segSize);
            SegmentRing ring = new SegmentRing(initial, segSize);
            SegmentManager manager = new SegmentManager(segSize, TimeUnit.SECONDS.toNanos(60));
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean();
            AtomicReference<Throwable> hookErr = new AtomicReference<>();
            boolean managerClosed = false;
            try {
                manager.register(ring, slot);
                manager.setBeforeInstallSyncHook(() -> {
                    if (!fired.compareAndSet(false, true)) return;
                    workerBlocked.countDown();
                    try {
                        if (!releaseWorker.await(10, TimeUnit.SECONDS)) {
                            hookErr.compareAndSet(null,
                                    new AssertionError("timed out waiting for test to release worker"));
                        }
                    } catch (Throwable t) {
                        hookErr.compareAndSet(null, t);
                    }
                });
                manager.start();
                Assert.assertTrue("worker did not reach install hook",
                        workerBlocked.await(5, TimeUnit.SECONDS));
                Assert.assertTrue("precondition: path scratch should be allocated",
                        readPathScratchImpl(manager) != 0L);

                manager.setWorkerJoinTimeoutMillis(50L);
                Thread.currentThread().interrupt();
                manager.close();
                Assert.assertTrue("close should preserve interrupted status",
                        Thread.interrupted());
                Thread worker = readWorkerThread(manager);
                Assert.assertTrue("worker should still be tracked after incomplete close",
                        worker != null && worker.isAlive());
                Assert.assertTrue("path scratch was freed while worker was still alive",
                        readPathScratchImpl(manager) != 0L);

                releaseWorker.countDown();
                manager.setWorkerJoinTimeoutMillis(TimeUnit.SECONDS.toMillis(60));
                manager.close();
                managerClosed = true;
                Assert.assertNull("successful close should clear workerThread",
                        readWorkerThread(manager));
                Assert.assertEquals("successful close should free path scratch",
                        0L, readPathScratchImpl(manager));
                if (hookErr.get() != null) {
                    throw new AssertionError("install hook failed", hookErr.get());
                }
            } finally {
                manager.setBeforeInstallSyncHook(null);
                releaseWorker.countDown();
                if (!managerClosed) {
                    Thread.interrupted();
                    manager.close();
                }
                ring.close();
            }
        });
    }

    @Test(timeout = 15_000L)
    public void testInterruptedCallerDoesNotAbandonReapableWorker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            String slot = tmpDir + "/interrupt-slot";
            Assert.assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
            MmapSegment initial = MmapSegment.create(slot + "/sf-initial.sfa", 0L, segSize);
            SegmentRing ring = new SegmentRing(initial, segSize);
            SegmentManager manager = new SegmentManager(segSize, TimeUnit.SECONDS.toNanos(60));
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            CountDownLatch closeReturned = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean();
            AtomicBoolean interruptPreserved = new AtomicBoolean();
            AtomicReference<Throwable> hookErr = new AtomicReference<>();
            AtomicReference<Throwable> closeErr = new AtomicReference<>();
            boolean managerClosed = false;
            try {
                manager.register(ring, slot);
                manager.setBeforeInstallSyncHook(() -> {
                    if (!fired.compareAndSet(false, true)) return;
                    workerBlocked.countDown();
                    try {
                        if (!releaseWorker.await(10, TimeUnit.SECONDS)) {
                            hookErr.compareAndSet(null,
                                    new AssertionError("timed out waiting for test to release worker"));
                        }
                    } catch (Throwable t) {
                        hookErr.compareAndSet(null, t);
                    }
                });
                manager.start();
                Assert.assertTrue("worker did not reach install hook",
                        workerBlocked.await(5, TimeUnit.SECONDS));

                Thread closer = new Thread(() -> {
                    Thread.currentThread().interrupt();
                    try {
                        manager.close();
                    } catch (Throwable t) {
                        closeErr.compareAndSet(null, t);
                    } finally {
                        interruptPreserved.set(Thread.currentThread().isInterrupted());
                        closeReturned.countDown();
                    }
                }, "interrupted-closer");
                closer.start();

                Assert.assertFalse("interrupted close() abandoned a live worker instead of waiting",
                        closeReturned.await(300, TimeUnit.MILLISECONDS));

                releaseWorker.countDown();
                Assert.assertTrue("close() never returned after the worker was released",
                        closeReturned.await(10, TimeUnit.SECONDS));
                closer.join(TimeUnit.SECONDS.toMillis(5));
                managerClosed = readWorkerThread(manager) == null;

                if (closeErr.get() != null) {
                    throw new AssertionError("close() threw", closeErr.get());
                }
                Assert.assertNull("close() must reap the worker despite the pending interrupt",
                        readWorkerThread(manager));
                Assert.assertTrue("close() must restore the caller's interrupt status",
                        interruptPreserved.get());
                if (hookErr.get() != null) {
                    throw new AssertionError("install hook failed", hookErr.get());
                }
            } finally {
                manager.setBeforeInstallSyncHook(null);
                releaseWorker.countDown();
                if (!managerClosed) {
                    Thread.interrupted();
                    manager.close();
                }
                ring.close();
            }
        });
    }

    private static void cleanupRecursively(String dir) {
        if (!Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find <= 0) return;
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                if (name != null && !".".equals(name) && !"..".equals(name)) {
                    String child = dir + "/" + name;
                    // best-effort: try as file; if remove fails, recurse.
                    if (!Files.remove(child)) {
                        cleanupRecursively(child);
                        Files.remove(child);
                    }
                }
                rc = Files.findNext(find);
            }
        } finally {
            Files.findClose(find);
        }
    }

    private static long readPathScratchImpl(SegmentManager manager) throws Exception {
        Field pathScratchF = SegmentManager.class.getDeclaredField("pathScratch");
        pathScratchF.setAccessible(true);
        DirectUtf8Sink pathScratch = (DirectUtf8Sink) pathScratchF.get(manager);
        Field sinkF = DirectUtf8Sink.class.getDeclaredField("sink");
        sinkF.setAccessible(true);
        DirectByteSink sink = (DirectByteSink) sinkF.get(pathScratch);
        Field implF = DirectByteSink.class.getDeclaredField("impl");
        implF.setAccessible(true);
        return implF.getLong(sink);
    }

    private static Thread readWorkerThread(SegmentManager manager) throws Exception {
        Field workerThreadF = SegmentManager.class.getDeclaredField("workerThread");
        workerThreadF.setAccessible(true);
        return (Thread) workerThreadF.get(manager);
    }
}
