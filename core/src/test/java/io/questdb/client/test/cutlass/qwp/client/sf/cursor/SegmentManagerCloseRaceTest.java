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
import io.questdb.client.std.Files;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

            int leaked = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                Object hs = rings[i].getHotSpareForTesting();
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

    /**
     * Pins the claim-time registration gate at the top of
     * {@code SegmentManager.serviceRing}: a snapshot entry whose ring was
     * deregistered BEFORE the worker claims it must be skipped entirely —
     * never claimed as {@code inService}, never handed to
     * {@code serviceRing0}. This is the exact guarantee
     * {@code CursorSendEngine.close()} relies on when it releases the ring,
     * watermark and slot flock right after
     * {@code awaitRingQuiescence(ring) == true}: the deregistering thread may
     * already be freeing those resources, so a stale snapshot entry must not
     * be touched at all (spare install, watermark write, drainTrimmable, or
     * path building under the slot dir).
     * <p>
     * Deterministic shape: three rings A, B, C registered before start, so
     * the worker's first snapshot is exactly [A, B, C]. The worker parks in
     * A's spare-install pass; while it is parked, B is deregistered (and a
     * quiescence barrier for B must pass immediately — no pass for B is in
     * flight). After release the worker walks the rest of its stale
     * snapshot: it must skip B and service C. Every serviced ring is
     * recorded from inside the worker's own pass (via the trim-sync hook
     * reading {@code inService}), so the assertion is exact — no timing
     * grace, no sleeps.
     */
    @Test(timeout = 15_000L)
    public void testStaleSnapshotEntrySkippedAfterDeregisterBeforeServiceClaim() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            SegmentManager manager = new SegmentManager(segSize, TimeUnit.SECONDS.toNanos(60));
            SegmentRing[] rings = new SegmentRing[3];
            String[] slots = new String[3];
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean();
            AtomicReference<Throwable> hookErr = new AtomicReference<>();
            // Rings the worker actually claimed and serviced, recorded from
            // the worker thread itself at the trim-sync point every service
            // pass reaches (spare needed or not).
            java.util.Set<Object> serviced =
                    java.util.Collections.synchronizedSet(
                            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>()));
            boolean managerClosed = false;
            try {
                for (int i = 0; i < 3; i++) {
                    slots[i] = tmpDir + "/claim-skip-slot-" + i;
                    Assert.assertEquals(0, Files.mkdir(slots[i], Files.DIR_MODE_DEFAULT));
                    MmapSegment initial = MmapSegment.create(
                            slots[i] + "/sf-initial.sfa", 0L, segSize);
                    rings[i] = new SegmentRing(initial, segSize);
                }
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
                manager.setBeforeTrimSyncHook(() -> {
                    try {
                        Object ring = readInServiceRing(manager);
                        if (ring != null) {
                            serviced.add(ring);
                        }
                    } catch (Throwable t) {
                        hookErr.compareAndSet(null, t);
                    }
                });
                // Register all three BEFORE start: the worker's first snapshot
                // is [A, B, C] and every fresh ring wants a hot spare, so the
                // install hook parks the worker inside A's pass.
                manager.register(rings[0], slots[0]);
                manager.register(rings[1], slots[1]);
                manager.register(rings[2], slots[2]);
                manager.start();
                Assert.assertTrue("worker did not reach A's install pass",
                        workerBlocked.await(5, TimeUnit.SECONDS));

                // B is deregistered while its snapshot entry is still ahead of
                // the worker's cursor. The quiescence barrier must pass at
                // once: the in-flight pass is A's, not B's — this is the state
                // in which an engine owner frees B's resources.
                manager.deregister(rings[1]);
                Assert.assertTrue("no pass for B is in flight — the barrier must pass immediately",
                        manager.awaitRingQuiescence(rings[1]));

                releaseWorker.countDown();

                // Positive marker that the worker walked PAST B's snapshot
                // slot: C sits after B in the same snapshot, so once C has
                // been serviced, B's claim-or-skip decision has been made.
                long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!serviced.contains(rings[2])) {
                    if (System.nanoTime() > deadlineNs) {
                        throw new AssertionError("worker never serviced ring C after release");
                    }
                    Thread.sleep(1);
                }

                Assert.assertTrue("ring A must have been serviced", serviced.contains(rings[0]));
                Assert.assertFalse(
                        "worker claimed and serviced a snapshot entry that was deregistered "
                                + "before its pass started — the deregistering thread may already "
                                + "be releasing the ring/watermark/slot lock, so a stale snapshot "
                                + "entry must be skipped at claim time",
                        serviced.contains(rings[1]));
                // Defense in depth: nothing may have been installed into the
                // deregistered ring either.
                Assert.assertNull("no hot spare may be installed into a deregistered ring",
                        readHotSpare(rings[1]));

                manager.close();
                managerClosed = true;
                if (hookErr.get() != null) {
                    throw new AssertionError("worker-side hook failed", hookErr.get());
                }
            } finally {
                manager.setBeforeInstallSyncHook(null);
                manager.setBeforeTrimSyncHook(null);
                releaseWorker.countDown();
                if (!managerClosed) {
                    manager.close();
                }
                for (SegmentRing ring : rings) {
                    if (ring != null) {
                        ring.close();
                    }
                }
            }
        });
    }

    /**
     * Pins the scratch-handoff half of the timed-out-close contract in
     * isolation: after {@code close()} gives up on the bounded join and hands
     * {@code pathScratch} ownership to the worker, the WORKER's exit block
     * alone must free the native buffer — with no retried {@code close()}
     * ever running. The sibling test
     * ({@link #testCloseDoesNotFreePathScratchWhenWorkerStillAlive}) retries
     * {@code close()} before asserting the free, so a regression that dropped
     * the worker-side free (leaving reclaim to a retry nobody is required to
     * make) would stay green there: production owners do NOT retry — a
     * timed-out close returns to the pool and the worker is the only thread
     * left that can reclaim the allocation.
     */
    @Test(timeout = 15_000L)
    public void testWorkerAloneFreesPathScratchAfterTimedOutClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            String slot = tmpDir + "/worker-frees-scratch-slot";
            Assert.assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
            MmapSegment initial = MmapSegment.create(slot + "/sf-initial.sfa", 0L, segSize);
            SegmentRing ring = new SegmentRing(initial, segSize);
            SegmentManager manager = new SegmentManager(segSize, TimeUnit.SECONDS.toNanos(60));
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean();
            AtomicReference<Throwable> hookErr = new AtomicReference<>();
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

                // Timed-out close: hands scratch ownership to the worker and
                // returns. This is the last close() call this test makes.
                manager.setWorkerJoinTimeoutMillis(50L);
                manager.close();
                Thread worker = readWorkerThread(manager);
                Assert.assertTrue("worker must still be live after the timed-out close",
                        worker != null && worker.isAlive());
                Assert.assertTrue("scratch must still be allocated while the worker may use it",
                        readPathScratchImpl(manager) != 0L);

                // Release the pass. running=false already, so the worker
                // finishes the pass and exits — its exit block must free the
                // handed-over scratch buffer without ANY further caller action.
                releaseWorker.countDown();
                worker.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("worker never exited after release", worker.isAlive());
                Assert.assertEquals(
                        "worker exit block must free the handed-over path scratch — no retried "
                                + "close() runs in production after a timed-out close, so leaving "
                                + "the free to a retry leaks the native buffer for the process "
                                + "lifetime",
                        0L, readPathScratchImpl(manager));

                // Reap the dead thread for tidiness; must not double-free.
                manager.close();
                Assert.assertNull("retried close must reap the exited worker",
                        readWorkerThread(manager));
                if (hookErr.get() != null) {
                    throw new AssertionError("install hook failed", hookErr.get());
                }
            } finally {
                manager.setBeforeInstallSyncHook(null);
                releaseWorker.countDown();
                manager.close();
                ring.close();
            }
        });
    }

    /**
     * Pins the {@link SegmentManager#deferUntilWorkerExit} handoff contract
     * that {@code CursorSendEngine.close()}'s slot-ownership transfer depends
     * on:
     * <ul>
     *   <li>rejects the handoff ({@code false}) when no worker ever started —
     *       the caller must clean up inline;</li>
     *   <li>accepts it ({@code true}) while the worker is live-but-slow
     *       mid service pass after a timed-out close(), and runs the cleanup
     *       exactly when the worker exits — never while the pass is still in
     *       flight;</li>
     *   <li>rejects it again once the worker loop has exited/been reaped.</li>
     * </ul>
     */
    @Test(timeout = 15_000L)
    public void testDeferUntilWorkerExitRunsCleanupAfterFinalPass() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            String slot = tmpDir + "/defer-slot";
            Assert.assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
            MmapSegment initial = MmapSegment.create(slot + "/sf-initial.sfa", 0L, segSize);
            SegmentRing ring = new SegmentRing(initial, segSize);
            SegmentManager manager = new SegmentManager(segSize, TimeUnit.SECONDS.toNanos(60));
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            CountDownLatch cleanupRan = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean();
            AtomicReference<Throwable> hookErr = new AtomicReference<>();
            boolean managerClosed = false;
            try {
                // Never-started manager: no worker will ever run the cleanup,
                // and none can touch a slot — the caller must do it inline.
                Assert.assertFalse("never-started manager must reject the handoff",
                        manager.deferUntilWorkerExit(cleanupRan::countDown));

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

                // Timed-out close: running=false, worker parked mid-pass — the
                // exact state CursorSendEngine.close() hands ownership over in.
                manager.setWorkerJoinTimeoutMillis(50L);
                manager.close();
                Assert.assertFalse("worker must not be reaped while parked mid-pass",
                        manager.isWorkerReaped());

                Assert.assertTrue("live-but-slow worker must accept the handoff",
                        manager.deferUntilWorkerExit(cleanupRan::countDown));
                Assert.assertEquals("cleanup must not run while the pass is still in flight",
                        1, cleanupRan.getCount());

                // Release the pass; the worker loop observes running=false,
                // exits, and must run the deferred cleanup on its way out.
                releaseWorker.countDown();
                Assert.assertTrue("cleanup never ran on worker exit",
                        cleanupRan.await(10, TimeUnit.SECONDS));

                // Reap the exited worker, then: no live worker, no handoff.
                manager.setWorkerJoinTimeoutMillis(TimeUnit.SECONDS.toMillis(60));
                manager.close();
                managerClosed = true;
                Assert.assertFalse("reaped manager must reject the handoff",
                        manager.deferUntilWorkerExit(() -> {
                        }));
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

    /**
     * Pins the {@link SegmentManager#awaitRingQuiescence} contract that
     * {@code CursorSendEngine.close()} depends on:
     * <ul>
     *   <li>returns {@code false} (never {@code true}) while a service pass
     *       for the ring is provably in flight and the timeout elapses;</li>
     *   <li>preserves a pending caller interrupt without aborting;</li>
     *   <li>returns {@code true} once the in-flight pass has finished.</li>
     * </ul>
     */
    @Test(timeout = 15_000L)
    public void testAwaitRingQuiescenceBlocksWhileServicePassInFlight() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            String slot = tmpDir + "/quiesce-slot";
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

                manager.deregister(ring);
                manager.setWorkerJoinTimeoutMillis(50L);
                Thread.currentThread().interrupt();
                Assert.assertFalse(
                        "awaitRingQuiescence returned true while the worker was parked "
                                + "inside the service pass for this ring",
                        manager.awaitRingQuiescence(ring));
                Assert.assertTrue("awaitRingQuiescence must preserve the caller's interrupt",
                        Thread.interrupted());

                releaseWorker.countDown();
                manager.setWorkerJoinTimeoutMillis(TimeUnit.SECONDS.toMillis(60));
                Assert.assertTrue(
                        "awaitRingQuiescence must return true once the in-flight pass finished",
                        manager.awaitRingQuiescence(ring));

                manager.close();
                managerClosed = true;
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

    private static Object readHotSpare(SegmentRing ring) {
        return ring.getHotSpareForTesting();
    }

    private static Object readInServiceRing(SegmentManager manager) {
        return manager.getInServiceRingForTesting();
    }

    /**
     * Pins the SECOND bounded join in {@link SegmentManager#close()}: when
     * the first (tunable) join times out but the worker has already left its
     * service loop ({@code workerLoopExited}) and is running only its finite
     * exit cleanups, close() must NOT hand off and walk away -- it gives the
     * cleanups a second, fixed-budget join and reaps the worker once they
     * finish. Without it, a bounded-join timeout landing mid-cleanup reaps
     * the worker while an engine's flock release is still in flight, so
     * callers observe a stale not-yet-closed state and schedule spurious
     * flock-release retries. Every existing close-race test parks the worker
     * MID-PASS (first-join territory); none reached this branch.
     */
    @Test(timeout = 20_000L)
    public void testSecondBoundedJoinReapsWorkerFinishingExitCleanups() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            String slot = tmpDir + "/second-join-slot";
            Assert.assertEquals(0, Files.mkdir(slot, Files.DIR_MODE_DEFAULT));
            MmapSegment initial = MmapSegment.create(slot + "/sf-initial.sfa", 0L, segSize);
            SegmentRing ring = new SegmentRing(initial, segSize);
            SegmentManager manager = new SegmentManager(segSize, TimeUnit.SECONDS.toNanos(60));
            CountDownLatch cleanupEntered = new CountDownLatch(1);
            CountDownLatch releaseCleanup = new CountDownLatch(1);
            AtomicReference<Throwable> err = new AtomicReference<>();
            Thread releaser = null;
            try {
                manager.register(ring, slot);
                manager.start();
                // Deferred exit cleanup that parks: once the worker runs it,
                // workerLoopExited is already true (set before cleanups) and
                // the thread is alive in its finite exit phase -- exactly the
                // second-join state.
                Assert.assertTrue("live worker must accept the exit-cleanup handoff",
                        manager.deferUntilWorkerExit(() -> {
                            cleanupEntered.countDown();
                            try {
                                if (!releaseCleanup.await(10, TimeUnit.SECONDS)) {
                                    err.compareAndSet(null, new AssertionError(
                                            "timed out waiting for test to release the exit cleanup"));
                                }
                            } catch (Throwable t) {
                                err.compareAndSet(null, t);
                            }
                        }));
                Thread worker = readWorkerThread(manager);
                Assert.assertNotNull(worker);

                // Hold the cleanup past the first join (200ms) and release it
                // well inside the second join's fixed 5s budget.
                releaser = new Thread(() -> {
                    try {
                        if (!cleanupEntered.await(10, TimeUnit.SECONDS)) {
                            err.compareAndSet(null, new AssertionError(
                                    "worker never reached its exit cleanups"));
                            return;
                        }
                        Thread.sleep(400L);
                    } catch (Throwable t) {
                        err.compareAndSet(null, t);
                    } finally {
                        releaseCleanup.countDown();
                    }
                }, "second-join-releaser");
                releaser.start();

                // close(): running=false, worker exits its loop promptly (it
                // is idle), sets workerLoopExited, parks in the cleanup. The
                // first join (200ms) expires against the parked cleanup; the
                // fall-through must take the second join, which reaps once
                // the releaser lets the cleanup finish.
                manager.setWorkerJoinTimeoutMillis(200L);
                manager.close();

                Assert.assertEquals("worker must have been parked in its exit cleanups",
                        0, cleanupEntered.getCount());
                Assert.assertTrue(
                        "second bounded join must reap a worker that finishes its exit "
                                + "cleanups inside the fixed budget -- close() returned unreaped",
                        manager.isWorkerReaped());
                worker.join(TimeUnit.SECONDS.toMillis(5));
                Assert.assertFalse("worker must be dead after the second join reaped it",
                        worker.isAlive());
                Assert.assertEquals(
                        "close() must free the path scratch itself after the second join "
                                + "confirmed termination (no handoff on this branch)",
                        0L, readPathScratchImpl(manager));
                if (err.get() != null) {
                    throw new AssertionError("async participant failed", err.get());
                }
            } finally {
                releaseCleanup.countDown();
                if (releaser != null) {
                    releaser.join(TimeUnit.SECONDS.toMillis(10));
                }
                Thread.interrupted();
                manager.close();
                ring.close();
            }
        });
    }

    private static long readPathScratchImpl(SegmentManager manager) {
        return manager.isPathScratchAllocatedForTesting() ? 1L : 0L;
    }

    private static Thread readWorkerThread(SegmentManager manager) {
        return manager.getWorkerThreadForTesting();
    }
}
