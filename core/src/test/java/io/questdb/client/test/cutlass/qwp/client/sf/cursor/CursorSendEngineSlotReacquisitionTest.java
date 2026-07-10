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

import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Engine-level regression for the shutdown hazard where
 * {@link CursorSendEngine#close()} released the slot lock, closed the ring
 * and watermark, and unlinked segment files while the shared
 * {@link SegmentManager} worker was still mid service pass for the engine's
 * ring. A replacement engine could acquire the same slot the moment the
 * lock was released, after which the stale worker's abandon/trim path could
 * unlink a segment path the replacement was actively writing through —
 * store-and-forward data loss after restart.
 * <p>
 * The fix makes {@code close()} run a quiescence barrier
 * ({@link SegmentManager#awaitRingQuiescence}) after {@code deregister} and
 * refuse to release any worker-reachable resource (ring, watermark, segment
 * files, slot lock) until the barrier confirms the worker cannot touch the
 * slot again. On barrier timeout an owned-manager engine hands cleanup
 * ownership to the worker's exit path (see
 * {@code SegmentManager.deferUntilWorkerExit}); a shared-manager engine
 * deliberately leaks and a later {@code close()} retries the cleanup.
 */
public class CursorSendEngineSlotReacquisitionTest {

    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-engine-slot-reacq-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        rmDirRecursive(tmpDir);
        Files.remove(tmpDir);
    }

    /**
     * The structural guarantee: while the manager worker is provably still
     * inside a service pass for the engine's ring, {@code close()} must NOT
     * hand the slot to anyone else. With the quiescence barrier reverted,
     * close() releases the slot lock immediately and the mid-test
     * {@code SlotLock.acquire} probe succeeds — failing the test.
     */
    @Test(timeout = 30_000L)
    public void testCloseRetainsSlotWhileWorkerIsMidServicePass() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            String slot = tmpDir + "/slot";
            // 60 s poll: the worker only acts when explicitly woken, so the
            // single pass we park below is the only pass in flight.
            SegmentManager manager = new SegmentManager(segSize, TimeUnit.SECONDS.toNanos(60));
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean();
            AtomicReference<Throwable> hookErr = new AtomicReference<>();
            boolean managerClosed = false;
            CursorSendEngine engine = null;
            try {
                manager.setBeforeInstallSyncHook(() -> {
                    if (!fired.compareAndSet(false, true)) return;
                    workerBlocked.countDown();
                    try {
                        if (!releaseWorker.await(20, TimeUnit.SECONDS)) {
                            hookErr.compareAndSet(null,
                                    new AssertionError("timed out waiting for test to release worker"));
                        }
                    } catch (Throwable t) {
                        hookErr.compareAndSet(null, t);
                    }
                });
                manager.start();

                // Shared manager: ownsManager=false, so engine close() cannot
                // fall back on manager.close()'s join — the per-ring barrier
                // is the only protection, which is exactly what we pin here.
                engine = new CursorSendEngine(slot, segSize, manager);
                Assert.assertTrue("worker never reached the install hook",
                        workerBlocked.await(5, TimeUnit.SECONDS));

                // Barrier must time out fast: the worker is parked inside the
                // service pass for this engine's ring.
                manager.setWorkerJoinTimeoutMillis(50L);
                engine.close();
                Assert.assertFalse("incomplete close must remain observable to the owner",
                        engine.isCloseCompleted());

                // The slot must still be locked: a replacement engine (or raw
                // SlotLock) acquiring it now would race the stale worker.
                try {
                    SlotLock probe = SlotLock.acquire(slot);
                    probe.close();
                    Assert.fail("engine.close() released the slot lock while the manager "
                            + "worker was still mid service pass for its ring — a "
                            + "replacement engine could acquire the slot and have its "
                            + "segment files unlinked by the stale worker");
                } catch (Exception expected) {
                    // good — slot retained.
                }

                // Let the worker finish its pass (it abandons the spare: the
                // ring was deregistered by the close attempt above).
                releaseWorker.countDown();
                manager.setWorkerJoinTimeoutMillis(TimeUnit.SECONDS.toMillis(60));

                // Retry close(): the barrier now succeeds and the full cleanup
                // (ring, watermark, unlink, slot lock) must complete.
                engine.close();
                Assert.assertTrue("retried close must report complete cleanup",
                        engine.isCloseCompleted());
                engine = null;

                try (SlotLock probe = SlotLock.acquire(slot)) {
                    Assert.assertNotNull("slot must be acquirable after a completed close", probe);
                } catch (Exception e) {
                    throw new AssertionError("retried close() did not release the slot lock", e);
                }

                manager.close();
                managerClosed = true;
                if (hookErr.get() != null) {
                    throw new AssertionError("install hook failed", hookErr.get());
                }
            } finally {
                manager.setBeforeInstallSyncHook(null);
                releaseWorker.countDown();
                if (engine != null) {
                    try {
                        engine.close();
                    } catch (Throwable ignored) {
                    }
                }
                if (!managerClosed) {
                    manager.close();
                }
            }
        });
    }

    /**
     * Owned-manager twin of {@link #testCloseRetainsSlotWhileWorkerIsMidServicePass}:
     * the ONLY construction shape production uses (Sender.build, BackgroundDrainer,
     * QwpWebSocketSender.connect all own their manager). The owned close path does
     * not run the per-ring barrier at all — it relies on {@code manager.close()}'s
     * bounded join and the {@code isWorkerReaped()} check. If that check regressed
     * to report quiescence unconditionally (or {@code isWorkerReaped()} itself
     * returned true while the worker is alive), close() would release the slot
     * lock mid service pass and the shared-manager tests would stay green — this
     * test is the red gate for the production path.
     * <p>
     * Determinism: the owned manager starts inside the engine ctor (1 ms poll),
     * so its first spare-install pass races test setup. We first wait until the
     * initial hot spare is installed — after that the worker cannot enter another
     * install pass until a rotation consumes the spare, so the park hook installed
     * afterwards can neither be missed nor fire early. Two appends then fill the
     * active segment and rotate onto the spare; the worker's next poll tick
     * re-enters the install pass and parks in the hook.
     */
    @Test(timeout = 30_000L)
    public void testOwnedEngineCloseRetainsSlotWhileWorkerIsMidServicePass() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int payloadLen = 32;
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + payloadLen);
            String slot = tmpDir + "/owned-parked-slot";
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean();
            AtomicReference<Throwable> hookErr = new AtomicReference<>();
            // Production shape: private, owned manager (ownsManager=true).
            CursorSendEngine engine = new CursorSendEngine(slot, segSize);
            SegmentManager manager = readManager(engine);
            long buf = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                // Phase 1: let the worker finish the initial spare install so
                // the hook below can only fire on the rotation-triggered pass.
                SegmentRing ring = readRing(engine);
                long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (ring.needsHotSpare()) {
                    if (System.nanoTime() > deadlineNs) {
                        throw new AssertionError("manager worker never installed the initial hot spare");
                    }
                    Thread.sleep(1);
                }
                manager.setBeforeInstallSyncHook(() -> {
                    if (!fired.compareAndSet(false, true)) return;
                    workerBlocked.countDown();
                    try {
                        if (!releaseWorker.await(20, TimeUnit.SECONDS)) {
                            hookErr.compareAndSet(null,
                                    new AssertionError("timed out waiting for test to release worker"));
                        }
                    } catch (Throwable t) {
                        hookErr.compareAndSet(null, t);
                    }
                });

                // Phase 2: one frame fills the active segment exactly; the
                // second forces rotation onto the spare. needsHotSpare() is
                // true again, so the worker's next tick parks in the hook.
                Unsafe.getUnsafe().putLong(buf, 0L);
                Assert.assertEquals(0L, engine.appendBlocking(buf, payloadLen));
                Assert.assertEquals(1L, engine.appendBlocking(buf, payloadLen));
                Assert.assertTrue("worker never re-entered a spare-install pass",
                        workerBlocked.await(5, TimeUnit.SECONDS));

                // Phase 3: owned close with the worker provably mid service
                // pass. manager.close()'s 50 ms join times out, the worker is
                // not reaped, and close() must retain every worker-reachable
                // resource — above all the slot flock.
                manager.setWorkerJoinTimeoutMillis(50L);
                engine.close();
                Assert.assertFalse("incomplete owned close must remain observable to the owner",
                        engine.isCloseCompleted());
                try {
                    SlotLock probe = SlotLock.acquire(slot);
                    probe.close();
                    Assert.fail("owned engine.close() released the slot lock while its manager "
                            + "worker was still mid service pass — a replacement engine could "
                            + "acquire the slot and have its segment files unlinked by the "
                            + "stale worker (the production SF-data-loss hazard)");
                } catch (Exception expected) {
                    // good — slot retained.
                }

                // Phase 4: release the worker (it abandons the spare: the ring
                // was deregistered by the close attempt) and retry. The join
                // now reaps the worker and the full cleanup must complete.
                releaseWorker.countDown();
                manager.setWorkerJoinTimeoutMillis(TimeUnit.SECONDS.toMillis(60));
                engine.close();
                Assert.assertTrue("retried owned close must report complete cleanup",
                        engine.isCloseCompleted());
                try (SlotLock probe = SlotLock.acquire(slot)) {
                    Assert.assertNotNull("slot must be acquirable after a completed close", probe);
                } catch (Exception e) {
                    throw new AssertionError("retried owned close() did not release the slot lock", e);
                }
                if (hookErr.get() != null) {
                    throw new AssertionError("install hook failed", hookErr.get());
                }
            } finally {
                Unsafe.free(buf, payloadLen, MemoryTag.NATIVE_DEFAULT);
                manager.setBeforeInstallSyncHook(null);
                releaseWorker.countDown();
                manager.setWorkerJoinTimeoutMillis(TimeUnit.SECONDS.toMillis(60));
                try {
                    engine.close();
                } catch (Throwable ignored) {
                }
            }
        });
    }

    /**
     * The ownership handoff (owned manager): when close() cannot confirm
     * worker quiescence within the bounded join, the terminal cleanup (ring,
     * watermark, flock release) transfers to the worker's exit path — the
     * worker is provably the last thread able to touch the slot directory.
     * Once the parked pass is released the worker must run the cleanup
     * itself, WITHOUT any retried {@code close()}: {@code isCloseCompleted()}
     * flips true and the slot becomes acquirable again. This is what lets a
     * pool recover a retired slot instead of losing its capacity until
     * process exit.
     */
    @Test(timeout = 30_000L)
    public void testOwnedEngineCloseHandsCleanupToWorkerExit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int payloadLen = 32;
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + payloadLen);
            String slot = tmpDir + "/owned-handoff-slot";
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean();
            AtomicReference<Throwable> hookErr = new AtomicReference<>();
            // Production shape: private, owned manager (ownsManager=true).
            CursorSendEngine engine = new CursorSendEngine(slot, segSize);
            SegmentManager manager = readManager(engine);
            long buf = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                // Phase 1: wait out the initial spare install so the park hook
                // can only fire on the rotation-triggered pass (see
                // testOwnedEngineCloseRetainsSlotWhileWorkerIsMidServicePass).
                SegmentRing ring = readRing(engine);
                long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (ring.needsHotSpare()) {
                    if (System.nanoTime() > deadlineNs) {
                        throw new AssertionError("manager worker never installed the initial hot spare");
                    }
                    Thread.sleep(1);
                }
                manager.setBeforeInstallSyncHook(() -> {
                    if (!fired.compareAndSet(false, true)) return;
                    workerBlocked.countDown();
                    try {
                        if (!releaseWorker.await(20, TimeUnit.SECONDS)) {
                            hookErr.compareAndSet(null,
                                    new AssertionError("timed out waiting for test to release worker"));
                        }
                    } catch (Throwable t) {
                        hookErr.compareAndSet(null, t);
                    }
                });

                // Phase 2: fill the active segment and rotate onto the spare;
                // the worker's next tick re-enters the install pass and parks.
                Unsafe.getUnsafe().putLong(buf, 0L);
                Assert.assertEquals(0L, engine.appendBlocking(buf, payloadLen));
                Assert.assertEquals(1L, engine.appendBlocking(buf, payloadLen));
                Assert.assertTrue("worker never re-entered a spare-install pass",
                        workerBlocked.await(5, TimeUnit.SECONDS));

                // Phase 3: owned close with the worker provably mid-pass. The
                // bounded join times out; cleanup ownership is handed to the
                // worker's exit path. Every worker-reachable resource — above
                // all the slot flock — must still be retained at this point.
                manager.setWorkerJoinTimeoutMillis(50L);
                engine.close();
                Assert.assertFalse("close must stay incomplete while the worker holds the handoff",
                        engine.isCloseCompleted());
                try {
                    SlotLock probe = SlotLock.acquire(slot);
                    probe.close();
                    Assert.fail("engine.close() released the slot lock while its manager worker "
                            + "was still mid service pass — the handoff must not weaken the "
                            + "quiescence gate");
                } catch (Exception expected) {
                    // good — slot retained while the worker can still touch it.
                }

                // Phase 4 — the contract under test: release the worker and do
                // NOT retry close(). The worker finishes its pass, exits, and
                // runs the deferred cleanup itself, flipping isCloseCompleted
                // and releasing the flock with no further caller action.
                releaseWorker.countDown();
                long cleanupDeadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!engine.isCloseCompleted()) {
                    if (System.nanoTime() > cleanupDeadlineNs) {
                        throw new AssertionError(
                                "deferred cleanup never ran on manager-worker exit — the slot "
                                        + "would stay retired until process exit");
                    }
                    Thread.sleep(1);
                }
                try (SlotLock probe = SlotLock.acquire(slot)) {
                    Assert.assertNotNull("slot must be acquirable after the worker-exit cleanup", probe);
                } catch (Exception e) {
                    throw new AssertionError("worker-exit cleanup did not release the slot lock", e);
                }
                if (hookErr.get() != null) {
                    throw new AssertionError("install hook failed", hookErr.get());
                }
            } finally {
                Unsafe.free(buf, payloadLen, MemoryTag.NATIVE_DEFAULT);
                manager.setBeforeInstallSyncHook(null);
                releaseWorker.countDown();
                manager.setWorkerJoinTimeoutMillis(TimeUnit.SECONDS.toMillis(60));
                try {
                    engine.close();
                } catch (Throwable ignored) {
                }
            }
        });
    }

    /**
     * Registration-failure twin of
     * {@link #testOwnedEngineCloseHandsCleanupToWorkerExit}: when
     * {@code deferUntilWorkerExit} itself throws (allocation failure while
     * building the handoff), close() must NOT mistake the swallowed throw
     * for "worker already exited" and run the terminal cleanup inline — the
     * worker is provably still mid service pass, so releasing the ring,
     * watermark or slot flock here is the original stale-worker UAF/data-loss
     * hazard. Every worker-reachable resource must be retained and the close
     * must stay incomplete; a retried close() after the worker exits
     * converges and releases the slot.
     */
    @Test(timeout = 30_000L)
    public void testOwnedEngineCloseRetainsSlotWhenHandoffRegistrationFails() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int payloadLen = 32;
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + payloadLen);
            String slot = tmpDir + "/owned-regfail-slot";
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean();
            AtomicReference<Throwable> hookErr = new AtomicReference<>();
            // Production shape: private, owned manager (ownsManager=true).
            CursorSendEngine engine = new CursorSendEngine(slot, segSize);
            SegmentManager manager = readManager(engine);
            long buf = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                // Phase 1: wait out the initial spare install so the park hook
                // can only fire on the rotation-triggered pass (see
                // testOwnedEngineCloseRetainsSlotWhileWorkerIsMidServicePass).
                SegmentRing ring = readRing(engine);
                long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (ring.needsHotSpare()) {
                    if (System.nanoTime() > deadlineNs) {
                        throw new AssertionError("manager worker never installed the initial hot spare");
                    }
                    Thread.sleep(1);
                }
                manager.setBeforeInstallSyncHook(() -> {
                    if (!fired.compareAndSet(false, true)) return;
                    workerBlocked.countDown();
                    try {
                        if (!releaseWorker.await(20, TimeUnit.SECONDS)) {
                            hookErr.compareAndSet(null,
                                    new AssertionError("timed out waiting for test to release worker"));
                        }
                    } catch (Throwable t) {
                        hookErr.compareAndSet(null, t);
                    }
                });

                // Phase 2: fill the active segment and rotate onto the spare;
                // the worker's next tick re-enters the install pass and parks.
                Unsafe.getUnsafe().putLong(buf, 0L);
                Assert.assertEquals(0L, engine.appendBlocking(buf, payloadLen));
                Assert.assertEquals(1L, engine.appendBlocking(buf, payloadLen));
                Assert.assertTrue("worker never re-entered a spare-install pass",
                        workerBlocked.await(5, TimeUnit.SECONDS));

                // Phase 3: make the handoff registration throw — simulating
                // an OutOfMemoryError allocating the cleanup lambda/list —
                // with the worker provably still mid service pass. close()
                // must retain everything: no inline finishClose, no flock
                // release, closeCompleted stays false.
                manager.setBeforeExitCleanupRegistrationHook(() -> {
                    throw new OutOfMemoryError("simulated allocation failure registering exit cleanup");
                });
                manager.setWorkerJoinTimeoutMillis(50L);
                engine.close();
                Assert.assertFalse("close must stay incomplete when handoff registration fails",
                        engine.isCloseCompleted());
                try {
                    SlotLock probe = SlotLock.acquire(slot);
                    probe.close();
                    Assert.fail("engine.close() released the slot lock after a failed handoff "
                            + "registration while the manager worker was still mid service "
                            + "pass — the swallowed throw was mistaken for proof the worker "
                            + "exited (stale-worker UAF/data-loss hazard)");
                } catch (Exception expected) {
                    // good — slot retained while the worker can still touch it.
                }

                // Phase 4: clear the fault, release the worker (its loop was
                // already stopped by the close attempt, so it exits), and
                // retry close(). The retry must converge via isWorkerReaped()
                // and release the slot.
                manager.setBeforeExitCleanupRegistrationHook(null);
                releaseWorker.countDown();
                manager.setWorkerJoinTimeoutMillis(TimeUnit.SECONDS.toMillis(60));
                engine.close();
                Assert.assertTrue("retried close must report complete cleanup",
                        engine.isCloseCompleted());
                try (SlotLock probe = SlotLock.acquire(slot)) {
                    Assert.assertNotNull("slot must be acquirable after the retried close", probe);
                } catch (Exception e) {
                    throw new AssertionError("retried close() did not release the slot lock", e);
                }
                if (hookErr.get() != null) {
                    throw new AssertionError("install hook failed", hookErr.get());
                }
            } finally {
                Unsafe.free(buf, payloadLen, MemoryTag.NATIVE_DEFAULT);
                manager.setBeforeInstallSyncHook(null);
                manager.setBeforeExitCleanupRegistrationHook(null);
                releaseWorker.countDown();
                manager.setWorkerJoinTimeoutMillis(TimeUnit.SECONDS.toMillis(60));
                try {
                    engine.close();
                } catch (Throwable ignored) {
                }
            }
        });
    }

    /**
     * Exactly-once contention on the terminal-cleanup claim
     * ({@code terminalCleanupClaimed} CAS): after a timed-out owned close
     * handed cleanup to the worker's exit path, a retried {@code close()}
     * that races the worker MID-{@code finishClose} must neither re-run the
     * terminal cleanup (double munmap / double flock release) nor block on
     * the worker, nor publish completion on the worker's behalf. The race
     * window is made deterministic by parking the worker inside
     * {@code finishClose} (via {@code beforeFlockReleaseHook}) while the
     * retried close() converges through {@code isWorkerReaped()} and loses
     * the CAS.
     */
    @Test(timeout = 30_000L)
    public void testTerminalCleanupRunsExactlyOnceWhenRetriedCloseRacesWorkerHandoff() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int payloadLen = 32;
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + payloadLen);
            String slot = tmpDir + "/cas-contention-slot";
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            CountDownLatch inFinishClose = new CountDownLatch(1);
            CountDownLatch releaseFinishClose = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean();
            AtomicInteger finishCloseRuns = new AtomicInteger();
            AtomicReference<Throwable> hookErr = new AtomicReference<>();
            // Production shape: private, owned manager (ownsManager=true).
            CursorSendEngine engine = new CursorSendEngine(slot, segSize);
            SegmentManager manager = readManager(engine);
            long buf = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                // Phase 1: wait out the initial spare install so the park hook
                // can only fire on the rotation-triggered pass.
                SegmentRing ring = readRing(engine);
                long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (ring.needsHotSpare()) {
                    if (System.nanoTime() > deadlineNs) {
                        throw new AssertionError("manager worker never installed the initial hot spare");
                    }
                    Thread.sleep(1);
                }
                manager.setBeforeInstallSyncHook(() -> {
                    if (!fired.compareAndSet(false, true)) return;
                    workerBlocked.countDown();
                    try {
                        if (!releaseWorker.await(20, TimeUnit.SECONDS)) {
                            hookErr.compareAndSet(null,
                                    new AssertionError("timed out waiting for test to release worker"));
                        }
                    } catch (Throwable t) {
                        hookErr.compareAndSet(null, t);
                    }
                });
                // Counts terminal-cleanup executions and parks the FIRST one
                // (the worker's deferred cleanup) mid-finishClose, before the
                // flock release — the exact window a retried close() races.
                engine.setBeforeFlockReleaseHook(() -> {
                    if (finishCloseRuns.incrementAndGet() == 1) {
                        inFinishClose.countDown();
                        try {
                            if (!releaseFinishClose.await(20, TimeUnit.SECONDS)) {
                                hookErr.compareAndSet(null, new AssertionError(
                                        "timed out waiting for test to release finishClose"));
                            }
                        } catch (Throwable t) {
                            hookErr.compareAndSet(null, t);
                        }
                    }
                });

                // Phase 2: rotate onto the spare so the worker parks in the
                // next install pass.
                Unsafe.getUnsafe().putLong(buf, 0L);
                Assert.assertEquals(0L, engine.appendBlocking(buf, payloadLen));
                Assert.assertEquals(1L, engine.appendBlocking(buf, payloadLen));
                Assert.assertTrue("worker never re-entered a spare-install pass",
                        workerBlocked.await(5, TimeUnit.SECONDS));

                // Phase 3: timed-out close — cleanup ownership transfers to
                // the worker's exit path.
                manager.setWorkerJoinTimeoutMillis(50L);
                engine.close();
                Assert.assertFalse("close must stay incomplete while the worker holds the handoff",
                        engine.isCloseCompleted());

                // Phase 4: release the pass. The worker exits its loop, wins
                // the cleanup CAS, enters finishClose and parks in the hook —
                // mid-cleanup, flock still held, completion unpublished.
                releaseWorker.countDown();
                Assert.assertTrue("worker never entered the deferred finishClose",
                        inFinishClose.await(10, TimeUnit.SECONDS));
                Assert.assertEquals(1, finishCloseRuns.get());
                Assert.assertFalse("completion must not be observable mid-finishClose",
                        engine.isCloseCompleted());

                // Phase 5 — the contention under test: a retried close() while
                // the worker is parked INSIDE finishClose. The worker loop has
                // already exited (workerLoopExited=true precedes the deferred
                // cleanups), so the short bounded join reaps the manager state
                // and close() converges to the CAS — which it must LOSE,
                // returning promptly without touching ring/watermark/flock.
                engine.close();
                Assert.assertEquals(
                        "retried close() re-ran the terminal cleanup while the worker's "
                                + "deferred cleanup was mid-flight — ring/watermark/flock would "
                                + "be double-released",
                        1, finishCloseRuns.get());
                Assert.assertFalse("retried close() must not publish completion on the worker's behalf",
                        engine.isCloseCompleted());
                try {
                    SlotLock probe = SlotLock.acquire(slot);
                    probe.close();
                    Assert.fail("slot lock observable as released while the worker was still "
                            + "parked before its flock release");
                } catch (Exception expected) {
                    // good — flock still held by the parked cleanup.
                }

                // Phase 6: let the worker finish. Completion publishes, the
                // slot becomes acquirable, and the cleanup count stays at 1.
                releaseFinishClose.countDown();
                long cleanupDeadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!engine.isCloseCompleted()) {
                    if (System.nanoTime() > cleanupDeadlineNs) {
                        throw new AssertionError("deferred cleanup never completed after release");
                    }
                    Thread.sleep(1);
                }
                Assert.assertEquals(1, finishCloseRuns.get());
                try (SlotLock probe = SlotLock.acquire(slot)) {
                    Assert.assertNotNull("slot must be acquirable after the worker-exit cleanup", probe);
                }
                // A final close() takes the fast no-op path.
                engine.close();
                Assert.assertEquals("post-completion close() must be a no-op",
                        1, finishCloseRuns.get());
                if (hookErr.get() != null) {
                    throw new AssertionError("hook failed", hookErr.get());
                }
            } finally {
                Unsafe.free(buf, payloadLen, MemoryTag.NATIVE_DEFAULT);
                manager.setBeforeInstallSyncHook(null);
                engine.setBeforeFlockReleaseHook(null);
                releaseWorker.countDown();
                releaseFinishClose.countDown();
                manager.setWorkerJoinTimeoutMillis(TimeUnit.SECONDS.toMillis(60));
                try {
                    engine.close();
                } catch (Throwable ignored) {
                }
            }
        });
    }

    /**
     * Memory-mode twin of {@link #testOwnedEngineCloseHandsCleanupToWorkerExit}:
     * {@code sfDir == null}, so there is no slot lock, no watermark and no
     * segment files — but the ring's malloc'd native segments are still
     * worker-reachable, so the timed-out close must take the same handoff
     * path with every SF-only resource null. Pins that (a) the handoff
     * branch tolerates null slotLock/watermark/sfDir without NPE, (b) the
     * close stays incomplete while the worker can still touch the ring, and
     * (c) the worker-exit cleanup completes the close and frees the ring's
     * native memory (assertMemoryLeak is the leak oracle here).
     */
    @Test(timeout = 30_000L)
    public void testMemoryModeOwnedCloseHandsCleanupToWorkerExit() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int payloadLen = 32;
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + payloadLen);
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean();
            AtomicReference<Throwable> hookErr = new AtomicReference<>();
            // Memory mode: null sfDir, private owned manager — the exact
            // shape non-SF async ingest uses.
            CursorSendEngine engine = new CursorSendEngine(null, segSize);
            SegmentManager manager = readManager(engine);
            long buf = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try {
                // Phase 1: wait out the initial spare install so the park hook
                // can only fire on the rotation-triggered pass.
                SegmentRing ring = readRing(engine);
                long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (ring.needsHotSpare()) {
                    if (System.nanoTime() > deadlineNs) {
                        throw new AssertionError("manager worker never installed the initial hot spare");
                    }
                    Thread.sleep(1);
                }
                manager.setBeforeInstallSyncHook(() -> {
                    if (!fired.compareAndSet(false, true)) return;
                    workerBlocked.countDown();
                    try {
                        if (!releaseWorker.await(20, TimeUnit.SECONDS)) {
                            hookErr.compareAndSet(null,
                                    new AssertionError("timed out waiting for test to release worker"));
                        }
                    } catch (Throwable t) {
                        hookErr.compareAndSet(null, t);
                    }
                });

                // Phase 2: rotate onto the spare; the worker's next tick
                // re-enters the (in-memory) install pass and parks.
                Unsafe.getUnsafe().putLong(buf, 0L);
                Assert.assertEquals(0L, engine.appendBlocking(buf, payloadLen));
                Assert.assertEquals(1L, engine.appendBlocking(buf, payloadLen));
                Assert.assertTrue("worker never re-entered a spare-install pass",
                        workerBlocked.await(5, TimeUnit.SECONDS));

                // Phase 3: timed-out memory-mode close. The worker can still
                // touch the ring's native memory, so the close must hand off
                // and stay incomplete — releasing the ring here would be a
                // use-after-free on the worker's install path.
                manager.setWorkerJoinTimeoutMillis(50L);
                engine.close();
                Assert.assertFalse(
                        "memory-mode close must stay incomplete while the worker is mid service pass",
                        engine.isCloseCompleted());

                // Phase 4: release the worker; its exit path must run the
                // deferred cleanup (null slotLock/watermark/sfDir) and flip
                // completion with no further caller action.
                releaseWorker.countDown();
                long cleanupDeadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                while (!engine.isCloseCompleted()) {
                    if (System.nanoTime() > cleanupDeadlineNs) {
                        throw new AssertionError(
                                "deferred memory-mode cleanup never ran on manager-worker exit — "
                                        + "the ring's native segments would leak for the process lifetime");
                    }
                    Thread.sleep(1);
                }
                if (hookErr.get() != null) {
                    throw new AssertionError("install hook failed", hookErr.get());
                }
            } finally {
                Unsafe.free(buf, payloadLen, MemoryTag.NATIVE_DEFAULT);
                manager.setBeforeInstallSyncHook(null);
                releaseWorker.countDown();
                manager.setWorkerJoinTimeoutMillis(TimeUnit.SECONDS.toMillis(60));
                try {
                    engine.close();
                } catch (Throwable ignored) {
                }
            }
        });
    }

    /**
     * An engine that owns its manager must use the whole-manager stop/join as
     * its only quiescence barrier. Calling the per-ring barrier first would
     * give a stuck worker two independent timeout budgets.
     */
    @Test(timeout = 30_000L)
    public void testOwnedManagerCloseSkipsPerRingQuiescenceWait() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = tmpDir + "/owned-slot";
            CursorSendEngine engine = new CursorSendEngine(slot, 4L * 1024 * 1024);
            SegmentManager manager = readManager(engine);
            AtomicBoolean perRingAwaited = new AtomicBoolean();
            try {
                manager.setBeforeRingQuiescenceAwaitHook(() -> perRingAwaited.set(true));
                engine.close();
                Assert.assertTrue("owned engine close did not complete", engine.isCloseCompleted());
                Assert.assertFalse("owned engine close spent a separate per-ring wait budget",
                        perRingAwaited.get());
            } finally {
                manager.setBeforeRingQuiescenceAwaitHook(null);
                engine.close();
            }
        });
    }

    /**
     * Plain-positive path: after a normal close (worker quiesces promptly),
     * a second engine must be able to acquire and use the same slot.
     */
    @Test(timeout = 30_000L)
    public void testSameSlotReacquirableAfterNormalClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = tmpDir + "/slot";
            CursorSendEngine first = new CursorSendEngine(slot, 4L * 1024 * 1024);
            first.close();
            CursorSendEngine second = new CursorSendEngine(slot, 4L * 1024 * 1024);
            try {
                Assert.assertFalse("fully-drained close must leave no segments to recover",
                        second.wasRecoveredFromDisk());
            } finally {
                second.close();
            }
        });
    }

    private static SegmentManager readManager(CursorSendEngine engine) throws Exception {
        Field field = CursorSendEngine.class.getDeclaredField("manager");
        field.setAccessible(true);
        return (SegmentManager) field.get(engine);
    }

    private static SegmentRing readRing(CursorSendEngine engine) throws Exception {
        Field field = CursorSendEngine.class.getDeclaredField("ring");
        field.setAccessible(true);
        return (SegmentRing) field.get(engine);
    }

    private static void rmDirRecursive(String dir) {
        if (!Files.exists(dir)) return;
        long find = Files.findFirst(dir);
        if (find <= 0) return;
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                if (name != null && !".".equals(name) && !"..".equals(name)) {
                    String child = dir + "/" + name;
                    if (!Files.remove(child)) {
                        rmDirRecursive(child);
                        Files.remove(child);
                    }
                }
                rc = Files.findNext(find);
            }
        } finally {
            Files.findClose(find);
        }
    }
}
