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

import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.std.Files;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * slot again. On barrier timeout the engine deliberately leaks and a later
 * {@code close()} retries the cleanup.
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

                // The slot must still be locked: a replacement engine (or raw
                // SlotLock) acquiring it now would race the stale worker.
                try {
                    SlotLock probe = SlotLock.acquire(slot);
                    probe.close();
                    Assert.fail("engine.close() released the slot lock while the manager "
                            + "worker was still mid service pass for its ring — a "
                            + "replacement engine could acquire the slot and have its "
                            + "segment files unlinked by the stale worker");
                } catch (IllegalStateException expected) {
                    Assert.assertTrue(
                            expected.getMessage(),
                            expected.getMessage().contains("sf slot already in use")
                    );
                }

                // Let the worker finish its pass (it abandons the spare: the
                // ring was deregistered by the close attempt above).
                releaseWorker.countDown();
                manager.setWorkerJoinTimeoutMillis(TimeUnit.SECONDS.toMillis(60));

                // Retry close(): the barrier now succeeds and the full cleanup
                // (ring, watermark, unlink, slot lock) must complete.
                engine.close();
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

    @Test(timeout = 30_000L)
    public void testDeferredCloseRetainsOwnershipWithoutBlockingCaller() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            String slot = tmpDir + "/deferred-slot";
            SegmentManager manager = new SegmentManager(segSize, TimeUnit.SECONDS.toNanos(60));
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean();
            CursorSendEngine engine = null;
            boolean managerClosed = false;
            try {
                manager.setBeforeInstallSyncHook(() -> {
                    if (!fired.compareAndSet(false, true)) return;
                    workerBlocked.countDown();
                    try {
                        releaseWorker.await(20, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                manager.start();
                engine = new CursorSendEngine(slot, segSize, manager);
                Assert.assertTrue("worker never reached the install hook",
                        workerBlocked.await(5, TimeUnit.SECONDS));

                manager.setWorkerJoinTimeoutMillis(50L);
                engine.close();
                Assert.assertFalse("first close must remain incomplete", engine.isCloseCompleted());
                long started = System.nanoTime();
                engine.closeEventually();
                Assert.assertTrue(
                        "ownership transfer must not block the lifecycle thread",
                        System.nanoTime() - started < TimeUnit.SECONDS.toNanos(1)
                );
                Thread.sleep(100L);
                Assert.assertFalse(
                        "the deferred owner must retain resources while the manager stays stalled",
                        engine.isCloseCompleted()
                );
                try (SlotLock ignored = SlotLock.acquire(slot)) {
                    Assert.fail("deferred cleanup must retain the slot lock while quiescence is stalled");
                } catch (IllegalStateException e) {
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("sf slot already in use"));
                }

                releaseWorker.countDown();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!engine.isCloseCompleted() && System.nanoTime() < deadline) {
                    Thread.sleep(1L);
                }
                Assert.assertTrue("deferred cleanup did not complete after quiescence", engine.isCloseCompleted());
                try (SlotLock probe = SlotLock.acquire(slot)) {
                    Assert.assertNotNull("slot must be acquirable after deferred cleanup", probe);
                }

                manager.close();
                managerClosed = true;
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

    @Test(timeout = 30_000L)
    public void testOwnedManagerReapCompletesAfterQuiescenceTimeout() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            SegmentManager manager = new SegmentManager(segSize, TimeUnit.SECONDS.toNanos(60));
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            CountDownLatch closeJoinEntered = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean();
            CursorSendEngine engine = null;
            try {
                manager.setBeforeInstallSyncHook(() -> {
                    if (!fired.compareAndSet(false, true)) return;
                    workerBlocked.countDown();
                    try {
                        releaseWorker.await(20, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                manager.setBeforeJoinAttemptHook(() -> {
                    closeJoinEntered.countDown();
                    releaseWorker.countDown();
                });
                manager.setWorkerJoinTimeoutMillis(50L);

                Constructor<CursorSendEngine> constructor = CursorSendEngine.class.getDeclaredConstructor(
                        String.class,
                        long.class,
                        SegmentManager.class,
                        boolean.class,
                        long.class
                );
                constructor.setAccessible(true);
                engine = constructor.newInstance(
                        null,
                        segSize,
                        manager,
                        true,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS
                );
                Assert.assertTrue("owned manager worker never entered its service pass",
                        workerBlocked.await(5, TimeUnit.SECONDS));

                engine.close();
                Assert.assertEquals("owned manager close must reach its join fallback", 0L,
                        closeJoinEntered.getCount());
                Assert.assertTrue("owned manager close must reap the worker", manager.isWorkerReaped());
                Assert.assertTrue(
                        "a reaped owned manager is a stronger barrier than the timed-out ring wait",
                        engine.isCloseCompleted()
                );
            } finally {
                manager.setBeforeInstallSyncHook(null);
                manager.setBeforeJoinAttemptHook(null);
                releaseWorker.countDown();
                if (engine != null) {
                    engine.close();
                } else {
                    manager.close();
                }
            }
        });
    }

    @Test(timeout = 30_000L)
    public void testOwnerCloseRetainsEngineUntilCleanupCompletes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long segSize = MmapSegment.HEADER_SIZE + (MmapSegment.FRAME_HEADER_SIZE + 32);
            String slot = tmpDir + "/owner-slot";
            SegmentManager manager = new SegmentManager(segSize, TimeUnit.SECONDS.toNanos(60));
            CountDownLatch workerBlocked = new CountDownLatch(1);
            CountDownLatch releaseWorker = new CountDownLatch(1);
            AtomicBoolean fired = new AtomicBoolean();
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 9000);
            CursorSendEngine engine = null;
            boolean managerClosed = false;
            try {
                manager.setBeforeInstallSyncHook(() -> {
                    if (!fired.compareAndSet(false, true)) return;
                    workerBlocked.countDown();
                    try {
                        releaseWorker.await(20, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                manager.start();
                engine = new CursorSendEngine(slot, segSize, manager);
                sender.setCursorEngine(engine, true);
                Assert.assertTrue("worker never reached the install hook",
                        workerBlocked.await(5, TimeUnit.SECONDS));

                manager.setWorkerJoinTimeoutMillis(50L);
                sender.close();
                Assert.assertFalse(
                        "owner must not report the slot lock released while engine cleanup is incomplete",
                        sender.isSlotLockReleased()
                );

                releaseWorker.countDown();
                manager.setWorkerJoinTimeoutMillis(TimeUnit.SECONDS.toMillis(60));
                sender.close();
                Assert.assertTrue(
                        "a repeated owner close must retry the retained engine and report completion",
                        sender.isSlotLockReleased()
                );
                try (SlotLock probe = SlotLock.acquire(slot)) {
                    Assert.assertNotNull("slot must be acquirable after owner retry", probe);
                }

                manager.close();
                managerClosed = true;
            } finally {
                manager.setBeforeInstallSyncHook(null);
                releaseWorker.countDown();
                try {
                    sender.close();
                } catch (Throwable ignored) {
                }
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
