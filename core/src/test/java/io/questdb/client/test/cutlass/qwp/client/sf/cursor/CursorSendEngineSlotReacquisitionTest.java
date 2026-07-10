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
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.std.Files;
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
