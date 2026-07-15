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

import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.std.Files;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FlockReleaseRetryDriverTest {

    private final List<String> sfDirs = new ArrayList<>();

    @After
    public void tearDown() {
        CursorSendEngine.setAfterFlockReleaseRetryFailureHook(null);
        CursorSendEngine.setFlockReleaseRetryParkOverride(null);
        CursorSendEngine.setFlockReleaseRetryThreadFactory(null);
        for (String sfDir : sfDirs) {
            removeDir(sfDir);
        }
    }

    /**
     * Driver-start failure must not strand retired capacity until process
     * exit. {@code Sender.close()} is a one-shot no-op by contract, so the
     * only recovery surface a pool has is its retired-slot probe
     * ({@code isSlotLockReleased()}, called from the housekeeper tick and
     * capacity-starved borrows) — that probe must re-arm the shared retry
     * driver once thread creation works again.
     */
    @Test(timeout = 30_000L)
    public void testDriverStartFailureRecoversViaPoolProbe() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicInteger starts = new AtomicInteger();
            CursorSendEngine.setFlockReleaseRetryThreadFactory(task -> new Thread(task) {
                @Override
                public synchronized void start() {
                    starts.incrementAndGet();
                    throw new IllegalStateException("injected start failure");
                }
            });

            CursorSendEngine engine = new CursorSendEngine(
                    newSfDir("probe-rearm"), 4L * 1024 * 1024);
            SlotLock slotLock = engine.getSlotLockForTesting();
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 1);
            sender.setCursorEngine(engine, true);
            AtomicReference<Thread> rearmedDriver = new AtomicReference<>();
            boolean recovered = false;
            SlotLock.ReleaseFailureForTesting releaseFailure = slotLock.injectReleaseFailureForTesting();
            try {
                sender.close();
                assertEquals("retry driver start must be attempted once", 1, starts.get());
                assertFalse("failed unlock must remain unpublished", engine.isCloseCompleted());

                // Sender.close() is idempotent: repeat calls never reach the
                // engine again, so they cannot restart the failed driver.
                sender.close();
                assertEquals("no-op close must not retry the driver start", 1, starts.get());

                // While the fault persists, each pool probe re-attempts the
                // re-arm (and fails again) without publishing a release.
                assertFalse("failed unlock must keep the slot reported as held",
                        sender.isSlotLockReleased());
                assertTrue("pool probe must re-attempt the driver start",
                        starts.get() >= 2);

                // The transient condition clears: thread creation works again
                // (the failed start drained the queue, so the factory swap is
                // legal) and the flock fd is back.
                CursorSendEngine.setFlockReleaseRetryThreadFactory(task -> {
                    Thread thread = new Thread(task, "test-rearmed-flock-release-retry");
                    rearmedDriver.set(thread);
                    return thread;
                });
                releaseFailure.close();
                CountDownLatch released = new CountDownLatch(1);
                engine.setSlotLockReleaseListener(released::countDown);

                // Production recovery surface: the pool re-probes the retired
                // slot through isSlotLockReleased().
                sender.isSlotLockReleased();
                assertTrue("pool probe must re-arm the flock-release retry after "
                                + "a driver start failure",
                        released.await(10, TimeUnit.SECONDS));
                assertTrue(engine.isCloseCompleted());
                assertTrue("probe must expose the recovered release",
                        sender.isSlotLockReleased());
                recovered = true;
            } finally {
                Thread driver = rearmedDriver.get();
                if (driver != null) {
                    driver.join(10_000L);
                }
                if (!recovered) {
                    CursorSendEngine.setFlockReleaseRetryThreadFactory(null);
                    releaseFailure.close();
                    if (!slotLock.release()) {
                        fail("restored flock fd did not release");
                    }
                }
            }
            CursorSendEngine.setFlockReleaseRetryThreadFactory(null);
        });
    }

    @Test(timeout = 30_000L)
    public void testPersistentFailuresShareOneRetryThread() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final int engineCount = 32;
            AtomicInteger retryFailures = new AtomicInteger();
            AtomicInteger threadsCreated = new AtomicInteger();
            AtomicReference<Thread> retryThreadRef = new AtomicReference<>();
            CountDownLatch retryFailuresObserved = new CountDownLatch(engineCount * 2);
            CountDownLatch retryThreadStarted = new CountDownLatch(1);
            CountDownLatch runRetryDriver = new CountDownLatch(1);
            CursorSendEngine.setAfterFlockReleaseRetryFailureHook(() -> {
                retryFailures.incrementAndGet();
                retryFailuresObserved.countDown();
            });
            CursorSendEngine.setFlockReleaseRetryThreadFactory(task -> {
                threadsCreated.incrementAndGet();
                Thread thread = new Thread(() -> {
                    retryThreadStarted.countDown();
                    try {
                        runRetryDriver.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    task.run();
                }, "test-shared-flock-release-retry");
                retryThreadRef.set(thread);
                return thread;
            });

            List<CursorSendEngine> engines = new ArrayList<>(engineCount);
            List<SlotLock.ReleaseFailureForTesting> releaseFailures = new ArrayList<>(engineCount);
            boolean failuresRestored = false;
            try {
                for (int i = 0; i < engineCount; i++) {
                    String sfDir = newSfDir("persistent-" + i);
                    CursorSendEngine engine = new CursorSendEngine(sfDir, 4L * 1024 * 1024);
                    SlotLock slotLock = engine.getSlotLockForTesting();
                    engines.add(engine);
                    boolean isTracked = false;
                    SlotLock.ReleaseFailureForTesting releaseFailure =
                            slotLock.injectReleaseFailureForTesting();
                    try {
                        releaseFailures.add(releaseFailure);
                        isTracked = true;
                        engine.close();
                    } finally {
                        if (!isTracked) {
                            releaseFailure.close();
                        }
                    }
                    assertFalse("injected unlock failure must keep close incomplete",
                            engine.isCloseCompleted());
                    if (i == 0) {
                        assertTrue("retry thread was not started",
                                retryThreadStarted.await(10, TimeUnit.SECONDS));
                    }
                }

                assertEquals("persistent failures must share one retry thread",
                        1, threadsCreated.get());
                runRetryDriver.countDown();
                assertTrue("driver did not perform two failed rounds",
                        retryFailuresObserved.await(10, TimeUnit.SECONDS));
                assertTrue("driver did not retain persistent failures",
                        retryFailures.get() >= engineCount * 2);
                for (CursorSendEngine engine : engines) {
                    assertFalse("failed releases must remain unpublished",
                            engine.isCloseCompleted());
                }

                restoreReleaseFailures(releaseFailures);
                failuresRestored = true;
                Thread retryThread = retryThreadRef.get();
                retryThread.join(10_000L);
                assertFalse("shared retry thread retained lifecycle resources after drain",
                        retryThread.isAlive());
                for (CursorSendEngine engine : engines) {
                    assertTrue("driver must release every restored flock",
                            engine.isCloseCompleted());
                }
                assertEquals("retries must not create another thread",
                        1, threadsCreated.get());
            } finally {
                if (!failuresRestored) {
                    restoreReleaseFailures(releaseFailures);
                }
                runRetryDriver.countDown();
                Thread retryThread = retryThreadRef.get();
                if (retryThread != null) {
                    retryThread.join(10_000L);
                }
                CursorSendEngine.setAfterFlockReleaseRetryFailureHook(null);
            }
            CursorSendEngine.setFlockReleaseRetryThreadFactory(null);
        });
    }

    /**
     * Schedule inspection for the shared driver's retry cadence: the
     * inter-round park must grow exponentially from 100 ms and cap at 5 s,
     * so a persistent unlock failure does not burn 10 syscalls per second
     * per engine forever. The park override replaces the real park, so the
     * test coordinates rounds without wall-clock waits.
     */
    @Test(timeout = 30_000L)
    public void testRetryBackoffDoublesToCap() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            List<Long> parks = new ArrayList<>();
            Semaphore parked = new Semaphore(0);
            Semaphore proceed = new Semaphore(0);
            AtomicReference<Thread> driverRef = new AtomicReference<>();
            CursorSendEngine.setFlockReleaseRetryParkOverride(nanos -> {
                parks.add(nanos);
                parked.release();
                proceed.acquireUninterruptibly();
            });
            CursorSendEngine.setFlockReleaseRetryThreadFactory(task -> {
                Thread thread = new Thread(task, "test-backoff-flock-release-retry");
                driverRef.set(thread);
                return thread;
            });

            CursorSendEngine engine = new CursorSendEngine(
                    newSfDir("backoff-cap"), 4L * 1024 * 1024);
            SlotLock slotLock = engine.getSlotLockForTesting();
            boolean failureRestored = false;
            SlotLock.ReleaseFailureForTesting releaseFailure = slotLock.injectReleaseFailureForTesting();
            try {
                engine.close();
                assertFalse("injected unlock failure must keep close incomplete",
                        engine.isCloseCompleted());

                // Eight failed rounds: enough to observe the full ramp and
                // two capped parks.
                for (int round = 1; round <= 8; round++) {
                    assertTrue("driver did not park after failed round " + round,
                            parked.tryAcquire(10, TimeUnit.SECONDS));
                    if (round < 8) {
                        proceed.release();
                    }
                }

                releaseFailure.close();
                failureRestored = true;
                proceed.release();
                Thread driver = driverRef.get();
                driver.join(10_000L);
                assertFalse("driver did not drain after the release succeeded",
                        driver.isAlive());
                assertTrue("restored flock must be released", engine.isCloseCompleted());

                List<Long> expected = new ArrayList<>();
                expected.add(100_000_000L);
                expected.add(200_000_000L);
                expected.add(400_000_000L);
                expected.add(800_000_000L);
                expected.add(1_600_000_000L);
                expected.add(3_200_000_000L);
                expected.add(5_000_000_000L);
                expected.add(5_000_000_000L);
                assertEquals("retry parks must double from 100ms and cap at 5s",
                        expected, parks);
            } finally {
                if (!failureRestored) {
                    releaseFailure.close();
                }
                proceed.release(1_000);
                Thread driver = driverRef.get();
                if (driver != null) {
                    driver.join(10_000L);
                }
            }
            CursorSendEngine.setFlockReleaseRetryParkOverride(null);
            CursorSendEngine.setFlockReleaseRetryThreadFactory(null);
        });
    }

    /**
     * A successful release in a round is progress: the driver must reset its
     * backoff to the base so the remaining engines are retried promptly
     * while the failure condition is clearing.
     */
    @Test(timeout = 30_000L)
    public void testRetryBackoffResetsOnProgress() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            List<Long> parks = new ArrayList<>();
            Semaphore parked = new Semaphore(0);
            Semaphore proceed = new Semaphore(0);
            AtomicReference<Thread> driverRef = new AtomicReference<>();
            CursorSendEngine.setFlockReleaseRetryParkOverride(nanos -> {
                parks.add(nanos);
                parked.release();
                proceed.acquireUninterruptibly();
            });
            CursorSendEngine.setFlockReleaseRetryThreadFactory(task -> {
                Thread thread = new Thread(task, "test-reset-flock-release-retry");
                driverRef.set(thread);
                return thread;
            });

            CursorSendEngine engineA = new CursorSendEngine(
                    newSfDir("backoff-reset-a"), 4L * 1024 * 1024);
            CursorSendEngine engineB = new CursorSendEngine(
                    newSfDir("backoff-reset-b"), 4L * 1024 * 1024);
            SlotLock slotLockA = engineA.getSlotLockForTesting();
            SlotLock slotLockB = engineB.getSlotLockForTesting();
            try {
                try (SlotLock.ReleaseFailureForTesting releaseFailureA =
                             slotLockA.injectReleaseFailureForTesting();
                     SlotLock.ReleaseFailureForTesting releaseFailureB =
                             slotLockB.injectReleaseFailureForTesting()) {
                    engineA.close();
                    engineB.close();

                    // Three failed rounds ramp the backoff to 400ms.
                    for (int round = 1; round <= 3; round++) {
                        assertTrue("driver did not park after failed round " + round,
                                parked.tryAcquire(10, TimeUnit.SECONDS));
                        if (round < 3) {
                            proceed.release();
                        }
                    }

                    // Engine A recovers; round 4 has one success and one failure,
                    // so its park must be back at the 100ms base.
                    releaseFailureA.close();
                    proceed.release();
                    assertTrue("driver did not park after the mixed round",
                            parked.tryAcquire(10, TimeUnit.SECONDS));
                    assertTrue("recovered engine must publish completion",
                            engineA.isCloseCompleted());

                    releaseFailureB.close();
                    proceed.release();
                    Thread driver = driverRef.get();
                    driver.join(10_000L);
                    assertFalse("driver did not drain after both releases succeeded",
                            driver.isAlive());
                    assertTrue(engineB.isCloseCompleted());

                    List<Long> expected = new ArrayList<>();
                    expected.add(100_000_000L);
                    expected.add(200_000_000L);
                    expected.add(400_000_000L);
                    expected.add(100_000_000L);
                    assertEquals("a successful release must reset the backoff to base",
                            expected, parks);
                }
            } finally {
                proceed.release(1_000);
                Thread driver = driverRef.get();
                if (driver != null) {
                    driver.join(10_000L);
                }
            }
            CursorSendEngine.setFlockReleaseRetryParkOverride(null);
            CursorSendEngine.setFlockReleaseRetryThreadFactory(null);
        });
    }

    @Test(timeout = 30_000L)
    public void testRetryThreadStartFailureLeavesExplicitCloseRetryable() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicInteger starts = new AtomicInteger();
            CursorSendEngine.setFlockReleaseRetryThreadFactory(task -> new Thread(task) {
                @Override
                public synchronized void start() {
                    starts.incrementAndGet();
                    throw new IllegalStateException("injected start failure");
                }
            });

            CursorSendEngine engine = new CursorSendEngine(
                    newSfDir("start-failure"), 4L * 1024 * 1024);
            SlotLock slotLock = engine.getSlotLockForTesting();
            SlotLock.ReleaseFailureForTesting releaseFailure = slotLock.injectReleaseFailureForTesting();
            try {
                engine.close();
                assertEquals("retry driver start must be attempted once", 1, starts.get());
                assertFalse("failed unlock must remain unpublished", engine.isCloseCompleted());

                // This also proves the failed driver cleared its queue: the
                // setter rejects replacement while any engine remains queued.
                CursorSendEngine.setFlockReleaseRetryThreadFactory(null);
                releaseFailure.close();
                engine.close();
                assertTrue("explicit close must recover after retry-thread start failure",
                        engine.isCloseCompleted());
            } finally {
                if (!engine.isCloseCompleted()) {
                    CursorSendEngine.setFlockReleaseRetryThreadFactory(null);
                    releaseFailure.close();
                    if (!slotLock.release()) {
                        fail("restored flock fd did not release");
                    }
                }
            }
        });
    }

    private static void removeDir(String sfDir) {
        long find = Files.findFirst(sfDir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(sfDir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(sfDir);
    }

    private static void restoreReleaseFailures(
            List<SlotLock.ReleaseFailureForTesting> releaseFailures
    ) {
        for (int i = 0; i < releaseFailures.size(); i++) {
            releaseFailures.get(i).close();
        }
    }

    private String newSfDir(String suffix) {
        String sfDir = Paths.get(
                System.getProperty("java.io.tmpdir"),
                "qdb-flock-release-retry-" + suffix + "-" + System.nanoTime()
        ).toString();
        sfDirs.add(sfDir);
        return sfDir;
    }
}
