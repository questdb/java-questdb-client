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

import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.std.Files;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
        CursorSendEngine.setFlockReleaseRetryThreadFactory(null);
        for (String sfDir : sfDirs) {
            removeDir(sfDir);
        }
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

            List<CursorSendEngine> engines = new ArrayList<>();
            List<SlotLock> slotLocks = new ArrayList<>();
            List<Integer> realFds = new ArrayList<>();
            boolean fdsRestored = false;
            try {
                for (int i = 0; i < engineCount; i++) {
                    String sfDir = newSfDir("persistent-" + i);
                    CursorSendEngine engine = new CursorSendEngine(sfDir, 4L * 1024 * 1024);
                    SlotLock slotLock = slotLock(engine);
                    int realFd = fd(slotLock);
                    engines.add(engine);
                    slotLocks.add(slotLock);
                    realFds.add(realFd);
                    setFd(slotLock, 1_000_000_000);
                    engine.close();
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

                restoreFds(slotLocks, realFds);
                fdsRestored = true;
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
                if (!fdsRestored) {
                    restoreFds(slotLocks, realFds);
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
            SlotLock slotLock = slotLock(engine);
            int realFd = fd(slotLock);
            try {
                setFd(slotLock, 1_000_000_000);
                engine.close();
                assertEquals("retry driver start must be attempted once", 1, starts.get());
                assertFalse("failed unlock must remain unpublished", engine.isCloseCompleted());

                // This also proves the failed driver cleared its queue: the
                // setter rejects replacement while any engine remains queued.
                CursorSendEngine.setFlockReleaseRetryThreadFactory(null);
                setFd(slotLock, realFd);
                engine.close();
                assertTrue("explicit close must recover after retry-thread start failure",
                        engine.isCloseCompleted());
            } finally {
                if (!engine.isCloseCompleted()) {
                    CursorSendEngine.setFlockReleaseRetryThreadFactory(null);
                    setFd(slotLock, realFd);
                    if (!slotLock.release()) {
                        fail("restored flock fd did not release");
                    }
                }
            }
        });
    }

    private static int fd(SlotLock slotLock) throws Exception {
        Field fdField = SlotLock.class.getDeclaredField("fd");
        fdField.setAccessible(true);
        return fdField.getInt(slotLock);
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

    private static void restoreFds(List<SlotLock> slotLocks, List<Integer> realFds) throws Exception {
        for (int i = 0; i < slotLocks.size(); i++) {
            SlotLock slotLock = slotLocks.get(i);
            synchronized (slotLock) {
                setFd(slotLock, realFds.get(i));
            }
        }
    }

    private static void setFd(SlotLock slotLock, int fd) throws Exception {
        Field fdField = SlotLock.class.getDeclaredField("fd");
        fdField.setAccessible(true);
        fdField.setInt(slotLock, fd);
    }

    private static SlotLock slotLock(CursorSendEngine engine) throws Exception {
        Field slotLockField = CursorSendEngine.class.getDeclaredField("slotLock");
        slotLockField.setAccessible(true);
        return (SlotLock) slotLockField.get(engine);
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
