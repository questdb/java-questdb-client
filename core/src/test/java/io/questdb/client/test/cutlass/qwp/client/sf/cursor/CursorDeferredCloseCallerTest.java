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

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Caller-level regressions for lifecycle paths that hand an incomplete
 * {@link CursorSendEngine#close()} to the deferred close owner. The tests hold
 * a real manager worker inside a service pass but replace the wire with an
 * in-process stub, so they exercise production teardown without a server.
 */
public class CursorDeferredCloseCallerTest {

    private static final long SEGMENT_BYTES = 64L * 1024L;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-cursor-deferred-caller-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) {
            return;
        }
        rmDirRecursive(tmpDir);
        Files.remove(tmpDir);
    }

    @Test(timeout = 30_000L)
    public void testBackgroundDrainerDefersCloseWhenManagerQuiescenceStalls() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = tmpDir + "/drainer-slot";
            SegmentManager manager = new SegmentManager(SEGMENT_BYTES, TimeUnit.SECONDS.toNanos(60));
            CountDownLatch releaseWorker = new CountDownLatch(1);
            CountDownLatch workerBlocked = new CountDownLatch(1);
            AtomicBoolean hookFired = new AtomicBoolean();
            CursorSendEngine engine = null;
            BackgroundDrainer drainer = null;
            Thread drainerThread = null;
            IdleWebSocketClient client = null;
            boolean managerClosed = false;
            try {
                manager.setBeforeInstallSyncHook(() -> {
                    if (!hookFired.compareAndSet(false, true)) {
                        return;
                    }
                    workerBlocked.countDown();
                    try {
                        releaseWorker.await(20, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                manager.start();
                engine = new CursorSendEngine(slot, SEGMENT_BYTES, manager);
                appendFrame(engine);
                Assert.assertTrue("manager worker never reached the install hook",
                        workerBlocked.await(5, TimeUnit.SECONDS));
                manager.setWorkerJoinTimeoutMillis(50L);

                client = new IdleWebSocketClient();
                IdleWebSocketClient connectedClient = client;
                drainer = new BackgroundDrainer(
                        slot,
                        SEGMENT_BYTES,
                        Long.MAX_VALUE,
                        () -> connectedClient,
                        5_000L,
                        10L,
                        50L,
                        false,
                        0L
                );
                drainer.setEngineForTesting(engine);
                drainerThread = new Thread(drainer, "qdb-drainer-deferred-close-test");
                drainerThread.setDaemon(true);
                drainerThread.start();
                Assert.assertTrue("drainer send loop never started",
                        client.sendAttempted.await(5, TimeUnit.SECONDS));

                long started = System.nanoTime();
                drainer.requestStop();
                drainerThread.join(5_000L);
                Assert.assertFalse("drainer blocked on incomplete engine close", drainerThread.isAlive());
                Assert.assertTrue(
                        "drainer close fallback exceeded its bounded lifecycle window",
                        System.nanoTime() - started < TimeUnit.SECONDS.toNanos(1)
                );
                Assert.assertEquals(BackgroundDrainer.DrainOutcome.STOPPED, drainer.outcome());
                Assert.assertFalse("stalled manager must keep engine close incomplete",
                        engine.isCloseCompleted());
                assertSlotLocked(slot);

                releaseWorker.countDown();
                awaitCloseCompleted(engine);
                try (SlotLock probe = SlotLock.acquire(slot)) {
                    Assert.assertNotNull("slot must be acquirable after deferred drainer cleanup", probe);
                }

                manager.close();
                managerClosed = true;
            } finally {
                manager.setBeforeInstallSyncHook(null);
                releaseWorker.countDown();
                if (drainer != null) {
                    drainer.requestStop();
                }
                if (drainerThread != null) {
                    drainerThread.join(5_000L);
                }
                if (client != null) {
                    client.close();
                }
                if (engine != null) {
                    engine.close();
                }
                if (!managerClosed) {
                    manager.close();
                }
            }
        });
    }

    @Test(timeout = 30_000L)
    public void testSendLoopDefersDelegatedCloseWhenManagerQuiescenceStalls() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            String slot = tmpDir + "/send-loop-slot";
            SegmentManager manager = new SegmentManager(SEGMENT_BYTES, TimeUnit.SECONDS.toNanos(60));
            CountDownLatch releaseWorker = new CountDownLatch(1);
            CountDownLatch workerBlocked = new CountDownLatch(1);
            AtomicBoolean hookFired = new AtomicBoolean();
            CursorSendEngine engine = null;
            CursorWebSocketSendLoop loop = null;
            IdleWebSocketClient client = null;
            boolean managerClosed = false;
            try {
                manager.setBeforeInstallSyncHook(() -> {
                    if (!hookFired.compareAndSet(false, true)) {
                        return;
                    }
                    workerBlocked.countDown();
                    try {
                        releaseWorker.await(20, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                manager.start();
                engine = new CursorSendEngine(slot, SEGMENT_BYTES, manager);
                Assert.assertTrue("manager worker never reached the install hook",
                        workerBlocked.await(5, TimeUnit.SECONDS));
                manager.setWorkerJoinTimeoutMillis(50L);

                client = new IdleWebSocketClient();
                loop = new CursorWebSocketSendLoop(
                        client,
                        engine,
                        0L,
                        CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                        null,
                        0L,
                        0L,
                        0L
                );
                loop.start();
                Assert.assertTrue("send loop never entered its receive pass",
                        client.receiveAttempted.await(5, TimeUnit.SECONDS));
                Thread ioThread = loop.getIoThreadForTesting();
                Assert.assertNotNull("send loop did not publish its I/O thread", ioThread);
                Assert.assertTrue("live send loop must adopt the delegated engine close",
                        loop.delegateEngineClose());

                long started = System.nanoTime();
                loop.close();
                ioThread.join(5_000L);
                Assert.assertFalse("send-loop exit path did not finish", ioThread.isAlive());
                Assert.assertTrue(
                        "send-loop close fallback exceeded its bounded lifecycle window",
                        System.nanoTime() - started < TimeUnit.SECONDS.toNanos(1)
                );
                Assert.assertFalse("stalled manager must keep engine close incomplete",
                        engine.isCloseCompleted());
                assertSlotLocked(slot);

                releaseWorker.countDown();
                awaitCloseCompleted(engine);
                try (SlotLock probe = SlotLock.acquire(slot)) {
                    Assert.assertNotNull("slot must be acquirable after deferred send-loop cleanup", probe);
                }

                manager.close();
                managerClosed = true;
            } finally {
                manager.setBeforeInstallSyncHook(null);
                releaseWorker.countDown();
                if (loop != null) {
                    loop.close();
                }
                if (client != null) {
                    client.close();
                }
                if (engine != null) {
                    engine.close();
                }
                if (!managerClosed) {
                    manager.close();
                }
            }
        });
    }

    private static void appendFrame(CursorSendEngine engine) {
        long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < 16; i++) {
                Unsafe.getUnsafe().putByte(buf + i, (byte) i);
            }
            Assert.assertEquals(0L, engine.appendBlocking(buf, 16));
        } finally {
            Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static void assertSlotLocked(String slot) {
        try (SlotLock ignored = SlotLock.acquire(slot)) {
            Assert.fail("incomplete close must retain the slot lock");
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("sf slot already in use"));
        }
    }

    private static void awaitCloseCompleted(CursorSendEngine engine) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!engine.isCloseCompleted() && System.nanoTime() < deadline) {
            Thread.sleep(1L);
        }
        Assert.assertTrue("deferred close did not complete after manager quiescence",
                engine.isCloseCompleted());
    }

    private static void rmDirRecursive(String dir) {
        if (!Files.exists(dir)) {
            return;
        }
        long find = Files.findFirst(dir);
        if (find <= 0) {
            return;
        }
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

    private static final class IdleWebSocketClient extends WebSocketClient {
        private final CountDownLatch receiveAttempted = new CountDownLatch(1);
        private final CountDownLatch sendAttempted = new CountDownLatch(1);

        private IdleWebSocketClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            sendAttempted.countDown();
        }

        @Override
        public void sendBinary(long dataPtr, int length, int timeout) {
            sendAttempted.countDown();
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            receiveAttempted.countDown();
            return false;
        }

        @Override
        protected void ioWait(int timeout, int op) {
            throw new UnsupportedOperationException("stub: no socket");
        }

        @Override
        protected void setupIoWait() {
            // no-op
        }
    }
}
