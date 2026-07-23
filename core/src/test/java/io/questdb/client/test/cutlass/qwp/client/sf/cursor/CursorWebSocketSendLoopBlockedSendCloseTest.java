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

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class CursorWebSocketSendLoopBlockedSendCloseTest {

    @Test(timeout = 30_000L)
    public void testCloseBreaksBlockedSendBeforeJoiningWorker() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            BlockingSendClient client = new BlockingSendClient(true);
            CursorSendEngine engine = new CursorSendEngine(null, 64 * 1024);
            CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                    client,
                    engine,
                    0L,
                    CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                    null,
                    100L,
                    1_000L,
                    5_000L,
                    false
            );
            long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            Thread closer = null;
            AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            try {
                Unsafe.getUnsafe().putLong(payload, 0x0102030405060708L);
                Unsafe.getUnsafe().putLong(payload + 8, 0x1112131415161718L);
                Assert.assertEquals(0L, engine.appendBlocking(payload, 16));
                loop.start();
                Assert.assertTrue("I/O worker never entered the blocking send",
                        client.sendEntered.await(5, TimeUnit.SECONDS));

                closer = new Thread(() -> {
                    try {
                        loop.close();
                    } catch (Throwable t) {
                        closeFailure.set(t);
                    }
                }, "blocked-send-closer");
                closer.start();

                Assert.assertTrue("close did not break the traffic path before joining",
                        client.trafficClosed.await(5, TimeUnit.SECONDS));
                Assert.assertEquals("traffic path must close exactly once", 1, client.trafficCloseCount.get());
                Assert.assertEquals("the closer must break traffic, not the I/O worker",
                        closer, client.trafficCloseThread.get());
                Assert.assertTrue("blocked send did not observe traffic-path closure",
                        client.sendExited.await(5, TimeUnit.SECONDS));

                closer.join(TimeUnit.SECONDS.toMillis(5));
                Assert.assertFalse("close did not join the I/O worker", closer.isAlive());
                Assert.assertNull("close failed", closeFailure.get());
                Assert.assertNull("ordinary close must not manufacture a terminal error", loop.getTerminalError());

                Thread ioThread = client.sendThread.get();
                Assert.assertNotNull(ioThread);
                // close() returns when the worker counts shutdownLatch down --
                // the worker's last action before its exit tail -- so the
                // thread can be observably alive for a scheduling beat after
                // close() returns. Quiescence is the latch plus the cleanup
                // asserts below (cleanup runs before the countdown), not
                // thread death: join briefly instead of asserting the race.
                ioThread.join(TimeUnit.SECONDS.toMillis(5));
                Assert.assertFalse("I/O worker did not exit after close returned", ioThread.isAlive());
                Assert.assertEquals("full client cleanup must run exactly once", 1, client.cleanupCount.get());
                Assert.assertEquals("the I/O worker must own cleanup before publishing exit",
                        ioThread, client.cleanupThread.get());
                Assert.assertFalse("close must not manufacture caller interruption",
                        closer.isInterrupted());
            } finally {
                client.releaseSend.countDown();
                if (closer != null) {
                    closer.join(TimeUnit.SECONDS.toMillis(5));
                }
                loop.close();
                engine.close();
                client.close();
                Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test(timeout = 30_000L)
    public void testUnsupportedCustomTransportFailsWithoutDestroyingWorkerResources() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            BlockingSendClient client = new BlockingSendClient(false);
            CursorSendEngine engine = new CursorSendEngine(null, 64 * 1024);
            CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                    client,
                    engine,
                    0L,
                    CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                    null,
                    100L,
                    1_000L,
                    5_000L,
                    false
            );
            long payload = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            Thread closer = null;
            AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            CountDownLatch closeEntered = new CountDownLatch(1);
            try {
                Unsafe.getUnsafe().putLong(payload, 0x0102030405060708L);
                Unsafe.getUnsafe().putLong(payload + 8, 0x1112131415161718L);
                Assert.assertEquals(0L, engine.appendBlocking(payload, 16));
                loop.start();
                Assert.assertTrue("I/O worker never entered the blocking send",
                        client.sendEntered.await(5, TimeUnit.SECONDS));

                closer = new Thread(() -> {
                    closeEntered.countDown();
                    try {
                        loop.close();
                    } catch (Throwable t) {
                        closeFailure.set(t);
                    }
                }, "unsupported-transport-closer");
                closer.start();

                Assert.assertTrue("close did not start", closeEntered.await(5, TimeUnit.SECONDS));
                closer.join(TimeUnit.SECONDS.toMillis(5));
                Assert.assertFalse("unsupported transport made close join indefinitely", closer.isAlive());
                Assert.assertTrue(closeFailure.get() instanceof LineSenderException);
                Assert.assertTrue(closeFailure.get().getCause() instanceof UnsupportedOperationException);
                Assert.assertEquals("unsupported cancellation must not release the blocked send",
                        1L, client.sendExited.getCount());
                Assert.assertEquals("unsupported cancellation must not perform full cleanup", 0, client.cleanupCount.get());
                Assert.assertTrue("worker must retain resource ownership", client.sendThread.get().isAlive());

                client.releaseSend.countDown();
                Assert.assertTrue("released send did not exit", client.sendExited.await(5, TimeUnit.SECONDS));
                Assert.assertTrue("worker did not complete delegated cleanup", client.cleanupDone.await(5, TimeUnit.SECONDS));
                client.sendThread.get().join(TimeUnit.SECONDS.toMillis(5));
                Assert.assertFalse("worker lingered after the custom transport was released", client.sendThread.get().isAlive());
                Assert.assertEquals(1, client.cleanupCount.get());
                Assert.assertNull("ordinary worker exit must remain non-terminal", loop.getTerminalError());
            } finally {
                client.releaseSend.countDown();
                if (closer != null) {
                    closer.join(TimeUnit.SECONDS.toMillis(5));
                }
                Thread ioThread = client.sendThread.get();
                if (ioThread != null) {
                    ioThread.join(TimeUnit.SECONDS.toMillis(5));
                }
                loop.close();
                engine.close();
                client.close();
                Unsafe.free(payload, 16, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private static final class BlockingSendClient extends WebSocketClient {
        private final AtomicBoolean cleanupClaimed = new AtomicBoolean();
        private final AtomicInteger cleanupCount = new AtomicInteger();
        private final CountDownLatch cleanupDone = new CountDownLatch(1);
        private final AtomicReference<Thread> cleanupThread = new AtomicReference<>();
        private final boolean trafficShutdownSupported;
        private final CountDownLatch releaseSend = new CountDownLatch(1);
        private final CountDownLatch sendEntered = new CountDownLatch(1);
        private final CountDownLatch sendExited = new CountDownLatch(1);
        private final AtomicReference<Thread> sendThread = new AtomicReference<>();
        private final AtomicInteger trafficCloseCount = new AtomicInteger();
        private final AtomicReference<Thread> trafficCloseThread = new AtomicReference<>();
        private final CountDownLatch trafficClosed = new CountDownLatch(1);

        private BlockingSendClient(boolean trafficShutdownSupported) {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
            this.trafficShutdownSupported = trafficShutdownSupported;
        }

        @Override
        public void close() {
            if (cleanupClaimed.compareAndSet(false, true)) {
                cleanupCount.incrementAndGet();
                cleanupThread.set(Thread.currentThread());
                cleanupDone.countDown();
            }
            super.close();
        }

        @Override
        public void closeTraffic() {
            if (!trafficShutdownSupported) {
                throw new UnsupportedOperationException("custom transport has no safe cancellation capability");
            }
            trafficCloseThread.compareAndSet(null, Thread.currentThread());
            trafficCloseCount.incrementAndGet();
            trafficClosed.countDown();
            releaseSend.countDown();
        }

        @Override
        public void sendBinary(long dataPtr, int length, int timeout) {
            sendThread.set(Thread.currentThread());
            sendEntered.countDown();
            try {
                if (!releaseSend.await(10, TimeUnit.SECONDS)) {
                    throw new AssertionError("traffic path was not closed");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("I/O worker was interrupted instead of traffic being closed", e);
            } finally {
                sendExited.countDown();
            }
            throw new LineSenderException("traffic path closed");
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
