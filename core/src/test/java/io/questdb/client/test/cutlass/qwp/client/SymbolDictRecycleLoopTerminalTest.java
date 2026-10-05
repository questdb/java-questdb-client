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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.LineSenderServerException;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SlotLock;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A terminal error the outgoing I/O loop latches while the symbol-dictionary
 * recycle is stopping it. {@code recordFatal} leaves the connect gate open,
 * so the stop still succeeds; the recycle must then keep the dead loop and
 * throw its error instead of swapping, or the sender would go on accepting
 * rows after a terminal rejection. Both places that drop a stopped loop are
 * pinned: recycle step 2, and the resume that finishes a failed step-2 stop.
 */
public class SymbolDictRecycleLoopTerminalTest {

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    @Test(timeout = 30_000L)
    public void testTerminalLatchedWhileStep2StopsTheLoopIsThrownAndNothingIsSwapped() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Disk mode, so step 0 takes the slot's logical lock and the test can see it given back.
            final String slot = new File(temporaryFolder.newFolder("sf"), "default").getAbsolutePath();
            final NackOnDemandClient client = new NackOnDemandClient();
            final CursorSendEngine engine = new CursorSendEngine(slot, 4L * 1024 * 1024);
            final CursorWebSocketSendLoop loop = newLoop(client, engine);
            final AtomicInteger rebuilds = new AtomicInteger();
            final QwpWebSocketSender sender = newArmedSender(engine, loop, rebuilds);
            try {
                final AtomicBoolean latchedInsideTheStop = new AtomicBoolean();
                client.onLinkCheckInside("closeIfLinkUp", () -> {
                    client.deliverSecurityNack();
                    latchedInsideTheStop.set(awaitTerminal(loop));
                });

                try {
                    sender.table("t");
                    Assert.fail("the row start must throw the terminal the loop latched while stopping");
                } catch (LineSenderException e) {
                    Assert.assertTrue("the terminal must have been latched inside the stop's own link check",
                            latchedInsideTheStop.get());
                    assertSecurityTerminal(loop, e);
                }
                Assert.assertEquals("a terminal loop must not be swapped out", 0, rebuilds.get());
                Assert.assertEquals(0L, sender.getSymbolDictEpoch());
                Assert.assertSame("the engine must stay attached", engine, sender.getCursorEngineForTesting());
                // Throws SlotLockContentionException while the recycle still holds it.
                SlotLock.acquireLogical(slot).close();

                try {
                    sender.table("t");
                    Assert.fail("every later call must keep throwing the terminal");
                } catch (LineSenderException e) {
                    assertSecurityTerminal(loop, e);
                }
            } finally {
                sender.close();
            }
        });
    }

    @Test(timeout = 30_000L)
    public void testTerminalLatchedWhileTheResumeFinishesTheStopIsThrown() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final NackOnDemandClient client = new NackOnDemandClient();
            final CursorSendEngine engine = new CursorSendEngine(null, 64 * 1024);
            final CursorWebSocketSendLoop loop = newLoop(client, engine);
            // Step 2 must fail fast: break traffic up front, then give the stuck I/O thread 200 ms.
            loop.setGracefulStopMillis(0L);
            loop.setShutdownAwaitTimeoutMillis(200L);
            final AtomicInteger rebuilds = new AtomicInteger();
            final QwpWebSocketSender sender = newArmedSender(engine, loop, rebuilds);
            try {
                client.blockNextReceive();
                Assert.assertTrue("the I/O thread never entered the blocked receive",
                        client.awaitReceiveBlocked());

                try {
                    sender.table("t");
                    Assert.fail("step 2 must abandon the recycle when the I/O thread does not stop");
                } catch (LineSenderException e) {
                    TestUtils.assertContains(e.getMessage(), "cursor I/O thread did not stop");
                }
                Assert.assertNull("precondition: no terminal yet", loop.getTerminalError());

                // The resume's close() breaks traffic again; this time the receive returns with the rejection.
                final AtomicBoolean latchedInsideTheStop = new AtomicBoolean();
                client.onNextCloseTraffic(() -> {
                    client.deliverSecurityNack();
                    client.releaseReceive();
                    latchedInsideTheStop.set(awaitTerminal(loop));
                });

                try {
                    sender.table("t");
                    Assert.fail("the resume must throw the terminal the loop latched while stopping");
                } catch (LineSenderException e) {
                    Assert.assertTrue("the terminal must have been latched inside the resume's stop",
                            latchedInsideTheStop.get());
                    assertSecurityTerminal(loop, e);
                }
                Assert.assertEquals(0, rebuilds.get());
                Assert.assertEquals(0L, sender.getSymbolDictEpoch());

                try {
                    sender.table("t");
                    Assert.fail("every later call must keep throwing the terminal");
                } catch (LineSenderException e) {
                    assertSecurityTerminal(loop, e);
                }
            } finally {
                client.releaseReceive();
                loop.setShutdownAwaitTimeoutMillis(CursorWebSocketSendLoop.DEFAULT_CLOSE_SHUTDOWN_AWAIT_MILLIS);
                sender.close();
            }
        });
    }

    private static void assertSecurityTerminal(CursorWebSocketSendLoop loop, LineSenderException thrown) {
        Assert.assertSame("the call must throw the loop's own terminal", loop.getTerminalError(), thrown);
        Assert.assertTrue(thrown instanceof LineSenderServerException);
        Assert.assertEquals(SenderError.Category.SECURITY_ERROR,
                ((LineSenderServerException) thrown).getServerError().getCategory());
    }

    private static boolean awaitTerminal(CursorWebSocketSendLoop loop) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (loop.getTerminalError() == null && System.nanoTime() < deadline) {
            Thread.yield();
        }
        return loop.getTerminalError() != null;
    }

    /**
     * A connected, drained sender on {@code loop} with a manual reset armed,
     * so its next {@code table()} runs the recycle.
     */
    private static QwpWebSocketSender newArmedSender(
            CursorSendEngine engine,
            CursorWebSocketSendLoop loop,
            AtomicInteger rebuilds
    ) {
        QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 1);
        sender.setCursorEngine(engine, true);
        loop.start();
        sender.setCursorSendLoopForTesting(loop);
        sender.setConnectedForTest(true);
        sender.setEngineRebuildFactory(() -> {
            rebuilds.incrementAndGet();
            return new CursorSendEngine(null, 64 * 1024);
        });
        sender.resetSymbolDictionary();
        Assert.assertTrue("precondition: the manual reset must arm", sender.isResetArmed());
        Assert.assertTrue("precondition: the loop holds a live link", loop.isLinkUp());
        return sender;
    }

    private static CursorWebSocketSendLoop newLoop(WebSocketClient client, CursorSendEngine engine) {
        return new CursorWebSocketSendLoop(
                client,
                engine,
                0L,
                CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                () -> {
                    throw new LineSenderException("no reconnect expected");
                },
                /* reconnectInitialBackoffMillis */ 1_000L,
                /* reconnectMaxBackoffMillis */ 5_000L,
                false
        );
    }

    /**
     * A live link that delivers one {@code SECURITY_ERROR} rejection on
     * demand, can hold the I/O thread inside a receive that the first
     * {@code closeTraffic()} does not break, and can run a hook inside a
     * link check made from a named caller.
     */
    private static final class NackOnDemandClient extends WebSocketClient {
        private final CountDownLatch receiveBlocked = new CountDownLatch(1);
        private final CountDownLatch receiveRelease = new CountDownLatch(1);
        private volatile boolean blockNextReceive;
        private volatile boolean closed;
        private volatile Runnable closeTrafficHook;
        private volatile Runnable linkCheckHook;
        private volatile String linkCheckHookCaller;
        private volatile boolean nackPending;

        private NackOnDemandClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void close() {
            closed = true;
            super.close();
        }

        @Override
        public void closeTraffic() {
            Runnable hook = closeTrafficHook;
            if (hook != null) {
                closeTrafficHook = null;
                hook.run();
            }
        }

        @Override
        public boolean isConnected() {
            Runnable hook = linkCheckHook;
            if (hook != null && isCalledFrom(linkCheckHookCaller)) {
                linkCheckHook = null;
                hook.run();
                // What a link check reads when the rejection lands just behind it.
                return true;
            }
            return !closed;
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            if (blockNextReceive) {
                blockNextReceive = false;
                receiveBlocked.countDown();
                while (receiveRelease.getCount() != 0L) {
                    try {
                        receiveRelease.await();
                    } catch (InterruptedException ignored) {
                        // a receive stuck past interrupts: only the release ends it
                    }
                }
            }
            if (!nackPending) {
                return false;
            }
            nackPending = false;
            // Error frame: status(1) + sequence(8) + msgLen(2) + message
            byte[] msg = "read-only instance".getBytes(StandardCharsets.UTF_8);
            int size = 11 + msg.length;
            long ptr = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
            try {
                Unsafe.getUnsafe().putByte(ptr, WebSocketResponse.STATUS_SECURITY_ERROR);
                Unsafe.getUnsafe().putLong(ptr + 1, 0L);
                Unsafe.getUnsafe().putShort(ptr + 9, (short) msg.length);
                for (int i = 0; i < msg.length; i++) {
                    Unsafe.getUnsafe().putByte(ptr + 11 + i, msg[i]);
                }
                handler.onBinaryMessage(ptr, size);
            } finally {
                Unsafe.free(ptr, size, MemoryTag.NATIVE_DEFAULT);
            }
            return true;
        }

        boolean awaitReceiveBlocked() throws InterruptedException {
            return receiveBlocked.await(5, TimeUnit.SECONDS);
        }

        void blockNextReceive() {
            blockNextReceive = true;
        }

        void deliverSecurityNack() {
            nackPending = true;
        }

        /** Runs {@code hook} inside the next {@code isConnected()} call made from the method {@code caller}. */
        void onLinkCheckInside(String caller, Runnable hook) {
            linkCheckHookCaller = caller;
            linkCheckHook = hook;
        }

        void onNextCloseTraffic(Runnable hook) {
            closeTrafficHook = hook;
        }

        void releaseReceive() {
            receiveRelease.countDown();
        }

        @Override
        protected void ioWait(int timeout, int op) {
            throw new UnsupportedOperationException("stub: no socket");
        }

        @Override
        protected void setupIoWait() {
            // no-op
        }

        private static boolean isCalledFrom(String method) {
            for (StackTraceElement frame : new Throwable().getStackTrace()) {
                if (method.equals(frame.getMethodName())) {
                    return true;
                }
            }
            return false;
        }
    }
}
