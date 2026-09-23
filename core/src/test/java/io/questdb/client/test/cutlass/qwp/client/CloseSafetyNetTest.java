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

import io.questdb.client.LineSenderServerException;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pins both branches of {@code QwpWebSocketSender.close()}'s safety-net
 * rethrow using a deterministic server-side parse rejection:
 * <ul>
 * <li>a config-string-only caller (no custom error handler) who never calls
 * flush() MUST see the latched terminal thrown from close() — close() is the
 * only channel left;</li>
 * <li>a caller whose custom error handler already received the terminal MUST
 * NOT have it thrown again from close() — that would double-signal an error
 * the user already handled and mask try-with-resources cleanup.</li>
 * </ul>
 * Both tests latch the terminal deterministically (await it) before calling
 * close(), so they pin the safety net's logic, not the snapshot race —
 * {@code CloseOwnershipRaceTest} covers that separately.
 */
public class CloseSafetyNetTest {

    @Rule
    public final TemporaryFolder sfDir = TemporaryFolder.builder().assureDeletion().build();

    @Test(timeout = 30_000)
    public void testCloseRethrowsUnsurfacedTerminalWithoutCustomHandler() throws Exception {
        GatedHaltHandler handler = new GatedHaltHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            Sender sender = Sender.fromConfig(cfg(server.getPort()));
            boolean closeAttempted = false;
            try {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                handler.releaseRejection();
                awaitLatchedTerminal((QwpWebSocketSender) sender);
                try {
                    closeAttempted = true;
                    sender.close();
                    Assert.fail("close() must rethrow a terminal error that no synchronous "
                            + "caller and no custom handler has seen");
                } catch (LineSenderException e) {
                    String msg = e.getMessage() == null ? "" : e.getMessage();
                    Assert.assertTrue("close() must rethrow the latched terminal: " + msg,
                            msg.contains("rejected") || msg.contains("parse error"));
                    Assert.assertTrue("the latched instance is the typed server exception",
                            e instanceof LineSenderServerException);
                }
            } finally {
                handler.releaseRejection();
                if (!closeAttempted) {
                    closeQuietly(sender);
                }
            }
        }
    }

    @Test(timeout = 30_000)
    public void testCloseStaysSilentWhenCustomHandlerAlreadyDelivered() throws Exception {
        GatedHaltHandler handler = new GatedHaltHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            ErrorInbox inbox = new ErrorInbox();
            Sender sender = Sender.builder(cfg(server.getPort()))
                    .errorHandler(inbox)
                    .build();
            boolean closed = false;
            try {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                handler.releaseRejection();
                Assert.assertTrue("terminal must reach the custom handler within 10s",
                        inbox.await(10, TimeUnit.SECONDS));
                Assert.assertNotNull(inbox.get());
                // The handler owns the error now; a rethrow here would double-signal.
                sender.close();
                closed = true;
            } finally {
                handler.releaseRejection();
                if (!closed) {
                    closeQuietly(sender);
                }
            }
        }
    }

    /**
     * Awaits the I/O thread's terminal latch via the read-only ops probe.
     * getLastTerminalError() does not mark the error as surfaced, so the
     * "no synchronous caller has seen it" precondition stays intact.
     */
    private static void awaitLatchedTerminal(QwpWebSocketSender sender) {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (sender.getLastTerminalError() == null) {
            if (System.nanoTime() > deadlineNanos) {
                throw new AssertionError("I/O thread did not latch a terminal within 10s");
            }
            io.questdb.client.std.Compat.onSpinWait();
        }
    }

    private String cfg(int port) {
        return "ws::addr=localhost:" + port
                + ";sf_dir=" + sfDir.getRoot().getAbsolutePath()
                + ";close_flush_timeout_millis=0;";
    }

    private static void closeQuietly(Sender sender) {
        try {
            sender.close();
        } catch (Throwable ignored) {
            // Cleanup after an earlier assertion; the primary failure wins.
        }
    }

    private static final class GatedHaltHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final CountDownLatch gate = new CountDownLatch(1);
        private final AtomicLong nextSeq = new AtomicLong();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                gate.await();
                client.sendBinary(buildErrorAck(nextSeq.getAndIncrement()));
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        void releaseRejection() {
            gate.countDown();
        }

        private static byte[] buildErrorAck(long seq) {
            byte[] message = "test: parse error".getBytes(StandardCharsets.UTF_8);
            byte[] response = new byte[1 + 8 + 2 + message.length];
            ByteBuffer buffer = ByteBuffer.wrap(response).order(ByteOrder.LITTLE_ENDIAN);
            buffer.put(WebSocketResponse.STATUS_PARSE_ERROR);
            buffer.putLong(seq);
            buffer.putShort((short) message.length);
            buffer.put(message);
            return response;
        }
    }

    private static class ErrorInbox implements SenderErrorHandler {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicReference<SenderError> ref = new AtomicReference<>();

        boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }

        SenderError get() {
            return ref.get();
        }

        @Override
        public void onError(@NotNull SenderError err) {
            if (ref.compareAndSet(null, err)) {
                latch.countDown();
            }
        }
    }
}
