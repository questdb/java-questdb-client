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

import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Regression test for finding <b>C1</b> (close-error-race review): close()
 * keyed its terminal safety-net on a sticky "the custom handler saw ANY error,
 * ever" flag ({@code SenderErrorDispatcher.hasDeliveredToCustomHandler()})
 * rather than "the custom handler owns THIS terminal". A routine
 * {@code RETRIABLE} rejection set that flag permanently, after which a
 * later genuine TERMINAL error was silently dropped from every synchronous
 * close() channel.
 * <p>
 * This test drives the deterministic, public-API path:
 * <ol>
 *   <li>a {@code WRITE_ERROR} (RETRIABLE) rejection is delivered to a
 *       custom handler — flipping the old "any error ever" flag; the loop
 *       recycles the connection and replays the batch, which then parks on
 *       the server's halt gate;</li>
 *   <li>{@code setErrorHandler(null)} reverts to the loud-not-silent default —
 *       which does NOT reset the sticky flag;</li>
 *   <li>a {@code PARSE_ERROR} (TERMINAL) rejection of the replayed frame
 *       latches with an unacked tail and is dispatched only to the default
 *       handler.</li>
 * </ol>
 * The old code returned from {@code close()} silently (flag still true ->
 * step-2 safety net skipped, drain took its silent branch). The fix gates on
 * {@code hasDeliveredTerminalToCustomHandler()} — true only when the latched
 * terminal itself reached a custom handler — so close() loudly rethrows the
 * terminal the user never saw.
 */
public class CloseTerminalConflationTest {

    @Test(timeout = 30_000)
    public void testCloseSurfacesTerminalAfterEarlierRetriableFlippedTheStickyFlag() throws Exception {
        DropThenGatedHaltHandler server = new DropThenGatedHaltHandler();
        try (TestWebSocketServer ws = new TestWebSocketServer(server)) {
            ws.start();
            Assert.assertTrue(ws.awaitStart(5, TimeUnit.SECONDS));

            int port = ws.getPort();
            // Memory mode + a positive drain timeout: drainOnClose() WILL run.
            String cfg = "ws::addr=localhost:" + port
                    + ";close_flush_timeout_millis=2000;";

            ErrorInbox customInbox = new ErrorInbox();
            Sender sender = Sender.builder(cfg).errorHandler(customInbox).build();
            QwpWebSocketSender wss = (QwpWebSocketSender) sender;
            try {
                // Batch 1: server replies WRITE_ERROR (RETRIABLE). The loop
                // dispatches to the custom handler (-> the old "delivered to
                // custom handler" flag flips true), recycles the connection,
                // and replays batch 1; the replayed frame parks on the
                // server's halt gate. Nothing is dropped, nothing latches.
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();

                Assert.assertTrue(
                        "precondition: the RETRIABLE rejection must reach the custom handler within 10s",
                        customInbox.await(10, TimeUnit.SECONDS));
                Assert.assertEquals(
                        "precondition: status 0x09 maps to WRITE_ERROR",
                        SenderError.Category.WRITE_ERROR, customInbox.get().getCategory());
                Assert.assertEquals(
                        "precondition: WRITE_ERROR defaults to RETRIABLE",
                        SenderError.Policy.RETRIABLE, customInbox.get().getAppliedPolicy());
                Assert.assertNull(
                        "precondition: a RETRIABLE rejection must NOT latch a terminal error",
                        wss.getLastTerminalError());

                // Revert to the loud-not-silent default handler. Per its
                // contract this is exactly the move a caller makes to get a
                // loud shutdown -- but it does not reset the sticky flag.
                wss.setErrorHandler(null);

                // Batch 2: published while the TERMINAL is gated, so flush()'s
                // own checkError() runs clean and nothing is surfaced
                // synchronously. publishedFsn -> 2, ackedFsn stays at 0 (no
                // frame was ever acked) -> the unacked-tail precondition for
                // the drain.
                sender.table("foo").longColumn("v", 2L).atNow();
                sender.flush();

                // Release the gate. The PARSE_ERROR rejection of the replayed
                // frame latches the terminal (recordFatal) and is dispatched
                // ONLY to the default handler -- never to a custom handler --
                // so close() must surface it loudly.
                server.releaseHalt();
                awaitLatchedTerminal(wss);

                try {
                    sender.close();
                    Assert.fail("C1: close() silently dropped a TERMINAL error. The custom handler "
                            + "only ever saw an earlier RETRIABLE rejection, and the terminal went to "
                            + "the default handler after setErrorHandler(null); close() must rethrow it.");
                } catch (LineSenderException expected) {
                    // Correct: the unsurfaced terminal is loud on shutdown.
                }
            } finally {
                server.releaseHalt();
                sender.close();
            }
        }
    }

    private static void awaitLatchedTerminal(QwpWebSocketSender sender) {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (sender.getLastTerminalError() == null) {
            if (System.nanoTime() > deadlineNanos) {
                throw new AssertionError("I/O thread did not latch a terminal within 10s");
            }
            io.questdb.client.std.Compat.onSpinWait();
        }
    }

    // Mirrors WebSocketResponse error layout: status u8 | seq u64 LE | msgLen u16 LE | msg UTF-8
    private static byte[] buildErrorAck(long seq, byte status, String msg) {
        byte[] msgBytes = msg.getBytes(StandardCharsets.UTF_8);
        byte[] buf = new byte[1 + 8 + 2 + msgBytes.length];
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        bb.put(status);
        bb.putLong(seq);
        bb.putShort((short) msgBytes.length);
        bb.put(msgBytes);
        return buf;
    }

    /**
     * First binary frame -> WRITE_ERROR (RETRIABLE) immediately; the client
     * recycles the connection and replays it. Subsequent frames (the replay,
     * then batch 2) -> PARSE_ERROR (TERMINAL), but only once the test releases
     * the gate, so the rejection reaches the user purely through the shutdown
     * path (not through flush()'s synchronous checkError()).
     */
    private static final class DropThenGatedHaltHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final CountDownLatch haltGate = new CountDownLatch(1);
        private final AtomicLong nextSeq = new AtomicLong();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                long seq = nextSeq.getAndIncrement();
                if (seq == 0) {
                    client.sendBinary(buildErrorAck(seq,
                            WebSocketResponse.STATUS_WRITE_ERROR, "test: write error (RETRIABLE)"));
                } else {
                    haltGate.await();
                    client.sendBinary(buildErrorAck(seq,
                            WebSocketResponse.STATUS_PARSE_ERROR, "test: parse error (TERMINAL)"));
                }
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        void releaseHalt() {
            haltGate.countDown();
        }
    }

    private static final class ErrorInbox implements SenderErrorHandler {
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
