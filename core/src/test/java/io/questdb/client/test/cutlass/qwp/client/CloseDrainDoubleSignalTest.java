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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.LineSenderServerException;
import io.questdb.client.Sender;
import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
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
 * Regression test for finding <b>M3</b> (close-error-race review): a
 * pre-existing {@code drainOnClose()} double-signal. Written RED, now green
 * after the drain stopped re-surfacing a terminal a custom handler already
 * owns; guards against the double-signal regressing.
 * <p>
 * When ALL of the following hold:
 * <ul>
 *   <li>an async custom error handler has already received the terminal
 *       (so {@code SenderErrorDispatcher.hasDeliveredToCustomHandler()} is
 *       true), and</li>
 *   <li>{@code close_flush_timeout_millis > 0} (so the bounded drain runs),
 *       and</li>
 *   <li>an unacked tail remains ({@code ackedFsn < publishedFsn} — the
 *       natural state after a TERMINAL rejection, which never advances
 *       {@code ackedFsn}),</li>
 * </ul>
 * then {@code QwpWebSocketSender.close()} re-throws the same terminal error
 * the user's handler already owns.
 * <p>
 * Root cause: in {@code close()} the {@code alreadyDeliveredToCustomHandler}
 * guard wraps ONLY the step-2 {@code checkUnsurfacedError()} safety net, not
 * the step-3 {@code drainOnClose()}. The drain's loop calls
 * {@code cursorSendLoop.checkError()} (QwpWebSocketSender.java:2657)
 * unconditionally, which re-throws the latched terminal. The end-of-close
 * self-suppression ({@code terminalError == alreadyOwnedByUser}) cannot catch
 * this because {@code alreadyOwnedByUser} is sourced from
 * {@code getSynchronouslySurfacedError()} — it tracks SYNCHRONOUS ownership
 * (a producer-thread {@code flush()}/{@code at()} that caught the error)
 * only, and is {@code null} when the error reached the user purely through
 * the async handler.
 * <p>
 * This contradicts the in-code comment's stated intent: "once the user has
 * owned an error, close() should not double-signal it."
 * <p>
 * The sibling green test
 * {@link CloseSafetyNetTest#testCloseStaysSilentWhenCustomHandlerAlreadyDelivered()}
 * passes only because it uses {@code close_flush_timeout_millis=0}, which
 * makes {@code drainOnClose()} return at its first guard so the buggy
 * {@code checkError()} is never reached. This test flips that one knob on and
 * adds the unacked tail.
 * <p>
 * Fix: {@code drainOnClose(boolean)} still stops on a latched terminal (acks
 * will never reach target) but re-throws only when no custom handler owns the
 * error, mirroring the step-2 safety-net gate.
 * <p>
 * Determinism: the server fixture holds the TERMINAL rejection behind a gate that
 * the test releases only AFTER {@code flush()} has returned. Without the gate,
 * the rejection could latch before {@code flush()}'s own
 * {@code checkError()} runs, surfacing the error synchronously, populating
 * {@code synchronouslySurfacedError}, and masking the double-signal via the
 * identity self-suppression (flaky green). Gating guarantees the error
 * reaches the user ONLY through the async handler — the exact precondition
 * the finding describes.
 */
public class CloseDrainDoubleSignalTest {

    @Test(timeout = 30_000)
    public void testCloseDoesNotDoubleSignalWhenAsyncHandlerOwnsErrorAndDrainRuns() throws Exception {
        GatedHaltHandler server = new GatedHaltHandler();
        try (TestWebSocketServer ws = new TestWebSocketServer(server)) {
            ws.start();
            Assert.assertTrue(ws.awaitStart(5, TimeUnit.SECONDS));

            int port = ws.getPort();
            // Memory mode + a positive drain timeout: drainOnClose() WILL run.
            String cfg = "ws::addr=localhost:" + port
                    + ";close_flush_timeout_millis=2000;";

            ErrorInbox inbox = new ErrorInbox();
            Sender sender = Sender.builder(cfg).errorHandler(inbox).build();
            try {
                // Publish exactly one batch: publishedFsn -> 1. flush() runs
                // its own checkError() here while the server is still holding
                // the rejection behind the gate, so it returns cleanly and
                // nothing is surfaced synchronously.
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();

                // Now let the server emit the TERMINAL rejection. It latches the
                // terminal (recordFatal) and dispatches it to the async
                // handler (dispatchError). ackedFsn stays at 0 (TERMINAL never
                // advances the watermark) -> the unacked tail precondition.
                server.releaseRejection();

                Assert.assertTrue(
                        "precondition: TERMINAL terminal must reach the async custom handler within 10s",
                        inbox.await(10, TimeUnit.SECONDS));

                SenderError delivered = inbox.get();
                Assert.assertNotNull("precondition: handler received a SenderError", delivered);
                Assert.assertEquals(
                        "precondition: server status 0x05 must map to PARSE_ERROR",
                        SenderError.Category.PARSE_ERROR, delivered.getCategory());
                Assert.assertEquals(
                        "precondition: PARSE_ERROR is a TERMINAL-policy rejection",
                        SenderError.Policy.TERMINAL, delivered.getAppliedPolicy());

                // Sanity-check the remaining preconditions are actually live
                // before we exercise close(): the terminal is latched and an
                // unacked tail remains.
                QwpWebSocketSender wss = (QwpWebSocketSender) sender;
                Assert.assertNotNull(
                        "precondition: I/O loop latched the terminal server error",
                        wss.getLastTerminalError());

                // The async handler now OWNS the error. Per the in-code
                // contract, close() must NOT double-signal it. Today close()
                // -> drainOnClose() -> checkError() re-throws it.
                try {
                    sender.close();
                } catch (LineSenderException e) {
                    Assert.fail(
                            "M3: close() double-signalled a terminal error the async custom "
                                    + "handler already owns. drainOnClose() (QwpWebSocketSender.java:2657) "
                                    + "re-threw from close(): "
                                    + e.getClass().getSimpleName() + ": " + e.getMessage());
                }
            } finally {
                // Idempotent: if the assertion above already drove close() to
                // completion (it runs all cleanup before re-throwing), this is
                // a no-op. Guarantees teardown if a precondition assert threw
                // before we reached close().
                server.releaseRejection();
                sender.close();
            }
        }
    }

    /**
     * Server fixture that responds to the first binary frame with a
     * {@code STATUS_PARSE_ERROR} (TERMINAL-policy) rejection, but only once the
     * test releases the gate. Blocking inside {@code onBinaryMessage} mirrors
     * the established {@code DelayingAckHandler} pattern in {@link CloseDrainTest}.
     */
    private static final class GatedHaltHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final CountDownLatch gate = new CountDownLatch(1);
        private final AtomicLong nextSeq = new AtomicLong();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                gate.await();
                client.sendBinary(buildErrorAck(nextSeq.getAndIncrement(),
                        WebSocketResponse.STATUS_PARSE_ERROR, "test: parse error (TERMINAL)"));
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        void releaseRejection() {
            gate.countDown();
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
        public void onError(SenderError err) {
            if (ref.compareAndSet(null, err)) {
                latch.countDown();
            }
        }
    }
}
