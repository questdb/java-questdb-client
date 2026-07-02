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
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Red end-to-end tests for the critical findings raised during the PR-17 code
 * review that need a real {@link TestWebSocketServer} fixture. Each test is
 * intentionally written to FAIL on current {@code vi_sf} HEAD.
 */
public class PrReviewRedTestsE2e {

    /**
     * Finding C4 — {@code recordFatal} is called AFTER {@code dispatchError}
     * in three sites of {@code CursorWebSocketSendLoop}:
     * <ul>
     *   <li>{@code handleServerRejection} TERMINAL branch (lines 864-871)</li>
     *   <li>{@code fail()} auth-terminal branch (lines 437-438)</li>
     *   <li>{@code fail()} budget-exhausted branch (lines 484-485)</li>
     * </ul>
     * The error-API contract ("Path 2: producer-side typed throw") requires
     * {@code signal.terminalError = err} to be written BEFORE
     * {@code errorInbox.offer(err)}.
     * <p>
     * Concrete consequence the spec calls out: a user-supplied error handler
     * that synchronously calls {@code sender.flush()} from inside
     * {@code onError} can observe {@code terminalError == null} and pass —
     * landing post-TERMINAL bytes in the engine.
     * <p>
     * This test asserts the spec invariant directly: by the time the
     * dispatcher delivers a {@link SenderError} to the user handler,
     * {@code QwpWebSocketSender#getLastTerminalError()} MUST already return
     * the same payload. We run multiple iterations to amplify race
     * observability.
     */
    @Test
    public void testC4_handlerMustObserveTerminalErrorWhenInvoked() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int iterations = 30;
            AtomicInteger nullObservations = new AtomicInteger();
            AtomicInteger totalObservations = new AtomicInteger();

            ParseErrorAckHandler serverHandler = new ParseErrorAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(serverHandler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                for (int iter = 0; iter < iterations; iter++) {
                    AtomicReference<QwpWebSocketSender> wssRef = new AtomicReference<>();
                    AtomicReference<Boolean> observedNonNull = new AtomicReference<>();
                    SenderErrorHandler handler = err -> {
                        QwpWebSocketSender wss = wssRef.get();
                        if (wss != null) {
                            // Spec: by the time the dispatcher fires the
                            // handler, the producer-observable terminal
                            // error MUST already be latched. If null, the
                            // I/O thread offered to the inbox before
                            // recordFatal — exactly the bug.
                            SenderError latched = wss.getLastTerminalError();
                            totalObservations.incrementAndGet();
                            if (latched == null) {
                                nullObservations.incrementAndGet();
                                observedNonNull.set(Boolean.FALSE);
                            } else {
                                observedNonNull.set(Boolean.TRUE);
                            }
                        }
                    };

                    String cfg = "ws::addr=localhost:" + port + ";";
                    try (Sender s = Sender.builder(cfg).errorHandler(handler).build()) {
                        wssRef.set((QwpWebSocketSender) s);
                        try {
                            s.table("foo").longColumn("v", 1L).atNow();
                            s.flush();
                        } catch (LineSenderException ignored) {
                            // Expected on TERMINAL path.
                        }
                        // Give the dispatcher up to 2s to fire the handler.
                        long deadline = System.nanoTime() + 2_000_000_000L;
                        while (System.nanoTime() < deadline && observedNonNull.get() == null) {
                            Thread.sleep(2);
                        }
                    } catch (LineSenderException ignored) {
                        // Sender close may also surface the terminal error.
                    }
                }
            }

            Assert.assertTrue(
                    "FINDING C4: dispatcher invoked handler at least once across "
                            + iterations + " iterations",
                    totalObservations.get() > 0);
            Assert.assertEquals(
                    "FINDING C4: spec requires signal.terminalError to be written "
                            + "BEFORE errorInbox.offer. Out of " + totalObservations.get()
                            + " handler invocations, " + nullObservations.get()
                            + " observed lastTerminalError == null at handler entry. "
                            + "The bug is in CursorWebSocketSendLoop.handleServerRejection "
                            + "and the two fail() branches: dispatchError must run AFTER "
                            + "recordFatal, not before.",
                    0, nullObservations.get());
        });
    }

    /**
     * Finding C11 — there is no end-to-end test pinning the central
     * user-visible contract of the new error API: a {@code flush()} after
     * the I/O loop has latched a TERMINAL-policy server rejection must throw a
     * typed {@link LineSenderServerException} carrying the matching
     * {@link SenderError} payload (category, policy, server message,
     * fromFsn).
     * <p>
     * Without this test, the spec contract is unverified on the e2e path.
     * Adding it here also guards against regressions to the
     * {@code recordFatal → checkError → producer-throw} chain.
     */
    @Test
    public void testC11_postHaltFlushThrowsTypedLineSenderServerException() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            ParseErrorAckHandler serverHandler = new ParseErrorAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(serverHandler)) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                String cfg = "ws::addr=localhost:" + port + ";";
                try (Sender sender = Sender.fromConfig(cfg)) {
                    // First batch — server returns STATUS_PARSE_ERROR (TERMINAL).
                    sender.table("foo").longColumn("v", 1L).atNow();
                    try {
                        sender.flush();
                    } catch (LineSenderException ignored) {
                        // The first flush may or may not surface the error
                        // depending on timing — the I/O loop processes ACKs
                        // asynchronously.
                    }

                    // Wait for the I/O loop to record the terminal error.
                    QwpWebSocketSender wss = (QwpWebSocketSender) sender;
                    long deadline = System.nanoTime() + 3_000_000_000L;
                    while (System.nanoTime() < deadline
                            && wss.getLastTerminalError() == null) {
                        Thread.sleep(10);
                    }
                    SenderError latched = wss.getLastTerminalError();
                    Assert.assertNotNull(
                            "FINDING C11: server emitted STATUS_PARSE_ERROR (TERMINAL) but "
                                    + "the I/O loop did not latch a typed terminal error within 3s",
                            latched);

                    // The contract under test: the next flush() MUST throw
                    // LineSenderServerException carrying the same SenderError.
                    LineSenderException thrown = null;
                    try {
                        sender.flush();
                        Assert.fail(
                                "FINDING C11: flush() after TERMINAL must throw "
                                        + "LineSenderServerException; instead returned cleanly. "
                                        + "Producer-thread typed-throw contract is broken.");
                    } catch (LineSenderException e) {
                        thrown = e;
                    }
                    Assert.assertTrue(
                            "FINDING C11: thrown exception must be LineSenderServerException "
                                    + "(typed). Got " + thrown.getClass().getName()
                                    + " — the producer cannot inspect the server payload.",
                            thrown instanceof LineSenderServerException);
                    SenderError payload = ((LineSenderServerException) thrown).getServerError();
                    Assert.assertNotNull("FINDING C11: getServerError() returned null", payload);
                    Assert.assertEquals(
                            "FINDING C11: category should be PARSE_ERROR for status byte 0x05",
                            SenderError.Category.PARSE_ERROR, payload.getCategory());
                    Assert.assertEquals(
                            "FINDING C11: policy should be TERMINAL for PARSE_ERROR",
                            SenderError.Policy.TERMINAL, payload.getAppliedPolicy());
                    Assert.assertTrue(
                            "FINDING C11: fromFsn should be >= 0; got " + payload.getFromFsn(),
                            payload.getFromFsn() >= 0L);
                } catch (LineSenderException expectedOnClose) {
                    // close() may also surface the same terminal error;
                    // that's fine — the contract is about the next flush()
                    // call, which is what we asserted above.
                }
            }
        });
    }

    /**
     * Server fixture that responds to every binary frame with
     * {@code STATUS_PARSE_ERROR} (a TERMINAL-policy rejection per spec).
     */
    private static final class ParseErrorAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(buildErrorAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // Mirrors WebSocketResponse error layout:
        //   status u8 | seq u64 LE | msgLen u16 LE | msg UTF-8
        private static byte[] buildErrorAck(long seq) {
            byte[] msg = "test: parse error".getBytes(StandardCharsets.UTF_8);
            byte[] buf = new byte[1 + 8 + 2 + msg.length];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put(WebSocketResponse.STATUS_PARSE_ERROR);
            bb.putLong(seq);
            bb.putShort((short) msg.length);
            bb.put(msg);
            return buf;
        }
    }
}
