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

import io.questdb.client.Sender;
import io.questdb.client.SenderError;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Regression: a HALT-policy NACK from the server (e.g.
 * {@code STATUS_PARSE_ERROR}) is a data-poisoning signal — reconnecting
 * and replaying the same bytes cannot fix it. The cursor I/O loop must
 * mark the sender terminal, surface the error to the next user-thread
 * API call, and NOT enter the reconnect retry loop.
 * <p>
 * Pre-fix the loop routes a non-success ACK through {@code fail()},
 * which reconnects on success → replays the same bad bytes → server
 * rejects again → fail() with a fresh per-outage budget. Result:
 * infinite loop within (and beyond) {@code reconnect_max_duration_millis},
 * the bad frame stays on disk in SF / drainer mode, and CPU + reconnect
 * attempts climb forever.
 * <p>
 * Note: the fixture must use a HALT-policy status byte
 * ({@link WebSocketResponse#STATUS_PARSE_ERROR}). HALT is the only policy
 * with terminal semantics. {@code STATUS_SCHEMA_MISMATCH} maps to
 * {@code DROP_AND_CONTINUE} per spec — DROP advances {@code ackedFsn}
 * past the rejected span and the loop continues, so the test's
 * "next flush() throws" assertion would not hold under DROP.
 */
public class ServerErrorAckTerminalTest {

    private static final int TEST_PORT = 19_400 + (int) (System.nanoTime() % 100);

    @Test
    public void testServerErrorAckIsTerminalAndDoesNotBurnReconnectBudget() throws Exception {
        int port = TEST_PORT + 1;
        ErrorAckHandler handler = new ErrorAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            // Tight reconnect cadence so the pre-fix loop accumulates
            // attempts quickly inside our observation window.
            String cfg = "ws::addr=localhost:" + port
                    + ";reconnect_max_duration_millis=10000"
                    + ";reconnect_initial_backoff_millis=10"
                    + ";reconnect_max_backoff_millis=50"
                    + ";";

            Sender sender = Sender.fromConfig(cfg);
            try {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();

                // Wait for the server to actually receive the batch and
                // for the error-ACK round-trip to complete.
                waitFor(() -> handler.totalBinaryReceived.get() >= 1, 5_000);

                // Give the I/O loop room to either go terminal (post-fix)
                // or spin up its reconnect cycle (pre-fix). 500ms at 10ms
                // initial backoff is enough for several pre-fix cycles.
                Thread.sleep(500);

                QwpWebSocketSender wss = (QwpWebSocketSender) sender;
                long attempts = wss.getTotalReconnectAttempts();
                Assert.assertEquals(
                        "non-success ACK must be terminal — the reconnect "
                                + "loop must not fire because reconnecting + "
                                + "replaying poisoned bytes can't fix the "
                                + "rejection. Saw " + attempts
                                + " reconnect attempt(s).",
                        0L, attempts);

                // Subsequent API call must surface the terminal failure to
                // the user thread so they can see the underlying server
                // error rather than a silent stall.
                LineSenderException thrown = null;
                try {
                    sender.table("foo").longColumn("v", 2L).atNow();
                    sender.flush();
                } catch (LineSenderException e) {
                    thrown = e;
                }
                Assert.assertNotNull(
                        "next flush() after a server error-ACK must throw "
                                + "LineSenderException to surface the rejection",
                        thrown);
                Assert.assertTrue(
                        "exception message should reference the server "
                                + "rejection; got: " + thrown.getMessage(),
                        thrown.getMessage() != null
                                && (thrown.getMessage().contains("rejected")
                                    || thrown.getMessage().contains("error")));
            } finally {
                // close() rethrows the latched terminal server-rejection error
                // (commit 052f6ee). Swallow it here — the test has already
                // observed and asserted on that error via flush() above.
                try {
                    sender.close();
                } catch (LineSenderException ignored) {
                }
            }
        }
    }

    /**
     * Sibling of the HALT test above: a DROP_AND_CONTINUE policy NACK
     * (e.g. {@code STATUS_SCHEMA_MISMATCH}) must NOT make the loop
     * terminal. The spec contract for DROP is:
     * <ul>
     *   <li>{@code getLastTerminalError()} stays {@code null} (no latch)</li>
     *   <li>The reconnect loop does not fire (replay can't fix the rejection,
     *       and DROP does not pretend it can)</li>
     *   <li>{@code engine.acknowledge(fsn)} runs, advancing
     *       {@code ackedFsn} past the rejected span — observable via
     *       {@code getTotalAcks() &gt; 0}</li>
     *   <li>The user error handler fires asynchronously with the typed
     *       payload carrying {@link SenderError.Policy#DROP_AND_CONTINUE}</li>
     *   <li>The next {@code flush()} does NOT throw — the sender is
     *       still operational and dropped only the rejected batch</li>
     * </ul>
     */
    @Test
    public void testDropPolicyNackDoesNotHaltAndAdvancesAck() throws Exception {
        int port = TEST_PORT + 2;
        SchemaMismatchAckHandler handler = new SchemaMismatchAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String cfg = "ws::addr=localhost:" + port
                    + ";reconnect_max_duration_millis=10000"
                    + ";reconnect_initial_backoff_millis=10"
                    + ";reconnect_max_backoff_millis=50"
                    + ";";

            AtomicReference<SenderError> observedError = new AtomicReference<>();
            try (Sender sender = Sender.builder(cfg)
                    .errorHandler(observedError::set)
                    .build()) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();

                waitFor(() -> handler.totalBinaryReceived.get() >= 1, 5_000);
                // Allow time for the rejection round-trip + dispatcher
                // delivery; DROP path also acknowledges, so wait for an ack
                // tick.
                QwpWebSocketSender wss = (QwpWebSocketSender) sender;
                long deadline = System.nanoTime() + 3_000_000_000L;
                while (System.nanoTime() < deadline
                        && (wss.getTotalServerErrors() == 0L
                            || observedError.get() == null)) {
                    Thread.sleep(10);
                }

                Assert.assertEquals(
                        "DROP path must not enter reconnect loop",
                        0L, wss.getTotalReconnectAttempts());
                Assert.assertNull(
                        "DROP must not latch a terminal error: getLastTerminalError() should stay null",
                        wss.getLastTerminalError());
                Assert.assertTrue(
                        "DROP path must record the server error in totalServerErrors",
                        wss.getTotalServerErrors() > 0L);
                Assert.assertTrue(
                        "DROP path must advance ackedFsn (visible via totalAcks)",
                        wss.getTotalAcks() > 0L);

                SenderError err = observedError.get();
                Assert.assertNotNull(
                        "user error handler must fire on DROP rejection",
                        err);
                Assert.assertEquals(
                        "handler must observe DROP_AND_CONTINUE policy",
                        SenderError.Policy.DROP_AND_CONTINUE, err.getAppliedPolicy());
                Assert.assertEquals(
                        "category must be SCHEMA_MISMATCH for status 0x03",
                        SenderError.Category.SCHEMA_MISMATCH, err.getCategory());

                // Sender must still be operational — the next flush() must
                // not throw a terminal exception.
                sender.table("foo").longColumn("v", 2L).atNow();
                sender.flush();
            }
        }
    }

    /** Server returns {@code STATUS_SCHEMA_MISMATCH} (DROP_AND_CONTINUE policy) for every received frame. */
    private static class SchemaMismatchAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicLong totalBinaryReceived = new AtomicLong();
        private final AtomicLong nextSeq = new AtomicLong();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            totalBinaryReceived.incrementAndGet();
            try {
                client.sendBinary(buildErrorAck(nextSeq.getAndIncrement(),
                        WebSocketResponse.STATUS_SCHEMA_MISMATCH,
                        "test: schema mismatch"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** Server returns {@code STATUS_PARSE_ERROR} (HALT-policy) for every received frame. */
    private static class ErrorAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicLong totalBinaryReceived = new AtomicLong();
        private final AtomicLong nextSeq = new AtomicLong();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            totalBinaryReceived.incrementAndGet();
            try {
                client.sendBinary(buildErrorAck(nextSeq.getAndIncrement(),
                        WebSocketResponse.STATUS_PARSE_ERROR,
                        "test: parse error"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // Mirrors WebSocketResponse error layout: status u8 | seq u64 | msgLen u16 | msg UTF-8
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

    private static void waitFor(BoolCondition cond, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (cond.test()) return;
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Assert.fail("interrupted");
            }
        }
        Assert.fail("waitFor timed out after " + timeoutMillis + "ms");
    }

    @FunctionalInterface
    private interface BoolCondition {
        boolean test();
    }
}
