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
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Regression: a TERMINAL-policy NACK from the server (e.g.
 * {@code STATUS_PARSE_ERROR}) is a data-poisoning signal — reconnecting
 * and replaying the same bytes cannot fix it. The cursor I/O loop must
 * mark the sender terminal, surface the error to the next user-thread
 * API call, and NOT enter the reconnect retry loop.
 * <p>
 * Note: the fixture must use a TERMINAL-policy status byte
 * ({@link WebSocketResponse#STATUS_PARSE_ERROR}). RETRIABLE statuses
 * (e.g. {@code STATUS_WRITE_ERROR}) recycle the connection and replay
 * from ackedFsn+1 — that path, plus its poison-frame escalation, is
 * covered by the sibling test below.
 */
public class ServerErrorAckTerminalTest {

    @Test
    public void testServerErrorAckIsTerminalAndDoesNotBurnReconnectBudget() throws Exception {
        ErrorAckHandler handler = new ErrorAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
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
     * Sibling of the TERMINAL test above: a RETRIABLE-policy NACK (e.g.
     * {@code STATUS_WRITE_ERROR}) must NOT drop data and must NOT latch a
     * terminal on first sight. The contract is:
     * <ul>
     *   <li>The user error handler fires with the typed payload carrying
     *       {@link SenderError.Policy#RETRIABLE} and category
     *       {@code WRITE_ERROR}</li>
     *   <li>The loop recycles the connection and replays the same frame —
     *       the server observes the batch again (no watermark advance, no
     *       drop: {@code getTotalAcks()} stays 0)</li>
     *   <li>A server that keeps NACKing the same head frame trips the
     *       poison-frame detector after
     *       {@link CursorWebSocketSendLoop#DEFAULT_MAX_HEAD_FRAME_REJECTIONS}
     *       consecutive strikes: a typed {@code PROTOCOL_VIOLATION}
     *       terminal is latched and the next {@code flush()} throws —
     *       instead of reconnect-looping forever</li>
     * </ul>
     */
    @Test
    public void testRetriableNackReplaysThenPoisonEscalates() throws Exception {
        WriteErrorAckHandler handler = new WriteErrorAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port
                    + ";reconnect_max_duration_millis=10000"
                    + ";reconnect_initial_backoff_millis=10"
                    + ";reconnect_max_backoff_millis=50"
                    // Escalate on strike count alone (no wall-clock dwell) so
                    // this test can assert the exact per-strike replay count.
                    + ";poison_min_escalation_window_millis=0"
                    + ";";

            AtomicReference<SenderError> firstError = new AtomicReference<>();
            Sender sender = Sender.builder(cfg)
                    .errorHandler(e -> firstError.compareAndSet(null, e))
                    .build();
            try {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();

                // Each NACK counts one poison strike and triggers a
                // reconnect+replay; after DEFAULT_MAX_HEAD_FRAME_REJECTIONS strikes
                // the loop escalates to a typed terminal. The server must
                // therefore observe the frame exactly that many times.
                waitFor(() -> handler.totalBinaryReceived.get()
                        >= CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS, 10_000);

                QwpWebSocketSender wss = (QwpWebSocketSender) sender;
                waitFor(() -> wss.getLastTerminalError() != null, 5_000);

                Assert.assertEquals(
                        "the same frame must be replayed once per strike -- no more:"
                                + " after escalation the loop must stop replaying",
                        CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                        handler.totalBinaryReceived.get());
                Assert.assertTrue(
                        "each RETRIABLE NACK recycles the wire: expected >= "
                                + (CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS - 1)
                                + " reconnect attempts, saw " + wss.getTotalReconnectAttempts(),
                        wss.getTotalReconnectAttempts()
                                >= CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS - 1);
                Assert.assertEquals(
                        "a rejection must never advance the watermark (no drop)",
                        0L, wss.getTotalAcks());

                SenderError err = firstError.get();
                Assert.assertNotNull("user error handler must fire on RETRIABLE rejection", err);
                Assert.assertEquals(
                        "handler must observe RETRIABLE policy",
                        SenderError.Policy.RETRIABLE, err.getAppliedPolicy());
                Assert.assertEquals(
                        "category must be WRITE_ERROR for status 0x09",
                        SenderError.Category.WRITE_ERROR, err.getCategory());

                // Poison escalation: the next producer call surfaces a typed
                // PROTOCOL_VIOLATION terminal naming the poisoned frame.
                LineSenderException thrown = null;
                try {
                    sender.table("foo").longColumn("v", 2L).atNow();
                    sender.flush();
                } catch (LineSenderException e) {
                    thrown = e;
                }
                Assert.assertNotNull(
                        "flush() after poison escalation must throw", thrown);
                Assert.assertTrue(
                        "terminal must name the poisoned frame; got: " + thrown.getMessage(),
                        thrown.getMessage() != null && thrown.getMessage().contains("poisoned frame"));
            } finally {
                // close() rethrows an unsurfaced latched terminal; the test
                // has already observed it via flush() above, but swallow
                // defensively in case the surfacing raced.
                try {
                    sender.close();
                } catch (LineSenderException ignored) {
                }
            }
        }
    }

    /**
     * The poison threshold is configurable end-to-end: with
     * {@code max_frame_rejections=2} in the connect string, a server that
     * NACKs every frame trips the poisoned-frame terminal after exactly TWO
     * deliveries of the same frame — not the default four. Pins the full
     * plumbing: connect-string parse → builder → QwpWebSocketSender →
     * CursorWebSocketSendLoop.
     */
    @Test
    public void testMaxFrameRejectionsConfigurableFromConnectString() throws Exception {
        WriteErrorAckHandler handler = new WriteErrorAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port
                    + ";reconnect_max_duration_millis=10000"
                    + ";reconnect_initial_backoff_millis=10"
                    + ";reconnect_max_backoff_millis=50"
                    + ";max_frame_rejections=2"
                    + ";poison_min_escalation_window_millis=0"
                    + ";";

            Sender sender = Sender.fromConfig(cfg);
            try {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();

                waitFor(() -> handler.totalBinaryReceived.get() >= 2, 10_000);
                QwpWebSocketSender wss = (QwpWebSocketSender) sender;
                waitFor(() -> wss.getLastTerminalError() != null, 5_000);

                Assert.assertEquals(
                        "with max_frame_rejections=2 the frame must be delivered exactly"
                                + " twice before poison escalation",
                        2, handler.totalBinaryReceived.get());
            } finally {
                try {
                    sender.close();
                } catch (LineSenderException ignored) {
                }
            }
        }
    }

    /**
     * Client half of the server stop-at-gap contract: the server acks the head,
     * RETRIABLE-NACKs the next frame, then goes silent for the frames the client
     * already pipelined behind it (never committed, never acked past the gap).
     * The client must recycle, replay its unacked tail from ackedFsn+1, and
     * deliver every row -- no hang on the unanswered tail, no data loss, no
     * poison escalation.
     */
    @Test
    public void testRecoversFromRetriableNackWithSilentTail() throws Exception {
        StopAtGapHandler handler = new StopAtGapHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            int port = server.getPort();
            String cfg = "ws::addr=localhost:" + port
                    + ";reconnect_max_duration_millis=10000"
                    + ";reconnect_initial_backoff_millis=10"
                    + ";reconnect_max_backoff_millis=50"
                    + ";close_flush_timeout_millis=5000"
                    + ";";

            Sender sender = Sender.fromConfig(cfg);
            try {
                // Pipeline four frames so the head NACK can leave a tail behind.
                for (int i = 0; i < 4; i++) {
                    sender.table("foo").longColumn("v", i).atNow();
                    sender.flush();
                }

                // The RETRIABLE NACK must recycle and replay the unacked tail on
                // a fresh connection.
                waitFor(() -> handler.connections.size() >= 2, 10_000);

                QwpWebSocketSender wss = (QwpWebSocketSender) sender;
                Assert.assertTrue("a RETRIABLE NACK must recycle the connection", wss.getTotalReconnectAttempts() >= 1);

                // The replayed tail is OK-acked on the new connection, so nothing
                // escalates to a terminal.
                Thread.sleep(300);
                Assert.assertNull("retriable NACK + silent tail must recover, not latch a terminal", wss.getLastTerminalError());

                // End to end: a further row flushes and drain-on-close completes
                // without throwing -- no stall, no loss.
                sender.table("foo").longColumn("v", 4).atNow();
                sender.flush();
                sender.close();
            } catch (LineSenderException e) {
                try {
                    sender.close();
                } catch (LineSenderException ignored) {
                }
                throw new AssertionError("sender must recover from the retriable NACK, but flush/close threw", e);
            }
        }
    }

    /**
     * First connection: ack the head, RETRIABLE-NACK the next frame, then stay
     * silent for the pipelined tail. Any later connection is the reconnect --
     * ack every replayed frame so the tail lands.
     */
    private static class StopAtGapHandler implements TestWebSocketServer.WebSocketServerHandler {
        final Set<TestWebSocketServer.ClientHandler> connections = ConcurrentHashMap.newKeySet();
        final AtomicLong tailSilenced = new AtomicLong();
        private TestWebSocketServer.ClientHandler firstClient;
        private int firstConnFrameIdx;
        private long okAckSeq;

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            connections.add(client);
            if (firstClient == null) {
                firstClient = client;
            }
            try {
                if (client == firstClient) {
                    int idx = firstConnFrameIdx++;
                    if (idx == 0) {
                        client.sendBinary(buildAck(okAckSeq++));
                    } else if (idx == 1) {
                        client.sendBinary(buildErrorAck(1, WebSocketResponse.STATUS_WRITE_ERROR, "test: retriable"));
                    } else {
                        tailSilenced.incrementAndGet();
                    }
                } else {
                    client.sendBinary(buildAck(okAckSeq++));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /** Server returns {@code STATUS_WRITE_ERROR} (RETRIABLE policy) for every received frame. */
    private static class WriteErrorAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicLong totalBinaryReceived = new AtomicLong();
        private final AtomicLong nextSeq = new AtomicLong();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            totalBinaryReceived.incrementAndGet();
            try {
                client.sendBinary(buildErrorAck(nextSeq.getAndIncrement(),
                        WebSocketResponse.STATUS_WRITE_ERROR,
                        "test: write error"));
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

    // Mirrors WebSocketResponse STATUS_OK layout: status u8 | sequence u64 | table_count u16
    private static byte[] buildAck(long seq) {
        byte[] buf = new byte[1 + 8 + 2];
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        bb.put(WebSocketResponse.STATUS_OK);
        bb.putLong(seq);
        bb.putShort((short) 0);
        return buf;
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
