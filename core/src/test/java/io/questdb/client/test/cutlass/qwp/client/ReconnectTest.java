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
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests for the reconnect machinery in {@link io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop}.
 * <p>
 * The cursor I/O loop used to treat any wire failure as terminal — first
 * disconnect = sender broken, every subsequent batch threw. Reconnect
 * machinery now handles transient drops: detect, build a fresh client
 * via the registered factory, reset wire state, reposition the replay
 * cursor at {@code engine.ackedFsn() + 1}, and notify the producer thread
 * (via {@code connectionGeneration} bump) so the next encode emits full
 * schema definitions.
 * <p>
 * This commit covers the mechanics with a single-attempt retry; backoff,
 * per-outage time cap, and auth-failure detection follow.
 */
public class ReconnectTest {

    private static final int TEST_PORT = 19_900 + (int) (System.nanoTime() % 100);

    @Test
    public void testReconnectAfterServerInducedDisconnect() throws Exception {
        // Server ACKs the first batch then closes the client connection.
        // Without reconnect, the next batch's flush() would throw. With
        // reconnect, the I/O loop opens a fresh connection (same port,
        // same server) and the second batch goes through.
        int port = TEST_PORT + 1;
        DisconnectAfterFirstAckHandler handler = new DisconnectAfterFirstAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String cfg = "ws::addr=localhost:" + port + ";";
            try (Sender sender = Sender.fromConfig(cfg)) {
                // Batch 1: server receives, ACKs, then closes the socket.
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                waitFor(() -> handler.totalBinaryReceived.get() >= 1, 5_000);

                // Brief pause so the I/O loop has time to see the EOF and
                // run through its reconnect path before we try to flush again.
                Thread.sleep(200);

                // Batch 2 must land on the new connection (server-side
                // counter advances) — proves the reconnect+resume worked
                // end-to-end. Producer's flush() must not throw.
                sender.table("foo").longColumn("v", 2L).atNow();
                sender.flush();
                waitFor(() -> handler.totalBinaryReceived.get() >= 2, 5_000);

                Assert.assertTrue(
                        "server must observe two distinct client connections "
                                + "(close-after-first-ACK forced reconnect): saw "
                                + handler.connectionsAccepted.get(),
                        handler.connectionsAccepted.get() >= 2);
            }
        }
    }

    @Test
    public void testReplayResendsUnackedFramesAcrossReconnect() throws Exception {
        // First batch is received but the server closes the socket BEFORE
        // sending its ACK. The sender's engine has the frame at FSN 0 but
        // ackedFsn is still -1. On reconnect, the cursor must reposition at
        // FSN 0 and replay it — the new connection should observe the
        // *same* batch a second time before any new batch arrives.
        int port = TEST_PORT + 2;
        ReceiveThenDisconnectHandler handler = new ReceiveThenDisconnectHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String cfg = "ws::addr=localhost:" + port + ";";
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 99L).atNow();
                sender.flush();
                // First connection received the batch and dropped without
                // ACKing → the I/O loop reconnects and replays. Wait for
                // the second connection to receive the (replayed) frame.
                waitFor(() -> handler.totalBinaryReceived.get() >= 2, 5_000);
                Assert.assertTrue(
                        "expected at least 2 binary frames across the two "
                                + "connections (replay): saw "
                                + handler.totalBinaryReceived.get(),
                        handler.totalBinaryReceived.get() >= 2);
                Assert.assertTrue(
                        "expected ≥ 2 distinct connections (reconnect): saw "
                                + handler.connectionsAccepted.get(),
                        handler.connectionsAccepted.get() >= 2);
            }
        }
    }

    /**
     * Polls a condition with a short sleep until it's true or the timeout
     * elapses. Throws {@link AssertionError} on timeout.
     */
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

    /**
     * Single-server handler shared across all client connections it serves.
     * On every binary frame: ACK; if this is the first connection's first
     * frame, close the connection right after sending the ACK so the
     * sender's I/O loop has to reconnect to deliver the second batch.
     */
    private static class DisconnectAfterFirstAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        final AtomicLong totalBinaryReceived = new AtomicLong();
        private final AtomicLong nextSeq = new AtomicLong(0);
        private TestWebSocketServer.ClientHandler firstClient;

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // First frame from a new client — record the connection.
            if (firstClient == null || firstClient != client) {
                connectionsAccepted.incrementAndGet();
                if (firstClient == null) {
                    firstClient = client;
                }
            }
            totalBinaryReceived.incrementAndGet();
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
                if (totalBinaryReceived.get() == 1) {
                    // Tear down this connection — sender must reconnect.
                    // Brief sleep so the ACK we just queued has time to flush
                    // before the socket is closed under it.
                    Thread.sleep(50);
                    client.close();
                }
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Receives the first frame on the first connection without ACKing,
     * then closes — forcing the sender's I/O loop to reconnect and replay
     * that unacked frame on the new connection. The new connection then
     * ACKs normally, so the test can observe the replay landing.
     */
    private static class ReceiveThenDisconnectHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicInteger connectionsAccepted = new AtomicInteger();
        final AtomicLong totalBinaryReceived = new AtomicLong();
        private final AtomicLong nextSeq = new AtomicLong(0);
        private TestWebSocketServer.ClientHandler firstClient;
        private boolean firstFrameDropped;

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (firstClient == null || firstClient != client) {
                connectionsAccepted.incrementAndGet();
                if (firstClient == null) {
                    firstClient = client;
                }
            }
            totalBinaryReceived.incrementAndGet();
            // First frame on the first connection: drop without ACKing,
            // then close so the sender has to reconnect + replay.
            if (!firstFrameDropped && client == firstClient) {
                firstFrameDropped = true;
                try {
                    Thread.sleep(20);
                    client.close();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return;
            }
            // Any later frame (including the replayed one): ACK normally.
            try {
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // Mirrors WebSocketResponse STATUS_OK layout: status u8 | sequence u64 | table_count u16
    static byte[] buildAck(long seq) {
        byte[] buf = new byte[1 + 8 + 2];
        ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 0x00); // STATUS_OK
        bb.putLong(seq);
        bb.putShort((short) 0);
        return buf;
    }
}
