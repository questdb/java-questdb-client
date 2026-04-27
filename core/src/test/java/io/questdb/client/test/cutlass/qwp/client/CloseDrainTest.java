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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Regression tests for the close() drain semantics specified in
 * design/qwp-cursor-durability.md.
 * <p>
 * Without {@code close_flush_timeout_millis}, close() returned as soon as
 * the cursor I/O loop's {@code running} flag flipped — meaning frames
 * still queued in the engine could be dropped when the JVM exited
 * immediately after close(). The drain timeout makes close() wait for
 * the server to ACK everything published before shutting the loop down.
 */
public class CloseDrainTest {

    private static final int TEST_PORT = 19_700 + (int) (System.nanoTime() % 100);

    @Test
    public void testCloseBlocksUntilAckArrives() throws Exception {
        // Server delays every ACK by 800ms. With the default
        // close_flush_timeout_millis=5000, close() must wait for that ACK
        // before returning. Pre-fix close() returned within milliseconds.
        int port = TEST_PORT + 1;
        long ackDelayMs = 800;
        DelayingAckHandler handler = new DelayingAckHandler(ackDelayMs);
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String cfg = "ws::addr=localhost:" + port + ";";  // memory mode
            long elapsedMs;
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                long t0 = System.nanoTime();
                sender.close();
                elapsedMs = (System.nanoTime() - t0) / 1_000_000;
            }
            Assert.assertTrue(
                    "close() took only " + elapsedMs + "ms — did not wait for ACK; "
                            + "drain timeout is broken or never enabled",
                    elapsedMs >= ackDelayMs / 2);
        }
    }

    @Test
    public void testCloseFastWhenTimeoutIsZero() throws Exception {
        // Same delayed-ACK server, but with close_flush_timeout_millis=0
        // (fast close). close() must return immediately, well before the
        // ACK delay would have elapsed.
        int port = TEST_PORT + 2;
        long ackDelayMs = 1500;
        DelayingAckHandler handler = new DelayingAckHandler(ackDelayMs);
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String cfg = "ws::addr=localhost:" + port
                    + ";close_flush_timeout_millis=0;";
            long elapsedMs;
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                long t0 = System.nanoTime();
                sender.close();
                elapsedMs = (System.nanoTime() - t0) / 1_000_000;
            }
            Assert.assertTrue(
                    "close() with timeout=0 took " + elapsedMs + "ms — fast close is broken",
                    elapsedMs < ackDelayMs / 2);
        }
    }

    @Test
    public void testCloseDrainTimesOutWhenAcksNeverArrive() throws Exception {
        // Server that buffers frames silently and never ACKs. close() must
        // return after roughly the configured timeout — not hang forever
        // and not return immediately.
        int port = TEST_PORT + 3;
        long timeoutMs = 500;
        SilentHandler handler = new SilentHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            String cfg = "ws::addr=localhost:" + port
                    + ";close_flush_timeout_millis=" + timeoutMs + ";";
            long elapsedMs;
            try (Sender sender = Sender.fromConfig(cfg)) {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();
                long t0 = System.nanoTime();
                sender.close();
                elapsedMs = (System.nanoTime() - t0) / 1_000_000;
            }
            Assert.assertTrue("close() returned too early: " + elapsedMs + "ms",
                    elapsedMs >= timeoutMs);
            Assert.assertTrue("close() exceeded the bounded timeout by too much: " + elapsedMs + "ms",
                    elapsedMs < timeoutMs * 4);
        }
    }

    /** Acks every binary frame after a fixed delay, so we can observe close() blocking. */
    private static class DelayingAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final long delayMs;
        private final AtomicLong nextSeq = new AtomicLong(0);

        DelayingAckHandler(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                Thread.sleep(delayMs);
                client.sendBinary(buildAck(nextSeq.getAndIncrement()));
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    /** Receives but never ACKs — used to verify close() honors its timeout cap. */
    private static class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // intentionally drop the frame on the floor
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
