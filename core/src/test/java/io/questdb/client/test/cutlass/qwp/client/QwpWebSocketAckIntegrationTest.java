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

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.client.std.Os;
import io.questdb.client.test.AbstractTest;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration tests for QWP v1 WebSocket ACK delivery mechanism.
 * These tests verify that the InFlightWindow and ACK responses work correctly end-to-end.
 */
public class QwpWebSocketAckIntegrationTest extends AbstractTest {

    private static final int TEST_PORT = 19_500 + (int) (System.nanoTime() % 100);

    @Test
    public void testAsyncFlushFailsFastOnInvalidAckPayload() throws Exception {
        InvalidAckPayloadHandler handler = new InvalidAckPayloadHandler();
        int port = TEST_PORT + 21;

        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            boolean errorCaught = false;
            long start = System.currentTimeMillis();
            try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                    "localhost", port, null, 0, 0, 0, QwpWebSocketSender.DEFAULT_IN_FLIGHT_WINDOW_SIZE, null)) {
                sender.table("test")
                        .longColumn("value", 1)
                        .atNow();
                sender.flush();
            } catch (Exception e) {
                errorCaught = true;
                Assert.assertTrue(
                        e.getMessage().contains("Invalid ACK response payload")
                                || e.getMessage().contains("Error in send queue")
                );
            }

            long duration = System.currentTimeMillis() - start;
            Assert.assertTrue("Expected invalid ACK error", errorCaught);
            Assert.assertTrue("Flush should fail quickly on invalid ACK [duration=" + duration + "ms]", duration < 10_000);
        }
    }

    @Test
    public void testAsyncFlushFailsFastOnServerClose() throws Exception {
        ClosingServerHandler handler = new ClosingServerHandler();
        int port = TEST_PORT + 20;

        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            boolean errorCaught = false;
            long start = System.currentTimeMillis();
            try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                    "localhost", port, null, 0, 0, 0, QwpWebSocketSender.DEFAULT_IN_FLIGHT_WINDOW_SIZE, null)) {
                sender.table("test")
                        .longColumn("value", 1)
                        .atNow();
                sender.flush();
            } catch (Exception e) {
                errorCaught = true;
                Assert.assertTrue(
                        e.getMessage().contains("closed")
                                || e.getMessage().contains("Error in send queue")
                                || e.getMessage().contains("failed")
                );
            }

            long duration = System.currentTimeMillis() - start;
            Assert.assertTrue("Expected async close error", errorCaught);
            Assert.assertTrue("Flush should fail quickly on close [duration=" + duration + "ms]", duration < 10_000);
        }
    }

    /**
     * Test that flush blocks until ACK is received.
     * Uses async mode to enable ACK handling via InFlightWindow.
     */
    @Test
    public void testFlushBlocksUntilAcked() throws Exception {
        final long DELAY_MS = 300; // 300ms delay before ACK
        DelayedAckHandler handler = new DelayedAckHandler(DELAY_MS);

        int port = TEST_PORT + 10;
        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                    "localhost", port, null, 0, 0, 0, QwpWebSocketSender.DEFAULT_IN_FLIGHT_WINDOW_SIZE, null)) {

                sender.table("test")
                        .longColumn("value", 42)
                        .atNow();

                long startTime = System.currentTimeMillis();
                sender.flush();
                long duration = System.currentTimeMillis() - startTime;

                Assert.assertTrue("Flush should have waited for ACK (took " + duration + "ms, expected >= " + (DELAY_MS / 2) + "ms)",
                        duration >= DELAY_MS / 2);

                LOG.info("Flush waited {}ms for ACK", duration);
            }
        }
    }

    @Test
    public void testSyncFlushFailsOnInvalidAckPayload() throws Exception {
        InvalidAckPayloadHandler handler = new InvalidAckPayloadHandler();
        int port = TEST_PORT + 22;

        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            boolean errorCaught = false;
            long start = System.currentTimeMillis();
            try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", port, null)) {
                sender.table("test")
                        .longColumn("value", 7)
                        .atNow();
                sender.flush();
            } catch (Exception e) {
                errorCaught = true;
                Assert.assertTrue(
                        e.getMessage().contains("Invalid ACK response payload")
                                || e.getMessage().contains("Failed to parse ACK response")
                );
            }

            long duration = System.currentTimeMillis() - start;
            Assert.assertTrue("Expected invalid ACK error in sync mode", errorCaught);
            Assert.assertTrue("Sync invalid ACK path should fail quickly [duration=" + duration + "ms]", duration < 10_000);
        }
    }

    @Test
    public void testSyncFlushIgnoresPingAndWaitsForAck() throws Exception {
        final long ackDelayMs = 300;
        PingThenDelayedAckHandler handler = new PingThenDelayedAckHandler(ackDelayMs);
        int port = TEST_PORT + 23;

        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", port, null)) {
                sender.table("test")
                        .longColumn("value", 11)
                        .atNow();

                long start = System.currentTimeMillis();
                sender.flush();
                long duration = System.currentTimeMillis() - start;

                Assert.assertTrue("Flush returned too early [duration=" + duration + "ms]", duration >= ackDelayMs / 2);
            }
        }
    }

    @Test
    public void testDurableAckUpgradeHeaderNotSentByDefault() throws Exception {
        int port = TEST_PORT + 31;
        AtomicReference<String> capturedRequest = new AtomicReference<>();

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setSoTimeout(5000);

            Thread serverThread = new Thread(() -> {
                try {
                    Socket client = serverSocket.accept();
                    InputStream in = client.getInputStream();
                    StringBuilder request = new StringBuilder();
                    byte[] buf = new byte[1];
                    while (true) {
                        int read = in.read(buf);
                        if (read <= 0) {
                            break;
                        }
                        request.append((char) buf[0]);
                        if (request.toString().endsWith("\r\n\r\n")) {
                            break;
                        }
                    }
                    capturedRequest.set(request.toString());
                    client.close();
                } catch (Exception e) {
                    // expected
                }
            });
            serverThread.start();

            try {
                QwpWebSocketSender.connect("localhost", port, null,
                        0, 0, 0, 1, null).close();
            } catch (LineSenderException e) {
                // expected - server doesn't complete handshake
            }

            serverThread.join(5000);

            String request = capturedRequest.get();
            Assert.assertNotNull("Server should have received upgrade request", request);
            Assert.assertFalse("Request should NOT contain X-QWP-Request-Durable-Ack header",
                    request.contains("X-QWP-Request-Durable-Ack"));
        }
    }

    @Test
    public void testDurableAckUpgradeHeaderSent() throws Exception {
        int port = TEST_PORT + 30;
        AtomicReference<String> capturedRequest = new AtomicReference<>();

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setSoTimeout(5000);

            Thread serverThread = new Thread(() -> {
                try {
                    Socket client = serverSocket.accept();
                    InputStream in = client.getInputStream();
                    StringBuilder request = new StringBuilder();
                    byte[] buf = new byte[1];
                    while (true) {
                        int read = in.read(buf);
                        if (read <= 0) {
                            break;
                        }
                        request.append((char) buf[0]);
                        if (request.toString().endsWith("\r\n\r\n")) {
                            break;
                        }
                    }
                    capturedRequest.set(request.toString());
                    client.close();
                } catch (Exception e) {
                    // expected
                }
            });
            serverThread.start();

            try {
                QwpWebSocketSender.connect("localhost", port, null,
                        0, 0, 0, 1, null,
                        QwpWebSocketSender.DEFAULT_MAX_SCHEMAS_PER_CONNECTION,
                        true).close();
            } catch (LineSenderException e) {
                // expected - server doesn't complete handshake
            }

            serverThread.join(5000);

            String request = capturedRequest.get();
            Assert.assertNotNull("Server should have received upgrade request", request);
            Assert.assertTrue("Request should contain X-QWP-Request-Durable-Ack header",
                    request.contains("X-QWP-Request-Durable-Ack: true"));
        }
    }

    @Test
    public void testSyncDurableAckDuringWaitForAck() throws Exception {
        int port = TEST_PORT + 25;
        DurableAckThenStatusOkHandler handler = new DurableAckThenStatusOkHandler();

        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            // window=1 for sync mode
            try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                    "localhost", port, null, 0, 0, 0, 1, null)) {
                sender.table("trades")
                        .longColumn("price", 100)
                        .atNow();
                sender.flush();

                Assert.assertEquals(42L, sender.getHighestDurableSeqTxn("trades"));
                Assert.assertEquals(10L, sender.getHighestAckedSeqTxn("trades"));
            }
        }
    }

    @Test
    public void testSyncFlushUpdatesCommittedSeqTxnsWithTableEntries() throws Exception {
        int port = TEST_PORT + 24;
        AckWithTableEntriesHandler handler = new AckWithTableEntriesHandler();

        try (TestWebSocketServer server = new TestWebSocketServer(port, handler)) {
            server.start();
            Assert.assertTrue("Server failed to start", server.awaitStart(5, TimeUnit.SECONDS));

            // window=1 for sync mode
            try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                    "localhost", port, null, 0, 0, 0, 1, null)) {
                sender.table("trades")
                        .longColumn("price", 100)
                        .atNow();
                sender.flush();

                Assert.assertEquals(10L, sender.getHighestAckedSeqTxn("trades"));
                Assert.assertEquals(20L, sender.getHighestAckedSeqTxn("orders"));
                Assert.assertEquals(-1L, sender.getHighestAckedSeqTxn("other"));
            }
        }
    }

    /**
     * Creates a binary ACK response using WebSocketResponse format.
     * Format: status (1) + sequence (8) + tableCount (2, zero entries)
     */
    private static byte[] createAckResponse(long sequence) {
        byte[] response = new byte[WebSocketResponse.MIN_OK_RESPONSE_SIZE];

        response[0] = WebSocketResponse.STATUS_OK;

        response[1] = (byte) (sequence & 0xFF);
        response[2] = (byte) ((sequence >> 8) & 0xFF);
        response[3] = (byte) ((sequence >> 16) & 0xFF);
        response[4] = (byte) ((sequence >> 24) & 0xFF);
        response[5] = (byte) ((sequence >> 32) & 0xFF);
        response[6] = (byte) ((sequence >> 40) & 0xFF);
        response[7] = (byte) ((sequence >> 48) & 0xFF);
        response[8] = (byte) ((sequence >> 56) & 0xFF);

        // tableCount = 0
        response[9] = 0;
        response[10] = 0;

        return response;
    }

    private static byte[] createAckResponseWithTables(long sequence, String[] tableNames, long[] seqTxns) {
        byte[][] nameBytes = new byte[tableNames.length][];
        int size = 1 + 8 + 2;
        for (int i = 0; i < tableNames.length; i++) {
            nameBytes[i] = tableNames[i].getBytes(StandardCharsets.UTF_8);
            size += 2 + nameBytes[i].length + 8;
        }

        byte[] response = new byte[size];
        int offset = 0;
        response[offset++] = WebSocketResponse.STATUS_OK;
        for (int i = 0; i < 8; i++) {
            response[offset++] = (byte) ((sequence >> (i * 8)) & 0xFF);
        }
        response[offset++] = (byte) (tableNames.length & 0xFF);
        response[offset++] = (byte) ((tableNames.length >> 8) & 0xFF);
        for (int i = 0; i < tableNames.length; i++) {
            response[offset++] = (byte) (nameBytes[i].length & 0xFF);
            response[offset++] = (byte) ((nameBytes[i].length >> 8) & 0xFF);
            System.arraycopy(nameBytes[i], 0, response, offset, nameBytes[i].length);
            offset += nameBytes[i].length;
            for (int j = 0; j < 8; j++) {
                response[offset++] = (byte) ((seqTxns[i] >> (j * 8)) & 0xFF);
            }
        }
        return response;
    }

    private static byte[] createDurableAckResponse(String[] tableNames, long[] seqTxns) {
        byte[][] nameBytes = new byte[tableNames.length][];
        int size = 1 + 2;
        for (int i = 0; i < tableNames.length; i++) {
            nameBytes[i] = tableNames[i].getBytes(StandardCharsets.UTF_8);
            size += 2 + nameBytes[i].length + 8;
        }

        byte[] response = new byte[size];
        int offset = 0;
        response[offset++] = WebSocketResponse.STATUS_DURABLE_ACK;
        response[offset++] = (byte) (tableNames.length & 0xFF);
        response[offset++] = (byte) ((tableNames.length >> 8) & 0xFF);
        for (int i = 0; i < tableNames.length; i++) {
            response[offset++] = (byte) (nameBytes[i].length & 0xFF);
            response[offset++] = (byte) ((nameBytes[i].length >> 8) & 0xFF);
            System.arraycopy(nameBytes[i], 0, response, offset, nameBytes[i].length);
            offset += nameBytes[i].length;
            for (int j = 0; j < 8; j++) {
                response[offset++] = (byte) ((seqTxns[i] >> (j * 8)) & 0xFF);
            }
        }
        return response;
    }

    private static class AckWithTableEntriesHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSequence = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long sequence = nextSequence.getAndIncrement();
            try {
                client.sendBinary(createAckResponseWithTables(sequence,
                        new String[]{"trades", "orders"},
                        new long[]{10L, 20L}));
            } catch (IOException e) {
                LOG.error("Failed to send ACK with tables", e);
            }
        }
    }

    private static class ClosingServerHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendClose(WebSocketCloseCode.GOING_AWAY, "bye");
            } catch (IOException e) {
                LOG.error("Failed to send close frame", e);
            }
        }
    }

    /**
     * Server handler that delays ACKs to test blocking behavior.
     */
    private static class DelayedAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final long delayMs;
        private final AtomicLong nextSequence = new AtomicLong(0);

        DelayedAckHandler(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long sequence = nextSequence.getAndIncrement();

            LOG.debug("Server delaying ACK by {}ms", delayMs);

            new Thread(() -> {
                try {
                    Os.sleep(delayMs);
                    byte[] ackResponse = createAckResponse(sequence);
                    client.sendBinary(ackResponse);
                    LOG.debug("Server sent delayed ACK for seq {}", sequence);
                } catch (Exception e) {
                    LOG.error("Failed to send delayed ACK", e);
                }
            }).start();
        }
    }

    private static class DurableAckThenStatusOkHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSequence = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long sequence = nextSequence.getAndIncrement();
            try {
                // Send durable ACK first
                client.sendBinary(createDurableAckResponse(
                        new String[]{"trades"},
                        new long[]{42L}));
                // Then send STATUS_OK with committed seqTxns
                client.sendBinary(createAckResponseWithTables(sequence,
                        new String[]{"trades"},
                        new long[]{10L}));
            } catch (IOException e) {
                LOG.error("Failed to send ACK frames", e);
            }
        }
    }

    private static class InvalidAckPayloadHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(new byte[]{1, 2, 3});
            } catch (IOException e) {
                LOG.error("Failed to send invalid payload", e);
            }
        }
    }

    private static class PingThenDelayedAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final long delayMs;
        private final AtomicLong nextSequence = new AtomicLong(0);

        private PingThenDelayedAckHandler(long delayMs) {
            this.delayMs = delayMs;
        }

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long sequence = nextSequence.getAndIncrement();
            try {
                client.sendPing(new byte[]{42});
            } catch (IOException e) {
                LOG.error("Failed to send ping", e);
            }

            new Thread(() -> {
                try {
                    Os.sleep(delayMs);
                    client.sendBinary(createAckResponse(sequence));
                } catch (Exception e) {
                    LOG.error("Failed to send delayed ACK", e);
                }
            }).start();
        }
    }
}
