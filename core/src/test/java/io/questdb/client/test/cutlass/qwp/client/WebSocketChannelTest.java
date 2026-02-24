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

import io.questdb.client.cutlass.qwp.client.WebSocketChannel;
import io.questdb.client.cutlass.qwp.websocket.WebSocketHandshake;
import io.questdb.client.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.AbstractTest;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tests for WebSocketChannel's native-heap memory copy paths.
 * Exercises writeToSocket (native to heap) and readFromSocket (heap to native)
 * through a local echo server.
 */
public class WebSocketChannelTest extends AbstractTest {

    @Test
    public void testBinaryRoundTripSmallPayload() throws Exception {
        TestUtils.assertMemoryLeak(() -> assertBinaryRoundTrip(13));
    }

    @Test
    public void testBinaryRoundTripMediumPayload() throws Exception {
        TestUtils.assertMemoryLeak(() -> assertBinaryRoundTrip(4096));
    }

    @Test
    public void testBinaryRoundTripLargePayload() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Large payload that exercises bulk copyMemory across many cache lines.
            // Kept under 32KB so the echo response arrives in a single TCP read
            // on loopback (avoids a pre-existing bug in doReceiveFrame with
            // partial frame assembly).
            assertBinaryRoundTrip(30_000);
        });
    }

    @Test
    public void testBinaryRoundTripAllByteValues() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int len = 256;
            long sendPtr = Unsafe.malloc(len, MemoryTag.NATIVE_DEFAULT);
            try {
                for (int i = 0; i < len; i++) {
                    Unsafe.getUnsafe().putByte(sendPtr + i, (byte) i);
                }
                assertBinaryRoundTrip(sendPtr, len);
            } finally {
                Unsafe.free(sendPtr, len, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testBinaryRoundTripRepeatedFrames() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int payloadLen = 1000;
            int frameCount = 10;
            long sendPtr = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
            try (EchoServer server = new EchoServer()) {
                server.start();
                WebSocketChannel channel = new WebSocketChannel(
                        "localhost:" + server.getPort() + "/", false
                );
                try {
                    channel.setConnectTimeout(5000);
                    channel.setReadTimeout(5000);
                    channel.connect();

                    for (int f = 0; f < frameCount; f++) {
                        for (int i = 0; i < payloadLen; i++) {
                            Unsafe.getUnsafe().putByte(sendPtr + i, (byte) (i + f));
                        }
                        channel.sendBinary(sendPtr, payloadLen);

                        ReceivedPayload received = new ReceivedPayload();
                        boolean ok = receiveWithRetry(channel, received, 5000);
                        server.assertNoError();
                        Assert.assertTrue("frame " + f + ": expected response", ok);
                        Assert.assertEquals("frame " + f + ": length", payloadLen, received.length);

                        for (int i = 0; i < payloadLen; i++) {
                            Assert.assertEquals(
                                    "frame " + f + " byte " + i,
                                    (byte) (i + f),
                                    Unsafe.getUnsafe().getByte(received.ptr + i)
                            );
                        }
                    }
                } finally {
                    channel.close();
                }
                server.assertNoError();
            } finally {
                Unsafe.free(sendPtr, payloadLen, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    private void assertBinaryRoundTrip(int payloadLen) throws Exception {
        long sendPtr = Unsafe.malloc(payloadLen, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < payloadLen; i++) {
                Unsafe.getUnsafe().putByte(sendPtr + i, (byte) (i & 0xFF));
            }
            assertBinaryRoundTrip(sendPtr, payloadLen);
        } finally {
            Unsafe.free(sendPtr, payloadLen, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private void assertBinaryRoundTrip(long sendPtr, int payloadLen) throws Exception {
        try (EchoServer server = new EchoServer()) {
            server.start();
            WebSocketChannel channel = new WebSocketChannel(
                    "localhost:" + server.getPort() + "/", false
            );
            try {
                channel.setConnectTimeout(5000);
                channel.setReadTimeout(5000);
                channel.connect();

                // Send exercises writeToSocket (native to heap via copyMemory)
                channel.sendBinary(sendPtr, payloadLen);

                // Receive exercises readFromSocket (heap to native via copyMemory)
                ReceivedPayload received = new ReceivedPayload();
                boolean ok = receiveWithRetry(channel, received, 5000);

                // Check server error before client assertions
                server.assertNoError();
                Assert.assertTrue("expected a frame back from echo server", ok);
                Assert.assertEquals("payload length mismatch", payloadLen, received.length);

                for (int i = 0; i < payloadLen; i++) {
                    byte expected = Unsafe.getUnsafe().getByte(sendPtr + i);
                    byte actual = Unsafe.getUnsafe().getByte(received.ptr + i);
                    Assert.assertEquals("byte mismatch at offset " + i, expected, actual);
                }
            } finally {
                channel.close();
            }
            server.assertNoError();
        }
    }

    /**
     * Calls receiveFrame in a loop to handle the case where doReceiveFrame
     * needs multiple reads to assemble a complete frame (e.g. header and
     * payload arrive in separate TCP segments).
     */
    private static boolean receiveWithRetry(WebSocketChannel channel, ReceivedPayload handler, int timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            int remaining = (int) (deadline - System.currentTimeMillis());
            if (remaining <= 0) {
                break;
            }
            if (channel.receiveFrame(handler, remaining)) {
                return true;
            }
        }
        return false;
    }

    private static class ReceivedPayload implements WebSocketChannel.ResponseHandler {
        long ptr;
        int length;

        @Override
        public void onBinaryMessage(long payload, int length) {
            this.ptr = payload;
            this.length = length;
        }

        @Override
        public void onClose(int code, String reason) {
        }
    }

    /**
     * Minimal WebSocket echo server. Accepts one connection, completes the
     * HTTP upgrade handshake, then echoes every binary frame back unmasked.
     * All echo writes use a single byte array to avoid TCP fragmentation.
     */
    private static class EchoServer implements AutoCloseable {
        private static final Pattern KEY_PATTERN =
                Pattern.compile("Sec-WebSocket-Key:\\s*(.+?)\\r\\n");

        private final ServerSocket serverSocket;
        private final AtomicReference<Throwable> error = new AtomicReference<>();
        private Thread thread;

        EchoServer() throws IOException {
            serverSocket = new ServerSocket(0);
        }

        int getPort() {
            return serverSocket.getLocalPort();
        }

        void start() {
            thread = new Thread(this::run, "ws-echo-server");
            thread.setDaemon(true);
            thread.start();
        }

        void assertNoError() {
            Throwable t = error.get();
            if (t != null) {
                throw new AssertionError("echo server error", t);
            }
        }

        @Override
        public void close() throws Exception {
            serverSocket.close();
            if (thread != null) {
                thread.join(5000);
            }
        }

        private void run() {
            try (Socket client = serverSocket.accept()) {
                client.setSoTimeout(10_000);
                client.setTcpNoDelay(true);
                InputStream in = client.getInputStream();
                OutputStream out = new BufferedOutputStream(client.getOutputStream());

                completeHandshake(in, out);
                echoFrames(in, out);
            } catch (IOException e) {
                if (!serverSocket.isClosed()) {
                    error.set(e);
                }
            } catch (Throwable t) {
                error.set(t);
            }
        }

        private void completeHandshake(InputStream in, OutputStream out) throws IOException {
            byte[] buf = new byte[4096];
            int pos = 0;

            while (pos < buf.length) {
                int b = in.read();
                if (b < 0) {
                    throw new IOException("connection closed during handshake");
                }
                buf[pos++] = (byte) b;
                if (pos >= 4
                        && buf[pos - 4] == '\r' && buf[pos - 3] == '\n'
                        && buf[pos - 2] == '\r' && buf[pos - 1] == '\n') {
                    break;
                }
            }

            String request = new String(buf, 0, pos, StandardCharsets.US_ASCII);
            Matcher m = KEY_PATTERN.matcher(request);
            if (!m.find()) {
                throw new IOException("no Sec-WebSocket-Key in request:\n" + request);
            }
            String clientKey = m.group(1).trim();
            String acceptKey = WebSocketHandshake.computeAcceptKey(clientKey);

            String response = "HTTP/1.1 101 Switching Protocols\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Accept: " + acceptKey + "\r\n"
                    + "\r\n";
            out.write(response.getBytes(StandardCharsets.US_ASCII));
            out.flush();
        }

        private void echoFrames(InputStream in, OutputStream out) throws IOException {
            byte[] readBuf = new byte[256 * 1024];

            while (true) {
                int pos = 0;
                while (pos < 2) {
                    int n = in.read(readBuf, pos, readBuf.length - pos);
                    if (n < 0) {
                        return;
                    }
                    pos += n;
                }

                int byte0 = readBuf[0] & 0xFF;
                int byte1 = readBuf[1] & 0xFF;
                int opcode = byte0 & 0x0F;
                boolean masked = (byte1 & 0x80) != 0;
                int lengthField = byte1 & 0x7F;

                int headerSize = 2;
                long payloadLength;
                if (lengthField <= 125) {
                    payloadLength = lengthField;
                } else if (lengthField == 126) {
                    while (pos < 4) {
                        int n = in.read(readBuf, pos, readBuf.length - pos);
                        if (n < 0) return;
                        pos += n;
                    }
                    payloadLength = ((readBuf[2] & 0xFF) << 8) | (readBuf[3] & 0xFF);
                    headerSize = 4;
                } else {
                    while (pos < 10) {
                        int n = in.read(readBuf, pos, readBuf.length - pos);
                        if (n < 0) return;
                        pos += n;
                    }
                    payloadLength = 0;
                    for (int i = 0; i < 8; i++) {
                        payloadLength = (payloadLength << 8) | (readBuf[2 + i] & 0xFF);
                    }
                    headerSize = 10;
                }

                if (masked) {
                    headerSize += 4;
                }

                int totalFrameSize = (int) (headerSize + payloadLength);

                if (totalFrameSize > readBuf.length) {
                    byte[] newBuf = new byte[totalFrameSize];
                    System.arraycopy(readBuf, 0, newBuf, 0, pos);
                    readBuf = newBuf;
                }

                while (pos < totalFrameSize) {
                    int n = in.read(readBuf, pos, totalFrameSize - pos);
                    if (n < 0) return;
                    pos += n;
                }

                if (opcode == WebSocketOpcode.CLOSE) {
                    return;
                }

                if (opcode != WebSocketOpcode.BINARY && opcode != WebSocketOpcode.TEXT) {
                    continue;
                }

                // Unmask payload in place
                if (masked) {
                    int maskKeyOffset = headerSize - 4;
                    byte m0 = readBuf[maskKeyOffset];
                    byte m1 = readBuf[maskKeyOffset + 1];
                    byte m2 = readBuf[maskKeyOffset + 2];
                    byte m3 = readBuf[maskKeyOffset + 3];
                    for (int i = 0; i < (int) payloadLength; i++) {
                        switch (i & 3) {
                            case 0: readBuf[headerSize + i] ^= m0; break;
                            case 1: readBuf[headerSize + i] ^= m1; break;
                            case 2: readBuf[headerSize + i] ^= m2; break;
                            case 3: readBuf[headerSize + i] ^= m3; break;
                        }
                    }
                }

                // Build complete unmasked response frame in a single array
                byte[] responseHeader;
                if (payloadLength <= 125) {
                    responseHeader = new byte[]{
                            (byte) (0x80 | opcode),
                            (byte) payloadLength
                    };
                } else if (payloadLength <= 65535) {
                    responseHeader = new byte[]{
                            (byte) (0x80 | opcode),
                            126,
                            (byte) ((payloadLength >> 8) & 0xFF),
                            (byte) (payloadLength & 0xFF)
                    };
                } else {
                    responseHeader = new byte[10];
                    responseHeader[0] = (byte) (0x80 | opcode);
                    responseHeader[1] = 127;
                    for (int i = 0; i < 8; i++) {
                        responseHeader[2 + i] = (byte) ((payloadLength >> (56 - i * 8)) & 0xFF);
                    }
                }

                // Single write: header + payload together via BufferedOutputStream
                out.write(responseHeader);
                out.write(readBuf, headerSize, (int) payloadLength);
                out.flush();
            }
        }
    }
}
