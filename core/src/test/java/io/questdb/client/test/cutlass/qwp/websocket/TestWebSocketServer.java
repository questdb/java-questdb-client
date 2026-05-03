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

package io.questdb.client.test.cutlass.qwp.websocket;

import io.questdb.client.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.client.cutlass.qwp.websocket.WebSocketOpcode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple WebSocket server for client integration testing.
 * Uses plain Java heap buffers - no native memory.
 */
public class TestWebSocketServer implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(TestWebSocketServer.class);
    private static final String WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    private final List<ClientHandler> clients = new CopyOnWriteArrayList<>();
    private final boolean emitDurableAckHeader;
    private final WebSocketServerHandler handler;
    private final int port;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private Thread acceptThread;
    private ServerSocket serverSocket;

    public TestWebSocketServer(int port, WebSocketServerHandler handler) {
        this(port, handler, false);
    }

    /**
     * @param emitDurableAckHeader when true, the 101 upgrade response includes
     *                             {@code X-QWP-Durable-Ack: enabled} so opted-in
     *                             clients (request_durable_ack=on) accept the
     *                             handshake. Set false to simulate an OSS server
     *                             that silently ignores the request and force
     *                             the client's early-fail check.
     */
    public TestWebSocketServer(int port, WebSocketServerHandler handler, boolean emitDurableAckHeader) {
        this.port = port;
        this.handler = handler;
        this.emitDurableAckHeader = emitDurableAckHeader;
    }

    public boolean awaitStart(long timeout, TimeUnit unit) throws InterruptedException {
        return startLatch.await(timeout, unit);
    }

    @Override
    public void close() {
        running.set(false);

        // Close the listener first. Clients reach for reconnects the moment we
        // close their sockets below — if the listener is still up, those
        // reconnects succeed and the new connections are never tracked here,
        // leaving them alive past close().
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                // ignore
            }
        }

        for (ClientHandler client : clients) {
            client.close();
        }
        clients.clear();

        if (acceptThread != null) {
            try {
                acceptThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void start() throws IOException {
        if (running.getAndSet(true)) {
            return;
        }

        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(100);

        acceptThread = new Thread(() -> {
            startLatch.countDown();
            while (running.get()) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    ClientHandler clientHandler = new ClientHandler(clientSocket);
                    clients.add(clientHandler);
                    clientHandler.start();
                } catch (SocketTimeoutException e) {
                    // expected, check running flag
                } catch (IOException e) {
                    if (running.get()) {
                        LOG.error("Accept error", e);
                    }
                }
            }
        }, "WebSocket-Accept");
        acceptThread.start();
    }

    /**
     * Interface for handling WebSocket server events.
     */
    public interface WebSocketServerHandler {
        default void onBinaryMessage(ClientHandler client, byte[] data) {
        }
    }

    /**
     * Handles a single WebSocket client connection.
     */
    public class ClientHandler implements Closeable {
        private final ByteBuffer recvBuffer = ByteBuffer.allocate(65_536).order(ByteOrder.BIG_ENDIAN);
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final Socket socket;
        private InputStream in;
        private boolean isClosed;
        private OutputStream out;
        private Thread readThread;

        ClientHandler(Socket socket) {
            this.socket = socket;
            recvBuffer.flip(); // start with nothing readable
        }

        @Override
        public void close() {
            running.set(false);
            try {
                socket.close();
            } catch (IOException e) {
                // ignore
            }
            if (readThread != null) {
                try {
                    readThread.join(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        public synchronized void sendBinary(byte[] data) throws IOException {
            writeFrame(WebSocketOpcode.BINARY, data, data.length);
        }

        public synchronized void sendClose(int code, String reason) throws IOException {
            byte[] reasonBytes = (reason != null && !reason.isEmpty())
                    ? reason.getBytes(StandardCharsets.UTF_8) : new byte[0];
            byte[] payload = new byte[2 + reasonBytes.length];
            payload[0] = (byte) ((code >> 8) & 0xFF);
            payload[1] = (byte) (code & 0xFF);
            System.arraycopy(reasonBytes, 0, payload, 2, reasonBytes.length);
            writeFrame(WebSocketOpcode.CLOSE, payload, payload.length);
        }

        public synchronized void sendPing(byte[] data) throws IOException {
            writeFrame(WebSocketOpcode.PING, data, data.length);
        }

        private String computeAcceptKey(String key) {
            try {
                MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
                sha1.update((key + WEBSOCKET_GUID).getBytes(StandardCharsets.US_ASCII));
                return Base64.getEncoder().encodeToString(sha1.digest());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void handleRead() {
            while (recvBuffer.remaining() >= 2) {
                recvBuffer.mark();

                int byte0 = recvBuffer.get() & 0xFF;
                int byte1 = recvBuffer.get() & 0xFF;

                int opcode = byte0 & 0x0F;
                boolean isMasked = (byte1 & 0x80) != 0;
                int lengthField = byte1 & 0x7F;

                long payloadLength;
                if (lengthField <= 125) {
                    payloadLength = lengthField;
                } else if (lengthField == 126) {
                    if (recvBuffer.remaining() < 2) {
                        recvBuffer.reset();
                        return;
                    }
                    payloadLength = (recvBuffer.get() & 0xFF) << 8 | (recvBuffer.get() & 0xFF);
                } else {
                    if (recvBuffer.remaining() < 8) {
                        recvBuffer.reset();
                        return;
                    }
                    payloadLength = recvBuffer.getLong();
                }

                int maskKeySize = isMasked ? 4 : 0;
                if (recvBuffer.remaining() < maskKeySize + payloadLength) {
                    recvBuffer.reset();
                    return;
                }

                byte[] maskKey = null;
                if (isMasked) {
                    maskKey = new byte[4];
                    recvBuffer.get(maskKey);
                }

                byte[] payload = new byte[(int) payloadLength];
                recvBuffer.get(payload);

                if (isMasked) {
                    for (int i = 0; i < payload.length; i++) {
                        payload[i] ^= maskKey[i & 3];
                    }
                }

                switch (opcode) {
                    case WebSocketOpcode.BINARY:
                        handler.onBinaryMessage(this, payload);
                        break;
                    case WebSocketOpcode.PING:
                        try {
                            writeFrame(WebSocketOpcode.PONG, payload, payload.length);
                        } catch (IOException e) {
                            LOG.error("Failed to send pong", e);
                        }
                        break;
                    case WebSocketOpcode.CLOSE: {
                        int code = WebSocketCloseCode.NORMAL_CLOSURE;
                        if (payload.length >= 2) {
                            code = ((payload[0] & 0xFF) << 8) | (payload[1] & 0xFF);
                        }
                        try {
                            sendClose(code, null);
                        } catch (IOException e) {
                            // client may have already disconnected
                        }
                        ClientHandler.this.running.set(false);
                        isClosed = true;
                        break;
                    }
                }
            }

            recvBuffer.compact();
            recvBuffer.flip();
        }

        private boolean performHandshake() throws IOException {
            StringBuilder request = new StringBuilder();
            byte[] buf = new byte[1];
            while (true) {
                int read = in.read(buf);
                if (read <= 0) {
                    return false;
                }
                request.append((char) buf[0]);
                if (request.toString().endsWith("\r\n\r\n")) {
                    break;
                }
                if (request.length() > 8192) {
                    return false;
                }
            }

            String key = null;
            for (String line : request.toString().split("\r\n")) {
                if (line.toLowerCase().startsWith("sec-websocket-key:")) {
                    key = line.substring(18).trim();
                    break;
                }
            }

            if (key == null) {
                return false;
            }

            String acceptKey = computeAcceptKey(key);

            StringBuilder sb = new StringBuilder()
                    .append("HTTP/1.1 101 Switching Protocols\r\n")
                    .append("Upgrade: websocket\r\n")
                    .append("Connection: Upgrade\r\n")
                    .append("Sec-WebSocket-Accept: ").append(acceptKey).append("\r\n");
            if (emitDurableAckHeader) {
                sb.append("X-QWP-Durable-Ack: enabled\r\n");
            }
            sb.append("\r\n");
            out.write(sb.toString().getBytes(StandardCharsets.US_ASCII));
            out.flush();

            return true;
        }

        private synchronized void writeFrame(int opcode, byte[] payload, int length) throws IOException {
            // first byte: FIN + opcode
            out.write(0x80 | (opcode & 0x0F));

            // payload length (unmasked - server to client)
            if (length <= 125) {
                out.write(length);
            } else if (length <= 65_535) {
                out.write(126);
                out.write((length >> 8) & 0xFF);
                out.write(length & 0xFF);
            } else {
                out.write(127);
                ByteBuffer lenBuf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
                lenBuf.putLong(length);
                out.write(lenBuf.array());
            }

            // payload
            out.write(payload, 0, length);
            out.flush();
        }

        void start() {
            if (running.getAndSet(true)) {
                return;
            }

            readThread = new Thread(() -> {
                try {
                    socket.setSoTimeout(100);

                    in = socket.getInputStream();
                    out = socket.getOutputStream();

                    if (!performHandshake()) {
                        LOG.error("Handshake failed");
                        return;
                    }

                    byte[] readBuf = new byte[8192];

                    while (running.get() && !isClosed) {
                        int read;
                        try {
                            read = in.read(readBuf);
                        } catch (SocketTimeoutException e) {
                            continue;
                        }
                        if (read <= 0) {
                            break;
                        }

                        // append to recvBuffer
                        recvBuffer.compact();
                        if (recvBuffer.remaining() < read) {
                            // should not happen with 64k buffer in tests
                            LOG.error("Receive buffer overflow");
                            break;
                        }
                        recvBuffer.put(readBuf, 0, read);
                        recvBuffer.flip();

                        handleRead();
                    }
                } catch (IOException e) {
                    if (running.get()) {
                        LOG.error("Client error", e);
                    }
                }
            }, "WebSocket-Client-" + socket.getPort());
            readThread.start();
        }
    }
}
