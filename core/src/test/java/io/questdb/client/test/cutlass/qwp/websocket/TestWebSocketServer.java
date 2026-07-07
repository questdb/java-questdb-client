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
import java.util.concurrent.atomic.AtomicInteger;

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
    // Count of WebSocket connections currently live from the server's view:
    // incremented when a handshake completes, decremented when that connection's
    // read thread exits (the client closed its socket). Lets a test assert that a
    // client-side pool actually closed the connections it opened.
    private final AtomicInteger liveConnections = new AtomicInteger();
    private final int port;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ServerSocket serverSocket;
    private final CountDownLatch startLatch = new CountDownLatch(1);
    // Monotonic count of completed handshakes over the server's lifetime. Unlike
    // liveConnections it never decrements, so a test can confirm how many clients
    // connected even after they have all disconnected.
    private final AtomicInteger totalHandshakes = new AtomicInteger();
    private Thread acceptThread;
    // X-QuestDB-Role value to emit on handshake responses. null = omit the
    // header (legacy behavior for tests written before role-aware failover).
    // The server emits the header on both the 101 success path and (when
    // rejectingRole != null) the 421 misdirected-request path.
    private volatile String advertisedRole;
    // When true, the server sends a SERVER_INFO frame right after a successful
    // 101 upgrade, mirroring the egress server contract (which always emits it).
    // The advertised role follows advertisedRole (STANDALONE when unset). Egress
    // QwpQueryClient tests enable this; ingress sender tests leave it off so their
    // connections carry only ACK frames.
    private volatile boolean sendServerInfo;
    private volatile int capabilities;
    // When true, the server fails the WebSocket upgrade on the egress read path
    // (/read...) by dropping the connection before the 101, while still serving
    // the ingest write path (/write...) normally. Lets one server + one cluster
    // config drive a build where the sender pool connects but the query pool
    // cannot. Set via setRejectReadUpgrade().
    private volatile boolean rejectReadUpgrade;
    // When non-null the next handshake responds with HTTP 421 Misdirected
    // Request + X-QuestDB-Role: <rejectingRole>, mimicking a server whose
    // QwpServerInfoProvider reports REPLICA / PRIMARY_CATCHUP. Set after
    // construction via setRejectWithRole().
    private volatile String rejectingRole;
    private volatile int rejectingStatusCode;
    // When true, 101 upgrade responses omit the X-QWP-Durable-Ack header even
    // though the server was constructed with emitDurableAckHeader=true --
    // simulating a rolling-upgrade window where an endpoint upgrades but does
    // not advertise durable ack (the drainer's capability-gap condition).
    // Live-updatable via setSuppressDurableAckHeader(), so a test can start
    // in the gap and later let the cluster "settle".
    private volatile boolean suppressDurableAckHeader;
    // When > 0, the next handshake responds with this status code + the
    // reason phrase from {@link #rejectingStatusReason}. Used to simulate
    // 401, 403, 404, 426, 503, etc. that the failover loop should
    // classify per failover.md §6.
    private volatile String rejectingStatusReason;

    public TestWebSocketServer(WebSocketServerHandler handler) throws IOException {
        this(handler, false);
    }

    /**
     * @param emitDurableAckHeader when true, the 101 upgrade response includes
     *                             {@code X-QWP-Durable-Ack: enabled} so opted-in
     *                             clients (request_durable_ack=on) accept the
     *                             handshake. Set false to simulate an OSS server
     *                             that silently ignores the request and force
     *                             the client's early-fail check.
     */
    public TestWebSocketServer(WebSocketServerHandler handler, boolean emitDurableAckHeader) throws IOException {
        this(handler, emitDurableAckHeader, null);
    }

    /**
     * @param advertisedRole when non-null, the value of the {@code X-QuestDB-Role}
     *                       response header on the 101 handshake — used to test
     *                       that the client accepts {@code PRIMARY} / {@code STANDALONE}
     *                       handshakes. Pass {@code null} for legacy handshakes
     *                       without the header.
     */
    public TestWebSocketServer(WebSocketServerHandler handler,
                               boolean emitDurableAckHeader, String advertisedRole) throws IOException {
        this(handler, emitDurableAckHeader, advertisedRole, 0);
    }

    /**
     * @param requestedPort loopback port to bind, or {@code 0} for an
     *                      OS-assigned ephemeral port. A caller-chosen port
     *                      lets a test model a server that goes DOWN and later
     *                      comes back UP on the SAME endpoint (down-then-up
     *                      outage realism): allocate via
     *                      {@code TestPorts.findUnusedPort()}, let the client
     *                      bang on the refused port, then bind here. Carries
     *                      the standard bind-close-reuse exposure every
     *                      pre-selected-port test in this suite accepts.
     */
    public TestWebSocketServer(WebSocketServerHandler handler,
                               boolean emitDurableAckHeader, String advertisedRole,
                               int requestedPort) throws IOException {
        this.handler = handler;
        this.emitDurableAckHeader = emitDurableAckHeader;
        this.advertisedRole = advertisedRole;
        // Bind the listener up front and hold it open for the server's whole
        // lifetime, then read the OS-assigned ephemeral port back via getPort().
        // Owning the socket from allocation to teardown closes the window in
        // which another process could grab a pre-selected port before start()
        // binds it. Pinning to loopback keeps client "localhost" connections
        // routed here rather than to a wildcard listener on the same port.
        serverSocket = new ServerSocket(requestedPort, 50, java.net.InetAddress.getLoopbackAddress());
        serverSocket.setSoTimeout(100);
        this.port = serverSocket.getLocalPort();
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
        try {
            serverSocket.close();
        } catch (IOException e) {
            // ignore
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

    /**
     * Returns the loopback port the listener is bound to. Stable for the
     * server's whole lifetime; safe to read immediately after construction.
     */
    public int getPort() {
        return port;
    }

    /**
     * Number of handshakes the server has completed over its lifetime
     * (monotonic; never decreases when clients disconnect).
     */
    public int handshakeCount() {
        return totalHandshakes.get();
    }

    /**
     * Number of WebSocket connections currently live from the server's view.
     * Drops back to zero once every client has closed its socket.
     */
    public int liveConnectionCount() {
        return liveConnections.get();
    }

    /**
     * Replaces the advertised role for subsequent handshakes (live update).
     */
    public void setAdvertisedRole(String role) {
        this.advertisedRole = role;
    }

    /**
     * Configure the server to reject the next handshake with HTTP 421 +
     * {@code X-QuestDB-Role: <role>}. Pass {@code null} to clear and resume
     * normal 101 upgrades. The setting applies to every new connection
     * until cleared, so tests can simulate a permanent replica or a node
     * that becomes primary mid-test.
     */
    public void setRejectWithRole(String role) {
        this.rejectingRole = role;
    }

    /**
     * When enabled, the server fails the WebSocket upgrade on the egress read
     * path ({@code /read/...}) while still serving the ingest write path
     * ({@code /write/...}) normally. This lets a single server, addressed by a
     * single cluster config, accept ingest senders but reject query clients --
     * e.g. to exercise build()'s unwind of an already-built sender pool when the
     * query pool fails.
     */
    public void setRejectReadUpgrade(boolean rejectReadUpgrade) {
        this.rejectReadUpgrade = rejectReadUpgrade;
    }

    /**
     * Configure the server to reject the next handshake with an arbitrary
     * HTTP status code (e.g. 401, 403, 404, 426, 503). Pass {@code 0} to
     * clear and resume normal 101 upgrades. Tests use this to drive the
     * client's terminal-vs-transient classification per failover.md §6.
     */
    public void setRejectWithStatus(int statusCode, String reasonPhrase) {
        this.rejectingStatusCode = statusCode;
        this.rejectingStatusReason = reasonPhrase;
    }

    /**
     * When enabled, 101 upgrade responses omit the {@code X-QWP-Durable-Ack}
     * header even on a server constructed with {@code emitDurableAckHeader} —
     * the next opted-in connect ({@code request_durable_ack=on}) observes a
     * durable-ack capability gap. Pass {@code false} to clear and resume
     * advertising, the way a rolling upgrade eventually settles. The setting
     * applies to every new handshake until cleared.
     */
    public void setSuppressDurableAckHeader(boolean suppressDurableAckHeader) {
        this.suppressDurableAckHeader = suppressDurableAckHeader;
    }

    /**
     * When enabled, the server sends a {@code SERVER_INFO} frame immediately
     * after a successful 101 upgrade on the egress read path ({@code /read/...}),
     * the way a real egress endpoint does. Ingest write-path ({@code /write/...})
     * connections never receive it -- their ACK-only response stream would choke
     * on an unexpected frame -- so one server can serve both an ingest and a
     * query pool from a single cluster config. The advertised role follows
     * {@link #setAdvertisedRole}, defaulting to {@code STANDALONE}.
     */
    public void setSendServerInfo(boolean sendServerInfo) {
        this.sendServerInfo = sendServerInfo;
    }

    // Advertised SERVER_INFO capabilities. CAP_ZONE is unsupported here: the
    // frame builder emits no zone_id trailer.
    public void setCapabilities(int capabilities) {
        this.capabilities = capabilities;
    }

    private static byte[] buildServerInfoFrame(byte role, int capabilities) {
        byte[] clusterId = "questdb".getBytes(StandardCharsets.UTF_8);
        byte[] nodeId = "test-node".getBytes(StandardCharsets.UTF_8);
        int bodyLen = 1 + 1 + 8 + 4 + 8 + 2 + clusterId.length + 2 + nodeId.length;
        ByteBuffer bb = ByteBuffer.allocate(12 + bodyLen).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 'Q').put((byte) 'W').put((byte) 'P').put((byte) '1');
        bb.put((byte) 1);       // version
        bb.put((byte) 0);       // flags
        bb.putShort((short) 0); // table_count (unused for control frames)
        bb.putInt(bodyLen);     // payload_length
        bb.put((byte) 0x18);    // SERVER_INFO msg_kind
        bb.put(role);
        bb.putLong(0L);         // epoch
        bb.putInt(capabilities); // CAP_ZONE unsupported here -> no zone_id trailer
        bb.putLong(1L);         // server_wall_ns (positive)
        bb.putShort((short) clusterId.length);
        bb.put(clusterId);
        bb.putShort((short) nodeId.length);
        bb.put(nodeId);
        return bb.array();
    }

    private static boolean isReadPath(String path) {
        return path != null && path.startsWith("/read");
    }

    private static byte roleByte(String role) {
        if (role == null) {
            return 0; // ROLE_STANDALONE
        }
        switch (role) {
            case "PRIMARY":
                return 1;
            case "REPLICA":
                return 2;
            case "PRIMARY_CATCHUP":
                return 3;
            default:
                return 0; // STANDALONE
        }
    }

    public void start() throws IOException {
        if (running.getAndSet(true)) {
            return;
        }

        // The listener is already bound (see the constructor); just spin up the
        // accept loop on it.
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
        // Request path from the WebSocket upgrade GET line (e.g. /write/v4,
        // /read/v1). Captured during the handshake so the post-upgrade logic can
        // distinguish ingest from egress connections.
        private String requestPath = "";

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
            String[] lines = request.toString().split("\r\n");
            if (lines.length > 0) {
                // GET <path> HTTP/1.1
                String[] parts = lines[0].split(" ");
                if (parts.length >= 2) {
                    requestPath = parts[1];
                }
            }
            for (String line : lines) {
                if (line.toLowerCase().startsWith("sec-websocket-key:")) {
                    key = line.substring(18).trim();
                    break;
                }
            }

            if (key == null) {
                return false;
            }

            // Read-path reject: drop the egress upgrade before the 101 so the
            // query pool's connect fails fast, while ingest write-path upgrades
            // still complete on this same server.
            if (rejectReadUpgrade && isReadPath(requestPath)) {
                return false;
            }

            // Arbitrary-status reject path: tests use setRejectWithStatus
            // to drive the failover loop's terminal-vs-transient
            // classification (failover.md §6).
            int customStatus = rejectingStatusCode;
            if (customStatus > 0) {
                String reason = rejectingStatusReason != null ? rejectingStatusReason : "";
                String sb = "HTTP/1.1 " + customStatus + ' ' + reason + "\r\n" +
                        "Connection: close\r\n" +
                        "Content-Length: 0\r\n" +
                        "\r\n";
                out.write(sb.getBytes(StandardCharsets.US_ASCII));
                out.flush();
                return false;
            }
            // Role-aware reject path: emit a 421 Misdirected Request +
            // X-QuestDB-Role: <role> response so the client treats this
            // node as REPLICA / PRIMARY_CATCHUP and rotates to the next
            // configured address. Mirrors what QwpWebSocketUpgradeProcessor
            // does on a server whose QwpServerInfoProvider reports a
            // non-writable role.
            String reject = rejectingRole;
            if (reject != null) {
                String sb = "HTTP/1.1 421 Misdirected Request\r\n" +
                        "Connection: close\r\n" +
                        "Content-Length: 0\r\n" +
                        "X-QuestDB-Role: " + reject + "\r\n" +
                        "\r\n";
                out.write(sb.getBytes(StandardCharsets.US_ASCII));
                out.flush();
                return false;
            }

            String acceptKey = computeAcceptKey(key);

            StringBuilder sb = new StringBuilder()
                    .append("HTTP/1.1 101 Switching Protocols\r\n")
                    .append("Upgrade: websocket\r\n")
                    .append("Connection: Upgrade\r\n")
                    .append("Sec-WebSocket-Accept: ").append(acceptKey).append("\r\n");
            if (emitDurableAckHeader && !suppressDurableAckHeader) {
                sb.append("X-QWP-Durable-Ack: enabled\r\n");
            }
            String role = advertisedRole;
            if (role != null) {
                sb.append("X-QuestDB-Role: ").append(role).append("\r\n");
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
                    totalHandshakes.incrementAndGet();
                    liveConnections.incrementAndGet();

                    try {
                        // SERVER_INFO is an egress-only frame: send it only on a
                        // read-path (query) connection. An ingest write-path
                        // connection parses every inbound frame as an ACK and
                        // would fail on it.
                        if (sendServerInfo && isReadPath(requestPath)) {
                            sendBinary(buildServerInfoFrame(roleByte(advertisedRole), capabilities));
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
                    } finally {
                        liveConnections.decrementAndGet();
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
