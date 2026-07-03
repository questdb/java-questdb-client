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

package io.questdb.client.cutlass.http.client;

import io.questdb.client.HttpClientConfiguration;
import io.questdb.client.cutlass.qwp.client.QwpVersionMismatchException;
import io.questdb.client.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.client.cutlass.qwp.websocket.WebSocketFrameParser;
import io.questdb.client.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.client.network.IOOperation;
import io.questdb.client.network.NetworkFacade;
import io.questdb.client.network.Socket;
import io.questdb.client.network.SocketFactory;
import io.questdb.client.network.TlsSessionInitFailedException;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Misc;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.SecureRnd;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.Vect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Zero-GC WebSocket client built on QuestDB's native socket infrastructure.
 * <p>
 * This client uses native memory buffers and non-blocking I/O with
 * platform-specific event notification (epoll/kqueue/select).
 * <p>
 * Features:
 * <ul>
 *   <li>Zero-copy send path using {@link WebSocketSendBuffer}</li>
 *   <li>Automatic ping/pong handling</li>
 *   <li>TLS support</li>
 *   <li>Connection keep-alive</li>
 * </ul>
 * <p>
 * Thread safety: This class is NOT thread-safe. Each connection should be
 * accessed from a single thread at a time.
 */
public abstract class WebSocketClient implements QuietCloseable {

    private static final int DEFAULT_RECV_BUFFER_SIZE = 65536;
    private static final int DEFAULT_SEND_BUFFER_SIZE = 65536;
    private static final Logger LOG = LoggerFactory.getLogger(WebSocketClient.class);
    // tryParseFrame() return values
    private static final int PARSE_INCOMPLETE = 0;
    private static final int PARSE_NEED_MORE = -1;
    private static final int PARSE_OK = 1;
    private static final String QUESTDB_ROLE_HEADER_NAME = "X-QuestDB-Role:";
    private static final String QUESTDB_ZONE_HEADER_NAME = "X-QuestDB-Zone:";
    private static final String QWP_CONTENT_ENCODING_HEADER_NAME = "X-QWP-Content-Encoding:";
    private static final String QWP_DURABLE_ACK_ENABLED_VALUE = "enabled";
    private static final String QWP_DURABLE_ACK_HEADER_NAME = "X-QWP-Durable-Ack:";
    private static final String QWP_MAX_BATCH_SIZE_HEADER_NAME = "X-QWP-Max-Batch-Size:";
    private static final String QWP_VERSION_HEADER_NAME = "X-QWP-Version:";
    private static final ThreadLocal<MessageDigest> SHA1_DIGEST = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 not available", e);
        }
    });
    private static final byte[] WEBSOCKET_GUID_BYTES = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11".getBytes(StandardCharsets.US_ASCII);
    protected final NetworkFacade nf;
    protected final Socket socket;
    private final WebSocketSendBuffer controlFrameBuffer;
    private final int defaultTimeout;
    private final WebSocketFrameParser frameParser;
    private final int maxRecvBufSize;
    private final SecureRnd rnd;
    private final WebSocketSendBuffer sendBuffer;
    // Written by whichever closer wins the CAS in close(); read by the I/O
    // thread in checkConnected()/sendFrame()/receiveFrame(). An AtomicBoolean
    // (not a bare volatile check-then-act) so concurrent closers cannot both
    // enter close() and double-run disconnect()/Unsafe.free.
    private final AtomicBoolean closed = new AtomicBoolean();
    // Upper bound (ms) on the TCP connect. <= 0 disables the application-level
    // timeout and falls back to the OS connect timeout. Seeded from the
    // configuration; the QWP sender may override it via setConnectTimeout().
    private int connectTimeoutMillis;
    private int fragmentBufPos;
    private long fragmentBufPtr;       // native buffer for accumulating fragment payloads
    private int fragmentBufSize;
    // Fragmentation state (RFC 6455 Section 5.4)
    private int fragmentOpcode = -1;   // opcode of first fragment, -1 = not in a fragmented message
    // Handshake key for verification
    private String handshakeKey;
    // Connection state
    private CharSequence host;
    private int port;
    // QWP version negotiation
    // Verbatim header value sent as X-QWP-Accept-Encoding during upgrade, e.g.
    // "zstd;level=1,raw". When null, the header is omitted and the server ships
    // batches uncompressed. The RESULT_BATCH decoder branches on FLAG_ZSTD in
    // every frame -- that's the authoritative signal for whether a given
    // payload is compressed. The echoed X-QWP-Content-Encoding response header
    // is parsed in addition (into {@link #serverNegotiatedZstdLevel}) so user
    // code and tests can observe the level the server actually applied, which
    // matters when the server uses qwp.egress.compression.force.level to cap
    // or pin the level regardless of the client's request.
    private String qwpAcceptEncoding;
    private String qwpClientId;
    // Client-requested per-batch row cap advertised via X-QWP-Max-Batch-Rows.
    // 0 means "omit the header" (server uses its default cap). Server may clamp
    // down to its own hard limit.
    private int qwpMaxBatchRows;
    private int qwpMaxVersion = 1;
    // Opt-in for STATUS_DURABLE_ACK frames; sent as X-QWP-Request-Durable-Ack: true
    private boolean qwpRequestDurableAck;
    // Receive buffer (native memory)
    private long recvBufPtr;
    private int recvBufSize;
    private int recvPos;      // Write position
    private int recvReadPos;  // Read position
    // Set during upgrade response validation when the server echoed
    // X-QWP-Durable-Ack: enabled. Tells the sender it landed on a server that
    // will actually emit STATUS_DURABLE_ACK frames, so its store-and-forward
    // path can rely on durable-ack-driven trim. Absence (after opting in via
    // setQwpRequestDurableAck) is the early-fail signal.
    private boolean serverDurableAckEnabled;
    // Server's hard cap on ingest QWP message payload bytes, extracted from
    // X-QWP-Max-Batch-Size on the 101 upgrade response. 0 when the server did
    // not advertise the header (older builds), in which case the sender falls
    // back to its locally configured budget.
    private int serverMaxBatchSize;
    // Zstd compression level the server actually applied for this connection,
    // extracted from the echoed X-QWP-Content-Encoding response header.
    // 0 means "no zstd" -- either the server picked raw, the header was absent
    // (older builds), or the value didn't match the "zstd;level=N" shape.
    // Non-zero values are clamped server-side to [1, 9] but we surface what
    // the wire said without re-clamping so a misconfigured server is observable.
    private int serverNegotiatedZstdLevel;
    private int serverQwpVersion = 1;
    private String upgradeRejectRole;
    // Server-advertised zone identifier from the most recent rejected upgrade,
    // captured from the X-QuestDB-Zone response header on a 421. Null when the
    // header was absent or empty. Per failover.md §5 servers SHOULD emit this
    // on every 421 reject so a client can record the host's zone tier without
    // a successful upgrade. Reset to null on every upgrade() invocation.
    private String upgradeRejectZone;
    private int upgradeStatusCode;
    private boolean upgraded;

    public WebSocketClient(HttpClientConfiguration configuration, SocketFactory socketFactory) {
        this.nf = configuration.getNetworkFacade();
        this.socket = socketFactory.newInstance(nf, LOG);
        this.defaultTimeout = configuration.getTimeout();
        this.connectTimeoutMillis = configuration.getConnectTimeout();

        int sendBufSize = Math.max(configuration.getInitialRequestBufferSize(), DEFAULT_SEND_BUFFER_SIZE);
        int maxSendBufSize = Math.max(configuration.getMaximumRequestBufferSize(), sendBufSize);
        WebSocketSendBuffer sendBuf = null;
        WebSocketSendBuffer controlBuf = null;
        try {
            sendBuf = new WebSocketSendBuffer(sendBufSize, maxSendBufSize);
            // Control frames (ping/pong/close) have max 125-byte payload + 14-byte header.
            // This dedicated buffer prevents sendPongFrame from clobbering an in-progress
            // frame being built in the main sendBuffer.
            controlBuf = new WebSocketSendBuffer(256, 256);

            this.recvBufSize = Math.max(configuration.getResponseBufferSize(), DEFAULT_RECV_BUFFER_SIZE);
            this.maxRecvBufSize = Math.max(configuration.getMaximumResponseBufferSize(), recvBufSize);
            this.recvBufPtr = Unsafe.malloc(recvBufSize, MemoryTag.NATIVE_DEFAULT);

            this.sendBuffer = sendBuf;
            this.controlFrameBuffer = controlBuf;
            this.recvPos = 0;
            this.recvReadPos = 0;

            this.frameParser = new WebSocketFrameParser();
            this.rnd = new SecureRnd();
            this.upgraded = false;
            this.closed.set(false);
        } catch (Throwable t) {
            if (recvBufPtr != 0) {
                Unsafe.free(recvBufPtr, recvBufSize, MemoryTag.NATIVE_DEFAULT);
                recvBufPtr = 0;
            }
            Misc.free(controlBuf);
            Misc.free(sendBuf);
            Misc.free(socket);
            throw t;
        }
    }

    @Override
    public void close() {
        // CAS gate: exactly one closer runs the teardown below. Closers can be
        // the owner thread, the I/O thread's exit path, or a stale duplicate
        // reference (see CursorWebSocketSendLoop) -- a bare volatile
        // check-then-act here would let two concurrent closers both enter and
        // double-run disconnect()/Unsafe.free (native double-free).
        if (closed.compareAndSet(false, true)) {

            // Try to send close frame
            if (upgraded && !socket.isClosed()) {
                try {
                    sendCloseFrame(WebSocketCloseCode.NORMAL_CLOSURE, null, 1000);
                } catch (Exception e) {
                    // Ignore errors during close
                }
            }

            disconnect();
            sendBuffer.close();
            controlFrameBuffer.close();

            if (fragmentBufPtr != 0) {
                Unsafe.free(fragmentBufPtr, fragmentBufSize, MemoryTag.NATIVE_DEFAULT);
                fragmentBufPtr = 0;
            }

            if (recvBufPtr != 0) {
                Unsafe.free(recvBufPtr, recvBufSize, MemoryTag.NATIVE_DEFAULT);
                recvBufPtr = 0;
            }
        }
    }

    /**
     * Connects to a WebSocket server.
     *
     * @param host the server hostname
     * @param port the server port
     */
    public void connect(CharSequence host, int port) {
        if (closed.get()) {
            throw new HttpClientException("WebSocket client is closed");
        }

        // Close existing connection if connecting to different host:port
        if (this.host != null && (!this.host.equals(host) || this.port != port)) {
            disconnect();
        }

        if (socket.isClosed()) {
            doConnect(host, port);
        }

        this.host = host;
        this.port = port;
    }

    /**
     * Disconnects the socket without closing the client.
     * The client can be reconnected by calling connect() again.
     * <p>
     * This method is NOT thread-safe. Only call it when no other thread
     * is using this client (e.g., after the I/O thread has stopped).
     */
    public void disconnect() {
        Misc.free(socket);
        upgraded = false;
        host = null;
        port = 0;
        recvPos = 0;
        recvReadPos = 0;
        resetFragmentState();
    }

    /**
     * Returns the connected host.
     */
    public CharSequence getHost() {
        return host;
    }

    /**
     * Returns the connected port.
     */
    public int getPort() {
        return port;
    }

    /**
     * Gets the send buffer for building WebSocket frames.
     * <p>
     * Usage:
     * <pre>
     * WebSocketSendBuffer buf = client.getSendBuffer();
     * buf.beginBinaryFrame();
     * buf.putLong(data);
     * WebSocketSendBuffer.FrameInfo frame = buf.endBinaryFrame();
     * client.sendFrame(frame, timeout);
     * buf.reset();
     * </pre>
     */
    public WebSocketSendBuffer getSendBuffer() {
        return sendBuffer;
    }

    /**
     * Server-advertised hard cap on QWP ingest payload bytes, taken from the
     * {@code X-QWP-Max-Batch-Size} response header on the 101 upgrade. Returns
     * {@code 0} when the server did not advertise the header, in which case
     * the caller must fall back to its own configured budget.
     */
    public int getServerMaxBatchSize() {
        return serverMaxBatchSize;
    }

    /**
     * Zstd level the server actually applied for this connection, parsed
     * from the echoed {@code X-QWP-Content-Encoding} response header.
     * <p>
     * Returns {@code 0} when the server picked raw transport (no compression),
     * the header was absent (older servers), or the value did not match the
     * {@code zstd;level=N} shape. Non-zero values are returned as the server
     * wrote them on the wire; this client does not re-clamp to {@code [1, 9]}
     * so a misconfigured server is observable rather than silently smoothed
     * over.
     * <p>
     * Useful for diagnostics and for tests that pin operator-side overrides
     * (see {@code qwp.egress.compression.force.level}) -- the level the
     * client requested via {@link #setQwpAcceptEncoding} is what it asked
     * for; this is what it actually got.
     */
    public int getServerNegotiatedZstdLevel() {
        return serverNegotiatedZstdLevel;
    }

    /**
     * Returns the QWP version selected by the server during the upgrade handshake.
     */
    public int getServerQwpVersion() {
        return serverQwpVersion;
    }

    /**
     * Role from {@code X-QuestDB-Role} on the most recent rejected upgrade,
     * or null when no such header was present.
     */
    public String getUpgradeRejectRole() {
        return upgradeRejectRole;
    }

    /**
     * Zone identifier from {@code X-QuestDB-Zone} on the most recent rejected
     * upgrade, or null when the header was absent or empty (after trimming).
     * Per {@code failover.md} §5 this is the upgrade-time companion to
     * {@code SERVER_INFO.zone_id} so a client can classify a host's zone tier
     * even when the upgrade did not succeed.
     */
    public String getUpgradeRejectZone() {
        return upgradeRejectZone;
    }

    /**
     * HTTP status code from the most recent rejected upgrade, or 0 if no
     * upgrade rejection has been observed yet.
     */
    public int getUpgradeStatusCode() {
        return upgradeStatusCode;
    }

    /**
     * Returns whether the WebSocket is connected and upgraded.
     */
    public boolean isConnected() {
        return upgraded && !closed.get() && !socket.isClosed();
    }

    /**
     * Returns true when the server echoed X-QWP-Durable-Ack: enabled in the
     * 101 upgrade response. Meaningful only after {@link #upgrade} returns;
     * always false when the client did not opt in via
     * {@link #setQwpRequestDurableAck}.
     */
    public boolean isServerDurableAckEnabled() {
        return serverDurableAckEnabled;
    }

    /**
     * Receives and processes WebSocket frames.
     *
     * @param handler frame handler callback
     * @param timeout timeout in milliseconds
     * @return true if a frame was received, false on timeout
     */
    public boolean receiveFrame(WebSocketFrameHandler handler, int timeout) {
        checkConnected();

        // First, try to parse any data already in buffer
        int result = tryParseFrame(handler);
        if (result != PARSE_NEED_MORE) {
            return result == PARSE_OK;
        }

        // Need more data
        long startTime = System.nanoTime();
        while (true) {
            int remainingTimeout = remainingTime(timeout, startTime);
            if (remainingTimeout <= 0) {
                return false; // Timeout
            }

            // Ensure buffer has space
            if (recvPos >= recvBufSize - 1024) {
                growRecvBuffer();
            }

            int bytesRead = recvOrTimeout(recvBufPtr + recvPos, recvBufSize - recvPos, remainingTimeout);
            if (bytesRead <= 0) {
                return false; // Timeout
            }
            recvPos += bytesRead;

            result = tryParseFrame(handler);
            if (result != PARSE_NEED_MORE) {
                return result == PARSE_OK;
            }
        }
    }

    /**
     * Sends binary data as a WebSocket binary frame.
     *
     * @param dataPtr pointer to data
     * @param length  data length
     * @param timeout timeout in milliseconds
     */
    public void sendBinary(long dataPtr, int length, int timeout) {
        checkConnected();
        sendBuffer.reset();
        sendBuffer.beginFrame();
        sendBuffer.putBlockOfBytes(dataPtr, length);
        WebSocketSendBuffer.FrameInfo frame = sendBuffer.endBinaryFrame();
        doSend(sendBuffer.getBufferPtr() + frame.offset, frame.length, timeout);
        sendBuffer.reset();
    }

    /**
     * Sends binary data with default timeout.
     */
    public void sendBinary(long dataPtr, int length) {
        sendBinary(dataPtr, length, defaultTimeout);
    }

    /**
     * Sends a close frame.
     */
    public void sendCloseFrame(int code, String reason, int timeout) {
        controlFrameBuffer.reset();
        WebSocketSendBuffer.FrameInfo frame = controlFrameBuffer.writeCloseFrame(code, reason);
        try {
            doSend(controlFrameBuffer.getBufferPtr() + frame.offset, frame.length, timeout);
        } finally {
            controlFrameBuffer.reset();
        }
    }

    /**
     * Sends a ping frame.
     */
    public void sendPing(int timeout) {
        checkConnected();
        controlFrameBuffer.reset();
        WebSocketSendBuffer.FrameInfo frame = controlFrameBuffer.writePingFrame();
        try {
            doSend(controlFrameBuffer.getBufferPtr() + frame.offset, frame.length, timeout);
        } finally {
            controlFrameBuffer.reset();
        }
    }

    /**
     * Overrides the TCP connect timeout (milliseconds) for subsequent
     * {@link #connect} calls. {@code <= 0} disables the application-level
     * timeout and falls back to the OS connect timeout. Must be called before
     * {@link #connect}.
     */
    public void setConnectTimeout(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    /**
     * Sets the value sent as the {@code X-QWP-Accept-Encoding} upgrade header,
     * e.g. {@code "zstd;level=1,raw"}. Pass {@code null} to omit the header
     * entirely (server ships uncompressed batches). Must be called before
     * {@link #upgrade}.
     */
    public void setQwpAcceptEncoding(String acceptEncoding) {
        this.qwpAcceptEncoding = acceptEncoding;
    }

    /**
     * Sets the QWP client identifier sent in the X-QWP-Client-Id upgrade header.
     */
    public void setQwpClientId(String clientId) {
        this.qwpClientId = clientId;
    }

    /**
     * Sets the client's preferred per-batch row cap, sent in the
     * {@code X-QWP-Max-Batch-Rows} upgrade header. {@code 0} (the default)
     * omits the header entirely and the server uses its own cap. Positive
     * values ask the server to flush batches sooner (lower time-to-first-row
     * for streaming consumers, at the cost of more per-batch overhead); the
     * server clamps down to its own maximum.
     */
    public void setQwpMaxBatchRows(int maxBatchRows) {
        this.qwpMaxBatchRows = maxBatchRows;
    }

    /**
     * Sets the maximum QWP version this client supports, sent in the X-QWP-Max-Version upgrade header.
     */
    public void setQwpMaxVersion(int maxVersion) {
        this.qwpMaxVersion = maxVersion;
    }

    /**
     * Enables the opt-in X-QWP-Request-Durable-Ack upgrade header. When set,
     * servers with primary replication configured will additionally emit
     * STATUS_DURABLE_ACK frames as the WAL containing committed client
     * messages reaches the object store.
     */
    public void setQwpRequestDurableAck(boolean enabled) {
        this.qwpRequestDurableAck = enabled;
    }

    /**
     * Non-blocking attempt to receive a WebSocket frame.
     * Returns immediately if no complete frame is available.
     *
     * @param handler frame handler callback
     * @return true if a frame was received, false if no data available
     */
    public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
        checkConnected();

        // First, try to parse any data already in buffer
        int result = tryParseFrame(handler);
        if (result != PARSE_NEED_MORE) {
            return result == PARSE_OK;
        }

        // Try one non-blocking recv
        if (recvPos >= recvBufSize - 1024) {
            growRecvBuffer();
        }

        int n = socket.recv(recvBufPtr + recvPos, recvBufSize - recvPos);
        if (n < 0) {
            throw new HttpClientException("peer disconnect [errno=").errno(nf.errno()).put(']');
        }
        if (n == 0) {
            return false; // No data available
        }
        recvPos += n;

        // Try to parse again
        result = tryParseFrame(handler);
        return result == PARSE_OK;
    }

    /**
     * Performs WebSocket upgrade handshake.
     *
     * @param path                the WebSocket endpoint path (e.g., "/ws")
     * @param timeout             timeout in milliseconds
     * @param authorizationHeader the Authorization header value (e.g., "Basic ..."), or null
     */
    public void upgrade(CharSequence path, int timeout, CharSequence authorizationHeader) {
        if (closed.get()) {
            throw new HttpClientException("WebSocket client is closed");
        }
        if (socket.isClosed()) {
            throw new HttpClientException("Not connected");
        }
        if (upgraded) {
            return; // Already upgraded
        }
        upgradeRejectRole = null;
        upgradeRejectZone = null;
        upgradeStatusCode = 0;

        // Generate random key
        byte[] keyBytes = new byte[16];
        for (int i = 0; i < 16; i++) {
            keyBytes[i] = (byte) rnd.nextInt();
        }
        handshakeKey = Base64.getEncoder().encodeToString(keyBytes);

        // Build upgrade request
        sendBuffer.reset();
        sendBuffer.putAscii("GET ");
        sendBuffer.putAscii(path);
        sendBuffer.putAscii(" HTTP/1.1\r\n");
        sendBuffer.putAscii("Host: ");
        sendBuffer.putAscii(host);
        if ((socket.supportsTls() && port != 443) || (!socket.supportsTls() && port != 80)) {
            sendBuffer.putAscii(":");
            sendBuffer.putAscii(Integer.toString(port));
        }
        sendBuffer.putAscii("\r\n");
        sendBuffer.putAscii("Upgrade: websocket\r\n");
        sendBuffer.putAscii("Connection: Upgrade\r\n");
        sendBuffer.putAscii("Sec-WebSocket-Key: ");
        sendBuffer.putAscii(handshakeKey);
        sendBuffer.putAscii("\r\n");
        sendBuffer.putAscii("Sec-WebSocket-Version: 13\r\n");
        sendBuffer.putAscii("X-QWP-Max-Version: ");
        sendBuffer.putAscii(Integer.toString(qwpMaxVersion));
        sendBuffer.putAscii("\r\n");
        if (qwpClientId != null) {
            sendBuffer.putAscii("X-QWP-Client-Id: ");
            sendBuffer.putAscii(qwpClientId);
            sendBuffer.putAscii("\r\n");
        }
        if (qwpAcceptEncoding != null) {
            sendBuffer.putAscii("X-QWP-Accept-Encoding: ");
            sendBuffer.putAscii(qwpAcceptEncoding);
            sendBuffer.putAscii("\r\n");
        }
        if (qwpMaxBatchRows > 0) {
            sendBuffer.putAscii("X-QWP-Max-Batch-Rows: ");
            sendBuffer.putAscii(Integer.toString(qwpMaxBatchRows));
            sendBuffer.putAscii("\r\n");
        }
        if (qwpRequestDurableAck) {
            sendBuffer.putAscii("X-QWP-Request-Durable-Ack: true\r\n");
        }
        if (authorizationHeader != null) {
            sendBuffer.putAscii("Authorization: ");
            sendBuffer.putAscii(authorizationHeader);
            sendBuffer.putAscii("\r\n");
        }
        sendBuffer.putAscii("\r\n");

        // Send request
        long startTime = System.nanoTime();
        doSend(sendBuffer.getBufferPtr(), sendBuffer.getWritePos(), timeout);

        // Read response
        int remainingTimeout = getRemainingTimeOrThrow(timeout, startTime);
        readUpgradeResponse(remainingTimeout);

        upgraded = true;
        sendBuffer.reset();
        if (LOG.isDebugEnabled()) {
            LOG.debug("WebSocket upgraded [path={}]", path);
        }
    }

    /**
     * Performs upgrade with default timeout and authorization header.
     */
    public void upgrade(CharSequence path, CharSequence authorizationHeader) {
        upgrade(path, defaultTimeout, authorizationHeader);
    }

    private static String computeAcceptKey(String key) {
        MessageDigest sha1 = SHA1_DIGEST.get();
        sha1.reset();
        for (int i = 0, n = key.length(); i < n; i++) {
            sha1.update((byte) key.charAt(i));
        }
        sha1.update(WEBSOCKET_GUID_BYTES);
        return Base64.getEncoder().encodeToString(sha1.digest());
    }

    private static boolean excludesHeaderValue(String response, String headerName, String expectedValue, boolean ignoreValueCase) {
        int headerLen = headerName.length();
        int responseLen = response.length();
        for (int i = 0; i <= responseLen - headerLen; i++) {
            if (response.regionMatches(true, i, headerName, 0, headerLen)) {
                int valueStart = i + headerLen;
                int lineEnd = response.indexOf('\r', valueStart);
                if (lineEnd < 0) {
                    lineEnd = responseLen;
                }
                String actualValue = response.substring(valueStart, lineEnd).trim();
                return ignoreValueCase ? !actualValue.equalsIgnoreCase(expectedValue) : !actualValue.equals(expectedValue);
            }
        }
        return true;
    }

    /**
     * Extracts the zstd level the server applied from the echoed
     * {@code X-QWP-Content-Encoding} response header. Returns 0 when the
     * header is absent (older servers, or the server picked raw transport)
     * or the value does not match the {@code zstd;level=N} shape. The
     * returned value is intentionally not re-clamped to {@code [1, 9]} so
     * a misconfigured server is observable from the client.
     */
    private static int extractContentEncodingZstdLevel(String response) {
        int headerLen = QWP_CONTENT_ENCODING_HEADER_NAME.length();
        int responseLen = response.length();
        for (int i = 0; i <= responseLen - headerLen; i++) {
            if (response.regionMatches(true, i, QWP_CONTENT_ENCODING_HEADER_NAME, 0, headerLen)) {
                int valueStart = i + headerLen;
                int lineEnd = response.indexOf('\r', valueStart);
                if (lineEnd < 0) {
                    lineEnd = responseLen;
                }
                String value = response.substring(valueStart, lineEnd).trim();
                // Expected shape: "zstd;level=N". Anything else -> 0 (treat as raw / unknown).
                int semi = value.indexOf(';');
                if (semi < 0) {
                    return 0;
                }
                if (!value.regionMatches(true, 0, "zstd", 0, 4) || semi != 4) {
                    return 0;
                }
                int eq = value.indexOf('=', semi + 1);
                if (eq < 0) {
                    return 0;
                }
                if (!value.regionMatches(true, semi + 1, "level", 0, 5) || eq != semi + 6) {
                    return 0;
                }
                try {
                    int parsed = Integer.parseInt(value.substring(eq + 1).trim());
                    return Math.max(parsed, 0);
                } catch (NumberFormatException e) {
                    return 0;
                }
            }
        }
        return 0;
    }

    private static boolean extractDurableAckEnabled(String response) {
        int headerLen = QWP_DURABLE_ACK_HEADER_NAME.length();
        int responseLen = response.length();
        for (int i = 0; i <= responseLen - headerLen; i++) {
            if (response.regionMatches(true, i, QWP_DURABLE_ACK_HEADER_NAME, 0, headerLen)) {
                int valueStart = i + headerLen;
                int lineEnd = response.indexOf('\r', valueStart);
                if (lineEnd < 0) {
                    lineEnd = responseLen;
                }
                String value = response.substring(valueStart, lineEnd).trim();
                return value.equalsIgnoreCase(QWP_DURABLE_ACK_ENABLED_VALUE);
            }
        }
        return false;
    }

    private static int extractMaxBatchSize(String response) {
        int headerLen = QWP_MAX_BATCH_SIZE_HEADER_NAME.length();
        int responseLen = response.length();
        for (int i = 0; i <= responseLen - headerLen; i++) {
            if (response.regionMatches(true, i, QWP_MAX_BATCH_SIZE_HEADER_NAME, 0, headerLen)) {
                int valueStart = i + headerLen;
                int lineEnd = response.indexOf('\r', valueStart);
                if (lineEnd < 0) {
                    lineEnd = responseLen;
                }
                String value = response.substring(valueStart, lineEnd).trim();
                try {
                    int parsed = Integer.parseInt(value);
                    return Math.max(parsed, 0);
                } catch (NumberFormatException e) {
                    return 0;
                }
            }
        }
        return 0;
    }

    private static int extractQwpVersion(String response) {
        int headerLen = QWP_VERSION_HEADER_NAME.length();
        int responseLen = response.length();
        for (int i = 0; i <= responseLen - headerLen; i++) {
            if (response.regionMatches(true, i, QWP_VERSION_HEADER_NAME, 0, headerLen)) {
                int valueStart = i + headerLen;
                int lineEnd = response.indexOf('\r', valueStart);
                if (lineEnd < 0) {
                    lineEnd = responseLen;
                }
                String value = response.substring(valueStart, lineEnd).trim();
                try {
                    return Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    return 1;
                }
            }
        }
        return 1;
    }

    private static String extractRoleHeader(String response) {
        int headerLen = QUESTDB_ROLE_HEADER_NAME.length();
        int responseLen = response.length();
        int lineStart = response.indexOf("\r\n");
        while (lineStart >= 0 && lineStart + 2 + headerLen <= responseLen) {
            int hStart = lineStart + 2;
            if (response.regionMatches(true, hStart, QUESTDB_ROLE_HEADER_NAME, 0, headerLen)) {
                int valueStart = hStart + headerLen;
                int lineEnd = response.indexOf('\r', valueStart);
                if (lineEnd < 0) {
                    lineEnd = responseLen;
                }
                String value = response.substring(valueStart, lineEnd).trim();
                return value.isEmpty() ? null : value;
            }
            lineStart = response.indexOf("\r\n", hStart);
        }
        return null;
    }

    private static String extractZoneHeader(String response) {
        int headerLen = QUESTDB_ZONE_HEADER_NAME.length();
        int responseLen = response.length();
        int lineStart = response.indexOf("\r\n");
        while (lineStart >= 0 && lineStart + 2 + headerLen <= responseLen) {
            int hStart = lineStart + 2;
            if (response.regionMatches(true, hStart, QUESTDB_ZONE_HEADER_NAME, 0, headerLen)) {
                int valueStart = hStart + headerLen;
                int lineEnd = response.indexOf('\r', valueStart);
                if (lineEnd < 0) {
                    lineEnd = responseLen;
                }
                String value = response.substring(valueStart, lineEnd).trim();
                return value.isEmpty() ? null : value;
            }
            lineStart = response.indexOf("\r\n", hStart);
        }
        return null;
    }

    private static int parseStatusCode(String statusLine) {
        int sp1 = statusLine.indexOf(' ');
        if (sp1 < 0 || sp1 + 4 > statusLine.length()) return 0;
        if (sp1 + 4 < statusLine.length()) {
            char afterCode = statusLine.charAt(sp1 + 4);
            if (afterCode != ' ' && afterCode != '\r' && afterCode != '\n') {
                return 0;
            }
        }
        int code = 0;
        for (int i = sp1 + 1; i < sp1 + 4; i++) {
            char c = statusLine.charAt(i);
            if (c < '0' || c > '9') return 0;
            code = code * 10 + (c - '0');
        }
        return code;
    }

    private static int remainingTime(int timeoutMillis, long startTimeNanos) {
        return timeoutMillis - (int) NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
    }

    private void appendToFragmentBuffer(long payloadPtr, int payloadLen) {
        if (payloadLen == 0) {
            return;
        }
        long required = (long) fragmentBufPos + payloadLen;
        if (required > maxRecvBufSize) {
            throw new HttpClientException("WebSocket fragment buffer size exceeded maximum [required=")
                    .put(required)
                    .put(", max=")
                    .put(maxRecvBufSize)
                    .put(']');
        }
        if (fragmentBufPtr == 0) {
            fragmentBufSize = (int) Math.max(required, DEFAULT_RECV_BUFFER_SIZE);
            fragmentBufPtr = Unsafe.malloc(fragmentBufSize, MemoryTag.NATIVE_DEFAULT);
        } else if (required > fragmentBufSize) {
            int newSize = (int) Math.min(Math.max((long) fragmentBufSize * 2, required), maxRecvBufSize);
            fragmentBufPtr = Unsafe.realloc(fragmentBufPtr, fragmentBufSize, newSize, MemoryTag.NATIVE_DEFAULT);
            fragmentBufSize = newSize;
        }
        Vect.memmove(fragmentBufPtr + fragmentBufPos, payloadPtr, payloadLen);
        fragmentBufPos += payloadLen;
    }

    private void checkConnected() {
        if (closed.get()) {
            throw new HttpClientException("WebSocket client is closed");
        }
        if (!upgraded) {
            throw new HttpClientException("WebSocket not connected or upgraded");
        }
    }

    private void compactRecvBuffer() {
        if (recvReadPos > 0) {
            int remaining = recvPos - recvReadPos;
            // recvPos >= recvReadPos always holds here: a handler-initiated
            // close() (which zeroes recvPos under our feet) is caught in
            // tryParseFrame's tail before this method is reached. If this
            // assert fires, someone reintroduced a post-callback touch of
            // recv state on a closed/disconnected client.
            assert remaining >= 0 : "recv buffer positions out of order [recvPos=" + recvPos + ", recvReadPos=" + recvReadPos + ']';
            if (remaining > 0) {
                Vect.memmove(recvBufPtr, recvBufPtr + recvReadPos, remaining);
            }
            recvPos = remaining;
            recvReadPos = 0;
        }
    }

    private int dieIfNegative(int byteCount) {
        if (byteCount < 0) {
            throw new HttpClientException("peer disconnect [errno=").errno(nf.errno()).put(']');
        }
        return byteCount;
    }

    private void doConnect(CharSequence host, int port) {
        int fd = nf.socketTcp(true);
        if (fd < 0) {
            throw new HttpClientException("could not allocate a file descriptor [errno=").errno(nf.errno()).put(']');
        }

        if (nf.setTcpNoDelay(fd, true) < 0) {
            LOG.info("could not disable Nagle's algorithm [fd={}, errno={}]", fd, nf.errno());
        }

        socket.of(fd);
        nf.configureKeepAlive(fd);

        long addrInfo = nf.getAddrInfo(host, port);
        if (addrInfo == -1) {
            disconnect();
            throw new HttpClientException("could not resolve host [host=").put(host).put(']');
        }

        final int connectResult = connectTimeoutMillis > 0
                ? nf.connectAddrInfoTimeout(fd, addrInfo, connectTimeoutMillis)
                : nf.connectAddrInfo(fd, addrInfo);
        if (connectResult != 0) {
            int errno = nf.errno();
            nf.freeAddrInfo(addrInfo);
            disconnect();
            if (connectResult == NetworkFacade.CONNECT_TIMEOUT) {
                throw new HttpClientException("connect timed out [host=").put(host)
                        .put(", port=").put(port)
                        .put(", timeout=").put(connectTimeoutMillis).put(']').flagAsTimeout();
            }
            throw new HttpClientException("could not connect [host=").put(host)
                    .put(", port=").put(port)
                    .put(", errno=").put(errno).put(']');
        }
        nf.freeAddrInfo(addrInfo);

        if (nf.configureNonBlocking(fd) < 0) {
            int errno = nf.errno();
            disconnect();
            throw new HttpClientException("could not configure non-blocking [fd=").put(fd)
                    .put(", errno=").put(errno).put(']');
        }

        // Register the fd with the event loop before the TLS handshake so the
        // handshake can park on socket readiness via ioWait() instead of
        // busy-spinning on the non-blocking socket.
        setupIoWait();

        if (socket.supportsTls()) {
            // Bound the TLS handshake by the connect budget (falling back to the
            // request timeout when connect_timeout is unset), so a peer that
            // completes TCP but stalls mid-handshake cannot hang or pin a CPU.
            final long tlsHandshakeStartNanos = System.nanoTime();
            final int tlsHandshakeBudgetMillis = connectTimeoutMillis > 0 ? connectTimeoutMillis : defaultTimeout;
            try {
                socket.startTlsSession(host, op -> ioWait(getRemainingTimeOrThrow(tlsHandshakeBudgetMillis, tlsHandshakeStartNanos), op));
            } catch (TlsSessionInitFailedException e) {
                int errno = nf.errno();
                disconnect();
                throw new HttpClientException("could not start TLS session [fd=").put(fd)
                        .put(", error=").put(e.getFlyweightMessage())
                        .put(", errno=").put(errno).put(']');
            } catch (Throwable t) {
                // ioWait() throws a timeout-flagged HttpClientException when the
                // handshake budget is exhausted; any other error can also surface
                // mid-handshake. Disconnect so the fd and native buffers do not
                // leak, then propagate.
                disconnect();
                throw t;
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Connected to [host={}, port={}]", host, port);
        }
    }

    private void doSend(long ptr, int len, int timeout) {
        long startTime = System.nanoTime();
        while (len > 0) {
            int remainingTimeout = getRemainingTimeOrThrow(timeout, startTime);
            ioWait(remainingTimeout, IOOperation.WRITE);
            int sent = dieIfNegative(socket.send(ptr, len));
            while (socket.wantsTlsWrite()) {
                remainingTimeout = getRemainingTimeOrThrow(timeout, startTime);
                ioWait(remainingTimeout, IOOperation.WRITE);
                dieIfNegative(socket.tlsIO(Socket.WRITE_FLAG));
            }
            if (sent > 0) {
                ptr += sent;
                len -= sent;
            }
        }
    }

    private int findHeaderEnd() {
        // Look for \r\n\r\n
        for (int i = 0; i < recvPos - 3; i++) {
            if (Unsafe.getUnsafe().getByte(recvBufPtr + i) == '\r' &&
                    Unsafe.getUnsafe().getByte(recvBufPtr + i + 1) == '\n' &&
                    Unsafe.getUnsafe().getByte(recvBufPtr + i + 2) == '\r' &&
                    Unsafe.getUnsafe().getByte(recvBufPtr + i + 3) == '\n') {
                return i + 4;
            }
        }
        return -1;
    }

    private int getRemainingTimeOrThrow(int timeoutMillis, long startTimeNanos) {
        int remaining = remainingTime(timeoutMillis, startTimeNanos);
        if (remaining <= 0) {
            throw new HttpClientException("timed out [errno=").errno(nf.errno()).put(']').flagAsTimeout();
        }
        return remaining;
    }

    private void growRecvBuffer() {
        int newSize = (int) Math.min((long) recvBufSize * 2, maxRecvBufSize);
        if (newSize >= maxRecvBufSize) {
            if (recvBufSize >= maxRecvBufSize) {
                throw new HttpClientException("WebSocket receive buffer size exceeded maximum [current=")
                        .put(recvBufSize)
                        .put(", max=")
                        .put(maxRecvBufSize)
                        .put(']');
            }
            newSize = maxRecvBufSize;
        }
        recvBufPtr = Unsafe.realloc(recvBufPtr, recvBufSize, newSize, MemoryTag.NATIVE_DEFAULT);
        recvBufSize = newSize;
    }

    private void readUpgradeResponse(int timeout) {
        // Read HTTP response into receive buffer
        long startTime = System.nanoTime();

        while (true) {
            int remainingTimeout = getRemainingTimeOrThrow(timeout, startTime);
            int bytesRead = recvOrDie(recvBufPtr + recvPos, recvBufSize - recvPos, remainingTimeout);
            if (bytesRead > 0) {
                recvPos += bytesRead;
            }

            // Check for end of headers (\r\n\r\n)
            int headerEnd = findHeaderEnd();
            if (headerEnd > 0) {
                validateUpgradeResponse(headerEnd);
                // Compact buffer - move remaining data to start
                int remaining = recvPos - headerEnd;
                if (remaining > 0) {
                    Vect.memmove(recvBufPtr, recvBufPtr + headerEnd, remaining);
                }
                recvPos = remaining;
                recvReadPos = 0;
                return;
            }

            if (recvPos >= recvBufSize) {
                throw new HttpClientException("HTTP response too large");
            }
        }
    }

    private int recvOrDie(long ptr, int len, int timeout) {
        long startTime = System.nanoTime();
        int n = dieIfNegative(socket.recv(ptr, len));
        if (n == 0) {
            ioWait(getRemainingTimeOrThrow(timeout, startTime), IOOperation.READ);
            n = dieIfNegative(socket.recv(ptr, len));
        }
        return n;
    }

    private int recvOrTimeout(long ptr, int len, int timeout) {
        int n = socket.recv(ptr, len);
        if (n < 0) {
            throw new HttpClientException("peer disconnect [errno=").errno(nf.errno()).put(']');
        }
        if (n == 0) {
            try {
                ioWait(timeout, IOOperation.READ);
            } catch (HttpClientException e) {
                if (!e.isTimeout()) {
                    throw e;
                }
                return 0;
            }
            n = socket.recv(ptr, len);
            if (n < 0) {
                throw new HttpClientException("peer disconnect [errno=").errno(nf.errno()).put(']');
            }
        }
        return n;
    }

    private void resetFragmentState() {
        fragmentOpcode = -1;
        fragmentBufPos = 0;
    }

    private void sendCloseFrameEcho(int code) {
        try {
            controlFrameBuffer.reset();
            WebSocketSendBuffer.FrameInfo frame = controlFrameBuffer.writeCloseFrame(code, null);
            doSend(controlFrameBuffer.getBufferPtr() + frame.offset, frame.length, 1000);
            controlFrameBuffer.reset();
        } catch (Exception e) {
            LOG.error("Failed to echo close frame: {}", e.getMessage());
        }
    }

    private void sendPongFrame(long payloadPtr, int payloadLen) {
        try {
            controlFrameBuffer.reset();
            WebSocketSendBuffer.FrameInfo frame = controlFrameBuffer.writePongFrame(payloadPtr, payloadLen);
            doSend(controlFrameBuffer.getBufferPtr() + frame.offset, frame.length, 1000);
            controlFrameBuffer.reset();
        } catch (Exception e) {
            LOG.error("Failed to send pong: {}", e.getMessage());
        }
    }

    private int tryParseFrame(WebSocketFrameHandler handler) {
        if (recvPos <= recvReadPos) {
            return PARSE_NEED_MORE;
        }

        frameParser.reset();
        int consumed = frameParser.parse(recvBufPtr + recvReadPos, recvBufPtr + recvPos);

        if (frameParser.getState() == WebSocketFrameParser.STATE_NEED_MORE ||
                frameParser.getState() == WebSocketFrameParser.STATE_NEED_PAYLOAD) {
            return PARSE_NEED_MORE;
        }

        if (frameParser.getState() == WebSocketFrameParser.STATE_ERROR) {
            int errorCode = frameParser.getErrorCode();
            try {
                sendCloseFrame(errorCode, null, 1000);
            } catch (Exception e) {
                // Best-effort close frame before disconnect
            }
            throw new HttpClientException("WebSocket frame parse error: ")
                    .put(WebSocketCloseCode.describe(errorCode));
        }

        if (frameParser.getState() == WebSocketFrameParser.STATE_COMPLETE) {
            long payloadPtr = recvBufPtr + recvReadPos + frameParser.getHeaderSize();
            long payloadLength = frameParser.getPayloadLength();
            if (payloadLength > Integer.MAX_VALUE) {
                throw new HttpClientException("WebSocket frame payload too large [length=")
                        .put(payloadLength).put(']');
            }
            int payloadLen = (int) payloadLength;

            // Handle frame by opcode
            int opcode = frameParser.getOpcode();
            switch (opcode) {
                case WebSocketOpcode.PING:
                    // Auto-respond with pong
                    sendPongFrame(payloadPtr, payloadLen);
                    if (handler != null) {
                        handler.onPing(payloadPtr, payloadLen);
                    }
                    break;
                case WebSocketOpcode.PONG:
                    if (handler != null) {
                        handler.onPong(payloadPtr, payloadLen);
                    }
                    break;
                case WebSocketOpcode.CLOSE:
                    int closeCode = 0;
                    String reason = null;
                    if (payloadLen >= 2) {
                        closeCode = ((Unsafe.getUnsafe().getByte(payloadPtr) & 0xFF) << 8)
                                | (Unsafe.getUnsafe().getByte(payloadPtr + 1) & 0xFF);
                        if (payloadLen > 2) {
                            byte[] reasonBytes = new byte[payloadLen - 2];
                            for (int i = 0; i < reasonBytes.length; i++) {
                                reasonBytes[i] = Unsafe.getUnsafe().getByte(payloadPtr + 2 + i);
                            }
                            reason = new String(reasonBytes, StandardCharsets.UTF_8);
                        }
                    }
                    // RFC 6455 Section 5.5.1: echo a close frame back before
                    // marking the connection as no longer upgraded
                    sendCloseFrameEcho(closeCode);
                    upgraded = false;
                    if (handler != null) {
                        handler.onClose(closeCode, reason);
                    }
                    break;
                case WebSocketOpcode.BINARY:
                case WebSocketOpcode.TEXT:
                    if (frameParser.isFin()) {
                        if (fragmentOpcode != -1) {
                            throw new HttpClientException("WebSocket protocol error: new data frame during fragmented message");
                        }
                        if (handler != null) {
                            if (opcode == WebSocketOpcode.BINARY) {
                                handler.onBinaryMessage(payloadPtr, payloadLen);
                            } else {
                                handler.onTextMessage(payloadPtr, payloadLen);
                            }
                        }
                    } else {
                        if (fragmentOpcode != -1) {
                            throw new HttpClientException("WebSocket protocol error: new data frame during fragmented message");
                        }
                        fragmentOpcode = opcode;
                        appendToFragmentBuffer(payloadPtr, payloadLen);
                    }
                    break;
                case WebSocketOpcode.CONTINUATION:
                    if (fragmentOpcode == -1) {
                        throw new HttpClientException("WebSocket protocol error: continuation frame without initial fragment");
                    }
                    appendToFragmentBuffer(payloadPtr, payloadLen);
                    if (frameParser.isFin()) {
                        if (handler != null) {
                            if (fragmentOpcode == WebSocketOpcode.BINARY) {
                                handler.onBinaryMessage(fragmentBufPtr, fragmentBufPos);
                            } else {
                                handler.onTextMessage(fragmentBufPtr, fragmentBufPos);
                            }
                        }
                        resetFragmentState();
                    }
                    break;
            }

            // A handler callback above may have close()d this client:
            // CursorWebSocketSendLoop's NACK/close recycle swaps in a new
            // client and synchronously closes this one (swapClient), then
            // control unwinds back here. close() -> disconnect() has already
            // zeroed recvPos/recvReadPos and freed recvBufPtr -- advancing
            // the read position or compacting would corrupt that state
            // (negative recvPos today; a memmove on freed memory if the
            // zeroing ever moved). The frame was fully dispatched, so
            // in-callback close is a supported contract: report success
            // and touch nothing. Same-thread, so the closed read is exact.
            if (closed.get()) {
                return PARSE_OK;
            }

            // Advance read position
            recvReadPos += consumed;

            // Compact buffer if needed
            compactRecvBuffer();

            return PARSE_OK;
        }

        return PARSE_INCOMPLETE;
    }

    private void validateUpgradeResponse(int headerEnd) {
        // Extract response as string for parsing
        byte[] responseBytes = new byte[headerEnd];
        for (int i = 0; i < headerEnd; i++) {
            responseBytes[i] = Unsafe.getUnsafe().getByte(recvBufPtr + i);
        }
        String response = new String(responseBytes, StandardCharsets.US_ASCII);

        if (!response.startsWith("HTTP/1.1 101")) {
            String statusLine = response.split("\r\n")[0];
            upgradeStatusCode = parseStatusCode(statusLine);
            if (upgradeStatusCode == 421) {
                upgradeRejectRole = extractRoleHeader(response);
                upgradeRejectZone = extractZoneHeader(response);
            }
            WebSocketUpgradeException ex = new WebSocketUpgradeException(
                    upgradeStatusCode, upgradeRejectRole, "WebSocket upgrade failed: ");
            ex.put(statusLine);
            throw ex;
        }

        // Verify Upgrade: websocket (case-insensitive value per RFC 6455 Section 4.1)
        if (excludesHeaderValue(response, "Upgrade:", "websocket", true)) {
            throw new HttpClientException("Missing or invalid Upgrade header in WebSocket response");
        }

        // Verify Connection: Upgrade (case-insensitive value per RFC 6455 Section 4.1)
        if (excludesHeaderValue(response, "Connection:", "Upgrade", true)) {
            throw new HttpClientException("Missing or invalid Connection header in WebSocket response");
        }

        // Verify Sec-WebSocket-Accept (exact value match per RFC 6455 Section 4.1)
        String expectedAccept = computeAcceptKey(handshakeKey);
        if (excludesHeaderValue(response, "Sec-WebSocket-Accept:", expectedAccept, false)) {
            throw new HttpClientException("Invalid Sec-WebSocket-Accept header");
        }

        // Extract X-QWP-Version (optional, defaults to 1 if absent).
        // Reject a server-advertised version outside [1, qwpMaxVersion]: this
        // is per-endpoint, not cluster-wide (a rolling upgrade can leave one
        // node ahead of or behind its peers), so the connect loop classifies
        // it as a transport error and walks to the next host. The typed
        // QwpVersionMismatchException lets the cursor send loop's terminal
        // classifier match by instanceof rather than message sniffing; see
        // failover.md §6.
        serverQwpVersion = extractQwpVersion(response);
        if (serverQwpVersion < 1 || serverQwpVersion > qwpMaxVersion) {
            throw new QwpVersionMismatchException(serverQwpVersion, qwpMaxVersion);
        }

        // Extract X-QWP-Durable-Ack confirmation (optional, absent on servers
        // without primary replication or when the client did not opt in).
        // Only meaningful when qwpRequestDurableAck is true; the sender
        // checks this value to fail at connect rather than silently
        // missing trim signals.
        serverDurableAckEnabled = extractDurableAckEnabled(response);

        // Extract X-QWP-Max-Batch-Size (optional). Older servers omit it; the
        // sender falls back to its locally configured byte budget in that case.
        serverMaxBatchSize = extractMaxBatchSize(response);

        // Extract X-QWP-Content-Encoding (optional). Surfaces what level the
        // server actually applied -- which may differ from what this client
        // asked for if the server has qwp.egress.compression.force.level set
        // or the server clamped a client request outside the wire range.
        // 0 means "no zstd" (raw transport, or header absent on older
        // servers); non-zero is the applied level.
        serverNegotiatedZstdLevel = extractContentEncodingZstdLevel(response);
    }

    protected void dieWaiting(int n) {
        if (n == 1) {
            return;
        }
        if (n == 0) {
            throw new HttpClientException("timed out [errno=").put(nf.errno()).put(']').flagAsTimeout();
        }
        throw new HttpClientException("queue error [errno=").put(nf.errno()).put(']');
    }

    /**
     * Waits for I/O readiness using platform-specific mechanism.
     *
     * @param timeout timeout in milliseconds
     * @param op      I/O operation (READ or WRITE)
     */
    protected abstract void ioWait(int timeout, int op);

    /**
     * Sets up platform-specific I/O wait mechanism after connection.
     */
    protected abstract void setupIoWait();
}
