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

package io.questdb.client.cutlass.qwp.client;

import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.impl.ConfStringParser;
import io.questdb.client.std.Chars;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.str.StringSink;

/**
 * QWP egress (query results) client.
 * <p>
 * Connection shape: one WebSocket to {@code /read/v1}, one dedicated I/O thread
 * that owns the socket and the decoder. The user thread submits a query and
 * drains result batches via the supplied {@link QwpColumnBatchHandler}; the
 * I/O thread reads and decodes ahead so that decoding of batch {@code N+1}
 * overlaps with the user's processing of batch {@code N}.
 * <p>
 * Thread safety: not thread-safe for concurrent queries on the same client.
 * One {@link #execute} at a time. Opening one client per query-issuing thread
 * is the recommended pattern.
 */
public class QwpQueryClient implements QuietCloseable {

    public static final String DEFAULT_ENDPOINT_PATH = "/read/v1";
    public static final int DEFAULT_WS_PORT = 9000;
    public static final int QWP_MAX_VERSION = 1;
    private static final int DEFAULT_IO_BUFFER_POOL_SIZE = 4;
    /**
     * Maximum time {@link #close()} will wait for the I/O thread to exit before giving up
     * and leaking the (daemon) thread + its native buffer pool + WebSocket socket. 5 seconds
     * is generous given the I/O thread polls on a 100 ms cadence; if it overshoots this,
     * something is seriously wrong (e.g., user handler stuck in onBatch).
     */
    private static final long SHUTDOWN_JOIN_MS = 5_000;
    private final CharSequence host;
    private final int port;
    private String authorizationHeader;
    private int bufferPoolSize = DEFAULT_IO_BUFFER_POOL_SIZE;
    private String clientId;
    private boolean connected;
    // Written on the user thread at entry to {@link #execute} and cleared on exit.
    // Read by {@link #cancel} from any thread. {@code volatile} to guarantee the
    // user thread's write is visible to a concurrent cancel caller; 64-bit writes
    // are atomic under {@code volatile long}.
    private volatile long currentRequestId = -1L;
    private String endpointPath = DEFAULT_ENDPOINT_PATH;
    // Credit-flow send-ahead budget. 0 = unbounded (Phase-1 default, no CREDIT
    // bookkeeping on either side). A positive value puts the stream under byte-
    // based flow control: the server emits at most this many bytes of result
    // payload before it parks, and the client auto-replenishes by the size of
    // each batch as the user releases it.
    private long initialCreditBytes;
    private QwpEgressIoThread ioThread;
    private Thread ioThreadHandle;
    private boolean lastCloseTimedOut;
    private int negotiatedQwpVersion;
    private long nextRequestId = 1;
    private WebSocketClient webSocketClient;

    private QwpQueryClient(CharSequence host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Builds a query client from a connection-string of the same shape used by
     * {@link io.questdb.client.Sender#fromConfig(CharSequence)}: {@code <schema>::key=value;key=value;...}.
     * <p>
     * Supported schemas:
     * <ul>
     *   <li>{@code ws::} -- plain WebSocket (matches QWP egress today; TLS not yet supported).</li>
     * </ul>
     * Supported keys:
     * <ul>
     *   <li>{@code addr=host[:port]} -- required. Default port is {@value #DEFAULT_WS_PORT}.</li>
     *   <li>{@code path=/read/v1} -- egress endpoint. Default {@value #DEFAULT_ENDPOINT_PATH}.</li>
     *   <li>{@code auth=<value>} -- sent as the HTTP {@code Authorization} header during the upgrade handshake.</li>
     *   <li>{@code client_id=<id>} -- sent as the {@code X-QWP-Client-Id} header.</li>
     *   <li>{@code buffer_pool_size=N} -- depth of the I/O thread's batch buffer pool. Default 4.</li>
     * </ul>
     * Examples:
     * <pre>
     *   ws::addr=localhost:9000;
     *   ws::addr=db.internal:9000;path=/read/v1;auth=Bearer abc123;client_id=dashboard/2.0;
     * </pre>
     */
    public static QwpQueryClient fromConfig(CharSequence configurationString) {
        if (configurationString == null || configurationString.length() == 0) {
            throw new IllegalArgumentException("configuration string cannot be empty");
        }
        StringSink sink = new StringSink();
        int pos = ConfStringParser.of(configurationString, sink);
        if (pos < 0) {
            throw new IllegalArgumentException("invalid configuration string: " + sink);
        }
        if (Chars.equals("wss", sink)) {
            throw new IllegalArgumentException("wss:: (TLS) is not supported by QwpQueryClient yet");
        }
        if (!Chars.equals("ws", sink)) {
            throw new IllegalArgumentException(
                    "unsupported schema [schema=" + sink + ", supported-schemas=[ws]]");
        }

        String addrHost = null;
        int addrPort = DEFAULT_WS_PORT;
        String path = DEFAULT_ENDPOINT_PATH;
        String auth = null;
        String cid = null;
        int poolSize = DEFAULT_IO_BUFFER_POOL_SIZE;

        while (ConfStringParser.hasNext(configurationString, pos)) {
            pos = ConfStringParser.nextKey(configurationString, pos, sink);
            if (pos < 0) {
                throw new IllegalArgumentException("invalid configuration string [error=" + sink + "]");
            }
            String key = sink.toString();
            pos = ConfStringParser.value(configurationString, pos, sink);
            if (pos < 0) {
                throw new IllegalArgumentException("invalid configuration string [error=" + sink + "]");
            }
            String value = sink.toString();
            switch (key) {
                case "addr": {
                    int colon = value.indexOf(':');
                    if (colon < 0) {
                        addrHost = value;
                    } else {
                        addrHost = value.substring(0, colon);
                        try {
                            addrPort = Integer.parseInt(value.substring(colon + 1));
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("invalid port in addr: " + value);
                        }
                    }
                    break;
                }
                case "path":
                    path = value;
                    break;
                case "auth":
                    auth = value;
                    break;
                case "client_id":
                    cid = value;
                    break;
                case "buffer_pool_size":
                    try {
                        poolSize = Integer.parseInt(value);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("invalid buffer_pool_size: " + value);
                    }
                    if (poolSize < 1) {
                        throw new IllegalArgumentException("buffer_pool_size must be >= 1");
                    }
                    break;
                default:
                    throw new IllegalArgumentException("unknown configuration key: " + key);
            }
        }
        if (addrHost == null) {
            throw new IllegalArgumentException("missing required key: addr");
        }
        QwpQueryClient client = new QwpQueryClient(addrHost, addrPort)
                .withEndpointPath(path)
                .withBufferPoolSize(poolSize);
        if (auth != null) client.withAuthorization(auth);
        if (cid != null) client.withClientId(cid);
        return client;
    }

    /**
     * Creates a plain-text (non-TLS) QWP query client.
     */
    public static QwpQueryClient newPlainText(CharSequence host, int port) {
        return new QwpQueryClient(host, port);
    }

    /**
     * Asks the server to cancel the currently executing query. No-op if no query
     * is in flight. Safe to call from a thread other than the one blocked inside
     * {@link #execute}. The server replies to the active query with a
     * {@code QUERY_ERROR} whose status byte is {@code STATUS_CANCELLED}; the
     * handler's {@code onError} (on the execute-ing thread) will see it.
     */
    public void cancel() {
        QwpEgressIoThread io = ioThread;
        long id = currentRequestId;
        if (io != null && id >= 0L) {
            io.requestCancel(id);
        }
    }

    /**
     * Shutdown order: signal the I/O thread, interrupt it to wake it from any blocking
     * {@code wsClient.receiveFrame(...)} or queue poll, wait for it to exit, then free
     * the buffer pool and close the underlying socket.
     * <p>
     * If the I/O thread fails to exit within {@link #SHUTDOWN_JOIN_MS} (default 5 s), this
     * method does <em>not</em> free the buffer pool or close the WebSocket -- both are
     * still in use by the thread, and freeing them would race into a JVM-killing
     * use-after-free. The thread is a daemon, so the JVM still exits normally; the
     * resources leak for the lifetime of the process. A warning is recorded by setting
     * {@link #lastCloseTimedOut} (queryable via {@link #wasLastCloseTimedOut}) so callers
     * can detect and report the condition.
     */
    @Override
    public void close() {
        connected = false;
        lastCloseTimedOut = false;
        if (ioThread != null) {
            ioThread.shutdown();
            // Wake the thread from any blocking poll / recv so it sees the shutdown flag promptly.
            if (ioThreadHandle != null) {
                ioThreadHandle.interrupt();
                boolean joined;
                try {
                    ioThreadHandle.join(SHUTDOWN_JOIN_MS);
                    joined = !ioThreadHandle.isAlive();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // Don't free anything -- preserve clean shutdown semantics on the next attempt.
                    return;
                }
                if (!joined) {
                    // Daemon thread is still running -- buffer pool and WebSocketClient may
                    // be in use. Leak them rather than risk a SIGSEGV by freeing under it.
                    lastCloseTimedOut = true;
                    ioThread = null;
                    ioThreadHandle = null;
                    webSocketClient = null;
                    return;
                }
            }
            ioThread.closePool();
            ioThread = null;
            ioThreadHandle = null;
        }
        if (webSocketClient != null) {
            webSocketClient.close();
            webSocketClient = null;
        }
    }

    /**
     * Opens the TCP connection, performs the WebSocket upgrade, and spawns the I/O thread.
     * Must be called before any query is submitted.
     */
    public void connect() {
        if (connected) {
            return;
        }
        webSocketClient = WebSocketClientFactory.newPlainTextInstance();
        webSocketClient.setQwpMaxVersion(QWP_MAX_VERSION);
        webSocketClient.setQwpClientId(clientId != null ? clientId : defaultClientId());
        webSocketClient.connect(host, port);
        webSocketClient.upgrade(endpointPath, authorizationHeader);
        negotiatedQwpVersion = webSocketClient.getServerQwpVersion();

        ioThread = new QwpEgressIoThread(webSocketClient, bufferPoolSize);
        ioThreadHandle = new Thread(ioThread, "qwp-egress-io");
        ioThreadHandle.setDaemon(true);
        ioThreadHandle.start();
        connected = true;
    }

    /**
     * Executes {@code sql} and drives the supplied handler through the result stream.
     * <p>
     * Blocks the calling thread until the server sends {@code RESULT_END} or
     * {@code QUERY_ERROR}. While the user thread is inside {@code handler.onBatch},
     * the I/O thread keeps reading and decoding ahead up to the configured buffer-pool depth.
     */
    public void execute(String sql, QwpColumnBatchHandler handler) {
        if (!connected) {
            throw new IllegalStateException("QwpQueryClient not connected; call connect() first");
        }
        // Cache the I/O thread reference at entry: close() may null the field while
        // we are inside this loop, so reading the field per-iteration would NPE
        // exactly when the user is mid-execute() and close() races. The queue and
        // pool the cached reference owns are still drained safely by closePool()
        // before close() returns.
        QwpEgressIoThread io = ioThread;
        if (io == null) {
            handler.onError((byte) 0, "QwpQueryClient is closed");
            return;
        }
        long requestId = nextRequestId++;
        currentRequestId = requestId;
        try {
            io.submitQuery(sql, requestId, initialCreditBytes);
            while (true) {
                QueryEvent ev = io.takeEvent();
                switch (ev.kind) {
                    case QueryEvent.KIND_BATCH:
                        try {
                            handler.onBatch(ev.buffer.batch);
                        } finally {
                            io.releaseBuffer(ev.buffer);
                        }
                        break;
                    case QueryEvent.KIND_END:
                        handler.onEnd(ev.totalRows);
                        return;
                    case QueryEvent.KIND_EXEC_DONE:
                        handler.onExecDone(ev.opType, ev.rowsAffected);
                        return;
                    case QueryEvent.KIND_ERROR:
                        handler.onError(ev.errorStatus, ev.errorMessage);
                        return;
                    default:
                        handler.onError((byte) 0, "unknown event kind " + ev.kind);
                        return;
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            handler.onError((byte) 0, "interrupted while waiting for server response");
        } finally {
            currentRequestId = -1L;
        }
    }

    public int getNegotiatedQwpVersion() {
        return negotiatedQwpVersion;
    }

    public boolean isConnected() {
        return connected;
    }

    /**
     * Returns true if the most recent {@link #close()} call abandoned the I/O thread
     * because it failed to exit within the join timeout. The native buffer pool and
     * WebSocket socket are leaked for the lifetime of the JVM; the daemon I/O thread
     * keeps running until process exit.
     */
    public boolean wasLastCloseTimedOut() {
        return lastCloseTimedOut;
    }

    public QwpQueryClient withAuthorization(String authorizationHeader) {
        this.authorizationHeader = authorizationHeader;
        return this;
    }

    /**
     * Overrides the default I/O buffer pool depth (4). Larger pools let the
     * I/O thread decode further ahead of the consumer at the cost of memory;
     * smaller pools reduce memory but may stall the I/O thread on slow consumers.
     * Must be called before {@link #connect()}.
     */
    public QwpQueryClient withBufferPoolSize(int size) {
        if (size < 1) throw new IllegalArgumentException("bufferPoolSize must be >= 1");
        this.bufferPoolSize = size;
        return this;
    }

    /**
     * Overrides the {@code X-QWP-Client-Id} header sent during the upgrade handshake.
     * Must be called before {@link #connect()}.
     */
    public QwpQueryClient withClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public QwpQueryClient withEndpointPath(String endpointPath) {
        this.endpointPath = endpointPath;
        return this;
    }

    /**
     * Opts the next {@link #execute} into credit-based flow control with
     * {@code bytes} of initial send-ahead budget. The server streams at most
     * {@code bytes} of result payload before pausing; the client auto-
     * replenishes by the size of each batch after the user's handler releases
     * it. Passing {@code 0} (the default) disables flow control entirely
     * (unbounded -- Phase-1 behaviour).
     * <p>
     * Must be called before {@link #connect}.
     */
    public QwpQueryClient withInitialCredit(long bytes) {
        if (bytes < 0) throw new IllegalArgumentException("initial credit must be >= 0");
        this.initialCreditBytes = bytes;
        return this;
    }

    private static String defaultClientId() {
        return "questdb-java-egress/1.0.0";
    }
}
