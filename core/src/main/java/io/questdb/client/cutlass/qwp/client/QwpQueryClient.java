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

import io.questdb.client.ClientTlsConfiguration;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.impl.ConfStringParser;
import io.questdb.client.std.Chars;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Zstd;
import io.questdb.client.std.str.StringSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

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
    /**
     * Hard ceiling on {@link #withMaxBatchRows}. Matches the client decoder's
     * own {@code MAX_ROWS_PER_BATCH} safety cap so a user cannot ask for a
     * per-batch row count the decoder would itself refuse. The server enforces
     * its own (typically smaller) cap independently; this is just the client's
     * sanity bound.
     */
    public static final int MAX_BATCH_ROWS_UPPER_BOUND = 1_048_576;
    public static final int QWP_MAX_VERSION = 1;
    private static final int DEFAULT_IO_BUFFER_POOL_SIZE = 4;
    private static final Logger LOG = LoggerFactory.getLogger(QwpQueryClient.class);
    private final CharSequence host;
    private final int port;
    private String authorizationHeader;
    private int bufferPoolSize = DEFAULT_IO_BUFFER_POOL_SIZE;
    private String clientId;
    private int compressionLevel = 3;
    // User-facing compression preference from the connection string. "raw" is
    // the library default -- no compression, no handshake header, no server-
    // side CPU burn on payloads where the network isn't the bottleneck
    // (colocated clients, loopback). Clients wanting zstd must opt in via
    // {@code compression=zstd} (demands zstd) or {@code compression=auto}
    // (advertises zstd,raw and lets the server pick).
    private String compressionPreference = "raw";
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
    // Client preference for server-side per-batch row cap. 0 means "unset",
    // server uses its default. Set via {@code max_batch_rows=N} in the
    // connection string or {@link #withMaxBatchRows}. Smaller values give
    // streaming consumers earlier access to the first rows at the cost of
    // more per-batch overhead; larger values amortise fixed costs over more
    // rows. Server may clamp down to its own hard cap.
    private int maxBatchRows;
    // Volatile so a cancel() call from a thread other than the one that ran
    // connect() sees the published reference (and a concurrent null-out from
    // close() is observed without a stale-reference race). The thread-safety
    // contract documented on cancel() relies on this.
    private volatile QwpEgressIoThread ioThread;
    private Thread ioThreadHandle;
    private boolean lastCloseTimedOut;
    private int negotiatedQwpVersion;
    private long nextRequestId = 1;
    // Maximum time close() will wait for the I/O thread to exit before giving up
    // and leaking the (daemon) thread + its native buffer pool + WebSocket socket.
    // 5 seconds is generous given the I/O thread polls on a 100 ms cadence; if
    // it overshoots this, something is seriously wrong (e.g., user handler stuck
    // in onBatch). Volatile (not final) so tests can reflectively shorten it to
    // hit the timeout branch in under a second instead of spending five.
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long shutdownJoinMs = 5_000;
    private boolean tlsEnabled;
    // Only meaningful when tlsEnabled. Default is full validation against the JVM's trust store.
    private int tlsValidationMode = ClientTlsConfiguration.TLS_VALIDATION_MODE_FULL;
    private char[] trustStorePassword;
    private String trustStorePath;
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
     *   <li>{@code ws::} -- plain WebSocket.</li>
     *   <li>{@code wss::} -- WebSocket over TLS. See {@code tls_verify} / {@code tls_roots} /
     *       {@code tls_roots_password} for trust configuration.</li>
     * </ul>
     * Supported keys:
     * <ul>
     *   <li>{@code addr=host[:port]} -- required. Default port is {@value #DEFAULT_WS_PORT}.</li>
     *   <li>{@code path=/read/v1} -- egress endpoint. Default {@value #DEFAULT_ENDPOINT_PATH}.</li>
     *   <li>{@code auth=<value>} -- sent verbatim as the HTTP {@code Authorization} header during the upgrade handshake.
     *       Mutually exclusive with {@code username}/{@code password} and {@code token}.</li>
     *   <li>{@code username=<name>;password=<secret>} -- HTTP Basic authentication. Server verifies the credentials
     *       against the same user store the Postgres wire protocol uses, so a user created via
     *       {@code CREATE USER ... WITH PASSWORD ...} can log in unchanged.
     *       Both keys must be present together; mutually exclusive with {@code auth} and {@code token}.</li>
     *   <li>{@code token=<access_token>} -- HTTP Bearer authentication with an OIDC access token (sent as
     *       {@code Authorization: Bearer <token>}). Mutually exclusive with {@code auth} and
     *       {@code username}/{@code password}.</li>
     *   <li>{@code client_id=<id>} -- sent as the {@code X-QWP-Client-Id} header.</li>
     *   <li>{@code buffer_pool_size=N} -- depth of the I/O thread's batch buffer pool. Default 4.</li>
     *   <li>{@code compression=zstd|raw|auto} -- compression codec the client
     *       asks the server to use for RESULT_BATCH bodies. {@code auto}
     *       (default) advertises {@code zstd,raw} so the server picks zstd
     *       when it supports it and falls back to raw otherwise.</li>
     *   <li>{@code compression_level=N} -- zstd level hint, clamped server-side
     *       to [1, 9]. Default 3. Ignored when {@code compression=raw}.</li>
     *   <li>{@code tls_verify=on|unsafe_off} -- TLS certificate validation. Default is {@code on}.
     *       Only allowed with the {@code wss::} schema. {@code unsafe_off} disables hostname and
     *       certificate chain validation; use only for testing.</li>
     *   <li>{@code tls_roots=<path>} -- path to a custom trust store (PKCS12 or JKS). Must be
     *       paired with {@code tls_roots_password}. Only allowed with {@code wss::}.</li>
     *   <li>{@code tls_roots_password=<secret>} -- password for the custom trust store.</li>
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
        boolean tls;
        if (Chars.equals("ws", sink)) {
            tls = false;
        } else if (Chars.equals("wss", sink)) {
            tls = true;
        } else {
            throw new IllegalArgumentException(
                    "unsupported schema [schema=" + sink + ", supported-schemas=[ws, wss]]");
        }

        String addrHost = null;
        int addrPort = DEFAULT_WS_PORT;
        String path = DEFAULT_ENDPOINT_PATH;
        String auth = null;
        String username = null;
        String password = null;
        String token = null;
        String cid = null;
        int poolSize = DEFAULT_IO_BUFFER_POOL_SIZE;
        // Default matches the field initializer in QwpQueryClient: raw wire,
        // zstd opt-in.
        String compression = "raw";
        int compressionLevel = 3;
        int maxBatchRows = 0;  // 0 = omit header, server uses its default
        // TLS validation mode: null means "unset in config". Explicit values kick in only when tls is true.
        Integer tlsValidation = null;
        String tlsRoots = null;
        String tlsRootsPassword = null;

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
                case "username":
                    username = value;
                    break;
                case "password":
                    password = value;
                    break;
                case "token":
                    token = value;
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
                case "compression":
                    if (!"zstd".equals(value) && !"raw".equals(value) && !"auto".equals(value)) {
                        throw new IllegalArgumentException(
                                "unsupported compression: " + value + " (expected zstd, raw, or auto)");
                    }
                    compression = value;
                    break;
                case "compression_level":
                    try {
                        compressionLevel = Integer.parseInt(value);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("invalid compression_level: " + value);
                    }
                    if (compressionLevel < 1 || compressionLevel > 22) {
                        throw new IllegalArgumentException("compression_level must be in [1, 22]");
                    }
                    break;
                case "max_batch_rows":
                    try {
                        maxBatchRows = Integer.parseInt(value);
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("invalid max_batch_rows: " + value);
                    }
                    if (maxBatchRows < 1 || maxBatchRows > MAX_BATCH_ROWS_UPPER_BOUND) {
                        throw new IllegalArgumentException(
                                "max_batch_rows must be in [1, " + MAX_BATCH_ROWS_UPPER_BOUND + "]");
                    }
                    break;
                case "tls_verify":
                    if ("on".equals(value)) {
                        tlsValidation = ClientTlsConfiguration.TLS_VALIDATION_MODE_FULL;
                    } else if ("unsafe_off".equals(value)) {
                        tlsValidation = ClientTlsConfiguration.TLS_VALIDATION_MODE_NONE;
                    } else {
                        throw new IllegalArgumentException(
                                "invalid tls_verify: " + value + " (expected on or unsafe_off)");
                    }
                    break;
                case "tls_roots":
                    tlsRoots = value;
                    break;
                case "tls_roots_password":
                    tlsRootsPassword = value;
                    break;
                default:
                    throw new IllegalArgumentException("unknown configuration key: " + key);
            }
        }
        if (addrHost == null) {
            throw new IllegalArgumentException("missing required key: addr");
        }
        boolean hasBasic = username != null || password != null;
        if (hasBasic && (username == null || password == null)) {
            throw new IllegalArgumentException("both username and password must be provided together");
        }
        int authModesSet = (auth != null ? 1 : 0) + (hasBasic ? 1 : 0) + (token != null ? 1 : 0);
        if (authModesSet > 1) {
            throw new IllegalArgumentException(
                    "auth, username/password, and token are mutually exclusive");
        }
        if (!tls && (tlsValidation != null || tlsRoots != null || tlsRootsPassword != null)) {
            throw new IllegalArgumentException(
                    "tls_verify/tls_roots/tls_roots_password require the wss:: schema");
        }
        if ((tlsRoots == null) != (tlsRootsPassword == null)) {
            throw new IllegalArgumentException(
                    "tls_roots and tls_roots_password must be provided together");
        }
        QwpQueryClient client = new QwpQueryClient(addrHost, addrPort)
                .withEndpointPath(path)
                .withBufferPoolSize(poolSize)
                .withCompression(compression, compressionLevel);
        if (tls) {
            if (tlsRoots != null) {
                client.withTrustStore(tlsRoots, tlsRootsPassword.toCharArray());
            } else if (tlsValidation != null && tlsValidation == ClientTlsConfiguration.TLS_VALIDATION_MODE_NONE) {
                client.withInsecureTls();
            } else {
                client.withTls();
            }
        }
        if (auth != null) client.withAuthorization(auth);
        if (hasBasic) client.withBasicAuth(username, password);
        if (token != null) client.withBearerToken(token);
        if (cid != null) client.withClientId(cid);
        if (maxBatchRows > 0) client.withMaxBatchRows(maxBatchRows);
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
     * If the I/O thread fails to exit within {@link #shutdownJoinMs} (default 5 s), this
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
                    ioThreadHandle.join(shutdownJoinMs);
                    joined = !ioThreadHandle.isAlive();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // Don't free anything -- preserve clean shutdown semantics on the next attempt.
                    return;
                }
                if (!joined) {
                    // Daemon thread is still running -- buffer pool and WebSocketClient may
                    // be in use. Leak them rather than risk a SIGSEGV by freeing under it.
                    // Log at ERROR so operators notice; wasLastCloseTimedOut() is the
                    // programmatic hook for monitoring or tests that want to assert on it.
                    LOG.error("QwpQueryClient close timed out after {} ms; leaking I/O thread, "
                                    + "buffer pool, and WebSocket to avoid freeing them from under "
                                    + "a running daemon. Common cause: a batch handler that never "
                                    + "returns (e.g. blocking I/O or deadlock).",
                            shutdownJoinMs);
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
        if (tlsEnabled) {
            webSocketClient = WebSocketClientFactory.newTlsInstance(
                    new ClientTlsConfiguration(trustStorePath, trustStorePassword, tlsValidationMode));
        } else {
            webSocketClient = WebSocketClientFactory.newPlainTextInstance();
        }
        webSocketClient.setQwpMaxVersion(QWP_MAX_VERSION);
        webSocketClient.setQwpClientId(clientId != null ? clientId : defaultClientId());
        webSocketClient.setQwpAcceptEncoding(buildAcceptEncodingHeader());
        webSocketClient.setQwpMaxBatchRows(maxBatchRows);
        webSocketClient.connect(host, port);
        webSocketClient.upgrade(endpointPath, authorizationHeader);
        negotiatedQwpVersion = webSocketClient.getServerQwpVersion();

        // Early probe: if we told the server we can accept zstd, make sure the
        // bundled native library actually provides the decompression symbols
        // before we start accepting batches. Without this, a client jar built
        // without the zstd submodule would only discover the missing symbols
        // mid-stream when it hits the first FLAG_ZSTD frame, and the error
        // would surface as an opaque "I/O thread failure: ..." callback on the
        // user handler. Fail loud here instead so the cause is obvious.
        if (!"raw".equals(compressionPreference)) {
            probeZstdAvailable();
        }

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

    /**
     * Returns the current compression preference: one of {@code raw} (the
     * library default, no compression), {@code zstd} (demand zstd), or
     * {@code auto} (advertise zstd and raw, let the server pick). Useful for
     * introspection and for tests that pin the default.
     */
    public String getCompressionPreference() {
        return compressionPreference;
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
     * Configures HTTP Basic authentication for the WebSocket upgrade request.
     * The server verifies the credentials against the same user store the
     * Postgres wire protocol uses, so a user created via
     * {@code CREATE USER ... WITH PASSWORD ...} can authenticate here unchanged.
     * Must be called before {@link #connect}.
     */
    public QwpQueryClient withBasicAuth(String username, String password) {
        if (username == null || password == null) {
            throw new IllegalArgumentException("username and password must not be null");
        }
        String credentials = username + ":" + password;
        this.authorizationHeader = "Basic " + Base64.getEncoder()
                .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        return this;
    }

    /**
     * Configures HTTP Bearer authentication with an OIDC access token for the
     * WebSocket upgrade request. The server verifies the token via the
     * configured OIDC provider and resolves the principal (and any groups)
     * from the token's claims. Must be called before {@link #connect}.
     */
    public QwpQueryClient withBearerToken(String token) {
        if (token == null) {
            throw new IllegalArgumentException("token must not be null");
        }
        this.authorizationHeader = "Bearer " + token;
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

    /**
     * Programmatic equivalent of the {@code compression=} / {@code compression_level=}
     * connection-string keys. {@code preference} is one of {@code zstd},
     * {@code raw} (default), or {@code auto}. {@code level} is the zstd
     * compression level hint passed to the server; clamped server-side to
     * [1, 9]. Must be called before {@link #connect}.
     */
    public QwpQueryClient withCompression(String preference, int level) {
        if (!"zstd".equals(preference) && !"raw".equals(preference) && !"auto".equals(preference)) {
            throw new IllegalArgumentException(
                    "unsupported compression: " + preference + " (expected zstd, raw, or auto)");
        }
        if (level < 1 || level > 22) {
            throw new IllegalArgumentException("compression level must be in [1, 22]");
        }
        this.compressionPreference = preference;
        this.compressionLevel = level;
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

    /**
     * Enables TLS with certificate validation disabled. Intended for testing only --
     * production code should use {@link #withTls} or {@link #withTrustStore}.
     * Must be called before {@link #connect}.
     */
    public QwpQueryClient withInsecureTls() {
        this.tlsEnabled = true;
        this.tlsValidationMode = ClientTlsConfiguration.TLS_VALIDATION_MODE_NONE;
        this.trustStorePath = null;
        this.trustStorePassword = null;
        return this;
    }

    /**
     * Asks the server to cap each {@code RESULT_BATCH} at {@code rows} rows.
     * Useful for latency-sensitive streaming consumers that want to start
     * processing the first row as soon as possible -- a smaller cap flushes
     * the first batch sooner, at the cost of more per-batch overhead (WS
     * header, send syscall, schema-reference decode). The server clamps down
     * to its own hard limit; a value of {@code 0} (default) omits the header
     * and the server uses its own cap.
     * <p>
     * Must be called before {@link #connect}.
     */
    public QwpQueryClient withMaxBatchRows(int rows) {
        if (rows < 1 || rows > MAX_BATCH_ROWS_UPPER_BOUND) {
            throw new IllegalArgumentException(
                    "max_batch_rows must be in [1, " + MAX_BATCH_ROWS_UPPER_BOUND + "]");
        }
        this.maxBatchRows = rows;
        return this;
    }

    /**
     * Enables TLS with full certificate validation against the JVM's default trust store.
     * Must be called before {@link #connect}.
     */
    public QwpQueryClient withTls() {
        this.tlsEnabled = true;
        this.tlsValidationMode = ClientTlsConfiguration.TLS_VALIDATION_MODE_FULL;
        this.trustStorePath = null;
        this.trustStorePassword = null;
        return this;
    }

    /**
     * Enables TLS with full certificate validation against the given custom trust store.
     * Must be called before {@link #connect}.
     *
     * @param trustStorePath     filesystem path to a PKCS12 or JKS trust store
     * @param trustStorePassword password for the trust store
     */
    public QwpQueryClient withTrustStore(String trustStorePath, char[] trustStorePassword) {
        if (trustStorePath == null || trustStorePassword == null) {
            throw new IllegalArgumentException("trustStorePath and trustStorePassword must not be null");
        }
        this.tlsEnabled = true;
        this.tlsValidationMode = ClientTlsConfiguration.TLS_VALIDATION_MODE_FULL;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    private static String defaultClientId() {
        return "questdb-java-egress/1.0.0";
    }

    /**
     * Builds the {@code X-QWP-Accept-Encoding} header value from the user's
     * preference. {@code raw} (the library default) omits the header entirely
     * so servers that don't know about compression see an unchanged handshake.
     * {@code zstd} asks for zstd first and falls back to raw. {@code auto}
     * advertises both and lets the server pick -- useful for cross-DC clients
     * where the bandwidth/CPU trade-off is worthwhile; an explicit opt-in.
     */
    private String buildAcceptEncodingHeader() {
        if ("raw".equals(compressionPreference)) {
            return null;
        }
        return "zstd;level=" + compressionLevel + ",raw";
    }

    /**
     * Allocates and immediately frees a {@code ZSTD_DCtx} so that any
     * {@link UnsatisfiedLinkError} from a client build that doesn't include
     * the bundled libzstd surfaces synchronously on the user thread at
     * {@code connect()} time. Closes the just-opened WebSocket on failure so
     * the caller doesn't inherit a half-open socket.
     */
    private void probeZstdAvailable() {
        try {
            long dctx = Zstd.createDCtx();
            if (dctx != 0) {
                Zstd.freeDCtx(dctx);
            }
        } catch (UnsatisfiedLinkError e) {
            LOG.error("zstd JNI symbols missing from libquestdb; aborting connect", e);
            if (webSocketClient != null) {
                webSocketClient.close();
                webSocketClient = null;
            }
            throw new HttpClientException("this client build does not support zstd compression -- "
                    + "libquestdb was built without the zstd submodule. Rebuild the native library "
                    + "with 'git submodule update --init --recursive' and 'cmake --build', or set "
                    + "compression=raw on the connection string to skip the probe. "
                    + "[cause=" + e.getMessage() + "]");
        }
    }
}
