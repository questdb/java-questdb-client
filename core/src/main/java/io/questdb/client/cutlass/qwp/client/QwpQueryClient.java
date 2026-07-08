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
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.impl.ConfigString;
import io.questdb.client.impl.ConfigView;
import io.questdb.client.std.Chars;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Zstd;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
 * <p>
 * Multi-endpoint routing: the connection string accepts a comma-separated list
 * of {@code addr=host:port[,host:port...]} endpoints plus a {@code target=}
 * filter ({@code any} | {@code primary} | {@code replica}). {@link #connect()}
 * walks the list in order, reads the server's role from the
 * {@code SERVER_INFO} frame, and picks the first endpoint matching the target.
 * When every endpoint reports a role the filter rejects, {@link #connect()}
 * throws {@link QwpRoleMismatchException} with the last observed info attached
 * so callers can distinguish "no primary available" from "all endpoints down".
 * <p>
 * Failover on transport failure: with {@code failover=on} (the default)
 * {@link #execute} transparently reconnects to another endpoint when the I/O
 * thread reports a transport- or protocol-level terminal failure mid-stream,
 * and re-submits the query. The user handler observes a
 * {@link QwpColumnBatchHandler#onFailoverReset} callback just before replayed
 * batches start arriving (batch_seq restarts at 0 on the new node), so
 * accumulating handlers can discard rows the old connection already delivered.
 * {@code failover=off} disables this -- terminal failures
 * surface immediately through {@link QwpColumnBatchHandler#onError}.
 * <p>
 * Terminal-failure latching: transport- or protocol-level faults detected by
 * the I/O thread (server close, truncated/unknown frames, send/recv exceptions)
 * latch a sticky terminal failure on the client. With failover on, the next
 * {@link #execute} transparently reconnects; with failover off, subsequent
 * {@link #execute} calls short-circuit via
 * {@link QwpColumnBatchHandler#onError} with the stored status/message. Per-
 * query {@code QUERY_ERROR} responses are NOT terminal -- the connection
 * remains usable for the next query.
 * <p>
 * Status byte convention on {@link QwpColumnBatchHandler#onError}: server-
 * emitted {@code QUERY_ERROR} frames surface with the server's status code
 * ({@link WebSocketResponse#STATUS_PARSE_ERROR}, {@code STATUS_INTERNAL_ERROR},
 * {@link QwpConstants#STATUS_CANCELLED}, {@link QwpConstants#STATUS_LIMIT_EXCEEDED},
 * etc.). Failures detected client-side (closed client, bind encoding error,
 * truncated / unknown frame, decoder out of sync, I/O thread interrupt) all
 * surface with {@link WebSocketResponse#STATUS_INTERNAL_ERROR} and the
 * specific cause in the message.
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
    public static final int QWP_MAX_VERSION = QwpConstants.VERSION;
    public static final String TARGET_ANY = "any";
    public static final String TARGET_PRIMARY = "primary";
    public static final String TARGET_REPLICA = "replica";
    /**
     * Default ceiling on the exponential failover backoff. A minutes-long
     * outage that burns through {@code failoverMaxAttempts} still completes
     * in bounded wall time: with 8 attempts and a 1 s cap the client gives
     * up after roughly 5 s of cumulative sleep.
     */
    private static final long DEFAULT_AUTH_TIMEOUT_MS = 15_000L;
    /**
     * Default initial backoff between failover attempts, in milliseconds.
     * Doubled on each subsequent retry up to
     * {@link #DEFAULT_FAILOVER_MAX_BACKOFF_MS}. Keeps a tight reconnect
     * storm from overwhelming an already-struggling cluster; small enough
     * that a one-off transient hiccup barely adds latency to the replay.
     */
    private static final long DEFAULT_FAILOVER_INITIAL_BACKOFF_MS = 50L;
    /**
     * Default cap on the number of {@code executeOnce} invocations per call
     * to {@link #execute}. Counts the initial attempt plus every failover
     * retry. Each attempt walks the endpoint list independently, so this
     * bound is normally well above the expected cluster size. Override
     * per-client via {@link #withFailoverMaxAttempts(int)} or the
     * {@code failover_max_attempts=} connection-string key.
     */
    private static final int DEFAULT_FAILOVER_MAX_ATTEMPTS = 8;
    private static final long DEFAULT_FAILOVER_MAX_BACKOFF_MS = 1_000L;
    private static final long DEFAULT_FAILOVER_MAX_DURATION_MS = 30_000L;
    private static final int DEFAULT_IO_BUFFER_POOL_SIZE = 4;
    /**
     * How long {@link #connect()} waits to read the {@code SERVER_INFO} frame
     * from each endpoint before giving up and moving to the next. 5 seconds is
     * comfortable on a WAN; the server writes SERVER_INFO into the same send
     * buffer as the 101 upgrade response so under normal conditions the frame
     * is already in the client's kernel recv buffer by the time this wait starts.
     */
    private static final int DEFAULT_SERVER_INFO_TIMEOUT_MS = 5_000;
    private static final Logger LOG = LoggerFactory.getLogger(QwpQueryClient.class);
    // Reusable typed bind-value sink. Populated on the user thread by the
    // {@link QwpBindSetter} passed to execute(); the pre-encoded bytes are
    // handed to the I/O thread via QueryRequest. Allocated once per client to
    // keep the per-query path zero-alloc after warmup. Freed in close() on the
    // clean-join path; leaked together with the buffer pool on the timeout path.
    private final QwpBindValues bindValues = new QwpBindValues();
    // Gates {@link #close()} so concurrent or repeat invocations are a no-op
    // after the first. Without this, a second close() would run through the
    // full shutdown path again and double-free {@link #bindValues} native
    // scratch.
    private final AtomicBoolean closedFlag = new AtomicBoolean();
    private final List<Endpoint> endpoints = new ArrayList<>();
    private final AtomicBoolean executing = new AtomicBoolean();
    private final Random failoverRandom = new Random();
    private long authTimeoutMs = DEFAULT_AUTH_TIMEOUT_MS;
    private String authorizationHeader;
    // Upper bound (ms) on each TCP connect attempt. 0 (default) falls back to
    // the OS connect timeout.
    private int connectTimeoutMs = 0;
    private int bufferPoolSize = DEFAULT_IO_BUFFER_POOL_SIZE;
    private String clientId;
    // Client-configured zone (failover.md §1.1), opaque case-insensitive
    // identifier used by the host tracker to prefer same-zone endpoints when
    // target=any|replica. Null/empty means zone-blind selection. Compared
    // case-insensitively against SERVER_INFO.zone_id and the X-QuestDB-Zone
    // upgrade header. Ignored when target=primary (writers must be followed
    // across zones).
    private String clientZone;
    private int compressionLevel = 1;
    // User-facing compression preference from the connection string. "raw" is
    // the library default -- no compression, no handshake header, no server-
    // side CPU burn on payloads where the network isn't the bottleneck
    // (colocated clients, loopback). Clients wanting zstd must opt in via
    // {@code compression=zstd} (demands zstd) or {@code compression=auto}
    // (advertises zstd,raw and lets the server pick).
    private String compressionPreference = "raw";
    // Published by connect() / reconnectViaTracker() and read by cancel(),
    // close(), and the pre-connect-guard on the configuration setters. Volatile
    // both for the standard happens-before relationship against pre-connect
    // configuration writes and so a second thread calling cancel() or close()
    // observes the freshly-bound state without explicit synchronisation.
    private volatile boolean connected;
    // Index of the endpoint the current connection is bound to. -1 when
    // disconnected. Used by the failover path to skip the current endpoint
    // when picking the next one to try. Volatile because execute() on the
    // user thread reads + rewrites across a cleanup-reconnect handoff and
    // cancel() from another thread may also read it for diagnostics.
    private volatile int currentEndpointIndex = -1;
    // One-shot terminal-failure listener held by the currently-bound I/O
    // thread. Every new QwpEgressIoThread gets a fresh instance wired to it
    // at construction time; {@link #cleanupFailedConnect} orphans the
    // outgoing instance so a late callback from an I/O thread that failed
    // to join cannot pollute the new connection's terminalFailure latch.
    private volatile GenerationListener currentGenerationListener;
    // Written on the user thread at entry to {@link #execute} and cleared on exit.
    // Read by {@link #cancel} from any thread. {@code volatile} to guarantee the
    // user thread's write is visible to a concurrent cancel caller; 64-bit writes
    // are atomic under {@code volatile long}.
    private volatile long currentRequestId = -1L;
    // True by default: on transport failure during execute(), reconnect to
    // another endpoint and replay the query. Callers that prefer to see the
    // error themselves opt out via {@code failover=off} in the connection
    // string or {@link #withFailover(boolean)}. Volatile as a paired write
    // with the connected flag so post-connect mutation is consistently
    // rejected across threads.
    private volatile boolean failoverEnabled = true;
    private long failoverInitialBackoffMs = DEFAULT_FAILOVER_INITIAL_BACKOFF_MS;
    private int failoverMaxAttempts = DEFAULT_FAILOVER_MAX_ATTEMPTS;
    private long failoverMaxBackoffMs = DEFAULT_FAILOVER_MAX_BACKOFF_MS;
    private long failoverMaxDurationMs = DEFAULT_FAILOVER_MAX_DURATION_MS;
    private QwpHostHealthTracker hostTracker;
    // Credit-flow send-ahead budget. 0 = unbounded (Phase-1 default, no CREDIT
    // bookkeeping on either side). A positive value puts the stream under byte-
    // based flow control: the server emits at most this many bytes of result
    // payload before it parks, and the client auto-replenishes by the size of
    // each batch as the user releases it.
    private long initialCreditBytes;
    // Volatile so a cancel() call from a thread other than the one that ran
    // connect() sees the published reference (and a concurrent null-out from
    // close() is observed without a stale-reference race). The thread-safety
    // contract documented on cancel() relies on this.
    // ioThreadHandle is volatile and written BEFORE the ioThread volatile
    // write in connectToEndpoint. Together this gives close() (called from a
    // third thread) the guarantee that a non-null read of ioThread implies a
    // visible non-null ioThreadHandle. Without volatile here, a third-thread
    // close could observe ioThread != null && ioThreadHandle == null, skip
    // the join, and then closePool() / Misc.free under a still-running daemon
    // -- crashing the JVM on the next decode cycle.
    private volatile QwpEgressIoThread ioThread;
    private volatile Thread ioThreadHandle;
    private boolean lastCloseTimedOut;
    // Client preference for server-side per-batch row cap. 0 means "unset",
    // server uses its default. Set via {@code max_batch_rows=N} in the
    // connection string or {@link #withMaxBatchRows}. Smaller values give
    // streaming consumers earlier access to the first rows at the cost of
    // more per-batch overhead; larger values amortise fixed costs over more
    // rows. Server may clamp down to its own hard cap.
    private int maxBatchRows;
    private int negotiatedQwpVersion;
    // Zstd compression level the server actually applied for this connection,
    // parsed from the echoed X-QWP-Content-Encoding response header. 0 means
    // "no zstd" -- the server picked raw or the header was absent. Non-zero
    // values are surfaced as the server wrote them on the wire (no client-side
    // re-clamp) so a misconfigured server is observable from user code.
    private int negotiatedZstdLevel;
    private long nextRequestId = 1;
    // Cancel intent latched between {@link #cancel} and the point where
    // {@link #executeOnce} assigns {@link #currentRequestId}. Without this
    // latch, a cancel arriving in the dispatch window (after the user thread's
    // submit() returned but before the worker reached the requestId write) is
    // dropped silently because cancel()'s wire-send is gated on a non-negative
    // currentRequestId. Volatile so a cancel from any thread is visible to the
    // worker thread's post-requestId read.
    private volatile boolean pendingCancel;
    // Decoded SERVER_INFO from the current connection's handshake. Null before
    // connect() has succeeded; non-null on every established connection (the
    // server always emits the frame). Volatile so getServerInfo(), callable
    // from any thread, observes the latest reconnect's value.
    private volatile QwpServerInfo serverInfo;
    private int serverInfoTimeoutMs = DEFAULT_SERVER_INFO_TIMEOUT_MS;
    // Maximum time close() will wait for the I/O thread to exit before giving up
    // and leaking the (daemon) thread + its native buffer pool + WebSocket socket.
    // 5 seconds is generous given the I/O thread polls on a 100 ms cadence; if
    // it overshoots this, something is seriously wrong (e.g., user handler stuck
    // in onBatch). Volatile (not final) so tests can reflectively shorten it to
    // hit the timeout branch in under a second instead of spending five.
    @SuppressWarnings("FieldMayBeFinal")
    private volatile long shutdownJoinMs = 5_000;
    // Routing filter: one of {@link #TARGET_ANY}, {@link #TARGET_PRIMARY},
    // {@link #TARGET_REPLICA}. Applied in {@link #connect()} against the role
    // byte from SERVER_INFO; endpoints that don't match are skipped.
    // {@link #TARGET_PRIMARY} accepts STANDALONE as well so OSS deployments
    // (which don't have a replication role) are not accidentally excluded.
    // Volatile so the post-connect mutation guard observes the live value
    // across threads.
    private volatile String target = TARGET_ANY;
    private boolean tlsEnabled;
    // Only meaningful when tlsEnabled. Default is full validation against the JVM's trust store.
    private int tlsValidationMode = ClientTlsConfiguration.TLS_VALIDATION_MODE_FULL;
    private char[] trustStorePassword;
    private String trustStorePath;
    private volatile WebSocketClient webSocketClient;

    private QwpQueryClient(String host, int port) {
        this.endpoints.add(new Endpoint(host, port));
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
     *   <li>{@code addr=host[:port][,host[:port]...]} -- required. Comma-separated list of
     *       WebSocket endpoints; {@link #connect()} walks them in order and stops at the
     *       first matching {@code target=}. Default port on each entry is {@value #DEFAULT_WS_PORT}.
     *       Per {@code failover.md} section 1, the comma form and repeated {@code addr=} keys
     *       both accumulate into a single ordered list; empty entries are rejected.</li>
     *   <li>{@code target=any|primary|replica} -- endpoint filter applied against the role
     *       byte from the {@code SERVER_INFO} frame. Default {@code any}. {@code primary}
     *       accepts {@code PRIMARY}, {@code PRIMARY_CATCHUP} and {@code STANDALONE}.</li>
     *   <li>{@code failover=on|off} -- default {@code on}. On transport failure during
     *       {@link #execute}, reconnect to another endpoint and re-submit the query.
     *       The user handler sees {@link QwpColumnBatchHandler#onFailoverReset} before
     *       replayed batches begin arriving (batch_seq restarts at 0 on the new node).</li>
     *   <li>{@code username=<name>;password=<secret>} -- HTTP Basic authentication. The client builds the
     *       {@code Authorization: Basic <base64>} header from these. Server verifies the credentials
     *       against the same user store the Postgres wire protocol uses, so a user created via
     *       {@code CREATE USER ... WITH PASSWORD ...} can log in unchanged.
     *       Both keys must be present together; mutually exclusive with {@code token}.</li>
     *   <li>{@code token=<access_token>} -- HTTP Bearer authentication with an OIDC access token (sent as
     *       {@code Authorization: Bearer <token>}). Mutually exclusive with
     *       {@code username}/{@code password}.</li>
     *   <li>{@code client_id=<id>} -- sent as the {@code X-QWP-Client-Id} header.</li>
     *   <li>{@code buffer_pool_size=N} -- depth of the I/O thread's batch buffer pool. Default 4.</li>
     *   <li>{@code compression=zstd|raw|auto} -- compression codec the client
     *       asks the server to use for RESULT_BATCH bodies. {@code auto}
     *       (default) advertises {@code zstd,raw} so the server picks zstd
     *       when it supports it and falls back to raw otherwise.</li>
     *   <li>{@code compression_level=N} -- zstd level hint, clamped server-side
     *       to [1, 9]. Default 1 (cheapest server-side CPU; compression ratio
     *       gain at higher levels is small for typical columnar payloads).
     *       Ignored when {@code compression=raw}.</li>
     *   <li>{@code tls_verify=on|unsafe_off} -- TLS certificate validation. Default is {@code on}.
     *       Only allowed with the {@code wss::} schema. {@code unsafe_off} disables hostname and
     *       certificate chain validation; use only for testing.</li>
     *   <li>{@code tls_roots=<path>} -- path to a custom trust store (PKCS12 or JKS). Must be
     *       paired with {@code tls_roots_password}. Only allowed with {@code wss::}.</li>
     *   <li>{@code tls_roots_password=<secret>} -- password for the custom trust store.</li>
     *   <li>{@code zone=<id>} -- client zone identifier (opaque, case-insensitive;
     *       e.g. {@code eu-west-1a}). When set with {@code target=any|replica},
     *       failover prefers endpoints whose server-advertised {@code zone_id}
     *       matches the configured value; cross-zone hosts still participate
     *       as fallbacks since state outranks zone in the priority lattice.
     *       Ignored when {@code target=primary} (writers are followed across
     *       zones). See {@code failover.md} §1.1, §2.</li>
     * </ul>
     * Examples:
     * <pre>
     *   ws::addr=localhost:9000;
     *   ws::addr=db.internal:9000;token=abc123;client_id=dashboard/2.0;
     *   ws::addr=db-a:9000,db-b:9000,db-c:9000;target=primary;failover=on;
     * </pre>
     */
    public static QwpQueryClient fromConfig(CharSequence configurationString) {
        ConfigString cs = ConfigString.parse(configurationString);
        boolean tls;
        if ("ws".equals(cs.schema())) {
            tls = false;
        } else if ("wss".equals(cs.schema())) {
            tls = true;
        } else {
            throw new IllegalArgumentException(
                    "unsupported schema [schema=" + cs.schema() + ", supported-schemas=[ws, wss]]");
        }
        ConfigView view = new ConfigView(cs);
        validateConfig(view, tls);

        List<Endpoint> parsedEndpoints = new ArrayList<>();
        view.getHostPorts("addr", DEFAULT_WS_PORT, (h, p) -> parsedEndpoints.add(new Endpoint(h, p)));

        String target = view.getEnum("target");
        if (target == null) {
            target = TARGET_ANY;
        }
        Boolean failover = view.has("failover") ? view.getBoolOnOff("failover", false) : null;
        Integer failoverMaxAttempts = view.has("failover_max_attempts")
                ? view.getInt("failover_max_attempts", 0) : null;
        Long failoverBackoffInitialMs = view.has("failover_backoff_initial_ms")
                ? view.getLong("failover_backoff_initial_ms", 0) : null;
        Long failoverBackoffMaxMs = view.has("failover_backoff_max_ms")
                ? view.getLong("failover_backoff_max_ms", 0) : null;
        Long failoverMaxDurationMs = view.has("failover_max_duration_ms")
                ? view.getLong("failover_max_duration_ms", 0) : null;
        Long authTimeoutMs = view.has("auth_timeout_ms") ? view.getLong("auth_timeout_ms", 0) : null;
        // getInt (not getLong + cast): withConnectTimeout takes an int, and an
        // over-int value must reject, not wrap.
        Integer connectTimeout = view.has("connect_timeout") ? view.getInt("connect_timeout", 0) : null;
        Long initialCredit = view.has("initial_credit") ? view.getLong("initial_credit", 0) : null;
        int poolSize = view.getInt("buffer_pool_size", DEFAULT_IO_BUFFER_POOL_SIZE);
        String compression = view.getEnum("compression");
        if (compression == null) {
            compression = "raw";
        }
        int compressionLevel = view.getInt("compression_level", 1);
        int maxBatchRows = view.getInt("max_batch_rows", 0);
        String username = view.getStr("username");
        String password = view.getStr("password");
        String token = view.getStr("token");
        String cid = view.getStr("client_id");
        String zone = view.getStr("zone");
        String tlsRoots = view.getStr("tls_roots");
        String tlsRootsPassword = view.getStr("tls_roots_password");
        Integer tlsValidation = null;
        String tlsVerify = view.getEnum("tls_verify");
        if ("on".equals(tlsVerify)) {
            tlsValidation = ClientTlsConfiguration.TLS_VALIDATION_MODE_FULL;
        } else if ("unsafe_off".equals(tlsVerify)) {
            tlsValidation = ClientTlsConfiguration.TLS_VALIDATION_MODE_NONE;
        }
        boolean hasBasic = username != null || password != null;
        Endpoint first = parsedEndpoints.get(0);
        QwpQueryClient client = new QwpQueryClient(first.host, first.port);
        // The constructor allocated native scratch (bindValues); close it if a
        // setter below rejects its input so a config error cannot leak it.
        // validateConfig above already rejects every value these setters check,
        // so this is a safety net against future drift, not a reachable path today.
        try {
            for (int i = 1; i < parsedEndpoints.size(); i++) {
                client.endpoints.add(parsedEndpoints.get(i));
            }
            client.withTarget(target);
            if (failover != null) {
                client.withFailover(failover);
            }
            if (failoverMaxAttempts != null) {
                client.withFailoverMaxAttempts(failoverMaxAttempts);
            }
            if (failoverBackoffInitialMs != null || failoverBackoffMaxMs != null) {
                long initial = failoverBackoffInitialMs != null
                        ? failoverBackoffInitialMs
                        : DEFAULT_FAILOVER_INITIAL_BACKOFF_MS;
                long max = failoverBackoffMaxMs != null
                        ? failoverBackoffMaxMs
                        : Math.max(initial, DEFAULT_FAILOVER_MAX_BACKOFF_MS);
                client.withFailoverBackoff(initial, max);
            }
            if (failoverMaxDurationMs != null) {
                client.withFailoverMaxDuration(failoverMaxDurationMs);
            }
            if (authTimeoutMs != null) {
                client.withAuthTimeout(authTimeoutMs);
            }
            if (connectTimeout != null) {
                client.withConnectTimeout(connectTimeout);
            }
            if (initialCredit != null) {
                client.withInitialCredit(initialCredit);
            }
            client.withBufferPoolSize(poolSize);
            client.withCompression(compression, compressionLevel);
            if (tls) {
                if (tlsRoots != null) {
                    client.withTrustStore(tlsRoots, tlsRootsPassword.toCharArray());
                } else if (tlsValidation != null && tlsValidation == ClientTlsConfiguration.TLS_VALIDATION_MODE_NONE) {
                    client.withInsecureTls();
                } else {
                    client.withTls();
                }
            }
            if (hasBasic) client.withBasicAuth(username, password);
            if (token != null) client.withBearerToken(token);
            if (cid != null) client.withClientId(cid);
            if (maxBatchRows > 0) client.withMaxBatchRows(maxBatchRows);
            if (zone != null) client.withZone(zone);
            return client;
        } catch (RuntimeException e) {
            client.close();
            throw e;
        }
    }

    /**
     * Validates the cross-key invariants of an egress {@code ws}/{@code wss}
     * config without constructing a client. Shared by {@link #fromConfig} and
     * the {@code QuestDB} facade's fail-fast build path. {@code tls} is true for
     * the {@code wss} schema.
     */
    public static void validateConfig(ConfigView view, boolean tls) {
        view.getHostPorts("addr", DEFAULT_WS_PORT, (h, p) -> {
        });
        if (!view.has("addr")) {
            throw new IllegalArgumentException("missing required key: addr");
        }
        // Trigger range/enum validation of every typed value.
        view.getEnum("target");
        view.getEnum("compression");
        view.getEnum("tls_verify");
        view.getBoolOnOff("failover", false);
        view.getInt("failover_max_attempts", 0);
        view.getInt("max_batch_rows", 0);
        view.getInt("buffer_pool_size", 0);
        view.getInt("compression_level", 0);
        boolean hasBackoffInitial = view.has("failover_backoff_initial_ms");
        boolean hasBackoffMax = view.has("failover_backoff_max_ms");
        // getLong also range-validates the value; call it even for an absent key.
        long backoffInitial = view.getLong("failover_backoff_initial_ms", -1);
        long backoffMax = view.getLong("failover_backoff_max_ms", -1);
        view.getLong("failover_max_duration_ms", -1);
        view.getLong("initial_credit", -1);
        view.getLong("auth_timeout_ms", -1);
        // getInt: connect_timeout feeds an int API, so validation must also
        // reject values that fit a long but not an int.
        view.getInt("connect_timeout", -1);
        String username = view.getStr("username");
        String password = view.getStr("password");
        String token = view.getStr("token");
        // A present-but-blank credential is rejected up front, matching the
        // ingress Sender, so a shared ws/wss string fails the same way on both
        // sides and the client never builds an empty Authorization header.
        if (username != null && Chars.isBlank(username)) {
            throw new IllegalArgumentException("username cannot be empty nor null");
        }
        if (password != null && Chars.isBlank(password)) {
            throw new IllegalArgumentException("password cannot be empty nor null");
        }
        if (token != null && Chars.isBlank(token)) {
            throw new IllegalArgumentException("token cannot be empty nor null");
        }
        boolean hasBasic = username != null || password != null;
        if (hasBasic && (username == null || password == null)) {
            throw new IllegalArgumentException("username and password must be provided together");
        }
        if (hasBasic && token != null) {
            throw new IllegalArgumentException("cannot use both token and username/password authentication");
        }
        String tlsVerify = view.getStr("tls_verify");
        String tlsRoots = view.getStr("tls_roots");
        String tlsRootsPassword = view.getStr("tls_roots_password");
        if (!tls && (tlsVerify != null || tlsRoots != null || tlsRootsPassword != null)) {
            throw new IllegalArgumentException(
                    "tls_verify/tls_roots/tls_roots_password require the wss:: schema");
        }
        if ((tlsRoots == null) != (tlsRootsPassword == null)) {
            throw new IllegalArgumentException("tls_roots and tls_roots_password must be provided together");
        }
        // Mirror fromConfig's effective values: a missing bound takes its
        // default, so the ordering is enforced even when only one key is set
        // (e.g. failover_backoff_max_ms alone, below the default initial backoff).
        if (hasBackoffInitial || hasBackoffMax) {
            long effectiveInitial = hasBackoffInitial ? backoffInitial : DEFAULT_FAILOVER_INITIAL_BACKOFF_MS;
            long effectiveMax = hasBackoffMax ? backoffMax : Math.max(effectiveInitial, DEFAULT_FAILOVER_MAX_BACKOFF_MS);
            if (effectiveMax < effectiveInitial) {
                throw new IllegalArgumentException(
                        "failover_backoff_max_ms must be >= failover_backoff_initial_ms");
            }
        }
    }

    /**
     * Creates a plain-text (non-TLS) QWP query client against {@code host:port}
     * with all other settings at their defaults. For TLS, authentication,
     * custom paths, compression, buffer pool sizing, or multi-endpoint routing
     * use {@link #fromConfig(CharSequence)} with a connection string instead.
     *
     * @see #fromConfig(CharSequence)
     */
    public static QwpQueryClient newPlainText(CharSequence host, int port) {
        return new QwpQueryClient(host.toString(), port);
    }

    /**
     * Asks the server to cancel the currently executing query. No-op if no query
     * is in flight. Safe to call from a thread other than the one blocked inside
     * {@link #execute}. The server replies to the active query with a
     * {@code QUERY_ERROR} whose status byte is {@code STATUS_CANCELLED}; the
     * handler's {@code onError} (on the execute-ing thread) will see it.
     */
    public void cancel() {
        // Latch FIRST so a cancel arriving in the dispatch window (after
        // execute() cleared the latch but before executeOnce() wrote
        // currentRequestId) is observed on the worker thread's
        // post-requestId read. Without this, cancel() reads
        // currentRequestId == -1, the wire-send is skipped, and the user's
        // intent is silently dropped.
        pendingCancel = true;
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
     * <p>
     * <strong>Threading contract:</strong> {@code close()} must be called from a thread
     * other than the one currently inside a batch handler, AND the user must finish
     * any in-flight {@code execute()} before calling {@code close()}. Calling
     * {@code close()} concurrently with a {@code handler.onBatch(...)} that is still
     * dereferencing batch column pointers can free the WebSocket recv buffer under
     * those pointers and SIGSEGV the JVM. The interrupt-driven I/O thread shutdown
     * does NOT detect or wait for a still-running user handler -- the timeout-based
     * leak fallback only protects against an unresponsive I/O thread, not an
     * unresponsive user thread. {@link #cancel()} is the right way to ask an
     * in-flight {@code execute()} to return; close after it does.
     */
    @Override
    public void close() {
        if (!closedFlag.compareAndSet(false, true)) {
            // Second (or concurrent) close call. Without this guard, two
            // closes would each Misc.free(bindValues.writer) the shared native
            // scratch, double-freeing it.
            return;
        }
        connected = false;
        lastCloseTimedOut = false;
        try {
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
                        // Don't free anything else -- preserve clean shutdown semantics on the next
                        // attempt. bindValues still closes via the enclosing finally because its
                        // bytes are copied into sendScratch at submit time and no other thread
                        // references it after that.
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
        } finally {
            // bindValues owns native scratch that is not shared with the I/O thread
            // (submitQuery copies its bytes into sendScratch), so it is safe to free
            // even when we otherwise leak the I/O thread and buffer pool.
            bindValues.close();
        }
    }

    /**
     * Opens a connection that satisfies the configured {@code target} filter and
     * performs the WebSocket upgrade. Must be called before any query is submitted.
     * <p>
     * Walks the endpoint list in order: for each entry it opens the TCP socket,
     * runs the HTTP upgrade, reads the {@code SERVER_INFO} frame, and accepts
     * the endpoint if the server's role matches the configured target. An
     * endpoint that matches becomes the bound connection and the I/O thread is
     * spawned. An endpoint whose role doesn't match is closed and the walk
     * continues to the next entry. Transport failures on a specific endpoint
     * are treated the same way -- the walk continues.
     * <p>
     * If every endpoint is tried and none matches the target, throws
     * {@link QwpRoleMismatchException} carrying the last {@link QwpServerInfo}
     * observed so callers can distinguish "no primary available" from "all
     * endpoints unreachable" (the latter surfaces as a plain
     * {@link HttpClientException}).
     */
    public synchronized void connect() {
        if (closedFlag.get()) {
            throw new IllegalStateException("QwpQueryClient is closed");
        }
        if (connected) {
            return;
        }
        lastCloseTimedOut = false;
        if (hostTracker == null) {
            hostTracker = new QwpHostHealthTracker(
                    endpoints.size(), clientZone, TARGET_PRIMARY.equals(target));
        } else {
            hostTracker.beginRound(false);
        }
        QwpServerInfo lastObservedMismatch = null;
        QwpIngressRoleRejectedException lastUpgradeRoleReject = null;
        Throwable lastTransportError = null;
        while (true) {
            int i = hostTracker.pickNext();
            if (i < 0) {
                break;
            }
            Endpoint ep = endpoints.get(i);
            try {
                connectToEndpoint(ep);
            } catch (QwpAuthFailedException ae) {
                cleanupFailedConnect();
                throw ae;
            } catch (QwpIngressRoleRejectedException re) {
                lastTransportError = re;
                lastUpgradeRoleReject = re;
                hostTracker.recordZone(i, re.getZoneId());
                hostTracker.recordRoleReject(i, re.isTransient());
                LOG.info("QwpQueryClient {}:{} rejected upgrade with role={}; trying next",
                        ep.host, ep.port, re.getRole());
                cleanupFailedConnect();
                continue;
            } catch (RuntimeException e) {
                lastTransportError = e;
                hostTracker.recordTransportError(i);
                LOG.warn("QwpQueryClient connect failed for {}:{} -- {}", ep.host, ep.port, e.getMessage());
                cleanupFailedConnect();
                continue;
            }
            // info is non-null: connectToEndpoint() returns only after
            // receiveServerInfoSync() set serverInfo (it returns non-null or throws).
            QwpServerInfo info = serverInfo;
            if ((info.getCapabilities() & QwpEgressMsgKind.CAP_ZONE) != 0) {
                hostTracker.recordZone(i, info.getZoneId());
            }
            if (!matchesTarget(info.getRole(), target)) {
                lastObservedMismatch = info;
                boolean isTransient = info.getRole() == QwpEgressMsgKind.ROLE_PRIMARY_CATCHUP;
                hostTracker.recordRoleReject(i, isTransient);
                LOG.info("QwpQueryClient {}:{} role={} does not match target={}, trying next",
                        ep.host, ep.port, QwpServerInfo.roleName(info.getRole()), target);
                cleanupFailedConnect();
                continue;
            }
            spawnIoThread();
            hostTracker.recordSuccess(i);
            currentEndpointIndex = i;
            connected = true;
            return;
        }
        if (lastObservedMismatch != null) {
            throw new QwpRoleMismatchException(
                    target,
                    lastObservedMismatch,
                    "no endpoint matches target=" + target + "; last observed role="
                            + QwpServerInfo.roleName(lastObservedMismatch.getRole())
                            + " cluster=" + lastObservedMismatch.getClusterId()
            );
        }
        if (lastUpgradeRoleReject != null) {
            // 421 + X-QuestDB-Role on every host: the cluster answered with role
            // information at upgrade time but no host satisfied target=. Surface
            // QwpRoleMismatchException with the last role for diagnostics so
            // callers can distinguish "no primary available" from "all unreachable"
            // (failover.md §6 Topology / wire-egress.md §11.9.2).
            QwpRoleMismatchException ex = new QwpRoleMismatchException(
                    target,
                    null,
                    "no endpoint matches target=" + target
                            + "; last observed role=" + lastUpgradeRoleReject.getRole()
                            + " at " + lastUpgradeRoleReject.getHost() + ':' + lastUpgradeRoleReject.getPort()
            );
            ex.initCause(lastUpgradeRoleReject);
            throw ex;
        }
        throw new HttpClientException(
                "all QWP endpoints unreachable [count=" + endpoints.size()
                        + ", lastError=" + (lastTransportError == null ? "<none>" : lastTransportError.getMessage()) + ']');
    }

    /**
     * Executes {@code sql} and drives the supplied handler through the result stream.
     * <p>
     * Blocks the calling thread until the server sends {@code RESULT_END} or
     * {@code QUERY_ERROR}. While the user thread is inside {@code handler.onBatch},
     * the I/O thread keeps reading and decoding ahead up to the configured buffer-pool depth.
     * <p>
     * With {@code failover=on} (the default), a transport failure mid-stream
     * triggers a transparent reconnect to another endpoint and re-submission of
     * the query. The user handler observes
     * {@link QwpColumnBatchHandler#onFailoverReset} just before the replayed
     * batches begin arriving on the new connection, and any rows delivered
     * before the reset should be discarded by the handler.
     */
    public void execute(CharSequence sql, QwpColumnBatchHandler handler) {
        execute(sql, null, handler, false);
    }

    /**
     * As {@link #execute(CharSequence, QwpColumnBatchHandler)}, but when
     * {@code resetSymbolDict} is set the server resets the connection-scoped
     * SYMBOL dict before this query, scoping the dict to this query's symbols.
     * The flag reaches the wire only when the server advertised
     * {@link QwpEgressMsgKind#CAP_QUERY_FLAGS}; otherwise it is silently ignored.
     */
    public void execute(CharSequence sql, QwpColumnBatchHandler handler, boolean resetSymbolDict) {
        execute(sql, null, handler, resetSymbolDict);
    }

    /**
     * Executes {@code sql} with typed bind parameters supplied by the given
     * {@link QwpBindSetter}. The setter runs on the calling thread before the
     * query is dispatched; bind values must be assigned in strictly ascending
     * index order starting at 0, matching the SQL placeholders ({@code $1, $2, ...}).
     * <p>
     * Passing the same SQL text on subsequent calls hits the server's
     * SQL-text-keyed factory cache -- the factory compiles once and binds
     * supply the per-call values. Interpolating values into the SQL string
     * defeats this reuse.
     */
    public void execute(CharSequence sql, QwpBindSetter binds, QwpColumnBatchHandler handler) {
        execute(sql, binds, handler, false);
    }

    /**
     * As {@link #execute(CharSequence, QwpBindSetter, QwpColumnBatchHandler)},
     * but when {@code resetSymbolDict} is set the server resets the
     * connection-scoped SYMBOL dict before this query, scoping the dict to this
     * query's symbols. The flag reaches the wire only when the server advertised
     * {@link QwpEgressMsgKind#CAP_QUERY_FLAGS}; otherwise it is silently ignored.
     */
    public void execute(CharSequence sql, QwpBindSetter binds, QwpColumnBatchHandler handler, boolean resetSymbolDict) {
        if (!executing.compareAndSet(false, true)) {
            throw new IllegalStateException(
                    "QwpQueryClient.execute called while another execute is in flight; one query at a time per client");
        }
        // Drop any cancel latched between calls (e.g., a watchdog that fired
        // while no request was in flight, or a previous pooled user that
        // released the client without ever calling execute). Failover retries
        // inside this execute() must still honor a fresh cancel, so the latch
        // is intentionally NOT cleared inside executeOnce().
        pendingCancel = false;
        try {
            executeImpl(sql, binds, handler, resetSymbolDict);
        } finally {
            executing.set(false);
        }
    }

    public long getAuthTimeoutMsForTest() {
        return authTimeoutMs;
    }

    /**
     * Returns the zstd level the client will advertise on the
     * {@code X-QWP-Accept-Encoding} header. Exposed so tests can pin the
     * default and the parser's accepted range; any drift here changes what
     * the server sees on the wire.
     */
    public int getCompressionLevelForTest() {
        return compressionLevel;
    }

    /**
     * Test-only hook: the synthesized {@code Authorization} header value
     * ({@code Basic ...} or {@code Bearer ...}), or null when no credentials
     * were configured.
     */
    @TestOnly
    public String getAuthorizationHeaderForTest() {
        return authorizationHeader;
    }

    /**
     * Snapshot of the egress config this client applied, keyed by connect-string
     * key name. Drives the per-key "honored" guard test -- proves each egress key
     * read from a config string reaches the client.
     */
    @TestOnly
    public java.util.Map<String, Object> configSnapshotForTest() {
        java.util.Map<String, Object> m = new java.util.HashMap<>();
        m.put("target", target);
        m.put("failover", failoverEnabled);
        m.put("failover_max_attempts", failoverMaxAttempts);
        m.put("failover_backoff_initial_ms", failoverInitialBackoffMs);
        m.put("failover_backoff_max_ms", failoverMaxBackoffMs);
        m.put("failover_max_duration_ms", failoverMaxDurationMs);
        m.put("max_batch_rows", maxBatchRows);
        m.put("initial_credit", initialCreditBytes);
        m.put("buffer_pool_size", bufferPoolSize);
        m.put("compression", compressionPreference);
        m.put("compression_level", compressionLevel);
        m.put("client_id", clientId);
        m.put("zone", clientZone);
        m.put("auth_timeout_ms", authTimeoutMs);
        m.put("connect_timeout", connectTimeoutMs);
        m.put("authorization_header", authorizationHeader);
        m.put("tls_verify", tlsValidationMode);
        m.put("tls_roots", trustStorePath);
        m.put("tls_roots_password", trustStorePassword == null ? null : new String(trustStorePassword));
        return m;
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

    public int getEndpointCountForTest() {
        return endpoints.size();
    }

    public String getEndpointHostForTest(int idx) {
        return endpoints.get(idx).host;
    }

    public int getEndpointPortForTest(int idx) {
        return endpoints.get(idx).port;
    }

    public long getFailoverMaxDurationMsForTest() {
        return failoverMaxDurationMs;
    }

    public int getNegotiatedQwpVersion() {
        return negotiatedQwpVersion;
    }

    /**
     * Zstd compression level the server actually applied for this connection,
     * parsed from the echoed {@code X-QWP-Content-Encoding} response header.
     * <p>
     * Returns {@code 0} when no zstd is in use -- the server picked raw, the
     * client never asked for zstd, or the header was absent on the wire
     * (older servers). A non-zero return is what the server wrote on the
     * wire verbatim, not what this client requested via
     * {@code compression_level=N}; the two differ when the server has
     * {@code qwp.egress.compression.force.level} set or when the server
     * clamped a client request to its {@code [1, 9]} accepted range.
     * <p>
     * Refreshed after every successful reconnect, so a forced-level change
     * on the server side picks up on the next round of failover.
     */
    public int getNegotiatedZstdLevel() {
        return negotiatedZstdLevel;
    }

    /**
     * Returns the {@link QwpServerInfo} decoded from the currently-bound
     * server's {@code SERVER_INFO} frame, or {@code null} if the client is not
     * connected. The value is refreshed on every successful failover reconnect.
     */
    public QwpServerInfo getServerInfo() {
        return serverInfo;
    }

    public boolean isConnected() {
        return connected;
    }

    /**
     * Test-only view of the dispatch-window cancel latch. Returns {@code true}
     * when {@link #cancel} was invoked after the outermost {@link #execute}
     * cleared the latch and before {@link #execute} has consumed it on the
     * {@code currentRequestId = requestId} write.
     */
    @TestOnly
    public boolean isPendingCancelForTest() {
        return pendingCancel;
    }

    /**
     * Test-only / diagnostics hook: injects a synthetic terminal failure
     * through the current generation's listener. Production code does not
     * call this -- transport failures arrive through the I/O thread's own
     * callback path. Public + name-tagged so tests in other packages can
     * call it without reflection; the {@code ForTest} suffix mirrors
     * {@link #seedFailoverRandomForTest} and makes it obvious at the call
     * site that this is not a normal API entry point.
     */
    public void recordTerminalFailureForTest(byte status, String message) {
        GenerationListener listener = currentGenerationListener;
        if (listener != null) {
            listener.onTerminalFailure(status, message);
        }
    }

    public void seedFailoverRandomForTest(long seed) {
        synchronized (failoverRandom) {
            failoverRandom.setSeed(seed);
        }
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

    /**
     * Per-endpoint timeout on the HTTP upgrade response read. Bounds the common
     * "host accepts TCP but never replies" blackhole. Does NOT bound the TCP
     * connect itself (no native knob); a routing blackhole with no SYN-ACK
     * still falls back to OS defaults. Default {@value #DEFAULT_AUTH_TIMEOUT_MS} ms.
     */
    public QwpQueryClient withAuthTimeout(long authTimeoutMs) {
        checkPreConnect("withAuthTimeout");
        if (authTimeoutMs <= 0L) {
            throw new IllegalArgumentException("authTimeoutMs must be > 0");
        }
        this.authTimeoutMs = authTimeoutMs;
        return this;
    }

    /**
     * Upper bound, in milliseconds, on establishing the TCP connection to an
     * endpoint. Unlike {@link #withAuthTimeout(long)} this DOES bound the TCP
     * connect itself (via a non-blocking connect), so a routing blackhole that
     * never returns SYN-ACK is aborted within this budget instead of riding the
     * OS connect timeout. {@code 0} (default) keeps the OS connect timeout.
     */
    public QwpQueryClient withConnectTimeout(int connectTimeoutMs) {
        checkPreConnect("withConnectTimeout");
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("connectTimeoutMs must be > 0");
        }
        this.connectTimeoutMs = connectTimeoutMs;
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
        checkPreConnect("withBasicAuth");
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
        checkPreConnect("withBearerToken");
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
    public void withBufferPoolSize(int size) {
        checkPreConnect("withBufferPoolSize");
        if (size < 1) throw new IllegalArgumentException("bufferPoolSize must be >= 1");
        this.bufferPoolSize = size;
    }

    /**
     * Overrides the {@code X-QWP-Client-Id} header sent during the upgrade handshake.
     * Must be called before {@link #connect()}.
     */
    public void withClientId(String clientId) {
        checkPreConnect("withClientId");
        this.clientId = clientId;
    }

    /**
     * Programmatic equivalent of the {@code compression=} / {@code compression_level=}
     * connection-string keys. {@code preference} is one of {@code zstd},
     * {@code raw} (default), or {@code auto}. {@code level} is the zstd
     * compression level hint passed to the server; clamped server-side to
     * [1, 9]. Must be called before {@link #connect}.
     */
    public void withCompression(String preference, int level) {
        checkPreConnect("withCompression");
        if (!"zstd".equals(preference) && !"raw".equals(preference) && !"auto".equals(preference)) {
            throw new IllegalArgumentException(
                    "unsupported compression: " + preference + " (expected zstd, raw, or auto)");
        }
        if (level < 1 || level > 22) {
            throw new IllegalArgumentException("compression level must be in [1, 22]");
        }
        this.compressionPreference = preference;
        this.compressionLevel = level;
    }

    /**
     * Programmatic equivalent of the {@code failover=} connection-string key.
     * Default is {@code true}: transport failures during {@link #execute} are
     * transparently retried against another endpoint.
     */
    public void withFailover(boolean enabled) {
        checkPreConnect("withFailover");
        this.failoverEnabled = enabled;
    }

    /**
     * Configures the exponential backoff applied between failover reconnect
     * attempts. {@code initialMs} is the delay before the first retry (the
     * second overall execute attempt); each subsequent retry doubles the
     * delay up to {@code maxMs}. Setting either {@code initialMs} or
     * {@code maxMs} to 0 disables backoff entirely -- retries fire back to
     * back, bounded only by {@link #withFailoverMaxAttempts} and
     * {@link #withFailoverMaxDuration}. Fine for fast LAN clusters, but
     * risks hammering a struggling one during a real outage.
     * Defaults: initial {@value #DEFAULT_FAILOVER_INITIAL_BACKOFF_MS} ms,
     * max {@value #DEFAULT_FAILOVER_MAX_BACKOFF_MS} ms.
     */
    public void withFailoverBackoff(long initialMs, long maxMs) {
        checkPreConnect("withFailoverBackoff");
        if (initialMs < 0) throw new IllegalArgumentException("failoverInitialBackoffMs must be >= 0");
        if (maxMs < initialMs) {
            throw new IllegalArgumentException("failoverMaxBackoffMs must be >= failoverInitialBackoffMs");
        }
        this.failoverInitialBackoffMs = initialMs;
        this.failoverMaxBackoffMs = maxMs;
    }

    /**
     * Configures the maximum number of {@code executeOnce} invocations per
     * {@link #execute} call. Counts the initial attempt plus every failover
     * retry. Default {@value #DEFAULT_FAILOVER_MAX_ATTEMPTS}.
     */
    public QwpQueryClient withFailoverMaxAttempts(int attempts) {
        checkPreConnect("withFailoverMaxAttempts");
        if (attempts < 1) throw new IllegalArgumentException("failoverMaxAttempts must be >= 1");
        this.failoverMaxAttempts = attempts;
        return this;
    }

    /**
     * Total wall-clock cap on the failover loop; {@code 0} disables.
     * Whichever of this or {@link #withFailoverMaxAttempts} fires first ends
     * the loop. Default {@value #DEFAULT_FAILOVER_MAX_DURATION_MS} ms.
     */
    public QwpQueryClient withFailoverMaxDuration(long maxDurationMs) {
        checkPreConnect("withFailoverMaxDuration");
        if (maxDurationMs < 0L) {
            throw new IllegalArgumentException("failoverMaxDurationMs must be >= 0");
        }
        this.failoverMaxDurationMs = maxDurationMs;
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
        checkPreConnect("withInitialCredit");
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
        checkPreConnect("withInsecureTls");
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
     * header, send syscall). The server clamps down
     * to its own hard limit; a value of {@code 0} (default) omits the header
     * and the server uses its own cap.
     * <p>
     * Must be called before {@link #connect}.
     */
    public QwpQueryClient withMaxBatchRows(int rows) {
        checkPreConnect("withMaxBatchRows");
        if (rows < 1 || rows > MAX_BATCH_ROWS_UPPER_BOUND) {
            throw new IllegalArgumentException(
                    "max_batch_rows must be in [1, " + MAX_BATCH_ROWS_UPPER_BOUND + "]");
        }
        this.maxBatchRows = rows;
        return this;
    }

    /**
     * Overrides the {@link #DEFAULT_SERVER_INFO_TIMEOUT_MS} wait for the
     * {@code SERVER_INFO} frame. Must be called before {@link #connect}.
     */
    public void withServerInfoTimeout(int ms) {
        checkPreConnect("withServerInfoTimeout");
        if (ms < 1) throw new IllegalArgumentException("serverInfoTimeoutMs must be >= 1");
        this.serverInfoTimeoutMs = ms;
    }

    /**
     * Programmatic equivalent of the {@code target=} connection-string key.
     * One of {@link #TARGET_ANY} (default), {@link #TARGET_PRIMARY},
     * {@link #TARGET_REPLICA}.
     */
    public void withTarget(String target) {
        checkPreConnect("withTarget");
        if (!TARGET_ANY.equals(target) && !TARGET_PRIMARY.equals(target) && !TARGET_REPLICA.equals(target)) {
            throw new IllegalArgumentException(
                    "invalid target: " + target + " (expected any, primary, or replica)");
        }
        this.target = target;
    }

    /**
     * Enables TLS with full certificate validation against the JVM's default trust store.
     * Must be called before {@link #connect}.
     */
    public QwpQueryClient withTls() {
        checkPreConnect("withTls");
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
        checkPreConnect("withTrustStore");
        if (trustStorePath == null || trustStorePassword == null) {
            throw new IllegalArgumentException("trustStorePath and trustStorePassword must not be null");
        }
        this.tlsEnabled = true;
        this.tlsValidationMode = ClientTlsConfiguration.TLS_VALIDATION_MODE_FULL;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    /**
     * Programmatic equivalent of the {@code zone=} connection-string key
     * (failover.md §1.1). Sets the client's zone identifier used by failover
     * to prefer endpoints whose server-advertised {@code zone_id} matches
     * (case-insensitive). Null or empty after trimming disables zone
     * preference. Ignored when {@code target=primary}.
     */
    public QwpQueryClient withZone(String zone) {
        checkPreConnect("withZone");
        if (zone == null) {
            this.clientZone = null;
        } else {
            String trimmed = zone.trim();
            this.clientZone = trimmed.isEmpty() ? null : trimmed;
        }
        return this;
    }

    private static String defaultClientId() {
        return "questdb-java-egress/1.0.0";
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private static boolean matchesTarget(byte role, String target) {
        if (TARGET_ANY.equals(target)) {
            return true;
        }
        if (TARGET_PRIMARY.equals(target)) {
            return role == QwpEgressMsgKind.ROLE_PRIMARY
                    || role == QwpEgressMsgKind.ROLE_PRIMARY_CATCHUP
                    || role == QwpEgressMsgKind.ROLE_STANDALONE;
        }
        if (TARGET_REPLICA.equals(target)) {
            return role == QwpEgressMsgKind.ROLE_REPLICA;
        }
        return true;
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
     * Guard for configuration setters that only apply at connect time.
     * Calling them after {@link #connect()} has bound to an endpoint is a
     * programming error: the new value would silently not apply until the
     * next reconnect, which is almost never what the caller wants. Throw
     * early with a clear message instead.
     */
    private void checkPreConnect(String setterName) {
        if (connected) {
            throw new IllegalStateException(setterName
                    + " must be called before connect(); the current value has already taken effect on the bound connection");
        }
    }

    /**
     * Tears down the half-built connection state left behind by a failed
     * endpoint attempt (transport error, role mismatch, SERVER_INFO decode
     * error). Idempotent; safe to call when no connection state has been
     * created yet.
     */
    private void cleanupFailedConnect() {
        // Orphan the outgoing generation's listener FIRST. Any callback from
        // the I/O thread after this point (including from a thread that never
        // exits the join below) becomes a no-op. Without this step, a late
        // terminalFailure notification could land in the new generation's
        // latch and cause spurious failover on the next execute().
        if (currentGenerationListener != null) {
            currentGenerationListener.orphan();
            currentGenerationListener = null;
        }
        boolean ioThreadJoined = true;
        if (ioThread != null) {
            ioThread.shutdown();
            if (ioThreadHandle != null) {
                ioThreadHandle.interrupt();
                try {
                    ioThreadHandle.join(shutdownJoinMs);
                    ioThreadJoined = !ioThreadHandle.isAlive();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    ioThreadJoined = false;
                }
            }
            if (ioThreadJoined) {
                ioThread.closePool();
            } else {
                // Daemon thread still alive -- buffer pool and WebSocket may be
                // in use. Leak them rather than risk a SIGSEGV by freeing them
                // from under a still-running consumer. lastCloseTimedOut is
                // surfaced via the existing {@link #wasLastCloseTimedOut()}
                // accessor so monitoring and tests can observe the condition.
                LOG.error("QwpQueryClient failover cleanup timed out after {} ms; leaking "
                                + "I/O thread, buffer pool, and WebSocket to avoid freeing them "
                                + "from under a running daemon. Common cause: a batch handler "
                                + "that never returns.",
                        shutdownJoinMs);
                lastCloseTimedOut = true;
            }
            ioThread = null;
            ioThreadHandle = null;
        }
        // Only close the socket if the I/O thread has actually exited; otherwise
        // the daemon may still be reading from it.
        if (ioThreadJoined && webSocketClient != null) {
            webSocketClient.close();
        }
        webSocketClient = null;
        serverInfo = null;
        currentEndpointIndex = -1;
    }

    private void connectToEndpoint(Endpoint ep) {
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
        webSocketClient.setConnectTimeout(connectTimeoutMs);
        runUpgradeWithTimeout(ep);
        negotiatedQwpVersion = webSocketClient.getServerQwpVersion();
        negotiatedZstdLevel = webSocketClient.getServerNegotiatedZstdLevel();

        // The server sends SERVER_INFO as the first WebSocket frame after the
        // upgrade response. Consume it synchronously on the user thread before
        // spawning the I/O thread, so the role filter can run without any
        // cross-thread synchronisation and so a mismatched role doesn't waste
        // the I/O thread setup + teardown.
        serverInfo = receiveServerInfoSync();

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
    }

    private void executeImpl(CharSequence sql, QwpBindSetter binds, QwpColumnBatchHandler handler, boolean resetSymbolDict) {
        if (closedFlag.get()) {
            throw new IllegalStateException("QwpQueryClient is closed");
        }
        if (!connected) {
            throw new IllegalStateException("QwpQueryClient not connected; call connect() first");
        }
        hostTracker.beginRound(false);
        long failoverDeadlineNanos;
        if (failoverMaxDurationMs > 0L) {
            long durationNanos = failoverMaxDurationMs > Long.MAX_VALUE / 1_000_000L
                    ? Long.MAX_VALUE
                    : failoverMaxDurationMs * 1_000_000L;
            long start = System.nanoTime();
            failoverDeadlineNanos = start + durationNanos < start
                    ? Long.MAX_VALUE
                    : start + durationNanos;
        } else {
            failoverDeadlineNanos = Long.MAX_VALUE;
        }
        int attempt = 0;
        while (true) {
            attempt++;
            FailoverProbeHandler probe = new FailoverProbeHandler(handler);
            executeOnce(sql, binds, probe, resetSymbolDict);
            if (!probe.transportFailureIntercepted) {
                return;
            }
            if (!failoverEnabled) {
                handler.onError(probe.interceptedRequestId, probe.interceptedStatus, probe.interceptedMessage);
                return;
            }
            if (attempt >= failoverMaxAttempts || System.nanoTime() - failoverDeadlineNanos >= 0) {
                int failovers = Math.max(0, attempt - 1);
                handler.onError(probe.interceptedRequestId, probe.interceptedStatus,
                        "transport failure after " + attempt + " execute attempt"
                                + (attempt == 1 ? "" : "s") + " ("
                                + failovers + " failover reconnect"
                                + (failovers == 1 ? "" : "s") + "); last error: "
                                + probe.interceptedMessage);
                return;
            }
            int failedIndex = currentEndpointIndex;
            if (hostTracker != null && failedIndex >= 0) {
                hostTracker.recordMidStreamFailure(failedIndex);
            }
            cleanupFailedConnect();
            connected = false;
            if (failoverInitialBackoffMs > 0L) {
                long base = failoverInitialBackoffMs << Math.min(attempt - 1, 30);
                if (base < 0L) base = failoverMaxBackoffMs;
                long capped = Math.min(base, failoverMaxBackoffMs);
                long delay;
                if (capped > 0L) {
                    synchronized (failoverRandom) {
                        delay = (failoverRandom.nextLong() & Long.MAX_VALUE) % capped;
                    }
                } else {
                    delay = 0L;
                }
                long remainingNanos = failoverDeadlineNanos - System.nanoTime();
                long remaining = remainingNanos <= 0L ? 0L : remainingNanos / 1_000_000L;
                if (remainingNanos <= 0L) {
                    int failovers = Math.max(0, attempt - 1);
                    handler.onError(probe.interceptedRequestId, probe.interceptedStatus,
                            "transport failure after " + attempt + " execute attempt"
                                    + (attempt == 1 ? "" : "s") + " ("
                                    + failovers + " failover reconnect"
                                    + (failovers == 1 ? "" : "s") + "); last error: "
                                    + probe.interceptedMessage);
                    return;
                }
                if (delay > remaining) {
                    delay = remaining;
                }
                if (delay > 0L) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        handler.onError(probe.interceptedRequestId, probe.interceptedStatus,
                                "failover interrupted while backing off after attempt "
                                        + attempt + "; last error: " + probe.interceptedMessage);
                        return;
                    }
                }
            }
            try {
                reconnectViaTracker();
            } catch (QwpAuthFailedException authErr) {
                // failover.md S6: AuthError is terminal across all hosts.
                // Credentials are cluster-wide, so retrying floods server logs
                // without recovery. Surface a distinct message so monitoring
                // can pull auth incidents apart from generic transport failures.
                handler.onError(probe.interceptedRequestId, probe.interceptedStatus,
                        "auth failure during failover reconnect [host="
                                + authErr.getHost() + ':' + authErr.getPort()
                                + ", status=" + authErr.getStatusCode()
                                + ", last error: " + probe.interceptedMessage + ']');
                return;
            } catch (RuntimeException reconnectErr) {
                handler.onError(probe.interceptedRequestId, probe.interceptedStatus,
                        "failover reconnect failed after " + attempt + " attempt"
                                + (attempt == 1 ? "" : "s") + " [last error: "
                                + probe.interceptedMessage + ", reconnect error: "
                                + reconnectErr.getMessage() + ']');
                return;
            }
            handler.onFailoverReset(probe.interceptedRequestId, serverInfo);
        }
    }

    /**
     * Inner loop for a single query attempt. Driven by {@link #execute}; wraps
     * the user's handler in a {@link FailoverProbeHandler} so that the outer
     * loop can intercept transport failures before they reach the user.
     */
    private void executeOnce(CharSequence sql, QwpBindSetter binds, FailoverProbeHandler probe, boolean resetSymbolDict) {
        // Cache the I/O thread reference at entry: close() may null the field while
        // we are inside this loop, so reading the field per-iteration would NPE
        // exactly when the user is mid-execute() and close() races. The queue and
        // pool the cached reference owns are still drained safely by closePool()
        // before close() returns.
        QwpEgressIoThread io = ioThread;
        if (io == null) {
            probe.onError(-1L, WebSocketResponse.STATUS_INTERNAL_ERROR, "QwpQueryClient is closed");
            return;
        }
        GenerationListener listener = currentGenerationListener;
        TerminalFailure tf = listener != null ? listener.get() : null;
        if (tf != null) {
            probe.markTransportFailure(-1L, tf.status, tf.message);
            return;
        }
        bindValues.reset();
        if (binds != null) {
            try {
                binds.apply(bindValues);
            } catch (RuntimeException e) {
                // Surface user-side bind errors through the handler contract rather
                // than propagating out of execute. Keeps error handling consistent
                // with the rest of the API and avoids leaving the client in a half-
                // prepared state for the next call. Bind encoding failures are NOT
                // transport failures -- they're deterministic on the user thread --
                // so they must not trigger failover.
                bindValues.reset();
                probe.deliverFinal(-1L,
                        "bind encoding failed: " + e.getMessage());
                return;
            }
        }
        long requestId = nextRequestId++;
        currentRequestId = requestId;
        // Honor a cancel that arrived during the dispatch window. The latch
        // is intentionally not cleared here: if this attempt fails over,
        // the retry must also be cancelled. The latch is cleared once at
        // the outermost execute() entry.
        if (pendingCancel) {
            io.requestCancel(requestId);
        }
        try {
            io.submitQuery(sql, requestId, initialCreditBytes, bindValues.count(), bindValues.bufferPtr(), bindValues.bufferLen(),
                    resolveQueryFlags(resetSymbolDict));
            while (true) {
                QueryEvent ev = io.takeEvent();
                try {
                    switch (ev.kind) {
                        case QueryEvent.KIND_BATCH:
                            try {
                                probe.onBatch(ev.buffer.batch);
                            } finally {
                                io.releaseBuffer(ev.buffer);
                            }
                            break;
                        case QueryEvent.KIND_END:
                            probe.onEnd(requestId, ev.totalRows);
                            return;
                        case QueryEvent.KIND_EXEC_DONE:
                            probe.onExecDone(requestId, ev.opType, ev.rowsAffected);
                            return;
                        case QueryEvent.KIND_ERROR:
                            probe.onError(requestId, ev.errorStatus, ev.errorMessage);
                            return;
                        case QueryEvent.KIND_TRANSPORT_ERROR:
                            probe.markTransportFailure(requestId, ev.errorStatus, ev.errorMessage);
                            return;
                        default:
                            probe.onError(requestId, WebSocketResponse.STATUS_INTERNAL_ERROR, "unknown event kind " + ev.kind);
                            return;
                    }
                } finally {
                    // Return the event to the I/O thread's pool so steady-state
                    // publishes stay allocation-free. Safe even if the handler
                    // threw: releaseEvent clears the event's fields and offers
                    // it to the pool, with overflow silently dropped.
                    io.releaseEvent(ev);
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // Interrupt on the user thread is not a transport failure; surface directly.
            probe.deliverFinal(requestId, "interrupted while waiting for server response");
        } finally {
            currentRequestId = -1L;
        }
    }

    /**
     * Allocates and immediately frees a {@code ZSTD_DCtx} so that any
     * {@link UnsatisfiedLinkError} from a client build that doesn't include
     * the bundled libzstd surfaces synchronously on the user thread at
     * {@code connect()} time. Closes the just-opened WebSocket on failure so
     * the caller doesn't inherit a half-open socket.
     */
    private void probeZstdAvailable() {
        long dctx = 0;
        try {
            try {
                dctx = Zstd.createDCtx();
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
            if (dctx == 0) {
                LOG.error("zstd createDCtx returned 0 (native allocation failure); aborting connect");
                if (webSocketClient != null) {
                    webSocketClient.close();
                    webSocketClient = null;
                }
                throw new HttpClientException("zstd decompression context allocation failed; "
                        + "cannot accept compressed batches. Set compression=raw on the connection "
                        + "string to disable compression, or retry once memory pressure subsides.");
            }
        } finally {
            if (dctx != 0) {
                Zstd.freeDCtx(dctx);
            }
        }
    }

    private QwpServerInfo receiveServerInfoSync() {
        ServerInfoReceiver receiver = new ServerInfoReceiver();
        long deadlineMs = System.currentTimeMillis() + serverInfoTimeoutMs;
        while (receiver.info == null
                && receiver.decodeError == null
                && receiver.closeCode < 0
                && receiver.unexpectedFrame == null) {
            int remaining = (int) (deadlineMs - System.currentTimeMillis());
            if (remaining <= 0) {
                throw new HttpClientException("timeout waiting for SERVER_INFO frame");
            }
            webSocketClient.receiveFrame(receiver, remaining);
        }
        if (receiver.closeCode >= 0) {
            throw new HttpClientException("server closed connection before SERVER_INFO [code="
                    + receiver.closeCode + ", reason=" + receiver.closeReason + ']');
        }
        if (receiver.decodeError != null) {
            throw new HttpClientException("SERVER_INFO decode failed: " + receiver.decodeError);
        }
        if (receiver.unexpectedFrame != null) {
            throw new HttpClientException("unexpected frame before SERVER_INFO: " + receiver.unexpectedFrame);
        }
        return receiver.info;
    }

    /**
     * Walks the endpoint list by tracker priority (HEALTHY → UNKNOWN →
     * TRANSIENT_REJECT → TRANSPORT_ERROR → TOPOLOGY_REJECT). The mid-stream
     * failed endpoint was demoted by {@link QwpHostHealthTracker#recordMidStreamFailure}
     * before this method is entered, so in multi-host configurations a different
     * endpoint is preferred; with a single configured endpoint the same host is
     * the only option. After the first round exhausts, classifications other
     * than HEALTHY are forgotten and the list is walked once more so a long-lived
     * client recovers from topology changes.
     * <p>
     * On success, leaves the client in the same state {@link #connect()}
     * produces: {@code connected=true}, {@code ioThread} spawned,
     * {@code serverInfo} populated. On exhaustion, raises the same exceptions
     * as {@link #connect()}.
     */
    private void reconnectViaTracker() {
        int total = endpoints.size();
        lastCloseTimedOut = false;
        hostTracker.beginRound(false);
        QwpServerInfo lastMismatch = null;
        Throwable lastError = null;
        boolean retriedAfterReset = false;
        while (true) {
            int i = hostTracker.pickNext();
            if (i < 0) {
                if (!retriedAfterReset) {
                    hostTracker.beginRound(true);
                    retriedAfterReset = true;
                    continue;
                }
                break;
            }
            Endpoint ep = endpoints.get(i);
            try {
                connectToEndpoint(ep);
            } catch (QwpAuthFailedException ae) {
                cleanupFailedConnect();
                throw ae;
            } catch (QwpIngressRoleRejectedException re) {
                lastError = re;
                hostTracker.recordZone(i, re.getZoneId());
                hostTracker.recordRoleReject(i, re.isTransient());
                cleanupFailedConnect();
                continue;
            } catch (RuntimeException e) {
                lastError = e;
                hostTracker.recordTransportError(i);
                cleanupFailedConnect();
                continue;
            }
            // info is non-null: connectToEndpoint() returns only after
            // receiveServerInfoSync() set serverInfo (it returns non-null or throws).
            QwpServerInfo info = serverInfo;
            if ((info.getCapabilities() & QwpEgressMsgKind.CAP_ZONE) != 0) {
                hostTracker.recordZone(i, info.getZoneId());
            }
            if (!matchesTarget(info.getRole(), target)) {
                lastMismatch = info;
                boolean isTransient = info.getRole() == QwpEgressMsgKind.ROLE_PRIMARY_CATCHUP;
                hostTracker.recordRoleReject(i, isTransient);
                cleanupFailedConnect();
                continue;
            }
            spawnIoThread();
            hostTracker.recordSuccess(i);
            currentEndpointIndex = i;
            connected = true;
            return;
        }
        if (lastMismatch != null) {
            throw new QwpRoleMismatchException(target, lastMismatch,
                    "no endpoint matches target=" + target + " on failover; last observed role="
                            + QwpServerInfo.roleName(lastMismatch.getRole()));
        }
        throw new HttpClientException(
                "all QWP endpoints unreachable on failover [count=" + total
                        + ", lastError=" + (lastError == null ? "<none>" : lastError.getMessage()) + ']');
    }

    private long resolveQueryFlags(boolean resetSymbolDict) {
        if (!resetSymbolDict) {
            return 0L;
        }
        QwpServerInfo info = serverInfo;
        return info != null && (info.getCapabilities() & QwpEgressMsgKind.CAP_QUERY_FLAGS) != 0
                ? QwpEgressMsgKind.QUERY_FLAG_RESET_DICT
                : 0L;
    }

    private void runUpgradeWithTimeout(Endpoint ep) {
        // Connect first, OUTSIDE the upgrade try. A connect-phase failure --
        // including a connect_timeout overage flagged via flagAsTimeout() -- must
        // keep its own message ("connect timed out ...") and must NOT be relabeled
        // as an auth_timeout overage below. doConnect() tears down its own socket
        // on failure; the failover walker treats the propagated HttpClientException
        // as a transport error and moves on to the next endpoint.
        webSocketClient.connect(ep.host, ep.port);

        int timeoutMs = (int) Math.min(authTimeoutMs, Integer.MAX_VALUE);
        try {
            webSocketClient.upgrade(DEFAULT_ENDPOINT_PATH, timeoutMs, authorizationHeader);
        } catch (HttpClientException ex) {
            if (ex.isTimeout()) {
                // Reachable only for an upgrade/auth-phase timeout now, so the
                // auth_timeout attribution is accurate.
                HttpClientException timeout = new HttpClientException("WebSocket upgrade to ")
                        .put(ep.host).put(':').put(ep.port)
                        .put(" exceeded auth_timeout=").put(authTimeoutMs).put("ms");
                timeout.initCause(ex);
                timeout.flagAsTimeout();
                throw timeout;
            }
            throw QwpUpgradeFailures.classify(webSocketClient, ep.host, ep.port, ex);
        }
    }

    private void spawnIoThread() {
        // Wire a fresh generation-scoped listener into this I/O thread. Each
        // listener owns its own terminal-failure latch, so even if a dying
        // I/O thread slips a late onTerminalFailure callback past the orphan
        // flag, the write lands on a ref nobody reads rather than poisoning
        // the next connection's state.
        GenerationListener listener = new GenerationListener();
        currentGenerationListener = listener;
        // Publish ioThreadHandle BEFORE ioThread. Both fields are volatile;
        // a third-thread close() that reads ioThread != null is guaranteed
        // (via the volatile happens-before edge on the write below) to also
        // see the non-null ioThreadHandle. Without this ordering, close()
        // could observe (ioThread != null, ioThreadHandle == null), skip the
        // join, and call closePool() / Misc.free with the daemon still alive.
        QwpEgressIoThread newIoThread = new QwpEgressIoThread(webSocketClient, bufferPoolSize, listener);
        Thread handle = new Thread(newIoThread, "qwp-egress-io");
        handle.setDaemon(true);
        handle.start();
        ioThreadHandle = handle;
        ioThread = newIoThread;
    }

    private static final class Endpoint {
        final String host;
        final int port;

        Endpoint(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }

    /**
     * Wraps the user's handler so {@link #execute} can classify a transport
     * failure (surfaced as {@link QueryEvent#KIND_TRANSPORT_ERROR}) and route
     * it into the failover loop instead of to the user. Server-emitted
     * {@code QUERY_ERROR} frames ({@link QueryEvent#KIND_ERROR}) go straight
     * through {@code onError} to the user. The two paths are kept strictly
     * separate at the event-dispatch layer so the classification is driven by
     * the I/O thread's decision at emit time, not reconstructed from a side-
     * channel latch.
     * <p>
     * Per-query state is fresh for every call of {@link #execute}; the caller
     * instantiates one probe per attempt.
     */
    private static final class FailoverProbeHandler implements QwpColumnBatchHandler {
        final QwpColumnBatchHandler delegate;
        String interceptedMessage;
        long interceptedRequestId = -1L;
        byte interceptedStatus;
        boolean transportFailureIntercepted;

        FailoverProbeHandler(QwpColumnBatchHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onBatch(QwpColumnBatch batch) {
            delegate.onBatch(batch);
        }

        @Override
        public void onEnd(long totalRows) {
            delegate.onEnd(totalRows);
        }

        @Override
        public void onEnd(long requestId, long totalRows) {
            delegate.onEnd(requestId, totalRows);
        }

        @Override
        public void onError(byte status, String message) {
            // Server-emitted QUERY_ERROR. Pass straight through. Transport
            // failures are delivered via markTransportFailure, not here.
            delegate.onError(status, message);
        }

        @Override
        public void onError(long requestId, byte status, String message) {
            delegate.onError(requestId, status, message);
        }

        @Override
        public void onExecDone(short opType, long rowsAffected) {
            delegate.onExecDone(opType, rowsAffected);
        }

        @Override
        public void onExecDone(long requestId, short opType, long rowsAffected) {
            delegate.onExecDone(requestId, opType, rowsAffected);
        }

        @Override
        public void onFailoverReset(QwpServerInfo newNode) {
            delegate.onFailoverReset(newNode);
        }

        @Override
        public void onFailoverReset(long requestId, QwpServerInfo newNode) {
            delegate.onFailoverReset(requestId, newNode);
        }

        /**
         * Bypass the interception logic and deliver the error straight to the
         * user. Used for failures that are not transport-related (bind-encode
         * errors, interrupts) so they don't trigger a spurious failover.
         */
        void deliverFinal(long requestId, String message) {
            delegate.onError(requestId, WebSocketResponse.STATUS_INTERNAL_ERROR, message);
        }

        /**
         * Records a transport-level failure so the outer {@code execute()}
         * loop can decide whether to replay or surface to the user. Does
         * NOT call any user callback -- the user sees either a clean replay
         * (failover=on + reconnect succeeds) or a single final {@code onError}
         * (failover=off or reconnect exhausted).
         */
        void markTransportFailure(long requestId, byte status, String message) {
            transportFailureIntercepted = true;
            interceptedRequestId = requestId;
            interceptedStatus = status;
            interceptedMessage = message;
        }
    }

    /**
     * One-shot terminal-failure listener scoped to a single {@link
     * QwpEgressIoThread} instance. Each instance owns its own
     * {@link AtomicReference} latch so an orphaned-but-in-flight callback
     * writes into its own generation's slot and cannot poison a successor
     * generation. The orphan flag is advisory: a late callback from a
     * dying I/O thread observes it and short-circuits; if it squeaks past
     * the check and into the CAS, the write lands on a ref that nobody
     * reads, so no poisoning occurs either way.
     * <p>
     * {@link #cleanupFailedConnect} calls {@link #orphan()} on the outgoing
     * listener before creating the next generation.
     */
    private static final class GenerationListener implements QwpEgressIoThread.TerminalFailureListener {
        private final AtomicReference<TerminalFailure> target = new AtomicReference<>();
        private volatile boolean orphaned;

        @Override
        public void onTerminalFailure(byte status, String message) {
            if (orphaned) {
                return;
            }
            target.compareAndSet(null, new TerminalFailure(status, message));
        }

        TerminalFailure get() {
            return target.get();
        }

        void orphan() {
            this.orphaned = true;
        }
    }

    /**
     * Buffers the inbound {@code SERVER_INFO} frame on the user thread during
     * {@link QwpQueryClient#connect()}. Exactly one of {@link #info},
     * {@link #decodeError}, {@link #closeCode} gets set per connect attempt;
     * the outer loop polls the fields to decide whether to accept the endpoint,
     * try the next one, or raise.
     */
    private static final class ServerInfoReceiver implements WebSocketFrameHandler {
        int closeCode = -1;
        String closeReason;
        String decodeError;
        QwpServerInfo info;
        String unexpectedFrame;

        @Override
        public void onBinaryMessage(long payloadPtr, int payloadLen) {
            if (info != null) {
                // Server sent a second binary frame before the client consumed
                // the first. Nothing legitimate does this; flag it so the
                // caller gets a clear diagnostic instead of a silent ignore.
                if (unexpectedFrame == null) {
                    unexpectedFrame = "second binary frame before consumer advanced";
                }
                return;
            }
            if (decodeError != null) {
                return;
            }
            try {
                info = QwpServerInfoDecoder.decode(payloadPtr, payloadLen);
            } catch (QwpDecodeException e) {
                decodeError = e.getMessage();
            }
        }

        @Override
        public void onClose(int code, String reason) {
            closeCode = code;
            closeReason = reason;
        }

        @Override
        public void onTextMessage(long payloadPtr, int payloadLen) {
            // A text frame before the SERVER_INFO binary frame is non-standard
            // for this protocol; flag it so the connect() error is specific
            // instead of a bland timeout.
            if (unexpectedFrame == null) {
                unexpectedFrame = "text frame";
            }
        }
    }

    private static final class TerminalFailure {
        final String message;
        final byte status;

        TerminalFailure(byte status, String message) {
            this.status = status;
            this.message = message;
        }
    }
}
