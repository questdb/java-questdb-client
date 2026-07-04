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
import io.questdb.client.Sender;
import io.questdb.client.SenderConnectionEvent;
import io.questdb.client.SenderConnectionListener;
import io.questdb.client.SenderError;
import io.questdb.client.SenderErrorHandler;
import io.questdb.client.SenderProgressHandler;
import io.questdb.client.cairo.TableUtils;
import io.questdb.client.cutlass.http.client.HttpClientException;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketClientFactory;
import io.questdb.client.cutlass.http.client.WebSocketUpgradeException;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerListener;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerPool;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.DefaultSenderConnectionListener;
import io.questdb.client.cutlass.qwp.client.sf.cursor.DefaultSenderErrorHandler;
import io.questdb.client.cutlass.qwp.client.sf.cursor.DefaultSenderProgressHandler;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderConnectionDispatcher;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderErrorDispatcher;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderProgressDispatcher;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.CharSequenceObjHashMap;
import io.questdb.client.std.Chars;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.Misc;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.bytes.DirectByteSlice;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * QWP v1 WebSocket client sender for streaming data to QuestDB.
 * <p>
 * This sender uses a double-buffering scheme with asynchronous I/O for high throughput:
 * <ul>
 *   <li>User thread writes rows to the active microbatch buffer</li>
 *   <li>When buffer is full (row count, byte size, or age), it's sealed and enqueued</li>
 *   <li>A dedicated I/O thread sends batches asynchronously</li>
 *   <li>Double-buffering ensures one buffer is always available for writing</li>
 * </ul>
 * <p>
 * Configuration options:
 * <ul>
 *   <li>{@code autoFlushRows} - Maximum rows per batch (default: 1000)</li>
 *   <li>{@code autoFlushBytes} - Maximum bytes per batch (default: disabled)</li>
 *   <li>{@code autoFlushIntervalNanos} - Maximum age before auto-flush (default: 100ms)</li>
 * </ul>
 * <p>
 * Example usage:
 * <pre>
 * try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", 9000)) {
 *     for (int i = 0; i &lt; 100_000; i++) {
 *         sender.table("metrics")
 *               .symbol("host", "server-" + (i % 10))
 *               .doubleColumn("cpu", Math.random() * 100)
 *               .atNow();
 *         // Rows are batched and sent asynchronously!
 *     }
 *     // flush() waits for all pending batches to be sent
 *     sender.flush();
 * }
 * </pre>
 * <p>
 * Failure handling: after this sender has established a WebSocket connection,
 * any WebSocket send failure, receive failure, ACK timeout, server error ACK,
 * invalid ACK, or server close is terminal for this sender instance. The first
 * such failure is retained and subsequent public operations rethrow the same
 * {@link LineSenderException}. {@link #reset()} only discards buffered row data;
 * it does not recover a terminal WebSocket failure. To resume sending after a
 * terminal WebSocket failure, close this sender and create a new instance.
 * <p>
 * Initial connection failures are not retained as terminal sender state; a later
 * operation may try to connect again.
 */
public class QwpWebSocketSender implements Sender {

    public static final long DEFAULT_AUTH_TIMEOUT_MS = 15_000L;
    // Soft per-batch byte budget. Trips a flush when raw column-buffer bytes
    // cross the threshold, well before the server's 16 MB wire cap. Wide-row
    // senders need this trigger (not autoFlushRows alone) to avoid producing
    // a batch the server will reject with STATUS_PARSE_ERROR.
    public static final int DEFAULT_AUTO_FLUSH_BYTES = 8 * 1024 * 1024;
    public static final long DEFAULT_AUTO_FLUSH_INTERVAL_NANOS = 100_000_000L; // 100ms
    public static final int DEFAULT_AUTO_FLUSH_ROWS = 1_000;
    // Finite fallback (ms) for BACKGROUND (drainer) TCP connects when the
    // user left connect_timeout unset. See effectiveConnectTimeoutMs.
    public static final int DEFAULT_BACKGROUND_CONNECT_TIMEOUT_MS = 15_000;
    private static final int DEFAULT_BUFFER_SIZE = 8192;
    private static final int DEFAULT_MICROBATCH_BUFFER_SIZE = 1024 * 1024; // 1MB
    private static final Logger LOG = LoggerFactory.getLogger(QwpWebSocketSender.class);
    private static final int MAX_TABLE_NAME_LENGTH = 127;
    // sf-client.md section 4.4 floor: drop-oldest under bursts needs a wide
    // enough window to preserve the trailing category distribution.
    private static final int MIN_ERROR_INBOX_CAPACITY = 16;
    private static final String WRITE_PATH = "/write/v4";
    private final String authorizationHeader;
    private final int autoFlushBytes;
    private final long autoFlushIntervalNanos;
    // Auto-flush configuration
    private final int autoFlushRows;
    private final MicrobatchBuffer buffer1;
    private final AtomicReference<LineSenderException> connectionError = new AtomicReference<>();
    private final Decimal256 currentDecimal256 = new Decimal256();
    // Encoder for QWP v1 messages
    private final QwpWebSocketEncoder encoder;
    private final List<Endpoint> endpoints;
    // Global symbol dictionary for delta encoding
    private final GlobalSymbolDictionary globalSymbolDictionary;
    // Serializes FOREGROUND connect walks only (see buildAndConnect): the
    // shared-round state in hostTracker (pickNext/beginRound/attempted
    // bits), roundSeq, roundConnectAttemptSeq, and the foreground lifecycle
    // commits (currentEndpointIdx, hasEverConnected, cap-derived sizing)
    // all have exactly one writer -- the foreground walk -- and foreground
    // walks cannot overlap by construction (the I/O loop is single-threaded
    // and the user-thread initial connect completes before the loop
    // starts); the lock is cheap insurance for that invariant. Background
    // (drainer) walks take NO lock at all: they walk a private
    // QwpHostHealthTracker.RoundCursor and record health-only results, so
    // no network I/O ever runs under a sender-wide lock for background
    // work, and neither the foreground's reconnect nor close() can queue
    // behind a drainer's endpoint walk.
    private final ReentrantLock connectWalkLock = new ReentrantLock();
    private final QwpHostHealthTracker hostTracker;
    private final CharSequenceObjHashMap<QwpTableBuffer> tableBuffers;
    // null means plain text (no TLS)
    private final ClientTlsConfiguration tlsConfig;
    private MicrobatchBuffer activeBuffer;
    private long authTimeoutMs = DEFAULT_AUTH_TIMEOUT_MS;
    // Upper bound (ms) on each TCP connect attempt. 0 (default) falls back to
    // the OS connect timeout. Applied to every WebSocketClient before connect.
    private int connectTimeoutMs = 0;
    // Double-buffering for async I/O
    private MicrobatchBuffer buffer0;
    // Cached column references to avoid repeated hashmap lookups
    private QwpTableBuffer.ColumnBuffer cachedTimestampColumn;
    private QwpTableBuffer.ColumnBuffer cachedTimestampNanosColumn;
    // WebSocket client (zero-GC native implementation)
    private WebSocketClient client;
    // Test seam: when non-null, buildAndConnect obtains its per-attempt
    // client here instead of WebSocketClientFactory, so JVM-error cleanup
    // tests can observe close() on a client whose connect() throws Error.
    // Null in production; set reflectively by tests.
    @TestOnly
    private volatile java.util.function.Supplier<WebSocketClient> clientFactoryOverride;
    // close() drain timeout in millis. Default applied at construction.
    // 0 or -1 means "fast close" (skip the drain); otherwise close blocks
    // up to this many millis for ackedFsn to catch up to publishedFsn.
    private long closeFlushTimeoutMillis = 5_000L;
    private volatile boolean closed;
    private boolean connected;
    private SenderConnectionDispatcher connectionDispatcher;
    // Async-delivery sink for SenderConnectionEvent notifications. Default
    // installed at construction; the builder hook can swap before connect()
    // runs, and post-connect setConnectionListener() propagates to the live
    // dispatcher.
    private SenderConnectionListener connectionListener = DefaultSenderConnectionListener.INSTANCE;
    private int connectionListenerInboxCapacity = SenderConnectionDispatcher.DEFAULT_CAPACITY;
    // Track max global symbol ID used in current batch (for delta calculation)
    private int currentBatchMaxSymbolId = -1;
    private volatile int currentEndpointIdx = -1;
    private QwpTableBuffer currentTableBuffer;
    // Tracks currentTableBuffer.getBufferedBytes() at the last point pendingBytes
    // was made consistent (end of sendRow(), or right after a table switch).
    // sendRow() advances pendingBytes by (now - snapshot) and re-snaps, which
    // keeps pendingBytes correct without re-walking every table per row.
    // The invariant holds because column setters and rollbackRow/cancelRow
    // only ever touch currentTableBuffer between the consistency points, and
    // table() bans switches while a row is in progress.
    private long currentTableBufferSnapshotBytes;
    private String currentTableName;
    // Cursor SF engine: the producer (user thread) writes encoded QWP frames
    // into the engine's mmap'd ring; the cursorSendLoop is the I/O thread
    // that walks the ring and sends frames.
    private CursorSendEngine cursorEngine;
    private CursorWebSocketSendLoop cursorSendLoop;
    private boolean deferCommit;
    // User-supplied observer for background orphan-slot drainer events.
    // Volatile: written by setDrainerListener (any thread, before or after
    // startOrphanDrainers) and read at pool-creation time. Null -> drainers
    // run without a listener.
    private volatile BackgroundDrainerListener drainerListener;
    // Orphan-slot drainer pool. Non-null only when the builder requested
    // drain_orphans=true AND we have a slot path to scan against. Closed
    // alongside the cursor send loop in close().
    private BackgroundDrainerPool drainerPool;
    // Keepalive PING cadence used by the I/O loop while
    // request_durable_ack=on AND there are pending durable-ack
    // confirmations. Default mirrors the loop's spec value; 0 or negative
    // disables keepalive PINGs entirely.
    private long durableAckKeepaliveIntervalMillis =
            CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS;
    // Effective per-batch soft-flush threshold in raw column-buffer bytes.
    // Initially equals autoFlushBytes; lowered to fit under the server's
    // advertised X-QWP-Max-Batch-Size at handshake so the wire payload stays
    // under the server's cap even with encoding overhead. Volatile because the
    // I/O thread writes this inside buildAndConnect on every successful
    // FOREGROUND (re)connect -- background drainer connects never touch it --
    // while the producer thread reads it from sendRow without
    // holding the sender monitor.
    private volatile int effectiveAutoFlushBytes;
    private SenderErrorDispatcher errorDispatcher;
    // Async-delivery sink for SenderError notifications. Default-constructed
    // here with the loud-not-silent default handler; a builder hook can swap
    // this before connect() runs.
    private SenderErrorHandler errorHandler = DefaultSenderErrorHandler.INSTANCE;
    private int errorInboxCapacity = SenderErrorDispatcher.DEFAULT_CAPACITY;
    private long firstPendingRowTimeNanos;
    private boolean hasDeferredMessages;
    // FSN of the last commit-bearing (non-FLAG_DEFER_COMMIT) frame this session
    // published, or -1 when none. Frames above it are deferred and uncommitted:
    // the server withholds their acks by design (their rows are rolled back on
    // any error, demote, or disconnect), so close-time drains must never wait
    // for them. Updated on every non-deferred publish; combined with the
    // engine's recovered boundary in drainOnClose.
    private long lastCommitBoundaryFsn = -1L;
    // Stickys true once any successful FOREGROUND connect has happened
    // (background drainer connects never set it). Drives the
    // CONNECTED-vs-RECONNECTED-vs-FAILED_OVER classification at the success
    // point in buildAndConnect.
    private boolean hasEverConnected;
    // OFF   → startup connect failure is immediately terminal (default).
    // SYNC  → startup connect retries with backoff on the user thread,
    //         bounded by reconnect_max_duration_millis; auth failures
    //         still terminal.
    // ASYNC → user thread does not connect at all. The I/O thread runs
    //         the reconnect loop in the background, indefinitely
    //         (Invariant B); terminal failures (auth/upgrade reject)
    //         are delivered to the SenderError dispatcher rather than
    //         thrown from the constructor.
    private Sender.InitialConnectMode initialConnectMode = Sender.InitialConnectMode.OFF;
    private boolean ownsCursorEngine;
    private long pendingBytes;
    // Set true by close() once the SF slot flock has been released (the normal
    // teardown path). Stays false if close() bailed early with the I/O thread
    // still running -- then cursorEngine.close() never ran and the flock is
    // still held, so the owning pool MUST keep the slot reserved rather than
    // hand the still-locked dir to the next borrow ("sf slot already in use").
    private boolean slotLockReleased;
    private int pendingRowCount;
    private SenderProgressDispatcher progressDispatcher;
    // Async-delivery sink for ack-watermark advances. Default no-op; a
    // setProgressHandler call before connect() swaps in a real handler.
    private SenderProgressHandler progressHandler = DefaultSenderProgressHandler.INSTANCE;
    // Poison-frame detector threshold forwarded to the cursor send loop and to
    // every background drainer (connect-string key max_frame_rejections).
    private int maxFrameRejections = CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS;
    // Minimum wall-clock dwell before poison escalation, forwarded alongside
    // maxFrameRejections (connect-string key poison_min_escalation_window_millis).
    private long poisonMinEscalationWindowMillis =
            CursorWebSocketSendLoop.DEFAULT_POISON_MIN_ESCALATION_WINDOW_MILLIS;
    private long reconnectInitialBackoffMillis =
            CursorWebSocketSendLoop.DEFAULT_RECONNECT_INITIAL_BACKOFF_MILLIS;
    private long reconnectMaxBackoffMillis =
            CursorWebSocketSendLoop.DEFAULT_RECONNECT_MAX_BACKOFF_MILLIS;
    // Reconnect policy. Defaults match CursorWebSocketSendLoop's per-spec
    // values; Sender.build can override via the new connect overload.
    private long reconnectMaxDurationMillis =
            CursorWebSocketSendLoop.DEFAULT_RECONNECT_MAX_DURATION_MILLIS;
    private boolean requestDurableAck;
    // Monotonic per-attempt counter snapshotted onto every connection event
    // fired from buildAndConnect. Counts every FOREGROUND endpoint try --
    // successes and failures alike -- across this sender's lifetime.
    // Background (drainer) walks fire no events and do not advance it.
    private long roundConnectAttemptSeq;
    // Monotonic per-round counter incremented inside buildAndConnect on each
    // beginRound(true) call. roundSeq=1 is the first round; CONNECTED in the
    // first round indicates the initial connect.
    private long roundSeq;
    // When true, auto-flush sends messages with FLAG_DEFER_COMMIT and only
    // explicit flush() triggers the server-side commit. Enables accumulating
    // arbitrarily large datasets that exceed the server's recv buffer.
    private boolean transactional;
    // Server-advertised hard cap on QWP ingest payload bytes, captured from
    // X-QWP-Max-Batch-Size on each successful FOREGROUND handshake (a
    // background drainer's endpoint cap is irrelevant to the producer's wire). 0 when the server
    // did not advertise the header (older builds); the sender then falls back
    // to its locally configured budget. Volatile because buildAndConnect can
    // refresh this from the cursor I/O thread on a mid-stream reconnect while
    // sendRow reads it on the producer thread with no synchronization.
    private volatile int serverMaxBatchSize;

    private QwpWebSocketSender(
            List<Endpoint> endpoints,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            String authorizationHeader
    ) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must be non-empty");
        }
        this.endpoints = Collections.unmodifiableList(new ArrayList<>(endpoints));
        this.hostTracker = new QwpHostHealthTracker(this.endpoints.size());
        this.authorizationHeader = authorizationHeader;
        this.tlsConfig = tlsConfig;
        this.encoder = new QwpWebSocketEncoder(DEFAULT_BUFFER_SIZE);
        this.tableBuffers = new CharSequenceObjHashMap<>();
        this.currentTableBuffer = null;
        this.currentTableBufferSnapshotBytes = 0;
        this.currentTableName = null;
        this.connected = false;
        this.closed = false;
        this.autoFlushRows = autoFlushRows;
        this.autoFlushBytes = autoFlushBytes;
        // Until the handshake completes, honor the configured budget verbatim.
        // applyServerBatchSizeLimit() clamps this on connect once the server's
        // X-QWP-Max-Batch-Size is known.
        this.effectiveAutoFlushBytes = autoFlushBytes;
        this.autoFlushIntervalNanos = autoFlushIntervalNanos;
        this.globalSymbolDictionary = new GlobalSymbolDictionary();

        int microbatchBufferSize = Math.max(DEFAULT_MICROBATCH_BUFFER_SIZE, autoFlushBytes * 2);
        try {
            this.buffer0 = new MicrobatchBuffer(microbatchBufferSize);
            this.buffer1 = new MicrobatchBuffer(microbatchBufferSize);
        } catch (Throwable t) {
            if (buffer0 != null) {
                buffer0.close();
            }
            encoder.close();
            throw t;
        }
        this.activeBuffer = buffer0;
    }

    /**
     * Creates a new sender and connects to the specified host and port.
     * Uses default auto-flush settings and in-flight window size.
     *
     * @param host server host
     * @param port server HTTP port (WebSocket upgrade happens on same port)
     * @return connected sender
     */
    public static QwpWebSocketSender connect(String host, int port) {
        return connect(host, port, null);
    }

    /**
     * Creates a new sender and connects to the specified host and port.
     * Uses default auto-flush settings and in-flight window size.
     *
     * @param host      server host
     * @param port      server HTTP port
     * @param tlsConfig TLS configuration, or null for plain text
     * @return connected sender
     */
    public static QwpWebSocketSender connect(String host, int port, ClientTlsConfiguration tlsConfig) {
        // Build a memory-mode cursor engine with the same defaults Sender.build
        // uses for an SF-less ws:: connect string (4 MiB segments, 128 MiB cap).
        CursorSendEngine engine = new CursorSendEngine(
                null,
                4L * 1024 * 1024,
                128L * 1024 * 1024,
                CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS
        );
        try {
            return connect(
                    host, port, tlsConfig,
                    DEFAULT_AUTO_FLUSH_ROWS, DEFAULT_AUTO_FLUSH_BYTES, DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                    null,
                    false, engine
            );
        } catch (Throwable t) {
            try {
                engine.close();
            } catch (Throwable ignored) {
                // best-effort
            }
            throw t;
        }
    }

    /**
     * Master connect overload — used by {@code Sender.fromConfig}. Always
     * runs through the cursor SF engine (memory-mode when {@code cursorEngine}
     * was constructed without an {@code sfDir}, file-mode otherwise).
     */
    public static QwpWebSocketSender connect(
            String host,
            int port,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            String authorizationHeader,
            boolean requestDurableAck,
            CursorSendEngine cursorEngine
    ) {
        return connect(host, port, tlsConfig, autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                authorizationHeader,
                requestDurableAck, cursorEngine, 5_000L);
    }

    /**
     * Connect overload that also configures the {@code close()} drain
     * timeout. {@code 0} or {@code -1} disables the drain (fast close);
     * any positive value bounds the wait for {@code ackedFsn} to catch
     * up to {@code publishedFsn} during {@code close()}.
     */
    public static QwpWebSocketSender connect(
            String host,
            int port,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            String authorizationHeader,
            boolean requestDurableAck,
            CursorSendEngine cursorEngine,
            long closeFlushTimeoutMillis
    ) {
        return connect(host, port, tlsConfig, autoFlushRows, autoFlushBytes,
                autoFlushIntervalNanos, authorizationHeader,
                requestDurableAck, cursorEngine,
                closeFlushTimeoutMillis,
                CursorWebSocketSendLoop.DEFAULT_RECONNECT_MAX_DURATION_MILLIS,
                CursorWebSocketSendLoop.DEFAULT_RECONNECT_INITIAL_BACKOFF_MILLIS,
                CursorWebSocketSendLoop.DEFAULT_RECONNECT_MAX_BACKOFF_MILLIS);
    }

    /**
     * Master connect overload — exposes every cursor-pipeline knob the
     * builder can set. The reconnect-policy parameters bound the I/O
     * loop's per-outage retry behavior (see
     * {@link CursorWebSocketSendLoop} javadoc).
     */
    public static QwpWebSocketSender connect(
            String host,
            int port,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            String authorizationHeader,
            boolean requestDurableAck,
            CursorSendEngine cursorEngine,
            long closeFlushTimeoutMillis,
            long reconnectMaxDurationMillis,
            long reconnectInitialBackoffMillis,
            long reconnectMaxBackoffMillis
    ) {
        return connect(host, port, tlsConfig, autoFlushRows, autoFlushBytes,
                autoFlushIntervalNanos, authorizationHeader,
                requestDurableAck, cursorEngine,
                closeFlushTimeoutMillis, reconnectMaxDurationMillis,
                reconnectInitialBackoffMillis, reconnectMaxBackoffMillis,
                Sender.InitialConnectMode.OFF);
    }

    /**
     * Master connect overload — also accepts {@code initialConnectMode}.
     * See {@link Sender.InitialConnectMode} for the value semantics:
     * {@code OFF} fails fast (default), {@code SYNC} retries on the user
     * thread up to the reconnect cap, {@code ASYNC} returns immediately
     * and lets the I/O thread retry in the background.
     */
    public static QwpWebSocketSender connect(
            String host,
            int port,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            String authorizationHeader,
            boolean requestDurableAck,
            CursorSendEngine cursorEngine,
            long closeFlushTimeoutMillis,
            long reconnectMaxDurationMillis,
            long reconnectInitialBackoffMillis,
            long reconnectMaxBackoffMillis,
            Sender.InitialConnectMode initialConnectMode
    ) {
        return connect(host, port, tlsConfig, autoFlushRows, autoFlushBytes,
                autoFlushIntervalNanos, authorizationHeader,
                requestDurableAck, cursorEngine,
                closeFlushTimeoutMillis, reconnectMaxDurationMillis,
                reconnectInitialBackoffMillis, reconnectMaxBackoffMillis,
                initialConnectMode, null, SenderErrorDispatcher.DEFAULT_CAPACITY);
    }

    /**
     * Connect overload with the SenderError dispatcher knobs. {@code errorHandler}
     * may be null to use the loud-not-silent default; {@code errorInboxCapacity}
     * must be {@code >= 1}.
     */
    public static QwpWebSocketSender connect(
            String host,
            int port,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            String authorizationHeader,
            boolean requestDurableAck,
            CursorSendEngine cursorEngine,
            long closeFlushTimeoutMillis,
            long reconnectMaxDurationMillis,
            long reconnectInitialBackoffMillis,
            long reconnectMaxBackoffMillis,
            Sender.InitialConnectMode initialConnectMode,
            SenderErrorHandler errorHandler,
            int errorInboxCapacity
    ) {
        return connect(host, port, tlsConfig, autoFlushRows, autoFlushBytes,
                autoFlushIntervalNanos, authorizationHeader,
                requestDurableAck, cursorEngine,
                closeFlushTimeoutMillis, reconnectMaxDurationMillis,
                reconnectInitialBackoffMillis, reconnectMaxBackoffMillis,
                initialConnectMode, errorHandler, errorInboxCapacity,
                CursorWebSocketSendLoop.DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS);
    }

    /**
     * Master connect overload — also accepts the keepalive PING cadence
     * the I/O loop uses while waiting on STATUS_DURABLE_ACK frames.
     * {@code 0} or negative disables the keepalive entirely (caller takes
     * responsibility for prodding the server).
     */
    public static QwpWebSocketSender connect(
            String host,
            int port,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            String authorizationHeader,
            boolean requestDurableAck,
            CursorSendEngine cursorEngine,
            long closeFlushTimeoutMillis,
            long reconnectMaxDurationMillis,
            long reconnectInitialBackoffMillis,
            long reconnectMaxBackoffMillis,
            Sender.InitialConnectMode initialConnectMode,
            SenderErrorHandler errorHandler,
            int errorInboxCapacity,
            long durableAckKeepaliveIntervalMillis
    ) {
        return connect(
                singleEndpoint(host, port), tlsConfig,
                autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                authorizationHeader,
                requestDurableAck, cursorEngine,
                closeFlushTimeoutMillis, reconnectMaxDurationMillis,
                reconnectInitialBackoffMillis, reconnectMaxBackoffMillis,
                initialConnectMode, errorHandler, errorInboxCapacity,
                durableAckKeepaliveIntervalMillis, DEFAULT_AUTH_TIMEOUT_MS);
    }

    /**
     * Multi-endpoint variant. {@code endpoints} must be non-empty; the order is the failover
     * preference (single-primary cluster: walk the list until one accepts the upgrade).
     * <p>
     * Delegates to the wider overload that also accepts the connection-listener
     * configuration; passes {@code null} listener and the dispatcher default
     * inbox capacity, matching the contract of the older callers that were
     * written before connection events shipped.
     */
    public static QwpWebSocketSender connect(
            List<Endpoint> endpoints,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            String authorizationHeader,
            boolean requestDurableAck,
            CursorSendEngine cursorEngine,
            long closeFlushTimeoutMillis,
            long reconnectMaxDurationMillis,
            long reconnectInitialBackoffMillis,
            long reconnectMaxBackoffMillis,
            Sender.InitialConnectMode initialConnectMode,
            SenderErrorHandler errorHandler,
            int errorInboxCapacity,
            long durableAckKeepaliveIntervalMillis,
            long authTimeoutMs
    ) {
        return connect(endpoints, tlsConfig, autoFlushRows, autoFlushBytes,
                autoFlushIntervalNanos, authorizationHeader,
                requestDurableAck, cursorEngine,
                closeFlushTimeoutMillis, reconnectMaxDurationMillis,
                reconnectInitialBackoffMillis, reconnectMaxBackoffMillis,
                initialConnectMode, errorHandler, errorInboxCapacity,
                durableAckKeepaliveIntervalMillis, authTimeoutMs,
                0, null, SenderConnectionDispatcher.DEFAULT_CAPACITY);
    }

    /**
     * Multi-endpoint variant that also accepts the async connection-event
     * listener and its dispatcher inbox capacity. Uses the default
     * poison-frame detector threshold.
     */
    public static QwpWebSocketSender connect(
            List<Endpoint> endpoints,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            String authorizationHeader,
            boolean requestDurableAck,
            CursorSendEngine cursorEngine,
            long closeFlushTimeoutMillis,
            long reconnectMaxDurationMillis,
            long reconnectInitialBackoffMillis,
            long reconnectMaxBackoffMillis,
            Sender.InitialConnectMode initialConnectMode,
            SenderErrorHandler errorHandler,
            int errorInboxCapacity,
            long durableAckKeepaliveIntervalMillis,
            long authTimeoutMs,
            int connectTimeoutMs,
            SenderConnectionListener connectionListener,
            int connectionListenerInboxCapacity
    ) {
        return connect(endpoints, tlsConfig, autoFlushRows, autoFlushBytes,
                autoFlushIntervalNanos, authorizationHeader, requestDurableAck,
                cursorEngine, closeFlushTimeoutMillis, reconnectMaxDurationMillis,
                reconnectInitialBackoffMillis, reconnectMaxBackoffMillis,
                initialConnectMode, errorHandler, errorInboxCapacity,
                durableAckKeepaliveIntervalMillis, authTimeoutMs, connectTimeoutMs,
                connectionListener, connectionListenerInboxCapacity,
                CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                CursorWebSocketSendLoop.DEFAULT_POISON_MIN_ESCALATION_WINDOW_MILLIS);
    }

    /**
     * Master connect overload — also accepts the poison-frame detector
     * threshold ({@code max_frame_rejections}): consecutive server-active
     * rejections of the same head-of-line frame, with no ack progress in
     * between, before the loop escalates to a typed terminal.
     */
    public static QwpWebSocketSender connect(
            List<Endpoint> endpoints,
            ClientTlsConfiguration tlsConfig,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos,
            String authorizationHeader,
            boolean requestDurableAck,
            CursorSendEngine cursorEngine,
            long closeFlushTimeoutMillis,
            long reconnectMaxDurationMillis,
            long reconnectInitialBackoffMillis,
            long reconnectMaxBackoffMillis,
            Sender.InitialConnectMode initialConnectMode,
            SenderErrorHandler errorHandler,
            int errorInboxCapacity,
            long durableAckKeepaliveIntervalMillis,
            long authTimeoutMs,
            int connectTimeoutMs,
            SenderConnectionListener connectionListener,
            int connectionListenerInboxCapacity,
            int maxFrameRejections,
            long poisonMinEscalationWindowMillis
    ) {
        QwpWebSocketSender sender = new QwpWebSocketSender(
                endpoints, tlsConfig,
                autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                authorizationHeader
        );
        try {
            sender.requestDurableAck = requestDurableAck;
            sender.authTimeoutMs = authTimeoutMs;
            sender.connectTimeoutMs = connectTimeoutMs;
            sender.closeFlushTimeoutMillis = closeFlushTimeoutMillis;
            sender.reconnectMaxDurationMillis = reconnectMaxDurationMillis;
            sender.reconnectInitialBackoffMillis = reconnectInitialBackoffMillis;
            sender.reconnectMaxBackoffMillis = reconnectMaxBackoffMillis;
            sender.durableAckKeepaliveIntervalMillis = durableAckKeepaliveIntervalMillis;
            sender.maxFrameRejections = maxFrameRejections;
            sender.poisonMinEscalationWindowMillis = poisonMinEscalationWindowMillis;
            sender.initialConnectMode = initialConnectMode == null
                    ? Sender.InitialConnectMode.OFF
                    : initialConnectMode;
            if (errorHandler != null) {
                sender.setErrorHandler(errorHandler);
            }
            sender.setErrorInboxCapacity(errorInboxCapacity);
            if (connectionListener != null) {
                sender.setConnectionListener(connectionListener);
            }
            sender.setConnectionListenerInboxCapacity(connectionListenerInboxCapacity);
            if (cursorEngine != null) {
                sender.setCursorEngine(cursorEngine, true);
            }
            sender.ensureConnected();
        } catch (Throwable t) {
            sender.close();
            throw t;
        }
        return sender;
    }

    /**
     * Creates a sender without connecting. For testing only.
     * <p>
     * This allows unit tests to test sender logic without requiring a real server.
     * Uses default auto-flush settings.
     *
     * @param host server host (not connected)
     * @param port server port (not connected)
     * @return unconnected sender
     */
    public static QwpWebSocketSender createForTesting(String host, int port) {
        return createForTesting(host, port, null);
    }

    public static QwpWebSocketSender createForTesting(String host, int port, String authorizationHeader) {
        return new QwpWebSocketSender(
                singleEndpoint(host, port), null,
                DEFAULT_AUTO_FLUSH_ROWS, DEFAULT_AUTO_FLUSH_BYTES, DEFAULT_AUTO_FLUSH_INTERVAL_NANOS,
                authorizationHeader
        );
    }

    /**
     * Creates a sender with custom flow control settings without connecting. For testing only.
     *
     * @param host                   server host (not connected)
     * @param port                   server port (not connected)
     * @param autoFlushRows          rows per batch (0 = no limit)
     * @param autoFlushBytes         bytes per batch (0 = no limit)
     * @param autoFlushIntervalNanos age before flush in nanos (0 = no limit)
     * @return unconnected sender
     */
    public static QwpWebSocketSender createForTesting(
            String host,
            int port,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos
    ) {
        return new QwpWebSocketSender(
                singleEndpoint(host, port), null,
                autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                null
        );
    }

    @Override
    public void at(long timestamp, ChronoUnit unit) {
        checkNotClosed();
        checkTableSelected();
        try {
            if (unit == ChronoUnit.NANOS) {
                atNanos(timestamp);
            } else {
                long micros = toMicros(timestamp, unit);
                atMicros(micros);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
    }

    @Override
    public void at(Instant timestamp) {
        checkNotClosed();
        checkTableSelected();
        try {
            long micros = timestamp.getEpochSecond() * 1_000_000L + timestamp.getNano() / 1000L;
            atMicros(micros);
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
    }

    @Override
    public void atNow() {
        checkNotClosed();
        checkTableSelected();
        try {
            // Server-assigned timestamp - just send the row without designated timestamp
            sendRow();
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
    }

    /**
     * Blocks until {@code ackedFsn() >= targetFsn}, or until {@code timeoutMillis}
     * elapses. Polls the cursor engine on a 50us park; surfaces I/O loop errors
     * synchronously via {@code cursorSendLoop.checkError()}.
     * <p>
     * Useful for tests and user code that need to confirm a specific publish
     * has been server-acknowledged. Pair with {@link #flushAndGetSequence()} to
     * obtain {@code targetFsn}.
     *
     * @param targetFsn     FSN to wait for; typically {@link #flushAndGetSequence()}'s return value
     * @param timeoutMillis upper bound on the wait; {@code <= 0} returns immediately
     * @return {@code true} if {@code ackedFsn() >= targetFsn} on return, {@code false} on timeout
     * @throws LineSenderException if the I/O loop has latched a terminal error
     */
    @Override
    public boolean awaitAckedFsn(long targetFsn, long timeoutMillis) {
        checkNotClosed();
        if (cursorEngine == null) {
            return targetFsn < 0L;
        }
        // Surface latched I/O errors before any early-return path, so a
        // caller polling with timeoutMillis <= 0 to drive their own loop
        // sees the terminal throw instead of an indefinite "not yet".
        if (cursorSendLoop != null) {
            cursorSendLoop.checkError();
        }
        checkConnectionError();
        if (cursorEngine.ackedFsn() >= targetFsn) {
            return true;
        }
        if (timeoutMillis <= 0L) {
            return false;
        }
        long deadlineNanos = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (cursorEngine.ackedFsn() < targetFsn) {
            if (cursorSendLoop != null) {
                cursorSendLoop.checkError();
            }
            checkConnectionError();
            if (System.nanoTime() >= deadlineNanos) {
                return false;
            }
            java.util.concurrent.locks.LockSupport.parkNanos(50_000L);
        }
        return true;
    }

    /**
     * Adds a BINARY column value to the current row. The bytes are written
     * verbatim with no encoding or transformation. A {@code null} array
     * reference is rejected so the NULL contract stays explicit (use the null
     * bitmap instead). An empty array is accepted on the wire but QuestDB's
     * BINARY storage uses the same NULL sentinel for zero-length and absent
     * values, so an empty payload round-trips as NULL on read.
     */
    @Override
    public QwpWebSocketSender binaryColumn(CharSequence columnName, byte[] value) {
        checkNotClosed();
        checkTableSelected();
        if (value == null) {
            throw new LineSenderException(
                    "BINARY value cannot be null; mark the row null via the null bitmap instead");
        }
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_BINARY, true);
            if (col != null) {
                col.addBinary(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    /**
     * Overrides the {@link Sender} interface default so the closed-sender
     * check fires before the null-slice check. Without this override, the
     * default throws "BINARY slice cannot be null" on a closed sender,
     * obscuring the canonical "Sender is closed" error.
     */
    @Override
    public QwpWebSocketSender binaryColumn(CharSequence columnName, DirectByteSlice slice) {
        checkNotClosed();
        checkTableSelected();
        if (slice == null) {
            throw new LineSenderException(
                    "BINARY slice cannot be null; mark the row null via the null bitmap instead");
        }
        return binaryColumn(columnName, slice.ptr(), slice.size());
    }

    /**
     * Zero-allocation BINARY overload: copies {@code len} bytes from native
     * memory at {@code ptr} into the column. See
     * {@link Sender#binaryColumn(CharSequence, long, long)} for the contract.
     */
    @Override
    public QwpWebSocketSender binaryColumn(CharSequence columnName, long ptr, long len) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_BINARY, true);
            if (col != null) {
                col.addBinary(ptr, len);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public QwpWebSocketSender boolColumn(CharSequence columnName, boolean value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_BOOLEAN, false);
            if (col != null) {
                col.addBoolean(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public DirectByteSlice bufferView() {
        throw new LineSenderException("bufferView() is not supported for WebSocket sender");
    }

    /**
     * Adds a BYTE column value to the current row.
     *
     * @param columnName the column name
     * @param value      the byte value
     * @return this sender for method chaining
     */
    public QwpWebSocketSender byteColumn(CharSequence columnName, byte value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_BYTE, false);
            if (col != null) {
                col.addByte(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public void cancelRow() {
        checkNotClosed();
        if (currentTableBuffer != null) {
            currentTableBuffer.cancelCurrentRow();
            currentTableBuffer.rollbackUncommittedColumns();
        }
    }

    /**
     * Adds a CHAR column value to the current row.
     * <p>
     * CHAR is stored as a 2-byte UTF-16 code unit in QuestDB.
     *
     * @param columnName the column name
     * @param value      the character value
     * @return this sender for method chaining
     */
    public QwpWebSocketSender charColumn(CharSequence columnName, char value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_CHAR, false);
            if (col != null) {
                col.addShort((short) value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    /**
     * Closes the sender: flushes user-thread state into the engine, drains
     * acked data within {@code close_flush_timeout}, stops the I/O loop,
     * closes the orphan-drainer pool, and frees buffers.
     * <p>
     * Worst-case latency budget (dominant contributors, sequential):
     * <ul>
     *   <li>bounded drain: up to {@code close_flush_timeout} when the server
     *       is slow or unreachable ({@code <= 0} opts out);</li>
     *   <li>I/O loop stop: the shutdown-latch await is untimed, but the loop
     *       exits promptly unless the I/O thread sits inside a blocking
     *       native connect — bounded by {@code connect_timeout}, or by the
     *       OS SYN-retry deadline (60-130s on Linux) when the default
     *       {@code 0} is in effect. Background drainer walks never delay
     *       this stop: they run lock-free on private round cursors and
     *       never hold anything the foreground waits on (see
     *       {@link #buildAndConnect});</li>
     *   <li>drainer pool: drainers still in their connect-retry phase are
     *       stop-signaled immediately (exit within ~50ms); drainers actively
     *       replaying frames get a 2.5s grace window plus a 0.5s stop window
     *       — worst case ~3s when a drainer sits in a blocking native
     *       connect (15s background deadline) and must be abandoned to exit
     *       on its own.</li>
     * </ul>
     */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            boolean ioThreadStopped = true;
            // Captures the first error from the flush/drain path AND any
            // secondary errors from cleanup steps (added via addSuppressed).
            // Silently swallowing any of these would hide latched terminal
            // SenderError HALTs (server-side rejections like MESSAGE_TOO_BIG,
            // SCHEMA_MISMATCH HALT) from users who only call close() and
            // never call flush() afterwards.
            Throwable terminalError = null;
            // Snapshot the exact terminal error instance that a user-thread
            // API call ALREADY caught (via flush()/at()) before close() ran.
            // If flushPendingRows/drainOnClose below also rethrow the same
            // instance, dropping it at the final rethrow avoids
            // try-with-resources self-suppression: Throwable.addSuppressed
            // raises IllegalArgumentException when primary == suppressed.
            // Must stay this single read: the snapshot needs the identity of
            // the error the user already owns, and only
            // getSynchronouslySurfacedError() holds it. Deriving it from two
            // separate latch reads races the I/O thread -- a terminal latched
            // between the reads would be adopted as user-owned and silently
            // dropped (see CloseOwnershipRaceTest).
            Throwable alreadyOwnedByUser = cursorSendLoop != null
                    ? cursorSendLoop.getSynchronouslySurfacedError() : null;

            try {
                // Only drain when both the engine and the I/O loop are wired
                // up — close() is also called from createForTesting() teardown
                // and from connect() rollback paths where one or both may be null.
                if (connectionError.get() == null && cursorEngine != null && cursorSendLoop != null) {
                    // 1) Flush user-thread state into the engine (encoded
                    //    rows -> mmap'd / malloc'd ring). After this, the
                    //    cursor engine's publishedFsn reflects the final
                    //    target the I/O loop must drive ackedFsn up to.
                    flushPendingRows(deferCommit);
                    if (!deferCommit && hasDeferredMessages) {
                        sendCommitMessage();
                    }
                    if (activeBuffer != null && activeBuffer.hasData()) {
                        sealAndSwapBuffer();
                        if (!deferCommit) {
                            lastCommitBoundaryFsn = cursorEngine.publishedFsn();
                        }
                    }
                    // 2) Safety-net rethrow: surface the latched terminal
                    //    error only when no other channel has already
                    //    delivered THIS terminal to the user. "Already
                    //    delivered" means either the producer thread saw it
                    //    synchronously via flush()/append() (checkUnsurfacedError
                    //    is silent in that case) or the async dispatcher
                    //    actually delivered the latched terminal to a
                    //    user-installed custom handler
                    //    (hasDeliveredTerminalToCustomHandler, checked here).
                    //    The test is terminal-specific on purpose: an earlier
                    //    routine RETRIABLE rejection delivered to the
                    //    handler must NOT suppress a later genuine TERMINAL
                    //    error (the "any error ever" flag did, silently
                    //    losing it). It also stays false when the terminal
                    //    reached only the default handler after a
                    //    setErrorHandler(null) revert, or is still
                    //    queued/abandoned behind a slow handler -- so a
                    //    config-string-only caller, and a reverting caller,
                    //    both still get the loud rethrow on shutdown.
                    boolean terminalOwnedByCustomHandler = errorDispatcher != null
                            && errorDispatcher.hasDeliveredTerminalToCustomHandler();
                    if (!terminalOwnedByCustomHandler) {
                        cursorSendLoop.checkUnsurfacedError();
                    }
                    // 3) Bounded drain: block until the server has ACK'd
                    //    everything we just published, or until the
                    //    configured timeout elapses. closeFlushTimeoutMillis
                    //    <= 0 opts out (fast close, may lose memory-mode
                    //    data on JVM exit). Pass the same ownership flag the
                    //    step-2 safety net used: when the custom handler
                    //    already owns THIS terminal, the drain must stop on it
                    //    without re-throwing (re-throwing would double-signal
                    //    an error the user already handled). Otherwise the
                    //    drain keeps the loud safety net and surfaces it.
                    if (closeFlushTimeoutMillis > 0L) {
                        drainOnClose(terminalOwnedByCustomHandler);
                    }
                }
            } catch (Throwable t) {
                terminalError = t;
            }

            // Shut down the I/O thread before closing the socket or buffers
            // it may be using. Must run even if the flush above failed.
            if (cursorSendLoop != null) {
                try {
                    cursorSendLoop.close();
                } catch (Throwable e) {
                    ioThreadStopped = false;
                    LOG.error("Error closing cursor send loop: {}", String.valueOf(e));
                    terminalError = captureCloseError(terminalError, e);
                }
            }
            // Drainer pool closes after the foreground I/O loop is wound
            // down. Drainers share buildAndConnect's endpoint walk and
            // hostTracker state with the foreground (never its observable
            // connection state or event stream), but their
            // connect gate is their own stop flag — NOT the foreground
            // loop's liveness — so the pool's graceful-drain window below
            // still lets in-flight drainers finish (including reconnects)
            // even though cursorSendLoop is already stopped.
            if (drainerPool != null) {
                try {
                    drainerPool.close();
                } catch (Throwable e) {
                    LOG.error("Error closing drainer pool: {}", String.valueOf(e));
                    terminalError = captureCloseError(terminalError, e);
                }
            }

            // Always free resources the I/O thread never touches:
            // encoder and table buffers are user-thread-only.
            try {
                encoder.close();
                ObjList<CharSequence> keys = tableBuffers.keys();
                for (int i = 0, n = keys.size(); i < n; i++) {
                    CharSequence key = keys.getQuick(i);
                    if (key != null) {
                        Misc.free(tableBuffers.get(key));
                    }
                }
                tableBuffers.clear();
            } catch (Throwable t) {
                LOG.error("Error closing encoder or table buffers: {}", String.valueOf(t));
                terminalError = captureCloseError(terminalError, t);
            }

            if (!ioThreadStopped) {
                // The I/O thread may still be using the socket and microbatch
                // buffers. Freeing them would risk SIGSEGV.
                LOG.error("I/O thread is still running, leaking WebSocket client and microbatch buffers");
                // The engine, however, need not leak: delegate its close to
                // the I/O thread's exit path, which runs it strictly after
                // the thread's last engine access — the mapping and slot
                // lock release as soon as the stuck wire call resolves
                // (bounded by OS timeouts). slotLockReleased intentionally
                // stays false: the lock is released only when the delegated
                // close actually runs, so the pool must not reuse the slot
                // meanwhile. A false return means the thread exited between
                // the failed close() and now — then closing here is safe.
                if (ownsCursorEngine && cursorEngine != null && cursorSendLoop != null
                        && !cursorSendLoop.delegateEngineClose()) {
                    try {
                        cursorEngine.close();
                    } catch (Throwable t) {
                        LOG.error("Error closing owned CursorSendEngine: {}", String.valueOf(t));
                        terminalError = captureCloseError(terminalError, t);
                    }
                    cursorEngine = null;
                    ownsCursorEngine = false;
                    slotLockReleased = true;
                }
                rethrowTerminal(terminalError);
                return;
            }

            if (buffer0 != null) {
                try {
                    buffer0.close();
                } catch (Throwable t) {
                    LOG.error("Error closing buffer0: {}", String.valueOf(t));
                    terminalError = captureCloseError(terminalError, t);
                }
            }
            if (buffer1 != null) {
                try {
                    buffer1.close();
                } catch (Throwable t) {
                    LOG.error("Error closing buffer1: {}", String.valueOf(t));
                    terminalError = captureCloseError(terminalError, t);
                }
            }

            if (client != null) {
                try {
                    client.close();
                } catch (Throwable t) {
                    LOG.error("Error closing WebSocket client: {}", String.valueOf(t));
                    terminalError = captureCloseError(terminalError, t);
                }
                client = null;
            }

            if (ownsCursorEngine && cursorEngine != null) {
                try {
                    cursorEngine.close();
                } catch (Throwable t) {
                    LOG.error("Error closing owned CursorSendEngine: {}", String.valueOf(t));
                    terminalError = captureCloseError(terminalError, t);
                }
                cursorEngine = null;
                ownsCursorEngine = false;
            }
            // Past the ioThreadStopped guard => cursorEngine.close() ran and
            // released the SF flock in its finally (or this sender owned no
            // engine holding one). Signal the pool it may reuse the slot.
            slotLockReleased = true;

            // Shutdown order: dispatcher last, after the I/O loop has stopped
            // producing into it. close() drains pending entries with a short
            // deadline so any final errors land in the user's handler.
            if (errorDispatcher != null) {
                try {
                    errorDispatcher.close();
                } catch (Throwable t) {
                    LOG.error("Error closing error dispatcher: {}", String.valueOf(t));
                    terminalError = captureCloseError(terminalError, t);
                }
            }
            if (progressDispatcher != null) {
                try {
                    progressDispatcher.close();
                } catch (Throwable t) {
                    LOG.error("Error closing progress dispatcher: {}", String.valueOf(t));
                    terminalError = captureCloseError(terminalError, t);
                }
            }
            if (connectionDispatcher != null) {
                try {
                    connectionDispatcher.close();
                } catch (Throwable t) {
                    LOG.error("Error closing connection dispatcher: {}", String.valueOf(t));
                    terminalError = captureCloseError(terminalError, t);
                }
            }

            LOG.info("QwpWebSocketSender closed");

            // If close() ended up holding the same instance the user already
            // caught earlier, suppress the rethrow. The user's catch block
            // wraps close() (try-with-resources), and Throwable refuses
            // self-suppression.
            if (terminalError != null && terminalError == alreadyOwnedByUser) {
                terminalError = null;
            }
            rethrowTerminal(terminalError);
        }
    }

    /**
     * True once {@link #close()} has released the store-and-forward slot
     * flock. False means close() leaked the still-running I/O thread (and its
     * resources), so the flock is still held; the owning pool must keep the
     * slot index reserved instead of reusing the still-locked slot dir.
     */
    public boolean isSlotLockReleased() {
        return slotLockReleased;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal64 value) {
        checkNotClosed();
        if (value == null || value.isNull()) return this;
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DECIMAL64, true);
            if (col != null) {
                col.addDecimal64(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal128 value) {
        checkNotClosed();
        if (value == null || value.isNull()) return this;
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DECIMAL128, true);
            if (col != null) {
                col.addDecimal128(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, Decimal256 value) {
        checkNotClosed();
        if (value == null || value.isNull()) return this;
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DECIMAL256, true);
            if (col != null) {
                col.addDecimal256(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender decimalColumn(CharSequence name, CharSequence value) {
        checkNotClosed();
        if (value == null || value.length() == 0) return this;
        checkTableSelected();
        try {
            currentDecimal256.ofString(value);
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DECIMAL256, true);
            if (col != null) {
                col.addDecimal256(currentDecimal256);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[] values) {
        checkNotClosed();
        if (values == null) return this;
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DOUBLE_ARRAY, true);
            if (col != null) {
                col.addDoubleArray(values);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][] values) {
        checkNotClosed();
        if (values == null) return this;
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DOUBLE_ARRAY, true);
            if (col != null) {
                col.addDoubleArray(values);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender doubleArray(@NotNull CharSequence name, double[][][] values) {
        checkNotClosed();
        if (values == null) return this;
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DOUBLE_ARRAY, true);
            if (col != null) {
                col.addDoubleArray(values);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender doubleArray(CharSequence name, DoubleArray array) {
        checkNotClosed();
        if (array == null) return this;
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_DOUBLE_ARRAY, true);
            if (col != null) {
                col.addDoubleArray(array);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public QwpWebSocketSender doubleColumn(CharSequence columnName, double value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_DOUBLE, true);
            if (col != null) {
                col.addDouble(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    /**
     * Adds a FLOAT column value to the current row.
     *
     * @param columnName the column name
     * @param value      the float value
     * @return this sender for method chaining
     */
    public QwpWebSocketSender floatColumn(CharSequence columnName, float value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_FLOAT, true);
            if (col != null) {
                col.addFloat(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    /**
     * Encodes pending rows into the cursor engine and returns once the data
     * is published into the engine — in-RAM for memory mode, on-disk for
     * store-and-forward mode. {@code flush()} does <b>not</b> wait for the
     * server to acknowledge the batches; ACKs arrive asynchronously and the
     * background I/O loop trims acked frames out of the engine independently.
     * <p>
     * If the engine's cursor ring is at the {@code sf_max_total_bytes} cap,
     * {@code flush()} blocks while the I/O loop drains acked frames and
     * frees space, up to {@code sf_append_deadline_millis} (default 30 s);
     * on deadline expiry, this method throws.
     * <p>
     * For close-time drain semantics — waiting for the server to ACK
     * everything published before shutting the I/O loop down — use
     * {@link io.questdb.client.Sender.LineSenderBuilder#closeFlushTimeoutMillis(long)}.
     * <p>
     * If a WebSocket send, receive, ACK timeout, server error ACK, invalid
     * ACK, or server close is observed after the connection has been
     * established, the sender enters a terminal failed state. The first
     * failure is retained and subsequent public operations rethrow the same
     * {@link LineSenderException}. Create a new sender to resume sending.
     *
     * @throws LineSenderException if the sender is closed, a row is still
     *                             in progress, connection setup fails, the
     *                             engine cap deadline expires, or a terminal
     *                             WebSocket failure is observed
     */
    @Override
    public void flush() {
        flushAndGetSequence();
    }

    /**
     * Same as {@link #flush()} but returns the highest FSN published into the
     * cursor engine by this call. Producer-side correlation handle: the user
     * logs {@code (returnedFsn, domainContext)} alongside the data, then joins
     * to the {@link SenderError#getFromFsn()} / {@link SenderError#getToFsn()}
     * span when an async error is delivered.
     *
     * <p>Returns {@code -1} when nothing was published (no active buffer with
     * data). The legacy {@link #flush()} discards this value.
     *
     * @return highest FSN published into the engine, or {@code -1} if no data
     */
    @Override
    public long flushAndGetSequence() {
        checkNotClosed();
        ensureNoInProgressRow();
        ensureConnected();

        long beforeFsn = cursorEngine != null ? cursorEngine.publishedFsn() : -1L;

        // Cursor SF: SF.append happens on the user thread inside
        // sealAndSwapBuffer, so by the time we reach here every encoded
        // batch is durable on its mmap'd segment. No processingCount to
        // drain, no awaitPendingAcks. Just surface any I/O thread error.
        flushPendingRows(deferCommit);
        if (!deferCommit && hasDeferredMessages) {
            sendCommitMessage();
        }
        if (activeBuffer != null && activeBuffer.hasData()) {
            sealAndSwapBuffer();
            if (!deferCommit) {
                // Same residual-seal boundary update as close(): a
                // non-deferred residual publish is commit-bearing and must be
                // covered by close-time drains, or drainOnClose would return
                // before its ack and memory-mode data could be lost on exit.
                lastCommitBoundaryFsn = cursorEngine.publishedFsn();
            }
        }
        cursorSendLoop.checkError();
        checkConnectionError();

        long afterFsn = cursorEngine != null ? cursorEngine.publishedFsn() : -1L;
        return afterFsn > beforeFsn ? afterFsn : -1L;
    }

    /**
     * Flushes pending rows and blocks until the server has acknowledged
     * every frame published so far (the current published-FSN watermark),
     * or until {@code timeoutMillis} elapses.
     * <p>
     * This override uses <b>watermark semantics</b> rather than per-call
     * semantics: it waits for the global {@code publishedFsn()}, not just
     * the FSN returned by the flush in this call. This is necessary because
     * {@link #flushAndGetSequence()} now returns {@code -1} when no data
     * was published by the call, and the default {@link Sender#drain}
     * implementation ({@code awaitAckedFsn(flushAndGetSequence(), timeout)})
     * would short-circuit immediately on an empty flush even when prior
     * publishes remain unacknowledged.
     * <p>
     * Close-time drain ({@code #drainOnClose()}) already uses the same
     * watermark approach directly.
     *
     * @param timeoutMillis upper bound on the wait; {@code <= 0} returns
     *                      the current state without blocking (the flush
     *                      still happens before the check)
     * @return {@code true} if the server has acknowledged every published
     *         frame on return, {@code false} on timeout
     * @throws LineSenderException if the transport has latched a terminal error
     */
    @Override
    public boolean drain(long timeoutMillis) {
        flush();
        long targetFsn = cursorEngine != null ? cursorEngine.publishedFsn() : -1L;
        return awaitAckedFsn(targetFsn, timeoutMillis);
    }

    /**
     * Adds a GEOHASH column value to the current row from pre-packed bits and
     * an explicit bit precision. Bits above {@code precisionBits} are masked
     * off and never reach the wire, so callers may pass an unmasked long.
     * <p>
     * Precision is locked the first time a value is added to the column: every
     * subsequent row must use the same precision. Precision must be in
     * {@code [1, 60]}.
     *
     * @param columnName    the column name
     * @param bits          packed geohash; low {@code precisionBits} bits significant
     * @param precisionBits number of significant bits, 1..60
     * @return this sender for method chaining
     */
    @Override
    public QwpWebSocketSender geoHashColumn(CharSequence columnName, long bits, int precisionBits) {
        checkNotClosed();
        checkTableSelected();
        if (precisionBits < 1 || precisionBits > 60) {
            throw new LineSenderException(
                    "invalid GEOHASH precision: " + precisionBits + " (must be 1-60)");
        }
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_GEOHASH, true);
            if (col != null) {
                col.addGeoHash(maskGeoHashBits(bits, precisionBits), precisionBits);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    /**
     * Adds a GEOHASH column value from a base32 geohash string (e.g. "u33d8").
     * The string is decoded as 5 bits per character; precision is set to
     * {@code value.length() * 5} and locked at the column on first use. The
     * accepted alphabet is digits {@code 0-9} plus {@code b c d e f g h j k m n
     * p q r s t u v w x y z}, case insensitive ({@code a, i, l, o} are
     * reserved). Maximum 12 characters (60 bits).
     *
     * @param columnName the column name
     * @param value      base32 geohash string, 1..12 characters; must not be null
     * @return this sender for method chaining
     * @throws LineSenderException if the string is null, empty, too long, or
     *                             contains a non-base32 character
     */
    @Override
    public QwpWebSocketSender geoHashColumn(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        if (value == null) {
            throw new LineSenderException(
                    "GEOHASH string cannot be null; mark the row null via the null bitmap instead");
        }
        int len = value.length();
        if (len == 0) {
            throw new LineSenderException("GEOHASH string cannot be empty");
        }
        if (len > 12) {
            throw new LineSenderException(
                    "GEOHASH string exceeds 12 characters: " + len);
        }
        long bits;
        try {
            bits = Numbers.parseGeoHashBase32(value, 0, len);
        } catch (NumericException e) {
            throw new LineSenderException("invalid GEOHASH string: ").put(value);
        }
        return geoHashColumn(columnName, bits, len * 5);
    }

    /**
     * Highest FSN that has been server-acknowledged. Rejections never advance
     * the watermark. {@code -1} if
     * the I/O loop has not yet started or no batch has been published.
     * <p>
     * Snapshot accessor — for a bounded wait, use
     * {@link #awaitAckedFsn(long, long)}.
     */
    @Override
    public long getAckedFsn() {
        return cursorEngine != null ? cursorEngine.ackedFsn() : -1L;
    }

    /**
     * Number of background orphan-slot drainers this sender is currently
     * running. Returns 0 when {@code drain_orphans} is disabled or no
     * orphan slots were discovered at startup. Carries the same lax
     * cleanup race as the underlying pool — a drainer that finished
     * moments ago may still count for a few ms.
     */
    public int getActiveBackgroundDrainers() {
        BackgroundDrainerPool pool = drainerPool;
        return pool == null ? 0 : pool.getActiveCount();
    }

    /**
     * Returns the auto-flush byte threshold.
     */
    public int getAutoFlushBytes() {
        return autoFlushBytes;
    }

    /**
     * Returns the auto-flush interval in nanoseconds.
     */
    public long getAutoFlushIntervalNanos() {
        return autoFlushIntervalNanos;
    }

    /**
     * Returns the auto-flush row threshold.
     */
    public int getAutoFlushRows() {
        return autoFlushRows;
    }

    /**
     * Number of {@link SenderConnectionEvent} notifications dropped because
     * the bounded inbox was full. Non-zero means the user-supplied
     * {@link SenderConnectionListener} cannot keep up. Returns 0 if the
     * dispatcher has not been allocated yet.
     */
    public long getDroppedConnectionNotifications() {
        SenderConnectionDispatcher d = connectionDispatcher;
        return d == null ? 0L : d.getDroppedNotifications();
    }

    /**
     * Number of {@link SenderError} notifications dropped because the
     * bounded inbox was full. Non-zero means the user-supplied
     * {@link SenderErrorHandler} cannot keep up. Returns 0 if the error
     * dispatcher has not been allocated yet.
     */
    public long getDroppedErrorNotifications() {
        SenderErrorDispatcher d = errorDispatcher;
        return d == null ? 0L : d.getDroppedNotifications();
    }

    /** Returns the live byte budget the auto-flush path actually enforces. */
    @TestOnly
    public int getEffectiveAutoFlushBytes() {
        return effectiveAutoFlushBytes;
    }

    /**
     * Snapshot of the typed payload for the latched terminal server-rejection error,
     * or {@code null} if the I/O loop has not latched a server-rejection terminal
     * (initial state, or only a wire-level failure has been latched). Read-only —
     * intended for ops dashboards and post-mortem inspection.
     */
    public SenderError getLastTerminalError() {
        CursorWebSocketSendLoop l = cursorSendLoop;
        return l == null ? null : l.getLastTerminalServerError();
    }

    /**
     * Registers a symbol value in the global dictionary and returns its global ID.
     * Called from {@link QwpTableBuffer.ColumnBuffer#addSymbol(CharSequence)}.
     *
     * @param symbol the symbol value to register
     * @return the global symbol ID
     */
    public int getOrAddGlobalSymbol(CharSequence symbol) {
        int globalId = globalSymbolDictionary.getOrAddSymbol(symbol);
        if (globalId > currentBatchMaxSymbolId) {
            currentBatchMaxSymbolId = globalId;
        }
        return globalId;
    }

    /**
     * Running tally the row builder maintains so auto-flush thresholds can be
     * evaluated without re-walking every table per row. Exposed for tests
     * that compare this incremental counter against a ground-truth walk.
     */
    @TestOnly
    public long getPendingBytes() {
        return pendingBytes;
    }

    /**
     * Server-advertised cap on the per-batch raw byte size. Zero before the
     * first connect; updated by every successful reconnect via
     * {@link #applyServerBatchSizeLimit(int)}.
     */
    @TestOnly
    public int getServerMaxBatchSize() {
        return serverMaxBatchSize;
    }

    @TestOnly
    public QwpTableBuffer getTableBuffer(String tableName) {
        QwpTableBuffer buffer = tableBuffers.get(tableName);
        if (buffer == null) {
            buffer = new QwpTableBuffer(tableName, this);
            tableBuffers.put(tableName, buffer);
        }
        currentTableBuffer = buffer;
        currentTableBufferSnapshotBytes = buffer.getBufferedBytes();
        currentTableName = tableName;
        return buffer;
    }

    /**
     * Total binary frames whose ACKs have been received and applied.
     */
    public long getTotalAcks() {
        CursorWebSocketSendLoop l = cursorSendLoop;
        return l == null ? 0L : l.getTotalAcks();
    }

    /**
     * Cumulative count of background orphan-slot drainers that exited
     * by dropping a {@code .failed} sentinel since this sender started.
     * Returns 0 when {@code drain_orphans} is disabled or no orphan
     * slots were discovered at startup.
     */
    public long getTotalBackgroundDrainersFailed() {
        BackgroundDrainerPool pool = drainerPool;
        return pool == null ? 0L : pool.getTotalFailed();
    }

    /**
     * Cumulative count of background orphan-slot drainers that drained
     * their slot fully and exited cleanly since this sender started.
     * Returns 0 when {@code drain_orphans} is disabled or no orphan
     * slots were discovered at startup.
     */
    public long getTotalBackgroundDrainersSucceeded() {
        BackgroundDrainerPool pool = drainerPool;
        return pool == null ? 0L : pool.getTotalSucceeded();
    }

    /**
     * Cumulative number of times {@code appendBlocking} hit a full engine
     * ring and parked waiting for the segment manager or the wire to free
     * space. One increment per blocking call, not per spin. Returns 0
     * when the cursor engine has not been allocated yet.
     */
    public long getTotalBackpressureStalls() {
        CursorSendEngine e = cursorEngine;
        return e == null ? 0L : e.getTotalBackpressureStalls();
    }

    /**
     * Number of {@link SenderConnectionEvent} notifications delivered to the
     * user listener since this sender started. Counts every delivery attempt,
     * including those where the listener threw.
     */
    public long getTotalConnectionEventsDelivered() {
        SenderConnectionDispatcher d = connectionDispatcher;
        return d == null ? 0L : d.getTotalDelivered();
    }

    /**
     * Number of {@link SenderError} notifications delivered to the user
     * handler since this sender started. Counts every delivery attempt,
     * including those where the handler threw. Returns 0 if the error
     * dispatcher has not been allocated yet.
     */
    public long getTotalErrorNotificationsDelivered() {
        SenderErrorDispatcher d = errorDispatcher;
        return d == null ? 0L : d.getTotalDelivered();
    }

    /**
     * Cumulative count of frames re-sent during post-reconnect catch-up
     * windows. Zero in steady state; a sustained nonzero rate signals
     * flapping where every reconnect replays meaningful work.
     */
    public long getTotalFramesReplayed() {
        CursorWebSocketSendLoop l = cursorSendLoop;
        return l == null ? 0L : l.getTotalFramesReplayed();
    }

    /**
     * Total binary frames the cursor I/O loop has issued to the wire.
     */
    public long getTotalFramesSent() {
        CursorWebSocketSendLoop l = cursorSendLoop;
        return l == null ? 0L : l.getTotalFramesSent();
    }

    /**
     * Number of reconnect attempts the cursor I/O loop has issued —
     * succeeded plus failed. Diverges from {@link #getTotalReconnectsSucceeded}
     * when the server is flapping. Returns 0 if no I/O loop is running.
     */
    public long getTotalReconnectAttempts() {
        CursorWebSocketSendLoop l = cursorSendLoop;
        return l == null ? 0L : l.getTotalReconnectAttempts();
    }

    /**
     * Number of successful reconnects. Returns 0 if no I/O loop is running.
     */
    public long getTotalReconnectsSucceeded() {
        CursorWebSocketSendLoop l = cursorSendLoop;
        return l == null ? 0L : l.getTotalReconnects();
    }

    /**
     * Total errors observed by the I/O loop (retriable and terminal combined).
     */
    public long getTotalServerErrors() {
        CursorWebSocketSendLoop l = cursorSendLoop;
        return l == null ? 0L : l.getTotalServerErrors();
    }

    /**
     * Adds an INT column value to the current row.
     *
     * @param columnName the column name
     * @param value      the int value
     * @return this sender for method chaining
     */
    public QwpWebSocketSender intColumn(CharSequence columnName, int value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_INT, true);
            if (col != null) {
                col.addInt(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    /**
     * Adds an IPv4 column value to the current row, as a packed 32-bit address
     * in host byte order (e.g. 192.168.1.1 -> 0xC0A80101).
     * <p>
     * Use {@link Numbers#parseIPv4(CharSequence)} to parse a dotted-quad string,
     * or call {@link #ipv4Column(CharSequence, CharSequence)}. Per QuestDB
     * convention, the address 0.0.0.0 maps to IPv4 NULL on read, regardless of
     * whether the row was marked null on the wire.
     *
     * @param columnName the column name
     * @param address    the packed IPv4 address
     * @return this sender for method chaining
     */
    @Override
    public QwpWebSocketSender ipv4Column(CharSequence columnName, int address) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_IPv4, true);
            if (col != null) {
                col.addIPv4(address);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    /**
     * Adds an IPv4 column value from a dotted-quad string (e.g. "192.168.1.1").
     * <p>
     * NULL handling on this overload is stricter than the underlying
     * {@link Numbers#parseIPv4(CharSequence)} contract:
     * <ul>
     *   <li>A {@code null} reference is a no-op: the column is skipped for
     *       this row and gets null-padded on commit (same shape as
     *       omitting the setter entirely).</li>
     *   <li>The literal string {@code "null"} (case-insensitive) is
     *       rejected with {@link LineSenderException}, even though
     *       {@code parseIPv4} would coerce it to the IPv4 NULL sentinel.</li>
     *   <li>The dotted-quad {@code "0.0.0.0"} is rejected for the same
     *       reason: its bit pattern is the IPv4 NULL sentinel and the
     *       value would silently round-trip as SQL NULL on read.</li>
     * </ul>
     * Pass a null reference (or omit the setter) when you want to mark a
     * row null; otherwise pass a real dotted-quad address.
     *
     * @param columnName the column name
     * @param address    dotted-quad IPv4 address; null reference is treated
     *                   as "skip this column for the current row"
     * @return this sender for method chaining
     * @throws LineSenderException if the address fails to parse, equals
     *                             {@code "null"} (case-insensitive), or
     *                             equals {@code "0.0.0.0"}
     */
    @Override
    public QwpWebSocketSender ipv4Column(CharSequence columnName, CharSequence address) {
        checkNotClosed();
        if (address == null) {
            return this;
        }
        checkTableSelected();
        if (Chars.equalsIgnoreCase("null", address) || Chars.equals("0.0.0.0", address)) {
            throw new LineSenderException(
                    "invalid IPv4 address: NULL sentinel inputs are rejected"
                            + "; pass a null reference or omit the setter to mark the row null [address=")
                    .put(address).put(']');
        }
        int packed;
        try {
            packed = Numbers.parseIPv4(address);
        } catch (NumericException e) {
            throw new LineSenderException("invalid IPv4 address: ").put(address);
        }
        return ipv4Column(columnName, packed);
    }

    /**
     * Adds a LONG256 column value to the current row.
     *
     * @param columnName the column name
     * @param l0         the least significant 64 bits
     * @param l1         the second 64 bits
     * @param l2         the third 64 bits
     * @param l3         the most significant 64 bits
     * @return this sender for method chaining
     */
    public QwpWebSocketSender long256Column(CharSequence columnName, long l0, long l1, long l2, long l3) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_LONG256, true);
            if (col != null) {
                col.addLong256(l0, l1, l2, l3);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[] values) {
        checkNotClosed();
        if (values == null) return this;
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_LONG_ARRAY, true);
            if (col != null) {
                col.addLongArray(values);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][] values) {
        checkNotClosed();
        if (values == null) return this;
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_LONG_ARRAY, true);
            if (col != null) {
                col.addLongArray(values);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, long[][][] values) {
        checkNotClosed();
        if (values == null) return this;
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_LONG_ARRAY, true);
            if (col != null) {
                col.addLongArray(values);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public Sender longArray(@NotNull CharSequence name, LongArray array) {
        checkNotClosed();
        if (array == null) return this;
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(name, QwpConstants.TYPE_LONG_ARRAY, true);
            if (col != null) {
                col.addLongArray(array);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public QwpWebSocketSender longColumn(CharSequence columnName, long value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_LONG, true);
            if (col != null) {
                col.addLong(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    /**
     * Returns a {@link CursorWebSocketSendLoop.ReconnectFactory} that, on each
     * call, performs the multi-endpoint walk and returns a freshly connected
     * {@link WebSocketClient}. Each factory holds private "previously-bound
     * endpoint" state for mid-stream-failure attribution; the host tracker
     * itself is shared across factories.
     */
    public CursorWebSocketSendLoop.ReconnectFactory newReconnectFactory() {
        return new ReconnectSupplier();
    }

    /**
     * Test seam: a BACKGROUND reconnect factory identical to the ones
     * {@link #startOrphanDrainers} hands to orphan drainers (abort gate =
     * the supplied stop flag, {@code isBackground()=true}), so tests can
     * exercise the background side of the connect-walk lock policy (see
     * {@link #buildAndConnect}) without reflection.
     */
    @TestOnly
    public CursorWebSocketSendLoop.ReconnectFactory newBackgroundReconnectFactory(
            java.util.function.BooleanSupplier stopFlag
    ) {
        return new ReconnectSupplier(stopFlag, "drainer stop requested during connect");
    }

    /**
     * Test seam: installs the per-attempt WebSocket client factory override
     * consulted by {@code newWebSocketClient()} inside the connect walk.
     * Production code never sets it.
     */
    @TestOnly
    public void setClientFactoryOverride(java.util.function.Supplier<WebSocketClient> factory) {
        this.clientFactoryOverride = factory;
    }

    @Override
    public void reset() {
        checkNotClosed();
        // Reset ALL table buffers, not just the current one
        ObjList<CharSequence> keys = tableBuffers.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            QwpTableBuffer buf = tableBuffers.get(keys.getQuick(i));
            if (buf != null) {
                buf.reset();
            }
        }
        pendingBytes = 0;
        pendingRowCount = 0;
        firstPendingRowTimeNanos = 0;
        currentTableBuffer = null;
        currentTableBufferSnapshotBytes = 0;
        currentTableName = null;
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
    }

    /**
     * Register an async listener for connection-state transitions: initial
     * connect, primary failover, endpoint attempt failures, the full address
     * list being unreachable, and terminal auth/budget rejections.
     * <p>
     * May be called either before or after {@code connect()} -- when called
     * after, the change propagates to the live dispatcher and takes effect on
     * the next delivery. Pass {@code null} to revert to the loud-not-silent
     * default.
     * <p>
     * The listener is invoked on a dedicated daemon dispatcher thread, never
     * on the I/O thread or the producer thread; slow listeners cannot stall
     * publishing or reconnect. See {@link SenderConnectionListener} for the
     * full delivery contract.
     */
    @TestOnly
    public void setConnectedForTest(boolean connected) {
        this.connected = connected;
    }

    public void setDeferCommit(boolean enabled) {
        this.deferCommit = enabled;
    }

    public void setConnectionListener(SenderConnectionListener listener) {
        SenderConnectionListener effective = listener != null ? listener : DefaultSenderConnectionListener.INSTANCE;
        this.connectionListener = effective;
        SenderConnectionDispatcher d = connectionDispatcher;
        if (d != null) {
            d.setListener(effective);
        }
    }

    /**
     * Configure the bounded inbox capacity used by the connection-event
     * dispatcher. Must be called before {@code connect()}; later changes have
     * no effect.
     */
    public void setConnectionListenerInboxCapacity(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("connectionListenerInboxCapacity must be >= 1, was " + capacity);
        }
        this.connectionListenerInboxCapacity = capacity;
    }

    /**
     * Attach a {@link CursorSendEngine} for store-and-forward. Must be called
     * before the first send.
     */
    public void setCursorEngine(CursorSendEngine engine, boolean takeOwnership) {
        if (closed) {
            throw new LineSenderException("Sender is closed");
        }
        if (connected) {
            throw new LineSenderException(
                    "setCursorEngine must be called before the first send");
        }
        this.cursorEngine = engine;
        this.ownsCursorEngine = takeOwnership && engine != null;
    }

    /**
     * Register an async observer for background orphan-slot drainer events.
     * May be called either before or after {@link #startOrphanDrainers} —
     * when called before, the drainer pool picks it up as its submit-time
     * default; when called after, it propagates to the pool AND to every
     * live drainer (per-drainer re-assignment while running is explicitly
     * permitted by the drainer's listener contract). Pass {@code null} to
     * clear. {@code synchronized} to coordinate with
     * {@code startOrphanDrainers}: a concurrent submit either observes the
     * pool listener already set or is covered by the snapshot propagation.
     */
    public synchronized void setDrainerListener(BackgroundDrainerListener listener) {
        this.drainerListener = listener;
        BackgroundDrainerPool pool = drainerPool;
        if (pool != null) {
            // Submit-time fallback for drainers not yet submitted...
            pool.setListener(listener);
            // ...and direct re-assignment for the ones already running (the
            // pool listener is only applied at submit time, never after).
            ObjList<io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer> live =
                    pool.snapshot();
            for (int i = 0, n = live.size(); i < n; i++) {
                live.getQuick(i).setListener(listener);
            }
        }
    }

    /**
     * Configure the user-supplied error handler. May be called either before
     * or after {@code connect()} — when called after, the change propagates
     * to the live dispatcher and takes effect on the next delivery. Pass
     * {@code null} to revert to the loud-not-silent default.
     */
    public void setErrorHandler(SenderErrorHandler handler) {
        SenderErrorHandler effective = handler != null ? handler : DefaultSenderErrorHandler.INSTANCE;
        this.errorHandler = effective;
        SenderErrorDispatcher d = errorDispatcher;
        if (d != null) {
            d.setHandler(effective);
        }
    }

    /**
     * Configure the bounded inbox capacity used by the dispatcher. Must be
     * called before {@code connect()}; later changes have no effect.
     * The minimum follows sf-client.md section 4.4: drop-oldest under bursts
     * needs a wide enough window to preserve the trailing category distribution.
     */
    public void setErrorInboxCapacity(int capacity) {
        if (capacity < MIN_ERROR_INBOX_CAPACITY) {
            throw new IllegalArgumentException("errorInboxCapacity must be >= "
                    + MIN_ERROR_INBOX_CAPACITY + ", was " + capacity);
        }
        this.errorInboxCapacity = capacity;
    }

    public void setTransactional(boolean transactional) {
        this.transactional = transactional;
    }

    /**
     * Register an async observer for ack-watermark advances. May be called
     * either before or after {@code connect()} — post-connect changes
     * propagate to the live dispatcher and take effect on the next delivery.
     * Pass {@code null} to revert to the no-op default.
     *
     * <p>The handler is invoked on a dedicated daemon dispatcher thread, not
     * on the I/O thread or the producer thread — slow handlers cannot stall
     * publishing. Multiple deliveries are permitted across the lifetime of a
     * sender, with monotonically-increasing FSNs; see
     * {@link SenderProgressHandler}.
     */
    public void setProgressHandler(SenderProgressHandler handler) {
        SenderProgressHandler effective = handler != null ? handler : DefaultSenderProgressHandler.INSTANCE;
        this.progressHandler = effective;
        SenderProgressDispatcher d = progressDispatcher;
        if (d != null) {
            d.setHandler(effective);
        }
    }

    /**
     * Adds a SHORT column value to the current row.
     *
     * @param columnName the column name
     * @param value      the short value
     * @return this sender for method chaining
     */
    public QwpWebSocketSender shortColumn(CharSequence columnName, short value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_SHORT, false);
            if (col != null) {
                col.addShort(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    /**
     * Starts orphan drainers for the given list of slot paths. Each path
     * gets its own drainer thread, capped at {@code maxBackgroundDrainers}
     * concurrent. Drainers run until the slot is fully drained or a
     * terminal error occurs (then they drop a {@code .failed} sentinel).
     * <p>
     * Should be called once, immediately after {@code connect()} returns.
     * Subsequent calls add more drainers to the same pool.
     */
    public synchronized void startOrphanDrainers(
            io.questdb.client.std.ObjList<String> orphanSlotPaths,
            int maxBackgroundDrainers,
            long segmentSizeBytes,
            long sfMaxTotalBytes
    ) {
        if (orphanSlotPaths == null || orphanSlotPaths.size() == 0
                || maxBackgroundDrainers <= 0) {
            return;
        }
        if (drainerPool == null) {
            drainerPool = new io.questdb.client.cutlass.qwp.client.sf.cursor
                    .BackgroundDrainerPool(maxBackgroundDrainers);
            // Install the user listener as the pool's submit-time default so
            // the drainers submitted below observe it from their first event.
            drainerPool.setListener(this.drainerListener);
        }
        for (int i = 0, n = orphanSlotPaths.size(); i < n; i++) {
            String slot = orphanSlotPaths.get(i);
            // The drainer's connects must NOT be gated on the foreground
            // sender's lifecycle: close() stops the foreground I/O loop
            // BEFORE the drainer pool's graceful-drain window, so a
            // foreground-gated factory would reject every drainer
            // (re)connect with "sender closed during connect" during that
            // window, leaving the orphan slot un-drained (and Invariant B
            // forbids quarantining it on a transport-shaped error). Gate
            // each drainer's factory on the drainer's OWN stop flag
            // instead. The one-element array breaks the construction cycle
            // (the factory needs the drainer, the drainer's constructor
            // needs the factory); the ref write happens-before the drainer
            // runs because submit() publishes the task afterwards.
            final io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer[] ref =
                    new io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer[1];
            ReconnectSupplier factory = new ReconnectSupplier(
                    () -> {
                        io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer d = ref[0];
                        return d != null && d.isStopRequested();
                    },
                    "drainer stop requested during connect");
            io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer drainer =
                    new io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer(
                            slot, segmentSizeBytes, sfMaxTotalBytes,
                            factory,
                            reconnectMaxDurationMillis,
                            reconnectInitialBackoffMillis,
                            reconnectMaxBackoffMillis,
                            requestDurableAck,
                            durableAckKeepaliveIntervalMillis,
                            maxFrameRejections,
                            poisonMinEscalationWindowMillis);
            ref[0] = drainer;
            drainerPool.submit(drainer);
        }
    }

    @Override
    public QwpWebSocketSender stringColumn(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_VARCHAR, true);
            if (col != null) {
                col.addString(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public QwpWebSocketSender symbol(CharSequence columnName, CharSequence value) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_SYMBOL, true);
            if (col != null) {
                col.addSymbol(value);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public QwpWebSocketSender table(CharSequence tableName) {
        checkNotClosed();
        // Fast path: if table name matches current, skip hashmap lookup
        if (currentTableName != null && currentTableBuffer != null && Chars.equals(tableName, currentTableName)) {
            return this;
        }
        // Prevent switching tables while a row is in progress
        if (currentTableBuffer != null && currentTableBuffer.hasInProgressRow()) {
            throw new LineSenderException("cannot switch tables while row is in progress"
                    + " [currentTable=").put(currentTableName).put(']');
        }
        // Table changed - invalidate cached column references
        validateTableName(tableName);
        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;
        currentTableBuffer = tableBuffers.get(tableName);
        if (currentTableBuffer != null) {
            currentTableName = currentTableBuffer.getTableName();
        } else {
            currentTableName = tableName.toString();
            currentTableBuffer = new QwpTableBuffer(currentTableName, this);
            tableBuffers.put(currentTableName, currentTableBuffer);
        }
        // Re-snap so sendRow()'s delta math is anchored to this table's
        // current byte count. The prior current table's bytes already match
        // its last-snapped value (the in-progress-row guard above ensures
        // no column setters ran on it since the last consistency point).
        currentTableBufferSnapshotBytes = currentTableBuffer.getBufferedBytes();
        // Both modes accumulate rows until flush
        return this;
    }

    @Override
    public QwpWebSocketSender timestampColumn(CharSequence columnName, long value, ChronoUnit unit) {
        checkNotClosed();
        checkTableSelected();
        try {
            if (unit == ChronoUnit.NANOS) {
                QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_TIMESTAMP_NANOS, true);
                if (col != null) {
                    col.addLong(value);
                }
            } else {
                long micros = toMicros(value, unit);
                QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_TIMESTAMP, true);
                if (col != null) {
                    col.addLong(micros);
                }
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    @Override
    public QwpWebSocketSender timestampColumn(CharSequence columnName, Instant value) {
        checkNotClosed();
        checkTableSelected();
        try {
            long micros = value.getEpochSecond() * 1_000_000L + value.getNano() / 1000L;
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_TIMESTAMP, true);
            if (col != null) {
                col.addLong(micros);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    /**
     * Adds a UUID column value to the current row.
     *
     * @param columnName the column name
     * @param lo         the low 64 bits of the UUID
     * @param hi         the high 64 bits of the UUID
     * @return this sender for method chaining
     */
    public QwpWebSocketSender uuidColumn(CharSequence columnName, long lo, long hi) {
        checkNotClosed();
        checkTableSelected();
        try {
            QwpTableBuffer.ColumnBuffer col = currentTableBuffer.getOrCreateColumn(columnName, QwpConstants.TYPE_UUID, true);
            if (col != null) {
                col.addUuid(hi, lo);
            }
        } catch (RuntimeException | Error e) {
            rollbackRow();
            throw e;
        }
        return this;
    }

    /**
     * True iff this sender has at least once installed a live (connected
     * + upgraded) WebSocket. Sticky — once true, stays true even after a
     * subsequent disconnect. Lets a {@link SenderErrorHandler}
     * disambiguate a "never reached the server" terminal failure (likely
     * a config typo or firewall block) from a "lost connection after we
     * were up" failure (likely transient). Returns {@code false} if no
     * I/O loop is running.
     */
    public boolean wasEverConnected() {
        CursorWebSocketSendLoop l = cursorSendLoop;
        return l != null && l.hasEverConnected();
    }

    private static Throwable captureCloseError(Throwable terminalError, Throwable t) {
        if (terminalError == null) {
            return t;
        }
        if (terminalError != t) {
            terminalError.addSuppressed(t);
        }
        return terminalError;
    }

    private static long maskGeoHashBits(long value, int precisionBits) {
        return precisionBits >= 64 ? value : value & ((1L << precisionBits) - 1L);
    }

    private static void rethrowTerminal(Throwable t) {
        if (t == null) {
            return;
        }
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        }
        if (t instanceof Error) {
            throw (Error) t;
        }
        // Wrap any checked Throwable so close() stays declared without a
        // throws clause. flush/drain only ever raises RuntimeException
        // subclasses today, but defending against future changes here is
        // cheaper than chasing a leaked checked throw later. Pass the
        // original as cause so the stack trace and chained causes survive.
        throw new LineSenderException("close failed: " + t.getMessage(), t);
    }

    private static List<Endpoint> singleEndpoint(String host, int port) {
        return Collections.singletonList(new Endpoint(host, port));
    }

    /**
     * Clamps the soft-flush byte budget to fit under the server's advertised
     * X-QWP-Max-Batch-Size minus a safety margin for encoding overhead
     * (schema, dict deltas, framing). A 0 advertisement means the server did
     * not send the header (older build) and the configured budget is kept
     * verbatim. Called on every successful connect because a rolling upgrade
     * can leave neighbouring endpoints with different caps.
     * <p>
     * Always updates {@link #serverMaxBatchSize} so the single-row hard guard
     * in {@link #sendRow} fires against the freshly advertised value. The
     * byte trigger, however, is only adjusted when the user left it enabled:
     * an explicit {@code auto_flush_bytes=off} (autoFlushBytes == 0) is
     * preserved even when the server advertises a cap, so applications that
     * opted out keep the contract they asked for.
     */
    @TestOnly
    public void applyServerBatchSizeLimit(int advertisedMaxBatchSize) {
        serverMaxBatchSize = advertisedMaxBatchSize;
        if (autoFlushBytes <= 0) {
            // User opted out of byte-based auto-flush; respect that even when
            // the server advertises a cap. The single-row guard still protects
            // against oversize individual rows via serverMaxBatchSize.
            effectiveAutoFlushBytes = 0;
            return;
        }
        if (advertisedMaxBatchSize <= 0) {
            effectiveAutoFlushBytes = autoFlushBytes;
            return;
        }
        // Cap at 90% of the server's hard limit. Raw column-buffer bytes are
        // a conservative proxy for wire size (compression usually shrinks the
        // payload), but schema and dict-delta overhead can push the wire size
        // above the raw total in pathological cases.
        long safeBudget = (long) advertisedMaxBatchSize * 9 / 10;
        if (autoFlushBytes < safeBudget) {
            effectiveAutoFlushBytes = autoFlushBytes;
        } else {
            effectiveAutoFlushBytes = (int) safeBudget;
        }
    }

    private void atMicros(long timestampMicros) {
        // Add designated timestamp column (empty name for designated timestamp)
        // Use cached reference to avoid hashmap lookup per row
        if (cachedTimestampColumn == null) {
            cachedTimestampColumn = currentTableBuffer.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP);
        }
        cachedTimestampColumn.addLong(timestampMicros);
        sendRow();
    }

    private void atNanos(long timestampNanos) {
        // Add designated timestamp column (empty name for designated timestamp)
        // Use cached reference to avoid hashmap lookup per row
        if (cachedTimestampNanosColumn == null) {
            cachedTimestampNanosColumn = currentTableBuffer.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP_NANOS);
        }
        cachedTimestampNanosColumn.addLong(timestampNanos);
        sendRow();
    }

    /**
     * Resolves the connect timeout for one {@code buildAndConnect} walk.
     * Foreground connects honour the configured value verbatim: 0 (the
     * default) keeps the historical untimed native connect, bounded only by
     * the OS (SYN retries, 60-130s on Linux). Background (drainer) connects
     * get a finite fallback instead: during an outage a drainer is routinely
     * parked inside a blocking native connect that neither unpark nor
     * interrupt cancels, so the drainer pool's shutdownNow path (~3s into
     * sender.close()) reliably lands on the failed-stop protocol -- the
     * WebSocket client and microbatch buffers are deliberately leaked and
     * the slot lock is held until the OS deadline resolves the connect. A
     * finite background deadline bounds that window to seconds without
     * changing foreground semantics. Exposed for unit tests.
     */
    @TestOnly
    public static int effectiveConnectTimeoutMs(boolean background, int configuredMs) {
        return background && configuredMs <= 0 ? DEFAULT_BACKGROUND_CONNECT_TIMEOUT_MS : configuredMs;
    }

    /**
     * Builds the per-attempt WebSocket client for {@link #buildAndConnect}.
     * Production path delegates to {@link WebSocketClientFactory}; tests may
     * install {@link #clientFactoryOverride} to substitute a stub.
     */
    /**
     * Best-effort close for a client being abandoned because a JVM Error is
     * about to be rethrown: under OOM {@code close()} itself can throw, and a
     * secondary failure must not mask the original Error. {@code close()} is
     * CAS-gated, so re-closing an already-closed client is a no-op.
     */
    private static void closeQuietlyOnError(WebSocketClient client) {
        try {
            client.close();
        } catch (Throwable ignored) {
            // best-effort; the original Error is what must surface
        }
    }

    private WebSocketClient newWebSocketClient() {
        java.util.function.Supplier<WebSocketClient> override = clientFactoryOverride;
        if (override != null) {
            return override.get();
        }
        return tlsConfig != null
                ? WebSocketClientFactory.newTlsInstance(tlsConfig)
                : WebSocketClientFactory.newPlainTextInstance();
    }

    /**
     * Multi-endpoint connect walk shared by the foreground sender and the
     * background orphan drainers. One invocation sweeps the endpoint list,
     * performing a TCP/TLS connect plus a WebSocket upgrade per endpoint;
     * worst-case sweep duration is
     * {@code endpoints x (connect timeout + upgrade timeout)}:
     * <ul>
     *   <li>foreground walk: {@code connect_timeout} verbatim -- the default
     *       {@code 0} keeps the untimed native connect, bounded only by the
     *       OS SYN-retry deadline (60-130s per endpoint on Linux) -- plus
     *       {@code auth_timeout_ms} (default 15s) for the upgrade;</li>
     *   <li>background walk: 15s connect fallback
     *       ({@link #DEFAULT_BACKGROUND_CONNECT_TIMEOUT_MS}) plus
     *       {@code auth_timeout_ms} -- see
     *       {@link #effectiveConnectTimeoutMs(boolean, int)}.</li>
     * </ul>
     * <p>
     * Concurrency policy -- no network I/O under a sender-wide lock for
     * background work. FOREGROUND walks (the producer's initial connect and
     * the I/O loop's reconnects) hold {@link #connectWalkLock} across the
     * sweep: they own the shared round state and the lifecycle commits, and
     * can only ever wait behind another foreground walk (which cannot
     * happen by construction -- the lock is insurance). BACKGROUND (drainer)
     * walks take NO lock: each sweeps a private
     * {@link QwpHostHealthTracker.RoundCursor} -- full sweep, claim-at-pick,
     * ordered by the live shared health state -- and records results with
     * the health-only overloads ({@code markRoundAttempted=false}), so
     * concurrent drainer sweeps proceed in parallel with each other and
     * with the foreground, share health observations, and can neither
     * consume nor poison the foreground's round. The foreground's
     * reconnect and {@code close()} paths are therefore never queued
     * behind a drainer's endpoint walk.
     */
    private WebSocketClient buildAndConnect(ReconnectSupplier ctx) {
        if (ctx.isBackground()) {
            // Lock-free: the walk below touches only internally-synchronized
            // hostTracker health state and walk-local/cursor-local state on
            // the background path.
            return connectWalk(ctx);
        }
        connectWalkLock.lock();
        try {
            return connectWalk(ctx);
        } finally {
            connectWalkLock.unlock();
        }
    }

    private WebSocketClient connectWalk(ReconnectSupplier ctx) {
        // Background (drainer) factories share this connect walk -- endpoint
        // list and hostTracker HEALTH state (never the shared round: a
        // background sweep walks its own RoundCursor and records with
        // markRoundAttempted=false, so it cannot consume the foreground's
        // round or skew roundSeq) -- but must stay INVISIBLE
        // in the foreground sender's observable state. SenderConnectionEvents
        // describe the FOREGROUND connection's lifecycle, and the cap-derived
        // sizing (serverMaxBatchSize / effectiveAutoFlushBytes) guards the
        // FOREGROUND wire: a drainer connect that committed either would
        // fabricate lifecycle transitions the foreground never had, steal the
        // once-per-lifetime CONNECTED classification, and re-size the
        // producer's batch guard for a connection the producer is not on
        // (oversize batch -> ws-close[1009] -> poison-frame escalation caused
        // by background activity).
        final boolean background = ctx.isBackground();
        // Private full-sweep cursor for background walks: claim-at-pick over
        // cursor-local attempted bits makes the pick -> record pair safe
        // without any walk-wide lock, and guarantees every sweep tries every
        // endpoint exactly once regardless of concurrent walkers.
        final QwpHostHealthTracker.RoundCursor cursor =
                background ? hostTracker.newRoundCursor() : null;
        int previousIdx = ctx.previousIdx;
        if (previousIdx >= 0) {
            // Mid-stream wire failure -- the I/O loop just observed the active
            // connection drop and called us via the reconnect factory. Only a
            // FOREGROUND drop surfaces DISCONNECTED: a drainer's wire drop is
            // not a foreground outage, and reporting it would claim an outage
            // against an endpoint the foreground may be healthily using. The
            // hostTracker health penalty is recorded either way -- the drop
            // was real, whichever loop observed it.
            if (!background) {
                Endpoint priorEp = endpoints.get(previousIdx);
                dispatchConnectionEvent(
                        SenderConnectionEvent.Kind.DISCONNECTED,
                        priorEp.host, priorEp.port,
                        null, SenderConnectionEvent.NO_PORT,
                        SenderConnectionEvent.NO_ATTEMPT_NUMBER,
                        roundSeq,
                        null);
            }
            hostTracker.recordMidStreamFailure(previousIdx);
            ctx.previousIdx = -1;
        }
        // Shared-round lifecycle is foreground-only: a background walk must
        // not advance the round (or roundSeq, which numbers foreground
        // events) under the foreground's feet.
        if (!background && hostTracker.isRoundExhausted()) {
            roundSeq++;
            hostTracker.beginRound(true);
        }
        Throwable lastError = null;
        // Latches the first typed upgrade failure that won't fix on retry
        // within this connect window: durable-ack mismatch (cluster-wide config),
        // version mismatch (rolling upgrade across all endpoints), or any other
        // non-421 WebSocketUpgradeException (4xx/5xx). The catch block walks
        // remaining endpoints in case the failure is per-endpoint, then surfaces
        // this latched typed exception when the round ends without a successful
        // connect -- except that a plain non-421 WebSocketUpgradeException is
        // demoted below role-reject evidence in the epilogue: when any endpoint
        // answered 421+role in the same sweep, the window is transient and the
        // retriable role-mismatch classification wins (the demoted error rides
        // along as a suppressed diagnostic). Auth failures are NOT latched here
        // -- they throw immediately because a rejected credential is uniformly
        // rejected across the cluster.
        HttpClientException terminalUpgradeError = null;
        QwpIngressRoleRejectedException lastRoleReject = null;
        Endpoint lastEndpoint = null;
        while (true) {
            if (ctx.isAborted()) {
                throw new LineSenderException(ctx.abortMessage());
            }
            int idx = background ? cursor.next() : hostTracker.pickNext();
            if (idx < 0) break;
            Endpoint ep = endpoints.get(idx);
            lastEndpoint = ep;
            // Attempt numbers exist for foreground observability only. A
            // background walk fires no events and must not skew the numbering
            // the user sees on subsequent foreground events.
            long attemptNumber = background
                    ? SenderConnectionEvent.NO_ATTEMPT_NUMBER
                    : ++roundConnectAttemptSeq;
            WebSocketClient newClient = newWebSocketClient();
            try {
                newClient.setQwpMaxVersion(QwpConstants.VERSION);
                newClient.setQwpClientId(QwpConstants.CLIENT_ID);
                newClient.setQwpRequestDurableAck(requestDurableAck);
                newClient.setConnectTimeout(effectiveConnectTimeoutMs(background, connectTimeoutMs));
                newClient.connect(ep.host, ep.port);
                int upgradeTimeoutMs = (int) Math.min(authTimeoutMs, Integer.MAX_VALUE);
                newClient.upgrade(WRITE_PATH, upgradeTimeoutMs, authorizationHeader);
            } catch (HttpClientException e) {
                // Close BEFORE classify: the sibling catch (Error) below does not
                // guard catch-arm bodies, so an Error thrown inside classify()
                // (it allocates on the role-reject/auth paths) would escape with
                // the client's fd and native buffers open. Safe to reorder --
                // classify reads only heap fields (upgradeRejectRole/Zone,
                // upgradeStatusCode) that are set during upgrade() and survive
                // close().
                newClient.close();
                HttpClientException classified = QwpUpgradeFailures.classify(newClient, ep.host, ep.port, e);
                if (classified instanceof QwpIngressRoleRejectedException) {
                    QwpIngressRoleRejectedException re = (QwpIngressRoleRejectedException) classified;
                    hostTracker.recordRoleReject(idx, re.isTransient(), !background);
                    lastError = re;
                    lastRoleReject = re;
                    if (!background) {
                        dispatchConnectionEvent(
                                SenderConnectionEvent.Kind.ENDPOINT_ATTEMPT_FAILED,
                                ep.host, ep.port, null, SenderConnectionEvent.NO_PORT,
                                attemptNumber, roundSeq, re);
                    }
                    continue;
                }
                if (classified instanceof QwpAuthFailedException) {
                    // Auth is uniform across the cluster; we won't keep walking
                    // endpoints. Fire AUTH_FAILED before throwing so the user
                    // listener observes the terminal classification at the
                    // moment the I/O thread gives up, ahead of the producer
                    // thread learning via LineSenderException on the next
                    // API call.
                    if (!background) {
                        dispatchConnectionEvent(
                                SenderConnectionEvent.Kind.AUTH_FAILED,
                                ep.host, ep.port, null, SenderConnectionEvent.NO_PORT,
                                attemptNumber, roundSeq, classified);
                    }
                    throw classified;
                }
                if (terminalUpgradeError == null && (
                        classified instanceof QwpVersionMismatchException
                                || (classified instanceof WebSocketUpgradeException
                                && !((WebSocketUpgradeException) classified).isRoleMismatch()))) {
                    terminalUpgradeError = classified;
                }
                hostTracker.recordTransportError(idx, !background);
                lastError = classified;
                if (!background) {
                    dispatchConnectionEvent(
                            SenderConnectionEvent.Kind.ENDPOINT_ATTEMPT_FAILED,
                            ep.host, ep.port, null, SenderConnectionEvent.NO_PORT,
                            attemptNumber, roundSeq, classified);
                }
                continue;
            } catch (Exception e) {
                newClient.close();
                hostTracker.recordTransportError(idx, !background);
                lastError = e;
                if (!background) {
                    dispatchConnectionEvent(
                            SenderConnectionEvent.Kind.ENDPOINT_ATTEMPT_FAILED,
                            ep.host, ep.port, null, SenderConnectionEvent.NO_PORT,
                            attemptNumber, roundSeq, e);
                }
                continue;
            } catch (Error e) {
                // JVM failure (OOM, LinkageError, StackOverflowError) during
                // connect/upgrade. Without this catch the half-built client
                // escaped with its fd and native buffers open -- unreachable
                // by GC, freed only in close(). Close it quietly: under OOM
                // close() itself can throw, and a secondary failure must not
                // mask the original Error. Deliberately NO hostTracker penalty
                // and NO ENDPOINT_ATTEMPT_FAILED event -- a JVM failure is not
                // endpoint health data, and misclassifying it would poison the
                // walk. Rethrow: every retry loop upstream (connectWithRetry,
                // the cursor reconnect loop, BackgroundDrainer) rethrows Error
                // rather than retrying, so this stays a loud one-shot failure.
                closeQuietlyOnError(newClient);
                throw e;
            }
            // Guard the post-upgrade tail: from here until newClient is
            // returned, an escaping JVM Error would leak the CONNECTED
            // client's fd and native buffers -- the same class the
            // connect/upgrade catch (Error) arm above closes over. The
            // success-event dispatch is the realistic trigger: it allocates
            // the SenderConnectionEvent plus a deque node, and on a clean
            // first connect it is also the dispatcher's first offer(), which
            // lazy-starts the dispatcher thread (Thread.start() can itself
            // fail with OOM). Same contract as the arm above: close quietly
            // (a secondary failure must not mask the original Error) and
            // rethrow. close() is CAS-gated, so re-closing after the
            // durable-ack arm's own close is a no-op.
            try {
                if (requestDurableAck && !newClient.isServerDurableAckEnabled()) {
                    newClient.close();
                    hostTracker.recordRoleReject(idx, false, !background);
                    QwpDurableAckMismatchException ackErr = new QwpDurableAckMismatchException(
                            ep.host, ep.port, null);
                    if (terminalUpgradeError == null) {
                        terminalUpgradeError = ackErr;
                    }
                    lastError = ackErr;
                    if (!background) {
                        dispatchConnectionEvent(
                                SenderConnectionEvent.Kind.ENDPOINT_ATTEMPT_FAILED,
                                ep.host, ep.port, null, SenderConnectionEvent.NO_PORT,
                                attemptNumber, roundSeq, ackErr);
                    }
                    continue;
                }
                hostTracker.recordSuccess(idx, !background);
                ctx.previousIdx = idx;
                if (background) {
                    // Walk bookkeeping only: recordSuccess feeds the shared health
                    // tracker and ctx.previousIdx arms this factory's own
                    // mid-stream-failure handling on its next reconnect. No
                    // lifecycle event, no CONNECTED/RECONNECTED/FAILED_OVER
                    // classification state, no producer batch re-sizing -- the
                    // drainer's lifecycle is observable via
                    // BackgroundDrainerListener and the drainer counters, never
                    // the foreground connection-event stream.
                    return newClient;
                }
                int previousLiveIdx = currentEndpointIdx;
                currentEndpointIdx = idx;
                // Classify the success. CONNECTED only fires once per sender
                // lifetime; subsequent successes are RECONNECTED (same endpoint
                // as before) or FAILED_OVER (different endpoint). hasEverConnected
                // is set after the classification so the very first success picks
                // CONNECTED before flipping the flag.
                SenderConnectionEvent.Kind successKind;
                String prevHost = null;
                int prevPort = SenderConnectionEvent.NO_PORT;
                if (!hasEverConnected) {
                    successKind = SenderConnectionEvent.Kind.CONNECTED;
                    hasEverConnected = true;
                } else if (previousLiveIdx == idx) {
                    successKind = SenderConnectionEvent.Kind.RECONNECTED;
                } else {
                    successKind = SenderConnectionEvent.Kind.FAILED_OVER;
                    if (previousLiveIdx >= 0) {
                        Endpoint prevEp = endpoints.get(previousLiveIdx);
                        prevHost = prevEp.host;
                        prevPort = prevEp.port;
                    }
                }
                dispatchConnectionEvent(
                        successKind, ep.host, ep.port, prevHost, prevPort,
                        attemptNumber, roundSeq, null);
                // Refresh the cap-derived state before returning the new client so
                // the producer thread observes the new endpoint's advertised
                // X-QWP-Max-Batch-Size from the next sendRow onwards. Skipping this
                // on a mid-stream failover leaves the sender sized for the prior
                // endpoint's cap; an oversize row then escapes the producer-side
                // guard and trips a wire-level ws-close[1009] downstream.
                applyServerBatchSizeLimit(newClient.getServerMaxBatchSize());
                return newClient;
            } catch (Error e) {
                closeQuietlyOnError(newClient);
                throw e;
            }
        }
        // Round walked every endpoint without a success. Surface
        // ALL_ENDPOINTS_UNREACHABLE before any of the typed throws so a
        // single failed sweep produces exactly one such event regardless of
        // which terminal branch fires next. The connectLoop wrapper retries,
        // and each retry that re-enters this method and fails again produces
        // its own ALL_ENDPOINTS_UNREACHABLE event.
        if (!background && lastEndpoint != null) {
            dispatchConnectionEvent(
                    SenderConnectionEvent.Kind.ALL_ENDPOINTS_UNREACHABLE,
                    lastEndpoint.host, lastEndpoint.port,
                    null, SenderConnectionEvent.NO_PORT,
                    SenderConnectionEvent.NO_ATTEMPT_NUMBER, roundSeq, lastError);
        }
        // Role-reject evidence outranks a latched plain non-421 upgrade
        // error. In a mixed sweep -- e.g. [replica(421+role),
        // replica(421+role), node(503)] -- the co-occurring 421+role
        // responses are positive evidence of a transient failover/promotion
        // window; throwing the latched 5xx/4xx here would misclassify that
        // window as terminal (dead foreground sender, or a drainer slot
        // quarantine via BackgroundDrainer markFailed). Only plain
        // WebSocketUpgradeException is demoted: the typed capability gaps
        // (QwpVersionMismatchException, QwpDurableAckMismatchException)
        // extend HttpClientException directly, so they fall through this
        // check and stay terminal even when replicas role-rejected in the
        // same sweep -- the contract the durable-ack paragraph below
        // documents and relies on.
        if (terminalUpgradeError != null
                && !(lastRoleReject != null
                && terminalUpgradeError instanceof WebSocketUpgradeException)) {
            throw terminalUpgradeError;
        }
        if (lastRoleReject != null) {
            // Every endpoint either role-rejected the /write/v4 upgrade or
            // failed with a demoted non-421 upgrade error: right now the
            // reachable, role-classified nodes are all replicas (or
            // primary-catchup). That is a TRANSIENT failover window, not a
            // terminal condition -- a replica can be promoted and a primary
            // will reappear. Surface it as a retriable
            // QwpRoleMismatchException so the SYNC/ASYNC connect and
            // reconnect loops keep the rows in store-and-forward and retry
            // within reconnect_max_duration_millis (for an SF sender the only
            // terminal condition is SF exhaustion).
            //
            // This holds even when durable ack was requested: a replica that
            // gets promoted serves durable ack, so an all-replica window must
            // NOT be reported as a durable-ack mismatch. Doing so conflated a
            // transient role state with a permanent capability gap and hard-
            // failed HA senders that should have recovered on promotion. A
            // genuine capability gap -- an endpoint that upgrades but does not
            // advertise durable ack -- is still terminal: it is raised as
            // terminalUpgradeError above, before this block.
            QwpRoleMismatchException ex = new QwpRoleMismatchException(
                    QwpIngressRoleRejectedException.ROLE_PRIMARY,
                    null,
                    "walked all " + endpoints.size()
                            + " endpoint(s); no endpoint matches target="
                            + QwpIngressRoleRejectedException.ROLE_PRIMARY
                            + "; last observed role=" + lastRoleReject.getRole()
                            + " at " + lastRoleReject.getHost() + ':' + lastRoleReject.getPort());
            ex.initCause(lastRoleReject);
            if (terminalUpgradeError != null) {
                // Keep the demoted non-421 upgrade error observable for
                // diagnostics without changing the surfaced classification.
                ex.addSuppressed(terminalUpgradeError);
            }
            throw ex;
        }
        LineSenderException ex = new LineSenderException(lastError);
        ex.put("Failed to connect: ");
        if (lastEndpoint == null) {
            ex.put("no endpoints available");
        } else {
            ex.put("all ").put(endpoints.size()).put(" endpoint(s) unreachable; last=")
                    .put(lastEndpoint.host).put(':').put(lastEndpoint.port);
        }
        throw ex;
    }

    private void checkConnectionError() {
        LineSenderException error = connectionError.get();
        if (error != null) {
            // Refresh the stack so subsequent public API calls point at the
            // call that observed the terminal sender state, not the I/O thread
            // that originally recorded the failure.
            error.fillInStackTrace();
            throw error;
        }
        // Poll the cursor I/O loop's terminalError too. Without this, a fatal
        // wire / server-rejection error recorded by the I/O thread would
        // only surface on the next flush() / close() — every row-level
        // method (table, longColumn, atNow, etc.) routes through
        // checkNotClosed → checkConnectionError, so failing to poll here
        // means callers can keep accumulating rows long after the sender
        // is already broken.
        if (cursorSendLoop != null) {
            cursorSendLoop.checkError();
        }
    }

    private void checkNotClosed() {
        if (closed) {
            throw new LineSenderException("Sender is closed");
        }
        checkConnectionError();
    }

    private void checkTableSelected() {
        if (currentTableBuffer == null) {
            throw new LineSenderException("table() must be called before adding columns");
        }
    }

    private int countNonEmptyTables(ObjList<CharSequence> keys) {
        int tableCount = 0;
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer != null && tableBuffer.getRowCount() > 0) {
                tableCount++;
            }
        }
        return tableCount;
    }

    private Endpoint currentEndpoint() {
        int idx = currentEndpointIdx;
        return idx < 0 ? null : endpoints.get(idx);
    }

    /**
     * Build and offer a connection event to the dispatcher. No-op when the
     * dispatcher has not been allocated yet (e.g. very early in connect()
     * before ensureConnected wired it up). The dispatcher itself is
     * non-blocking and drops on overflow; this helper is safe to call from
     * any thread.
     */
    private void dispatchConnectionEvent(
            SenderConnectionEvent.Kind kind,
            String host,
            int port,
            String previousHost,
            int previousPort,
            long attemptNumber,
            long roundNumber,
            Throwable cause
    ) {
        SenderConnectionDispatcher d = connectionDispatcher;
        if (d == null) {
            return;
        }
        d.offer(new SenderConnectionEvent(
                kind, host, port, previousHost, previousPort,
                attemptNumber, roundNumber, cause, System.currentTimeMillis()));
    }

    /**
     * Bounded drain on close: block until {@code ackedFsn >= publishedFsn}
     * or until {@code closeFlushTimeoutMillis} elapses. {@code <= 0} skips
     * the drain (fast close). On timeout, throw a {@link LineSenderException}
     * so the caller cannot silently lose data — close() collects the
     * exception, finishes shutdown, and rethrows it from close() itself.
     * SF-mode users can recover the unacked tail by reopening a sender on
     * the same SF directory; memory-mode users have no recovery path and
     * must treat this as fatal.
     * <p>
     * A latched terminal error means the server will never ACK up to
     * {@code target}, so the drain must stop on it regardless. Whether it is
     * also re-thrown from close() is a separate surfacing policy that mirrors
     * the step-2 safety net in {@link #close()}:
     * <ul>
     *   <li>{@code errorOwnedByCustomHandler == true}: a custom error handler
     *   has already delivered this terminal to the user, so stop silently —
     *   re-throwing here would double-signal it (the M3 drainOnClose
     *   double-signal).</li>
     *   <li>{@code errorOwnedByCustomHandler == false}: re-throw via
     *   {@code checkError()} to preserve the loud safety net (a
     *   config-string-only caller's only channel). The throw also breaks the
     *   loop; an error a synchronous {@code flush()}/{@code at()} caller
     *   already owns is then suppressed by close()'s
     *   {@code terminalError == alreadyOwnedByUser} check, so it is not
     *   double-signalled either.</li>
     * </ul>
     *
     * @param errorOwnedByCustomHandler whether the async dispatcher has
     *                                  already delivered a terminal to a
     *                                  user-installed handler
     */
    private void drainOnClose(boolean errorOwnedByCustomHandler) {
        if (closeFlushTimeoutMillis <= 0L) {
            return;
        }
        long published = cursorEngine.publishedFsn();
        // Never wait for uncommitted deferred frames: the server withholds
        // their acks by design (FLAG_DEFER_COMMIT rows are rolled back on
        // error/demote/disconnect and must stay replayable client-side), so a
        // drain targeting them can only time out. The drain target is the last
        // commit-bearing frame this session published, or -- for a ring
        // recovered from disk with no commit published this session -- the
        // recovered commit boundary.
        long boundary = Math.max(lastCommitBoundaryFsn, cursorEngine.recoveredCommitBoundaryFsn());
        long target = Math.min(published, boundary);
        if (target < published) {
            LOG.warn("close() abandoning {} uncommitted deferred frame(s) [commitBoundaryFsn={}, publishedFsn={}] "
                            + "-- their transaction was never committed; the server rolls their rows back. "
                            + "Call flush() before close() to commit, or ignore if the abort is intentional.",
                    published - target, target, published);
        }
        if (cursorEngine.ackedFsn() >= target) {
            return;
        }
        long deadlineNanos = System.nanoTime() + closeFlushTimeoutMillis * 1_000_000L;
        while (cursorEngine.ackedFsn() < target) {
            // Stop on a latched terminal (acks will never reach target);
            // surface it only when no other channel already delivered it.
            if (errorOwnedByCustomHandler) {
                if (cursorSendLoop.getTerminalError() != null) {
                    return;
                }
            } else {
                cursorSendLoop.checkError();
            }
            if (System.nanoTime() >= deadlineNanos) {
                long acked = cursorEngine.ackedFsn();
                LOG.warn("close() drain timed out after {}ms [target={} acked={}], pending data may be lost",
                        closeFlushTimeoutMillis, target, acked);
                throw new LineSenderException("close() drain timed out after ")
                        .put(closeFlushTimeoutMillis).put(" ms [targetFsn=")
                        .put(target).put(", ackedFsn=").put(acked)
                        .put("] - server did not acknowledge ")
                        .put(target - acked)
                        .put(" pending batches; data may be lost (use larger closeFlushTimeoutMillis or smaller batches)");
            }
            java.util.concurrent.locks.LockSupport.parkNanos(50_000L);
        }
    }

    /**
     * Ensures the active buffer is ready for writing (in FILLING state).
     * If the buffer is in RECYCLED state, resets it. If it's in use, waits for it.
     */
    private void ensureActiveBufferReady() {
        if (activeBuffer.isFilling()) {
            return; // Already ready
        }

        if (activeBuffer.isRecycled()) {
            // Buffer was recycled but not reset - reset it now
            activeBuffer.reset();
            return;
        }

        // Buffer is in use (SEALED or SENDING) - wait for it
        // Use a while loop to handle spurious wakeups and race conditions with the latch
        while (activeBuffer.isInUse()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Waiting for active buffer [id={}, state={}]", activeBuffer.getBatchId(), MicrobatchBuffer.stateName(activeBuffer.getState()));
            }
            boolean recycled = activeBuffer.awaitRecycled(30, TimeUnit.SECONDS);
            if (!recycled) {
                throw new LineSenderException("Timeout waiting for active buffer to be recycled");
            }
        }

        // Buffer should now be RECYCLED - reset it
        if (activeBuffer.isRecycled()) {
            activeBuffer.reset();
        }
    }

    private void ensureConnected() {
        checkNotClosed();
        if (connected) {
            return;
        }
        if (cursorEngine == null) {
            throw new LineSenderException("cursor engine must be attached before connect");
        }
        // Allocate the connection-event dispatcher BEFORE any buildAndConnect
        // attempt so fire points inside buildAndConnect (CONNECTED,
        // ENDPOINT_ATTEMPT_FAILED, AUTH_FAILED, ALL_ENDPOINTS_UNREACHABLE,
        // FAILED_OVER, RECONNECTED) always have a real sink. Dispatcher
        // dispatcher thread is lazy-started on the first offer().
        if (connectionDispatcher == null) {
            connectionDispatcher = new SenderConnectionDispatcher(
                    connectionListener, connectionListenerInboxCapacity);
        }
        CursorWebSocketSendLoop.ReconnectFactory reconnectFactory = newReconnectFactory();
        switch (initialConnectMode) {
            case SYNC:
                client = CursorWebSocketSendLoop.connectWithRetry(
                        reconnectFactory,
                        reconnectMaxDurationMillis,
                        reconnectInitialBackoffMillis,
                        reconnectMaxBackoffMillis,
                        "initial connect");
                break;
            case ASYNC:
                // Defer the actual connect to the I/O thread. The user thread
                // returns immediately; rows accumulate in the cursor SF engine.
                // Encoder stays at its default (V1 -- the only supported wire
                // version today). Frames written before the first successful
                // connect commit to V1 because cursor segments are immutable;
                // a future version bump must account for that. Auth/upgrade
                // rejects are surfaced via the error inbox by the I/O
                // thread, not thrown here; plain connect failures retry
                // indefinitely (Invariant B).
                client = null;
                break;
            case OFF:
            default:
                try {
                    client = reconnectFactory.reconnect();
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new LineSenderException(e).put("Failed to connect");
                }
                break;
        }

        try {
            cursorSendLoop = new CursorWebSocketSendLoop(
                    client, cursorEngine,
                    0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                    reconnectFactory,
                    reconnectMaxDurationMillis,
                    reconnectInitialBackoffMillis,
                    reconnectMaxBackoffMillis,
                    requestDurableAck,
                    durableAckKeepaliveIntervalMillis,
                    maxFrameRejections,
                    poisonMinEscalationWindowMillis);
            // Plug the async-delivery sink before start() so the I/O thread
            // never observes a null dispatcher between recordFatal and
            // notification — the test for null in dispatchError handles
            // even unconfigured paths, but starting wired is cleaner.
            if (errorDispatcher == null) {
                errorDispatcher = new SenderErrorDispatcher(errorHandler, errorInboxCapacity);
            }
            cursorSendLoop.setErrorDispatcher(errorDispatcher);
            // Symmetric progress dispatcher: lazy-allocated mirror of the
            // error path. Wired before start() for the same reason -- the
            // I/O thread should never observe a null dispatcher between
            // an ack arrival and the notify call.
            if (progressDispatcher == null) {
                progressDispatcher = new SenderProgressDispatcher(progressHandler, SenderProgressDispatcher.DEFAULT_CAPACITY);
            }
            cursorSendLoop.setProgressDispatcher(progressDispatcher);
            // Connection-event dispatcher: lets the cursor I/O loop fire
            // DISCONNECTED on outage entry. Sender-side fire points
            // (buildAndConnect) write directly to connectionDispatcher; this
            // getter just shares the same instance with the loop. (Invariant B:
            // the loop no longer fires a terminal budget-exhaustion event -- it
            // retries indefinitely.)
            cursorSendLoop.setConnectionDispatcher(connectionDispatcher);
            cursorSendLoop.start();
        } catch (Throwable t) {
            if (client != null) {
                client.close();
                client = null;
            }
            Endpoint ep = currentEndpoint();
            LineSenderException ex = new LineSenderException(t);
            ex.put("Failed to start cursor I/O thread for ");
            if (ep == null) {
                ex.put("<unbound>");
            } else {
                ex.put(ep.host).put(':').put(ep.port);
            }
            throw ex;
        }

        if (client != null) {
            Endpoint ep = currentEndpoint();
            String host = ep == null ? "<unbound>" : ep.host;
            int port = ep == null ? -1 : ep.port;
            encoder.setVersion((byte) client.getServerQwpVersion());
            // serverMaxBatchSize / effectiveAutoFlushBytes were already
            // refreshed by buildAndConnect just before it returned this
            // client; same path runs on every reconnect.
            LOG.info("Connected to WebSocket [host={}, port={}, qwpVersion={}, serverMaxBatchSize={}, effectiveAutoFlushBytes={}]",
                    host, port, client.getServerQwpVersion(), serverMaxBatchSize, effectiveAutoFlushBytes);
        } else {
            // Async mode: I/O thread will drive the connect. Encoder uses
            // its default version (V1). The symbol-dict watermark still gets
            // reset for consistency with the sync path; the post-connect replay
            // path does not need a producer-side reset signal because every
            // cursor frame is self-sufficient.
            Endpoint ep = endpoints.get(0);
            LOG.info("Async initial connect deferred to I/O thread [firstHost={}, firstPort={}, endpointCount={}]",
                    ep.host, ep.port, endpoints.size());
        }
        // Server starts fresh on each connection, so reset the symbol-dict
        // watermark. Cursor frames are self-sufficient (every frame carries its
        // full inline schema + a symbol-dict delta from id 0), so post-reconnect
        // replay needs no producer-side reset signal.
        resetSymbolDictStateForNewConnection();
        connectionError.set(null);

        connected = true;
    }

    private void ensureNoInProgressRow() {
        if (currentTableBuffer != null && currentTableBuffer.hasInProgressRow()) {
            throw new LineSenderException(
                    "Cannot flush while row is in progress. "
                            + "Use sender.at(), sender.atNow(), or sender.cancelRow() first."
            );
        }
    }

    /**
     * Flushes pending rows by encoding and sending them.
     * When all tables fit in a single message, the encoder produces one
     * WebSocket frame. When the encoded size exceeds {@code serverMaxBatchSize},
     * the method splits tables across multiple messages using
     * {@code FLAG_DEFER_COMMIT}: all but the last message carry the flag so
     * the server appends rows without committing, and the final message
     * triggers the commit.
     *
     * @param deferCommit when true, the message carries FLAG_DEFER_COMMIT
     *                    so the server appends rows but does not commit.
     *                    Used by auto-flush in transactional mode.
     */
    private void flushPendingRows(boolean deferCommit) {
        if (pendingRowCount <= 0) {
            return;
        }

        cachedTimestampColumn = null;
        cachedTimestampNanosColumn = null;

        ObjList<CharSequence> keys = tableBuffers.keys();
        int tableCount = countNonEmptyTables(keys);
        if (tableCount == 0) {
            pendingBytes = 0;
            currentTableBufferSnapshotBytes = currentTableBuffer == null
                    ? 0 : currentTableBuffer.getBufferedBytes();
            pendingRowCount = 0;
            firstPendingRowTimeNanos = 0;
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Flushing pending rows [count={}, tables={}, defer={}]", pendingRowCount, tableCount, deferCommit);
        }

        ensureActiveBufferReady();
        // Cursor SF requires every on-disk frame to be self-sufficient:
        // recorded frames replay to fresh server connections (orphan-slot
        // drainers and post-reconnect replay), so always emit the full
        // symbol-dict delta from id=0 and the full column schema inline,
        // never a back-reference the target server may not have seen.
        encoder.setDeferCommit(deferCommit);
        encoder.beginMessage(tableCount, globalSymbolDictionary,
                /*confirmedMaxId=*/ -1, currentBatchMaxSymbolId);
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer == null || tableBuffer.getRowCount() == 0) {
                continue;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Encoding table [name={}, rows={}, batchMaxId={}]",
                        tableName, tableBuffer.getRowCount(), currentBatchMaxSymbolId);
            }

            encoder.addTable(tableBuffer);
        }
        int messageSize = encoder.finishMessage();
        QwpBufferWriter buffer = encoder.getBuffer();

        if (serverMaxBatchSize > 0 && messageSize > serverMaxBatchSize) {
            flushPendingRowsSplit(keys, deferCommit);
            return;
        }

        activeBuffer.ensureCapacity(messageSize);
        activeBuffer.write(buffer.getBufferPtr(), messageSize);
        activeBuffer.incrementRowCount();
        sealAndSwapBuffer();

        hasDeferredMessages = deferCommit;
        if (!deferCommit) {
            lastCommitBoundaryFsn = cursorEngine.publishedFsn();
        }

        resetTableBuffersAfterFlush(keys);
    }

    /**
     * Splitting path: the full batch exceeds serverMaxBatchSize, so
     * flushPendingRows() delegates here. Each non-empty table gets its
     * own message. All messages except the last carry FLAG_DEFER_COMMIT
     * so the server appends rows without committing until the final
     * message arrives.
     *
     * @param deferCommit when true, ALL messages (including the last)
     *                    carry FLAG_DEFER_COMMIT. When false, only the
     *                    last message omits the flag.
     */
    private void flushPendingRowsSplit(ObjList<CharSequence> keys, boolean deferCommit) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Splitting flush across multiple messages [serverMaxBatchSize={}, defer={}]", serverMaxBatchSize, deferCommit);
        }

        // Collect non-empty table indices so we know which is last.
        int nonEmptyCount = 0;
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer != null && tableBuffer.getRowCount() > 0) {
                nonEmptyCount++;
            }
        }

        int sent = 0;
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer == null || tableBuffer.getRowCount() == 0) {
                continue;
            }

            sent++;
            boolean isLast = (sent == nonEmptyCount);
            boolean deferThis = deferCommit || !isLast;

            encoder.setDeferCommit(deferThis);
            encoder.beginMessage(1, globalSymbolDictionary,
                    /*confirmedMaxId=*/ -1, currentBatchMaxSymbolId);
            encoder.addTable(tableBuffer);
            int messageSize = encoder.finishMessage();
            QwpBufferWriter buffer = encoder.getBuffer();

            if (messageSize > serverMaxBatchSize) {
                resetTableBuffersAfterFlush(keys);
                throw new LineSenderException("single table batch too large for server batch cap")
                        .put(" [table=").put(tableName)
                        .put(", messageSize=").put(messageSize)
                        .put(", serverMaxBatchSize=").put(serverMaxBatchSize).put(']');
            }

            ensureActiveBufferReady();
            activeBuffer.ensureCapacity(messageSize);
            activeBuffer.write(buffer.getBufferPtr(), messageSize);
            activeBuffer.incrementRowCount();
            sealAndSwapBuffer();
        }

        encoder.setDeferCommit(false);
        hasDeferredMessages = deferCommit;
        if (!deferCommit) {
            // The last message of the split carried no defer flag -- it
            // committed the whole group.
            lastCommitBoundaryFsn = cursorEngine.publishedFsn();
        }
        resetTableBuffersAfterFlush(keys);
    }

    private void resetTableBuffersAfterFlush(ObjList<CharSequence> keys) {
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer == null || tableBuffer.getRowCount() == 0) {
                continue;
            }
            tableBuffer.reset();
        }
        currentBatchMaxSymbolId = -1;
        pendingBytes = 0;
        currentTableBufferSnapshotBytes = 0;
        pendingRowCount = 0;
        firstPendingRowTimeNanos = 0;
    }

    /**
     * Sends an empty QWP message without FLAG_DEFER_COMMIT to trigger
     * the server-side commit of all previously deferred rows.
     */
    private void sendCommitMessage() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending commit message for deferred batch");
        }
        encoder.setDeferCommit(false);
        encoder.beginMessage(0, globalSymbolDictionary,
                /*confirmedMaxId=*/ -1, currentBatchMaxSymbolId);
        int messageSize = encoder.finishMessage();
        QwpBufferWriter buffer = encoder.getBuffer();
        ensureActiveBufferReady();
        activeBuffer.ensureCapacity(messageSize);
        activeBuffer.write(buffer.getBufferPtr(), messageSize);
        activeBuffer.incrementRowCount();
        sealAndSwapBuffer();
        hasDeferredMessages = false;
        lastCommitBoundaryFsn = cursorEngine.publishedFsn();
    }

    private void resetSymbolDictStateForNewConnection() {
        // The new server has an empty symbol dictionary, so the next batch
        // must ship a delta starting at id 0. beginMessage() always passes
        // confirmedMaxId = -1; resetting the batch watermark here keeps a
        // stale value from suppressing re-emission of symbol ids the new
        // server has never seen.
        currentBatchMaxSymbolId = -1;
    }

    private void rollbackRow() {
        if (currentTableBuffer != null) {
            currentTableBuffer.cancelCurrentRow();
            currentTableBuffer.rollbackUncommittedColumns();
        }
    }

    /**
     * Seals the current buffer and swaps to the other buffer.
     * Enqueues the sealed buffer for async sending.
     */
    private void sealAndSwapBuffer() {
        if (!activeBuffer.hasData()) {
            return; // Nothing to send
        }

        MicrobatchBuffer toSend = activeBuffer;
        toSend.seal();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sealing buffer [id={}, rows={}, bytes={}]", toSend.getBatchId(), toSend.getRowCount(), toSend.getBufferPos());
        }

        // Swap to the other buffer
        activeBuffer = (activeBuffer == buffer0) ? buffer1 : buffer0;

        // If the other buffer is still being sent, wait for it
        // Use a while loop to handle spurious wakeups and race conditions with the latch
        while (activeBuffer.isInUse()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Waiting for buffer recycle [id={}, state={}]", activeBuffer.getBatchId(), MicrobatchBuffer.stateName(activeBuffer.getState()));
            }
            boolean recycled = activeBuffer.awaitRecycled(30, TimeUnit.SECONDS);
            if (!recycled) {
                throw new LineSenderException("Timeout waiting for buffer to be recycled");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Buffer recycled [id={}, state={}]", activeBuffer.getBatchId(), MicrobatchBuffer.stateName(activeBuffer.getState()));
            }
        }

        // Reset the new active buffer
        int stateBeforeReset = activeBuffer.getState();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Resetting buffer [id={}, state={}]", activeBuffer.getBatchId(), MicrobatchBuffer.stateName(stateBeforeReset));
        }
        activeBuffer.reset();

        // Hand off the sealed buffer to the cursor engine on the user thread
        // (durable mmap append, returns once published).
        try {
            toSend.markSending();
            cursorEngine.appendBlocking(toSend.getBufferPtr(), toSend.getBufferPos());
            toSend.markRecycled();
        } catch (Throwable t) {
            // appendBlocking failed synchronously on the user thread — the
            // payload never reached the engine, so no I/O thread will
            // recycle toSend. Recycle it here so a later flush can swap
            // back to it; flushPendingRows aborts its post-enqueue state
            // updates after this throw, so the source rows stay intact and the
            // next batch re-emits the same rows along with the full inline
            // schema and symbol-dict delta from id 0.
            if (toSend.isSending()) {
                toSend.markRecycled();
            } else if (toSend.isSealed()) {
                toSend.rollbackSealForRetry();
            }
            // Surface any I/O thread error first — appendBlocking itself only
            // throws on PAYLOAD_TOO_LARGE / backpressure deadline, but the
            // I/O loop can have failed independently.
            cursorSendLoop.checkError();
            throw new LineSenderException("cursor SF append failed", t);
        }
    }

    /**
     * Accumulates the current row.
     * Rows buffer until flush (explicit or auto-flush).
     */
    private void sendRow() {
        ensureConnected();

        // Hard guard: if THIS row's bytes already exceed the server's wire
        // cap, the flush would produce an oversize WS frame the server
        // closes with ws-close[1009]. Check the per-row delta before
        // nextRow() commits the row, so the at()/atNow() error path can
        // roll back via rollbackRow() and prior committed rows in the
        // batch stay intact. (The check ignores the null-padding bytes
        // nextRow() will add; that's bounded by numColumns * elemSize and
        // far below any realistic cap.)
        if (serverMaxBatchSize > 0) {
            long rowBytes = currentTableBuffer.getBufferedBytes() - currentTableBufferSnapshotBytes;
            if (rowBytes > serverMaxBatchSize) {
                throw new LineSenderException("row too large for server batch cap")
                        .put(" [rowBytes=").put(rowBytes)
                        .put(", serverMaxBatchSize=").put(serverMaxBatchSize).put(']');
            }
        }

        currentTableBuffer.nextRow();

        if (pendingRowCount == 0) {
            firstPendingRowTimeNanos = System.nanoTime();
        }
        pendingRowCount++;

        // Advance pendingBytes by the bytes the just-committed row added to
        // the current table, then re-snap. Column setters and nextRow()
        // only ever touch currentTableBuffer between consistency points, so
        // the per-row work stays O(numColumns of the current table) -- no
        // map walk, no scaling with the number of tables this sender has
        // seen across its lifetime.
        long bufferedNow = currentTableBuffer.getBufferedBytes();
        pendingBytes += bufferedNow - currentTableBufferSnapshotBytes;
        currentTableBufferSnapshotBytes = bufferedNow;

        if (shouldAutoFlush()) {
            flushPendingRows(transactional);
        }
    }

    /**
     * Checks if any auto-flush threshold is exceeded.
     */
    private boolean shouldAutoFlush() {
        if (pendingRowCount <= 0) {
            return false;
        }
        if (autoFlushRows > 0 && pendingRowCount >= autoFlushRows) {
            return true;
        }
        if (effectiveAutoFlushBytes > 0 && getPendingBytes() >= effectiveAutoFlushBytes) {
            return true;
        }
        if (autoFlushIntervalNanos > 0) {
            long ageNanos = System.nanoTime() - firstPendingRowTimeNanos;
            return ageNanos >= autoFlushIntervalNanos;
        }
        return false;
    }

    private long toMicros(long value, ChronoUnit unit) {
        switch (unit) {
            case NANOS:
                return value / 1000L;
            case MICROS:
                return value;
            case MILLIS:
                return value * 1000L;
            case SECONDS:
                return value * 1_000_000L;
            case MINUTES:
                return value * 60_000_000L;
            case HOURS:
                return value * 3_600_000_000L;
            case DAYS:
                return value * 86_400_000_000L;
            default:
                throw new LineSenderException("Unsupported time unit: " + unit);
        }
    }

    /**
     * Total buffered bytes across every per-table column buffer. Sums the
     * tableBuffers map with the same null-tolerant walk the old sendRow path
     * used. Currently dead in production code (the sendRow accounting was
     * inlined for tighter bookkeeping) but kept so
     * {@code QwpWebSocketSenderTest} can exercise the walk shape directly
     * and {@code QwpTotalBufferedBytesBenchmark} can quote it as the
     * baseline it benchmarks. Removing it would break both.
     */
    @TestOnly
    public long totalBufferedBytes() {
        long total = 0;
        ObjList<CharSequence> keys = tableBuffers.keys();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            QwpTableBuffer tb = tableBuffers.get(tableName);
            if (tb != null) {
                total += tb.getBufferedBytes();
            }
        }
        return total;
    }

    private void validateTableName(CharSequence name) {
        if (name == null || !TableUtils.isValidTableName(name, MAX_TABLE_NAME_LENGTH)) {
            if (name == null || name.length() == 0) {
                throw new LineSenderException("table name cannot be empty");
            }
            if (name.length() > MAX_TABLE_NAME_LENGTH) {
                throw new LineSenderException("table name too long [maxLength=" + MAX_TABLE_NAME_LENGTH + "]");
            }
            throw new LineSenderException("table name contains illegal characters: " + name);
        }
    }

    public static final class Endpoint {
        public final String host;
        public final int port;

        public Endpoint(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }

    private final class ReconnectSupplier implements CursorWebSocketSendLoop.ReconnectFactory {
        /**
         * Optional caller-owned liveness gate. {@code null} means this factory
         * serves the foreground sender and aborts when the foreground I/O loop
         * stops. Non-null means the factory serves a {@code BackgroundDrainer}:
         * the drainer must be able to (re)connect during the sender's close
         * sequence (the drainer pool's graceful-drain window runs AFTER the
         * foreground loop is stopped), so its gate is the drainer's own stop
         * flag, supplied here, instead of the foreground loop's state.
         */
        private final java.util.function.BooleanSupplier abortCheck;
        private final String abortMessage;
        private int previousIdx = -1;

        private ReconnectSupplier() {
            this(null, null);
        }

        private ReconnectSupplier(java.util.function.BooleanSupplier abortCheck, String abortMessage) {
            this.abortCheck = abortCheck;
            this.abortMessage = abortMessage;
        }

        String abortMessage() {
            return abortCheck != null ? abortMessage : "sender closed during connect";
        }

        /**
         * True when this factory serves a background drainer. Background
         * connects share buildAndConnect's endpoint walk and hostTracker
         * health state, but commit none of the foreground sender's
         * observable connection state and fire no connection events.
         */
        boolean isBackground() {
            return abortCheck != null;
        }

        boolean isAborted() {
            return abortCheck != null
                    ? abortCheck.getAsBoolean()
                    : (cursorSendLoop == null ? closed : !cursorSendLoop.isRunning());
        }

        @Override
        public WebSocketClient reconnect() {
            return buildAndConnect(this);
        }
    }
}
