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
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerListener;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerPool;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.cutlass.qwp.client.sf.cursor.DefaultSenderConnectionListener;
import io.questdb.client.cutlass.qwp.client.sf.cursor.DefaultSenderErrorHandler;
import io.questdb.client.cutlass.qwp.client.sf.cursor.DefaultSenderProgressHandler;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.PersistedSymbolDict;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderConnectionDispatcher;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderErrorDispatcher;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SenderProgressDispatcher;
import io.questdb.client.cutlass.qwp.client.sf.cursor.UnreplayableSlotException;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.CharSequenceObjHashMap;
import io.questdb.client.std.Chars;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.IntList;
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
    // Not final: seedGlobalDictionaryFromPersisted replaces it with a pre-sized instance
    // before anything can observe the original. Nothing retains a reference -- the encoder
    // and the persisted dictionary both take it as a per-call parameter -- and the swap
    // happens during construction, before the producer or the I/O thread exist.
    private GlobalSymbolDictionary globalSymbolDictionary;
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
    // Per-table encoded body byte counts captured during flushPendingRows' combined
    // encode. flushPendingRowsSplit uses them both for preflight sizing and to walk
    // the staged body slices without encoding the batch a second time. Cleared and
    // repopulated on every flush; reused to stay zero-GC.
    // The non-empty tables of the batch currently being flushed, collected ONCE per
    // flush and then iterated by every pass. Each pass used to re-walk tableBuffers.keys()
    // and re-probe the hash map per table -- 3 probes per table on a plain flush and 5 on
    // a split, on the producer's thread. Reused across flushes to stay zero-GC. Held in
    // lockstep so index i names the same table in both, and in the same order as
    // splitFrameBodyBytes, which is what lets the split passes index them directly.
    private final ObjList<QwpTableBuffer> flushTableBuffers = new ObjList<>();
    private final ObjList<CharSequence> flushTableNames = new ObjList<>();
    private final IntList splitFrameBodyBytes = new IntList();
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
    // Test-only lifecycle witness. drainOnClose() invokes and clears it after
    // confirming a real unacknowledged close target, immediately before waiting.
    private volatile Runnable closeDrainWaitingHook;
    // close() drain timeout in millis. Default applied at construction.
    // 0 or -1 means "fast close" (skip the drain); otherwise close blocks
    // up to this many millis for ackedFsn to catch up to publishedFsn.
    private volatile boolean closeCleanupComplete;
    private boolean closeCleanupStarted;
    private long closeFlushTimeoutMillis = 5_000L;
    private volatile boolean closed;
    // Test-only lifecycle witness. close() invokes and clears it strictly after
    // publishing closed=true and before starting any drain or teardown work.
    private volatile Runnable closeStartedHook;
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
    // True when the sender emits incremental (delta) symbol dictionaries: each
    // message carries only symbol ids not yet sent on the wire, rather than the
    // full dictionary from id 0. Enabled in memory-mode (a reconnect replays from
    // the in-process ring) and in file-mode store-and-forward when the per-slot
    // persisted dictionary opened. In both, the I/O thread re-registers the whole
    // dictionary via a catch-up frame before replaying, so a non-self-sufficient
    // delta frame never dangles an id on a fresh server. Falls back to full
    // self-sufficient frames only when the persisted dictionary is unavailable in
    // file-mode (recovery/orphan-drain would then have nothing to rebuild the
    // deltas from). Set in setCursorEngine.
    private boolean deltaDictEnabled;
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
    private volatile SenderErrorDispatcher errorDispatcher;
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
    //         (Invariant B); endpoint-policy and transport failures stay
    //         contained in that loop and never reach the producer.
    private Sender.InitialConnectMode initialConnectMode = Sender.InitialConnectMode.OFF;
    private boolean ownsCursorEngine;
    // Whether close() may let the engine reclaim the parent-anchored LOGICAL slot lock.
    // False only while an outer frame holds it: Sender.build() acquires it for the whole
    // construct -> connect -> quarantine transition, and connect()'s own rollback closes
    // this sender from INSIDE that scope. A fresh slot is "fully drained" by definition
    // (publishedFsn < 0), so the default close(true) took the reclaim branch and unlinked
    // the very lock file build() was holding -- on POSIX that frees the pathname without
    // releasing the flock, so the next acquireLogical creates a SECOND inode and locks it.
    // build()'s own careful close(false) calls could not prevent it, because connect()
    // closed the engine first. NEVER reset back to true: the only writer is connect()'s
    // rollback, which always rethrows, so the sender carrying false is always discarded
    // and one that reaches the user still has true and retires the lock normally on a
    // later close(). A future path that clears this WITHOUT rethrowing must restore it,
    // or that sender's logical lock is never reclaimed.
    private boolean reclaimLogicalSlotLockOnClose = true;
    private long pendingBytes;
    // Set true by close() once the SF slot flock has been released (the normal
    // teardown path). Stays false if an I/O or manager worker did not stop and
    // cursorEngine retained the flock, so the owning pool MUST keep the slot
    // reserved rather than hand the still-locked dir to the next borrow
    // ("sf slot already in use"). May flip to true LATER via the getter's
    // re-probe of retainedEngine, once the deferred cleanup (manager-worker
    // exit path or delegated I/O-thread close) releases the flock — pools
    // re-probe retired slots to recover their capacity. volatile: written on
    // the closing thread, read by pool threads.
    private volatile boolean slotLockReleased;
    // Optional owning-pool notification, relayed after the engine confirms the
    // flock release and slotLockReleased is visible to pool re-probes.
    private volatile Runnable slotLockReleaseListener;
    // Engine whose close() could not complete during sender close() — its
    // cleanup is pending on a worker/I/O-thread exit path. isSlotLockReleased()
    // re-probes it so a late flock release becomes visible to the owning pool.
    // Only ever set inside close(); null for a sender that closed cleanly.
    private volatile CursorSendEngine retainedEngine;
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
    // Minimum wall-clock dwell a symbol-dict catch-up cap gap must persist before an
    // orphan drainer may quarantine its slot (connect-string key
    // catch_up_cap_gap_min_escalation_window_millis). Foreground senders retry forever. See
    // CursorWebSocketSendLoop.DEFAULT_CATCHUP_CAP_GAP_MIN_ESCALATION_WINDOW_MILLIS.
    private long catchUpCapGapMinEscalationWindowMillis =
            CursorWebSocketSendLoop.DEFAULT_CATCHUP_CAP_GAP_MIN_ESCALATION_WINDOW_MILLIS;
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
    // Highest global symbol id the producer has baked into a frame so far, or -1.
    // Lifetime-monotonic in delta mode -- it is NOT reset on reconnect, because
    // the I/O thread re-registers the full dictionary via a catch-up frame before
    // replaying, so the producer's delta baseline stays valid across the wire
    // boundary. Used only when deltaDictEnabled; ignored in full-dict mode.
    private int sentMaxSymbolId = -1;
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
                CursorWebSocketSendLoop.DEFAULT_POISON_MIN_ESCALATION_WINDOW_MILLIS,
                CursorWebSocketSendLoop.DEFAULT_CATCHUP_CAP_GAP_MIN_ESCALATION_WINDOW_MILLIS);
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
            long poisonMinEscalationWindowMillis,
            long catchUpCapGapMinEscalationWindowMillis
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
            sender.catchUpCapGapMinEscalationWindowMillis = catchUpCapGapMinEscalationWindowMillis;
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
            // Preserve t's IDENTITY through the rollback. Sender.build() routes on the
            // exception type -- only UnreplayableSlotException reaches its quarantine
            // handler -- and close() accumulates cleanup errors and ends in
            // rethrowTerminal, so letting a close failure propagate here would REPLACE t
            // and silently demote a recoverable slot back to the permanent build() brick.
            // This rollback always runs inside Sender.build()'s acquireLogical scope, so
            // the logical slot lock is held one frame up. Closing the engine with the
            // default reclaim would unlink the lock file build() is still holding.
            sender.reclaimLogicalSlotLockOnClose = false;
            try {
                sender.close();
            } catch (Throwable closeFailure) {
                t.addSuppressed(closeFailure);
            }
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
        cursorEngine.checkDurability();
        // Surface latched errors before any early-return path, so a caller
        // polling with timeoutMillis <= 0 to drive their own loop sees the
        // throw instead of an indefinite "not yet". The durability latch
        // above is transient: it throws while latched, and clears once a
        // later periodic sync pass fully succeeds so producers can resume.
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
            cursorEngine.checkDurability();
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
     *       on its own;</li>
     *   <li>SF manager stop: normally immediate, bounded by 5s when its worker
     *       is stuck in a filesystem operation. On timeout the slot remains
     *       locked rather than being exposed to a stale worker.</li>
     * </ul>
     */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            Runnable hook = closeStartedHook;
            closeStartedHook = null;
            if (hook != null) {
                try {
                    hook.run();
                } catch (Throwable t) {
                    // A test witness must never prevent production resource cleanup.
                    LOG.error("Error in close-started test hook: {}", String.valueOf(t));
                }
            }
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
                    //    A pre-flight rejection means this batch cannot fit
                    //    the current cap however it is split. It is
                    //    RETAINED by design so it can go out once a
                    //    larger-cap node is reached -- but on close there is
                    //    no later flush, and letting the throw escape here
                    //    skips sendCommitMessage, sealAndSwapBuffer and
                    //    drainOnClose, abandoning every row an earlier
                    //    successful flush already published. The message
                    //    that path emits tells the caller to close the
                    //    sender to discard the batch, so honour that:
                    //    discard it, remember the error, and let the rest of
                    //    close() run. rethrowTerminal below still surfaces it.
                    try {
                        flushPendingRows(deferCommit);
                    } catch (BatchTooLargeForCapException e) {
                        resetTableBuffersAfterFlush();
                        terminalError = captureCloseError(terminalError, e);
                    }
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
                // The worker may still touch every resource below. Hand the
                // complete sender-owned tail to its exit path rather than
                // permanently leaking everything except the engine. The
                // callback is idempotence-gated by closeRemainingResources().
                if (ownsCursorEngine && cursorEngine != null) {
                    retainedEngine = cursorEngine;
                }
                Runnable closeCallback = () -> closeRemainingResources(null);
                if (cursorSendLoop != null && cursorSendLoop.delegateClose(closeCallback)) {
                    rethrowTerminal(terminalError);
                    return;
                }
                // The worker exited between close() failing and delegation.
                // Cleanup is safe here and its failures remain suppressed on
                // the original close error.
                terminalError = closeRemainingResources(terminalError);
            } else {
                terminalError = closeRemainingResources(terminalError);
            }

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

    @TestOnly
    public boolean isCloseCleanupComplete() {
        return closeCleanupComplete;
    }

    /**
     * True once the store-and-forward slot flock has been released. False
     * means an I/O or manager worker did not stop and close() retained the
     * lock and worker-reachable resources; the owning pool must keep the slot
     * index reserved instead of reusing the still-locked dir.
     * <p>
     * Not a one-shot snapshot: when close() left engine cleanup pending on a
     * manager-worker quiescence or I/O-thread exit path, this re-probes the
     * retained engine and latches true the moment that cleanup completes — pools re-probe retired
     * slots through this getter to recover their capacity. Monotonic:
     * false→true only, never back. Cheap (volatile reads on every common
     * path) so pools may call it under their capacity lock; only the rare
     * orphaned-retry state below does more.
     * <p>
     * The probe is also the recovery surface for a retained engine whose
     * flock-release retry fell off the shared driver because the driver
     * thread could not start (e.g. OOM at thread creation): close() is
     * one-shot, so without the re-arm below that slot's capacity would stay
     * lost until process exit.
     */
    public boolean isSlotLockReleased() {
        if (slotLockReleased) {
            return true;
        }
        CursorSendEngine engine = retainedEngine;
        if (engine != null) {
            if (engine.isCloseCompleted()) {
                // Benign latch race: concurrent callers may both observe the
                // completed cleanup and both write true.
                slotLockReleased = true;
                return true;
            }
            engine.ensureFlockReleaseRetryScheduled();
        }
        return false;
    }

    /**
     * Registers a callback for confirmed SF slot-lock release. Pools use this
     * to wake borrowers without waiting for a timeout or housekeeper tick.
     */
    public void setSlotLockReleaseListener(Runnable listener) {
        slotLockReleaseListener = listener;
        if (listener != null && slotLockReleased) {
            listener.run();
        }
    }

    @TestOnly
    public void setSlotLockReleasedForTesting(boolean isReleased) {
        slotLockReleased = isReleased;
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
        if (cursorEngine != null) {
            cursorEngine.checkDurability();
        }
        ensureNoInProgressRow();
        ensureConnected();

        long beforeFsn = cursorEngine != null ? cursorEngine.publishedFsn() : -1L;

        // Cursor SF: append happens on the user thread inside
        // sealAndSwapBuffer, so by the time we reach here every encoded batch
        // is published in its mmap'd segment. PERIODIC stable-storage barriers
        // run independently in the manager. No processingCount to drain and no
        // awaitPendingAcks here; just surface any I/O thread error.
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

    @TestOnly
    public SenderConnectionDispatcher getConnectionDispatcherForTesting() {
        return connectionDispatcher;
    }

    @TestOnly
    public CursorSendEngine getCursorEngineForTesting() {
        return cursorEngine;
    }

    /**
     * Background orphan-drainer pool, or {@code null} when
     * {@code drain_orphans} is off or no orphan slot was adopted.
     */
    @TestOnly
    public BackgroundDrainerPool getDrainerPoolForTesting() {
        return drainerPool;
    }

    @TestOnly
    public SenderErrorDispatcher getErrorDispatcherForTesting() {
        return errorDispatcher;
    }

    @TestOnly
    public Sender.InitialConnectMode getInitialConnectModeForTesting() {
        return initialConnectMode;
    }

    @TestOnly
    public SenderProgressDispatcher getProgressDispatcherForTesting() {
        return progressDispatcher;
    }

    @TestOnly
    public Runnable getSlotLockReleaseListenerForTesting() {
        return slotLockReleaseListener;
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
     * Live view of the producer's global symbol dictionary, so tests can drive
     * the dictionary to the protocol cap without pushing a million rows through
     * the row API. Producer-thread only, like every dictionary access.
     */
    @TestOnly
    public GlobalSymbolDictionary getGlobalSymbolDictionaryForTest() {
        return globalSymbolDictionary;
    }

    /**
     * Snapshot of the producer's symbol prefix whose persisted-dictionary chunks
     * have committed. The persisted size advances only after the chunk CRC and
     * payload have been written, so this observes the write-ahead boundary without
     * reopening the live mmap-backed dictionary (which would attempt recovery-tail
     * truncation and is not supported while the producer owns the file on Windows).
     * Returns {@code null} in memory mode or when the persisted dictionary is
     * unavailable.
     */
    @TestOnly
    public ObjList<String> getPersistedSymbolsForTest() {
        CursorSendEngine engine = cursorEngine;
        if (engine == null) {
            return null;
        }
        PersistedSymbolDict persisted = engine.getPersistedSymbolDict();
        if (persisted == null) {
            return null;
        }
        int persistedSize = persisted.size();
        int globalSize = globalSymbolDictionary.size();
        if (persistedSize > globalSize) {
            throw new IllegalStateException("persisted symbol dictionary exceeds producer dictionary"
                    + " [persisted=" + persistedSize + ", producer=" + globalSize + ']');
        }
        ObjList<String> snapshot = new ObjList<>(persistedSize);
        for (int i = 0; i < persistedSize; i++) {
            snapshot.add(globalSymbolDictionary.getSymbol(i));
        }
        return snapshot;
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
     * Whether this sender is still in delta-encoded mode. Flips to {@code false}
     * permanently once {@link #disableDeltaDict} fires (a persisted-dictionary
     * write failure, including a recognised mmap access fault) -- every later
     * flush then ships full self-sufficient frames instead.
     */
    @TestOnly
    public boolean isDeltaDictEnabledForTest() {
        return deltaDictEnabled;
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

    @TestOnly
    public void setClientForTesting(WebSocketClient client) {
        this.client = client;
    }

    @TestOnly
    public void setClosedForTesting(boolean isClosed) {
        this.closed = isClosed;
    }

    /**
     * Installs a one-shot test witness that close-time drain invokes after it
     * observes a real unacknowledged target and before it starts waiting.
     * Production code never sets it.
     */
    @TestOnly
    public void setCloseDrainWaitingHook(Runnable hook) {
        this.closeDrainWaitingHook = hook;
    }

    /**
     * Installs a one-shot test witness that {@link #close()} invokes after it
     * publishes the closed-state transition and before it starts drain or
     * teardown work. Production code never sets it.
     */
    @TestOnly
    public void setCloseStartedHook(Runnable hook) {
        this.closeStartedHook = hook;
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
        // Drop the batch's symbol watermark along with the rows that raised it. A
        // later flush encodes the delta section as
        // [sentMaxSymbolId+1 .. currentBatchMaxSymbolId], so a watermark left behind
        // by the discarded batch keeps re-encoding that batch's symbols: a single-row
        // batch after reset() would still carry the whole abandoned range and hit the
        // very cap rejection reset() is documented to clear, leaving the sender unable
        // to flush anything at all. -1 is the same value resetTableBuffersAfterFlush
        // leaves behind after a successful flush, and sendCommitMessage already reads
        // it as an empty delta.
        currentBatchMaxSymbolId = -1;
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
     * before the first send. Once a non-null engine has been attached, it
     * cannot be replaced or detached. Ownership of a rejected engine remains
     * with the caller.
     */
    public void setCursorEngine(CursorSendEngine engine, boolean takeOwnership) {
        if (closed) {
            throw new LineSenderException("Sender is closed");
        }
        if (connected) {
            throw new LineSenderException(
                    "setCursorEngine must be called before the first send");
        }
        if (cursorEngine != null) {
            throw new LineSenderException("CursorSendEngine is already attached");
        }
        this.cursorEngine = engine;
        this.ownsCursorEngine = takeOwnership && engine != null;
        // Delta encoding is available in memory-mode (in-process catch-up) and in
        // file-mode when the persisted dictionary opened (recovery / orphan-drain
        // rebuild the dictionary from it). Otherwise fall back to full self-
        // sufficient frames. See CursorSendEngine.isDeltaDictEnabled.
        this.deltaDictEnabled = engine != null && engine.isDeltaDictEnabled();
        // Recovery: repopulate the producer's global dictionary from the slot's
        // persisted dictionary so newly ingested symbols continue from the
        // recovered ids (rather than colliding with them at 0), and the delta
        // baseline resumes where the crashed session left off.
        // NOT gated on deltaDictEnabled. That flag is false exactly when the slot's dictionary
        // failed to open -- which is precisely when the frames on disk are the only surviving
        // copy of the symbols and the rebuild matters most. Gating the seed on it made the
        // rebuild dead code for the very case it was written for.
        if (engine != null && engine.wasRecoveredFromDisk()) {
            seedGlobalDictionaryFromPersisted(engine.getPersistedSymbolDict());
        }
        if (engine != null) {
            engine.setSlotLockReleaseListener(this::onSlotLockReleased);
        }
    }

    @TestOnly
    public void setCursorSendLoopForTesting(CursorWebSocketSendLoop loop) {
        cursorSendLoop = loop;
        if (connectionDispatcher == null) {
            connectionDispatcher = new SenderConnectionDispatcher(
                    connectionListener, connectionListenerInboxCapacity);
        }
        if (errorDispatcher == null) {
            errorDispatcher = new SenderErrorDispatcher(errorHandler, errorInboxCapacity);
        }
        if (progressDispatcher == null) {
            progressDispatcher = new SenderProgressDispatcher(
                    progressHandler, SenderProgressDispatcher.DEFAULT_CAPACITY);
        }
        loop.setConnectionDispatcher(connectionDispatcher);
        loop.setErrorDispatcher(errorDispatcher);
        loop.setProgressDispatcher(progressDispatcher);
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
            ObjList<BackgroundDrainer> live =
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
        startOrphanDrainers(
                orphanSlotPaths,
                maxBackgroundDrainers,
                segmentSizeBytes,
                sfMaxTotalBytes,
                0L);
    }

    /**
     * Starts orphan drainers while preserving the foreground sender's periodic
     * store-and-forward checkpoint interval.
     */
    public synchronized void startOrphanDrainers(
            io.questdb.client.std.ObjList<String> orphanSlotPaths,
            int maxBackgroundDrainers,
            long segmentSizeBytes,
            long sfMaxTotalBytes,
            long syncIntervalNanos
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
            // Route drainer data-loss reports through the sender's own error
            // dispatcher: async, bounded, and contained exactly like every
            // other SenderError. The dispatcher field is read lazily because
            // it is created on connect, which can complete after this pool is
            // built; a null dispatcher (never connected) leaves the site's own
            // LOG line as the only announcement, same as before this sink.
            drainerPool.setErrorSink(err -> {
                SenderErrorDispatcher d = errorDispatcher;
                if (d != null) {
                    d.offer(err);
                }
            });
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
            final BackgroundDrainer[] ref =
                    new BackgroundDrainer[1];
            ReconnectSupplier factory = new ReconnectSupplier(
                    () -> {
                        BackgroundDrainer d = ref[0];
                        return d != null && d.isStopRequested();
                    },
                    "drainer stop requested during connect");
            BackgroundDrainer drainer =
                    new BackgroundDrainer(
                            slot, segmentSizeBytes, sfMaxTotalBytes,
                            syncIntervalNanos,
                            factory,
                            reconnectMaxDurationMillis,
                            reconnectInitialBackoffMillis,
                            reconnectMaxBackoffMillis,
                            requestDurableAck,
                            durableAckKeepaliveIntervalMillis,
                            maxFrameRejections,
                            poisonMinEscalationWindowMillis,
                            catchUpCapGapMinEscalationWindowMillis);
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
    private WebSocketClient buildAndConnect(ReconnectSupplier ctx, CursorWebSocketSendLoop.ConnectCancellation cancellation) {
        if (ctx.isBackground()) {
            // Lock-free: the walk below touches only internally-synchronized
            // hostTracker health state and walk-local/cursor-local state on
            // the background path.
            return connectWalk(ctx, cancellation);
        }
        connectWalkLock.lock();
        try {
            return connectWalk(ctx, cancellation);
        } finally {
            connectWalkLock.unlock();
        }
    }

    // Drop the in-flight connect handle once the walk has disposed a client on
    // a connect/upgrade FAILURE, so inFlight never dangles at a disposed client
    // across the inter-attempt backoff. Without this a concurrent
    // close()->cancel() could closeTraffic() a client the walk no longer owns;
    // proven a no-op on both production transports today (closed-fd closeTraffic
    // no-ops), but a future custom transport that threw on a closed socket would
    // spuriously loud-fail close(). Null-guarded: the no-arg reconnect() path
    // and the Unsafe.allocateInstance bare-loop tests pass a null handle. Pairs
    // with the success-path clear() after upgrade().
    private static void clearInFlight(CursorWebSocketSendLoop.ConnectCancellation cancellation) {
        if (cancellation != null) {
            cancellation.clear();
        }
    }

    private WebSocketClient connectWalk(ReconnectSupplier ctx, CursorWebSocketSendLoop.ConnectCancellation cancellation) {
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
                if (cancellation != null) {
                    // Publish the client we are about to block on so a
                    // concurrent CursorWebSocketSendLoop.close() can break its
                    // traffic and unwind a black-holed native connect
                    // (connect_timeout=0 => OS SYN-retry) instead of hanging on
                    // the untimed shutdown-latch await. The publish-then-check
                    // handshake pairs with ConnectCancellation.cancel(): if we
                    // observe cancellation here we skip the blocking connect
                    // entirely; otherwise cancel() observed this client and
                    // breaks it. The walk's per-attempt catch disposes the
                    // client and, since running has flipped false, the
                    // top-of-loop ctx.isAborted() gate ends the walk.
                    cancellation.publish(newClient);
                    if (cancellation.isCancelled()) {
                        throw new LineSenderException(ctx.abortMessage());
                    }
                }
                newClient.connect(ep.host, ep.port);
                int upgradeTimeoutMs = (int) Math.min(authTimeoutMs, Integer.MAX_VALUE);
                newClient.upgrade(WRITE_PATH, upgradeTimeoutMs, authorizationHeader);
                if (cancellation != null) {
                    // connect()+upgrade() completed: this client is no longer
                    // blocking, so drop it from the in-flight handle before it
                    // becomes the loop's `client` field. close() must then break
                    // its traffic via the field path exactly once -- leaving it
                    // in the handle too would double-shut-down the socket.
                    cancellation.clear();
                }
            } catch (HttpClientException e) {
                // Close BEFORE classify: the sibling catch (Error) below does not
                // guard catch-arm bodies, so an Error thrown inside classify()
                // (it allocates on the role-reject/auth paths) would escape with
                // the client's fd and native buffers open. Safe to reorder --
                // classify reads only heap fields (upgradeRejectRole/Zone,
                // upgradeStatusCode) that are set during upgrade() and survive
                // close().
                newClient.close();
                clearInFlight(cancellation);
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
                clearInFlight(cancellation);
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
                clearInFlight(cancellation);
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

    private synchronized Throwable closeRemainingResources(Throwable terminalError) {
        if (closeCleanupStarted) {
            return terminalError;
        }
        closeCleanupStarted = true;
        try {
            try {
                buffer0.close();
            } catch (Throwable t) {
                LOG.error("Error closing buffer0: {}", String.valueOf(t));
                terminalError = captureCloseError(terminalError, t);
            }
            try {
                buffer1.close();
            } catch (Throwable t) {
                LOG.error("Error closing buffer1: {}", String.valueOf(t));
                terminalError = captureCloseError(terminalError, t);
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
                CursorSendEngine engine = cursorEngine;
                try {
                    // reclaimLogicalSlotLockOnClose is false when a caller above us
                    // (Sender.build()) still HOLDS the parent-anchored logical lock:
                    // unlinking it here would free the pathname without releasing the
                    // flock, letting the next acquireLogical create a second inode.
                    engine.close(reclaimLogicalSlotLockOnClose);
                } catch (Throwable t) {
                    LOG.error("Error closing owned CursorSendEngine: {}", String.valueOf(t));
                    terminalError = captureCloseError(terminalError, t);
                }
                if (engine.isCloseCompleted()) {
                    cursorEngine = null;
                    ownsCursorEngine = false;
                    slotLockReleased = true;
                } else {
                    slotLockReleased = false;
                    retainedEngine = engine;
                }
            } else {
                slotLockReleased = true;
            }
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
            return terminalError;
        } finally {
            closeCleanupComplete = true;
        }
    }

    /**
     * Collects the batch's non-empty tables into {@link #flushTableNames} /
     * {@link #flushTableBuffers} and returns how many there are. One walk of the key
     * list and one hash probe per table, for the whole flush.
     */
    private int collectNonEmptyTables(ObjList<CharSequence> keys) {
        flushTableNames.clear();
        flushTableBuffers.clear();
        for (int i = 0, n = keys.size(); i < n; i++) {
            CharSequence tableName = keys.getQuick(i);
            if (tableName == null) {
                continue;
            }
            QwpTableBuffer tableBuffer = tableBuffers.get(tableName);
            if (tableBuffer != null && tableBuffer.getRowCount() > 0) {
                flushTableNames.add(tableName);
                flushTableBuffers.add(tableBuffer);
            }
        }
        return flushTableBuffers.size();
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
        Runnable hook = closeDrainWaitingHook;
        closeDrainWaitingHook = null;
        if (hook != null) {
            try {
                hook.run();
            } catch (Throwable t) {
                // A test witness must never prevent production resource cleanup.
                LOG.error("Error in close-drain-waiting test hook: {}", String.valueOf(t));
            }
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
                // Name the outage the I/O thread is riding out, when there is one. A
                // foreground sender now retries endpoint-policy rejections indefinitely,
                // so a revoked token reaches the operator HERE, and blaming timeout
                // tuning for what is actually an auth failure would misdirect them.
                CursorWebSocketSendLoop loop = cursorSendLoop;
                Throwable outage = loop == null ? null : loop.lastReconnectError();
                LOG.warn("close() drain timed out after {}ms [target={} acked={}], pending data may be lost{}",
                        closeFlushTimeoutMillis, target, acked,
                        outage == null ? "" : "; wire is not draining: " + outage.getMessage());
                throw new LineSenderException("close() drain timed out after ")
                        .put(closeFlushTimeoutMillis).put(" ms [targetFsn=")
                        .put(target).put(", ackedFsn=").put(acked)
                        .put("] - server did not acknowledge ")
                        .put(target - acked)
                        .put(outage == null
                                ? " pending batches; data may be lost (use larger closeFlushTimeoutMillis or smaller batches)"
                                : " pending batches; the wire is not draining: " + outage.getMessage());
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
                // a future version bump must account for that. Transport
                // failures retry indefinitely on the I/O thread (Invariant B).
                // But a terminal auth, upgrade or capability rejection on this
                // initial connect -- before the wire is ever up -- is surfaced
                // to the async SenderErrorHandler and latched for a close()
                // rethrow, not retried.
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
                    reconnectInitialBackoffMillis,
                    reconnectMaxBackoffMillis,
                    requestDurableAck,
                    durableAckKeepaliveIntervalMillis,
                    maxFrameRejections,
                    poisonMinEscalationWindowMillis,
                    catchUpCapGapMinEscalationWindowMillis,
                    CursorWebSocketSendLoop.ReconnectPolicy.FOREGROUND);
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
            // start() (or dispatcher construction) failed after cursorSendLoop was
            // assigned. Close it so a caller that retries -- re-entering
            // ensureConnected and reassigning cursorSendLoop above -- cannot orphan
            // a recovered slot's ctor-seeded native mirror (freed only by close()
            // or the I/O loop, neither of which has run). close() is idempotent and
            // frees the mirror via its loopNeverRan path; it also closes the shared
            // client, so the client.close() below is a safe idempotent no-op.
            if (cursorSendLoop != null) {
                cursorSendLoop.close();
                cursorSendLoop = null;
            }
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
            // its default version (V1). The per-batch symbol-dict watermark still
            // gets reset for consistency with the sync path; the post-connect
            // replay path needs no producer-side reset signal (see below).
            Endpoint ep = endpoints.get(0);
            LOG.info("Async initial connect deferred to I/O thread [firstHost={}, firstPort={}, endpointCount={}]",
                    ep.host, ep.port, endpoints.size());
        }
        // Server starts fresh on each connection, so reset the per-batch
        // symbol-dict watermark. Every frame still carries its full inline schema,
        // and the fresh server's dictionary is re-established either by a full-dict
        // frame (full-dict mode) or by an I/O-thread catch-up frame before replay
        // (delta mode), so post-reconnect replay needs no producer-side reset signal.
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
        int tableCount = collectNonEmptyTables(keys);
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
        // In full-dict mode every frame is self-sufficient: it carries the whole
        // symbol dictionary from id 0 so orphan-drain / recovery replay to a fresh
        // server never dangles a symbol id. In delta mode (memory-mode, and
        // file-mode store-and-forward once the persisted dictionary opened) each
        // frame carries only ids above sentMaxSymbolId; a reconnect re-registers
        // the dictionary via an I/O-thread catch-up frame before replay, so the
        // producer's monotonic baseline stays valid across the wire boundary.
        // Snapshot the volatile cap ONCE for this whole flush. The I/O thread lowers
        // serverMaxBatchSize on a mid-stream failover to a smaller-cap node
        // (applyServerBatchSizeLimit); if the dictionary pre-registration, the split
        // pre-flight and the publish loop re-read the field independently, a failover
        // between them would size frames against different caps -- breaking the
        // all-or-nothing guarantee. They all use this snapshot; the next flush picks
        // up the new cap.
        int cap = serverMaxBatchSize;
        int deltaBaseline = symbolDeltaBaseline();
        int combinedBodyStart = encodeCombinedFrame(tableCount, deferCommit, deltaBaseline);
        int messageSize = encoder.finishMessage();

        // Full-dict over-cap fallback. A full-dictionary frame carries the whole
        // dictionary from id 0, so its fixed overhead grows with lifetime symbol
        // cardinality: eventually the section alone fills the cap, and before that
        // the section plus a table body pushes every split frame over it. Either
        // way the split's pre-flight would reject a batch that IS shippable, and
        // reset() cannot shrink a dictionary. Move the section into its own
        // DEFERRED, table-less chunk frames and re-encode the batch against the
        // resulting empty delta.
        //
        // Publishing happens HERE and nowhere earlier, which is what keeps the
        // flush all-or-nothing. An earlier revision pre-registered the chunks
        // before the bodies were sized, so an oversized table left them stranded
        // on the ring -- and because the split's throw RETAINS the batch by
        // design, and its message invites the caller to retry, every retry
        // published the whole dictionary again onto a ring whose ack watermark
        // was frozen (the server withholds the ack for a deferred frame until
        // its group commits, so trim could never free them). The
        // splitFramesFit(currentBatchMaxSymbolId) guard below is the proof that
        // chunking will actually make the batch shippable, so nothing is ever
        // published for a batch that then throws.
        //
        // The re-encode is required, not just a baseline switch inside the split:
        // publishing a chunk resets the encoder buffer the split's staged body
        // slices live in (beginMessage calls buffer.reset()).
        if (cap > 0 && messageSize > cap
                && !deltaDictEnabled && currentBatchMaxSymbolId > deltaBaseline
                && !splitFramesFit(cap, deltaBaseline)
                && splitFramesFit(cap, currentBatchMaxSymbolId)) {
            publishDictionaryChunks(cap, deltaBaseline + 1, currentBatchMaxSymbolId);
            deltaBaseline = currentBatchMaxSymbolId;
            combinedBodyStart = encodeCombinedFrame(tableCount, deferCommit, deltaBaseline);
            messageSize = encoder.finishMessage();
        }
        QwpBufferWriter buffer = encoder.getBuffer();

        if (cap > 0 && messageSize > cap) {
            // Keep the completed combined frame staged in the encoder while the
            // split path copies its delta entries and table-body slices.
            flushPendingRowsSplit(deferCommit, combinedBodyStart, cap, deltaBaseline);
            return;
        }

        // Write-ahead: durably persist this frame's new symbols BEFORE it is
        // published, so a recovered/orphan-drained slot can always rebuild the
        // dictionary the (non-self-sufficient) delta frame references. No-op in
        // memory mode and when the frame introduces no new symbols.
        persistNewSymbolsBeforePublish();
        activeBuffer.ensureCapacity(messageSize);
        activeBuffer.write(buffer.getBufferPtr(), messageSize);
        activeBuffer.incrementRowCount();
        sealAndSwapBuffer();
        // The frame carrying ids up to currentBatchMaxSymbolId is now on the ring;
        // advance the delta baseline so the next frame ships only newer ids.
        advanceSentMaxSymbolId();

        hasDeferredMessages = deferCommit;
        if (!deferCommit) {
            lastCommitBoundaryFsn = cursorEngine.publishedFsn();
        }

        resetTableBuffersAfterFlush();
    }

    /**
     * Encodes the staged batch as one combined frame at {@code deltaBaseline}:
     * begins the message, appends every non-empty table, and records each table's
     * encoded body length in {@code splitFrameBodyBytes} (when the batch needs
     * splitting, those lengths delimit immutable body slices in the combined
     * encoder buffer for direct frame assembly; the capture is a couple of int ops
     * per table on the common path). The caller finishes the message; calling
     * again re-encodes from scratch, since beginMessage resets the encoder buffer.
     *
     * @return the buffer position where the first table body starts
     */
    private int encodeCombinedFrame(int tableCount, boolean deferCommit, int deltaBaseline) {
        encoder.setDeferCommit(deferCommit);
        encoder.beginMessage(tableCount, globalSymbolDictionary,
                deltaBaseline, currentBatchMaxSymbolId);
        splitFrameBodyBytes.clear();
        int combinedBodyStart = encoder.getBuffer().getPosition();
        int bodyStart = combinedBodyStart;
        for (int i = 0; i < tableCount; i++) {
            QwpTableBuffer tableBuffer = flushTableBuffers.getQuick(i);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Encoding table [name={}, rows={}, batchMaxId={}]",
                        flushTableNames.getQuick(i), tableBuffer.getRowCount(), currentBatchMaxSymbolId);
            }

            encoder.addTable(tableBuffer);
            int bodyEnd = encoder.getBuffer().getPosition();
            splitFrameBodyBytes.add(bodyEnd - bodyStart);
            bodyStart = bodyEnd;
        }
        return combinedBodyStart;
    }

    /**
     * Whether every per-table split frame of the staged batch fits {@code cap}
     * when sized at {@code baseline}. Mirrors flushPendingRowsSplit's pre-flight
     * in full-dict mode, where the baseline never advances across frames. Only
     * two baselines are legal: the staged message's own, and
     * {@code currentBatchMaxSymbolId} (an empty delta, which getSplitMessageSize
     * sizes without consulting staged state).
     */
    private boolean splitFramesFit(int cap, int baseline) {
        for (int i = 0, n = splitFrameBodyBytes.size(); i < n; i++) {
            if (encoder.getSplitMessageSize(
                    splitFrameBodyBytes.getQuick(i), baseline, currentBatchMaxSymbolId) > cap) {
                return false;
            }
        }
        return true;
    }

    /**
     * Splitting path: the full batch exceeds serverMaxBatchSize, so
     * flushPendingRows() delegates here. Each non-empty table gets its
     * own message. All messages except the last carry FLAG_DEFER_COMMIT
     * so the server appends rows without committing until the final
     * message arrives.
     * <p>
     * <b>Not atomic across frames.</b> The frames publish one at a time, so a
     * publish failure partway through -- {@link #sealAndSwapBuffer()} throwing on
     * frame k&gt;1, e.g. a backpressure deadline or the buffer-recycle timeout --
     * leaves frames 1..k-1 already on the ring as deferred (appended, not yet
     * committed). The throw propagates past the {@code resetTableBuffersAfterFlush}
     * at the end of the loop, so the source rows survive in their table buffers
     * and the NEXT flush re-emits the whole batch; the eventual commit then
     * commits the already-published prefix alongside the re-sent copies,
     * delivering those rows at-least-once (duplicated), not exactly-once. This is
     * within store-and-forward's at-least-once contract -- a DEDUP table or a
     * durable-ack await absorbs the duplicate, and the symbol-dict state stays
     * consistent on the retry (the re-sent frames carry empty deltas and the
     * write-ahead persist is a {@code pd.size()} no-op). Making the split atomic
     * (rolling back the published prefix, or skipping it on retry) would be a
     * larger change.
     *
     * @param deferCommit when true, ALL messages (including the last)
     *                    carry FLAG_DEFER_COMMIT. When false, only the
     *                    last message omits the flag.
     */
    private void flushPendingRowsSplit(
            boolean deferCommit,
            int combinedBodyStart,
            int cap,
            int deltaBaseline
    ) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Splitting flush across multiple messages [serverMaxBatchSize={}, defer={}]", cap, deferCommit);
        }

        // Collect non-empty table indices so we know which is last, AND pre-flight
        // every split frame's size BEFORE publishing any of them. The split hands
        // frames to the ring one at a time (all but the last deferred -- appended but
        // uncommitted); if a later table's frame were only found oversized
        // mid-publish, the already-published prefix would strand on the ring, a
        // subsequent commit would deliver it as a partial batch, and
        // resetTableBuffersAfterFlush would discard every source row -- a partial
        // commit the caller was told (by the throw) had failed. Checking all sizes up
        // front makes the split all-or-nothing: either every frame fits and all
        // publish, or none publish and we throw with nothing stranded.
        //
        // Each split frame's size is derived from the combined encode flushPendingRows
        // already performed. simBaseline mirrors the publish loop's baseline advance
        // (advanceSentMaxSymbolId), so each size equals the frame the staged-slice
        // assembler will build; this pass mutates no delta/persist state.
        int nonEmptyCount = flushTableBuffers.size();
        int simBaseline = deltaBaseline;
        for (int i = 0; i < nonEmptyCount; i++) {
            CharSequence tableName = flushTableNames.getQuick(i);
            int messageSize = encoder.getSplitMessageSize(
                    splitFrameBodyBytes.getQuick(i), simBaseline, currentBatchMaxSymbolId);
            if (messageSize > cap) {
                // The batch stays BUFFERED: this throw precedes every publish, and a
                // rejected flush must not silently discard the caller's rows (see
                // SelfSufficientFramesTest#testOversizedTableSplitStrandsNothing). So the
                // next flush() re-encodes and re-rejects the same batch until either the
                // reachable cap grows -- a failover to a larger-cap node, which is the
                // case retaining the rows exists to survive -- or the sender is closed,
                // which discards them. It cannot drain against a cap this table will
                // never fit, so say that here rather than let a caller read the repeat
                // rejections as a transient and keep appending to a batch that only
                // grows.
                // In FULL-DICT mode an over-cap dictionary has been handled upstream:
                // flushPendingRows' fallback moved the section into its own chunk frames
                // and re-encoded this batch against an empty delta, so reaching here means
                // a table BODY is what does not fit. In DELTA mode that fallback is gated
                // off -- publishing before persistNewSymbolsBeforePublish would break the
                // write-ahead ordering -- so the section can still be the half that does
                // not fit, and reset() alone does not shrink it: the next batch's delta
                // starts at the same sentMaxSymbolId+1 and spans up to whatever id it
                // references. Pick the remedy from which half actually exceeds the cap
                // rather than prescribing one that cannot work.
                int frameOverhead = encoder.getSplitMessageSize(
                        0, simBaseline, currentBatchMaxSymbolId);
                String remedy = frameOverhead > cap
                        ? "the symbol dictionary section alone exceeds the cap, so neither reset() "
                        + "nor smaller batches shrink it -- every later batch re-registers from the "
                        + "same id. Close this sender and build a new one to restart the id space, "
                        + "raise the server's maximum batch size, or use a varchar column instead of "
                        + "symbol for this data"
                        : "call reset() to discard the retained batch and keep this sender, close the "
                        + "sender to discard everything, or produce smaller batches";
                throw new BatchTooLargeForCapException("single table batch too large for server batch cap")
                        .put(" [table=").put(tableName)
                        .put(", messageSize=").put(messageSize)
                        .put(", dictionaryFrameBytes=").put(frameOverhead)
                        .put(", serverMaxBatchSize=").put(cap).put(']')
                        .put("; the batch is retained for retry and every flush() will "
                                + "reject it again until a larger-cap node is reached -- ")
                        .put(remedy);
            }
            // Mirror advanceSentMaxSymbolId: once the first frame ships the batch's
            // new ids, the remaining frames carry an empty delta above the baseline.
            if (deltaDictEnabled && currentBatchMaxSymbolId > simBaseline) {
                simBaseline = currentBatchMaxSymbolId;
            }
        }

        int tableBodyOffset = combinedBodyStart;
        // Mirrors simBaseline above and advanceSentMaxSymbolId below, so the frame
        // this loop assembles is byte-identical to the one the pre-flight sized.
        // Re-reading symbolDeltaBaseline() here instead would ignore the dictionary
        // pre-registration, which advances the baseline without touching
        // sentMaxSymbolId (full-dict mode never advances it).
        int frameBaseline = deltaBaseline;
        for (int i = 0; i < nonEmptyCount; i++) {
            CharSequence tableName = flushTableNames.getQuick(i);

            boolean isLast = (i == nonEmptyCount - 1);
            boolean deferThis = deferCommit || !isLast;

            int tableBodyLength = splitFrameBodyBytes.getQuick(i);
            // Persist before touching activeBuffer. If the write-ahead fails, the
            // caller can retry with both the source rows and active microbatch
            // unchanged. The first frame carries the batch's new symbols; later
            // frames are no-ops once the baseline has advanced.
            persistNewSymbolsBeforePublish();
            ensureActiveBufferReady();
            // The combined encoder buffer remains immutable for the whole split.
            // Assemble this frame directly into the active microbatch: patched
            // header + staged delta bytes + the staged table-body slice. No row or
            // column is encoded a second time.
            int messageSize = encoder.copySplitMessage(
                    activeBuffer,
                    tableBodyOffset,
                    tableBodyLength,
                    deferThis,
                    frameBaseline,
                    currentBatchMaxSymbolId
            );
            tableBodyOffset += tableBodyLength;
            // The pre-flight pass above already verified every split frame fits the
            // cap, so none can be found oversized here -- which is what keeps this
            // loop from publishing (and stranding) a deferred prefix before an
            // oversized table. Both passes size against the SAME snapshot cap, so a
            // mid-flush failover cannot make them disagree; the assert therefore only
            // catches a genuine divergence between the pre-flight arithmetic and the
            // staged assembler (a future bug), not a cap race. It deliberately does NOT
            // reset+throw here, because by this point a prefix may already be on the ring.
            assert messageSize <= cap
                    : "split frame exceeded serverMaxBatchSize after pre-flight [table=" + tableName
                    + ", messageSize=" + messageSize + ", serverMaxBatchSize=" + cap + ']';

            activeBuffer.incrementRowCount();
            sealAndSwapBuffer();
            // Frame queued: advance so the next split frame's delta starts above
            // the ids this one just registered.
            advanceSentMaxSymbolId();
            if (deltaDictEnabled && currentBatchMaxSymbolId > frameBaseline) {
                frameBaseline = currentBatchMaxSymbolId;
            }
        }

        encoder.setDeferCommit(false);
        hasDeferredMessages = deferCommit;
        if (!deferCommit) {
            // The last message of the split carried no defer flag -- it
            // committed the whole group.
            lastCommitBoundaryFsn = cursorEngine.publishedFsn();
        }
        resetTableBuffersAfterFlush();
    }

    private void onSlotLockReleased() {
        slotLockReleased = true;
        Runnable listener = slotLockReleaseListener;
        if (listener != null) {
            listener.run();
        }
    }

    private void resetTableBuffersAfterFlush() {
        for (int i = 0, n = flushTableBuffers.size(); i < n; i++) {
            flushTableBuffers.getQuick(i).reset();
        }
        // Drop the references; the next flush re-collects.
        flushTableNames.clear();
        flushTableBuffers.clear();
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
        // A commit carries no rows, and it must also carry NO symbols. Passing the
        // baseline as BOTH bounds makes the delta empty by construction in either
        // mode -- [baseline+1 .. baseline] -- which is the only shape that is
        // unconditionally correct here:
        //
        //  - Delta mode: sendCommitMessage does NOT write-ahead-persist the
        //    dictionary, so shipping a symbol would put an id on the wire that a
        //    recovered slot cannot rebuild from the persisted .symbol-dict,
        //    diverging the producer dictionary from the surviving frames and
        //    silently misattributing reused ids after a crash.
        //  - Full-dict mode: the baseline is -1, so the frame carries deltaStart 0
        //    with a zero count -- always accepted, and it needs to register
        //    nothing because the group's data frames already did. Deriving the
        //    bound from currentBatchMaxSymbolId instead USED to be safe only
        //    because "the prior flush reset it to -1", which is not guaranteed:
        //    flushPendingRows returns early without resetting it when
        //    pendingRowCount is 0 or every table is empty, and cancelRow leaves a
        //    registered symbol's id behind. A commit reached through that window
        //    re-shipped the ENTIRE dictionary from id 0 in a frame no cap check
        //    or chunker covers -- the exact oversized-frame wall the chunking
        //    above exists to remove, on the one path that bypassed it.
        //
        // Any symbol a cancelled row leaked is picked up (and, in delta mode,
        // persisted) by the next real flush, whose persistNewSymbolsBeforePublish
        // resumes from pd.size().
        int commitBaseline = symbolDeltaBaseline();
        encoder.beginMessage(0, globalSymbolDictionary, commitBaseline, commitBaseline);
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

    /**
     * Advances the delta baseline once a frame carrying the current batch's
     * symbols has been queued onto the ring. No-op in full-dict mode. Only ever
     * moves the baseline forward, so a batch that used no new symbols leaves it
     * unchanged.
     */
    private void advanceSentMaxSymbolId() {
        if (deltaDictEnabled && currentBatchMaxSymbolId > sentMaxSymbolId) {
            sentMaxSymbolId = currentBatchMaxSymbolId;
        }
    }

    /**
     * Stops emitting delta dictionaries for the rest of this sender's life, after the
     * per-slot {@code .symbol-dict} has proved unwritable.
     * <p>
     * The side-file can stop accepting appends mid-run -- a full disk or an exhausted
     * quota, where SF's own segments stay writable because they are pre-allocated mmap
     * files while the dictionary is the one thing still growing. Without a way back,
     * {@code deltaDictEnabled} is written once at {@code setCursorEngine} and every
     * later {@code flush()} re-throws forever: a condition store-and-forward is built
     * to survive becomes total, permanent ingestion loss.
     * <p>
     * Full self-sufficient frames need no side file at all -- each carries the whole
     * dictionary from id 0, which is exactly what recovery and orphan-drain replay
     * against a fresh server. So degrade instead of dying. The producer's monotonic
     * baseline stops being consulted ({@link #symbolDeltaBaseline()} returns -1), and
     * the write-ahead persist becomes a no-op.
     * <p>
     * Producer-thread only, like every other reader of {@code deltaDictEnabled}.
     */
    private void disableDeltaDict(Throwable cause) {
        if (!deltaDictEnabled) {
            return;
        }
        deltaDictEnabled = false;
        LOG.warn("symbol dictionary persistence failed; this sender has switched to full "
                + "self-sufficient frames for the rest of its life (bandwidth cost only -- "
                + "no data is at risk, and recovery replays such frames without a side file)",
                cause);
    }

    /**
     * Writes the ids the surviving frames contributed above the persisted prefix back
     * into {@code .symbol-dict}, immediately, before any new frame can be published.
     * <p>
     * {@link #seedGlobalDictionaryFromPersisted} can rebuild the producer dictionary from
     * TWO sources -- the side-file's intact prefix and the surviving frames' own delta
     * sections -- and then resumes {@code sentMaxSymbolId} at the combined tip. When the
     * frames contributed anything, that tip is ABOVE {@code pd.size()}, so every frame
     * published from here on carries a {@code deltaStart} the side-file cannot describe.
     * That breaks the write-ahead invariant the whole design rests on: the persisted
     * dictionary must be a superset of every recoverable frame's references.
     * <p>
     * The steady-state write-ahead does NOT close that gap on its own. It persists
     * {@code [pd.size() .. currentBatchMaxSymbolId]} and returns early when the batch's
     * highest id is below {@code pd.size()}, so it heals only if -- and only as far as --
     * a later batch happens to reference the recovered high ids. Meanwhile the frames
     * that carry those ids are the oldest unacked, so they are the FIRST to be acked and
     * trimmed. Once they are gone, an ordinary process crash (which store-and-forward
     * promises to survive; only the original tear needs a host crash) leaves a slot whose
     * frames reference ids nothing holds: recovery marks a gap and {@code build()}
     * quarantines it with "resend the affected data".
     * <p>
     * Healing here, eagerly and in full, restores the invariant before the window opens.
     */
    /**
     * On-wire byte cost of one symbol-dictionary entry, exactly as
     * {@code NativeBufferWriter.putString} writes it: {@code [varint utf8Len][utf8]}.
     * Both of that method's branches (the ASCII fast path, which reserves
     * {@code varintSize(charLen) == varintSize(utf8Len)}, and the two-pass fallback)
     * produce this size, so the chunker below sizes frames against the same
     * arithmetic the encoder will use rather than an independent estimate.
     */
    private int dictionaryEntryWireBytes(int id) {
        int utf8Len = NativeBufferWriter.utf8Length(globalSymbolDictionary.getSymbol(id));
        return NativeBufferWriter.varintSize(utf8Len) + utf8Len;
    }

    private void healPersistedDictionary(PersistedSymbolDict pd) {
        if (pd == null || !deltaDictEnabled) {
            return;
        }
        int from = pd.size();
        int to = globalSymbolDictionary.size() - 1;
        if (to < from) {
            return; // the side-file already covers everything the frames defined
        }
        try {
            pd.appendSymbols(globalSymbolDictionary, from, to);
        } catch (Throwable t) {
            // A recognised mmap access fault is NOT a JVM failure to propagate: it is how
            // an unbacked or sparse append page surfaces, and commitMappedChunk uses
            // Crc32c.updateUnsafe precisely so it arrives catchable here. Rethrowing it
            // raw skips disableDeltaDict, and from healPersistedDictionary it escapes
            // Sender.build() as neither UnreplayableSlotException nor LineSenderException,
            // so the slot is neither quarantined nor reported and every restart re-faults
            // the same page.
            if (t instanceof Error && !MmapSegment.isMmapAccessFault(t)) {
                throw (Error) t;
            }
            // Do NOT fail recovery: the surviving frames still carry these ids in their
            // own deltas, so THIS session replays correctly either way. Only a future
            // recovery, after those frames are trimmed, would be affected -- and the
            // degrade below removes even that exposure by dropping back to frames that
            // need no side file.
            disableDeltaDict(t);
        }
    }

    /**
     * Appends the symbols this frame introduces ({@code [sentMaxSymbolId+1 ..
     * currentBatchMaxSymbolId]}) to the slot's persisted dictionary BEFORE the
     * frame is published to the ring. This write-ahead ordering keeps the
     * persisted dictionary a superset of every process-crash-recoverable frame's
     * references, so recovery and orphan-drain can re-register it on a fresh
     * server. Not fsync'd (see PersistedSymbolDict) -- a host crash that tears it
     * is caught by the send loop's replay guard. No-op in memory mode (no
     * persisted dictionary) and when the frame introduces no new symbols.
     */
    private void persistNewSymbolsBeforePublish() {
        if (!deltaDictEnabled || cursorEngine == null) {
            return;
        }
        PersistedSymbolDict pd = cursorEngine.getPersistedSymbolDict();
        if (pd == null) {
            return;
        }
        // Persist [pd.size() .. currentBatchMaxSymbolId] as ONE write, BEFORE the
        // frame is published.
        //
        // Resume from the dictionary's own durable size, NOT sentMaxSymbolId+1:
        // the persist advances pd.size() only after a full write, whereas
        // sentMaxSymbolId only advances after the WHOLE frame is published (via
        // advanceSentMaxSymbolId, after activeBuffer.write). If a prior persist
        // threw (short write -- disk full/quota) or the publish threw, the frame
        // was not published and sentMaxSymbolId stayed put, while the symbols
        // before the failure are already on disk. Keying the resume point off
        // sentMaxSymbolId+1 would re-append that persisted prefix on the retry,
        // duplicating entries and corrupting the dense id->symbol mapping recovery
        // relies on (position i must be symbol id i). pd.size() resumes exactly
        // past what is already durable, so the write-ahead is idempotent.
        int from = pd.size();
        if (currentBatchMaxSymbolId < from) {
            return; // nothing new to persist (warm batch, or an idempotent retry)
        }
        // Fast path: the frame the encoder just built already holds these symbols
        // in its delta section as [len][utf8]... -- byte-identical to what
        // PersistedSymbolDict stores. In the common case pd.size() equals the
        // frame's delta start id (sentMaxSymbolId+1), so persist those bytes
        // straight from the frame instead of re-encoding the symbols. After a
        // failed publish the durable size has run ahead of the wire baseline, so
        // the frame's delta covers MORE than remains to persist; then re-encode
        // just the [from .. currentBatchMaxSymbolId] suffix.
        try {
            if (from == sentMaxSymbolId + 1) {
                QwpBufferWriter buffer = encoder.getBuffer();
                pd.appendRawEntries(
                        buffer.getBufferPtr() + encoder.getDeltaEntriesStart(),
                        encoder.getDeltaEntriesLen(),
                        currentBatchMaxSymbolId - from + 1);
            } else {
                pd.appendSymbols(globalSymbolDictionary, from, currentBatchMaxSymbolId);
            }
        } catch (Throwable t) {
            // A failed write to the persisted dictionary throws a low-level
            // IllegalStateException: in production that is ff.allocate refusing to grow
            // the mmap append window (how a full disk / exhausted quota surfaces there),
            // and behind an injected facade a short positioned write. Surface it as a
            // LineSenderException -- like every other flush-path failure, e.g. the cursor
            // append in sealAndSwapBuffer -- so a caller catching LineSenderException
            // around flush() also catches a disk-full during the write-ahead persist. The
            // persist ran before publish and pd.size() did not advance on the failure, so
            // the still-buffered rows re-persist the same range idempotently on retry.
            // A JVM Error is never a persist failure; let it propagate -- except a
            // recognised mmap access fault, which is NOT a JVM failure to propagate: it
            // is how an unbacked or sparse append page surfaces, and commitMappedChunk
            // uses Crc32c.updateUnsafe precisely so it arrives catchable here. Rethrowing
            // it raw skips disableDeltaDict, and from healPersistedDictionary it escapes
            // Sender.build() as neither UnreplayableSlotException nor LineSenderException,
            // so the slot is neither quarantined nor reported and every restart re-faults
            // the same page.
            if (t instanceof Error && !MmapSegment.isMmapAccessFault(t)) {
                throw (Error) t;
            }
            // Degrade before throwing, so this failure is survivable rather than terminal:
            // every LATER flush emits full self-sufficient frames, which need no side file
            // (see disableDeltaDict). This one flush still has to fail -- beginMessage has
            // already baked a delta deltaStart into the staged frame, and publishing it
            // would put ids on the ring that the side-file cannot describe. The throw
            // precedes every publish, so the caller's rows stay buffered and the next
            // flush() re-encodes them from id 0.
            disableDeltaDict(t);
            throw new LineSenderException("failed to persist symbol dictionary before publish; "
                    + "this sender has switched to full self-sufficient frames -- retry the flush", t);
        }
    }

    /**
     * Publishes symbol ids {@code [from, batchMaxId]} as DEFERRED, table-less
     * frames, each chunked under {@code cap}, so the batch's data frames can then
     * encode against an empty delta.
     * <p>
     * <b>Why this exists.</b> A full-dictionary frame carries the whole dictionary from
     * id 0, so its fixed overhead grows with lifetime symbol cardinality. Once that
     * overhead reaches the server's batch cap -- alone, or beside a table body --
     * EVERY frame is oversized however the batch is split,
     * {@code flushPendingRowsSplit}'s pre-flight rejects it, and the sender can never
     * flush again: {@code reset()} discards rows, not the dictionary, so the next
     * batch fails identically. Chunking the dictionary into its own frames removes
     * the wall -- each chunk carries a contiguous id range sized under the cap,
     * exactly as {@code CursorWebSocketSendLoop.sendDictCatchUp} chunks the reconnect
     * catch-up.
     * <p>
     * <b>Why it is safe to make the data frames non-self-sufficient.</b> Full-dict mode
     * exists so a recovered or orphan-drained slot never replays a frame whose symbol
     * ids nothing registered. The chunks preserve that, one level up: they are emitted
     * with {@code FLAG_DEFER_COMMIT}, and the server refuses to advance its cumulative
     * ack watermark over uncommitted deferred rows (it marks them and, as a last
     * resort, clamps the watermark and logs {@code critical}). A deferred group is
     * therefore atomic with respect to the client's trim watermark: the chunks cannot
     * be trimmed away ahead of the data frames that depend on them, so the GROUP is
     * self-sufficient even though its individual frames are not. Recovery replays it
     * whole, in order, and {@code RecoveredFrameAnalysis} folds the chunks' deltas
     * before it reaches the data frames.
     * <p>
     * The baseline is deliberately NOT persisted into {@code sentMaxSymbolId}: full-dict
     * mode carries no cross-batch dictionary state, so every batch re-registers. That
     * keeps the bandwidth cost full-dict mode already accepts, and keeps each batch
     * independently replayable.
     * <p>
     * All-or-nothing in both directions. Every entry is validated against the cap
     * BEFORE any chunk is published, so a symbol too large to ship at all throws with
     * nothing on the ring; and the sole caller only reaches here once it has proven
     * the batch's bodies fit an empty delta, so a batch that will be rejected never
     * publishes a chunk either.
     */
    private void publishDictionaryChunks(int cap, int from, int batchMaxId) {
        assert !deltaDictEnabled;
        // Pass one: prove every entry is shippable on its own, before anything
        // reaches the ring. A symbol wider than the cap cannot be split across
        // frames, so it can never be registered and the batch is unshippable --
        // say that plainly rather than let it surface as an unexplained oversized
        // frame from the chunk loop below.
        for (int id = from; id <= batchMaxId; id++) {
            long soloFrameBytes = (long) QwpConstants.HEADER_SIZE
                    + NativeBufferWriter.varintSize(id)
                    + NativeBufferWriter.varintSize(1)
                    + dictionaryEntryWireBytes(id);
            if (soloFrameBytes > cap) {
                throw new BatchTooLargeForCapException("a single symbol value is too large for the server batch cap")
                        .put(" [symbolId=").put(id)
                        .put(", frameBytes=").put(soloFrameBytes)
                        .put(", serverMaxBatchSize=").put(cap).put(']')
                        .put("; a symbol value cannot be split across frames -- shorten it, "
                                + "raise the server's maximum batch size, or use a varchar "
                                + "column instead of symbol for this data");
            }
        }
        int chunkStart = from;
        long chunkBytes = 0;
        for (int id = from; id <= batchMaxId; id++) {
            int entryBytes = dictionaryEntryWireBytes(id);
            // Size the frame this entry WOULD produce, with the count varint the
            // grown chunk actually needs -- a reserve-based estimate can be one byte
            // short exactly when the count crosses a varint boundary.
            long withEntry = (long) QwpConstants.HEADER_SIZE
                    + NativeBufferWriter.varintSize(chunkStart)
                    + NativeBufferWriter.varintSize(id - chunkStart + 1)
                    + chunkBytes + entryBytes;
            if (id > chunkStart && withEntry > cap) {
                publishDictionaryChunk(chunkStart, id - 1);
                chunkStart = id;
                chunkBytes = 0;
            }
            chunkBytes += entryBytes;
        }
        publishDictionaryChunk(chunkStart, batchMaxId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Registered symbol dictionary in chunks [from={}, to={}, cap={}]",
                    from, batchMaxId, cap);
        }
    }

    /**
     * Publishes one deferred, table-less frame registering symbol ids
     * {@code [startId, endId]}. Mirrors {@link #sendCommitMessage()}'s publish
     * sequence; the caller restores the encoder's defer flag before encoding the
     * batch's data frames.
     */
    private void publishDictionaryChunk(int startId, int endId) {
        encoder.setDeferCommit(true);
        // confirmedMaxId = startId - 1 makes beginMessage emit deltaStart = startId,
        // deltaCount = endId - startId + 1.
        encoder.beginMessage(0, globalSymbolDictionary, startId - 1, endId);
        int messageSize = encoder.finishMessage();
        QwpBufferWriter buffer = encoder.getBuffer();
        ensureActiveBufferReady();
        activeBuffer.ensureCapacity(messageSize);
        activeBuffer.write(buffer.getBufferPtr(), messageSize);
        activeBuffer.incrementRowCount();
        sealAndSwapBuffer();
        // The chunk is on the ring carrying FLAG_DEFER_COMMIT, so the server withholds
        // its ack and clamps the connection's cumulative-ack watermark until the group
        // commits. Record that debt here rather than leaving it to the caller: when the
        // batch meant to close the group throws instead -- an oversized table body
        // reaching the split pre-flight -- flushPendingRows never reaches its own
        // hasDeferredMessages assignment, close() skips sendCommitMessage, and the group
        // never commits. ackedFsn then freezes for the connection's whole life, so trim
        // stops for every frame and the ring fills. A later successful flush reassigns
        // this from its own deferCommit, which is correct: its data frame closes the group.
        hasDeferredMessages = true;
    }

    private void resetSymbolDictStateForNewConnection() {
        // Runs on the foreground (initial) connect only -- NOT on the I/O thread's
        // reconnect/failover path. The per-batch watermark is drained state, so
        // clearing it here is harmless. sentMaxSymbolId is deliberately left
        // untouched: in delta mode the I/O thread re-registers the whole
        // dictionary with a catch-up frame on reconnect, so the producer's
        // monotonic baseline must survive the wire boundary; resetting it would
        // desync the producer from the I/O thread's sent-dictionary count.
        currentBatchMaxSymbolId = -1;
    }

    /**
     * On recovery, repopulates the producer's {@link GlobalSymbolDictionary} so that newly
     * ingested symbols continue ABOVE every id the surviving frames already define, and
     * resumes the delta baseline at that tip.
     * <p>
     * Seeds from TWO sources, in this order:
     * <ol>
     *   <li>the slot's persisted {@code .symbol-dict} -- its intact prefix; then</li>
     *   <li>the surviving frames' OWN delta sections, for every id above that prefix
     *       ({@link CursorSendEngine#addRecoveredSymbolsTo}).</li>
     * </ol>
     * Those are exactly the two sources, in exactly the order, that the send loop's mirror
     * is built from: its constructor seeds {@code sentDictCount} from the same dictionary,
     * and {@code accumulateSentDict} then extends it from the same frames as they replay. So
     * the producer's {@code sentMaxSymbolId + 1} and the loop's {@code sentDictCount} land on
     * the same number BY CONSTRUCTION -- the invariant the torn-dictionary guard rests on --
     * rather than by the two happening to agree.
     * <p>
     * Uses {@link GlobalSymbolDictionary#addRecoveredSymbol} (append, NOT de-dup): the
     * persisted dictionary, the on-wire delta and the mirror all key on the entry POSITION
     * (id), so the producer's id space must match the recovered entry count exactly.
     * {@code getOrAddSymbol} would collapse two source strings that decode to the same
     * characters -- only malformed lone UTF-16 surrogates, which UTF-8-encode to {@code '?'}
     * -- leaving this dictionary SHORTER than the count and silently misattributing later
     * symbols.
     * <p>
     * <b>Why seeding from the frames matters.</b> The dictionary is not fsync'd (see
     * {@code PersistedSymbolDict}), so a host/power crash can tear off its newest entries
     * while the segment frames that introduced those ids survive -- and those newest frames,
     * being the least likely to be acked, are exactly the ones that replay. Seeded from the
     * short dictionary alone, this producer would hand its next new symbol an id those frames
     * already define, putting two symbols on one id and silently misattributing values. The
     * old code detected that and threw, which was safe but far too blunt: it bricked
     * {@code build()} for slots the background drainer replays PERFECTLY, because the frames
     * carry the torn-off symbols in their own deltas and {@code accumulateSentDict} rebuilds
     * the dictionary from them. This method now rebuilds the producer from the same bytes,
     * so a torn -- or entirely lost -- dictionary is recoverable whenever the surviving
     * frames define the ids themselves. The next flush's write-ahead persist then re-writes
     * those ids (it resumes from {@code pd.size()}), healing the side-file on disk.
     * <p>
     * <b>What still fails clean.</b> A genuine GAP: the ids below a surviving frame's delta
     * start were introduced by frames that were acked and TRIMMED away, so they lived only in
     * the lost dictionary and nothing can rebuild them.
     * {@code addRecoveredSymbolsTo} returns -1 for that and we throw. It is the same
     * condition the send loop's replay guard ({@code deltaStart > sentDictCount}) trips on, so
     * producer and drainer now agree on exactly which slots are recoverable, instead of the
     * producer rejecting slots the drainer drains.
     */
    private void seedGlobalDictionaryFromPersisted(PersistedSymbolDict pd) {
        if (cursorEngine == null) {
            return;
        }
        // 1. The dictionary's intact prefix. addRecoveredSymbol appends without de-dup, so
        //    the producer's size tracks pd.size() exactly -- which is what the send loop's
        //    mirror also seeds sentDictCount from.
        // Pre-size before pouring the recovered symbols in. The default capacity is 64,
        // so rebuilding a large dictionary rehashed the map ~log2(n/64) times, each pass
        // O(current size) -- roughly doubling the rebuild and touching a growing table
        // the whole way. recoveredMaxSymbolId + 1 is the upper bound the seed can reach.
        long expected = Math.max(pd == null ? 0L : pd.recoveredSize(),
                cursorEngine.recoveredMaxSymbolId() + 1L);
        if (expected > globalSymbolDictionary.size() && expected <= Integer.MAX_VALUE) {
            globalSymbolDictionary = new GlobalSymbolDictionary((int) expected);
        }
        int baseline = 0;
        if (pd != null && pd.size() > 0) {
            pd.addLoadedSymbolsTo(globalSymbolDictionary);
            baseline = globalSymbolDictionary.size();
        }
        // 2. Everything the surviving frames define above that prefix, straight out of their
        //    own delta sections -- the same bytes, in the same order, accumulateSentDict will
        //    feed the mirror as those frames go back on the wire.
        long coverage = cursorEngine.addRecoveredSymbolsTo(baseline, globalSymbolDictionary);
        if (coverage < 0) {
            // A gap: the surviving frames reference ids below their own delta start,
            // introduced by frames since acked and trimmed away, and the persisted
            // dictionary no longer holds them (a host crash tore its unsynced tail, or it
            // could not be opened). That gap only matters for frames that will REPLAY.
            // When every recovered committed frame is already acked
            // (ackedFsn >= recoveredCommitBoundaryFsn), NOTHING replays: the gap is in
            // data the server already has, and the retired orphan-deferred tail above the
            // commit boundary is never transmitted. Throwing here would raise a false
            // "resend required" for delivered data AND -- because such a slot is fully
            // drained -- let build()'s connect-path close unlink the (already-delivered)
            // bytes the quarantine claims to preserve. So DON'T throw: seed the intact
            // prefix only; addRecoveredSymbolsTo adds nothing on a -1 exactly as the
            // send loop's mirror does, so the producer baseline and the mirror's
            // sentDictCount still agree by construction. The producer resumes above the
            // prefix and the fully-drained slot is cleaned up on close.
            long ackedFsn = cursorEngine.ackedFsn();
            long commitBoundaryFsn = cursorEngine.recoveredCommitBoundaryFsn();
            if (ackedFsn >= commitBoundaryFsn) {
                sentMaxSymbolId = globalSymbolDictionary.size() - 1;
                LOG.info("recovered store-and-forward slot has a torn/incomplete symbol dictionary, "
                                + "but every committed frame was already acked so nothing needs replaying; "
                                + "resuming on the intact prefix without quarantine and without data loss "
                                + "[recoveredPrefixSize={}, ackedFsn={}, commitBoundaryFsn={}]",
                        baseline, ackedFsn, commitBoundaryFsn);
                return;
            }
            // Genuine loss: unacked committed frames reference ids nothing still holds.
            // Typed, because Sender.build() sets such a slot aside instead of failing: this
            // is the point at which every source of truth has been tried and none of them
            // holds the missing ids. See UnreplayableSlotException.
            throw new UnreplayableSlotException(
                    "recovered store-and-forward symbol dictionary is incomplete and cannot be "
                            + "rebuilt from the surviving frames (likely a host crash tore its unsynced "
                            + "tail): the frames reference symbol ids below their own delta start, which "
                            + "were introduced by frames since acked and trimmed away, so nothing still "
                            + "holds them; the recovered dictionary holds only "
                            + (pd == null ? 0 : pd.size()) + " id(s) -- resend the affected data");
        }
        // Producer baseline == the coverage the replay will establish == the mirror's
        // sentDictCount once those frames have gone out. The first new frame therefore
        // starts its delta exactly at the tip, and the replay guard passes.
        sentMaxSymbolId = globalSymbolDictionary.size() - 1;
        // ...but the baseline now sits ABOVE pd.size() whenever the frames contributed
        // ids, so restore the write-ahead invariant right now rather than hoping a later
        // batch reaches high enough to do it. See healPersistedDictionary.
        healPersistedDictionary(pd);
    }

    /**
     * The symbol id below which the server already holds every dictionary entry,
     * used as {@code confirmedMaxId} when encoding a frame. In delta mode this is
     * the producer's monotonic sent watermark; in full-dict mode it is -1 so every
     * frame re-ships the dictionary from id 0.
     */
    private int symbolDeltaBaseline() {
        return deltaDictEnabled ? sentMaxSymbolId : -1;
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
            // schema and the symbol-dict delta the batch requires.
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

        // Hard guard: a single row whose bytes exceed the server's wire cap
        // would flush as an oversize WS frame the server closes with
        // ws-close[1009]. nextRow() measures the row -- padding included,
        // since padding goes to the wire too -- inside its existing walk and
        // throws BEFORE committing, so the at()/atNow() error path can roll
        // back via rollbackRow() and prior committed rows in the batch stay
        // intact.
        // Snapshot the volatile cap ONCE, as flushPendingRows does. The I/O thread
        // lowers serverMaxBatchSize -- or clears it to 0 on a failover to a node that
        // advertises no cap -- mid-stream via applyServerBatchSizeLimit. Re-reading the
        // field across the guard and the throw could observe it drop to 0 between reads
        // and reject the row against a "cap" of 0, which actually means "no cap".
        int cap = serverMaxBatchSize;
        long bufferedNow = currentTableBuffer.nextRow(
                currentTableBufferSnapshotBytes, cap > 0 ? cap : Long.MAX_VALUE);

        if (pendingRowCount == 0) {
            firstPendingRowTimeNanos = System.nanoTime();
        }
        pendingRowCount++;

        // Advance pendingBytes by the bytes the just-committed row added to
        // the current table. nextRow() accumulated the total during its
        // null-padding walk, so there is no second O(columns) walk needed here.
        // The per-row work stays O(numColumns of the current table) -- no
        // map walk, no scaling with the number of tables this sender has
        // seen across its lifetime.
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
            return buildAndConnect(this, null);
        }

        @Override
        public WebSocketClient reconnect(CursorWebSocketSendLoop.ConnectCancellation cancellation) {
            return buildAndConnect(this, cancellation);
        }
    }
}
