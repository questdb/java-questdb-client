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

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.LineSenderServerException;
import io.questdb.client.SenderConnectionEvent;
import io.questdb.client.SenderError;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.http.client.WebSocketUpgradeException;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.cutlass.qwp.client.QwpDurableAckMismatchException;
import io.questdb.client.cutlass.qwp.client.QwpIngressRoleRejectedException;
import io.questdb.client.cutlass.qwp.client.QwpRoleMismatchException;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.client.std.CharSequenceLongHashMap;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * The cursor-engine I/O loop. Owns one I/O thread that:
 * <ol>
 *   <li>Polls {@link CursorSendEngine#publishedFsn()} and walks newly-published
 *       frames from the engine's segments, sending each as one WebSocket
 *       binary frame to the server.</li>
 *   <li>Polls the WebSocket for server ACK frames; on each ACK with
 *       cumulative wire sequence {@code N}, calls
 *       {@code engine.acknowledge(fsnAtZero + N)} so the segment manager
 *       can trim fully-acked segments.</li>
 *   <li>On wire failure, runs the configured reconnect policy: backoff
 *       with jitter up to {@code reconnect_max_duration_millis}, with
 *       auth-style failures (401/403/non-101 upgrade reject) treated as
 *       terminal. On reconnect success, repositions the cursor at
 *       {@code ackedFsn+1} and replays.</li>
 * </ol>
 * No locks on the steady-state path. The producer thread (user) writes
 * into the engine; this thread reads. {@code engine.publishedFsn()} is
 * the volatile publish barrier.
 * <p>
 * Errors are reported via {@link #getTerminalError()}; the I/O thread sets it
 * and exits. Producers polling {@link #checkError()} surface the failure.
 */
public final class CursorWebSocketSendLoop implements QuietCloseable {

    /**
     * Default cadence for the keepalive PING the I/O loop emits while
     * waiting on STATUS_DURABLE_ACK frames. See
     * {@link #sendDurableAckKeepaliveIfDue()} for the rationale: the OSS
     * server only flushes pending durable-ack frames on inbound recv
     * events, so an opted-in idle client has to prod it. {@code 200} ms
     * trades one PING per 200 ms per idle opted-in connection for
     * sub-second confirmation latency once the upload completes
     * server-side. {@code 0} or negative disables the keepalive entirely.
     */
    public static final long DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS = 200L;
    public static final long DEFAULT_PARK_NANOS = 50_000L; // 50us idle backoff
    /**
     * Default initial reconnect backoff (100 ms).
     */
    public static final long DEFAULT_RECONNECT_INITIAL_BACKOFF_MILLIS = 100L;
    /**
     * Default reconnect max backoff (5 s).
     */
    public static final long DEFAULT_RECONNECT_MAX_BACKOFF_MILLIS = 5_000L;
    /**
     * Default per-outage reconnect time cap (5 min).
     */
    public static final long DEFAULT_RECONNECT_MAX_DURATION_MILLIS = 300_000L;
    private static final Logger LOG = LoggerFactory.getLogger(CursorWebSocketSendLoop.class);
    /**
     * Throttle "reconnect attempt N failed" WARN logs to one per 5 s.
     */
    private static final long RECONNECT_LOG_THROTTLE_NANOS = 5_000_000_000L;
    // Pre-converted to nanos for the comparison in sendDurableAckKeepaliveIfDue.
    // Zero or negative disables the keepalive entirely.
    private final long durableAckKeepaliveIntervalNanos;
    // When true, OK frames do NOT advance engine.acknowledge -- only
    // STATUS_DURABLE_ACK frames do. The OK frame's wireSeq is stashed in
    // pendingDurable along with its per-table seqTxns, and trim only advances
    // when a durable-ack covers every batch up to some wireSeq. When false
    // (default), the loop trims on OK as it always has and ignores any
    // STATUS_DURABLE_ACK frames that might still arrive (logs a warning).
    private final boolean durableAckMode;
    // Per-table cumulative durable-upload watermarks, populated only when
    // durableAckMode is true. Updated from STATUS_DURABLE_ACK frame entries
    // (each entry is monotonically non-decreasing per spec). Reset on every
    // reconnect because the new connection's cumulative state is re-emitted
    // by the server -- holding stale watermarks across the wire boundary
    // would falsely advance trim before re-confirmation.
    private final CharSequenceLongHashMap durableTableWatermarks = new CharSequenceLongHashMap();
    private final CursorSendEngine engine;
    private final long parkNanos;
    // FIFO of OK-acked batches awaiting durable-upload confirmation. Used only
    // when durableAckMode is true. Each entry binds a wireSeq to the per-table
    // (name, seqTxn) pairs the server reported on the OK frame. The queue is
    // drained from the head every time a STATUS_DURABLE_ACK frame advances
    // any watermark; an entry pops when every (name, seqTxn) it carries is
    // covered by durableTableWatermarks. Bounded in practice by the SF on-disk
    // cap: once the producer hits sf_max_bytes it blocks, which caps how far
    // the durable watermark can lag behind the OK watermark.
    private final ArrayDeque<PendingDurableEntry> pendingDurable = new ArrayDeque<>();
    private final ArrayDeque<PendingDurableEntry> pendingDurablePool = new ArrayDeque<>();
    // Optional reconnect plumbing. When non-null, a wire failure triggers a
    // reconnect attempt instead of a terminal fail(). The factory produces a
    // fresh, connected+upgraded WebSocketClient.
    private final ReconnectFactory reconnectFactory;
    private final long reconnectInitialBackoffMillis;
    private final long reconnectMaxBackoffMillis;
    private final long reconnectMaxDurationMillis;
    private final WebSocketResponse response = new WebSocketResponse();
    private final ResponseHandler responseHandler = new ResponseHandler();
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final AtomicLong totalAcks = new AtomicLong();
    // Counters for observability of the durable-ack path. Both are zero
    // when durableAckMode is false.
    private final AtomicLong totalDurableAcks = new AtomicLong();
    private final AtomicLong totalDurableTrimAdvances = new AtomicLong();
    // Cumulative count of frames the loop has re-sent during post-reconnect
    // catch-up windows. Bumped once per frame on every iteration that
    // observes replayTargetFsn >= 0. A flat zero confirms steady state; a
    // sustained nonzero rate means the connection is flapping and replay
    // is doing real work each cycle.
    private final AtomicLong totalFramesReplayed = new AtomicLong();
    private final AtomicLong totalFramesSent = new AtomicLong();
    // Every iteration of the reconnect loop bumps this — failures and
    // success alike. Diverges from totalReconnects (success-only) when the
    // server is flapping. Useful for "is reconnect making progress?"
    // observability.
    private final AtomicLong totalReconnectAttempts = new AtomicLong();
    private final AtomicLong totalReconnects = new AtomicLong();
    // Total non-OK / non-DURABLE_ACK frames received from the server, classified
    // by category. Includes both DROP_AND_CONTINUE and HALT outcomes — i.e. every
    // server-side rejection observed regardless of how the loop reacted.
    private final AtomicLong totalServerErrors = new AtomicLong();
    private WebSocketClient client;
    // Optional: when non-null, every server-rejection error (DROP and HALT
    // alike) is offered to the dispatcher for async delivery to the user's
    // handler. Null disables async delivery entirely; the producer-side
    // typed-throw path is unaffected.
    // Optional: when non-null, RECONNECT_BUDGET_EXHAUSTED is offered to the
    // dispatcher for async delivery to the user's listener at the moment
    // connectLoop gives up. Sender-side fire points (CONNECTED, FAILED_OVER,
    // ENDPOINT_ATTEMPT_FAILED, AUTH_FAILED, ALL_ENDPOINTS_UNREACHABLE) write
    // directly to the same dispatcher from QwpWebSocketSender.
    private volatile SenderConnectionDispatcher connectionDispatcher;
    private volatile SenderErrorDispatcher errorDispatcher;
    // The send cursor has two coordinate systems:
    //
    //   FSN: durable frame sequence number in the local cursor engine. This is
    //        stable across reconnects and is what ACKs trim from disk.
    //   wireSeq: per-WebSocket-connection sequence number. The server resets
    //        this to 0 for every new connection, so the client must translate
    //        every ACK/NACK back to an FSN before touching the engine watermark.
    //
    // fsnAtZero is that translation anchor: FSN that wireSeq=0 maps to on the
    // current connection. For a fresh connection, this is 0. After a reconnect,
    // it is engine.ackedFsn() + 1, so the first replayed frame on the new
    // connection is wireSeq=0 and server-side cumulative ACKs still line up.
    private long fsnAtZero;
    // Sticky flag: false until the very first time a live client is installed
    // (either via the constructor in SYNC/OFF mode or via swapClient on a
    // successful connect attempt in any mode). Once true, stays true. Used to
    // distinguish "never reached the server" budget exhaustion (looks like a
    // config typo or firewall block) from "lost connection after we were
    // up" (looks transient).
    private volatile boolean hasEverConnected;
    private volatile Thread ioThread;
    // The latched terminal failure — THE exception every checkError() call
    // rethrows. Write-once for the loop's lifetime: the only writer is
    // recordFatal on the I/O thread (first-writer-wins). The whole
    // close()-ownership protocol rests on that — the identity comparisons
    // in hasUnsurfacedError() and in close()'s suppression are only
    // meaningful because the latched instance never changes.
    // Non-LineSenderException causes are wrapped once at latch time, so
    // rethrows always deliver the same instance.
    private volatile LineSenderException terminalError;
    // Wall clock of the last outbound activity on the wire -- a sent frame
    // (trySendOne) or a keepalive PING (sendDurableAckKeepaliveIfDue).
    // Throttles the durable-ack keepalive PING so it fires only when the
    // configured interval has elapsed since the most recent outbound event.
    // Zero until the first send; reset to zero on reconnect.
    private long lastFrameOrPingNanos;
    private long nextWireSeq;
    private volatile SenderProgressDispatcher progressDispatcher;
    // Frames sent during the post-reconnect catch-up window — i.e. frames
    // whose FSN was already published before the wire dropped. A non-zero
    // value confirms replay is working; a sustained nonzero rate means
    // the connection is flapping and replay is doing real work each cycle.
    // Set at swapClient time to publishedFsn at that moment; cleared back
    // to -1 once trySendOne has caught up past it. Used to count replay
    // frames without a per-frame branch on the steady-state path.
    private long replayTargetFsn = -1L;
    private volatile boolean running;
    // sendOffset: byte offset inside sendingSegment of the first not-yet-sent
    // byte. Initialized to MmapSegment.HEADER_SIZE on a fresh segment.
    private long sendOffset = MmapSegment.HEADER_SIZE;
    // sendingSegment: the segment we're currently consuming bytes from. Starts
    // at engine.activeSegment(); advances to newer sealed segments / the new
    // active as the producer rotates.
    private MmapSegment sendingSegment;
    // Exact terminalError instance that checkError() has thrown to a synchronous
    // user-thread caller (flush/append/close). close() uses the instance so it
    // only suppresses errors the user already owned before close() began.
    private volatile LineSenderException synchronouslySurfacedError;

    /**
     * Full constructor with explicit reconnect-policy knobs. When
     * {@code reconnectFactory} is non-null, the I/O thread treats wire
     * failures (send/receive errors, server-initiated close) as recoverable:
     * it calls the factory to obtain a fresh connected client, resets wire
     * state, and repositions its replay cursor at
     * {@code engine.ackedFsn() + 1}. A null factory disables reconnect
     * (single failure is terminal).
     * <p>
     * {@code client} may be {@code null} only if {@code reconnectFactory}
     * is non-null — this is the async-initial-connect path: the I/O thread
     * runs the same retry loop on its first iteration to obtain a live
     * client, and a terminal failure (auth/upgrade reject or budget
     * exhaustion) is delivered through the dispatcher rather than thrown
     * to the constructor's caller.
     */
    public CursorWebSocketSendLoop(WebSocketClient client, CursorSendEngine engine,
                                   long fsnAtZero, long parkNanos,
                                   ReconnectFactory reconnectFactory,
                                   long reconnectMaxDurationMillis,
                                   long reconnectInitialBackoffMillis,
                                   long reconnectMaxBackoffMillis) {
        this(client, engine, fsnAtZero, parkNanos, reconnectFactory,
                reconnectMaxDurationMillis, reconnectInitialBackoffMillis,
                reconnectMaxBackoffMillis, false);
    }

    /**
     * Same as the seven-arg constructor but with explicit control over
     * durable-ack-driven trim. {@code durableAckMode = true} switches the loop
     * to trim only on {@link WebSocketResponse#STATUS_DURABLE_ACK} frames; OK
     * frames are queued until their per-table seqTxns are covered by a durable
     * watermark. The default (false) preserves the historical OK-driven trim
     * and ignores any durable-ack frames that arrive (logging a warning, since
     * a server should not emit them when the client did not opt in).
     */
    public CursorWebSocketSendLoop(WebSocketClient client, CursorSendEngine engine,
                                   long fsnAtZero, long parkNanos,
                                   ReconnectFactory reconnectFactory,
                                   long reconnectMaxDurationMillis,
                                   long reconnectInitialBackoffMillis,
                                   long reconnectMaxBackoffMillis,
                                   boolean durableAckMode) {
        this(client, engine, fsnAtZero, parkNanos, reconnectFactory,
                reconnectMaxDurationMillis, reconnectInitialBackoffMillis,
                reconnectMaxBackoffMillis, durableAckMode,
                DEFAULT_DURABLE_ACK_KEEPALIVE_INTERVAL_MILLIS);
    }

    /**
     * Master constructor — also accepts the cadence at which the I/O loop
     * sends keepalive PINGs while waiting on STATUS_DURABLE_ACK frames.
     * Pass {@code 0} or negative to disable keepalive PINGs entirely (the
     * caller takes responsibility for prodding the server, e.g. by sending
     * data, or by accepting indefinite waits on idle connections).
     */
    public CursorWebSocketSendLoop(WebSocketClient client, CursorSendEngine engine,
                                   long fsnAtZero, long parkNanos,
                                   ReconnectFactory reconnectFactory,
                                   long reconnectMaxDurationMillis,
                                   long reconnectInitialBackoffMillis,
                                   long reconnectMaxBackoffMillis,
                                   boolean durableAckMode,
                                   long durableAckKeepaliveIntervalMillis) {
        if (engine == null) {
            throw new IllegalArgumentException("engine must be non-null");
        }
        if (client == null && reconnectFactory == null) {
            throw new IllegalArgumentException(
                    "client and reconnectFactory cannot both be null");
        }
        this.client = client;
        this.engine = engine;
        this.fsnAtZero = fsnAtZero;
        this.parkNanos = parkNanos;
        this.reconnectFactory = reconnectFactory;
        this.reconnectMaxDurationMillis = reconnectMaxDurationMillis;
        this.reconnectInitialBackoffMillis = reconnectInitialBackoffMillis;
        this.reconnectMaxBackoffMillis = reconnectMaxBackoffMillis;
        this.durableAckMode = durableAckMode;
        this.durableAckKeepaliveIntervalNanos = durableAckKeepaliveIntervalMillis > 0
                ? durableAckKeepaliveIntervalMillis * 1_000_000L
                : 0L;
        // SYNC/OFF startup hands a live client to the constructor, so we
        // already know we reached the server at least once. ASYNC startup
        // hands null and lets the I/O thread connect — hasEverConnected
        // stays false until swapClient sees its first success.
        this.hasEverConnected = client != null;
    }

    /**
     * Maps a server status byte to a {@link SenderError.Category}. Exposed for unit tests.
     */
    @TestOnly
    public static SenderError.Category classify(byte status) {
        switch (status) {
            case WebSocketResponse.STATUS_SCHEMA_MISMATCH:
                return SenderError.Category.SCHEMA_MISMATCH;
            case WebSocketResponse.STATUS_PARSE_ERROR:
                return SenderError.Category.PARSE_ERROR;
            case WebSocketResponse.STATUS_INTERNAL_ERROR:
                return SenderError.Category.INTERNAL_ERROR;
            case WebSocketResponse.STATUS_SECURITY_ERROR:
                return SenderError.Category.SECURITY_ERROR;
            case WebSocketResponse.STATUS_WRITE_ERROR:
                return SenderError.Category.WRITE_ERROR;
            default:
                return SenderError.Category.UNKNOWN;
        }
    }

    /**
     * Same retry-with-exponential-backoff-and-jitter loop the I/O thread
     * uses on a wire failure, but reusable from {@code ensureConnected} to
     * implement {@code initial_connect_retry=true}. Returns the connected
     * client on success; throws on terminal upgrade error (won't retry) or
     * budget exhaustion.
     * <p>
     * Caller-supplied {@code factory} is invoked once per attempt and
     * should produce a fresh, connected, upgraded client (or throw). The
     * lambda is intentionally a {@link ReconnectFactory} so the same
     * implementation in {@code QwpWebSocketSender.buildAndConnect()} can
     * serve both startup and reconnect paths verbatim.
     */
    public static WebSocketClient connectWithRetry(
            ReconnectFactory factory,
            long maxDurationMillis,
            long initialBackoffMillis,
            long maxBackoffMillis,
            String contextLabel
    ) {
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + maxDurationMillis * 1_000_000L;
        long backoffMillis = initialBackoffMillis;
        int attempts = 0;
        long lastLogNanos = 0L;
        Throwable lastError = null;
        while (System.nanoTime() < deadlineNanos) {
            attempts++;
            try {
                WebSocketClient c = factory.reconnect();
                if (c != null) {
                    long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
                    if (attempts > 1) {
                        LOG.info("{} succeeded after {}ms / {} attempts",
                                contextLabel, elapsedMs, attempts);
                    }
                    return c;
                }
            } catch (QwpAuthFailedException | QwpDurableAckMismatchException
                     | WebSocketUpgradeException e) {
                // Terminal across all configured endpoints per sf-client.md sections
                // 8.1 (durable-ack mismatch) and 13.3 (auth). Version mismatch is
                // NOT terminal here -- it falls through to the Throwable branch and
                // consumes the per-outage budget so the loop walks the cluster
                // across rolling-upgrade windows. See the parallel catch in the
                // cursor reconnect loop above for why WebSocketUpgradeException
                // reaching here is always non-421.
                LOG.error("{} hit terminal upgrade error, won't retry: {}",
                        contextLabel, e.getMessage());
                throw e;
            } catch (Throwable e) {
                lastError = e;
                long now = System.nanoTime();
                if (now - lastLogNanos >= RECONNECT_LOG_THROTTLE_NANOS) {
                    LOG.warn("{} attempt {} failed: {}",
                            contextLabel, attempts, e.getMessage());
                    lastLogNanos = now;
                }
            }
            long jitter = ThreadLocalRandom.current().nextLong(backoffMillis);
            long sleepMillis = backoffMillis + jitter;
            long remainingMillis = (deadlineNanos - System.nanoTime()) / 1_000_000L;
            if (remainingMillis <= 0) {
                break;
            }
            if (sleepMillis > remainingMillis) {
                sleepMillis = remainingMillis;
            }
            LockSupport.parkNanos(sleepMillis * 1_000_000L);
            backoffMillis = Math.min(backoffMillis * 2, maxBackoffMillis);
        }
        long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
        String lastMsg = lastError == null ? "no attempts made" : lastError.getMessage();
        throw new LineSenderException(
                contextLabel + " failed after " + elapsedMs + "ms / "
                        + attempts + " attempts: " + lastMsg,
                lastError);
    }

    /**
     * Default policy per spec § "Default category → policy". User overrides
     * (builder + connect-string) plug in here in a later commit; today this is
     * the only resolver. Exposed for unit tests.
     */
    @TestOnly
    public static SenderError.Policy defaultPolicyFor(SenderError.Category category) {
        switch (category) {
            case SCHEMA_MISMATCH:
            case WRITE_ERROR:
                return SenderError.Policy.DROP_AND_CONTINUE;
            case PARSE_ERROR:
            case INTERNAL_ERROR:
            case SECURITY_ERROR:
            case PROTOCOL_VIOLATION:
            case UNKNOWN:
            default:
                return SenderError.Policy.HALT;
        }
    }

    /**
     * True if a WebSocket close code signals an unrecoverable protocol-layer
     * violation: replaying the same bytes will produce the same close. Reserved
     * codes that "MUST NOT be sent in a Close frame" (1004/1005/1006/1015) are
     * intentionally not classified as terminal here — when they arrive in
     * practice they signal abnormal disconnect rather than the server's
     * reasoned rejection of payload bytes, so reconnect is the right reaction.
     * Exposed for unit tests.
     */
    @TestOnly
    public static boolean isTerminalCloseCode(int code) {
        switch (code) {
            case WebSocketCloseCode.PROTOCOL_ERROR:
            case WebSocketCloseCode.UNSUPPORTED_DATA:
            case WebSocketCloseCode.INVALID_PAYLOAD_DATA:
            case WebSocketCloseCode.POLICY_VIOLATION:
            case WebSocketCloseCode.MESSAGE_TOO_BIG:
            case WebSocketCloseCode.MANDATORY_EXTENSION:
                return true;
            default:
                return false;
        }
    }

    /**
     * Surfaces any error the I/O thread recorded. Called by the producer
     * thread (typically from inside its append wrapper) so failures don't
     * stay silent. Every call rethrows the exact latched instance — close()
     * relies on that identity to suppress double-signals. Idempotent; once
     * an error is set the loop has already exited.
     */
    public void checkError() {
        LineSenderException e = terminalError;
        if (e != null) {
            synchronouslySurfacedError = e;
            throw e;
        }
    }

    /**
     * Safety-net variant of {@link #checkError()} for
     * {@code QwpWebSocketSender.close()}: rethrows the latched terminal error
     * only when no synchronous caller has owned it yet. A user who already
     * caught the error from flush()/append() stays undisturbed — throwing
     * again from close() would double-signal an error they already handled.
     * A user who only ever called close() (e.g. async-initial-connect that
     * never reached the server) still gets the loud failure.
     */
    public void checkUnsurfacedError() {
        if (hasUnsurfacedError()) {
            checkError();
        }
    }

    @Override
    public synchronized void close() {
        // Synchronized on the same monitor as start(): a close() racing a
        // slow start() would otherwise read ioThread==null and skip the
        // latch await, while the I/O thread is mid-sendBinary. Holding the
        // monitor across the whole close path forces close() to either run
        // entirely before start() commits ioThread (in which case running
        // is false and start's ioLoop will exit immediately) or entirely
        // after — the latch await is only skipped when the loop never ran.
        running = false;
        Thread t = ioThread;
        if (t != null) {
            LockSupport.unpark(t);
            // Only await the shutdown latch if the I/O thread actually ran.
            // If start() failed after assigning ioThread but before t.start()
            // succeeded (e.g. native stack OOM), ioLoop never ran and its
            // finally{shutdownLatch.countDown()} never fired — awaiting here
            // would block forever. isAlive()==false also covers the normal
            // post-exit case where the latch is already counted down.
            if (t.isAlive()) {
                try {
                    shutdownLatch.await();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            ioThread = null;
        }
        // Close the current client. After a reconnect, swapClient has
        // replaced the original (and closed it); the owner only retains
        // the stale pre-reconnect reference. Without closing the live
        // client here, its native socket and fds leak past sender.close()
        // every time the loop reconnected at least once. close() is
        // idempotent, so the owner's duplicate close on its stale
        // reference is still safe.
        WebSocketClient c = client;
        if (c != null) {
            try {
                c.close();
            } catch (Throwable ignored) {
                // best-effort
            }
            client = null;
        }
    }

    /**
     * Typed server-rejection payload of the latched terminal error, or
     * {@code null} when the loop latched a wire-level failure (or nothing).
     * Derived from the latch — a server-rejection terminal is always latched
     * as a {@link LineSenderServerException} carrying its {@link SenderError}.
     */
    public SenderError getLastTerminalServerError() {
        LineSenderException e = terminalError;
        return e instanceof LineSenderServerException
                ? ((LineSenderServerException) e).getServerError() : null;
    }

    /**
     * Returns the exact latched throwable instance already thrown by
     * {@link #checkError()}, or {@code null} when no synchronous caller has
     * owned the terminal error yet.
     * <p>
     * This is the single read {@code QwpWebSocketSender.close()} uses to learn
     * which terminal error the user already owns. The ownership decision must
     * be taken from this one field only: deriving it from two separate latch
     * reads (e.g. an unsurfaced-check followed by {@link #getTerminalError()})
     * races the I/O thread — a terminal latched between the reads gets
     * mis-captured as already-owned and is then silently dropped on close().
     * Guarded by {@code CloseOwnershipRaceTest}.
     */
    public Throwable getSynchronouslySurfacedError() {
        return synchronouslySurfacedError;
    }

    /**
     * The latched terminal failure, or {@code null} while the loop is
     * healthy. Read-only — does not mark the error as surfaced.
     */
    public Throwable getTerminalError() {
        return terminalError;
    }

    public long getTotalAcks() {
        return totalAcks.get();
    }

    /**
     * Total {@code STATUS_DURABLE_ACK} frames received since the loop started.
     * Always 0 when {@code durableAckMode} is false. Useful for confirming
     * the server is actually emitting durable acks under load.
     */
    public long getTotalDurableAcks() {
        return totalDurableAcks.get();
    }

    /**
     * Total times a durable-ack frame caused {@link CursorSendEngine#acknowledge}
     * to advance. Always 0 when {@code durableAckMode} is false. A non-zero
     * value bounded below {@code getTotalDurableAcks} is normal -- many
     * durable-acks land on watermarks that don't yet cover any pending
     * entries (e.g. one of two tables has caught up but the other has not).
     */
    public long getTotalDurableTrimAdvances() {
        return totalDurableTrimAdvances.get();
    }

    /**
     * Cumulative count of frames re-sent during post-reconnect catch-up
     * windows. One increment per replayed frame. Zero in steady state; a
     * sustained nonzero rate signals flapping where every reconnect replays
     * meaningful work.
     */
    public long getTotalFramesReplayed() {
        return totalFramesReplayed.get();
    }

    public long getTotalFramesSent() {
        return totalFramesSent.get();
    }

    /**
     * Total reconnect attempts (succeeded + failed).
     */
    public long getTotalReconnectAttempts() {
        return totalReconnectAttempts.get();
    }

    public long getTotalReconnects() {
        return totalReconnects.get();
    }

    /**
     * Total server-side rejection frames observed since the loop started. Counts both
     * DROP_AND_CONTINUE and HALT outcomes — every non-OK frame the server sent that
     * the client classified as a {@link SenderError}.
     */
    public long getTotalServerErrors() {
        return totalServerErrors.get();
    }

    /**
     * True iff the I/O loop has at least once installed a live (connected
     * + upgraded) WebSocket client. Sticky — once true, stays true even
     * after a subsequent disconnect. Lets a {@code SenderErrorHandler}
     * disambiguate a "never reached the server" budget exhaustion (likely
     * a config typo or firewall block) from a "lost connection after we
     * were up" failure (likely transient).
     */
    public boolean hasEverConnected() {
        return hasEverConnected;
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * Plug an async-delivery sink for {@link SenderConnectionEvent}
     * notifications. The loop fires {@code RECONNECT_BUDGET_EXHAUSTED}
     * through this sink when {@code connectLoop} gives up; other connection
     * events fire from {@code QwpWebSocketSender.buildAndConnect} directly
     * into the same dispatcher. Same lifecycle contract as
     * {@link #setErrorDispatcher}.
     */
    public void setConnectionDispatcher(SenderConnectionDispatcher dispatcher) {
        this.connectionDispatcher = dispatcher;
    }

    /**
     * Plug an async-delivery sink for {@link SenderError} notifications.
     * Idempotent — set once before {@link #start()}; later reassignment is
     * permitted but races between dispatchers are the caller's problem.
     */
    public void setErrorDispatcher(SenderErrorDispatcher dispatcher) {
        this.errorDispatcher = dispatcher;
    }

    /**
     * Plug an async-delivery sink for ack-watermark advances. Same lifecycle
     * contract as {@link #setErrorDispatcher} — set once before
     * {@link #start()}.
     */
    public void setProgressDispatcher(SenderProgressDispatcher dispatcher) {
        this.progressDispatcher = dispatcher;
    }

    public synchronized void start() {
        if (ioThread != null) {
            throw new IllegalStateException("already started");
        }
        running = true;
        // Position the cursor at the first unsent FSN before spinning the
        // I/O thread. For a fresh sender, ackedFsn=-1 → start at FSN 0,
        // which lands on the (empty) initial active — same as the prior
        // hardcoded "sendingSegment = engine.activeSegment()". For a
        // recovered sender with sealed segments holding unsent data, this
        // walks back to the lowest unacked frame so sealed-segment data
        // actually reaches the wire — without it, start() would skip
        // straight to the active and orphan everything in sealed.
        positionCursorForStart();
        Thread t = new Thread(this::ioLoop, "qdb-cursor-ws-io");
        t.setDaemon(true);
        try {
            t.start();
        } catch (Throwable th) {
            // Thread.start() failed (e.g. native stack alloc OOM). ioLoop
            // never ran, so its finally{shutdownLatch.countDown()} never
            // fires. Release the latch and reset state so a subsequent
            // close() doesn't block on a thread that doesn't exist.
            running = false;
            shutdownLatch.countDown();
            throw th;
        }
        // Commit ioThread only after t.start() succeeded — otherwise close()
        // would observe a non-null ioThread for a thread that never ran.
        ioThread = t;
    }

    private PendingDurableEntry acquirePendingEntry() {
        PendingDurableEntry e = pendingDurablePool.pollFirst();
        return e != null ? e : new PendingDurableEntry();
    }

    /**
     * Walks to the next segment when the current one is sealed and fully
     * drained. Returns the next segment to consume (newer sealed if available,
     * else the active). Returns the same segment if it's still being written
     * (we're on the active and just need to wait for more publishedFsn).
     * <p>
     * Uses {@link CursorSendEngine#nextSealedAfter} so we never have to
     * snapshot the full sealed list — important when the producer outpaces
     * the I/O thread and the sealed list can grow to thousands of entries
     * (cursor SF lets the producer fan out at memory speed; the wire path
     * catches up at WebSocket speed).
     */
    private MmapSegment advanceSegment() {
        MmapSegment current = sendingSegment;
        MmapSegment liveActive = engine.activeSegment();
        if (current == liveActive) {
            // We're on the active — there's no "next", just wait for more
            // bytes to be published into it. Caller's sendOne will see
            // publishedOffset > sendOffset eventually and resume.
            return current;
        }
        sendOffset = MmapSegment.HEADER_SIZE;
        MmapSegment next = engine.nextSealedAfter(current);
        if (next != null) {
            return next;
        }
        // current was the newest sealed (no later sealed exists). If it's
        // still in the sealed list, the next segment must be the active;
        // if it's been trimmed out from under us, fall back to the oldest
        // remaining sealed before resorting to the active.
        next = engine.firstSealed();
        if (next != null && next.baseSeq() > current.baseSeq()) {
            return next;
        }
        return liveActive;
    }

    private void applyDurableAck() {
        // Update per-table watermarks from the inbound frame, taking the
        // max so a reordered or older cumulative frame can't move a watermark
        // backwards. Then walk the head of pendingDurable, popping every
        // entry whose tables are all covered. The map's NO_ENTRY_VALUE
        // sentinel is -1L; valid seqTxns are non-negative, so the guard
        // doubles as an "absent" check.
        int n = response.getTableEntryCount();
        for (int i = 0; i < n; i++) {
            String name = response.getTableName(i);
            long seqTxn = response.getTableSeqTxn(i);
            long current = durableTableWatermarks.get(name);
            if (seqTxn > current) {
                durableTableWatermarks.put(name, seqTxn);
            }
        }
        drainPendingDurable();
    }

    /**
     * Drives the very first connect attempt on the I/O thread, used in the
     * async-initial-connect mode (constructed with {@code client == null}).
     * Reuses the same retry+backoff machinery as {@link #fail(Throwable)} —
     * a terminal upgrade reject or budget exhaustion is delivered through
     * the dispatcher, not thrown to the producer.
     */
    private void attemptInitialConnect() {
        connectLoop(new LineSenderException(
                        "async initial connect deferred to I/O thread"),
                "initial connect");
    }

    private void clearDurableAckTracking() {
        if (!durableAckMode) {
            return;
        }
        while (!pendingDurable.isEmpty()) {
            releasePendingEntry(pendingDurable.pollFirst());
        }
        durableTableWatermarks.clear();
        // Reset the keepalive throttle so the new connection can prod the
        // server immediately rather than waiting out the leftover interval
        // from before the reconnect.
        lastFrameOrPingNanos = 0L;
    }

    /**
     * Shared per-outage retry loop. Used by {@link #fail(Throwable)} for
     * mid-flight wire failures (phase="reconnect") and by
     * {@link #attemptInitialConnect()} for the async-initial-connect path
     * (phase="initial connect"). The phase string only affects log lines
     * and the {@link SenderError} message — control flow is identical.
     */
    private void connectLoop(Throwable initial, String phase) {
        if (reconnectFactory == null || !running) {
            recordFatal(initial);
            return;
        }
        LOG.warn("cursor I/O loop entering {} loop: {}",
                phase, initial.getMessage());
        long outageStartNanos = System.nanoTime();
        long deadlineNanos = outageStartNanos + reconnectMaxDurationMillis * 1_000_000L;
        long backoffMillis = reconnectInitialBackoffMillis;
        int attempts = 0;
        long lastLogNanos = 0L;
        Throwable lastReconnectError = initial;
        while (running && System.nanoTime() < deadlineNanos) {
            attempts++;
            totalReconnectAttempts.incrementAndGet();
            try {
                WebSocketClient newClient = reconnectFactory.reconnect();
                if (newClient != null) {
                    swapClient(newClient);
                    totalReconnects.incrementAndGet();
                    long elapsedMs = (System.nanoTime() - outageStartNanos) / 1_000_000L;
                    LOG.info("cursor I/O loop {} succeeded after {}ms, {} attempts; "
                                    + "replaying from FSN {}",
                            phase, elapsedMs, attempts, fsnAtZero);
                    return;
                }
            } catch (QwpAuthFailedException | WebSocketUpgradeException e) {
                // Terminal across all configured endpoints per spec sf-client.md
                // section 13.3: auth (401/403) bypasses reconnect and surfaces as
                // SECURITY_ERROR. WebSocketUpgradeException reaching here is always
                // non-421: QwpUpgradeFailures.classify upstream converts a
                // 421-with-X-QuestDB-Role to QwpIngressRoleRejectedException, and a
                // 421 without that header walks the transport-error path in
                // buildAndConnect and lands as a LineSenderException, falling into
                // the Throwable branch below.
                LOG.error("terminal upgrade error during {} -- won't retry: {}",
                        phase, e.getMessage());
                long fromFsn = engine.ackedFsn() + 1L;
                long toFsn = Math.max(fromFsn, engine.publishedFsn());
                SenderError err = new SenderError(
                        SenderError.Category.SECURITY_ERROR,
                        SenderError.Policy.HALT,
                        SenderError.NO_STATUS_BYTE,
                        "ws-upgrade-failed: " + e.getMessage(),
                        SenderError.NO_MESSAGE_SEQUENCE,
                        fromFsn,
                        toFsn,
                        null,
                        System.nanoTime()
                );
                totalServerErrors.incrementAndGet();
                recordFatal(new LineSenderServerException(err));
                dispatchError(err);
                return;
            } catch (QwpDurableAckMismatchException e) {
                // Per spec sf-client.md section 8.1: the client opted into durable
                // ack but the cluster cannot honour it. Loud fail at connect rather
                // than silently waiting for ack frames that will never arrive.
                // Classified as PROTOCOL_VIOLATION (config/capability mismatch),
                // not SECURITY_ERROR -- this is not an auth failure.
                LOG.error("durable-ack mismatch during {} -- won't retry: {}",
                        phase, e.getMessage());
                long fromFsn = engine.ackedFsn() + 1L;
                long toFsn = Math.max(fromFsn, engine.publishedFsn());
                SenderError err = new SenderError(
                        SenderError.Category.PROTOCOL_VIOLATION,
                        SenderError.Policy.HALT,
                        SenderError.NO_STATUS_BYTE,
                        "durable-ack-mismatch: " + e.getMessage(),
                        SenderError.NO_MESSAGE_SEQUENCE,
                        fromFsn,
                        toFsn,
                        null,
                        System.nanoTime()
                );
                totalServerErrors.incrementAndGet();
                recordFatal(new LineSenderServerException(err));
                dispatchError(err);
                return;
            } catch (QwpRoleMismatchException | QwpIngressRoleRejectedException e) {
                // Role mismatch: cluster reconfigured during this connect, the
                // previously-writable endpoint is now read-only. Reset backoff
                // (don't double on each role reject -- failover usually clears
                // within seconds) and park for the initial interval before the
                // next attempt.
                backoffMillis = reconnectInitialBackoffMillis;
                lastReconnectError = e;
                if (running) {
                    long remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0L) {
                        break;
                    }
                    long parkNanos = Math.min(reconnectInitialBackoffMillis * 1_000_000L, remainingNanos);
                    LockSupport.parkNanos(parkNanos);
                }
                continue;
            } catch (Throwable e) {
                lastReconnectError = e;
                long now = System.nanoTime();
                if (now - lastLogNanos >= RECONNECT_LOG_THROTTLE_NANOS) {
                    LOG.warn("{} attempt {} failed: {}", phase, attempts, e.getMessage());
                    lastLogNanos = now;
                }
            }
            if (running) {
                long jitter = ThreadLocalRandom.current().nextLong(backoffMillis);
                long sleepMillis = backoffMillis + jitter;
                long remainingMillis = (deadlineNanos - System.nanoTime()) / 1_000_000L;
                if (remainingMillis <= 0) {
                    break;
                }
                if (sleepMillis > remainingMillis) {
                    sleepMillis = remainingMillis;
                }
                LockSupport.parkNanos(sleepMillis * 1_000_000L);
                backoffMillis = Math.min(backoffMillis * 2, reconnectMaxBackoffMillis);
            }
        }
        long elapsedMs = (System.nanoTime() - outageStartNanos) / 1_000_000L;
        String lastMsg = lastReconnectError.getMessage();
        LOG.error("cursor I/O loop giving up {} after {}ms, {} attempts; last error: {}",
                phase, elapsedMs, attempts, lastMsg);
        long fromFsn = engine.ackedFsn() + 1L;
        long toFsn = Math.max(fromFsn, engine.publishedFsn());
        // Disambiguate by what the sender saw on the wire: if we never got
        // a successful upgrade, the user is most likely looking at a config
        // problem (typo in addr, wrong port, firewall, server not deployed
        // yet); if we connected at least once and then exhausted the budget,
        // it's a transient connectivity issue (server down, network flap).
        // Tag and free-text hint encode the same signal so both grep-the-logs
        // and read-the-message users get it without parsing.
        String connectivityTag;
        String connectivityHint;
        if (hasEverConnected) {
            connectivityTag = "connection-lost-budget-exhausted";
            connectivityHint = "server unreachable since last connect (transient)";
        } else {
            connectivityTag = "never-connected-budget-exhausted";
            connectivityHint = "never reached the server (check addr/port/firewall)";
        }
        SenderError err = new SenderError(
                SenderError.Category.PROTOCOL_VIOLATION,
                SenderError.Policy.HALT,
                SenderError.NO_STATUS_BYTE,
                connectivityTag + ": " + elapsedMs + "ms / " + attempts
                        + " attempts; " + connectivityHint
                        + "; last error: " + lastMsg,
                SenderError.NO_MESSAGE_SEQUENCE,
                fromFsn,
                toFsn,
                null,
                System.nanoTime()
        );
        totalServerErrors.incrementAndGet();
        // recordFatal MUST run before dispatchError so the producer-observable
        // terminal error is latched before the handler is invoked.
        recordFatal(new LineSenderServerException(err));
        dispatchError(err);
        // Surface the terminal classification through the connection-event
        // dispatcher too. Listeners learn about budget exhaustion without
        // having to also subscribe to SenderError. Fire AFTER recordFatal so
        // a listener that immediately checks the producer-side terminal state
        // sees a consistent picture.
        SenderConnectionDispatcher cd = connectionDispatcher;
        if (cd != null) {
            cd.offer(new SenderConnectionEvent(
                    SenderConnectionEvent.Kind.RECONNECT_BUDGET_EXHAUSTED,
                    null, SenderConnectionEvent.NO_PORT,
                    null, SenderConnectionEvent.NO_PORT,
                    attempts,
                    SenderConnectionEvent.NO_ROUND_NUMBER,
                    lastReconnectError,
                    System.currentTimeMillis()));
        }
    }

    /**
     * Send {@code err} to the async-delivery dispatcher if one is configured.
     * Producer-side typed throw (HALT) goes through {@code recordFatal} +
     * {@code checkError} regardless — this is purely the async observer path.
     */
    private void dispatchError(SenderError err) {
        SenderErrorDispatcher d = errorDispatcher;
        if (d != null) {
            d.offer(err);
        }
    }

    /**
     * Notify the progress dispatcher that the ack watermark advanced to
     * {@code ackedFsn}. Caller must already have observed the advance via
     * {@link CursorSendEngine#acknowledge}'s boolean return; this method
     * does no further filtering.
     */
    private void dispatchProgress(long ackedFsn) {
        SenderProgressDispatcher d = progressDispatcher;
        if (d != null) {
            d.offer(ackedFsn);
        }
    }

    /**
     * Pop every head entry whose tables are all covered by the durable
     * watermarks and call {@link CursorSendEngine#acknowledge} once with
     * the highest popped wireSeq. Trivially-durable entries (tableCount=0,
     * from empty-WAL OK frames or NACK frames) pop unconditionally.
     */
    private void drainPendingDurable() {
        long highest = Long.MIN_VALUE;
        while (!pendingDurable.isEmpty()) {
            PendingDurableEntry head = pendingDurable.peekFirst();
            if (!head.isDurableUnder(durableTableWatermarks)) {
                break;
            }
            highest = head.wireSeq;
            releasePendingEntry(pendingDurable.pollFirst());
        }
        if (highest != Long.MIN_VALUE) {
            long fsn = fsnAtZero + highest;
            if (engine.acknowledge(fsn)) {
                totalDurableTrimAdvances.incrementAndGet();
                dispatchProgress(fsn);
            }
        }
    }

    /**
     * Stash a wireSeq + per-table seqTxns from the current OK / NACK frame
     * for later durable-ack confirmation. {@link #response} must hold the
     * OK or rejection frame at call time. NACK frames carry no per-table
     * entries, so they enqueue as trivially-durable empty placeholders.
     */
    private void enqueuePendingOk(long wireSeq) {
        PendingDurableEntry e = acquirePendingEntry();
        e.wireSeq = wireSeq;
        int n = response.getTableEntryCount();
        e.ensureCapacity(n);
        for (int i = 0; i < n; i++) {
            e.tableNames[i] = response.getTableName(i);
            e.seqTxns[i] = response.getTableSeqTxn(i);
        }
        e.tableCount = n;
        pendingDurable.addLast(e);
    }

    /**
     * Surface a wire failure. With reconnect plumbing wired (factory +
     * listener both non-null), enters the per-outage retry loop:
     * exponential backoff with jitter, time-capped at
     * {@code reconnectMaxDurationMillis}, terminal on auth/upgrade
     * rejections (so the budget isn't burned on errors that won't fix
     * themselves). On the first successful reconnect within the budget,
     * the I/O loop resumes with reset wire state and replays from
     * {@code engine.ackedFsn() + 1}.
     * <p>
     * Without reconnect plumbing, the failure is immediately terminal
     * (legacy behavior).
     */
    private void fail(Throwable initial) {
        connectLoop(initial, "reconnect");
    }

    /**
     * True when {@link #terminalError} is set AND no synchronous user-thread
     * caller has yet seen that same instance via {@link #checkError()}.
     * The {@link #checkUnsurfacedError()} safety net composes this with
     * checkError(); reads {@code terminalError} once so the comparison cannot
     * tear against a concurrent latch.
     */
    private boolean hasUnsurfacedError() {
        Throwable e = terminalError;
        return e != null && synchronouslySurfacedError != e;
    }

    private void ioLoop() {
        try {
            // Async-initial-connect path: ctor accepted a null client because
            // a reconnect factory is wired. Drive the very first connect on
            // this thread so the producer thread never blocks on it.
            // attemptInitialConnect either sets `client` (success) or records
            // a terminal failure and clears `running` (auth/upgrade reject or
            // budget exhaustion). Either way, the main loop below sees the
            // outcome via the `running` and `client` fields.
            if (client == null && running) {
                attemptInitialConnect();
            }
            while (running) {
                boolean didWork = trySendOne();
                // 1. Try to send next frame(s).
                // 2. Try to receive ACKs.
                if (tryReceiveAcks()) {
                    didWork = true;
                }
                // 3. In durable-ack mode, prod the server with a keepalive
                //    PING when there are pending OKs awaiting confirmation
                //    and we have nothing else to do. The server only flushes
                //    durable-ack frames on inbound traffic, so an idle
                //    client otherwise never sees the WAL-upload completion.
                //    Skipped entirely when the user has disabled the
                //    keepalive (interval <= 0).
                if (!didWork && running && durableAckMode
                        && durableAckKeepaliveIntervalNanos > 0L
                        && !pendingDurable.isEmpty()) {
                    sendDurableAckKeepaliveIfDue();
                }
                if (!didWork && running) {
                    LockSupport.parkNanos(parkNanos);
                }
            }
        } catch (Throwable t) {
            fail(t);
        } finally {
            shutdownLatch.countDown();
        }
    }

    /**
     * Walk the engine's segments to find the one containing {@code targetFsn},
     * and set {@code sendOffset} to the byte offset of that frame within it.
     * This is called at startup and after every reconnect, after fsnAtZero has
     * already been reset to {@code targetFsn} and nextWireSeq to 0.
     * <p>
     * If {@code targetFsn} is already published, the method positions the byte
     * cursor exactly at that frame. If {@code targetFsn} is not published yet,
     * the method positions at the active segment's current tip; the normal send
     * loop will then wait until the producer publishes more bytes.
     */
    private void positionCursorAt(long targetFsn) {
        MmapSegment seg = engine.findSegmentContaining(targetFsn);
        if (seg == null) {
            // No segment currently advertises targetFsn. That normally means
            // targetFsn is just past publishedFsn and there is nothing to
            // replay yet, so the cursor should resume from the active tip.
            //
            // The producer is concurrent with this I/O thread, though. It can
            // publish targetFsn after the first findSegmentContaining() returns
            // null but before or during the active-tip snapshot below.
            sendingSegment = engine.activeSegment();
            sendOffset = sendingSegment.publishedOffset();
            // The publishedOffset read is the producer's volatile publish
            // barrier. If it saw the new frame bytes, the frameCount write that
            // makes targetFsn discoverable is also visible, so a second lookup
            // must now find it. If the producer publishes later, sendOffset is
            // still at the old tip and trySendOne() will send the frame normally.
            seg = engine.findSegmentContaining(targetFsn);
            if (seg != null) {
                positionCursorInSegment(seg, targetFsn);
            }
            return;
        }
        positionCursorInSegment(seg, targetFsn);
    }

    /**
     * Position the byte cursor inside a known segment by scanning frame lengths
     * from that segment's first frame. MmapSegment frame boundaries are not
     * indexed, so landing on FSN N means walking payload lengths from baseSeq
     * until the desired FSN is reached.
     */
    private void positionCursorInSegment(MmapSegment seg, long targetFsn) {
        sendingSegment = seg;
        // Walk frame-by-frame from HEADER_SIZE until we land on targetFsn.
        long offset = MmapSegment.HEADER_SIZE;
        long fsn = seg.baseSeq();
        long base = seg.address();
        while (fsn < targetFsn) {
            int payloadLen = Unsafe.getUnsafe().getInt(base + offset + 4);
            offset += MmapSegment.FRAME_HEADER_SIZE + payloadLen;
            fsn++;
        }
        sendOffset = offset;
    }

    /**
     * Mark the loop as fatally failed. Caller has decided no reconnect
     * is possible (or it ran out of budget) — latch the error so
     * {@link #checkError} can surface it to the producer thread, then
     * stop the loop. First-writer-wins: only the first failure latches.
     * The check-then-latch is unsynchronized and is safe ONLY because
     * every caller runs on the I/O thread (connectLoop and the
     * receive-path rejection handlers are all pumped by ioLoop); calling
     * this from any other thread would be a lost-update race.
     * Non-{@link LineSenderException} causes are wrapped once here, so
     * every rethrow delivers the same instance.
     */
    private void recordFatal(Throwable t) {
        if (terminalError == null) {
            terminalError = t instanceof LineSenderException
                    ? (LineSenderException) t
                    : new LineSenderException("I/O thread failed: " + t.getMessage(), t);
        }
        running = false;
        if (t instanceof LineSenderServerException) {
            // server rejections carry a structured message; the stack adds noise
            LOG.error("Cursor I/O loop failure: {}", t.getMessage());
        } else {
            LOG.error("Cursor I/O loop failure: {}", t.getMessage(), t);
        }
    }

    private void releasePendingEntry(PendingDurableEntry e) {
        if (e == null) return;
        e.tableCount = 0;
        // Null out name references so released entries don't pin Strings
        // alive across reconnects. Length is small, so the loop cost is
        // negligible compared to the indirect tenuring savings.
        if (e.tableNames != null) {
            Arrays.fill(e.tableNames, null);
        }
        pendingDurablePool.addFirst(e);
    }

    /**
     * Send a WebSocket PING to prod the server into flushing pending
     * STATUS_DURABLE_ACK frames, but only when the throttle interval has
     * elapsed since the last outbound activity -- a sent frame or a prior
     * keepalive PING. The server's egress code only runs flushPendingAck on
     * inbound recv events; without this prod, an idle connection waiting on
     * durable-ack confirmation can sit forever. The "last sent frame" half
     * of the gate avoids a redundant PING shortly after a producer batch
     * goes idle: the recent frame already triggered a server-side flush.
     * <p>
     * Best-effort: any send failure routes through the standard fail() path
     * so the reconnect loop can take over. Caller is responsible for the
     * "do we even need to send" gate (durableAckMode + non-empty pending).
     */
    private void sendDurableAckKeepaliveIfDue() {
        long now = System.nanoTime();
        if (now - lastFrameOrPingNanos < durableAckKeepaliveIntervalNanos) {
            return;
        }
        lastFrameOrPingNanos = now;
        try {
            client.sendPing(1000);
        } catch (Throwable t) {
            fail(t);
        }
    }

    /**
     * Reset wire state for a fresh connection: install the new client,
     * realign {@code fsnAtZero} to the next unacked FSN, restart wire
     * sequencing from 0, and reposition the cursor so the next
     * {@link #trySendOne} call replays the first unacked frame.
     */
    private void swapClient(WebSocketClient newClient) {
        WebSocketClient old = this.client;
        this.client = newClient;
        // Sticky: once the wire is up, we've reached the server at least
        // once for this sender's lifetime. Used downstream to classify a
        // subsequent budget exhaustion as transient vs config-likely.
        this.hasEverConnected = true;
        if (old != null) {
            try {
                old.close();
            } catch (Throwable ignored) {
                // best-effort
            }
        }
        long replayStart = engine.ackedFsn() + 1L;
        this.fsnAtZero = replayStart;
        this.nextWireSeq = 0L;
        // Snapshot publishedFsn at swap time — frames at FSN ≤ this value
        // were already on the wire before the drop and will be replayed.
        // trySendOne resets replayTargetFsn to -1 once we cross the boundary.
        long pubAtSwap = engine.publishedFsn();
        this.replayTargetFsn = pubAtSwap >= replayStart ? pubAtSwap : -1L;
        // Drop any durable-ack tracking from the previous connection. The
        // new connection will re-OK every replayed batch and the server
        // re-emits cumulative durable-ack watermarks from scratch, so
        // carrying stale state across the wire boundary would either
        // double-trim or starve the queue.
        clearDurableAckTracking();
        positionCursorAt(replayStart);
    }

    private boolean tryReceiveAcks() {
        boolean any = false;
        try {
            while (running && client.tryReceiveFrame(responseHandler)) {
                any = true;
            }
        } catch (Throwable t) {
            fail(t);
        }
        return any;
    }

    /**
     * Returns true if at least one frame was sent (caller skips the park).
     * Bounded: sends at most one frame per call so the ACK side gets
     * scheduling fairness.
     */
    private boolean trySendOne() {
        long pub = sendingSegment.publishedOffset();
        if (sendOffset >= pub) {
            // Nothing more in the current segment. If it's a sealed segment
            // (no longer the live active), advance to the next one.
            if (sendingSegment != engine.activeSegment()) {
                // The producer can publish the current segment's last frame
                // between our first publishedOffset() read and the active
                // segment check above. Re-read before leaving the segment, or
                // that frame is skipped permanently.
                pub = sendingSegment.publishedOffset();
                if (sendOffset >= pub) {
                    MmapSegment next = advanceSegment();
                    if (next != sendingSegment) {
                        sendingSegment = next;
                        return true; // let the next iteration try sending
                    }
                }
            } else {
                return false;
            }
        }
        // At least the frame header is published; check we have the full frame.
        if (sendOffset + MmapSegment.FRAME_HEADER_SIZE > pub) {
            return false;
        }
        long base = sendingSegment.address();
        // Frame layout: [u32 crc][u32 payloadLen][payload].
        int payloadLen = Unsafe.getUnsafe().getInt(base + sendOffset + 4);
        if (payloadLen < 0) {
            fail(new LineSenderException(
                    "negative payloadLen at offset " + sendOffset
                            + " in segment baseSeq=" + sendingSegment.baseSeq()));
            return false;
        }
        long frameEnd = sendOffset + MmapSegment.FRAME_HEADER_SIZE + payloadLen;
        if (frameEnd > pub) {
            return false; // payload not fully published yet
        }
        try {
            client.sendBinary(base + sendOffset + MmapSegment.FRAME_HEADER_SIZE, payloadLen);
        } catch (Throwable t) {
            fail(t);
            return false;
        }
        lastFrameOrPingNanos = System.nanoTime();
        sendOffset = frameEnd;
        long fsnSent = fsnAtZero + nextWireSeq;
        nextWireSeq++;
        totalFramesSent.incrementAndGet();
        if (replayTargetFsn >= 0) {
            totalFramesReplayed.incrementAndGet();
            if (fsnSent >= replayTargetFsn) {
                replayTargetFsn = -1L; // catch-up complete
            }
        }
        return true;
    }

    /**
     * Sets {@code fsnAtZero}, {@code nextWireSeq}, and the cursor
     * (sendingSegment + sendOffset) to the first unsent FSN. Visible for
     * tests so they can assert correct positioning without spinning a
     * real I/O thread + WebSocket.
     */
    void positionCursorForStart() {
        long replayStart = engine.ackedFsn() + 1L;
        this.fsnAtZero = replayStart;
        this.nextWireSeq = 0L;
        positionCursorAt(replayStart);
    }

    /**
     * Factory used by the I/O loop to build a fresh, connected, upgraded
     * {@link WebSocketClient} after a wire failure. Implementations close
     * the old client (if needed), build a new one with the same auth/TLS
     * config, connect, perform the WebSocket upgrade, and return it ready
     * to send. Throw on a terminal failure (auth rejection, etc.) — the
     * I/O loop will treat the throw as fatal.
     */
    @FunctionalInterface
    public interface ReconnectFactory {
        WebSocketClient reconnect() throws Exception;
    }

    /**
     * One slot in the pendingDurable FIFO. Holds a wireSeq plus the per-table
     * (name, seqTxn) pairs from its OK frame. Empty entries (tableCount = 0)
     * represent batches that committed nothing to a WAL table -- spec defines
     * them as trivially durable as soon as preceding entries are durable.
     * <p>
     * Reused via the loop's pendingDurablePool to keep steady-state allocation
     * confined to capacity growth.
     */
    private static final class PendingDurableEntry {
        long[] seqTxns;
        int tableCount;
        String[] tableNames;
        long wireSeq;

        void ensureCapacity(int n) {
            if (tableNames == null || tableNames.length < n) {
                int newCap = Math.max(n, tableNames == null ? 4 : tableNames.length * 2);
                tableNames = new String[newCap];
                seqTxns = new long[newCap];
            }
        }

        boolean isDurableUnder(CharSequenceLongHashMap watermarks) {
            for (int i = 0; i < tableCount; i++) {
                // NO_ENTRY_VALUE is -1L; valid seqTxns are non-negative, so
                // a single comparison covers both "absent" and "behind".
                if (watermarks.get(tableNames[i]) < seqTxns[i]) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * Inner ACK handler — parses the binary frame, calls engine.acknowledge.
     */
    private final class ResponseHandler implements WebSocketFrameHandler {
        @Override
        public void onBinaryMessage(long payloadPtr, int payloadLen) {
            if (!response.readFrom(payloadPtr, payloadLen)) {
                fail(new LineSenderException(
                        "Invalid ACK response payload [length=" + payloadLen + ']'));
                return;
            }
            long wireSeq = response.getSequence();
            if (response.isSuccess()) {
                // Same sanity clamp as legacy: don't trust an ACK beyond
                // what we've actually sent, otherwise a malformed/replayed
                // server response would force trim of segments the new
                // server hasn't seen.
                long highestSent = nextWireSeq - 1;
                if (highestSent < 0) return; // ACK before any send — ignore
                long capped = Math.min(wireSeq, highestSent);
                if (capped < wireSeq) {
                    LOG.warn("server ACK wire seq {} exceeds highest sent {}, clamping",
                            wireSeq, highestSent);
                }
                totalAcks.incrementAndGet();
                if (durableAckMode) {
                    // Durable mode: stash the (wireSeq, table_seqTxns) tuple
                    // and wait for STATUS_DURABLE_ACK to release it. Empty
                    // OK frames (tableCount=0) are trivially durable per
                    // spec, but they still chain behind any earlier
                    // non-empty entries -- the queue keeps wireSeq order.
                    // Drain on enqueue too: when a durable-ack arrived ahead
                    // of an empty / already-covered OK, the queued entry
                    // would otherwise wait for the next durable-ack to
                    // drain. Calling drain here is O(coverage) and keeps
                    // ackedFsn current with no extra wire round-trip.
                    enqueuePendingOk(capped);
                    drainPendingDurable();
                    return;
                }
                if (engine.acknowledge(fsnAtZero + capped)) {
                    dispatchProgress(fsnAtZero + capped);
                }
                return;
            }
            if (response.isDurableAck()) {
                if (!durableAckMode) {
                    // Spec contract: servers must not emit STATUS_DURABLE_ACK
                    // unless the client opted in. Treat as a server bug and
                    // log it once -- ignoring is safer than failing the
                    // connection over what is, in the worst case, a stray
                    // informational frame.
                    LOG.warn("received STATUS_DURABLE_ACK frame without opt-in -- ignoring");
                    return;
                }
                totalDurableAcks.incrementAndGet();
                applyDurableAck();
                return;
            }
            // Application-layer rejection by the server. Classify by status
            // byte → SenderError.Category, resolve policy (default mapping
            // for now; user-override resolution lands in a later commit),
            // dispatch.
            handleServerRejection(wireSeq);
        }

        @Override
        public void onClose(int code, String reason) {
            // Terminal close codes signal the server has rejected the wire
            // bytes themselves — reconnecting and replaying the same bytes
            // produces the same close. Stash a typed PROTOCOL_VIOLATION
            // SenderError and halt directly. Reconnect-eligible codes
            // (NORMAL_CLOSURE, GOING_AWAY, ABNORMAL_CLOSURE, etc.) still go
            // through fail() so the reconnect retry loop can handle them.
            if (isTerminalCloseCode(code)) {
                long fromFsn = engine.ackedFsn() + 1L;
                long toFsn = Math.max(fromFsn, engine.publishedFsn());
                String msg = "ws-close[" + code + " " + WebSocketCloseCode.describe(code)
                        + "]: " + reason;
                SenderError err = new SenderError(
                        SenderError.Category.PROTOCOL_VIOLATION,
                        SenderError.Policy.HALT,
                        SenderError.NO_STATUS_BYTE,
                        msg,
                        SenderError.NO_MESSAGE_SEQUENCE,
                        fromFsn,
                        toFsn,
                        null,
                        System.nanoTime()
                );
                totalServerErrors.incrementAndGet();
                // recordFatal MUST run before dispatchError so the producer-
                // observable terminal error is latched before the handler is
                // invoked.
                recordFatal(new LineSenderServerException(err));
                dispatchError(err);
                return;
            }
            fail(new LineSenderException(
                    "WebSocket closed by server: code=" + code + " reason=" + reason));
        }

        private void handlePreSendRejection(long wireSeq, byte status,
                                            SenderError.Category category,
                                            SenderError.Policy policy) {
            LOG.warn("server rejection wire seq {} (category={}, status=0x{}) before any send -- skipping ack advance",
                    wireSeq, category, Integer.toHexString(status & 0xFF));
            // Use the same [ackedFsn+1, publishedFsn] span the
            // protocol-violation close path uses (see onClose above): there
            // is no FSN we can attribute the rejection to, so we report
            // the unacked range the producer can correlate against.
            long fromFsn = engine.ackedFsn() + 1L;
            long toFsn = Math.max(fromFsn, engine.publishedFsn());
            String tableName = response.getTableEntryCount() == 1
                    ? response.getTableName(0)
                    : null;
            SenderError err = new SenderError(
                    category,
                    policy,
                    status & 0xFF,
                    response.getErrorMessage(),
                    wireSeq,
                    fromFsn,
                    toFsn,
                    tableName,
                    System.nanoTime()
            );
            totalServerErrors.incrementAndGet();
            if (policy == SenderError.Policy.HALT) {
                // Latch the typed terminal error before invoking the handler
                // so a synchronous probe of getLastTerminalError() / flush()
                // from inside the handler observes the typed error. Mirrors
                // the ordering in the post-send HALT path below.
                recordFatal(new LineSenderServerException(err));
            }
            // DROP_AND_CONTINUE: no watermark advance -- there is nothing
            // sent on this connection to drop. The dispatch is the user's
            // only handle to the server's complaint.
            dispatchError(err);
        }

        private void handleServerRejection(long wireSeq) {
            byte status = response.getStatus();
            SenderError.Category category = classify(status);
            SenderError.Policy policy = defaultPolicyFor(category);
            // Same sanity clamp as the success branch above: do not trust a
            // rejection wireSeq beyond what we've actually sent. Without this
            // clamp the DROP path advances ackedFsn past publishedFsn, which
            // makes the segment manager trim sealed segments the I/O thread
            // is still reading — and the next Unsafe.getInt SEGVs the JVM.
            long highestSent = nextWireSeq - 1L;
            if (highestSent < 0L) {
                // Pre-send rejection: server emitted an error frame before
                // we sent anything on this connection (typical after a
                // fresh swapClient — auth failure, server-initiated halt,
                // etc.). The server-named wireSeq does not correspond to
                // any frame we sent, so clamping it to 0 and acknowledging
                // fsnAtZero would silently advance ackedFsn past a real
                // unsent batch (fsnAtZero == ackedFsn + 1 right after a
                // swap). Skip the watermark advance entirely; still surface
                // the error so the user's handler sees it and HALT errors
                // remain producer-observable.
                handlePreSendRejection(wireSeq, status, category, policy);
                return;
            }
            long cappedSeq = Math.max(0L, Math.min(wireSeq, highestSent));
            if (cappedSeq < wireSeq) {
                LOG.warn("server NACK wire seq {} exceeds highest sent {}, clamping",
                        wireSeq, highestSent);
            }
            long fsn = fsnAtZero + cappedSeq;
            // Best-effort table attribution: the parser populates
            // response.tableNames on error frames the same way it does on
            // STATUS_OK. If exactly one table was named, surface it; if
            // zero or many, leave null (multi-table batch or unattributable).
            String tableName = response.getTableEntryCount() == 1
                    ? response.getTableName(0)
                    : null;
            SenderError err = new SenderError(
                    category,
                    policy,
                    status & 0xFF,
                    response.getErrorMessage(),
                    wireSeq,
                    fsn,
                    fsn,
                    tableName,
                    System.nanoTime()
            );
            totalServerErrors.incrementAndGet();

            if (policy == SenderError.Policy.HALT) {
                // Terminal: stash the typed payload BEFORE dispatching to the
                // handler. The spec requires signal.terminalError to be latched
                // before the handler is invoked so a handler that synchronously
                // probes getLastTerminalError() (or calls flush()) sees the
                // typed error rather than null. Bytes on disk are the bytes
                // the server rejected; reconnect/replay cannot fix them.
                recordFatal(new LineSenderServerException(err));
                dispatchError(err);
            } else {
                // DROP_AND_CONTINUE: advance ackedFsn past the rejected span
                // so the loop drains subsequent batches. The data is dropped
                // from the SF disk store via the existing trim path; the
                // dispatch is the user's only handle to dead-letter.
                LOG.warn("server rejected wire seq {} (category={}, status=0x{}) -- dropping batch and continuing",
                        wireSeq, category, Integer.toHexString(status & 0xFF));
                totalAcks.incrementAndGet();
                if (durableAckMode) {
                    // A rejected batch never reaches the WAL, so the server
                    // will not emit a durable-ack for it. Stash an empty
                    // entry so the queue still advances past it, but only
                    // after every preceding OK'd batch is durable -- trimming
                    // past unfilled durable slots would corrupt SF semantics.
                    enqueuePendingOk(cappedSeq);
                    drainPendingDurable();
                } else if (engine.acknowledge(fsn)) {
                    // DROP_AND_CONTINUE on the non-durable path advanced the
                    // watermark past the rejected FSN; observers waiting on
                    // a target FSN should see that advance, even though it
                    // represents a drop rather than a successful commit.
                    dispatchProgress(fsn);
                }
                dispatchError(err);
            }
        }
    }
}
