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
import io.questdb.client.cutlass.qwp.client.QwpVersionMismatchException;
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
 *   <li>On wire failure, runs the configured reconnect policy: capped
 *       exponential backoff with jitter, retried indefinitely (Invariant B --
 *       a store-and-forward drainer never gives up on a wall-clock budget),
 *       with only auth-style failures (401/403/non-101 upgrade reject) treated
 *       as terminal. On reconnect success, repositions the cursor at
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
    /**
     * Default poison-frame detector threshold: consecutive server-active
     * rejections (NACK or non-orderly close) of the same head-of-line frame, with
     * no ack progress in between, before the loop declares the frame poisoned and
     * halts with a typed {@link SenderError.Category#PROTOCOL_VIOLATION} terminal.
     * Below the threshold a retriable rejection recycles the connection and
     * replays from ackedFsn+1. Configurable per sender via the
     * {@code max_frame_rejections} connect-string key or
     * {@code LineSenderBuilder.maxFrameRejections(int)}.
     */
    public static final int DEFAULT_MAX_HEAD_FRAME_REJECTIONS = 4;
    /**
     * Default minimum wall-clock dwell (millis) a suspected frame must remain
     * poisoned before the detector escalates to a typed terminal, even once
     * {@link #DEFAULT_MAX_HEAD_FRAME_REJECTIONS} strikes have accrued. Decouples
     * "how many strikes prove determinism" from "how long a transient is allowed
     * to look poisoned": with pacing, strikes can accrue in well under a second
     * (e.g. an accepting middlebox/LB that closes each cycle while its backend is
     * briefly down), so a strike count alone can false-positive a transient into
     * a producer-fatal terminal. Requiring the suspect to survive this window
     * gives a brief outage time to clear (an OK at/beyond the suspect resets the
     * detector) before escalation. {@code 0} = legacy immediate escalation at the
     * strike threshold. Configurable per sender via the
     * {@code poison_min_escalation_window_millis} connect-string key or
     * {@code LineSenderBuilder.poisonMinEscalationWindowMillis(long)}.
     */
    public static final long DEFAULT_POISON_MIN_ESCALATION_WINDOW_MILLIS = 5_000L;
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
    // Retained for constructor symmetry and passed in by callers, but NOT
    // consulted by the background loop: Invariant B removed the wall-clock
    // give-up from connectLoop. The budget still bounds the blocking (non-lazy)
    // initial connect via QwpWebSocketSender -> connectWithRetry, which takes it
    // as an explicit argument rather than reading this field.
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
    // by category. Includes both retriable and terminal outcomes — i.e. every
    // server-side rejection observed regardless of how the loop reacted.
    private final AtomicLong totalServerErrors = new AtomicLong();
    private WebSocketClient client;
    // Optional: when non-null, every server-rejection error (retriable and
    // terminal alike) is offered to the dispatcher for async delivery to the user's
    // handler. Null disables async delivery entirely; the producer-side
    // typed-throw path is unaffected.
    // Optional: when non-null, sender-side connection events (CONNECTED,
    // FAILED_OVER, ENDPOINT_ATTEMPT_FAILED, AUTH_FAILED, ALL_ENDPOINTS_UNREACHABLE)
    // are written to this dispatcher from QwpWebSocketSender. connectLoop itself
    // no longer emits a terminal budget-exhaustion event (Invariant B: it retries
    // indefinitely and never gives up on a wall-clock budget).
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
    // distinguish a "never reached the server" terminal failure (looks like a
    // config typo or firewall block) from "lost connection after we were
    // up" (looks transient).
    private volatile boolean hasEverConnected;
    private volatile Thread ioThread;
    // Typed marker for a durable-ack CAPABILITY-GAP terminal: set (before the
    // terminalError latch, so a checkError() caller that observes the latch is
    // guaranteed to observe this marker too) when a reconnect sweep threw
    // QwpDurableAckMismatchException. The orphan drainer consults it to route
    // a mid-drain capability gap into its budgeted settle-retry
    // (BackgroundDrainer.connectWithDurableAckRetry) instead of quarantining
    // the slot on the first sweep; the foreground sender ignores it and keeps
    // its spec'd loud-fail (sf-client.md section 8.1). Write-once alongside
    // terminalError: the only writer runs on the I/O thread under the same
    // first-writer-wins latch.
    private volatile QwpDurableAckMismatchException capabilityGapTerminal;
    // Failed-stop hand-off flag: set by delegateEngineClose() when an owner's
    // close() could not stop the I/O thread and the engine close is therefore
    // performed by the I/O thread's exit path. Write-once, owner thread only;
    // read by the I/O thread strictly after its shutdown-latch countdown (see
    // the handshake contract on delegateEngineClose).
    private volatile boolean engineCloseDelegated;
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
    // Recovered orphaned deferred tail: frames [orphanSkipStartFsn ..
    // orphanSkipTipFsn] carry FLAG_DEFER_COMMIT with no covering commit frame
    // -- a transaction whose commit was never published (producer crashed or
    // closed mid-transaction). They must never reach the wire: a
    // commit-bearing frame from THIS session would commit them server-side
    // and resurrect a partial transaction. trySendOne stops the cursor at the
    // tail; tryRetireOrphanTail retires it with a cumulative self-acknowledge
    // once everything below is server-acked. -1 when none/retired. Written by
    // the constructor and start() (user thread, before the I/O thread exists)
    // and by the I/O thread afterwards -- never concurrently.
    private long orphanSkipStartFsn = -1L;
    private long orphanSkipTipFsn = -1L;
    // Poison-frame detector state (I/O thread only). poisonFsn is the FSN of the
    // frame implicated by the most recent server-active rejection: the NACK-named
    // frame, or the OK-level head-of-line frame (highestOkFsn+1) for a
    // non-orderly close after at least one send on the connection. poisonStrikes
    // counts consecutive rejections of that same FSN; an OK at-or-beyond
    // poisonFsn proves the rejection was not deterministic and resets both.
    // Progress is deliberately measured against highestOkFsn -- OK-level wire
    // progress -- and NOT against the engine's trim watermark: in durable-ack
    // mode ackedFsn advances only on STATUS_DURABLE_ACK coverage, so a
    // post-NACK recycle replays from the durable watermark and re-OKs frames
    // BEHIND the poisoned one; keying or resetting on trim would let those
    // re-OKs launder the strike count every cycle and a deterministically-
    // NACKing frame would recycle the connection forever. At
    // maxHeadFrameRejections strikes the frame is declared poisoned: the server
    // (or an intermediary) deterministically rejects these exact bytes, so
    // replay cannot succeed and the loop halts with a typed PROTOCOL_VIOLATION
    // terminal instead of reconnect-looping forever. Both fields deliberately
    // survive reconnects (swapClient) -- the detector spans connections.
    private long poisonFsn = -1L;
    private int poisonStrikes;
    // Wall-clock nanos of the FIRST strike of the current poisonFsn run. The
    // detector escalates only once poisonStrikes >= maxHeadFrameRejections AND
    // at least poisonMinEscalationWindowNanos have elapsed since this instant,
    // so a transient shorter than the window always gets a reset chance. I/O
    // thread only; survives reconnects alongside poisonFsn/poisonStrikes.
    private long poisonFirstStrikeNanos;
    // Highest FSN the server has acknowledged at the OK level (STATUS_OK),
    // across connections. In default mode this tracks the trim watermark
    // exactly; in durable-ack mode it runs AHEAD of ackedFsn, which waits for
    // STATUS_DURABLE_ACK coverage. I/O thread only; survives reconnects. Used
    // by the poison-frame detector to measure genuine acceptance progress.
    private long highestOkFsn = -1L;
    // Zero-progress recycle pacer (I/O thread only; survives reconnects).
    // Counts consecutive strike-EXEMPT recycles -- orderly closes
    // (NORMAL_CLOSURE/GOING_AWAY), non-orderly closes before any send on the
    // connection, and pre-send RETRIABLE_OTHER rejections -- with no
    // acceptance progress in between. These paths deliberately carry no
    // poison strike (they are not a verdict on the bytes), which also exempts
    // them from failPaced's strike-keyed dose; and a peer that completes
    // TCP+TLS+upgrade before closing makes every connect attempt "succeed",
    // so connectLoop's failed-connect backoff never engages either. Without
    // this counter such recycles churn at handshake RTT rate with no bound
    // (Invariant B removed the wall-clock cap), each cycle burning a fresh
    // WebSocketClient + SSLContext/trust-store read. Progress is measured as
    // max(ackedFsn, highestOkFsn): both watermarks are monotonic and advance
    // only on genuine new acceptance (replayed re-OKs of already-OK'd frames
    // advance neither), so replay cannot launder the counter. Pacing only --
    // this counter NEVER escalates to a terminal (Invariant B).
    private int zeroProgressRecycles;
    private long progressAtLastExemptRecycle = Long.MIN_VALUE;
    // Poison-frame detector threshold for this loop. Constructor-configured
    // (connect-string key max_frame_rejections); defaults to
    // DEFAULT_MAX_HEAD_FRAME_REJECTIONS.
    private final int maxHeadFrameRejections;
    // Minimum wall-clock dwell (nanos) a suspect frame must stay poisoned before
    // escalation, even after the strike threshold is hit (connect-string key
    // poison_min_escalation_window_millis). 0 = legacy immediate escalation.
    private final long poisonMinEscalationWindowNanos;
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
     * client, and a terminal failure (auth/upgrade reject) is delivered
     * through the dispatcher rather than thrown to the constructor's
     * caller; plain connect failures are retried indefinitely
     * (Invariant B: no wall-clock budget give-up).
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
        this(client, engine, fsnAtZero, parkNanos, reconnectFactory,
                reconnectMaxDurationMillis, reconnectInitialBackoffMillis,
                reconnectMaxBackoffMillis, durableAckMode,
                durableAckKeepaliveIntervalMillis, DEFAULT_MAX_HEAD_FRAME_REJECTIONS);
    }

    /**
     * Eleven-arg overload — omits the poison-escalation dwell window, which
     * defaults to {@code 0} (legacy: escalate as soon as maxHeadFrameRejections
     * strikes accrue, with no minimum wall-clock). The user-facing 5s default
     * ({@link #DEFAULT_POISON_MIN_ESCALATION_WINDOW_MILLIS}) is applied at the
     * config layer (Sender / QwpWebSocketSender), so this raw constructor stays
     * unopinionated for tests and internal callers that want deterministic
     * strike-count escalation.
     */
    public CursorWebSocketSendLoop(WebSocketClient client, CursorSendEngine engine,
                                   long fsnAtZero, long parkNanos,
                                   ReconnectFactory reconnectFactory,
                                   long reconnectMaxDurationMillis,
                                   long reconnectInitialBackoffMillis,
                                   long reconnectMaxBackoffMillis,
                                   boolean durableAckMode,
                                   long durableAckKeepaliveIntervalMillis,
                                   int maxHeadFrameRejections) {
        this(client, engine, fsnAtZero, parkNanos, reconnectFactory,
                reconnectMaxDurationMillis, reconnectInitialBackoffMillis,
                reconnectMaxBackoffMillis, durableAckMode,
                durableAckKeepaliveIntervalMillis, maxHeadFrameRejections, 0L);
    }

    /**
     * Master constructor — also accepts the poison-frame detector threshold
     * ({@code max_frame_rejections}): consecutive server-active rejections of
     * the same head-of-line frame, with no ack progress in between, before the
     * loop escalates to a typed terminal. Must be {@code >= 1}. The final
     * argument is the minimum wall-clock dwell (millis) the suspect must stay
     * poisoned before escalation ({@code poison_min_escalation_window_millis};
     * {@code >= 0}, where 0 = legacy immediate escalation at the threshold).
     */
    public CursorWebSocketSendLoop(WebSocketClient client, CursorSendEngine engine,
                                   long fsnAtZero, long parkNanos,
                                   ReconnectFactory reconnectFactory,
                                   long reconnectMaxDurationMillis,
                                   long reconnectInitialBackoffMillis,
                                   long reconnectMaxBackoffMillis,
                                   boolean durableAckMode,
                                   long durableAckKeepaliveIntervalMillis,
                                   int maxHeadFrameRejections,
                                   long poisonMinEscalationWindowMillis) {
        if (maxHeadFrameRejections < 1) {
            throw new IllegalArgumentException(
                    "maxHeadFrameRejections must be >= 1: " + maxHeadFrameRejections);
        }
        if (poisonMinEscalationWindowMillis < 0) {
            throw new IllegalArgumentException(
                    "poisonMinEscalationWindowMillis must be >= 0: " + poisonMinEscalationWindowMillis);
        }
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
        this.maxHeadFrameRejections = maxHeadFrameRejections;
        this.poisonMinEscalationWindowNanos = poisonMinEscalationWindowMillis > 0
                ? poisonMinEscalationWindowMillis * 1_000_000L
                : 0L;
        // SYNC/OFF startup hands a live client to the constructor, so we
        // already know we reached the server at least once. ASYNC startup
        // hands null and lets the I/O thread connect — hasEverConnected
        // stays false until swapClient sees its first success.
        this.hasEverConnected = client != null;
        // Adopt the engine's recovered orphaned-deferred-tail range (if any).
        // See the field docs on orphanSkipStartFsn/orphanSkipTipFsn; the
        // BackgroundDrainer path builds its loop through this same
        // constructor, so drained orphan slots get the identical containment.
        long orphanTip = engine.recoveredOrphanTipFsn();
        if (orphanTip >= 0) {
            this.orphanSkipStartFsn = engine.recoveredCommitBoundaryFsn() + 1L;
            this.orphanSkipTipFsn = orphanTip;
        }
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
            case WebSocketResponse.STATUS_NOT_WRITABLE:
                return SenderError.Category.NOT_WRITABLE;
            default:
                return SenderError.Category.UNKNOWN;
        }
    }

    /**
     * Same exponential-backoff-with-jitter machinery as the I/O thread's
     * {@code connectLoop}, but reusable from {@code ensureConnected} to
     * implement {@code initial_connect_retry=true}. Unlike {@code connectLoop}
     * (which retries indefinitely under Invariant B), this blocking variant
     * IS bounded by {@code maxDurationMillis}: it returns the connected
     * client on success and throws on terminal upgrade error (won't retry)
     * or budget exhaustion.
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
                if (e instanceof Error) {
                    // JVM/programming failure (OOM, LinkageError): not a
                    // transport outage, retrying cannot clear it. Propagate
                    // to the caller instead of burning the connect budget.
                    throw (Error) e;
                }
                lastError = e;
                long now = System.nanoTime();
                if (now - lastLogNanos >= RECONNECT_LOG_THROTTLE_NANOS) {
                    if (e instanceof QwpVersionMismatchException) {
                        // Reachable but protocol-incompatible: consumes the connect
                        // budget (walks the cluster across a rolling-upgrade window)
                        // and, on exhaustion, surfaces as the terminal
                        // LineSenderException below. Name the condition so a version
                        // skew is diagnosable, not read as a generic connect failure.
                        LOG.warn("{} attempt {}: every reachable endpoint advertises an unsupported "
                                        + "QWP protocol version ({}); retrying within connect budget",
                                contextLabel, attempts, e.getMessage());
                    } else {
                        LOG.warn("{} attempt {} failed: {}",
                                contextLabel, attempts, e.getMessage());
                    }
                    lastLogNanos = now;
                }
            }
            // Math.max guards nextLong's bound>0 contract (IAE on <= 0)
            // against non-builder callers -- builder-validated config keeps
            // this > 0. Mirrors BackgroundDrainer's sweep-loop jitter guard.
            long jitter = ThreadLocalRandom.current().nextLong(Math.max(1L, backoffMillis));
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
     * Default category → policy mapping. There is no drop policy: a rejection is
     * either replayed (RETRIABLE / RETRIABLE_OTHER — the bytes stay in the SF log,
     * the connection is recycled, replay resumes from ackedFsn+1) or halts the
     * sender loudly (TERMINAL — reserved for rejections that are deterministic
     * under byte-identical replay; the bytes stay on disk). UNKNOWN fails open to
     * RETRIABLE so a status byte from a newer server degrades to retry, not to a
     * dead sender; a deterministic repeat is caught by the poison-frame detector.
     * User overrides (builder + connect-string) plug in here in a later commit;
     * today this is the only resolver. Exposed for unit tests.
     */
    @TestOnly
    public static SenderError.Policy defaultPolicyFor(SenderError.Category category) {
        switch (category) {
            case WRITE_ERROR:     // transient server state (disk pressure, suspended table)
            case INTERNAL_ERROR:  // transient by definition; deterministic repeats poison-escalate
            case UNKNOWN:         // fail open: status byte from a newer server
                return SenderError.Policy.RETRIABLE;
            case NOT_WRITABLE:    // read-only replica / demoting primary: rotate endpoints
                return SenderError.Policy.RETRIABLE_OTHER;
            case SCHEMA_MISMATCH: // deterministic: same bytes, same mismatch
            case PARSE_ERROR:     // deterministic: malformed bytes never parse
            case SECURITY_ERROR:  // ACL denial on a writable node (read-only refusals arrive as role-change closes)
            case PROTOCOL_VIOLATION:
            default:
                return SenderError.Policy.TERMINAL;
        }
    }

    /**
     * True if a WebSocket close code nominally signals a protocol-layer
     * violation per RFC 6455. NO LONGER consulted by the policy path: WS close
     * codes carry no policy semantics (policy travels only in QWP NACK frames),
     * every close is reconnect-eligible, and a frame that deterministically
     * kills the connection is caught behaviorally by the poison-frame detector
     * ({@link #DEFAULT_MAX_HEAD_FRAME_REJECTIONS}) rather than by this code list.
     * Retained for diagnostics and tests.
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
     * The typed durable-ack capability-gap terminal, or {@code null} if the
     * loop's terminal (if any) is a different failure class. Non-null only
     * after {@link #checkError()} started throwing: the marker is written
     * before the {@code terminalError} latch, both on the I/O thread.
     * <p>
     * Consumer contract: the orphan drainer ({@code BackgroundDrainer})
     * checks this after a {@code checkError()} throw to decide between
     * re-entering its budgeted settle-retry (capability gap: the rolling
     * upgrade may still settle) and quarantining the slot (every other
     * terminal). Package-private on purpose -- the foreground sender must
     * not branch on it (spec'd loud-fail, sf-client.md section 8.1).
     */
    QwpDurableAckMismatchException capabilityGapTerminal() {
        return capabilityGapTerminal;
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
                } catch (InterruptedException e) {
                    // Re-assert the flag for the caller's stack, then decide.
                    // If the I/O thread has genuinely not exited (latch still
                    // up — it may be inside a blocking native connect/send
                    // that neither unpark nor interrupt cancels), touching the
                    // client here would free native buffers under a possibly
                    // mid-send thread, and returning quietly would let the
                    // owner unmap the engine under it (C5 SEGV). Signal the
                    // failed stop loudly instead: QwpWebSocketSender.close()
                    // keys its ioThreadStopped guard on this throw, and
                    // BackgroundDrainer switches to delegateEngineClose().
                    // The I/O thread's own exit path (ioLoop's finally)
                    // disposes of the client either way. ioThread stays set,
                    // so a duplicate close() re-signals rather than silently
                    // succeeding against a still-live thread.
                    Thread.currentThread().interrupt();
                    if (shutdownLatch.getCount() != 0L) {
                        throw new LineSenderException(
                                "cursor I/O thread did not stop: close() was interrupted "
                                        + "while awaiting shutdown; client/engine teardown "
                                        + "is delegated to the I/O thread's exit path");
                    }
                    // Latch hit zero concurrently with the interrupt: the
                    // thread is past its last client/engine access — proceed
                    // with normal teardown.
                }
            }
            ioThread = null;
        }
        // Close the current client. After a reconnect, swapClient has
        // replaced the original (and closed it); the owner only retains
        // the stale pre-reconnect reference. Without closing the live
        // client here, its native socket and fds leak past sender.close()
        // every time the loop reconnected at least once. ioLoop's finally
        // also closes the current client on I/O-thread exit, so this read
        // matters chiefly when the loop never started (SYNC construction,
        // close() before start()) — and doubles as a safety net. close()
        // is idempotent, so duplicate closes on any path are safe.
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
     * Failed-stop hand-off for the engine. Called by an owner whose
     * {@link #close()} threw because the I/O thread would not stop: the owner
     * must not free the engine (munmap/Unsafe.free of segment memory) while
     * the thread may still touch it with raw {@code Unsafe} reads. Setting
     * the delegation flag makes the I/O thread run {@code engine.close()} on
     * its exit path, strictly after its last engine access and after the
     * shutdown-latch countdown — releasing the slot lock as soon as the
     * stuck wire call resolves (bounded by OS timeouts) instead of leaking
     * the mapping and lock forever.
     * <p>
     * Returns {@code true} when the I/O thread is still live and has adopted
     * the engine close; {@code false} when the thread has already exited —
     * the caller must close the engine itself.
     * <p>
     * Memory model — the classic store/load handshake: this method writes the
     * volatile flag, then reads the latch count; the exit path counts the
     * latch down, then reads the flag. Under the sequential consistency of
     * volatile (and AQS latch state) accesses, if this method observes the
     * latch still up, the exit path is guaranteed to observe the flag — no
     * missed close. If both sides act, {@link CursorSendEngine#close()} is
     * synchronized and idempotent, so the double close is benign.
     */
    public boolean delegateEngineClose() {
        engineCloseDelegated = true;
        return shutdownLatch.getCount() != 0L;
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
     * retriable and terminal outcomes — every non-OK frame the server sent that
     * the client classified as a {@link SenderError}.
     */
    public long getTotalServerErrors() {
        return totalServerErrors.get();
    }

    /**
     * True iff the I/O loop has at least once installed a live (connected
     * + upgraded) WebSocket client. Sticky — once true, stays true even
     * after a subsequent disconnect. Lets a {@code SenderErrorHandler}
     * disambiguate a "never reached the server" terminal failure (likely
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
     * notifications. Connection events fire from
     * {@code QwpWebSocketSender.buildAndConnect} directly into this dispatcher;
     * {@code connectLoop} no longer emits a terminal budget-exhaustion event
     * (Invariant B: it retries indefinitely). Same lifecycle contract as
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
     * connect failures are retried indefinitely (Invariant B), and a
     * terminal upgrade reject is delivered through the dispatcher, not
     * thrown to the producer.
     */
    private void attemptInitialConnect() {
        connectLoop(new LineSenderException(
                        "async initial connect deferred to I/O thread"),
                "initial connect", 0L);
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
     * {@code paceFirstAttemptMillis} > 0 parks for that long (+jitter)
     * before the first connect attempt — see {@link #failPaced}.
     */
    private void connectLoop(Throwable initial, String phase, long paceFirstAttemptMillis) {
        if (reconnectFactory == null || !running) {
            recordFatal(initial);
            return;
        }
        LOG.warn("cursor I/O loop entering {} loop: {}",
                phase, initial.getMessage());
        long outageStartNanos = System.nanoTime();
        // INVARIANT B: a store-and-forward drainer must NEVER terminate on a
        // wall-clock reconnect budget. A replica-only / all-endpoints-replica
        // window is TRANSIENT -- a replica gets promoted, a primary reappears --
        // so this background loop retries for as long as it is running, backing
        // off between attempts. The ONLY terminal conditions are a genuinely
        // non-retriable upgrade (auth / non-421 upgrade / durable-ack capability
        // gap), which return directly below, or the sender being stopped. SF
        // exhaustion is surfaced to the PRODUCER as append backpressure, never
        // here. reconnect_max_duration_millis is intentionally NOT consulted: it
        // bounds only the blocking (non-lazy) initial connect in
        // QwpWebSocketSender.buildAndConnect, never this background loop.
        long backoffMillis = reconnectInitialBackoffMillis;
        if (paceFirstAttemptMillis > 0 && running) {
            // NACK-initiated recycle against a reachable server: pace the
            // recycle with a backoff+jitter dose (sized by the caller --
            // failPaced escalates it with consecutive same-frame strikes),
            // or the recycle loop runs at server NACK rate. The dose is a
            // hard pacing guarantee, so ride out spurious parkNanos returns
            // and stale unpark permits with a deadline loop; close() flips
            // running=false and unparks, exiting promptly.
            long jitter = ThreadLocalRandom.current().nextLong(paceFirstAttemptMillis);
            long deadlineNanos = System.nanoTime()
                    + (paceFirstAttemptMillis + jitter) * 1_000_000L;
            long remaining;
            while (running && (remaining = deadlineNanos - System.nanoTime()) > 0) {
                LockSupport.parkNanos(remaining);
            }
        }
        int attempts = 0;
        long lastLogNanos = 0L;
        Throwable lastReconnectError = initial;
        while (running) {
            attempts++;
            totalReconnectAttempts.incrementAndGet();
            try {
                WebSocketClient newClient = reconnectFactory.reconnect();
                if (newClient != null) {
                    if (!running) {
                        // close() ran while this connect attempt was in
                        // flight. Its latch await may have been interrupted
                        // (BackgroundDrainerPool.close()'s shutdownNow path)
                        // and returned already — the owner's teardown,
                        // including the engine unmap in BackgroundDrainer's
                        // finally, can be complete. Installing the client now
                        // would (a) touch engine memory via positionCursorAt
                        // after a possible unmap and (b) abandon a live socket
                        // in a loop nothing will revisit — close() has run,
                        // its client read saw the pre-connect field. The
                        // attempt owns the client until it is installed, so
                        // dispose of it here, on the I/O thread, and exit
                        // through the quiet stopped path below.
                        try {
                            newClient.close();
                        } catch (Throwable ignored) {
                            // best-effort
                        }
                        break;
                    }
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
                        SenderError.Policy.TERMINAL,
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
                if (terminalError == null) {
                    // Mirror recordFatal's first-writer-wins latch: only the
                    // sweep that owns the terminal may mark the gap, and the
                    // marker must be visible before the terminalError volatile
                    // write that checkError() keys on.
                    capabilityGapTerminal = e;
                }
                long fromFsn = engine.ackedFsn() + 1L;
                long toFsn = Math.max(fromFsn, engine.publishedFsn());
                SenderError err = new SenderError(
                        SenderError.Category.PROTOCOL_VIOLATION,
                        SenderError.Policy.TERMINAL,
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
                // Role mismatch: every reachable endpoint role-rejected the
                // upgrade -- right now they are all replicas / primary-catchup.
                // This is a TRANSIENT failover window (a replica is promotable),
                // so keep retrying with no wall-clock deadline (Invariant B).
                // Do NOT reset the backoff or pin it at the initial interval:
                // fall through to the shared capped exponential backoff-with-
                // jitter block below. Pinning at reconnectInitialBackoffMillis
                // turned a persistent all-replica window (e.g. an address list
                // pointing at replicas only, now surfaced here as a retriable
                // role reject rather than a terminal durable-ack mismatch) into
                // a fixed ~10/s storm of fresh TLS handshakes -- new
                // WebSocketClient, new SSLContext, trust-store re-read -- per
                // endpoint, forever. Growing to reconnectMaxBackoffMillis
                // mirrors the orphan drainer's role-reject path and honours the
                // documented capped-exponential-backoff contract.
                lastReconnectError = e;
                long now = System.nanoTime();
                if (now - lastLogNanos >= RECONNECT_LOG_THROTTLE_NANOS) {
                    LOG.warn("{} attempt {}: every reachable endpoint is a replica "
                                    + "(transient failover window); retrying with capped backoff -- "
                                    + "if this persists the configured address list may point at replicas only",
                            phase, attempts);
                    lastLogNanos = now;
                }
                // fall through to the shared capped-backoff block
            } catch (Throwable e) {
                if (e instanceof Error) {
                    // JVM/programming failure (OOM, LinkageError): retrying
                    // cannot clear it -- Invariant B covers transport outages
                    // only. Latch it as terminal FIRST so a producer parked in
                    // checkError() observes the failure and `running` flips
                    // false, then rethrow so the I/O thread dies loudly
                    // instead of reconnect-looping. The fail() call site sits
                    // inside ioLoop's catch, so ioLoop's finally still counts
                    // down the shutdown latch and close() cannot hang.
                    recordFatal(e);
                    throw (Error) e;
                }
                lastReconnectError = e;
                long now = System.nanoTime();
                if (now - lastLogNanos >= RECONNECT_LOG_THROTTLE_NANOS) {
                    if (e instanceof QwpVersionMismatchException) {
                        // Not a transport failure: the server completed the WS
                        // upgrade but advertised a QWP version this client cannot
                        // speak. Retried indefinitely under Invariant B (a rolling
                        // upgrade clears it once peers converge), but log the real
                        // condition so a persistent client/cluster version skew is
                        // diagnosable instead of reading as a generic connect fail.
                        LOG.warn("{} attempt {}: every reachable endpoint advertises an unsupported "
                                        + "QWP protocol version ({}); retrying (rolling-upgrade window) -- "
                                        + "if this persists the client is version-incompatible with the cluster",
                                phase, attempts, e.getMessage());
                    } else {
                        LOG.warn("{} attempt {} failed: {}", phase, attempts, e.getMessage());
                    }
                    lastLogNanos = now;
                }
            }
            if (running) {
                // Math.max guards nextLong's bound>0 contract (IAE on <= 0):
                // an IAE here would climb through fail() into ioLoop's
                // secondary-failure guard and latch a terminal -- correct but
                // needlessly fatal for a jitter computation. Builder-validated
                // config keeps this > 0; the guard covers direct constructor
                // use. Mirrors BackgroundDrainer's sweep-loop jitter guard.
                long jitter = ThreadLocalRandom.current().nextLong(Math.max(1L, backoffMillis));
                long sleepMillis = backoffMillis + jitter;
                LockSupport.parkNanos(sleepMillis * 1_000_000L);
                backoffMillis = Math.min(backoffMillis * 2, reconnectMaxBackoffMillis);
            }
        }
        // The loop exits ONLY because running == false, i.e. the sender is
        // closing / stopping. Under Invariant B this is NOT a budget give-up
        // (there is no wall-clock terminal): we retried until asked to stop, so
        // we return quietly and let close() drive shutdown. Un-acked rows remain
        // in on-disk SF for this sender's next run or an orphan drainer to ship.
        long elapsedMs = (System.nanoTime() - outageStartNanos) / 1_000_000L;
        String lastMsg = lastReconnectError == null ? "n/a" : lastReconnectError.getMessage();
        LOG.info("cursor I/O loop {} stopped after {}ms, {} attempts (sender closing); "
                        + "un-acked rows remain in SF for retry; last error: {}",
                phase, elapsedMs, attempts, lastMsg);
    }

    /**
     * Send {@code err} to the async-delivery dispatcher if one is configured.
     * Producer-side typed throw (TERMINAL) goes through {@code recordFatal} +
     * {@code checkError} regardless — this is purely the async observer path.
     */
    private void dispatchError(SenderError err) {
        SenderErrorDispatcher d = errorDispatcher;
        if (d != null) {
            d.offer(err);
        }
    }

    /**
     * Poison-frame detector: record a server-active rejection (NACK, or non-orderly
     * close after at least one send on this connection) of {@code rejectedFsn} —
     * the NACK-named frame, or the OK-level head-of-line frame for a close.
     * Returns true when that same FSN has now been rejected
     * {@code maxHeadFrameRejections} consecutive times (configurable; default
     * {@link #DEFAULT_MAX_HEAD_FRAME_REJECTIONS}) with no OK-level acceptance
     * at or beyond it in between — the frame is poisoned and replay is
     * deterministic. Strikes are keyed on the rejected frame itself, never on
     * the engine's trim watermark: in durable-ack mode the trim watermark lags
     * OK-level progress, and both the key and the reset must be immune to the
     * replay re-OKs of frames behind the suspect. I/O thread only.
     */
    private boolean recordHeadRejectionStrike(long rejectedFsn) {
        long now = System.nanoTime();
        if (rejectedFsn == poisonFsn) {
            poisonStrikes++;
        } else {
            poisonFsn = rejectedFsn;
            poisonStrikes = 1;
            poisonFirstStrikeNanos = now;
        }
        if (poisonStrikes < maxHeadFrameRejections) {
            return false;
        }
        // Minimum wall-clock dwell gate: even once the strike count proves
        // determinism, hold escalation until the suspect has stayed poisoned for
        // at least poisonMinEscalationWindowNanos. Strikes can accrue in well
        // under a second (paced recycles against a reachable-but-closing
        // middlebox, or a fast NACK loop), so the count alone would false-
        // positive a brief outage into a producer-fatal terminal. Any OK at/
        // beyond the suspect during the window resets the detector (see the
        // STATUS_OK handler). Window 0 = legacy immediate escalation.
        return (now - poisonFirstStrikeNanos) >= poisonMinEscalationWindowNanos;
    }

    /**
     * Escalate a poisoned frame to a typed terminal: the server (or an intermediary)
     * has deterministically rejected the same frame {@code maxHeadFrameRejections}
     * consecutive times with no OK-level acceptance at or beyond it, so
     * replaying it cannot succeed and retrying would loop forever. recordFatal runs
     * before dispatchError so the producer-observable terminal is latched before the
     * user's handler is invoked. The rejected bytes remain in the SF log on disk —
     * nothing is silently discarded.
     */
    private void haltOnPoisonedFrame(String lastRejection, long toFsnHint) {
        // Name the frame the server actually rejected (the strike key), not
        // ackedFsn+1: in durable-ack mode the trim watermark can sit on
        // frames the server already ACCEPTED at the OK level, and pointing
        // the operator at those bytes would misattribute the poison. The
        // caller supplies the span end: a NACK names the exact frame, so the
        // span is that single frame; a non-orderly close cannot single one
        // out, so it spans to publishedFsn.
        long fromFsn = poisonFsn;
        long toFsn = Math.max(fromFsn, toFsnHint);
        String msg = "frame at fsn=" + fromFsn + " rejected " + poisonStrikes
                + " consecutive times with no acceptance at or beyond it -- poisoned frame, replay cannot succeed (last: "
                + lastRejection + ')';
        SenderError err = new SenderError(
                SenderError.Category.PROTOCOL_VIOLATION,
                SenderError.Policy.TERMINAL,
                SenderError.NO_STATUS_BYTE,
                msg,
                SenderError.NO_MESSAGE_SEQUENCE,
                fromFsn,
                toFsn,
                null,
                System.nanoTime()
        );
        totalServerErrors.incrementAndGet();
        recordFatal(new LineSenderServerException(err));
        dispatchError(err);
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
     * from empty-WAL OK frames) pop unconditionally.
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
     * Stash a wireSeq + per-table seqTxns from the current OK frame for
     * later durable-ack confirmation. {@link #response} must hold the OK
     * frame at call time. Only the OK path enqueues: rejections never
     * advance the durable watermark -- handleServerRejection either latches
     * a terminal or recycles the connection.
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
     * listener both non-null), enters the per-outage retry loop: capped
     * exponential backoff with jitter, retried for as long as the loop is
     * running -- there is NO wall-clock give-up (Invariant B: a store-and-
     * forward drainer only terminates on SF exhaustion or a genuinely non-
     * retriable auth/upgrade reject). On the first successful reconnect the
     * I/O loop resumes with reset wire state and replays from
     * {@code engine.ackedFsn() + 1}.
     * <p>
     * Without reconnect plumbing, the failure is immediately terminal
     * (legacy behavior).
     */
    private void fail(Throwable initial) {
        connectLoop(initial, "reconnect", 0L);
    }

    /**
     * Same recycle machinery as {@link #fail}, but parks for a backoff dose
     * (+jitter) BEFORE the first connect attempt. Used for server-active
     * RETRIABLE rejections (NACKs): the server is reachable — it just
     * answered — so the first reconnect attempt will almost always succeed
     * and connectLoop's failed-connect backoff never engages. This park is
     * what delivers the "capped backoff + jitter" promise of
     * design/qwp-nack-policy-v2.md for the RETRIABLE policy against a
     * healthy server; transport failures keep the unpaced {@link #fail}
     * (an immediate first reconnect attempt is desirable there).
     * <p>
     * The dose escalates with consecutive poison strikes against the same
     * frame — initial backoff, doubling per strike, capped at the max
     * backoff. Escalation matters beyond politeness: a TRANSIENT same-frame
     * rejection (e.g. sustained disk pressure NACKing the head batch) now
     * accumulates strikes toward the poison terminal even in durable-ack
     * mode, and the widening gaps between replays are what give a transient
     * condition time to clear before the threshold fires. A NACK sequence
     * that IS making progress (different frame each time) resets strikes to
     * 1 and keeps the dose at the initial backoff.
     */
    private void failPaced(Throwable initial) {
        long dose = reconnectInitialBackoffMillis;
        if (dose > 0) {
            int strikes = Math.max(poisonStrikes, 1);
            dose <<= Math.min(strikes - 1, 6);
            if (reconnectMaxBackoffMillis > 0 && dose > reconnectMaxBackoffMillis) {
                dose = reconnectMaxBackoffMillis;
            }
        }
        connectLoop(initial, "reconnect", dose);
    }

    /**
     * Recycle path for strike-exempt wire events: orderly closes
     * (NORMAL_CLOSURE / GOING_AWAY), non-orderly closes before any send on
     * the connection, and RETRIABLE_OTHER rejections (pre- and post-send:
     * NOT_WRITABLE is a node-state verdict, not a frame verdict). None of
     * these implicate the head frame, so they carry no poison strike -- but that
     * also means neither existing pacer can bound them: {@link #failPaced}
     * keys its dose on poisonStrikes, and connectLoop's backoff engages only
     * on FAILED connect attempts, while a peer that completes the upgrade
     * before closing makes every attempt succeed. A load balancer draining
     * with GOING_AWAY, a health-checking middlebox that upgrades then drops,
     * or an all-replica window answering NOT_WRITABLE would
     * otherwise drive full recycles -- fresh WebSocketClient, SSLContext,
     * trust-store read -- at handshake RTT rate, forever (Invariant B
     * deliberately removed the wall-clock cap).
     * <p>
     * The first recycle after any acceptance progress stays IMMEDIATE: a
     * one-off orderly handoff must rotate endpoints without delay (failover
     * latency). Consecutive recycles with no OK-level progress in between
     * park for the same doubling, capped dose failPaced applies. Pacing
     * only, never a terminal: unlike the poison detector these events are
     * not a verdict on the bytes, so under Invariant B they retry forever --
     * just not at wire speed.
     */
    private void failExemptPaced(Throwable cause) {
        long progress = Math.max(engine.ackedFsn(), highestOkFsn);
        if (progress > progressAtLastExemptRecycle) {
            zeroProgressRecycles = 0;
        }
        progressAtLastExemptRecycle = progress;
        int level = zeroProgressRecycles++;
        if (level == 0) {
            fail(cause);
            return;
        }
        long dose = reconnectInitialBackoffMillis;
        if (dose > 0) {
            dose <<= Math.min(level - 1, 6);
            if (reconnectMaxBackoffMillis > 0 && dose > reconnectMaxBackoffMillis) {
                dose = reconnectMaxBackoffMillis;
            }
        }
        connectLoop(cause, "reconnect", dose);
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
            // a terminal failure and clears `running` (auth/upgrade reject;
            // plain connect failures retry indefinitely under Invariant B).
            // Either way, the main loop below sees the outcome via the
            // `running` and `client` fields.
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
            if (t instanceof Error) {
                // Never funnel a JVM Error into the reconnect loop: latch it
                // as terminal so checkError() surfaces it to the producer,
                // then rethrow so the thread dies loudly. The finally still
                // counts down the shutdown latch, so close() cannot hang.
                recordFatal(t);
                throw (Error) t;
            }
            try {
                fail(t);
            } catch (Throwable secondary) {
                // fail()/connectLoop itself threw. Without this guard the
                // secondary failure escapes ioLoop's catch and kills the I/O
                // thread with `running` still true and no terminal latched:
                // checkError() stays clean and the producer keeps buffering
                // into SF against a dead loop -- the exact silent stall
                // Invariant B exists to prevent. Latch-and-die like the Error
                // arm above instead; the finally still counts down the
                // shutdown latch, so close() cannot hang. recordFatal is
                // first-writer-wins, so a terminal already latched inside
                // connectLoop (e.g. a throw from dispatchError after
                // recordFatal) is preserved.
                if (secondary != t) {
                    secondary.addSuppressed(t);
                }
                recordFatal(secondary);
                if (secondary instanceof Error) {
                    throw (Error) secondary;
                }
            }
        } finally {
            // Last act of the I/O thread: dispose of whatever client it
            // holds. This is the airtight half of the close()-vs-reconnect
            // race — when close()'s latch await is interrupted (drainer pool
            // shutdownNow), close() returns before this thread has exited,
            // and its own client close saw the pre-reconnect field. A client
            // swapped in by the tail of an in-flight connect attempt (running
            // flipped false between connectLoop's check and swapClient) would
            // be abandoned live without this. Runs BEFORE the latch countdown
            // so a non-interrupted close() observes a fully disposed loop.
            // Duplicate closes — loop.close()'s own, owners' stale references
            // — stay safe: WebSocketClient.close() is idempotent.
            WebSocketClient c = client;
            if (c != null) {
                try {
                    c.close();
                } catch (Throwable ignored) {
                    // best-effort
                }
            }
            shutdownLatch.countDown();
            // Failed-stop hand-off (see delegateEngineClose): the owner could
            // not free the engine safely while this thread was alive, so the
            // engine close — and with it the slot-lock release — happens
            // here, strictly after this thread's last engine access. The flag
            // is read only after the countDown: the store/load pairing with
            // delegateEngineClose's flag-write-then-latch-read guarantees
            // either this branch or the owner's fallback runs (or both —
            // engine.close() is idempotent).
            if (engineCloseDelegated) {
                try {
                    engine.close();
                } catch (Throwable ignored) {
                    // best-effort
                }
            }
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
     * is possible — latch the error so
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
            // Tell the async dispatcher which SenderError IS the terminal, so
            // close() can distinguish "the custom handler owns THIS terminal"
            // from "the custom handler saw some earlier DROP_AND_CONTINUE".
            // Same instance dispatchError() delivers (the err wrapped here),
            // so the dispatcher's identity compare matches. Marked under the
            // write-once latch guard so a stray later HALT cannot re-point it.
            if (terminalError instanceof LineSenderServerException) {
                SenderErrorDispatcher d = errorDispatcher;
                if (d != null) {
                    d.markTerminal(((LineSenderServerException) terminalError).getServerError());
                }
            }
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
        // subsequent terminal failure as transient vs config-likely.
        this.hasEverConnected = true;
        if (old != null) {
            try {
                old.close();
            } catch (Throwable ignored) {
                // best-effort
            }
        }
        // A recovered orphaned deferred tail may be retirable now (e.g. the
        // acks covering everything below it arrived just before the drop).
        // Retiring before computing replayStart anchors the new connection
        // past the tail instead of replaying into it.
        tryRetireOrphanTail();
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
        if (orphanSkipTipFsn >= 0 && fsnAtZero + nextWireSeq >= orphanSkipStartFsn) {
            // The send cursor reached the orphaned deferred tail. Its frames
            // belong to an aborted transaction and must never be transmitted
            // (a commit-bearing frame from this session would commit them --
            // partial-transaction resurrection).
            if (!tryRetireOrphanTail()) {
                // Frames below the tail still await server acks; keep ticking
                // (tryReceiveAcks runs every loop iteration) until the ack
                // watermark reaches the tail's lower edge.
                return false;
            }
            if (nextWireSeq == 0) {
                // Nothing sent on this connection yet: re-anchor in place past
                // the retired tail. The wireSeq<->FSN mapping is untouched
                // because no wire sequence has been consumed.
                positionCursorForStart();
                return true;
            }
            // Frames were already sent on this connection: the linear
            // fsnAtZero + wireSeq mapping cannot express the gap the retired
            // tail leaves behind. Recycle; swapClient re-anchors the mapping
            // at ackedFsn + 1 = the first frame past the tail. One reconnect,
            // once per recovery -- and only on the slow path (unacked
            // committed frames existed below the tail).
            fail(new LineSenderException(
                    "recycling connection after retiring orphaned deferred tail (wire-seq realignment)"));
            return false;
        }
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
        // Fast path for a recovered orphaned deferred tail: when everything
        // below the tail is already acked (the common crash profile -- acks
        // were flowing until the crash, only the in-flight transaction is
        // unacked; and trivially when the WHOLE recovered log is the tail),
        // the tail retires before any connection exists and the cursor
        // starts past it. Zero wire cost, no recycle.
        tryRetireOrphanTail();
        long replayStart = engine.ackedFsn() + 1L;
        this.fsnAtZero = replayStart;
        this.nextWireSeq = 0L;
        positionCursorAt(replayStart);
    }

    /**
     * Retires the recovered orphaned deferred tail once retirable.
     * <p>
     * The tail's frames all carry FLAG_DEFER_COMMIT with no covering commit
     * frame: the producer died (or closed) mid-transaction, so the
     * transaction is aborted by definition. Retirement is a cumulative
     * self-acknowledge of the tail's top FSN -- legal once every frame BELOW
     * the tail is server-acked, because the tail frames were never
     * transmitted and no wire sequence references them. The freed slots are
     * trimmed by the normal ack-driven machinery.
     * <p>
     * Crash-safe: dying before the self-ack leaves the tail in place; the
     * next recovery re-detects the same range and retries. Idempotent once
     * retired.
     *
     * @return true when no orphan tail remains (retired now, previously, or
     *         never existed); false while frames below the tail are unacked
     */
    private boolean tryRetireOrphanTail() {
        if (orphanSkipTipFsn < 0) {
            return true;
        }
        if (engine.ackedFsn() < orphanSkipStartFsn - 1L) {
            return false;
        }
        LOG.warn("retiring orphaned deferred tail: {} frame(s) [fsn {}..{}] belong to a transaction "
                        + "whose commit was never published; aborting them (never transmitted, slots trimmed)",
                orphanSkipTipFsn - orphanSkipStartFsn + 1, orphanSkipStartFsn, orphanSkipTipFsn);
        engine.acknowledge(orphanSkipTipFsn);
        orphanSkipStartFsn = -1L;
        orphanSkipTipFsn = -1L;
        return true;
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
                long okFsn = fsnAtZero + capped;
                if (okFsn > highestOkFsn) {
                    highestOkFsn = okFsn;
                }
                // Clear poison-frame suspicion only when OK-level progress
                // reaches the suspected frame itself: acceptance at or beyond
                // poisonFsn proves the earlier rejections were not
                // deterministic. Re-OKs of frames BEHIND the suspect -- the
                // norm in durable-ack mode, where every recycle replays from
                // the durable trim watermark, which lags the OK level -- say
                // nothing about the poisoned bytes and must not reset the
                // count, or a deterministically-NACKing frame recycles the
                // connection indefinitely.
                if (poisonFsn != -1L && okFsn >= poisonFsn) {
                    poisonFsn = -1L;
                    poisonStrikes = 0;
                }
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
            // WS close codes carry no policy semantics: policy travels only in
            // QWP NACK frames (a server rejecting bytes NACKs before it closes).
            // Every close is therefore a transport event and reconnect-eligible:
            // fail() enters the retry loop and replays from ackedFsn+1. The one
            // guarded case is a frame that deterministically kills the connection
            // without a NACK (e.g. an intermediary's frame-size limit). That is
            // caught behaviorally, not by code list: a non-orderly close arriving
            // after this connection already sent the head frame counts a poison
            // strike; maxHeadFrameRejections consecutive strikes at the same
            // head FSN with no ack progress escalate to a typed terminal. Orderly
            // closes (NORMAL_CLOSURE role-change handoff, GOING_AWAY restart
            // drain) never count strikes — they are the server asking us to go
            // elsewhere, not a verdict on the bytes.
            boolean orderly = code == WebSocketCloseCode.NORMAL_CLOSURE
                    || code == WebSocketCloseCode.GOING_AWAY;
            LineSenderException cause = new LineSenderException(
                    "WebSocket closed by server: code=" + code + " reason=" + reason);
            if (!orderly && nextWireSeq > 0) {
                if (recordHeadRejectionStrike(Math.max(engine.ackedFsn(), highestOkFsn) + 1L)) {
                    haltOnPoisonedFrame("ws-close[" + code + ' '
                                    + WebSocketCloseCode.describe(code) + "]: " + reason,
                            engine.publishedFsn());
                    return;
                }
                // Strike recorded but below threshold: pace the recycle exactly
                // like the below-threshold RETRIABLE NACK path. A middlebox/LB
                // that completes the WS upgrade, accepts the head frame, then
                // non-orderly-closes while its backend is down succeeds at
                // connect every cycle, so connectLoop's failed-connect backoff
                // never engages -- an unpaced fail() here would burn through
                // maxHeadFrameRejections strikes at a never-advancing head FSN
                // in well under a second and latch a PROTOCOL_VIOLATION terminal
                // on a TRANSIENT outage (Invariant B / qwp-nack-policy-v2.md:
                // "never false-positives on outages"). failPaced parks before
                // the first reconnect, and its dose escalates with poisonStrikes
                // (already incremented above), so the widening replay gaps give
                // the outage time to clear before the threshold fires.
                failPaced(cause);
                return;
            }
            // No strike (orderly close, or a close before any send on this
            // connection): not a verdict on the bytes. But strike-exempt must
            // not mean pace-exempt: a peer that completes the upgrade then
            // closes (LB drain window, GOING_AWAY rolling restart, health-
            // checking middlebox in front of a dead backend) succeeds at
            // connect every cycle, so connectLoop's failed-connect backoff
            // never engages, and with no strikes there is no poison-terminal
            // bound either -- an unpaced fail() here churns full recycles at
            // handshake RTT rate, forever. failExemptPaced keeps the first
            // recycle immediate (failover latency) and paces consecutive
            // zero-progress repeats with the escalating reconnect backoff.
            failExemptPaced(cause);
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
            if (policy == SenderError.Policy.TERMINAL) {
                // Latch the typed terminal error before invoking the handler
                // so a synchronous probe of getLastTerminalError() / flush()
                // from inside the handler observes the typed error. Mirrors
                // the ordering in the post-send TERMINAL path below.
                recordFatal(new LineSenderServerException(err));
                dispatchError(err);
                return;
            }
            // RETRIABLE / RETRIABLE_OTHER: nothing was sent on this connection,
            // so the rejection cannot implicate the head frame -- no poison
            // strike. Surface for observability, then recycle the wire; the
            // reconnect path replays from ackedFsn+1. No watermark advance --
            // nothing is dropped. RETRIABLE is paced for the same reason as
            // the post-send path: the server is reachable, so the recycle
            // would otherwise run at its rejection rate.
            dispatchError(err);
            LineSenderException recycleCause = new LineSenderException(
                    "pre-send server rejection (" + category + "): "
                            + response.getErrorMessage());
            if (policy == SenderError.Policy.RETRIABLE) {
                failPaced(recycleCause);
            } else {
                // RETRIABLE_OTHER pre-send (NOT_WRITABLE before we sent
                // anything): rotate endpoints -- but through the zero-
                // progress pacer, not raw fail(). An all-replica window
                // answers NOT_WRITABLE on every fresh connection, each
                // connect attempt "succeeds", and pre-send rejections record
                // no strike, so nothing else bounds the churn. The first
                // recycle stays immediate so a genuine failover rotates
                // without delay.
                failExemptPaced(recycleCause);
            }
        }

        private void handleServerRejection(long wireSeq) {
            byte status = response.getStatus();
            SenderError.Category category = classify(status);
            SenderError.Policy policy = defaultPolicyFor(category);
            // Same sanity clamp as the success branch above: do not trust a
            // rejection wireSeq beyond what we've actually sent. The clamped
            // value is only used to attribute an FSN to the error report --
            // a rejection never advances the watermark.
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

            if (policy == SenderError.Policy.TERMINAL) {
                // Terminal: stash the typed payload BEFORE dispatching to the
                // handler. The spec requires signal.terminalError to be latched
                // before the handler is invoked so a handler that synchronously
                // probes getLastTerminalError() (or calls flush()) sees the
                // typed error rather than null. Bytes on disk are the bytes
                // the server rejected; reconnect/replay cannot fix them. The
                // bytes stay in the SF log -- nothing is silently discarded.
                recordFatal(new LineSenderServerException(err));
                dispatchError(err);
                return;
            }
            // RETRIABLE / RETRIABLE_OTHER: never drop, never latch. The bytes
            // stay in the SF log; recycle the wire and replay from ackedFsn+1
            // through the same reconnect machinery a transport failure uses
            // (capped backoff + jitter, endpoint rotation via the reconnect
            // factory, no wall-clock give-up -- Invariant B). The dispatch is
            // informational. A RETRIABLE frame the server keeps rejecting with
            // no ack progress escalates to a poisoned-frame terminal instead
            // of reconnect-looping forever.
            LOG.warn("server rejected wire seq {} (category={}, policy={}, status=0x{}) -- recycling connection, will replay from fsn {}",
                    wireSeq, category, policy, Integer.toHexString(status & 0xFF), engine.ackedFsn() + 1L);
            dispatchError(err);
            LineSenderException recycleCause = new LineSenderException(
                    "server NACK (" + category + ", " + policy + "): "
                            + response.getErrorMessage());
            if (policy == SenderError.Policy.RETRIABLE) {
                // Only RETRIABLE rejections implicate the head frame: the node
                // accepted the stream and judged the bytes, so a deterministic
                // repeat counts toward the poison terminal.
                if (recordHeadRejectionStrike(fsn)) {
                    haltOnPoisonedFrame("server NACK status=0x"
                                    + Integer.toHexString(status & 0xFF) + " (" + category + "): "
                                    + response.getErrorMessage(),
                            fsn);
                    return;
                }
                // RETRIABLE recycles are paced: the server is reachable and
                // healthy (it just NACK'd a frame), so the reconnect will
                // succeed immediately and connectLoop's failed-connect backoff
                // never engages -- without pacing, a repeatedly-NACKing server
                // drives full recycle cycles at its NACK rate.
                failPaced(recycleCause);
            } else {
                // RETRIABLE_OTHER (NOT_WRITABLE): a node-state verdict, not a
                // frame verdict -- the node cannot serve writes at all, so the
                // rejection says nothing about the bytes and must never count
                // a poison strike (a transient all-replica window would
                // otherwise escalate to a producer-fatal terminal, violating
                // Invariant B). Mirror the pre-send path: rotate endpoints
                // through the zero-progress pacer -- the first recycle stays
                // immediate so a genuine failover rotates without delay, but
                // consecutive recycles with no OK progress park for the capped
                // dose instead of churning full TLS reconnects at handshake
                // RTT rate for the whole window.
                failExemptPaced(recycleCause);
            }
        }
    }
}
