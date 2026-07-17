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
import io.questdb.client.cutlass.qwp.client.NativeBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.cutlass.qwp.client.QwpDurableAckMismatchException;
import io.questdb.client.cutlass.qwp.client.QwpIngressRoleRejectedException;
import io.questdb.client.cutlass.qwp.client.QwpRoleMismatchException;
import io.questdb.client.cutlass.qwp.client.QwpVersionMismatchException;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.websocket.WebSocketCloseCode;
import io.questdb.client.std.CharSequenceLongHashMap;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
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
 *       with endpoint-policy failures terminal only for an orphan drainer. A
 *       foreground sender keeps retrying from asynchronous startup onward so
 *       credential and rolling-capability changes remain contained by
 *       store-and-forward. On reconnect success, repositions the cursor at
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
    /**
     * Default minimum wall-clock dwell (millis) a symbol-dict catch-up CAP GAP must
     * persist before an orphan drainer may latch a terminal, even once
     * {@link #MAX_CATCHUP_CAP_GAP_ATTEMPTS} cap gaps have accrued. Same idea as
     * {@link #DEFAULT_POISON_MIN_ESCALATION_WINDOW_MILLIS}, for the same reason: a
     * strike count measures "how many times did we look", not "how long has this been
     * true", and only the latter distinguishes a permanent cluster capability gap from
     * a node that is briefly away.
     * <p>
     * It matters here because the cap gap's trigger IS a failover. With the reconnect
     * backoff capped at {@link #DEFAULT_RECONNECT_MAX_BACKOFF_MILLIS} (5 s), 16 cap
     * gaps accrue in roughly two minutes -- comfortably less than an ordinary rolling
     * restart of the larger-cap node. A count-only budget could therefore quarantine an
     * otherwise drainable orphan slot during a routine cluster operation. Requiring the
     * dwell as well means the orphan terminal needs BOTH "we have looked many times" AND
     * "it has stayed true for a long time". Foreground senders never apply this terminal
     * policy: they retry indefinitely until a larger-cap node returns.
     * <p>
     * 5 minutes -- the same figure as {@link #DEFAULT_RECONNECT_MAX_DURATION_MILLIS},
     * this codebase's existing notion of how long a transient outage may plausibly
     * last. Configurable per sender via the
     * {@code catchup_cap_gap_min_escalation_window_millis} connect-string key or
     * {@code LineSenderBuilder.catchUpCapGapMinEscalationWindowMillis(long)}; {@code 0}
     * restores count-only escalation for orphan drainers.
     */
    public static final long DEFAULT_CATCHUP_CAP_GAP_MIN_ESCALATION_WINDOW_MILLIS = 300_000L;
    private static final Logger LOG = LoggerFactory.getLogger(CursorWebSocketSendLoop.class);
    // Settle budget for the symbol-dict catch-up cap gap: how many cap-gap attempts
    // -- catch-ups that reached a fresh server and found a single dictionary entry
    // too large for its advertised batch cap -- may occur consecutively before an
    // orphan drainer latches a terminal. This is a
    // SANCTIONED orphan-only terminal (a genuine cluster batch-size capability gap),
    // the connect-time analog of the orphan drainer's durable-ack capability gap
    // (DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS). A homogeneous cluster never trips
    // it -- an entry that fit its data frame under a cap always fits its bare catch-up
    // frame under that same cap -- so it only affects a heterogeneous / rolling-cap
    // cluster, where a failover to a smaller-cap node can hit it for an entry an
    // earlier node accepted. A foreground sender retries forever. An orphan drainer
    // rides out the transient window until a larger-cap node returns; only a persistent
    // gap (this many consecutive cap gaps with no successful catch-up or unrelated
    // reconnect state in between) latches.
    //
    // Budget accounting (satisfies "a transient must never burn the terminal
    // budget"): catchUpCapGapAttempts increments ONLY inside sendDictCatchUp when a
    // node is reached and an entry is oversized. A successful catch-up ends the
    // episode, as does any unrelated reconnect state (connect refusal, catch-up send
    // failure, upgrade/role rejection): otherwise its downtime would count toward the
    // wall-clock dwell even though no cap gap was observed. The cap-gap exception
    // itself does NOT reset the episode, so consecutive small-cap nodes still prove a
    // persistent cluster capability gap and can exhaust the orphan policy.
    private static final int MAX_CATCHUP_CAP_GAP_ATTEMPTS = 16;
    // Hard ceiling for the lifetime-monotonic sent-dictionary mirror. The mirror
    // fields are int, so it cannot exceed Integer.MAX_VALUE bytes; reaching even
    // this needs ~200M+ distinct symbols on a single connection, far past any real
    // workload. The guard exists so that pathological growth fails loudly instead
    // of overflowing the int capacity math into a heap-corrupting copyMemory.
    private static final int MAX_SENT_DICT_BYTES = Integer.MAX_VALUE - 8;
    private static final int MAX_SENT_DICT_ENTRIES = MAX_SENT_DICT_BYTES / Integer.BYTES;
    /**
     * Throttle "reconnect attempt N failed" WARN logs to one per 5 s.
     */
    private static final long RECONNECT_LOG_THROTTLE_NANOS = 5_000_000_000L;
    // Test seam: when true, recovery mirror seeding throws immediately AFTER
    // ensureSentDictCapacity has grown (and therefore taken ownership of) the mirror,
    // standing in for the copyRecoveredSymbolSuffix-adjacent failure that leaves a
    // freshly malloc'd mirror with no owner. It sits after the grow deliberately: the
    // constructor's cleanup only frees an OWNED mirror, so a seam before the grow
    // would leave nothing to free and the leak guard would pass with the cleanup
    // deleted. Production never sets it; volatile only so a test thread's write is
    // visible to the loop under test.
    @TestOnly
    static volatile boolean forceMirrorSeedFailureForTest;
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
    // Pre-converted to nanos. Consulted only by the orphan terminal policy. Zero disables
    // the dwell entirely (count-only escalation at MAX_CATCHUP_CAP_GAP_ATTEMPTS); the
    // user-facing 5-minute default is applied at the config layer.
    private final long catchUpCapGapMinEscalationWindowNanos;
    private final CatchUpCapGapPolicy catchUpCapGapPolicy;
    private final ReconnectPolicy reconnectPolicy;
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
    // Delta symbol dictionary catch-up state (see swapClient).
    // ALWAYS active -- in memory mode, in disk mode, and (critically) even when the
    // per-slot persisted dictionary failed to open. sentDictCount is this loop's model
    // of how many ids the CURRENT server has been told about, and the torn-dict guard
    // in trySendOne reads it, so it must track the wire in every mode; gating it on
    // engine.isDeltaDictEnabled() is what once froze it at 0 and terminal'd a slot
    // whose frames replay perfectly from id 0 (see the constructor). On a recovered /
    // orphan-drained slot the constructor SEEDS sentDict* from the persisted
    // dictionary when there is one; otherwise the mirror starts empty and grows from
    // the frames themselves. The loop mirrors, in sentDict*, every symbol it has ever
    // sent -- the concatenated [len varint][utf8] bytes in global-id order
    // (sentDictBytes*) plus the count (sentDictCount) -- so that on reconnect it can
    // re-register the whole dictionary on the fresh server (which discards its
    // dictionary on every disconnect) before replaying frames whose deltas start above
    // id 0. All of this is touched only by the I/O thread.
    // Footprint note: this mirror is a SECOND copy of the dictionary -- the same
    // symbols the producer's GlobalSymbolDictionary already holds as Java Strings --
    // kept as native UTF-8 bytes for the reconnect-catch-up capability. So a
    // memory-mode connection's steady-state dictionary footprint is ~2x the symbol
    // set. It is bounded by distinct-symbol count (not per-row) and never trimmed for
    // the connection's lifetime (a reconnect may need the whole dictionary at any
    // moment), so it cannot be dropped; it is an intentional cost of the feature.
    private long sentDictBytesAddr;
    private int sentDictBytesCapacity;
    private int sentDictBytesLen;
    // False only while an orphan-drainer loop borrows the persisted prefix.
    // Any growth first performs a copy-on-write; only owned buffers are freed.
    private boolean sentDictBytesOwned;
    private int sentDictCount;
    // Relative byte end of every [len varint][utf8] entry in sentDictBytesAddr.
    // The mirror is parsed once as it is built/recovered; reconnect catch-up then
    // reads fixed-width offsets instead of decoding every length varint again.
    private long sentDictEntryEndsAddr;
    private int sentDictEntryEndsCapacity;
    private int sentDictIndexedCount;
    // True when replay frames can start above dictionary id zero and therefore
    // depend on a catch-up on a fresh connection. Delta-enabled live engines
    // always have this dependency. A recovered delta slot whose dictionary
    // failed to open is identified by its non-zero recovered delta start. The
    // remaining false case is full-dictionary fallback: every frame re-registers
    // from id zero, so sending the mirror first would duplicate the dictionary.
    private final boolean hasReplayDictionaryDependency;
    // Reusable staging buffer for the small QWP catch-up prefix (fixed header plus
    // delta range varints). Symbol bytes stay in sentDictBytesAddr and are passed as a
    // second WebSocket payload slice, avoiding a full-dictionary staging copy.
    private long catchUpFrameAddr;
    private int catchUpFrameCapacity;
    private int catchUpEntryIndexBuildCount;
    private int catchUpFrameGrowthCount;
    // Orphan-policy cap-gap attempts -- catch-ups that reached a node and found an entry
    // too large for its batch cap -- with no intervening successful catch-up or unrelated
    // reconnect state (see MAX_CATCHUP_CAP_GAP_ATTEMPTS for the full budget accounting).
    // Foreground retries never increment it. I/O-thread-only.
    private int catchUpCapGapAttempts;
    // System.nanoTime() of the FIRST cap gap of the current orphan-policy episode.
    // Anchors the escalation dwell. Reset together with catchUpCapGapAttempts on a
    // successful catch-up or an unrelated reconnect state, so only an uninterrupted run
    // of cap-gap observations contributes wall-clock dwell. Anchoring at the FIRST gap
    // (not refreshed per attempt) lets consecutive small-cap nodes satisfy the dwell;
    // resetting after transport/role states prevents their downtime from doing so.
    // I/O-thread-only, like catchUpCapGapAttempts.
    //
    // -1 marks "no episode open" for a debugger or a heap dump, but it is NOT the test
    // for one -- catchUpCapGapAttempts == 0 is (see sendDictCatchUp). A nanoTime instant
    // is only meaningful as a difference: its origin is arbitrary and the spec permits
    // negative values, so no state may ride on this field's sign.
    private long catchUpCapGapFirstNanos = -1L;
    // True once a real ring frame (data or commit) has been sent on the CURRENT
    // connection, as opposed to only the dictionary catch-up. The catch-up consumes
    // wire sequences (nextWireSeq), so nextWireSeq > 0 no longer implies "the head
    // frame was sent": onClose's poison-strike gate and handleServerRejection's
    // pre-send gate key off THIS instead. Without it, a transient outage AFTER the
    // catch-up but BEFORE the first data frame (a flapping LB/middlebox that accepts
    // the upgrade + catch-up then closes) would be mistaken for a deterministic
    // head-frame rejection and escalate to a PROTOCOL_VIOLATION terminal -- breaking
    // the store-and-forward "retry a transient outage forever" contract. Reset per
    // connection in setWireBaselineWithCatchUp; set in trySendOne after a successful
    // send.
    private boolean dataFrameSentThisConnection;
    // End position (native address) written by the last readVarintAt() call.
    private long varintEnd;
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
    // successful connect attempt in any mode). Once true, stays true. This is
    // connection-state observability only; reconnect policy is role-based.
    private volatile boolean hasEverConnected;
    private volatile Thread ioThread;
    // Typed marker for a durable-ack CAPABILITY-GAP terminal: set (before the
    // terminalError latch, so a checkError() caller that observes the latch is
    // guaranteed to observe this marker too) when a reconnect sweep threw
    // QwpDurableAckMismatchException. The orphan drainer consults it to route
    // a mid-drain capability gap into its budgeted settle-retry
    // (BackgroundDrainer.connectWithDurableAckRetry) instead of quarantining
    // the slot on the first sweep. Foreground reconnects never set this marker;
    // they keep retrying after a successful initial connection. Write-once alongside
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
     * client. Every endpoint-policy and transport failure stays inside that
     * background retry loop; it is never delivered to the foreground producer
     * (Invariant B: no wall-clock budget give-up). Blocking OFF/SYNC startup
     * performs its fail-fast policy before constructing this loop.
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
     * Twelve-arg overload — omits the symbol-dict cap-gap escalation dwell, which
     * defaults to {@code 0}. This and the thirteen-arg overload use the foreground-safe
     * retry-forever policy; orphan drainers opt into count-and-dwell escalation through
     * the policy-aware master constructor.
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
        this(client, engine, fsnAtZero, parkNanos, reconnectFactory,
                reconnectMaxDurationMillis, reconnectInitialBackoffMillis,
                reconnectMaxBackoffMillis, durableAckMode,
                durableAckKeepaliveIntervalMillis, maxHeadFrameRejections,
                poisonMinEscalationWindowMillis, 0L);
    }

    /**
     * Thirteen-arg overload. Catch-up cap gaps are retried indefinitely, which is the
     * required policy for a foreground store-and-forward sender. Orphan drainers use the
     * policy-aware overload below to opt into bounded terminal escalation.
     * <p>
     * Also accepts the poison-frame detector threshold
     * ({@code max_frame_rejections}): consecutive server-active rejections of
     * the same head-of-line frame, with no ack progress in between, before the
     * loop escalates to a typed terminal. Must be {@code >= 1}. Then the minimum
     * wall-clock dwell (millis) the suspect must stay poisoned before escalation
     * ({@code poison_min_escalation_window_millis}; {@code >= 0}, where 0 = legacy
     * immediate escalation at the threshold).
     * <p>
     * The final argument is the analogous dwell for the symbol-dict catch-up cap gap.
     * It is retained here for source and binary compatibility but is consulted only when
     * the policy-aware overload selects
     * {@link CatchUpCapGapPolicy#TERMINAL_AFTER_SETTLE_BUDGET}.
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
                                   long poisonMinEscalationWindowMillis,
                                   long catchUpCapGapMinEscalationWindowMillis) {
        this(client, engine, fsnAtZero, parkNanos, reconnectFactory,
                reconnectMaxDurationMillis, reconnectInitialBackoffMillis,
                reconnectMaxBackoffMillis, durableAckMode,
                durableAckKeepaliveIntervalMillis, maxHeadFrameRejections,
                poisonMinEscalationWindowMillis, catchUpCapGapMinEscalationWindowMillis,
                CatchUpCapGapPolicy.RETRY_FOREVER);
    }

    /**
     * Compatibility policy-aware constructor. New production call sites should
     * use the {@link ReconnectPolicy} overload below, which names the foreground
     * versus orphan ownership distinction directly.
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
                                   long poisonMinEscalationWindowMillis,
                                   long catchUpCapGapMinEscalationWindowMillis,
                                   CatchUpCapGapPolicy catchUpCapGapPolicy) {
        if (maxHeadFrameRejections < 1) {
            throw new IllegalArgumentException(
                    "maxHeadFrameRejections must be >= 1: " + maxHeadFrameRejections);
        }
        if (poisonMinEscalationWindowMillis < 0) {
            throw new IllegalArgumentException(
                    "poisonMinEscalationWindowMillis must be >= 0: " + poisonMinEscalationWindowMillis);
        }
        if (catchUpCapGapMinEscalationWindowMillis < 0) {
            throw new IllegalArgumentException(
                    "catchUpCapGapMinEscalationWindowMillis must be >= 0: "
                            + catchUpCapGapMinEscalationWindowMillis);
        }
        // TimeUnit conversion saturates at Long.MAX_VALUE. A raw multiply
        // wraps large valid millisecond values negative, which makes the
        // elapsed-time gate appear satisfied as soon as the strike threshold
        // is reached and can quarantine a recoverable orphan slot prematurely.
        this.catchUpCapGapMinEscalationWindowNanos =
                TimeUnit.MILLISECONDS.toNanos(catchUpCapGapMinEscalationWindowMillis);
        if (catchUpCapGapPolicy == null) {
            throw new IllegalArgumentException("catchUpCapGapPolicy must be non-null");
        }
        this.catchUpCapGapPolicy = catchUpCapGapPolicy;
        this.reconnectPolicy = catchUpCapGapPolicy == CatchUpCapGapPolicy.RETRY_FOREVER
                ? ReconnectPolicy.FOREGROUND
                : ReconnectPolicy.ORPHAN;
        if (engine == null) {
            throw new IllegalArgumentException("engine must be non-null");
        }
        if (client == null && reconnectFactory == null) {
            throw new IllegalArgumentException(
                    "client and reconnectFactory cannot both be null");
        }
        this.client = client;
        this.engine = engine;
        this.hasReplayDictionaryDependency = engine.isDeltaDictEnabled()
                || engine.recoveredMaxSymbolDeltaStart() > 0L;
        // Recovery / orphan-drain: the loop starts with a fresh in-memory mirror,
        // so seed it from the slot's persisted dictionary. That way the very first
        // connection re-registers the whole dictionary (via a catch-up frame)
        // before replaying the recovered delta frames.
        //
        // Deliberately NOT gated on engine.isDeltaDictEnabled(). The mirror, the
        // catch-up and the replay guard together are this loop's model of what the
        // SERVER's dictionary holds, and that model must track every mode. Gating
        // them on that flag is what once bricked a recoverable slot: an unopenable
        // .symbol-dict reports isDeltaDictEnabled()=false, yet the frames already on
        // disk are still DELTA frames (deltaStart 0,1,2,...). With the mirror gated
        // off, sentDictCount froze at 0 while the ungated guard kept comparing
        // against it, so the frame at deltaStart=1 tripped a terminal -- even though
        // replaying the whole sequence from id 0 rebuilds the dictionary on the
        // server contiguously and drains perfectly. isDeltaDictEnabled() decides
        // what the PRODUCER emits and persists; it must never decide what this loop
        // tracks. When the dictionary could not be opened, pd is null, the mirror
        // simply starts empty and grows from the frames themselves.
        PersistedSymbolDict pd = engine.getPersistedSymbolDict();
        int persistedPrefixLen = 0;
        if (pd != null && pd.size() > 0) {
            int len = pd.loadedEntriesLen();
            if (len > 0) {
                // Seed by reference. The foreground loop takes ownership after
                // construction succeeds; orphan-drainer loops keep borrowing because
                // one engine can create several sessions during capability-gap
                // recycling. A borrowed mirror performs copy-on-write if a recovered
                // frame suffix must extend it.
                persistedPrefixLen = len;
                sentDictBytesAddr = pd.loadedEntriesAddr();
                sentDictBytesCapacity = len;
                sentDictBytesLen = len;
                // Set the count only alongside the bytes so sentDictCount can
                // never claim symbols the mirror does not hold. A recovered slot
                // always has loadedEntriesLen > 0 when size > 0, so this is the
                // same result -- it just makes the coupling explicit.
                sentDictCount = pd.size();
            }
        }
        // ...and then from the surviving frames' own delta sections, exactly as the producer
        // seeds its dictionary. The mirror is the loop's model of what the SERVER holds and it
        // is what the catch-up frame ships, so when it stops at the persisted prefix while the
        // producer's baseline runs past it, the loop condemns (deltaStart > sentDictCount) a
        // slot the producer just seeded successfully. Same two sources, same order, so the two
        // land on the same number by construction.
        // ...but ONLY when a surviving frame actually depends on ids it does not carry
        // (recoveredMaxSymbolDeltaStart > 0). At zero every frame is self-sufficient and
        // re-registers its dictionary from id 0 as it replays, so seeding the mirror would buy
        // nothing and cost a catch-up frame on every connection -- full-dict slots must stay
        // catch-up-free.
        // The prefix seed above may be borrowed, while the entry-end index is always
        // loop-owned native memory. Build/extend both inside one cleanup boundary:
        // any allocation failure propagates out of the constructor before a caller
        // can own the half-built loop, so nothing else could release them.
        try {
            if (engine.recoveredMaxSymbolDeltaStart() > 0L) {
                int baseline = sentDictCount;
                if (engine.recoveredSymbolCoverage(baseline) >= 0L) {
                    int suffixLen = engine.recoveredSymbolSuffixLen(baseline);
                    int suffixCount = engine.recoveredSymbolSuffixCount(baseline);
                    if (suffixLen > 0) {
                        ensureSentDictCapacity((long) sentDictBytesLen + suffixLen);
                        if (forceMirrorSeedFailureForTest) {
                            // Throw AFTER the grow, never before it. ensureSentDictCapacity
                            // is what copy-on-writes a borrowed prefix into a loop-OWNED
                            // allocation (sentDictBytesOwned = true), and the catch's
                            // releaseSentDictBytes() is gated on exactly that flag. A seam
                            // in front of the grow fires while the mirror is still borrowed
                            // from PersistedSymbolDict, so the cleanup frees nothing and the
                            // guard below passes even with the cleanup deleted -- it would
                            // stop pinning the leak it exists for.
                            throw new LineSenderException(
                                    "simulated mirror seed allocation failure (test only)");
                        }
                        engine.copyRecoveredSymbolSuffix(
                                baseline, sentDictBytesAddr + sentDictBytesLen);
                        sentDictBytesLen += suffixLen;
                        sentDictCount += suffixCount;
                    }
                }
            }
            // The entry-ends index is NOT built here. sendDictCatchUp is its only
            // reader and builds it on demand, so a recovered slot that drains without
            // ever reconnecting -- the normal case -- never pays the O(n) walk nor
            // retains the 4-bytes-per-symbol index.
        } catch (Throwable t) {
            releaseSentDictBytes();
            throw t;
        }
        // QwpWebSocketSender seeds the producer dictionary before constructing
        // its one foreground loop, so after the loop has built its mirror it can
        // retire both engine-owned recovery sources. BackgroundDrainer must retain
        // them for later recycled loops and therefore keeps borrowing them.
        if (reconnectPolicy == ReconnectPolicy.FOREGROUND) {
            if (persistedPrefixLen > 0) {
                long persistedPrefixAddr = pd.takeLoadedEntries();
                if (sentDictBytesOwned) {
                    // Copy-on-grow already produced the combined mirror. Retire the
                    // no-longer-needed persisted prefix after successful construction.
                    Unsafe.free(persistedPrefixAddr, persistedPrefixLen, MemoryTag.NATIVE_DEFAULT);
                } else {
                    assert persistedPrefixAddr == sentDictBytesAddr;
                    sentDictBytesOwned = true;
                }
            }
            engine.releaseRecoveredSymbolStorage();
        }
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
     * Policy-aware master constructor. A foreground sender fails fast while
     * establishing its first connection, then retries endpoint-policy failures
     * indefinitely after it has been live. An orphan drainer returns such failures
     * to its owner so the slot can follow its settle/quarantine policy.
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
                                   long poisonMinEscalationWindowMillis,
                                   long catchUpCapGapMinEscalationWindowMillis,
                                   ReconnectPolicy reconnectPolicy) {
        this(client, engine, fsnAtZero, parkNanos, reconnectFactory,
                reconnectMaxDurationMillis, reconnectInitialBackoffMillis,
                reconnectMaxBackoffMillis, durableAckMode,
                durableAckKeepaliveIntervalMillis, maxHeadFrameRejections,
                poisonMinEscalationWindowMillis, catchUpCapGapMinEscalationWindowMillis,
                catchUpPolicyFor(reconnectPolicy));
    }

    private static CatchUpCapGapPolicy catchUpPolicyFor(ReconnectPolicy reconnectPolicy) {
        if (reconnectPolicy == null) {
            throw new IllegalArgumentException("reconnectPolicy must be non-null");
        }
        return reconnectPolicy == ReconnectPolicy.FOREGROUND
                ? CatchUpCapGapPolicy.RETRY_FOREVER
                : CatchUpCapGapPolicy.TERMINAL_AFTER_SETTLE_BUDGET;
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
        // The symbol-dict mirror (sentDictBytesAddr) is I/O-thread-owned and gets
        // freed on ioLoop's exit path. When t == null the loop never ran (start()
        // was never called, or t.start() failed before committing ioThread), so
        // that free never happens and a seeded (recovery / orphan-drain) mirror
        // would leak. Capture that here; free it below, after client teardown.
        boolean loopNeverRan = t == null;
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
        // Free the I/O-thread-owned symbol-dict mirror ONLY when the loop never
        // ran (see loopNeverRan). If it ran, ioLoop's exit already freed it -- and
        // on the failed-stop path (interrupted latch await, ioThread left set) the
        // thread may still be mid-send, so touching the mirror here would race.
        // A duplicate close observes sentDictBytesAddr == 0 and skips.
        if (loopNeverRan && sentDictBytesAddr != 0) {
            releaseSentDictBytes();
        }
        if (loopNeverRan) {
            freeCatchUpFrameBuffer();
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
        try {
            positionCursorForStart();
        } catch (CatchUpSendException e) {
            // A recovered sender re-registers its dictionary with a catch-up on
            // the very first connect. Here that runs on the CALLER thread (sync
            // start), so we must NOT let it drive connectLoop -- that would block
            // Sender construction forever on a transient outage. Drop the dead
            // client instead: the I/O thread then reconnects via
            // attemptInitialConnect -> swapClient and re-sends the catch-up off
            // this thread. If the failure was already terminal (recordFatal set
            // running=false, e.g. an entry too large for the batch cap), the I/O
            // thread simply winds down and checkError() surfaces it.
            WebSocketClient dead = client;
            client = null;
            if (dead != null) {
                try {
                    dead.close();
                } catch (Throwable ignored) {
                    // best-effort
                }
            }
        }
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
     * Reuses the same retry+backoff machinery as {@link #fail(Throwable)}.
     * Transport, authentication, upgrade and capability failures are all
     * retried indefinitely here (Invariant B); none is surfaced to the
     * foreground producer.
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

    private static boolean isCatchUpCapGap(Throwable t) {
        return t instanceof CatchUpSendException && ((CatchUpSendException) t).capGap;
    }

    private void resetCatchUpCapGapEpisode() {
        catchUpCapGapAttempts = 0;
        catchUpCapGapFirstNanos = -1L;
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
        // A wire/role state that interrupted an orphan cap-gap run is not evidence that
        // the cluster's batch cap stayed incompatible during the outage. Start a fresh
        // episode; the cap-gap exception itself is the one reconnect cause that preserves
        // the existing attempt count and dwell anchor.
        if (!isCatchUpCapGap(initial)) {
            resetCatchUpCapGapEpisode();
        }
        LOG.warn("cursor I/O loop entering {} loop: {}",
                phase, initial.getMessage());
        long outageStartNanos = System.nanoTime();
        // INVARIANT B: a store-and-forward loop must NEVER terminate on a
        // wall-clock reconnect budget. A replica-only / all-endpoints-replica
        // window is TRANSIENT -- a replica gets promoted, a primary reappears --
        // so this background loop retries for as long as it is running, backing
        // off between attempts. Endpoint-policy failures (auth / non-421
        // upgrade / durable-ack capability gap) are terminal only for orphan
        // drainers. Foreground senders retry them from asynchronous startup onward
        // so a credential or cluster capability rotation cannot stop the producer. SF
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
                // A null factory result is an unsuccessful connect state, not a cap-gap
                // observation. Do not let the time spent retrying it satisfy orphan dwell.
                resetCatchUpCapGapEpisode();
            } catch (QwpAuthFailedException | WebSocketUpgradeException e) {
                if (endpointPolicyFailureIsTerminal()) {
                    // Orphans return control to their quarantine owner.
                    // WebSocketUpgradeException reaching here is always non-421:
                    // role rejects are classified into the transient branch below.
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
                }
                resetCatchUpCapGapEpisode();
                lastReconnectError = e;
                long now = System.nanoTime();
                if (now - lastLogNanos >= RECONNECT_LOG_THROTTLE_NANOS) {
                    LOG.warn("{} attempt {}: foreground auth/upgrade policy rejected the connection; "
                                    + "retrying while store-and-forward owns the buffered data -- {}",
                            phase, attempts, e.getMessage());
                    lastLogNanos = now;
                }
            } catch (QwpDurableAckMismatchException e) {
                if (endpointPolicyFailureIsTerminal()) {
                    // Orphans hand a capability gap back to BackgroundDrainer's
                    // settle budget.
                    LOG.error("durable-ack mismatch during {} -- won't retry: {}",
                            phase, e.getMessage());
                    if (terminalError == null) {
                        // Publish the marker before terminalError, which is the
                        // volatile first-writer-wins latch observed by the owner.
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
                }
                resetCatchUpCapGapEpisode();
                lastReconnectError = e;
                long now = System.nanoTime();
                if (now - lastLogNanos >= RECONNECT_LOG_THROTTLE_NANOS) {
                    LOG.warn("{} attempt {}: foreground durable-ack capability is temporarily "
                                    + "unavailable; retrying while store-and-forward owns the buffered data -- {}",
                            phase, attempts, e.getMessage());
                    lastLogNanos = now;
                }
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
                resetCatchUpCapGapEpisode();
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
                if (!isCatchUpCapGap(e)) {
                    resetCatchUpCapGapEpisode();
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
     * Whether an endpoint-policy failure (auth, non-421 upgrade, durable-ack
     * capability gap) latches a terminal instead of retrying under Invariant B.
     * <p>
     * Two callers own a terminal, for different reasons:
     * <ul>
     *   <li>An ORPHAN drainer: a sanctioned terminal that hands the slot back to
     *       its quarantine owner.</li>
     *   <li>A sender still INITIALIZING ({@code !hasEverConnected}): connectivity
     *       problems are the caller's problem during startup and must reach it, so
     *       an operator learns their credentials are wrong instead of watching a
     *       silent sender buffer forever. SYNC/OFF startup reports them by throwing
     *       from {@code build()}; ASYNC startup has no caller left to throw at, so
     *       the latched terminal reaches the user through {@code SenderErrorHandler}
     *       and a {@code close()} rethrow. The constructor seeds
     *       {@code hasEverConnected = client != null}, so SYNC/OFF (which is handed a
     *       live client) is already past initialization here and never latches.</li>
     * </ul>
     * Once a FOREGROUND sender has reached the server even once, initialization is
     * over and store-and-forward owns the buffered data: every endpoint-policy
     * failure is then a transient the drainer must ride out, never a producer-fatal
     * terminal (Invariant B).
     */
    private boolean endpointPolicyFailureIsTerminal() {
        return reconnectPolicy == ReconnectPolicy.ORPHAN || !hasEverConnected;
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
     * forward drainer does not give up on endpoint or transport state). On the
     * first successful reconnect the
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
            // attemptInitialConnect retries every endpoint/transport failure
            // indefinitely under Invariant B and returns only after it installs
            // a client or close() clears `running`. The main loop below sees the
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
            // The symbol-dict mirror is I/O-thread-owned; free it here, on the
            // owning thread's exit path, after the last send that could touch it.
            if (sentDictBytesAddr != 0) {
                releaseSentDictBytes();
            }
            freeCatchUpFrameBuffer();
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
     * This is called at startup and after every reconnect, once
     * {@link #setWireBaselineWithCatchUp} has anchored the wire baseline
     * ({@code fsnAtZero} / {@code nextWireSeq}) -- which may leave {@code nextWireSeq}
     * past the catch-up frames it emitted. This method only positions the byte
     * cursor at {@code targetFsn}; it does not touch the wire mapping.
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
        // Sticky: once the wire is up, we've reached the server at least once
        // for this sender's lifetime. Exposed to the owning sender for
        // connection-state observability, and it ends initialization: from here
        // on endpointPolicyFailureIsTerminal() stops latching endpoint-policy
        // failures on a foreground sender and rides them out instead, because
        // store-and-forward now owns the buffered data (Invariant B).
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
        setWireBaselineWithCatchUp(replayStart);
        positionCursorAt(replayStart);
    }

    /**
     * Sets the wire-sequence baseline for a fresh connection and, when the symbol
     * dictionary mirror is non-empty, emits a full-dictionary catch-up frame first
     * so the fresh server (whose dictionary starts empty) can resolve the
     * non-self-sufficient delta frames that replay next.
     * <p>
     * The catch-up occupies wire seq 0, which maps to the already-acked FSN just
     * below {@code replayStart} (a harmless re-ack); real replay frames then follow
     * from wire seq 1. With nothing to catch up (fresh sender, or full-dict mode),
     * or before a client exists (async initial connect), keep the plain 1:1
     * {@code fsnAtZero == replayStart} mapping; the catch-up then happens on the
     * first real connection via swapClient.
     */
    private void setWireBaselineWithCatchUp(long replayStart) {
        // Fresh connection: no data frame has been sent on it yet. Reset before the
        // catch-up (which sends only dictionary frames) so onClose /
        // handleServerRejection can tell "only the catch-up went out" from "the
        // head data frame went out".
        dataFrameSentThisConnection = false;
        // Do not gate solely on isDeltaDictEnabled(): a recovered delta slot can
        // lose access to its side-file while its on-disk frames still start above
        // zero. hasReplayDictionaryDependency preserves that distinction while
        // keeping full-dictionary fallback catch-up-free across every reconnect.
        if (client != null && sentDictCount > 0 && hasReplayDictionaryDependency) {
            this.nextWireSeq = 0L;
            // The catch-up may span several frames when the dictionary exceeds the
            // server's batch cap; each consumes a wire sequence (0 .. n-1) that maps
            // to an already-acked FSN, so the first real frame still lands on
            // replayStart.
            int catchUpFrames = sendDictCatchUp();
            this.fsnAtZero = replayStart - catchUpFrames;
        } else {
            this.fsnAtZero = replayStart;
            this.nextWireSeq = 0L;
        }
    }

    /**
     * Returns the symbol-dictionary delta start id of a frame, or -1 when the
     * frame carries no delta section. Used by the pre-send torn-dictionary guard.
     */
    private int frameDeltaStart(long payloadAddr, int payloadLen) {
        if (!isDeltaFrame(payloadAddr, payloadLen)) {
            return -1;
        }
        return (int) readVarintAt(payloadAddr + QwpConstants.HEADER_SIZE, payloadAddr + payloadLen);
    }

    // True only for a well-formed QWP frame this encoder produced that carries a
    // delta symbol-dict section. The magic check keeps the dict logic from
    // misreading non-QWP payloads (e.g. synthetic frames injected by tests) whose
    // bytes happen to set the delta flag.
    private static boolean isDeltaFrame(long payloadAddr, int payloadLen) {
        if (payloadLen < QwpConstants.HEADER_SIZE
                || Unsafe.getUnsafe().getInt(payloadAddr) != QwpConstants.MAGIC_MESSAGE) {
            return false;
        }
        byte flags = Unsafe.getUnsafe().getByte(payloadAddr + QwpConstants.HEADER_OFFSET_FLAGS);
        return (flags & QwpConstants.FLAG_DELTA_SYMBOL_DICT) != 0;
    }

    /**
     * Copies the symbol-dictionary delta a just-sent frame carries into the
     * loop-local mirror ({@link #sentDictBytesAddr}) so a future reconnect can
     * re-register it. Frames are sent in FSN order carrying monotonically
     * extending deltas, so a frame whose delta starts exactly at
     * {@link #sentDictCount} extends the mirror; a replayed or empty-delta frame
     * (nothing new) is skipped. Only ever called in delta mode, for a frame the
     * pre-send guard already classified as a delta frame.
     *
     * @param payloadAddr address of the QWP message (12-byte header first)
     * @param payloadLen  message length in bytes
     * @param deltaStart  the frame's delta start id, already decoded by the
     *                    pre-send guard ({@link #frameDeltaStart}) -- passed in so
     *                    the magic/flags and start-id varint are not re-parsed
     */
    private void accumulateSentDict(long payloadAddr, int payloadLen, int deltaStart) {
        long limit = payloadAddr + payloadLen;
        // deltaStart is known (the guard decoded it); locate deltaCount just past
        // its canonical LEB128 encoding rather than re-reading the header and the
        // start-id varint.
        long p = payloadAddr + QwpConstants.HEADER_SIZE + NativeBufferWriter.varintSize(deltaStart);
        long deltaCount = readVarintAt(p, limit);
        p = varintEnd;
        // The mirror holds ids [0, sentDictCount). Accumulate ONLY the part of this
        // frame's delta [deltaStart, deltaStart+deltaCount) that extends past the
        // tip -- ids [sentDictCount, deltaStart+deltaCount). Cases:
        //   - deltaStart > sentDictCount: a gap. trySendOne's torn-dictionary guard
        //     rejects it before send, so it never reaches here; bail defensively
        //     rather than accumulate past a hole.
        //   - deltaEnd <= sentDictCount: a pure replay/overlap we already hold --
        //     nothing new.
        //   - deltaStart <= sentDictCount < deltaEnd: extend the mirror by the tail.
        //     deltaStart == sentDictCount is the steady-state case (skip == 0).
        // Handling the partial overlap explicitly -- rather than dropping the whole
        // frame whenever deltaStart != sentDictCount -- keeps the mirror complete
        // even if a future producer ever emits a delta that overlaps the tip;
        // silently dropping the new tail would leave the reconnect catch-up
        // incomplete and shift server-side ids.
        long deltaEnd = deltaStart + deltaCount;
        if (deltaCount <= 0 || deltaStart > sentDictCount || deltaEnd <= sentDictCount) {
            return;
        }
        // Deliberately does NOT touch the entry-ends index. Only sendDictCatchUp reads
        // it, so maintaining it here would put a store per new symbol on the per-frame
        // I/O path -- and on the workload this feature targets (a new symbol per ROW)
        // that is a store per row, plus 4 bytes per symbol retained for the connection's
        // whole life, to serve a reconnect that may never happen. sendDictCatchUp calls
        // ensureSentDictEntryIndex(), which notices sentDictIndexedCount has fallen
        // behind sentDictCount and rebuilds; that O(n) walk runs at most once per
        // reconnect and is dwarfed by shipping the same n bytes over the wire. A
        // connection that never reconnects now never allocates the index at all.
        //
        // Walk past the already-held prefix [deltaStart, sentDictCount), then copy
        // the new tail [sentDictCount, deltaEnd).
        int skip = sentDictCount - deltaStart;
        for (int i = 0; i < skip; i++) {
            long len = readVarintAt(p, limit);
            p = varintEnd + len;
            if (p > limit) {
                return; // malformed -- bail rather than corrupt the mirror
            }
        }
        long regionStart = p;
        long newCount = deltaEnd - sentDictCount;
        // Walk the new tail only to find where it ends; the entry boundaries are not
        // recorded here (see above -- the catch-up rebuilds them on demand).
        for (long i = 0; i < newCount; i++) {
            long len = readVarintAt(p, limit);
            p = varintEnd + len;
            if (p > limit) {
                // Malformed -- never happens for frames we encoded; bail rather
                // than corrupt the mirror.
                return;
            }
        }
        int regionBytes = (int) (p - regionStart);
        // long sum: sentDictBytesLen + regionBytes can exceed Integer.MAX_VALUE on
        // a very-high-cardinality lifetime connection; ensureSentDictCapacity then
        // fails loudly rather than overflowing to a negative int (which would make
        // the capacity check pass and copyMemory scribble past the buffer).
        ensureSentDictCapacity((long) sentDictBytesLen + regionBytes);
        Unsafe.getUnsafe().copyMemory(regionStart, sentDictBytesAddr + sentDictBytesLen, regionBytes);
        sentDictBytesLen += regionBytes;
        sentDictCount += (int) newCount;
    }

    private void ensureSentDictCapacity(long required) {
        if (sentDictBytesCapacity >= required) {
            return;
        }
        if (required > MAX_SENT_DICT_BYTES) {
            // Latch a terminal, do NOT just throw: accumulateSentDict runs AFTER
            // the frame's sendBinary, so a bare throw unwinds to ioLoop -> fail()
            // -> connectLoop, which (running still true) reconnects and replays the
            // same frame, which re-overflows the never-shrinking mirror -- an
            // unbounded reconnect livelock rather than the "fails loudly" the
            // MAX_SENT_DICT_BYTES ceiling promises. recordFatal flips running=false
            // so connectLoop's !running guard winds the loop down and checkError()
            // surfaces the terminal, matching sendCatchUpChunk's guard for the same
            // ceiling. The throw still unwinds past the pending copyMemory.
            LineSenderException err = new LineSenderException("symbol dictionary mirror exceeds the maximum size ["
                    + "required=" + required + ", max=" + MAX_SENT_DICT_BYTES + ']');
            recordFatal(err);
            throw err;
        }
        // Grow in long to avoid the capacity*2 int overflow (negative) that would
        // otherwise degrade the doubling near 1 GB; clamp to the int ceiling.
        long newCap = Math.max((long) sentDictBytesCapacity * 2, Math.max(4096L, required));
        if (newCap > MAX_SENT_DICT_BYTES) {
            newCap = MAX_SENT_DICT_BYTES;
        }
        if (sentDictBytesOwned) {
            sentDictBytesAddr = Unsafe.realloc(
                    sentDictBytesAddr,
                    sentDictBytesCapacity,
                    (int) newCap,
                    MemoryTag.NATIVE_DEFAULT);
        } else {
            long newAddr = Unsafe.malloc((int) newCap, MemoryTag.NATIVE_DEFAULT);
            if (sentDictBytesLen > 0) {
                Unsafe.getUnsafe().copyMemory(sentDictBytesAddr, newAddr, sentDictBytesLen);
            }
            sentDictBytesAddr = newAddr;
            sentDictBytesOwned = true;
        }
        sentDictBytesCapacity = (int) newCap;
    }

    private void releaseSentDictBytes() {
        if (sentDictBytesAddr != 0 && sentDictBytesOwned) {
            Unsafe.free(sentDictBytesAddr, sentDictBytesCapacity, MemoryTag.NATIVE_DEFAULT);
        }
        if (sentDictEntryEndsAddr != 0L) {
            Unsafe.free(
                    sentDictEntryEndsAddr,
                    sentDictEntryEndsCapacity * Integer.BYTES,
                    MemoryTag.NATIVE_DEFAULT);
        }
        sentDictBytesAddr = 0;
        sentDictBytesCapacity = 0;
        sentDictBytesLen = 0;
        sentDictBytesOwned = false;
        sentDictCount = 0;
        sentDictEntryEndsAddr = 0L;
        sentDictEntryEndsCapacity = 0;
        sentDictIndexedCount = 0;
    }

    private void ensureSentDictEntryEndsCapacity(long required) {
        if (required <= sentDictEntryEndsCapacity) {
            return;
        }
        if (required > MAX_SENT_DICT_ENTRIES) {
            LineSenderException err = new LineSenderException(
                    "symbol dictionary entry index exceeds the maximum size [required="
                            + required + ", max=" + MAX_SENT_DICT_ENTRIES + ']');
            recordFatal(err);
            throw err;
        }
        long newCapacity = Math.max(
                (long) sentDictEntryEndsCapacity * 2L,
                Math.max(1024L, required));
        if (newCapacity > MAX_SENT_DICT_ENTRIES) {
            newCapacity = MAX_SENT_DICT_ENTRIES;
        }
        sentDictEntryEndsAddr = Unsafe.realloc(
                sentDictEntryEndsAddr,
                sentDictEntryEndsCapacity * Integer.BYTES,
                (int) newCapacity * Integer.BYTES,
                MemoryTag.NATIVE_DEFAULT);
        sentDictEntryEndsCapacity = (int) newCapacity;
    }

    private void ensureSentDictEntryIndex() {
        if (sentDictIndexedCount != sentDictCount) {
            rebuildSentDictEntryIndex();
        }
    }

    private void rebuildSentDictEntryIndex() {
        ensureSentDictEntryEndsCapacity(sentDictCount);
        long p = sentDictBytesAddr;
        long limit = sentDictBytesAddr + sentDictBytesLen;
        for (int i = 0; i < sentDictCount; i++) {
            long len = readVarintAt(p, limit);
            p = varintEnd + len;
            if (p > limit) {
                throw new LineSenderException(
                        "invalid symbol dictionary mirror while building the entry index [entry="
                                + i + ", count=" + sentDictCount + ']');
            }
            Unsafe.getUnsafe().putInt(
                    sentDictEntryEndsAddr + (long) i * Integer.BYTES,
                    (int) (p - sentDictBytesAddr));
        }
        if (p != limit) {
            throw new LineSenderException(
                    "invalid symbol dictionary mirror length while building the entry index [indexed="
                            + (p - sentDictBytesAddr) + ", length=" + sentDictBytesLen + ']');
        }
        sentDictIndexedCount = sentDictCount;
        catchUpEntryIndexBuildCount++;
    }

    private long readVarintAt(long p, long limit) {
        long value = 0;
        int shift = 0;
        long cur = p;
        while (cur < limit) {
            byte b = Unsafe.getUnsafe().getByte(cur++);
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                break;
            }
            shift += 7;
            if (shift > 35) {
                // Defensive bound, matching PersistedSymbolDict.decodeVarint: a
                // canonical entry-length / delta varint is <= 5 bytes. Every caller
                // reads freshly-encoded, CRC- or openExisting-validated bytes, so
                // this is unreachable, but it stops a corrupt continuation run from
                // over-shifting into a garbage length.
                break;
            }
        }
        varintEnd = cur;
        return value;
    }

    /**
     * Re-registers the whole symbol dictionary on a fresh connection, split into
     * as many table-less frames as the server's advertised batch cap requires so
     * no single frame exceeds it (a large dictionary would otherwise be rejected).
     * Each chunk carries a contiguous id range {@code [start .. start+count)}, in
     * order, so the server accumulates them exactly as it would the original
     * per-frame deltas. Returns the number of frames sent (each consumed a wire
     * sequence), so the caller can align {@code fsnAtZero}. Throws {@link
     * CatchUpSendException} on a send error (retriable -- the caller reconnects);
     * a single entry too large for the cap is non-retriable, so it latches a
     * terminal before throwing.
     */
    private int sendDictCatchUp() {
        int cap = client.getServerMaxBatchSize();
        long fullFrameLen = QwpConstants.HEADER_SIZE
                + NativeBufferWriter.varintSize(0)
                + NativeBufferWriter.varintSize(sentDictCount)
                + (long) sentDictBytesLen;
        if (cap <= 0 && fullFrameLen <= MAX_SENT_DICT_BYTES) {
            sendCatchUpChunk(0, sentDictCount, sentDictBytesAddr, sentDictBytesLen);
            resetCatchUpCapGapEpisode();
            return 1;
        }
        ensureSentDictEntryIndex();
        // The frame ceiling a catch-up chunk must not exceed: the server's
        // advertised cap, or -- when the server advertises none (cap <= 0) --
        // MAX_SENT_DICT_BYTES so sendCatchUpChunk's int frameLen (HEADER_SIZE +
        // varints + symbolsLen) cannot overflow on a pathological multi-GB
        // dictionary (unreachable at real cardinality; defensive). Used by the
        // single-entry terminal below, which measures the real solo frame.
        int frameLimit = cap > 0 ? cap : MAX_SENT_DICT_BYTES;
        // Symbol-bytes budget for PACKING several entries into one chunk, leaving
        // room for the 12-byte header and the two delta-section varints. Kept
        // deliberately conservative (reserving 16 for the varints): it only makes a
        // multi-entry chunk split marginally earlier, never over the cap. It must
        // NOT gate the single-entry terminal -- that reserve is larger than the
        // minimal data-frame overhead, so an entry the producer already shipped
        // under this cap could exceed the reserve yet still fit its own catch-up
        // frame; the terminal tests the real solo frame against frameLimit instead.
        int budget = cap > 0
                ? Math.max(1, cap - QwpConstants.HEADER_SIZE - 16)
                : MAX_SENT_DICT_BYTES - QwpConstants.HEADER_SIZE - 16;
        int framesSent = 0;
        int chunkStartId = 0;
        long chunkStartAddr = sentDictBytesAddr;
        int chunkSymbols = 0;
        long chunkBytes = 0;
        for (int entryId = 0; entryId < sentDictCount; entryId++) {
            int entryStartOffset = entryId == 0
                    ? 0
                    : Unsafe.getUnsafe().getInt(
                            sentDictEntryEndsAddr + (long) (entryId - 1) * Integer.BYTES);
            int entryEndOffset = Unsafe.getUnsafe().getInt(
                    sentDictEntryEndsAddr + (long) entryId * Integer.BYTES);
            long entryStart = sentDictBytesAddr + entryStartOffset;
            long entryBytes = entryEndOffset - entryStartOffset;
            // The exact table-less frame sendCatchUpChunk would build for THIS entry
            // alone: header + deltaStart varint (the entry's own global id) +
            // deltaCount varint (1) + the entry bytes. A cap gap exists only when even
            // that solo frame exceeds the cap -- i.e. the entry genuinely cannot be
            // re-registered. Testing the real solo frame (not the conservative
            // packing budget above) is what keeps a HOMOGENEOUS cluster
            // livelock-free: an entry the producer already shipped in a data frame
            // under this cap (header + delta varints + entry + schema + >=1 row) is
            // strictly larger than its bare catch-up frame, so it always fits here.
            long soloFrameLen = QwpConstants.HEADER_SIZE
                    + NativeBufferWriter.varintSize(chunkStartId + chunkSymbols)
                    + NativeBufferWriter.varintSize(1)
                    + entryBytes;
            if (soloFrameLen > frameLimit) {
                // Cap gap: this entry cannot be re-registered under the fresh
                // server's advertised cap. A HOMOGENEOUS cluster never reaches here
                // (an entry that fit its data frame under a cap always fits its bare
                // catch-up frame under that same cap), so the only way in is a
                // heterogeneous / rolling-cap failover to a smaller-cap node.
                //
                // A foreground sender retries indefinitely: store-and-forward must
                // contain a transient heterogeneous-cluster window instead of surfacing
                // it to the producer. Only an orphan drainer may apply the settle budget
                // below and quarantine its slot after a persistent gap. Under budget the
                // throw is RETRIABLE (no recordFatal) -- connectLoop reconnects with
                // backoff and re-runs the catch-up, which resets the episode on a node
                // that accepts it. An unrelated transport/upgrade/role state also resets
                // the episode, so its downtime cannot satisfy the orphan dwell. On
                // exhaustion latch via recordFatal, NOT fail() --
                // failing from inside the catch-up would re-enter connectLoop (see
                // CatchUpSendException); the data must be resent after the cap is raised.
                // Escalation needs BOTH the strike count AND a minimum wall-clock dwell
                // (see DEFAULT_CATCHUP_CAP_GAP_MIN_ESCALATION_WINDOW_MILLIS). A count
                // alone measures how often we looked, not how long the gap has held --
                // and 16 attempts at the capped backoff take only ~2 minutes, less than
                // an ordinary rolling restart of the larger-cap node. Escalating on the
                // count alone would therefore hard-fail a live store-and-forward
                // producer during a routine cluster operation.
                //
                // The STRIKE COUNT -- never the anchor's sign -- says whether an episode is
                // already open. A System.nanoTime() instant is only meaningful as a
                // difference: its origin is arbitrary and the spec permits negative values,
                // so a sign test cannot tell an unset anchor from a real one. Wherever it
                // misreads a real anchor as unset it re-anchors to now on EVERY strike,
                // pinning episodeNanos at ~0, and the dwell can then never be satisfied --
                // the terminal never latches, however long the gap truly persists.
                // recordHeadRejectionStrike keys its episode off poisonStrikes for the same
                // reason.
                if (catchUpCapGapPolicy == CatchUpCapGapPolicy.RETRY_FOREVER) {
                    throw new CatchUpSendException(new LineSenderException(
                            "symbol dictionary entry too large for the server batch cap during catch-up ["
                                    + "frameLen=" + soloFrameLen + ", cap=" + cap + ']'
                                    + "; retrying indefinitely -- a larger-cap node may return"), true);
                }

                long nowNanos = System.nanoTime();
                if (catchUpCapGapAttempts == 0) {
                    catchUpCapGapFirstNanos = nowNanos;
                }
                catchUpCapGapAttempts++;
                long episodeNanos = nowNanos - catchUpCapGapFirstNanos;
                boolean exhausted = catchUpCapGapAttempts >= MAX_CATCHUP_CAP_GAP_ATTEMPTS
                        && episodeNanos >= catchUpCapGapMinEscalationWindowNanos;
                LineSenderException err = new LineSenderException(
                        "symbol dictionary entry too large for the server batch cap during catch-up ["
                                + "frameLen=" + soloFrameLen + ", cap=" + cap + ", attempt="
                                + catchUpCapGapAttempts + '/' + MAX_CATCHUP_CAP_GAP_ATTEMPTS
                                + ", episodeMillis=" + (episodeNanos / 1_000_000L)
                                + '/' + (catchUpCapGapMinEscalationWindowNanos / 1_000_000L) + ']'
                                + (exhausted
                                ? "; the data must be resent after the cap is raised"
                                : "; retrying -- a larger-cap node may return"));
                if (exhausted) {
                    recordFatal(err);
                }
                throw new CatchUpSendException(err, true);
            }
            if (chunkSymbols > 0 && chunkBytes + entryBytes > budget) {
                sendCatchUpChunk(chunkStartId, chunkSymbols, chunkStartAddr, (int) chunkBytes);
                framesSent++;
                chunkStartId += chunkSymbols;
                chunkStartAddr = entryStart;
                chunkSymbols = 0;
                chunkBytes = 0;
            }
            chunkSymbols++;
            chunkBytes += entryBytes;
        }
        if (chunkSymbols > 0) {
            sendCatchUpChunk(chunkStartId, chunkSymbols, chunkStartAddr, (int) chunkBytes);
            framesSent++;
        }
        // The whole dictionary re-registered without a cap gap: this node accepts
        // every entry, so the cap-gap episode (if any) is over -- reset BOTH the strike
        // count and the wall-clock anchor. Resetting only the count would leave a stale
        // anchor from an old episode, so the very first strike of a LATER episode would
        // already satisfy the dwell.
        resetCatchUpCapGapEpisode();
        return framesSent;
    }

    /**
     * Sends one table-less catch-up frame carrying dictionary ids
     * {@code [deltaStart .. deltaStart+deltaCount)}. Throws {@link
     * CatchUpSendException} on a send error instead of calling {@link #fail}
     * (see that type for why the catch-up must not re-enter the reconnect loop);
     * the caller turns it into a single, non-re-entrant reconnect.
     */
    private void sendCatchUpChunk(int deltaStart, int deltaCount, long symbolsAddr, int symbolsLen) {
        // Compute the frame size in long and fail loud if it would overflow the int
        // size math into a negative Unsafe.malloc. sendDictCatchUp already caps each
        // chunk's symbol bytes under the budget, so this is unreachable at real
        // cardinality -- but the mirror-side ensureSentDictCapacity guards the same
        // math, and a future caller must not be able to overflow this one silently.
        long payloadLenL = (long) NativeBufferWriter.varintSize(deltaStart)
                + NativeBufferWriter.varintSize(deltaCount)
                + symbolsLen;
        long frameLenL = QwpConstants.HEADER_SIZE + payloadLenL;
        if (frameLenL > MAX_SENT_DICT_BYTES) {
            LineSenderException err = new LineSenderException(
                    "symbol dictionary catch-up frame exceeds the maximum size ["
                            + "frameLen=" + frameLenL + ", max=" + MAX_SENT_DICT_BYTES + ']');
            recordFatal(err);
            throw new CatchUpSendException(err);
        }
        int payloadLen = (int) payloadLenL;
        int prefixLen = QwpConstants.HEADER_SIZE
                + NativeBufferWriter.varintSize(deltaStart)
                + NativeBufferWriter.varintSize(deltaCount);
        ensureCatchUpFrameCapacity(prefixLen);
        long frame = catchUpFrameAddr;
        try {
            Unsafe.getUnsafe().putByte(frame, (byte) 'Q');
            Unsafe.getUnsafe().putByte(frame + 1, (byte) 'W');
            Unsafe.getUnsafe().putByte(frame + 2, (byte) 'P');
            Unsafe.getUnsafe().putByte(frame + 3, (byte) '1');
            Unsafe.getUnsafe().putByte(frame + 4, (byte) client.getServerQwpVersion());
            // FLAG_DEFER_COMMIT: the catch-up carries dictionary entries but NO
            // rows, so it must never trigger a server-side commit. Today it is
            // always the first frame on a fresh (empty-transaction) connection, so
            // committing nothing is a no-op -- but that invariant is load-bearing
            // and unasserted. Deferring the (empty) commit removes the dependency:
            // a future mid-stream catch-up cannot prematurely commit an in-flight
            // deferred transaction. The dictionary delta still registers (as any
            // deferred data frame's does); only the row commit is deferred, and the
            // next real frame commits it.
            Unsafe.getUnsafe().putByte(frame + QwpConstants.HEADER_OFFSET_FLAGS,
                    (byte) (QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_DELTA_SYMBOL_DICT
                            | QwpConstants.FLAG_DEFER_COMMIT));
            Unsafe.getUnsafe().putShort(frame + 6, (short) 0); // tableCount
            Unsafe.getUnsafe().putInt(frame + 8, payloadLen);
            long q = NativeBufferWriter.writeVarint(frame + QwpConstants.HEADER_SIZE, deltaStart);
            NativeBufferWriter.writeVarint(q, deltaCount);
            client.sendBinary(frame, prefixLen, symbolsAddr, symbolsLen);
        } catch (Throwable t) {
            // Do NOT fail() here -- see CatchUpSendException. Signal the failure
            // up so exactly one non-re-entrant reconnect follows. A JVM Error is
            // never a transient reconnect case; let it propagate as-is so the
            // I/O loop latches it terminal rather than looping on it.
            if (t instanceof Error) {
                throw (Error) t;
            }
            throw new CatchUpSendException(t);
        }
        nextWireSeq++; // this catch-up chunk consumed a wire sequence
        lastFrameOrPingNanos = System.nanoTime();
        totalFramesSent.incrementAndGet();
    }

    @TestOnly
    public int catchUpEntryIndexBuildCount() {
        return catchUpEntryIndexBuildCount;
    }

    @TestOnly
    public int catchUpFrameGrowthCount() {
        return catchUpFrameGrowthCount;
    }

    private void ensureCatchUpFrameCapacity(int required) {
        if (catchUpFrameCapacity >= required) {
            return;
        }
        long newCapacity = Math.max(required, Math.max(4096L, (long) catchUpFrameCapacity * 2L));
        if (newCapacity > MAX_SENT_DICT_BYTES) {
            newCapacity = MAX_SENT_DICT_BYTES;
        }
        catchUpFrameAddr = Unsafe.realloc(
                catchUpFrameAddr,
                catchUpFrameCapacity,
                (int) newCapacity,
                MemoryTag.NATIVE_DEFAULT);
        catchUpFrameCapacity = (int) newCapacity;
        catchUpFrameGrowthCount++;
    }

    private void freeCatchUpFrameBuffer() {
        if (catchUpFrameAddr != 0L) {
            Unsafe.free(catchUpFrameAddr, catchUpFrameCapacity, MemoryTag.NATIVE_DEFAULT);
            catchUpFrameAddr = 0L;
            catchUpFrameCapacity = 0;
        }
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
                try {
                    positionCursorForStart();
                } catch (CatchUpSendException e) {
                    // Re-anchor's catch-up send failed. fail() here is a fresh,
                    // non-re-entrant connectLoop entry from the I/O loop body --
                    // the same recovery a normal trySendOne send failure takes.
                    fail(e.getCause());
                    return false;
                }
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
        long frameAddr = base + sendOffset + MmapSegment.FRAME_HEADER_SIZE;
        // Torn-dictionary guard. sentDictCount is this loop's model of how many ids
        // the CURRENT server has been told about: seeded from the persisted
        // dictionary, re-registered by the catch-up, and extended by
        // accumulateSentDict as each frame goes out. A frame whose delta starts ABOVE
        // that coverage references ids the server was never given; replaying it would
        // make the server null-pad the hole (QwpMessageCursor grows the dict with
        // nulls to deltaStart+deltaCount) and land rows with SILENTLY NULL symbols.
        // Fail terminally instead; the unreplayable data must be resent.
        //
        // The guard, the mirror and the catch-up are ONE mechanism and are all
        // ungated (see the constructor). A frame at deltaStart == sentDictCount is
        // contiguous and extends the coverage, so a slot whose frames start at id 0
        // replays cleanly with no persisted dictionary at all -- which is exactly why
        // the mirror below must keep advancing even when the dictionary is missing.
        // A gap here therefore means genuine loss: a host/power crash tore the
        // unsynced .symbol-dict below the ids the surviving frames still reference
        // (SF is process-crash but not host-crash durable).
        int deltaStart = frameDeltaStart(frameAddr, payloadLen);
        if (deltaStart > sentDictCount) {
            recordFatal(new LineSenderException(
                    "recovered store-and-forward symbol dictionary is incomplete (likely a host crash): "
                            + "frame delta start " + deltaStart + " exceeds recovered dictionary size "
                            + sentDictCount + "; cannot replay without corrupting data -- resend required"));
            return false;
        }
        try {
            client.sendBinary(frameAddr, payloadLen);
        } catch (Throwable t) {
            fail(t);
            return false;
        }
        // A real ring frame (data or commit) has now gone out on this connection,
        // as opposed to only the dictionary catch-up. onClose / handleServerRejection
        // key their poison-strike vs pre-send decision off this, not off nextWireSeq
        // (which the catch-up advances).
        dataFrameSentThisConnection = true;
        if (deltaStart >= 0) {
            // Mirror the symbols this frame just registered on the server, so a later
            // reconnect can rebuild the whole dictionary and the guard above keeps an
            // accurate view of the server's coverage. Idempotent on replay: a frame
            // whose delta we already hold advances nothing.
            //
            // Ungated (see the constructor): this is the ONLY thing that advances
            // sentDictCount from the frames themselves, so gating it while leaving the
            // guard ungated froze the coverage at 0 and terminal'd a slot that replays
            // perfectly from id 0.
            accumulateSentDict(frameAddr, payloadLen, deltaStart);
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
        // Recovery / orphan-drain seed the dictionary mirror, so the initial
        // connection may also need a catch-up (client is non-null in the
        // sync-start and drainer paths; null in async-initial, where swapClient
        // handles it on the first connect).
        setWireBaselineWithCatchUp(replayStart);
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
        if (!engine.retireRecoveredOrphanTailIfReady()) {
            return false;
        }
        orphanSkipStartFsn = -1L;
        orphanSkipTipFsn = -1L;
        return true;
    }

    /**
     * Determines whether an oversized symbol-dictionary catch-up entry is always
     * retriable or may become terminal after the orphan settle budget.
     */
    public enum CatchUpCapGapPolicy {
        /** Keep reconnecting until a node with a compatible batch cap returns. */
        RETRY_FOREVER,
        /** Quarantine an orphan slot after both the attempt and dwell limits expire. */
        TERMINAL_AFTER_SETTLE_BUDGET
    }

    /** Identifies who owns data while the reconnect loop is unavailable. */
    public enum ReconnectPolicy {
        /** A live producer keeps buffering and retries endpoint-policy failures. */
        FOREGROUND,
        /** An orphan drainer returns terminal states to its quarantine owner. */
        ORPHAN
    }

    /**
     * Factory used by the I/O loop to build a fresh, connected, upgraded
     * {@link WebSocketClient} after a wire failure. Implementations close
     * the old client (if needed), build a new one with the same auth/TLS
     * config, connect, perform the WebSocket upgrade, and return it ready
     * to send. The loop's {@link ReconnectPolicy} decides whether endpoint-policy
     * failures are retried or returned as terminal.
     */
    @FunctionalInterface
    public interface ReconnectFactory {
        WebSocketClient reconnect() throws Exception;
    }

    /**
     * Signals that a symbol-dictionary catch-up frame could not be sent on the
     * current connection. Thrown by {@link #sendDictCatchUp}/{@link
     * #sendCatchUpChunk} instead of calling {@link #fail}: the catch-up runs
     * inside {@link #connectLoop} (via {@link #swapClient}) and, on the initial
     * connect, inside {@link #start()} / {@link #trySendOne} on the caller
     * thread. Calling {@code fail()} from there would re-enter {@code
     * connectLoop} -- corrupting the {@code fsnAtZero}/{@code nextWireSeq} wire
     * mapping (a subsequent ACK then trims un-acked frames) and growing the
     * stack until it overflows into a terminal, breaking the "retry a transient
     * outage forever" invariant -- or run {@code connectLoop} on the caller
     * thread and block {@code Sender} construction indefinitely. Each catch
     * site instead turns it into ONE non-re-entrant reconnect: {@code
     * connectLoop}'s own retry catch (swapClient path), a fresh {@code fail()}
     * from the I/O loop body (trySendOne path), or dropping the dead client so
     * the I/O thread reconnects (start path). A JVM {@code Error} is never
     * wrapped -- it must stay terminal. The {@code capGap} marker lets the reconnect
     * loop preserve only consecutive incompatible-cap observations; ordinary catch-up
     * send failures restart the orphan settle episode.
     */
    private static final class CatchUpSendException extends RuntimeException {
        private final boolean capGap;

        CatchUpSendException(Throwable cause) {
            this(cause, false);
        }

        CatchUpSendException(Throwable cause, boolean capGap) {
            super(cause);
            this.capGap = capGap;
        }
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
            if (!orderly && dataFrameSentThisConnection) {
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
            if (!dataFrameSentThisConnection) {
                // Pre-send rejection: the server emitted an error frame before we
                // sent any DATA frame on this connection (typical after a fresh
                // swapClient -- auth failure, server-initiated halt, or a rejection
                // of the dictionary catch-up itself). nextWireSeq may be > 0 here
                // because the catch-up consumed wire sequences, so this keys off
                // dataFrameSentThisConnection, not highestSent >= 0 -- otherwise a
                // transient NACK of a catch-up frame would take the post-send
                // poison-strike path and could escalate a transient outage to a
                // terminal. The server-named wireSeq does not correspond to any
                // data frame we sent, so clamping it to 0 and acknowledging
                // fsnAtZero would silently advance ackedFsn past a real unsent
                // batch. Skip the watermark advance entirely; still surface the
                // error so the user's handler sees it and HALT errors remain
                // producer-observable.
                handlePreSendRejection(wireSeq, status, category, policy);
                return;
            }
            long cappedSeq = Math.max(0L, Math.min(wireSeq, highestSent));
            if (cappedSeq < wireSeq) {
                LOG.warn("server NACK wire seq {} exceeds highest sent {}, clamping",
                        wireSeq, highestSent);
            }
            long fsn = fsnAtZero + cappedSeq;
            if (fsn <= engine.ackedFsn()) {
                // The clamped wire seq maps at or below the replay head, so this
                // NACK is for a dictionary catch-up frame -- which occupies the
                // already-acked wire sequences below replayStart -- not a data frame
                // this connection sent. (dataFrameSentThisConnection can be true here
                // because trySendOne ships the head data frame before tryReceiveAcks
                // reads the catch-up's NACK in the same loop iteration.) Attributing
                // it a data FSN would key recordHeadRejectionStrike() off a
                // below-baseline FSN -- negative when replayStart < catchUpFrames --
                // colliding with the poisonFsn == -1 "no suspect" sentinel and
                // laundering a genuine poison run, and would report a bogus FSN.
                // Treat it exactly like a pre-send rejection: surface + recycle, no
                // poison strike, no watermark advance. Symmetric with the success
                // path, where engine.acknowledge() no-ops at or below ackedFsn. A
                // real replayed data frame is at fsn > ackedFsn, so it is never
                // caught here.
                //
                // Catch-up frames therefore sit OUTSIDE the poison detector: a
                // deterministically-NACKed catch-up recycles forever (paced, so no
                // busy-loop). That is acceptable -- a catch-up only re-registers
                // symbols the cluster already accepted, so a persistent NACK of one
                // is a server bug, not a poison-frame the client can quarantine, and
                // Invariant B's "retry a transient outage forever" applies.
                handlePreSendRejection(wireSeq, status, category, policy);
                return;
            }
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
