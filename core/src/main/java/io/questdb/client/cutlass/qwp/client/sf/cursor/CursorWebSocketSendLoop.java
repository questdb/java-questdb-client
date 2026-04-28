/*******************************************************************************
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

import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Errors are reported via {@link #getLastError()}; the I/O thread sets it
 * and exits. Producers polling {@link #checkError()} surface the failure.
 */
public final class CursorWebSocketSendLoop implements QuietCloseable {

    public static final long DEFAULT_PARK_NANOS = 50_000L; // 50us idle backoff
    /** Default per-outage reconnect time cap (5 min). */
    public static final long DEFAULT_RECONNECT_MAX_DURATION_MILLIS = 300_000L;
    /** Default initial reconnect backoff (100 ms). */
    public static final long DEFAULT_RECONNECT_INITIAL_BACKOFF_MILLIS = 100L;
    /** Default reconnect max backoff (5 s). */
    public static final long DEFAULT_RECONNECT_MAX_BACKOFF_MILLIS = 5_000L;
    /** Throttle "reconnect attempt N failed" WARN logs to one per 5 s. */
    private static final long RECONNECT_LOG_THROTTLE_NANOS = 5_000_000_000L;
    private static final Logger LOG = LoggerFactory.getLogger(CursorWebSocketSendLoop.class);

    private final AtomicLong consecutiveSendErrors = new AtomicLong();
    private final CursorSendEngine engine;
    private final long parkNanos;
    private final WebSocketResponse response = new WebSocketResponse();
    private final ResponseHandler responseHandler = new ResponseHandler();
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final AtomicLong totalAcks = new AtomicLong();
    private final AtomicLong totalFramesSent = new AtomicLong();
    private final AtomicLong totalReconnects = new AtomicLong();
    // Every iteration of the reconnect loop bumps this — failures and
    // success alike. Diverges from totalReconnects (success-only) when the
    // server is flapping. Useful for "is reconnect making progress?"
    // observability.
    private final AtomicLong totalReconnectAttempts = new AtomicLong();
    // Frames sent during the post-reconnect catch-up window — i.e. frames
    // whose FSN was already published before the wire dropped. A non-zero
    // value confirms replay is working; a sustained nonzero rate means
    // the connection is flapping and replay is doing real work each cycle.
    private final AtomicLong totalFramesReplayed = new AtomicLong();
    // Set at swapClient time to publishedFsn at that moment; cleared back
    // to -1 once trySendOne has caught up past it. Used to count replay
    // frames without a per-frame branch on the steady-state path.
    private long replayTargetFsn = -1L;
    // Optional reconnect plumbing. When non-null, a wire failure triggers a
    // reconnect attempt instead of a terminal fail(). The factory produces a
    // fresh, connected+upgraded WebSocketClient.
    private final ReconnectFactory reconnectFactory;
    private final long reconnectMaxDurationMillis;
    private final long reconnectInitialBackoffMillis;
    private final long reconnectMaxBackoffMillis;
    private WebSocketClient client;
    // fsnAtZero: FSN that wireSeq=0 maps to on the current connection. For
    // a fresh connection, this is 0. After a reconnect, it's set to
    // engine.ackedFsn() + 1 — the first frame we replay maps to wireSeq=0
    // on the new connection so server-side ACK math stays aligned.
    private long fsnAtZero;
    // sendingSegment: the segment we're currently consuming bytes from. Starts
    // at engine.activeSegment(); advances to newer sealed segments / the new
    // active as the producer rotates.
    private MmapSegment sendingSegment;
    // sendOffset: byte offset inside sendingSegment of the first not-yet-sent
    // byte. Initialized to MmapSegment.HEADER_SIZE on a fresh segment.
    private long sendOffset = MmapSegment.HEADER_SIZE;
    private long nextWireSeq;
    private volatile boolean running;
    private volatile Throwable lastError;
    private Thread ioThread;

    public CursorWebSocketSendLoop(WebSocketClient client, CursorSendEngine engine) {
        this(client, engine, 0L, DEFAULT_PARK_NANOS, null);
    }

    public CursorWebSocketSendLoop(WebSocketClient client, CursorSendEngine engine,
                                   long fsnAtZero, long parkNanos) {
        this(client, engine, fsnAtZero, parkNanos, null);
    }

    /**
     * Full constructor with reconnect plumbing. When {@code reconnectFactory}
     * is non-null, the I/O thread treats wire failures (send/receive errors,
     * server-initiated close) as recoverable: it calls the factory to obtain
     * a fresh connected client, resets wire state, and repositions its replay
     * cursor at {@code engine.ackedFsn() + 1}. A null factory disables
     * reconnect (legacy behavior — single failure is terminal).
     */
    public CursorWebSocketSendLoop(WebSocketClient client, CursorSendEngine engine,
                                   long fsnAtZero, long parkNanos,
                                   ReconnectFactory reconnectFactory) {
        this(client, engine, fsnAtZero, parkNanos, reconnectFactory,
                DEFAULT_RECONNECT_MAX_DURATION_MILLIS,
                DEFAULT_RECONNECT_INITIAL_BACKOFF_MILLIS,
                DEFAULT_RECONNECT_MAX_BACKOFF_MILLIS);
    }

    /**
     * Full constructor with explicit reconnect-policy knobs. Used by the
     * builder when the user has overridden the defaults.
     */
    public CursorWebSocketSendLoop(WebSocketClient client, CursorSendEngine engine,
                                   long fsnAtZero, long parkNanos,
                                   ReconnectFactory reconnectFactory,
                                   long reconnectMaxDurationMillis,
                                   long reconnectInitialBackoffMillis,
                                   long reconnectMaxBackoffMillis) {
        if (client == null || engine == null) {
            throw new IllegalArgumentException("client and engine must be non-null");
        }
        this.client = client;
        this.engine = engine;
        this.fsnAtZero = fsnAtZero;
        this.parkNanos = parkNanos;
        this.reconnectFactory = reconnectFactory;
        this.reconnectMaxDurationMillis = reconnectMaxDurationMillis;
        this.reconnectInitialBackoffMillis = reconnectInitialBackoffMillis;
        this.reconnectMaxBackoffMillis = reconnectMaxBackoffMillis;
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
     * Surfaces any error the I/O thread recorded. Called by the producer
     * thread (typically from inside its append wrapper) so failures don't
     * stay silent. Idempotent; once an error is set the loop has already
     * exited.
     */
    public void checkError() {
        Throwable e = lastError;
        if (e != null) {
            if (e instanceof LineSenderException) throw (LineSenderException) e;
            throw new LineSenderException("I/O thread failed: " + e.getMessage(), e);
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

    public Throwable getLastError() {
        return lastError;
    }

    public long getTotalAcks() {
        return totalAcks.get();
    }

    public long getTotalFramesSent() {
        return totalFramesSent.get();
    }

    public long getTotalReconnects() {
        return totalReconnects.get();
    }

    /** Total reconnect attempts (succeeded + failed). */
    public long getTotalReconnectAttempts() {
        return totalReconnectAttempts.get();
    }

    /** Total frames re-sent on the post-reconnect replay window. */
    public long getTotalFramesReplayed() {
        return totalFramesReplayed.get();
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
        if (reconnectFactory == null || !running) {
            recordFatal(initial);
            return;
        }
        LOG.warn("cursor I/O loop wire failure, entering reconnect loop: {}",
                initial.getMessage());
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
                    LOG.info("cursor I/O loop reconnected after {}ms, {} attempts; "
                                    + "replaying from FSN {}",
                            elapsedMs, attempts, fsnAtZero);
                    return;
                }
            } catch (Throwable e) {
                if (isTerminalUpgradeError(e)) {
                    String upgradeMsg = findUpgradeFailureMessage(e);
                    LOG.error("terminal upgrade error during reconnect — won't retry: {}",
                            upgradeMsg);
                    recordFatal(new LineSenderException(
                            "WebSocket upgrade failed during reconnect (won't retry): "
                                    + upgradeMsg, e));
                    return;
                }
                lastReconnectError = e;
                long now = System.nanoTime();
                if (now - lastLogNanos >= RECONNECT_LOG_THROTTLE_NANOS) {
                    LOG.warn("reconnect attempt {} failed: {}", attempts, e.getMessage());
                    lastLogNanos = now;
                }
            }
            // Backoff with jitter: sleep [backoff, 2*backoff). Cap the
            // sleep at the remaining budget so we don't oversleep past
            // the deadline.
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
        LOG.error("cursor I/O loop giving up reconnecting after {}ms, {} attempts; "
                        + "last error: {}",
                elapsedMs, attempts, lastReconnectError.getMessage());
        recordFatal(new LineSenderException(
                "reconnect failed after " + elapsedMs + "ms / " + attempts + " attempts: "
                        + lastReconnectError.getMessage(), lastReconnectError));
    }

    /**
     * Mark the loop as fatally failed. Caller has decided no reconnect
     * is possible (or it ran out of budget) — record the error so
     * {@link #checkError} can surface it to the producer thread, then
     * stop the loop.
     */
    private void recordFatal(Throwable t) {
        if (lastError == null) {
            lastError = t;
        }
        running = false;
        LOG.error("Cursor I/O loop failure: {}", t.getMessage(), t);
    }

    /**
     * True when the given throwable indicates a server-side reject that
     * won't fix itself on retry. Today this is detected by message
     * sniffing: WebSocket upgrade failures with a non-101 HTTP status
     * (401 unauthorized, 403 forbidden, 426 upgrade-required, etc.)
     * indicate auth or version mismatch — retrying just delays the user
     * seeing the misconfig. Other failures (TCP refused, IO error during
     * handshake) are treated as transient.
     */
    private static boolean isTerminalUpgradeError(Throwable t) {
        return findUpgradeFailureMessage(t) != null;
    }

    /**
     * Walks the cause chain looking for the WebSocketClient's
     * "WebSocket upgrade failed:" sentinel and returns its message, or
     * {@code null} if not present. The upgrade failure is thrown deep
     * inside WebSocketClient and gets wrapped by the connect path before
     * reaching us — so we have to look past the outermost wrapper.
     */
    private static String findUpgradeFailureMessage(Throwable t) {
        for (Throwable cur = t; cur != null; cur = cur.getCause()) {
            String msg = cur.getMessage();
            if (msg != null && msg.contains("WebSocket upgrade failed:")) {
                return msg;
            }
            if (cur.getCause() == cur) break;
        }
        return null;
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
            } catch (Throwable e) {
                if (isTerminalUpgradeError(e)) {
                    String upgradeMsg = findUpgradeFailureMessage(e);
                    LOG.error("{} hit terminal upgrade error — won't retry: {}",
                            contextLabel, upgradeMsg);
                    throw new LineSenderException(
                            "WebSocket upgrade failed during " + contextLabel
                                    + " (won't retry): " + upgradeMsg, e);
                }
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
     * Reset wire state for a fresh connection: install the new client,
     * realign {@code fsnAtZero} to the next unacked FSN, restart wire
     * sequencing from 0, and reposition the cursor so the next
     * {@link #trySendOne} call replays the first unacked frame.
     */
    private void swapClient(WebSocketClient newClient) {
        WebSocketClient old = this.client;
        this.client = newClient;
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
        this.consecutiveSendErrors.set(0L);
        // Snapshot publishedFsn at swap time — frames at FSN ≤ this value
        // were already on the wire before the drop and will be replayed.
        // trySendOne increments totalFramesReplayed for each one, then
        // resets replayTargetFsn to -1 once we cross the boundary.
        long pubAtSwap = engine.publishedFsn();
        this.replayTargetFsn = pubAtSwap >= replayStart ? pubAtSwap : -1L;
        positionCursorAt(replayStart);
    }

    /**
     * Walk the engine's segments to find the one containing {@code targetFsn},
     * and set {@code sendOffset} to the byte offset of that frame within it.
     * If {@code targetFsn} is past everything published, park at the live
     * active segment's published offset (caller will wait for new bytes).
     */
    private void positionCursorAt(long targetFsn) {
        MmapSegment seg = engine.findSegmentContaining(targetFsn);
        if (seg == null) {
            // targetFsn is at or past publishedFsn — nothing to replay.
            // Resume from the active segment's tip; producer may add more.
            sendingSegment = engine.activeSegment();
            sendOffset = sendingSegment.publishedOffset();
            return;
        }
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

    private void ioLoop() {
        try {
            while (running) {
                boolean didWork = false;
                // 1. Try to send next frame(s).
                if (trySendOne()) {
                    didWork = true;
                }
                // 2. Try to receive ACKs.
                if (tryReceiveAcks()) {
                    didWork = true;
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
                MmapSegment next = advanceSegment();
                if (next != sendingSegment) {
                    sendingSegment = next;
                    return true; // let the next iteration try sending
                }
            }
            return false;
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
        consecutiveSendErrors.set(0);
        return true;
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

    /** Inner ACK handler — parses the binary frame, calls engine.acknowledge. */
    private final class ResponseHandler implements WebSocketFrameHandler {
        @Override
        public void onClose(int code, String reason) {
            fail(new LineSenderException("WebSocket closed by server: code=" + code + " reason=" + reason));
        }

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
                    LOG.warn("server ACK wire seq {} exceeds highest sent {} — clamping",
                            wireSeq, highestSent);
                }
                engine.acknowledge(fsnAtZero + capped);
                totalAcks.incrementAndGet();
            } else {
                // Application-layer rejection by the server (e.g.
                // STATUS_SCHEMA_MISMATCH, STATUS_PARSE_ERROR). The bytes
                // on disk are the bytes the server rejected — reconnecting
                // and replaying them cannot fix the rejection, it just
                // burns CPU and reconnect attempts forever (each successful
                // reconnect resets the per-outage budget). Mark the loop
                // terminal directly via recordFatal so the next user-thread
                // API call surfaces the rejection, instead of routing
                // through fail() which would enter the reconnect retry
                // loop. Wire-level failures (sendBinary throw, server
                // close, parse-fail of the response payload) still go
                // through fail() — those CAN be fixed by reconnecting.
                recordFatal(new LineSenderException(
                        "server rejected wire seq " + wireSeq
                                + " (status=" + response.getStatusName()
                                + ") — terminal, sender will not replay"));
            }
        }
    }
}
