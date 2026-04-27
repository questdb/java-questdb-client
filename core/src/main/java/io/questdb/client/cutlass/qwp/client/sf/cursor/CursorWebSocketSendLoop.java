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
 * The cursor-engine equivalent of {@code WebSocketSendQueue}'s I/O loop.
 * Owns one I/O thread that:
 * <ol>
 *   <li>Polls {@link CursorSendEngine#publishedFsn()} and walks newly-published
 *       frames from the engine's segments, sending each as one WebSocket
 *       binary frame to the server.</li>
 *   <li>Polls the WebSocket for server ACK frames; on each ACK with
 *       cumulative wire sequence {@code N}, calls
 *       {@code engine.acknowledge(fsnAtZero + N)} so the segment manager
 *       can trim fully-acked segments.</li>
 * </ol>
 * No locks. The producer thread (user) writes into the engine; this thread
 * reads. {@code engine.publishedFsn()} is the volatile publish barrier.
 * <p>
 * <b>PR1 scope (deliberately minimal):</b>
 * <ul>
 *   <li>Happy-path send + ACK round-trip only.</li>
 *   <li>No ping/pong, no fsync requests, no per-table seqTxn tracking
 *       (the legacy {@code WebSocketSendQueue} has all of these — port
 *       them as PR2 once latency wins are confirmed).</li>
 *   <li>No reconnect / replay — a connection failure is fatal; the user
 *       must construct a new sender. Replay-on-reconnect needs to walk
 *       segments from {@code ackedFsn+1} forward and is the next PR.</li>
 *   <li>Single-connection only (no failover); WebSocketClient is provided
 *       and assumed to be already connected.</li>
 *   <li>Engine starts fresh (no on-disk recovery into the wire path).</li>
 * </ul>
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
    // Optional reconnect plumbing. If both are non-null, a wire failure
    // triggers a reconnect attempt instead of a terminal fail(). The factory
    // produces a fresh, connected+upgraded WebSocketClient; the listener is
    // notified after the wire state has been reset so the producer thread
    // can bump its connectionGeneration.
    private final ReconnectFactory reconnectFactory;
    private final ReconnectListener reconnectListener;
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
        this(client, engine, 0L, DEFAULT_PARK_NANOS, null, null);
    }

    public CursorWebSocketSendLoop(WebSocketClient client, CursorSendEngine engine,
                                   long fsnAtZero, long parkNanos) {
        this(client, engine, fsnAtZero, parkNanos, null, null);
    }

    /**
     * Full constructor with reconnect plumbing. When {@code reconnectFactory}
     * and {@code reconnectListener} are both non-null, the I/O thread treats
     * wire failures (send/receive errors, server-initiated close) as
     * recoverable: it calls the factory to obtain a fresh connected client,
     * resets wire state, repositions its replay cursor at
     * {@code engine.ackedFsn() + 1}, and notifies the listener so the
     * producer can bump its {@code connectionGeneration}. Either being null
     * disables reconnect (legacy behavior — single failure is terminal).
     */
    public CursorWebSocketSendLoop(WebSocketClient client, CursorSendEngine engine,
                                   long fsnAtZero, long parkNanos,
                                   ReconnectFactory reconnectFactory,
                                   ReconnectListener reconnectListener) {
        this(client, engine, fsnAtZero, parkNanos, reconnectFactory, reconnectListener,
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
                                   ReconnectListener reconnectListener,
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
        this.reconnectListener = reconnectListener;
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
     * Notified after a successful reconnect — wire state has been reset and
     * the cursor repositioned for replay. Implementations typically bump a
     * {@code connectionGeneration} counter the producer thread reads so
     * the next encode emits full schema definitions instead of refs.
     */
    @FunctionalInterface
    public interface ReconnectListener {
        void onReconnect();
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
    public void close() {
        running = false;
        if (ioThread != null) {
            try {
                shutdownLatch.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            ioThread = null;
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

    public synchronized void start() {
        if (ioThread != null) {
            throw new IllegalStateException("already started");
        }
        running = true;
        sendingSegment = engine.activeSegment();
        ioThread = new Thread(this::ioLoop, "qdb-cursor-ws-io");
        ioThread.setDaemon(true);
        ioThread.start();
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
        if (reconnectFactory == null || reconnectListener == null || !running) {
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
            try {
                WebSocketClient newClient = reconnectFactory.reconnect();
                if (newClient != null) {
                    swapClient(newClient);
                    totalReconnects.incrementAndGet();
                    reconnectListener.onReconnect();
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
        nextWireSeq++;
        totalFramesSent.incrementAndGet();
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
                fail(new LineSenderException(
                        "server reported error for wire seq " + wireSeq));
            }
        }
    }
}
