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
    private static final Logger LOG = LoggerFactory.getLogger(CursorWebSocketSendLoop.class);

    private final WebSocketClient client;
    private final AtomicLong consecutiveSendErrors = new AtomicLong();
    private final CursorSendEngine engine;
    // fsnAtZero: FSN that wireSeq=0 maps to on this connection. For a fresh
    // connection starting from a fresh engine (no recovery), this is 0.
    // Once recovery / reconnect lands (PR2), this is set to the first
    // unacked FSN at connect time so wire-seq math stays aligned.
    private final long fsnAtZero;
    private final long parkNanos;
    private final WebSocketResponse response = new WebSocketResponse();
    private final ResponseHandler responseHandler = new ResponseHandler();
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final AtomicLong totalAcks = new AtomicLong();
    private final AtomicLong totalFramesSent = new AtomicLong();
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
        this(client, engine, 0L, DEFAULT_PARK_NANOS);
    }

    public CursorWebSocketSendLoop(WebSocketClient client, CursorSendEngine engine,
                                   long fsnAtZero, long parkNanos) {
        if (client == null || engine == null) {
            throw new IllegalArgumentException("client and engine must be non-null");
        }
        this.client = client;
        this.engine = engine;
        this.fsnAtZero = fsnAtZero;
        this.parkNanos = parkNanos;
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

    private void fail(Throwable t) {
        if (lastError == null) {
            lastError = t;
        }
        running = false;
        LOG.error("Cursor I/O loop failure: {}", t.getMessage(), t);
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
