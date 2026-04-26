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

import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.sf.SegmentLog;
import io.questdb.client.cutlass.qwp.client.sf.SfDiskFullException;
import io.questdb.client.cutlass.qwp.client.sf.SfException;
import io.questdb.client.std.CharSequenceLongHashMap;
import io.questdb.client.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Asynchronous I/O handler for WebSocket microbatch transmission.
 * <p>
 * This class manages a dedicated I/O thread that handles both:
 * <ul>
 *   <li>Sending batches via a single-slot handoff (volatile reference)</li>
 *   <li>Receiving and processing server ACK responses</li>
 * </ul>
 * The single-slot design matches the double-buffering scheme: at most one
 * sealed buffer is pending while the other is being filled.
 * Using a single thread eliminates concurrency issues with the WebSocket channel.
 * <p>
 * Thread safety:
 * <ul>
 *   <li>The pending slot is thread-safe for concurrent access</li>
 *   <li>Only the I/O thread interacts with the WebSocket channel</li>
 *   <li>Buffer state transitions ensure safe hand-over</li>
 * </ul>
 * <p>
 * Backpressure:
 * <ul>
 *   <li>When the slot is occupied, {@link #enqueue} blocks</li>
 *   <li>This propagates backpressure to the user thread</li>
 * </ul>
 */
public class WebSocketSendQueue implements QuietCloseable {

    private static final int DRAIN_SPIN_TRIES = 16;
    public static final long DEFAULT_ENQUEUE_TIMEOUT_MS = 30_000;
    public static final long DEFAULT_SHUTDOWN_TIMEOUT_MS = 10_000;
    private static final Logger LOG = LoggerFactory.getLogger(WebSocketSendQueue.class);
    // The WebSocket client for I/O (single-threaded access only). Replaced on
    // reconnect when SF is enabled.
    private WebSocketClient client;
    @Nullable
    private final Reconnector reconnector;
    private volatile boolean reconnectRequested;
    // Configuration
    private final long enqueueTimeoutMs;
    private final long pingTimeoutMs;
    @Nullable
    private final ConnectionFailureListener connectionFailureListener;
    // Optional InFlightWindow for tracking sent batches awaiting ACK
    @Nullable
    private final InFlightWindow inFlightWindow;
    // Optional SegmentLog for store-and-forward durability. When non-null, every
    // outgoing batch is captured to disk before it leaves the wire and trimmed
    // on cumulative ACK. The log also becomes the batch-sequence authority so
    // sequence numbers survive restart.
    @Nullable
    private final SegmentLog segmentLog;

    // The I/O thread for async send/receive
    private final Thread ioThread;
    // Serializes concurrent ping() callers so each one gets its own PING/PONG
    // round-trip. Without this, two callers can race on pingComplete and the
    // second caller can return on the first caller's PONG, observing a stale
    // durable watermark.
    private final Object pingLock = new Object();
    // Counter for batches currently being processed by the I/O thread
    // This tracks batches that have been dequeued but not yet fully sent
    private final AtomicInteger processingCount = new AtomicInteger(0);
    // Lock for all coordination between user thread and I/O thread.
    // Used for: queue poll + processingCount increment atomicity,
    // flush() waiting, I/O thread waiting when idle.
    private final Object processingLock = new Object();
    // Response parsing
    private final WebSocketResponse response = new WebSocketResponse();
    private final ResponseHandler responseHandler = new ResponseHandler();
    // Synchronization for flush/close
    private final CountDownLatch shutdownLatch;
    private final long shutdownTimeoutMs;
    // Per-table seqTxn watermarks. Written by the I/O thread only; read by user threads.
    // All accesses synchronize on the map instance itself for publication and monotonic updates.
    private final CharSequenceLongHashMap committedSeqTxns = new CharSequenceLongHashMap();
    private final CharSequenceLongHashMap durableSeqTxns = new CharSequenceLongHashMap();
    // Statistics - receiving
    private final AtomicLong totalAcks = new AtomicLong(0);
    // Statistics - sending
    private final AtomicLong totalBatchesSent = new AtomicLong(0);
    private final AtomicLong totalBytesSent = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    // Close guard: ensures only one thread executes the shutdown sequence
    private final AtomicBoolean closeCalled = new AtomicBoolean(false);
    // Error handling
    private volatile Throwable lastError;
    // Wire batch sequence counter — fresh per connection (must match server's messageSequence
    // which starts at 0 for each new connection).
    private long nextBatchSequence = 0;
    // SF frame-sequence number (FSN) that corresponds to wire seq 0 on this connection.
    // Lets us translate between the wire seq the server acks and the persistent FSN that
    // SegmentLog uses for trim. Invariant: fsn = fsnAtZero + wireSeq for every sent batch.
    private long fsnAtZero;
    // Single pending buffer slot (double-buffering means at most 1 item in queue)
    // Zero allocation - just a volatile reference handoff
    private volatile MicrobatchBuffer pendingBuffer;
    // Buffer that we polled out of pendingBuffer but couldn't persist (disk full
    // on SF.append). The I/O thread keeps it here and retries on each loop iteration
    // until disk space frees up via trim. While stalled, processingCount stays > 0
    // so the user thread's flush() blocks — natural backpressure.
    // Volatile because close()/isPendingEmpty() observe from the user thread.
    private volatile MicrobatchBuffer stalledBuffer;
    private long lastDiskFullLogMs;
    // Counter exposed for tests/observability: number of times a batch was stalled
    // due to disk-full and had to be retried.
    private final AtomicLong totalDiskFullStalls = new AtomicLong(0);
    private volatile boolean pingComplete;
    private volatile boolean pingRequested;
    private volatile boolean pongReceived;
    private long pingDeadlineNanos;
    // Running state
    private volatile boolean running;
    private volatile boolean shuttingDown;

    /**
     * Creates a new send queue with custom configuration.
     *
     * @param client            the WebSocket client for I/O
     * @param inFlightWindow    the window to track sent batches awaiting ACK (may be null)
     * @param enqueueTimeoutMs  timeout for enqueue operations (ms)
     * @param shutdownTimeoutMs timeout for graceful shutdown (ms)
     */
    public WebSocketSendQueue(WebSocketClient client, @Nullable InFlightWindow inFlightWindow,
                              long enqueueTimeoutMs, long shutdownTimeoutMs) {
        this(client, inFlightWindow, enqueueTimeoutMs, shutdownTimeoutMs, null, null, null);
    }

    public WebSocketSendQueue(WebSocketClient client, @Nullable InFlightWindow inFlightWindow,
                              long enqueueTimeoutMs, long shutdownTimeoutMs,
                              @Nullable ConnectionFailureListener connectionFailureListener) {
        this(client, inFlightWindow, enqueueTimeoutMs, shutdownTimeoutMs, connectionFailureListener, null, null);
    }

    public WebSocketSendQueue(WebSocketClient client, @Nullable InFlightWindow inFlightWindow,
                              long enqueueTimeoutMs, long shutdownTimeoutMs,
                              @Nullable ConnectionFailureListener connectionFailureListener,
                              @Nullable SegmentLog segmentLog) {
        this(client, inFlightWindow, enqueueTimeoutMs, shutdownTimeoutMs, connectionFailureListener, segmentLog, null);
    }

    /**
     * Creates a new send queue with custom configuration.
     *
     * @param client                    the WebSocket client for I/O
     * @param inFlightWindow            the window to track sent batches awaiting ACK (may be null)
     * @param enqueueTimeoutMs          timeout for enqueue operations (ms)
     * @param shutdownTimeoutMs         timeout for graceful shutdown (ms)
     * @param connectionFailureListener notified once when the queue detects a terminal connection failure
     * @param segmentLog                optional store-and-forward log; when set, every outgoing batch
     *                                  is captured to disk before send and trimmed on ACK, and seq
     *                                  numbering is taken from the log so it survives restart
     * @param reconnector               optional reconnect callback; when set together with segmentLog,
     *                                  the queue absorbs transient connection failures by calling
     *                                  {@link Reconnector#reconnect()} with exponential backoff and
     *                                  replaying SF state. Required for SF auto-reconnect.
     */
    public WebSocketSendQueue(WebSocketClient client, @Nullable InFlightWindow inFlightWindow,
                              long enqueueTimeoutMs, long shutdownTimeoutMs,
                              @Nullable ConnectionFailureListener connectionFailureListener,
                              @Nullable SegmentLog segmentLog,
                              @Nullable Reconnector reconnector) {
        if (client == null) {
            throw new IllegalArgumentException("client cannot be null");
        }
        if (segmentLog != null && inFlightWindow == null) {
            throw new IllegalArgumentException("segmentLog requires inFlightWindow (async mode)");
        }

        this.client = client;
        this.inFlightWindow = inFlightWindow;
        this.segmentLog = segmentLog;
        this.reconnector = reconnector;
        this.enqueueTimeoutMs = enqueueTimeoutMs;
        this.shutdownTimeoutMs = shutdownTimeoutMs;
        this.pingTimeoutMs = inFlightWindow != null ? inFlightWindow.getTimeoutMs() : InFlightWindow.DEFAULT_TIMEOUT_MS;
        this.connectionFailureListener = connectionFailureListener;
        this.running = true;
        this.shuttingDown = false;
        this.shutdownLatch = new CountDownLatch(1);

        if (segmentLog != null) {
            // Wire seq always starts at 0 on a fresh connection. Persistent SF FSNs
            // are decoupled from the wire — fsnAtZero pins the relationship so we
            // can translate server acks (wire seq) back to SF FSNs for trim.
            long oldest = segmentLog.oldestSeq();
            this.fsnAtZero = oldest >= 0 ? oldest : segmentLog.nextSeq();
        }

        // Start the I/O thread (handles both sending and receiving)
        this.ioThread = new Thread(this::ioLoop, "questdb-websocket-io");
        this.ioThread.setDaemon(true);
        this.ioThread.start();

        LOG.info("WebSocket I/O thread started");
    }

    /**
     * Closes the send queue gracefully.
     * <p>
     * This method:
     * 1. Stops accepting new batches
     * 2. Waits for pending batches to be sent
     * 3. Stops the I/O thread
     * <p>
     * Note: This does NOT close the WebSocket channel - that's the caller's responsibility.
     */
    @Override
    public void close() {
        if (!closeCalled.compareAndSet(false, true)) {
            return;
        }
        if (!running) {
            awaitShutdown(shutdownTimeoutMs);
            return;
        }

        LOG.info("Closing WebSocket send queue [pending={}]", getPendingSize());

        // Signal shutdown
        shuttingDown = true;

        // Wait for pending batches to be sent
        long startTime = System.currentTimeMillis();
        synchronized (processingLock) {
            while (!isPendingEmpty()) {
                long elapsed = System.currentTimeMillis() - startTime;
                if (elapsed >= shutdownTimeoutMs) {
                    LOG.error("Shutdown timeout, {} batches not sent", getPendingSize());
                    break;
                }
                try {
                    processingLock.wait(shutdownTimeoutMs - elapsed);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        // Stop the I/O thread
        running = false;

        // Wake up I/O thread if it's blocked on processingLock.wait()
        synchronized (processingLock) {
            processingLock.notifyAll();
        }
        ioThread.interrupt();

        // Wait for I/O thread to finish before allowing the caller to free
        // the socket and client-owned native buffers. If a send/recv call is
        // still blocked, disconnect the socket to force it to unwind.
        if (!awaitShutdown(shutdownTimeoutMs)) {
            LOG.warn("I/O thread did not stop within {}ms, disconnecting socket", shutdownTimeoutMs);
            client.forceDisconnect();
            ioThread.interrupt();
            if (!awaitShutdown(shutdownTimeoutMs)) {
                throw new LineSenderException("Timed out waiting for WebSocket I/O thread to stop");
            }
        }

        LOG.info("WebSocket send queue closed [totalBatches={}, totalBytes={}]", totalBatchesSent.get(), totalBytesSent.get());
    }

    /**
     * Enqueues a sealed buffer for sending.
     * <p>
     * The buffer must be in SEALED state. After this method returns successfully,
     * ownership of the buffer transfers to the send queue.
     *
     * @param buffer the sealed buffer to send
     * @return true if enqueued successfully
     * @throws LineSenderException if the buffer is not sealed or an error occurred
     */
    public boolean enqueue(MicrobatchBuffer buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("buffer cannot be null");
        }
        if (!buffer.isSealed()) {
            throw new LineSenderException("Buffer must be sealed before enqueue, state=" +
                    MicrobatchBuffer.stateName(buffer.getState()));
        }
        checkError();
        if (!running || shuttingDown) {
            checkError();
            throw new LineSenderException("Send queue is not running");
        }

        final long deadline = System.currentTimeMillis() + enqueueTimeoutMs;
        synchronized (processingLock) {
            while (true) {
                checkError();
                if (!running || shuttingDown) {
                    checkError();
                    throw new LineSenderException("Send queue is not running");
                }

                if (offerPending(buffer)) {
                    processingLock.notifyAll();
                    break;
                }

                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    throw new LineSenderException("Enqueue timeout after " + enqueueTimeoutMs + "ms");
                }
                try {
                    processingLock.wait(Math.min(10, remaining));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new LineSenderException("Interrupted while enqueueing", e);
                }
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Enqueued batch [id={}, bytes={}, rows={}]", buffer.getBatchId(), buffer.getBufferPos(), buffer.getRowCount());
        }
        return true;
    }

    /**
     * Waits for all pending batches to be sent.
     * <p>
     * This method blocks until the queue is empty and all in-flight sends complete.
     * It does not close the queue - new batches can still be enqueued after flush.
     *
     * @throws LineSenderException if an error occurs during flush
     */
    public void flush() {
        checkError();

        long startTime = System.currentTimeMillis();

        // Wait under lock until the queue becomes empty and no batch is being sent.
        synchronized (processingLock) {
            while (running) {
                // Atomically check: queue empty AND not processing
                if (isPendingEmpty() && processingCount.get() == 0) {
                    break; // All done
                }

                long remaining = enqueueTimeoutMs - (System.currentTimeMillis() - startTime);
                if (remaining <= 0) {
                    throw new LineSenderException("Flush timeout after " + enqueueTimeoutMs + "ms, " +
                            "queue=" + getPendingSize() + ", processing=" + processingCount.get());
                }

                try {
                    processingLock.wait(remaining);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new LineSenderException("Interrupted while flushing", e);
                }

                // Check for errors
                checkError();
            }
        }

        // If loop exited because running=false we still need to surface the root cause.
        checkError();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Flush complete");
        }
    }

    /**
     * Waits for all in-flight batches to be acknowledged.
     */
    public void awaitPendingAcks() {
        if (inFlightWindow == null) {
            return;
        }

        checkError();
        inFlightWindow.awaitEmpty();
        checkError();
    }

    /**
     * Returns the last error that occurred in the I/O thread, or null if no error.
     */
    public Throwable getLastError() {
        return lastError;
    }

    public long getCommittedSeqTxn(CharSequence tableName) {
        synchronized (committedSeqTxns) {
            return committedSeqTxns.get(tableName);
        }
    }

    public long getDurableSeqTxn(CharSequence tableName) {
        synchronized (durableSeqTxns) {
            return durableSeqTxns.get(tableName);
        }
    }

    /**
     * Requests the I/O thread to send a WebSocket PING and blocks until
     * the PONG arrives. The I/O loop continues its normal work (sending
     * batches, draining ACKs) while waiting for the PONG.
     * <p>
     * The server flushes pending durable ACKs before sending the PONG,
     * so after this method returns {@code getDurableSeqTxn()} reflects
     * all durable progress up to the moment the server processed the PING.
     * <p>
     * Concurrent ping callers are serialized: each caller gets its own
     * PING / PONG round-trip so the post-condition holds for every caller
     * independently. A second caller may wait up to {@code pingTimeoutMs}
     * for an in-flight ping to complete before its own ping starts.
     */
    public void ping() {
        synchronized (pingLock) {
            checkError();
            synchronized (processingLock) {
                pingComplete = false;
                pingRequested = true;
                processingLock.notifyAll();
                long deadline = System.nanoTime() + pingTimeoutMs * 1_000_000L;
                while (!pingComplete && running) {
                    long remaining = (deadline - System.nanoTime()) / 1_000_000L;
                    if (remaining <= 0) {
                        throw new LineSenderException("Ping timed out");
                    }
                    try {
                        processingLock.wait(remaining);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new LineSenderException("Ping interrupted");
                    }
                }
                if (!pingComplete) {
                    checkError();
                    throw new LineSenderException("Ping aborted: send queue is shutting down");
                }
            }
            checkError();
        }
    }

    /**
     * Returns the total number of batches sent.
     */
    public long getTotalBatchesSent() {
        return totalBatchesSent.get();
    }

    /**
     * Returns the total number of bytes sent.
     */
    public long getTotalBytesSent() {
        return totalBytesSent.get();
    }

    /**
     * Checks if an error occurred in the I/O thread and throws if so.
     */
    private void checkError() {
        Throwable error = lastError;
        if (error != null) {
            throw new LineSenderException("Error in send queue I/O thread: " + error.getMessage(), error);
        }
    }

    /**
     * Computes the current I/O state based on queue, in-flight, and ping status.
     */
    private IoState computeState(boolean hasInFlight) {
        if (!isPendingEmpty()) {
            return IoState.ACTIVE;
        } else if (hasInFlight || pingDeadlineNanos > 0) {
            return IoState.DRAINING;
        } else {
            return IoState.IDLE;
        }
    }

    private void failConnection(LineSenderException error) {
        failConnection(error, false);
    }

    /**
     * Mark the connection as failed. When {@code fatal} is true (e.g. an SF
     * storage error like corruption or a frame too large for a segment), bypass
     * the SF auto-reconnect path and go terminal — these errors won't recover
     * by reconnecting and silent retry would loop forever.
     */
    private void failConnection(LineSenderException error, boolean fatal) {
        // SF + reconnector mode: don't go terminal for transient connection-level
        // errors. Signal the I/O loop to close the broken client and reconnect
        // with backoff. Bytes for any unacked batches are already on disk in the
        // SegmentLog; replay-on-reconnect re-sends them.
        if (!fatal && segmentLog != null && reconnector != null && !shuttingDown) {
            if (!reconnectRequested) {
                LOG.warn("Connection failed (SF will reconnect): {}", error.getMessage());
                reconnectRequested = true;
                synchronized (processingLock) {
                    processingLock.notifyAll();
                }
            }
            return;
        }
        Throwable rootError = lastError;
        boolean firstFailure = rootError == null;
        if (rootError == null) {
            lastError = error;
            rootError = error;
        }
        if (firstFailure && connectionFailureListener != null) {
            try {
                connectionFailureListener.onConnectionFailure(error);
            } catch (Throwable t) {
                LOG.error("Error notifying connection failure listener", t);
            }
        }
        running = false;
        shuttingDown = true;
        if (inFlightWindow != null) {
            inFlightWindow.failAll(rootError);
        }
        synchronized (processingLock) {
            //noinspection resource
            MicrobatchBuffer dropped = pollPending();
            if (dropped != null) {
                if (dropped.isSealed()) {
                    dropped.markSending();
                }
                if (dropped.isSending()) {
                    dropped.markRecycled();
                }
            }
            processingLock.notifyAll();
        }
    }

    private int getPendingSize() {
        return pendingBuffer == null ? 0 : 1;
    }

    private int idleDuringDrain(int idleCycles) {
        if (idleCycles < DRAIN_SPIN_TRIES) {
            Thread.onSpinWait();
            return idleCycles + 1;
        }
        Thread.yield();
        return DRAIN_SPIN_TRIES;
    }

    /**
     * The main I/O loop that handles both sending batches and receiving ACKs.
     * <p>
     * Uses a state machine:
     * <ul>
     *   <li>IDLE: block on processingLock.wait() until work arrives</li>
     *   <li>ACTIVE: non-blocking poll queue, send batches, check for ACKs</li>
     *   <li>DRAINING: no batches but ACKs pending - poll for ACKs with non-blocking backoff</li>
     * </ul>
     */
    private void ioLoop() {
        LOG.info("I/O loop started");

        if (segmentLog != null) {
            replayPersistedFrames();
        }

        long reconnectBackoffMs = 100;
        try {
            int drainIdleCycles = 0;
            while (running || !isPendingEmpty()) {

                if (reconnectRequested) {
                    boolean ok = doReconnectCycle(reconnectBackoffMs);
                    if (ok) {
                        reconnectBackoffMs = 100;
                        reconnectRequested = false;
                    } else {
                        // reconnect attempt failed; keep flag set, retry after longer backoff
                        reconnectBackoffMs = Math.min(reconnectBackoffMs * 2, 30_000);
                    }
                    continue; // re-evaluate state machine after reconnect attempt
                }
                // Send a pending PING if requested
                if (pingRequested) {
                    pingRequested = false;
                    pongReceived = false;
                    pingDeadlineNanos = System.nanoTime() + pingTimeoutMs * 1_000_000L;
                    try {
                        client.sendPing(1000);
                    } catch (Exception e) {
                        pingDeadlineNanos = 0;
                        failConnection(new LineSenderException("Ping failed", e));
                        completePing();
                    }
                }

                MicrobatchBuffer batch = null;
                boolean hasInFlight = (inFlightWindow != null && inFlightWindow.getInFlightCount() > 0);
                IoState state = computeState(hasInFlight);
                boolean receivedAcks = false;

                switch (state) {
                    case IDLE:
                        drainIdleCycles = 0;
                        // Nothing to do - wait for work under lock
                        synchronized (processingLock) {
                            // Re-check under lock to avoid missed wakeup
                            if (isPendingEmpty() && running && !pingRequested) {
                                try {
                                    processingLock.wait(100);
                                } catch (InterruptedException e) {
                                    if (!running) return;
                                }
                            }
                        }
                        break;

                    case ACTIVE:
                    case DRAINING:
                        // Try to receive any pending ACKs first — they may trim
                        // sealed segments and free disk space, unblocking a stalled
                        // SF retry.
                        if (client.isConnected()) {
                            receivedAcks = tryReceiveAcks();
                        }

                        // Check if a pending PING has been answered
                        if (pingDeadlineNanos > 0) {
                            if (pongReceived) {
                                pingDeadlineNanos = 0;
                                completePing();
                            } else if (System.nanoTime() >= pingDeadlineNanos) {
                                pingDeadlineNanos = 0;
                                failConnection(new LineSenderException("Ping timed out waiting for PONG"));
                                completePing();
                            }
                        }

                        // Retry the stalled batch (SF disk-full backpressure path).
                        // While stalled, do not poll new batches — keep processingCount > 0
                        // so the user thread's flush() blocks until disk frees.
                        if (stalledBuffer != null) {
                            if (!running) {
                                // Shutdown requested with disk still full. Abandon the
                                // stalled batch so the I/O loop can terminate. The
                                // user's data was never persisted — this is the
                                // "shutdown timeout under disk full" data-loss path.
                                abandonStalled();
                            } else {
                                retryStalled();
                            }
                            break;
                        }

                        // Try to dequeue and send a batch
                        boolean hasWindowSpace = (inFlightWindow == null || inFlightWindow.hasWindowSpace());
                        if (hasWindowSpace) {
                            // Atomically: poll queue + increment processingCount
                            synchronized (processingLock) {
                                batch = pollPending();
                                if (batch != null) {
                                    processingCount.incrementAndGet();
                                }
                            }

                            if (batch != null) {
                                boolean stalled = false;
                                try {
                                    sendBatch(batch);
                                } catch (SfDiskFullException dfe) {
                                    stalled = true;
                                    stalledBuffer = batch;
                                    totalDiskFullStalls.incrementAndGet();
                                    logDiskFull(batch.getBatchId());
                                    // Do not recycle the buffer; retry will pick it up.
                                } catch (SfException sfe) {
                                    // Non-disk-full SF storage error (corruption, frame
                                    // too large, etc.) — won't recover by reconnect; fail
                                    // hard so the user sees it instead of looping.
                                    LOG.error("Fatal SF storage error [id={}]", batch.getBatchId(), sfe);
                                    failConnection(new LineSenderException(
                                            "SF storage error: " + sfe.getMessage(), sfe), true);
                                    if (batch.isSealed()) batch.markSending();
                                    if (batch.isSending()) batch.markRecycled();
                                } catch (Throwable t) {
                                    LOG.error("Error sending batch [id={}]", batch.getBatchId(), t);
                                    failConnection(new LineSenderException(
                                            "Error sending batch " + batch.getBatchId() + ": " + t.getMessage(), t));
                                    if (batch.isSealed()) batch.markSending();
                                    if (batch.isSending()) batch.markRecycled();
                                }
                                if (!stalled) {
                                    synchronized (processingLock) {
                                        processingCount.decrementAndGet();
                                        processingLock.notifyAll();
                                    }
                                }
                            }
                        }

                        // In DRAINING state with no work, stay non-blocking and use
                        // a simple spin/yield backoff.
                        if (state == IoState.DRAINING && batch == null) {
                            if (receivedAcks) {
                                drainIdleCycles = 0;
                            } else {
                                drainIdleCycles = idleDuringDrain(drainIdleCycles);
                            }
                        } else {
                            drainIdleCycles = 0;
                        }
                        break;
                }
            }
        } finally {
            shutdownLatch.countDown();
            LOG.info("I/O loop stopped [totalAcks={}, totalErrors={}]", totalAcks.get(), totalErrors.get());
        }
    }

    private void completePing() {
        synchronized (processingLock) {
            pingComplete = true;
            processingLock.notifyAll();
        }
    }

    /**
     * Tear down the broken connection, sleep for backoff, ask the {@link Reconnector}
     * for a fresh client, reset wire-level state, and re-stream SF.
     * <p>
     * Returns {@code true} when the new connection is up and SF replay completed.
     * Returns {@code false} if the reconnect itself failed; the caller will retry
     * after a longer backoff.
     */
    private boolean doReconnectCycle(long sleepMs) {
        // Drop any half-written buffer first so the user thread can keep producing.
        synchronized (processingLock) {
            //noinspection resource
            MicrobatchBuffer dropped = pollPending();
            if (dropped != null) {
                if (dropped.isSealed()) {
                    dropped.markSending();
                }
                if (dropped.isSending()) {
                    dropped.markRecycled();
                }
            }
            processingLock.notifyAll();
        }
        try {
            client.forceDisconnect();
        } catch (Throwable ignored) {
            // best-effort
        }
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
        }
        if (!running) {
            return false;
        }
        WebSocketClient newClient;
        try {
            newClient = reconnector.reconnect();
        } catch (Throwable t) {
            LOG.warn("SF reconnect failed: {}", t.getMessage());
            return false;
        }
        this.client = newClient;
        // Reset wire-level state. SegmentLog is the source of truth for unacked
        // bytes; we discard the in-flight window's seq tracking and rebuild via
        // replay.
        nextBatchSequence = 0;
        if (inFlightWindow != null) {
            inFlightWindow.reset();
        }
        long oldest = segmentLog.oldestSeq();
        fsnAtZero = oldest >= 0 ? oldest : segmentLog.nextSeq();
        try {
            replayPersistedFrames();
        } catch (Throwable t) {
            LOG.warn("SF replay after reconnect failed: {}", t.getMessage());
            return false;
        }
        LOG.info("SF reconnect complete");
        return true;
    }

    /**
     * Stream every frame currently on disk back to the server. Runs once at I/O
     * loop startup before any user-thread batches are pulled. The server dedups
     * at table-seqTxn level (the seqTxn lives inside the captured wire bytes), so
     * frames that the server already applied in a previous session are silently
     * dropped on receive.
     */
    private void replayPersistedFrames() {
        final long[] count = {0};
        try {
            segmentLog.replay((fsn, addr, len) -> {
                if (!running) {
                    return false;
                }
                long wireSeq = nextBatchSequence;
                // FSNs come out of SF in monotonic order. Replay starts at the oldest
                // FSN, which we pinned as fsnAtZero in the constructor — so the first
                // replayed FSN must equal fsnAtZero, and subsequent ones increment
                // alongside wireSeq. Drift here means SF state changed between open
                // and ioLoop start, which shouldn't happen.
                if (fsn != fsnAtZero + wireSeq) {
                    throw new LineSenderException(
                            "SF replay FSN drift: fsn=" + fsn + " expected=" + (fsnAtZero + wireSeq));
                }
                if (inFlightWindow != null) {
                    while (running && !inFlightWindow.hasWindowSpace()) {
                        if (client.isConnected()) {
                            tryReceiveAcks();
                        }
                        Thread.onSpinWait();
                    }
                    if (!running) {
                        return false;
                    }
                    if (!inFlightWindow.tryAddInFlight(wireSeq)) {
                        return false;
                    }
                }
                client.sendBinary(addr, len);
                nextBatchSequence = wireSeq + 1;
                totalBatchesSent.incrementAndGet();
                totalBytesSent.addAndGet(len);
                count[0]++;
                return true;
            });
        } catch (Throwable t) {
            LOG.error("SF replay failed", t);
            failConnection(new LineSenderException("SF replay failed: " + t.getMessage(), t));
            return;
        }
        if (count[0] > 0) {
            LOG.info("Replayed {} persisted frames from SF [highestWireSeq={}, fsnAtZero={}]",
                    count[0], nextBatchSequence - 1, fsnAtZero);
        }
    }

    private boolean isPendingEmpty() {
        // A stalled buffer (SF disk-full) counts as pending — the user's flush()
        // and close() must wait until it's either retried successfully or
        // abandoned at shutdown timeout.
        return pendingBuffer == null && stalledBuffer == null;
    }

    private boolean awaitShutdown(long timeoutMs) {
        try {
            return shutdownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return shutdownLatch.getCount() == 0;
        }
    }

    private boolean offerPending(MicrobatchBuffer buffer) {
        if (pendingBuffer != null) {
            return false; // slot occupied
        }
        pendingBuffer = buffer;
        return true;
    }

    private MicrobatchBuffer pollPending() {
        MicrobatchBuffer buffer = pendingBuffer;
        if (buffer != null) {
            pendingBuffer = null;
        }
        return buffer;
    }

    private void logDiskFull(long bufferId) {
        long now = System.currentTimeMillis();
        if (now - lastDiskFullLogMs > 5_000) {
            lastDiskFullLogMs = now;
            LOG.warn("SF disk full — back-pressuring user thread [bufferId={}, totalStalls={}]",
                    bufferId, totalDiskFullStalls.get());
        }
    }

    /**
     * Retries a stalled batch (set when SF.append failed with disk-full). Called
     * from the I/O loop after each ACK-recv pass — any ACK may have triggered a
     * trim that freed disk space. Brief sleep on continued failure to avoid
     * busy-spinning on a permanently-full disk.
     */
    private void retryStalled() {
        MicrobatchBuffer batch = stalledBuffer;
        boolean cleared = false;
        try {
            sendBatch(batch);
            cleared = true;
        } catch (SfDiskFullException dfe) {
            // still stuck; brief sleep so we don't burn CPU
            try {
                Thread.sleep(50);
            } catch (InterruptedException ignored) {
                if (!running) {
                    Thread.currentThread().interrupt();
                }
            }
        } catch (Throwable t) {
            // Non-disk-full failure during retry — recycle and surface.
            LOG.error("Error retrying stalled batch [id={}]", batch.getBatchId(), t);
            failConnection(new LineSenderException(
                    "Error retrying stalled batch " + batch.getBatchId() + ": " + t.getMessage(), t));
            if (batch.isSealed()) batch.markSending();
            if (batch.isSending()) batch.markRecycled();
            cleared = true;
        }
        if (cleared) {
            stalledBuffer = null;
            synchronized (processingLock) {
                processingCount.decrementAndGet();
                processingLock.notifyAll();
            }
        }
    }

    public long getTotalDiskFullStalls() {
        return totalDiskFullStalls.get();
    }

    /**
     * Drop the stalled batch without retrying. Called from the I/O loop when the
     * queue has been told to shut down while disk-full backpressure is active —
     * we'd otherwise loop forever waiting for space that won't arrive.
     */
    private void abandonStalled() {
        MicrobatchBuffer batch = stalledBuffer;
        if (batch == null) return;
        LOG.warn("Shutdown while SF disk full — abandoning stalled batch [bufferId={}]",
                batch.getBatchId());
        if (batch.isSealed()) batch.markSending();
        if (batch.isSending()) batch.markRecycled();
        stalledBuffer = null;
        synchronized (processingLock) {
            processingCount.decrementAndGet();
            processingLock.notifyAll();
        }
    }

    /**
     * Sends a batch with error handling. Does NOT manage processingCount.
     */
    private void safeSendBatch(MicrobatchBuffer batch) {
        try {
            sendBatch(batch);
        } catch (Throwable t) {
            LOG.error("Error sending batch [id={}]{}", batch.getBatchId(), "", t);
            failConnection(new LineSenderException("Error sending batch " + batch.getBatchId() + ": " + t.getMessage(), t));
            // Mark as recycled even on error to allow cleanup
            if (batch.isSealed()) {
                batch.markSending();
            }
            if (batch.isSending()) {
                batch.markRecycled();
            }
        }
    }

    /**
     * Sends a single batch over the WebSocket channel.
     */
    private void sendBatch(MicrobatchBuffer batch) {
        // Transition state: SEALED -> SENDING
        batch.markSending();

        int bytes = batch.getBufferPos();
        int rows = batch.getRowCount();

        // Persist to disk first when SF is enabled, so a crash between persist and
        // wire send still has the bytes recoverable for replay. The server tracks
        // its own per-connection seq starting at 0, so wireSeq stays decoupled from
        // the persistent SF FSN.
        long batchSequence = nextBatchSequence++;
        if (segmentLog != null) {
            long fsn = segmentLog.append(batch.getBufferPtr(), bytes);
            // Sanity: SF.append produces FSNs strictly monotonic, and we always send
            // exactly what we appended in order, so fsn must equal fsnAtZero+wireSeq.
            if (fsn != fsnAtZero + batchSequence) {
                throw new LineSenderException(
                        "SF/wire seq drift: fsn=" + fsn + " expected=" + (fsnAtZero + batchSequence));
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Sending batch [seq={}, bytes={}, rows={}, bufferId={}]", batchSequence, bytes, rows, batch.getBatchId());
        }

        // Add to in-flight window BEFORE sending (so we're ready for ACK)
        // Use non-blocking tryAddInFlight since we already checked window space in ioLoop
        if (inFlightWindow != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Adding to in-flight window [seq={}, inFlight={}, max={}]", batchSequence, inFlightWindow.getInFlightCount(), inFlightWindow.getMaxWindowSize());
            }
            if (!inFlightWindow.tryAddInFlight(batchSequence)) {
                // Should not happen since we checked hasWindowSpace before polling
                throw new LineSenderException("In-flight window unexpectedly full");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Added to in-flight window [seq={}]", batchSequence);
            }
        }

        // Send over WebSocket
        if (LOG.isDebugEnabled()) {
            LOG.debug("Calling sendBinary [seq={}]", batchSequence);
        }
        client.sendBinary(batch.getBufferPtr(), bytes);
        if (LOG.isDebugEnabled()) {
            LOG.debug("sendBinary returned [seq={}]", batchSequence);
        }

        // Update statistics
        totalBatchesSent.incrementAndGet();
        totalBytesSent.addAndGet(bytes);

        // Transition state: SENDING -> RECYCLED
        batch.markRecycled();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Batch sent and recycled [seq={}, bufferId={}]", batchSequence, batch.getBatchId());
        }
    }

    /**
     * Tries to receive ACKs from the server (non-blocking).
     */
    private boolean tryReceiveAcks() {
        boolean received = false;
        try {
            while (client.tryReceiveFrame(responseHandler)) {
                received = true;
                // Drain all buffered ACKs before returning to the I/O loop.
            }
        } catch (Exception e) {
            if (running) {
                LOG.error("Error receiving response: {}", e.getMessage());
                failConnection(new LineSenderException("Error receiving response: " + e.getMessage(), e));
            }
        }
        return received;
    }

    /**
     * I/O loop states for the state machine.
     * <ul>
     *   <li>IDLE: queue empty, no in-flight batches - can block waiting for work</li>
     *   <li>ACTIVE: have batches to send - non-blocking loop</li>
     *   <li>DRAINING: queue empty but ACKs pending - poll for ACKs with non-blocking backoff</li>
     * </ul>
     */
    private enum IoState {
        IDLE, ACTIVE, DRAINING
    }

    @FunctionalInterface
    public interface ConnectionFailureListener {
        void onConnectionFailure(LineSenderException error);
    }

    /**
     * Handler for received WebSocket frames (ACKs from server).
     */
    private class ResponseHandler implements WebSocketFrameHandler {

        @Override
        public void onBinaryMessage(long payloadPtr, int payloadLen) {
            // readFrom validates inline; a single pass parses and bounds-checks.
            if (!response.readFrom(payloadPtr, payloadLen)) {
                LineSenderException error = new LineSenderException(
                        "Invalid ACK response payload [length=" + payloadLen + ']'
                );
                LOG.error("Invalid ACK response payload [length={}]", payloadLen);
                failConnection(error);
                return;
            }

            long sequence = response.getSequence();

            if (response.isSuccess()) {
                if (inFlightWindow != null) {
                    int acked = inFlightWindow.acknowledgeUpTo(sequence);
                    if (acked > 0) {
                        totalAcks.addAndGet(acked);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Cumulative ACK received [upTo={}, acked={}]", sequence, acked);
                        }
                    } else if (LOG.isDebugEnabled()) {
                        LOG.debug("ACK for already-acknowledged sequences [upTo={}]", sequence);
                    }
                }
                if (segmentLog != null) {
                    // Translate wire seq → FSN. Cumulative ack of wire seq N means
                    // every FSN up to fsnAtZero+N has been applied server-side.
                    segmentLog.trim(fsnAtZero + sequence);
                }
                for (int i = 0, n = response.getTableEntryCount(); i < n; i++) {
                    advanceSeqTxn(committedSeqTxns, response.getTableName(i), response.getTableSeqTxn(i));
                }
            } else if (response.isDurableAck()) {
                for (int i = 0, n = response.getTableEntryCount(); i < n; i++) {
                    advanceSeqTxn(durableSeqTxns, response.getTableName(i), response.getTableSeqTxn(i));
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Durable ACK received [tables={}]", response.getTableEntryCount());
                }
            } else {
                // Error - fail the batch
                String errorMessage = response.getErrorMessage();
                LOG.error("Error response [seq={}, status={}, error={}]", sequence, response.getStatusName(), errorMessage);

                LineSenderException error = new LineSenderException(
                        "Server error for batch " + sequence + ": " +
                                response.getStatusName() + " - " + errorMessage);
                totalErrors.incrementAndGet();
                failConnection(error);
            }
        }

        @Override
        public void onClose(int code, String reason) {
            LOG.info("WebSocket closed by server [code={}, reason={}]", code, reason);
            failConnection(new LineSenderException("WebSocket closed by server [code=" + code + ", reason=" + reason + ']'));
        }

        @Override
        public void onPong(long payloadPtr, int payloadLen) {
            pongReceived = true;
        }
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private static void advanceSeqTxn(CharSequenceLongHashMap map, String tableName, long seqTxn) {
        synchronized (map) {
            if (seqTxn > map.get(tableName)) {
                map.put(tableName, seqTxn);
            }
        }
    }
}
