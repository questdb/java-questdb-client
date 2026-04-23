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
import io.questdb.client.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
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
    // The WebSocket client for I/O (single-threaded access only)
    private final WebSocketClient client;
    // Configuration
    private final long enqueueTimeoutMs;
    // Optional InFlightWindow for tracking sent batches awaiting ACK
    @Nullable
    private final InFlightWindow inFlightWindow;

    // The I/O thread for async send/receive
    private final Thread ioThread;
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
    private final ConcurrentHashMap<String, SeqTxn> committedSeqTxns = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, SeqTxn> durableSeqTxns = new ConcurrentHashMap<>();
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
    // Batch sequence counter (must match server's messageSequence)
    private long nextBatchSequence = 0;
    // Single pending buffer slot (double-buffering means at most 1 item in queue)
    // Zero allocation - just a volatile reference handoff
    private volatile MicrobatchBuffer pendingBuffer;
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
        if (client == null) {
            throw new IllegalArgumentException("client cannot be null");
        }

        this.client = client;
        this.inFlightWindow = inFlightWindow;
        this.enqueueTimeoutMs = enqueueTimeoutMs;
        this.shutdownTimeoutMs = shutdownTimeoutMs;
        this.running = true;
        this.shuttingDown = false;
        this.shutdownLatch = new CountDownLatch(1);

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
        if (!running || shuttingDown) {
            throw new LineSenderException("Send queue is not running");
        }

        // Check for errors from I/O thread
        checkError();

        final long deadline = System.currentTimeMillis() + enqueueTimeoutMs;
        synchronized (processingLock) {
            while (true) {
                if (!running || shuttingDown) {
                    throw new LineSenderException("Send queue is not running");
                }
                checkError();

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

    public long getCommittedSeqTxn(String tableName) {
        SeqTxn s = committedSeqTxns.get(tableName);
        return s != null ? s.get() : -1L;
    }

    public long getDurableSeqTxn(String tableName) {
        SeqTxn s = durableSeqTxns.get(tableName);
        return s != null ? s.get() : -1L;
    }

    /**
     * Requests the I/O thread to send a WebSocket PING and blocks until
     * the PONG arrives. The I/O loop continues its normal work (sending
     * batches, draining ACKs) while waiting for the PONG.
     * <p>
     * The server flushes pending durable ACKs before sending the PONG,
     * so after this method returns {@code getDurableSeqTxn()} reflects
     * all durable progress up to the moment the server processed the PING.
     */
    public void ping() {
        checkError();
        synchronized (processingLock) {
            pingComplete = false;
            pingRequested = true;
            processingLock.notifyAll();
            long deadline = System.nanoTime() + InFlightWindow.DEFAULT_TIMEOUT_MS * 1_000_000L;
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
        }
        checkError();
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

    private void failTransport(LineSenderException error) {
        Throwable rootError = lastError;
        if (rootError == null) {
            lastError = error;
            rootError = error;
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

        try {
            int drainIdleCycles = 0;
            while (running || !isPendingEmpty()) {
                // Send a pending PING if requested
                if (pingRequested) {
                    pingRequested = false;
                    pongReceived = false;
                    pingDeadlineNanos = System.nanoTime() + InFlightWindow.DEFAULT_TIMEOUT_MS * 1_000_000L;
                    try {
                        client.sendPing(1000);
                    } catch (Exception e) {
                        pingDeadlineNanos = 0;
                        failTransport(new LineSenderException("Ping failed", e));
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
                        // Try to receive any pending ACKs (non-blocking)
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
                                failTransport(new LineSenderException("Ping timed out waiting for PONG"));
                                completePing();
                            }
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
                                try {
                                    safeSendBatch(batch);
                                } finally {
                                    // Atomically: decrement + notify flush()
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

    private boolean isPendingEmpty() {
        return pendingBuffer == null;
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

    /**
     * Sends a batch with error handling. Does NOT manage processingCount.
     */
    private void safeSendBatch(MicrobatchBuffer batch) {
        try {
            sendBatch(batch);
        } catch (Throwable t) {
            LOG.error("Error sending batch [id={}]{}", batch.getBatchId(), "", t);
            failTransport(new LineSenderException("Error sending batch " + batch.getBatchId() + ": " + t.getMessage(), t));
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

        // Use our own sequence counter (must match server's messageSequence)
        long batchSequence = nextBatchSequence++;
        int bytes = batch.getBufferPos();
        int rows = batch.getRowCount();

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
                failTransport(new LineSenderException("Error receiving response: " + e.getMessage(), e));
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

    /**
     * Handler for received WebSocket frames (ACKs from server).
     */
    private class ResponseHandler implements WebSocketFrameHandler {

        @Override
        public void onBinaryMessage(long payloadPtr, int payloadLen) {
            if (!WebSocketResponse.isStructurallyValid(payloadPtr, payloadLen)) {
                LineSenderException error = new LineSenderException(
                        "Invalid ACK response payload [length=" + payloadLen + ']'
                );
                LOG.error("Invalid ACK response payload [length={}]", payloadLen);
                failTransport(error);
                return;
            }

            // Parse response from binary payload
            if (!response.readFrom(payloadPtr, payloadLen)) {
                LineSenderException error = new LineSenderException("Failed to parse ACK response");
                LOG.error("Failed to parse response");
                failTransport(error);
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

                if (inFlightWindow != null) {
                    LineSenderException error = new LineSenderException(
                            "Server error for batch " + sequence + ": " +
                                    response.getStatusName() + " - " + errorMessage);
                    inFlightWindow.fail(sequence, error);
                }
                totalErrors.incrementAndGet();
            }
        }

        @Override
        public void onClose(int code, String reason) {
            LOG.info("WebSocket closed by server [code={}, reason={}]", code, reason);
            failTransport(new LineSenderException("WebSocket closed by server [code=" + code + ", reason=" + reason + ']'));
        }

        @Override
        public void onPong(long payloadPtr, int payloadLen) {
            pongReceived = true;
        }
    }

    static final class SeqTxn {
        private final AtomicLong value;

        SeqTxn(long value) {
            this.value = new AtomicLong(value);
        }

        long get() {
            return value.get();
        }

        void advance(long newValue) {
            long curr;
            do {
                curr = value.get();
                if (curr >= newValue) {
                    return;
                }
            } while (!value.compareAndSet(curr, newValue));
        }
    }

    private static void advanceSeqTxn(ConcurrentHashMap<String, SeqTxn> map, String tableName, long seqTxn) {
        SeqTxn existing = map.get(tableName);
        if (existing != null) {
            existing.advance(seqTxn);
            return;
        }
        map.computeIfAbsent(tableName, k -> new SeqTxn(seqTxn)).advance(seqTxn);
    }
}
