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

import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.qwp.client.QwpDurableAckMismatchException;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

/**
 * Empties one orphan slot, then exits. Owned by
 * {@link BackgroundDrainerPool}; one instance per slot.
 * <p>
 * Lifecycle:
 * <ol>
 *   <li>Acquire the slot's {@code .lock}; skip silently on contention.</li>
 *   <li>Open a {@link CursorSendEngine} on the slot — recovery picks up
 *       every {@code .sfa} file already on disk.</li>
 *   <li>Open a fresh {@link WebSocketClient} via the supplied factory
 *       (separate connection from the foreground sender).</li>
 *   <li>Run a {@link CursorWebSocketSendLoop} until {@code ackedFsn}
 *       catches up to the snapshot of {@code publishedFsn} taken at
 *       startup. No appends — the drainer is read-only on the slot.</li>
 *   <li>Close everything in reverse order; release the lock.</li>
 * </ol>
 * <p>
 * On terminal failure (auth-rejection on reconnect, reconnect-budget
 * exhaustion, recovery error), the drainer drops a
 * {@link OrphanScanner#FAILED_SENTINEL_NAME} sentinel into the slot
 * before exiting. Future scans skip the slot until an operator clears
 * the sentinel — bounded automatic retry, then human-in-the-loop.
 */
public final class BackgroundDrainer implements Runnable {

    /**
     * Cap on consecutive {@link QwpDurableAckMismatchException} attempts at
     * initial connect before the drainer escalates to a {@code .failed}
     * sentinel. The wall-clock budget {@code reconnectMaxDurationMillis}
     * also caps the same loop; whichever is hit first triggers escalation.
     * 16 attempts gives the cluster room to settle through a rolling
     * upgrade (each attempt walks every endpoint internally) without
     * letting a genuine cluster-wide misconfig hang the drainer forever.
     */
    public static final int DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS = 16;
    private static final Logger LOG = LoggerFactory.getLogger(BackgroundDrainer.class);
    /** How often to wake and re-check ackedFsn vs target. */
    private static final long POLL_NANOS = 50_000_000L; // 50 ms

    private final String slotPath;
    private final long segmentSizeBytes;
    private final long sfMaxTotalBytes;
    private final CursorWebSocketSendLoop.ReconnectFactory clientFactory;
    private final long reconnectMaxDurationMillis;
    private final long reconnectInitialBackoffMillis;
    private final long reconnectMaxBackoffMillis;
    private final boolean requestDurableAck;
    private final long durableAckKeepaliveIntervalMillis;
    private volatile boolean stopRequested;
    /** Latest known {@code engine.ackedFsn()}; published for visibility. */
    private volatile long ackedFsn = -1L;
    private volatile DrainOutcome outcome = DrainOutcome.PENDING;
    private volatile String lastErrorMessage;
    /**
     * Optional observer for durable-ack-unavailable transients and the
     * eventual escalation. Assignable any time before {@link #run()} starts;
     * setting it after the drainer is already running is permitted but
     * implementations must remain non-blocking and tolerate concurrent
     * re-assignment. Reads use a single volatile load into a local.
     */
    private volatile BackgroundDrainerListener listener;

    public BackgroundDrainer(
            String slotPath,
            long segmentSizeBytes,
            long sfMaxTotalBytes,
            CursorWebSocketSendLoop.ReconnectFactory clientFactory,
            long reconnectMaxDurationMillis,
            long reconnectInitialBackoffMillis,
            long reconnectMaxBackoffMillis,
            boolean requestDurableAck,
            long durableAckKeepaliveIntervalMillis
    ) {
        this.slotPath = slotPath;
        this.segmentSizeBytes = segmentSizeBytes;
        this.sfMaxTotalBytes = sfMaxTotalBytes;
        this.clientFactory = clientFactory;
        this.reconnectMaxDurationMillis = reconnectMaxDurationMillis;
        this.reconnectInitialBackoffMillis = reconnectInitialBackoffMillis;
        this.reconnectMaxBackoffMillis = reconnectMaxBackoffMillis;
        this.requestDurableAck = requestDurableAck;
        this.durableAckKeepaliveIntervalMillis = durableAckKeepaliveIntervalMillis;
    }

    /**
     * No-op drainer used by tests that exercise listener propagation in
     * {@link BackgroundDrainerPool}. All collaborators are null/zero, so
     * {@link #run()} fails fast — the executor swallows the failure, which
     * is fine because the tests assert listener state on the drainer
     * instance directly, not on any side effect of running.
     */
    @TestOnly
    public BackgroundDrainer() {
        this(null, 0L, 0L, null, 0L, 0L, 0L, false, 0L);
    }

    public DrainOutcome outcome() {
        return outcome;
    }

    public long getAckedFsn() {
        return ackedFsn;
    }

    public String getLastErrorMessage() {
        return lastErrorMessage;
    }

    /** Currently installed listener, or {@code null}. */
    public BackgroundDrainerListener getListener() {
        return listener;
    }

    public void requestStop() {
        stopRequested = true;
    }

    /**
     * Plug an observer for durable-ack-related events. {@code null} clears
     * any previously installed listener. See {@link BackgroundDrainerListener}
     * for thread-safety contract.
     */
    public void setListener(BackgroundDrainerListener listener) {
        this.listener = listener;
    }

    @Override
    public void run() {
        CursorSendEngine engine = null;
        WebSocketClient client = null;
        CursorWebSocketSendLoop loop = null;
        try {
            // The engine acquires the slot's .lock itself — we don't need
            // (and must not) double-lock it. If another sender or drainer
            // holds it, the engine constructor throws and we exit silently
            // (no .failed sentinel — contention is expected, not an error).
            try {
                engine = new CursorSendEngine(slotPath, segmentSizeBytes,
                        sfMaxTotalBytes, CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS);
            } catch (IllegalStateException t) {
                String msg = t.getMessage();
                if (msg != null && msg.contains("already in use")) {
                    LOG.info("orphan slot already locked, skipping: {} ({})",
                            slotPath, msg);
                    outcome = DrainOutcome.LOCKED_BY_OTHER;
                    return;
                }
                throw t;
            }
            long target = engine.publishedFsn();
            if (engine.ackedFsn() >= target) {
                LOG.info("orphan slot already drained: {} (acked={} target={})",
                        slotPath, engine.ackedFsn(), target);
                outcome = DrainOutcome.SUCCESS;
                return;
            }
            client = connectWithDurableAckRetry();
            if (client == null) {
                // outcome already set (FAILED or STOPPED); markFailed sentinel
                // already dropped on the FAILED path.
                return;
            }
            loop = new CursorWebSocketSendLoop(
                    client, engine,
                    0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                    clientFactory,
                    reconnectMaxDurationMillis,
                    reconnectInitialBackoffMillis,
                    reconnectMaxBackoffMillis,
                    requestDurableAck,
                    durableAckKeepaliveIntervalMillis);
            loop.start();

            while (!stopRequested) {
                long acked = engine.ackedFsn();
                this.ackedFsn = acked;
                if (acked >= target) {
                    outcome = DrainOutcome.SUCCESS;
                    LOG.info("drainer fully drained slot {} (target={}, acked={})",
                            slotPath, target, acked);
                    return;
                }
                try {
                    loop.checkError();
                } catch (Throwable t) {
                    String msg = t.getMessage();
                    LOG.error("drainer wire error for slot {}: {}", slotPath, msg);
                    lastErrorMessage = msg;
                    OrphanScanner.markFailed(slotPath, "wire: " + msg);
                    outcome = DrainOutcome.FAILED;
                    return;
                }
                java.util.concurrent.locks.LockSupport.parkNanos(POLL_NANOS);
            }
            outcome = DrainOutcome.STOPPED;
        } catch (Throwable t) {
            String msg = t.getMessage();
            LOG.error("drainer setup failed for slot {}: {}", slotPath, msg, t);
            lastErrorMessage = msg;
            try {
                OrphanScanner.markFailed(slotPath, "setup: " + msg);
            } catch (Throwable ignored) {
                // best-effort
            }
            outcome = DrainOutcome.FAILED;
        } finally {
            if (loop != null) {
                try {
                    loop.close();
                } catch (Throwable ignored) {
                }
            }
            if (client != null) {
                try {
                    client.close();
                } catch (Throwable ignored) {
                }
            }
            if (engine != null) {
                try {
                    // engine.close() releases the slot lock too.
                    engine.close();
                } catch (Throwable ignored) {
                }
            }
        }
    }

    /**
     * Initial connect with retry on whole-cluster durable-ack unavailability.
     * The wrapped {@code clientFactory.reconnect()} already walks every
     * configured endpoint per attempt and only throws
     * {@link QwpDurableAckMismatchException} when none of them advertise
     * durable ack -- i.e. the symptom of a misconfigured cluster or a
     * rolling-upgrade transient.
     * <p>
     * For the foreground sender that condition is loud-fail: the producer
     * is actively pushing data. The drainer is asymmetric: source data is
     * pinned (durable-ack-mode trims only on STATUS_DURABLE_ACK frames,
     * which the offending endpoints by definition do not send), so we
     * give the cluster a budget to settle before quarantining the slot.
     * On each failed sweep the listener is notified and the loop backs
     * off; once consecutive attempts or wall time exceed the configured
     * budget, the drainer drops a {@code .failed} sentinel and exits
     * exactly as the original single-shot path did.
     * <p>
     * Other exceptions (auth failure, version mismatch, transport error,
     * etc.) preserve the original behavior: mark failed, exit. They are
     * either terminal in their own right or already retried inside
     * {@code reconnect()}.
     *
     * @return a fresh durable-ack-capable client, or {@code null} if
     *         {@link #outcome} has been set to FAILED or STOPPED
     */
    @TestOnly
    public WebSocketClient connectWithDurableAckRetry() {
        long startNanos = System.nanoTime();
        long deadlineNanos = startNanos + reconnectMaxDurationMillis * 1_000_000L;
        long backoffMillis = reconnectInitialBackoffMillis;
        int mismatchAttempts = 0;
        while (!stopRequested) {
            try {
                return clientFactory.reconnect();
            } catch (QwpDurableAckMismatchException e) {
                mismatchAttempts++;
                long now = System.nanoTime();
                long elapsedMs = (now - startNanos) / 1_000_000L;
                boolean exhausted = mismatchAttempts >= DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS
                        || now >= deadlineNanos;
                BackgroundDrainerListener l = listener;
                if (exhausted) {
                    LOG.error("drainer giving up on slot {} after {} durable-ack-mismatch attempts ({}ms): {}",
                            slotPath, mismatchAttempts, elapsedMs, e.getMessage());
                    if (l != null) {
                        try {
                            l.onDurableAckPersistentFailure(slotPath, mismatchAttempts, elapsedMs);
                        } catch (Throwable cb) {
                            LOG.warn("drainer listener onDurableAckPersistentFailure threw: {}",
                                    cb.getMessage());
                        }
                    }
                    lastErrorMessage = e.getMessage();
                    OrphanScanner.markFailed(slotPath,
                            "durable-ack persistently unavailable after "
                                    + mismatchAttempts + " attempts: " + e.getMessage());
                    outcome = DrainOutcome.FAILED;
                    return null;
                }
                if (l != null) {
                    try {
                        l.onDurableAckUnavailable(slotPath, mismatchAttempts);
                    } catch (Throwable cb) {
                        LOG.warn("drainer listener onDurableAckUnavailable threw: {}",
                                cb.getMessage());
                    }
                }
                LOG.warn("drainer slot {} attempt {}: durable-ack unavailable, retrying after backoff",
                        slotPath, mismatchAttempts);
            } catch (Throwable t) {
                String msg = t.getMessage();
                LOG.error("drainer initial connect failed for slot {}: {}", slotPath, msg);
                lastErrorMessage = msg;
                OrphanScanner.markFailed(slotPath, "initial connect: " + msg);
                outcome = DrainOutcome.FAILED;
                return null;
            }
            // Backoff before the next sweep. Honor stopRequested by parking in
            // small chunks rather than a single long park so close() doesn't
            // wait for a full sleep to elapse.
            long jitter = ThreadLocalRandom.current().nextLong(Math.max(1L, backoffMillis));
            long sleepMillis = Math.min(backoffMillis + jitter,
                    Math.max(0L, (deadlineNanos - System.nanoTime()) / 1_000_000L));
            if (sleepMillis > 0L && !stopRequested) {
                LockSupport.parkNanos(sleepMillis * 1_000_000L);
            }
            backoffMillis = Math.min(backoffMillis * 2L, reconnectMaxBackoffMillis);
        }
        outcome = DrainOutcome.STOPPED;
        return null;
    }

    /** Terminal state of a drainer's run. */
    public enum DrainOutcome {
        PENDING,
        LOCKED_BY_OTHER,
        SUCCESS,
        FAILED,
        STOPPED
    }
}
