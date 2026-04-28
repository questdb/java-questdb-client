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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private volatile boolean stopRequested;
    /** Snapshot of {@code engine.publishedFsn()} at start, or -1 if not yet set. */
    private volatile long targetFsn = -1L;
    /** Latest known {@code engine.ackedFsn()}; published for visibility. */
    private volatile long ackedFsn = -1L;
    private volatile DrainOutcome outcome = DrainOutcome.PENDING;
    private volatile String lastErrorMessage;

    public BackgroundDrainer(
            String slotPath,
            long segmentSizeBytes,
            long sfMaxTotalBytes,
            CursorWebSocketSendLoop.ReconnectFactory clientFactory,
            long reconnectMaxDurationMillis,
            long reconnectInitialBackoffMillis,
            long reconnectMaxBackoffMillis
    ) {
        this.slotPath = slotPath;
        this.segmentSizeBytes = segmentSizeBytes;
        this.sfMaxTotalBytes = sfMaxTotalBytes;
        this.clientFactory = clientFactory;
        this.reconnectMaxDurationMillis = reconnectMaxDurationMillis;
        this.reconnectInitialBackoffMillis = reconnectInitialBackoffMillis;
        this.reconnectMaxBackoffMillis = reconnectMaxBackoffMillis;
    }

    public String slotPath() {
        return slotPath;
    }

    public DrainOutcome outcome() {
        return outcome;
    }

    public long getTargetFsn() {
        return targetFsn;
    }

    public long getAckedFsn() {
        return ackedFsn;
    }

    public String getLastErrorMessage() {
        return lastErrorMessage;
    }

    public void requestStop() {
        stopRequested = true;
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
            this.targetFsn = target;
            if (engine.ackedFsn() >= target) {
                LOG.info("orphan slot already drained: {} (acked={} target={})",
                        slotPath, engine.ackedFsn(), target);
                outcome = DrainOutcome.SUCCESS;
                return;
            }
            try {
                client = clientFactory.reconnect();
            } catch (Throwable t) {
                String msg = t.getMessage();
                LOG.error("drainer initial connect failed for slot {}: {}",
                        slotPath, msg);
                lastErrorMessage = msg;
                OrphanScanner.markFailed(slotPath, "initial connect: " + msg);
                outcome = DrainOutcome.FAILED;
                return;
            }
            loop = new CursorWebSocketSendLoop(
                    client, engine,
                    0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                    clientFactory,
                    reconnectMaxDurationMillis,
                    reconnectInitialBackoffMillis,
                    reconnectMaxBackoffMillis);
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

    /** Terminal state of a drainer's run. */
    public enum DrainOutcome {
        PENDING,
        LOCKED_BY_OTHER,
        SUCCESS,
        FAILED,
        STOPPED
    }
}
