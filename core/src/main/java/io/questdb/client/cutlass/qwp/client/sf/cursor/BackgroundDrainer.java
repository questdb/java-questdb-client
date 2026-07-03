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
import io.questdb.client.cutlass.http.client.WebSocketUpgradeException;
import io.questdb.client.cutlass.qwp.client.QwpAuthFailedException;
import io.questdb.client.cutlass.qwp.client.QwpDurableAckMismatchException;
import io.questdb.client.cutlass.qwp.client.QwpIngressRoleRejectedException;
import io.questdb.client.cutlass.qwp.client.QwpRoleMismatchException;
import io.questdb.client.cutlass.qwp.client.QwpVersionMismatchException;
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
 * On terminal failure (auth-rejection on reconnect, a cluster-wide durable-ack
 * capability gap that exhausts its settle budget, recovery error), the drainer
 * drops a {@link OrphanScanner#FAILED_SENTINEL_NAME} sentinel into the slot
 * before exiting. Future scans skip the slot until an operator clears the
 * sentinel — bounded automatic retry, then human-in-the-loop. A transient
 * all-replica failover window is NOT terminal: it is retried indefinitely
 * (Invariant B), never quarantined on a wall-clock budget or attempt cap.
 */
public final class BackgroundDrainer implements Runnable {

    /**
     * Cap on consecutive {@link QwpDurableAckMismatchException} attempts at
     * initial connect before the drainer escalates to a {@code .failed}
     * sentinel. Applies ONLY to a genuine cluster-wide durable-ack capability
     * gap (a server that upgrades but does not advertise durable ack); a
     * transient all-replica failover window (role reject) is retried
     * indefinitely and is never subject to this cap (Invariant B). The
     * wall-clock budget {@code reconnectMaxDurationMillis} also caps this
     * capability-gap loop; whichever is hit first triggers escalation. Both
     * halves of the budget measure a capability-gap <i>episode</i>: the
     * wall clock accumulates only across uninterrupted gap-to-gap intervals
     * (never before the first gap is observed, and never across an
     * intervening transport window -- an unreachable cluster is not
     * "failing to settle"), and an intervening role reject restarts the
     * episode -- it proves the topology changed, so the next capability-gap
     * error is a fresh episode against a newly promoted node. 16
     * attempts gives the cluster room to settle through a rolling upgrade
     * (each attempt walks every endpoint internally) without letting a genuine
     * cluster-wide misconfig hang the drainer forever.
     */
    public static final int DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS = 16;
    private static final Logger LOG = LoggerFactory.getLogger(BackgroundDrainer.class);
    /** How often to wake and re-check ackedFsn vs target. */
    private static final long POLL_NANOS = 50_000_000L; // 50 ms
    /**
     * Upper bound on a single backoff park so {@link #requestStop()} is
     * honored promptly even without the unpark (e.g. a permit consumed by
     * an earlier spurious wakeup). Keeps the pool's post-stop grace window
     * ({@code BackgroundDrainerPool.STOP_GRACE_MILLIS}) meaningful: a
     * stopping drainer wakes at least every 50ms to re-check the flag.
     */
    private static final long STOP_CHECK_PARK_CHUNK_NANOS = 50_000_000L; // 50 ms
    private final CursorWebSocketSendLoop.ReconnectFactory clientFactory;
    private final long durableAckKeepaliveIntervalMillis;
    private final long reconnectInitialBackoffMillis;
    private final long reconnectMaxBackoffMillis;
    private final long reconnectMaxDurationMillis;
    private final boolean requestDurableAck;
    private final long segmentSizeBytes;
    private final long sfMaxTotalBytes;
    private final String slotPath;
    /** Latest known {@code engine.ackedFsn()}; published for visibility. */
    private volatile long ackedFsn = -1L;
    private volatile String lastErrorMessage;
    /**
     * Optional observer for durable-ack-unavailable transients and the
     * eventual escalation. Assignable any time before {@link #run()} starts;
     * setting it after the drainer is already running is permitted but
     * implementations must remain non-blocking and tolerate concurrent
     * re-assignment. Reads use a single volatile load into a local.
     */
    private volatile BackgroundDrainerListener listener;
    private volatile DrainOutcome outcome = DrainOutcome.PENDING;
    /**
     * Thread currently executing {@link #run()} (or a direct
     * {@link #connectWithDurableAckRetry()} call from tests). Lets
     * {@link #requestStop()} unpark a drainer sleeping in a backoff or
     * poll park instead of waiting for the park to elapse.
     */
    private volatile Thread runnerThread;
    private volatile boolean stopRequested;
    // Poison-frame detector threshold forwarded to every drain loop this
    // drainer creates; mirrors the owner sender's max_frame_rejections config.
    private final int maxHeadFrameRejections;
    // Minimum wall-clock dwell before poison escalation, forwarded to every
    // drain loop; mirrors the owner sender's poison_min_escalation_window_millis.
    private final long poisonMinEscalationWindowMillis;

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
        this(slotPath, segmentSizeBytes, sfMaxTotalBytes, clientFactory,
                reconnectMaxDurationMillis, reconnectInitialBackoffMillis,
                reconnectMaxBackoffMillis, requestDurableAck,
                durableAckKeepaliveIntervalMillis,
                CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS,
                CursorWebSocketSendLoop.DEFAULT_POISON_MIN_ESCALATION_WINDOW_MILLIS);
    }

    /**
     * Master constructor — also accepts the poison-frame detector threshold
     * ({@code max_frame_rejections}) forwarded to the drain loop's
     * {@link CursorWebSocketSendLoop}: the drainer replays the owner sender's
     * SF data, so it must honor the same configured threshold.
     */
    public BackgroundDrainer(
            String slotPath,
            long segmentSizeBytes,
            long sfMaxTotalBytes,
            CursorWebSocketSendLoop.ReconnectFactory clientFactory,
            long reconnectMaxDurationMillis,
            long reconnectInitialBackoffMillis,
            long reconnectMaxBackoffMillis,
            boolean requestDurableAck,
            long durableAckKeepaliveIntervalMillis,
            int maxHeadFrameRejections,
            long poisonMinEscalationWindowMillis
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
        this.maxHeadFrameRejections = maxHeadFrameRejections;
        this.poisonMinEscalationWindowMillis = poisonMinEscalationWindowMillis;
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
        this(null, 0L, 0L, null, 0L, 0L, 0L, false, 0L,
                CursorWebSocketSendLoop.DEFAULT_MAX_HEAD_FRAME_REJECTIONS, 0L);
    }

    /**
     * Budgeted connect with retry on whole-cluster durable-ack unavailability:
     * the initial connect, and re-entered from {@link #run()} whenever a
     * mid-drain reconnect sweep hits the same capability gap (each re-entry
     * is a fresh episode -- a successful connect ended the previous one).
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
     * The budget measures a capability-gap <i>episode</i>: consecutive
     * {@link QwpDurableAckMismatchException} sweeps only. Transient
     * conditions -- an all-replica failover window (role reject) or a
     * transport error -- are retried indefinitely (Invariant B) and never
     * consume the budget: the wall-clock half accumulates only across
     * uninterrupted gap-to-gap intervals, so a mid-episode transport window
     * pauses the clock (without touching the attempt count), and a role
     * reject additionally restarts the episode, because it proves the
     * topology changed under the rolling upgrade.
     * Genuine terminals (auth failure, non-421 upgrade reject) preserve
     * the original behavior: mark failed, exit.
     *
     * @return a fresh durable-ack-capable client, or {@code null} if
     *         {@link #outcome} has been set to FAILED or STOPPED
     */
    @TestOnly
    public WebSocketClient connectWithDurableAckRetry() {
        // run() already set runnerThread; setting it again here is a no-op
        // on that path but wires up direct @TestOnly calls so requestStop()
        // can unpark them too.
        runnerThread = Thread.currentThread();
        long backoffMillis = reconnectInitialBackoffMillis;
        // Capability-gap settle budget. Counts ONLY consecutive
        // QwpDurableAckMismatchException sweeps; the wall-clock half
        // accumulates ONLY across uninterrupted gap-to-gap intervals, so
        // transient churn (role reject, transport) can never burn the budget
        // -- neither before the first gap is observed nor mid-episode (a
        // cluster unreachable for longer than the whole budget that comes
        // back still gapped has consumed none of it). An intervening role
        // reject resets the episode (topology churn: the offending node is
        // gone); a transport error neither increments nor resets the attempt
        // count -- a dropped socket does not prove promotion churn, and
        // resetting on it would let a flaky-but-misconfigured cluster evade
        // the cap forever -- it only pauses the wall clock: the gap-to-gap
        // interval spanning the transport window is not charged.
        int capabilityGapAttempts = 0;
        // Wall-clock time accumulated across uninterrupted gap-to-gap
        // intervals of the current episode; escalates once it reaches
        // capabilityGapBudgetNanos (or the attempt cap fires first).
        long capabilityGapElapsedNanos = 0L;
        // Timestamp of the previous capability-gap sweep; 0 = the next gap
        // charges nothing (episode start, post-role-reject restart, or the
        // interval was interrupted by a transport window).
        long lastCapabilityGapNanos = 0L;
        final long capabilityGapBudgetNanos = reconnectMaxDurationMillis * 1_000_000L;
        // Observability-only counter for the transient all-replica window;
        // never consulted for escalation (Invariant B).
        int roleRejectAttempts = 0;
        // Throttle the all-replica retry WARN to one per 5s: a real failover
        // window can last minutes and (Invariant B) is retried indefinitely, so
        // per-attempt logging would flood. Mirrors CursorWebSocketSendLoop.
        long lastReplicaWarnNanos = 0L;
        long lastTransportWarnNanos = 0L;
        while (!stopRequested) {
            // True only for a genuine durable-ack CAPABILITY gap, which is
            // bounded by the settle budget / attempt cap. A transient all-replica
            // failover window (role reject) is retried indefinitely under
            // Invariant B and leaves this false, so its backoff is never clamped
            // to the deadline (which would otherwise busy-loop once past it).
            boolean boundedByBudget = false;
            try {
                return clientFactory.reconnect();
            } catch (QwpAuthFailedException | WebSocketUpgradeException e) {
                // Genuinely non-retriable across the cluster (auth 401/403, or a
                // non-421 upgrade reject): waiting will not fix it, so quarantine
                // immediately -- exactly as the live sender's background loop
                // (CursorWebSocketSendLoop.connectLoop) halts on these errors.
                String msg = e.getMessage();
                LOG.error("drainer terminal upgrade/auth error for slot {}: {}", slotPath, msg);
                lastErrorMessage = msg;
                OrphanScanner.markFailed(slotPath, "auth/upgrade: " + msg);
                outcome = DrainOutcome.FAILED;
                return null;
            } catch (QwpRoleMismatchException | QwpIngressRoleRejectedException e) {
                // INVARIANT B: every reachable endpoint is a REPLICA right now.
                // A replica is promotable and a primary will reappear, so this is
                // a TRANSIENT failover window, NOT a capability gap. The drainer
                // must keep retrying (capped backoff) until a primary is reachable,
                // stopRequested, or SF exhaustion -- it must NEVER quarantine the
                // slot on a wall-clock budget or an attempt cap. Surface the
                // per-attempt observability callback, then back off and retry.
                roleRejectAttempts++;
                // Topology is mid-churn: whatever node produced any earlier
                // capability-gap errors is no longer the primary the next
                // sweep hits, so the gap episode (attempts + wall clock)
                // restarts and the next gap gets the full settle budget.
                capabilityGapAttempts = 0;
                capabilityGapElapsedNanos = 0L;
                lastCapabilityGapNanos = 0L;
                BackgroundDrainerListener l = listener;
                if (l != null) {
                    try {
                        l.onPrimaryUnavailable(slotPath, roleRejectAttempts);
                    } catch (Throwable cb) {
                        LOG.warn("drainer listener onPrimaryUnavailable threw: {}",
                                cb.getMessage());
                    }
                }
                long nowWarn = System.nanoTime();
                if (nowWarn - lastReplicaWarnNanos >= 5_000_000_000L) {
                    LOG.warn("drainer slot {} attempt {}: all endpoints are replicas "
                            + "(transient failover window), retrying after backoff",
                            slotPath, roleRejectAttempts);
                    lastReplicaWarnNanos = nowWarn;
                }
            } catch (QwpDurableAckMismatchException e) {
                // Genuine cluster-wide durable-ack CAPABILITY gap: a server
                // upgraded but does not advertise durable ack. Unlike a role
                // reject this will not clear by waiting for a promotion, so it
                // stays terminal for the drainer -- give the cluster a bounded
                // settle budget (rolling upgrade), then quarantine the slot.
                capabilityGapAttempts++;
                long now = System.nanoTime();
                if (lastCapabilityGapNanos != 0L) {
                    // Charge only the interval since the PREVIOUS gap sweep,
                    // and only when no transient error interrupted it. Time
                    // spent in a transient window -- before the first gap or
                    // between two gaps -- is never charged to the episode.
                    capabilityGapElapsedNanos += now - lastCapabilityGapNanos;
                }
                lastCapabilityGapNanos = now;
                long elapsedMs = capabilityGapElapsedNanos / 1_000_000L;
                boolean exhausted = capabilityGapAttempts >= DEFAULT_MAX_DURABLE_ACK_MISMATCH_ATTEMPTS
                        || capabilityGapElapsedNanos >= capabilityGapBudgetNanos;
                BackgroundDrainerListener l = listener;
                if (exhausted) {
                    LOG.error("drainer giving up on slot {} after {} durable-ack-mismatch attempts ({}ms): {}",
                            slotPath, capabilityGapAttempts, elapsedMs, e.getMessage());
                    if (l != null) {
                        try {
                            l.onDurableAckPersistentFailure(slotPath, capabilityGapAttempts, elapsedMs);
                        } catch (Throwable cb) {
                            LOG.warn("drainer listener onDurableAckPersistentFailure threw: {}",
                                    cb.getMessage());
                        }
                    }
                    lastErrorMessage = e.getMessage();
                    OrphanScanner.markFailed(slotPath,
                            "durable-ack persistently unavailable after "
                                    + capabilityGapAttempts + " attempts: " + e.getMessage());
                    outcome = DrainOutcome.FAILED;
                    return null;
                }
                boundedByBudget = true;
                if (l != null) {
                    try {
                        l.onDurableAckUnavailable(slotPath, capabilityGapAttempts);
                    } catch (Throwable cb) {
                        LOG.warn("drainer listener onDurableAckUnavailable threw: {}",
                                cb.getMessage());
                    }
                }
                LOG.warn("drainer slot {} attempt {}: durable-ack unavailable, retrying after backoff",
                        slotPath, capabilityGapAttempts);
            } catch (Throwable t) {
                if (t instanceof Error) {
                    // java.lang.Error (OOM, LinkageError, StackOverflowError)
                    // is a JVM/programming failure, not a transport outage:
                    // retrying cannot clear it, and spinning here would pin
                    // the slot .lock forever with no .failed sentinel and only
                    // a throttled, possibly-null-message WARN as a trace.
                    // Rethrow: run()'s outer catch quarantines the slot
                    // (markFailed + FAILED) and its finally releases the lock
                    // -- quarantine-and-exit, exactly as genuine terminals do.
                    throw (Error) t;
                }
                // INVARIANT B: a transport failure -- the whole cluster is
                // unreachable right now (server down, network partition) -- is
                // TRANSIENT, exactly as the live sender's background loop treats
                // it. The server will come back; keep retrying (capped backoff)
                // until it does, stopRequested, or SF exhaustion. NEVER quarantine
                // the slot on a transport error. Genuine terminals (auth /
                // non-421 upgrade / durable-ack capability gap) are handled by the
                // catches above and still fail fast. A QWP version mismatch also
                // reaches here (it extends HttpClientException, not
                // WebSocketUpgradeException) and is intentionally retried under
                // Invariant B -- but it is NOT a transport outage, so log it
                // truthfully below rather than mislabelling it "cluster unreachable".
                lastErrorMessage = t.getMessage();
                // Pause the episode wall clock: the gap-to-gap interval this
                // window interrupts is never charged. Attempts and elapsed
                // already accumulated are preserved (anti-evasion: see the
                // budget comment above).
                lastCapabilityGapNanos = 0L;
                long nowWarn = System.nanoTime();
                if (nowWarn - lastTransportWarnNanos >= 5_000_000_000L) {
                    if (t instanceof QwpVersionMismatchException) {
                        // The cluster IS reachable: every endpoint completed the
                        // WebSocket upgrade but advertised a QWP protocol version
                        // this client cannot speak. A rolling upgrade clears this
                        // once peers converge, so Invariant B keeps retrying -- but
                        // if it persists the client binary is version-incompatible
                        // with the whole cluster and an operator must intervene
                        // (upgrade the client or the servers). Name the real
                        // condition so it is diagnosable, not hidden behind a
                        // network-outage message.
                        LOG.warn("drainer slot {}: every reachable endpoint advertises an unsupported "
                                        + "QWP protocol version ({}); retrying (rolling-upgrade window) -- "
                                        + "if this persists the client is version-incompatible with the cluster",
                                slotPath, t.getMessage());
                    } else {
                        LOG.warn("drainer slot {}: cluster unreachable ({}), retrying after backoff",
                                slotPath, t.getMessage());
                    }
                    lastTransportWarnNanos = nowWarn;
                }
            }
            // Backoff before the next sweep. Honor stopRequested by parking in
            // small chunks rather than a single long park so close() doesn't
            // wait for a full sleep to elapse. Only the bounded (capability-gap)
            // path clamps to the remaining budget (the post-gap sleep is charged
            // to the episode by the next gap sweep) so it escalates promptly once
            // the accumulated gap-time runs out; the transient failover path
            // retries indefinitely and just backs off (capped exponential),
            // never busy-looping past an exhausted budget.
            long jitter = ThreadLocalRandom.current().nextLong(Math.max(1L, backoffMillis));
            long sleepMillis = backoffMillis + jitter;
            if (boundedByBudget) {
                sleepMillis = Math.min(sleepMillis,
                        Math.max(0L, (capabilityGapBudgetNanos - capabilityGapElapsedNanos) / 1_000_000L));
            }
            if (sleepMillis > 0L && !stopRequested) {
                long parkDeadlineNanos = System.nanoTime() + sleepMillis * 1_000_000L;
                long remaining;
                while (!stopRequestedOrInterrupted()
                        && (remaining = parkDeadlineNanos - System.nanoTime()) > 0L) {
                    LockSupport.parkNanos(Math.min(remaining, STOP_CHECK_PARK_CHUNK_NANOS));
                }
            }
            backoffMillis = Math.min(backoffMillis * 2L, reconnectMaxBackoffMillis);
        }
        outcome = DrainOutcome.STOPPED;
        return null;
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

    public DrainOutcome outcome() {
        return outcome;
    }

    public void requestStop() {
        stopRequested = true;
        // Wake the drainer out of any backoff/poll park immediately so the
        // pool's bounded stop-grace window is spent unwinding (release slot
        // lock, close engine), not sleeping out the remainder of a capped
        // exponential backoff.
        Thread t = runnerThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    /** True once {@link #requestStop()} has been called. */
    public boolean isStopRequested() {
        return stopRequested;
    }

    /**
     * Stop check for the runner thread's park loops that also folds a
     * pending thread interrupt into the stop protocol. The pool delivers
     * cancellation as an interrupt ({@code shutdownNow}) and pairs it with a
     * {@link #requestStop()} sweep — but that pairing is caller discipline.
     * An interrupt arriving WITHOUT the flag would otherwise be pathological:
     * every wait here is a {@code LockSupport.parkNanos}, which returns
     * immediately while the status is pending and never clears it, so the
     * backoff/poll loops would degrade into a 100% CPU busy-spin (for as
     * long as an outage lasts) with the slot lock pinned. Mapping the
     * interrupt onto {@code stopRequested} routes them through the normal
     * STOPPED exit instead.
     * <p>
     * The status is deliberately left set ({@code isInterrupted()}, not
     * {@code Thread.interrupted()}): the teardown in {@link #run}'s finally
     * relies on it — {@code loop.close()}'s latch await must throw rather
     * than block under a wedged I/O thread, routing engine teardown through
     * the delegation protocol (pinned by
     * {@code BackgroundDrainerInterruptedTeardownTest}).
     * <p>
     * Called on the runner thread only. The unsynchronized check-then-set is
     * safe against a concurrent {@link #requestStop()}: both writers only
     * ever transition {@code stopRequested} false→true.
     */
    private boolean stopRequestedOrInterrupted() {
        if (!stopRequested && Thread.currentThread().isInterrupted()) {
            stopRequested = true;
        }
        return stopRequested;
    }

    @Override
    public void run() {
        runnerThread = Thread.currentThread();
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
            // One iteration per wire session. Re-entered ONLY when a mid-drain
            // reconnect sweep hit a durable-ack CAPABILITY gap: that is the
            // exact rolling-upgrade condition the settle budget in
            // connectWithDurableAckRetry() exists for, so it must not
            // quarantine on the first sweep the way the initial-connect path
            // never does. The engine stays alive across sessions (it holds the
            // slot lock; only loop + client are recycled), and target remains
            // valid -- the slot is orphaned, nothing appends to it.
            drain:
            while (!stopRequested) {
                loop = new CursorWebSocketSendLoop(
                        client, engine,
                        0L, CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                        clientFactory,
                        reconnectMaxDurationMillis,
                        reconnectInitialBackoffMillis,
                        reconnectMaxBackoffMillis,
                        requestDurableAck,
                        durableAckKeepaliveIntervalMillis,
                        maxHeadFrameRejections,
                        poisonMinEscalationWindowMillis);
                loop.start();

                while (!stopRequestedOrInterrupted()) {
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
                        if (loop.capabilityGapTerminal() != null) {
                            // Capability gap mid-drain: recycle the wire, NOT
                            // the slot. connectWithDurableAckRetry() owns the
                            // episode budget (16 consecutive gap sweeps /
                            // wall clock) and drops the sentinel itself if the
                            // gap persists. The loop's own failed sweep is not
                            // counted toward the fresh episode -- an off-by-one
                            // that is immaterial at budget 16.
                            LOG.warn("drainer slot {}: durable-ack capability gap "
                                            + "mid-drain ({}), re-entering settle budget",
                                    slotPath, t.getMessage());
                            try {
                                loop.close();
                            } catch (Throwable closeFailure) {
                                // Interrupted shutdown mid-recycle (pool
                                // shutdownNow): the old I/O thread is still
                                // alive, so opening a new wire session against
                                // the same engine would race its exit — and
                                // closing the client under a possibly mid-send
                                // thread risks SEGV. Bail out; the finally
                                // re-runs loop.close(), which re-signals the
                                // failed stop and routes client/engine
                                // teardown to the delegation protocol there.
                                LOG.warn("drainer slot {}: stop requested mid-recycle and the "
                                                + "I/O thread did not stop ({}); abandoning recycle",
                                        slotPath, closeFailure.getMessage());
                                outcome = stopRequested ? DrainOutcome.STOPPED : DrainOutcome.FAILED;
                                return;
                            }
                            try {
                                client.close();
                            } catch (Throwable ignored) {
                            }
                            loop = null;
                            client = connectWithDurableAckRetry();
                            if (client == null) {
                                // outcome already set (FAILED after budget
                                // exhaustion, or STOPPED); sentinel handled.
                                return;
                            }
                            continue drain;
                        }
                        String msg = t.getMessage();
                        LOG.error("drainer wire error for slot {}: {}", slotPath, msg);
                        lastErrorMessage = msg;
                        OrphanScanner.markFailed(slotPath, "wire: " + msg);
                        outcome = DrainOutcome.FAILED;
                        return;
                    }
                    java.util.concurrent.locks.LockSupport.parkNanos(POLL_NANOS);
                }
                // Inner loop exits only on stopRequested; fall through to the
                // outer condition, which is false for the same reason.
            }
            outcome = DrainOutcome.STOPPED;
        } catch (Throwable t) {
            String msg = t.getMessage();
            if (slotPath != null) {
                // Real orphan slot: a setup failure means unacked data on disk
                // could not be drained to the server -- a durability concern
                // that stays at ERROR so operators see it.
                LOG.error("drainer setup failed for slot {}: {}", slotPath, msg, t);
            } else if (LOG.isDebugEnabled()) {
                // Only @TestOnly drainers carry a null slot (zero segment size);
                // they fast-fail by design and would otherwise flood CI logs.
                // The isDebugEnabled() guard avoids the varargs array and the
                // message formatting when DEBUG is off, so it makes no garbage.
                LOG.debug("drainer setup failed for slot {}: {}", slotPath, msg, t);
            }
            lastErrorMessage = msg;
            try {
                OrphanScanner.markFailed(slotPath, "setup: " + msg);
            } catch (Throwable ignored) {
                // best-effort
            }
            outcome = DrainOutcome.FAILED;
        } finally {
            boolean ioThreadStopped = true;
            if (loop != null) {
                try {
                    loop.close();
                } catch (Throwable e) {
                    // The loop's I/O thread would not stop — close() was
                    // interrupted (the pool's shutdownNow path) while the
                    // thread sat in a blocking native connect/send that
                    // neither unpark nor interrupt cancels. Freeing the
                    // client's buffers or unmapping the engine now would
                    // race the live thread (C5 SEGV); both are delegated to
                    // the thread's own exit path below.
                    ioThreadStopped = false;
                    LOG.warn("drainer slot {}: I/O thread did not stop during close ({}); "
                                    + "delegating client/engine teardown to its exit path",
                            slotPath, e.getMessage());
                }
            }
            if (client != null && ioThreadStopped) {
                // Skipped on a failed stop: the thread may be mid-send on
                // this very client; ioLoop's finally closes the loop's
                // current client (this one, unless a reconnect swapped it —
                // in which case swapClient already closed this reference).
                try {
                    client.close();
                } catch (Throwable ignored) {
                }
            }
            if (engine != null) {
                // Failed-stop hand-off: delegateEngineClose() makes the I/O
                // thread run engine.close() strictly after its last engine
                // access, releasing the slot lock as soon as the stuck wire
                // call resolves — deferred teardown, never abandoned. The
                // false return covers the race where the thread exited
                // between the failed close() and now: then it is safe (and
                // necessary) to close the engine here.
                if (ioThreadStopped || !loop.delegateEngineClose()) {
                    try {
                        // engine.close() releases the slot lock too.
                        engine.close();
                    } catch (Throwable ignored) {
                    }
                } else {
                    LOG.warn("drainer slot {}: engine close delegated to the I/O thread; "
                            + "slot lock releases when it exits", slotPath);
                }
            }
            // Don't let a later requestStop() unpark an unrelated task that
            // the pool's executor may have scheduled onto this same thread.
            runnerThread = null;
        }
    }

    /**
     * Plug an observer for durable-ack-related events. {@code null} clears
     * any previously installed listener. See {@link BackgroundDrainerListener}
     * for thread-safety contract.
     */
    public void setListener(BackgroundDrainerListener listener) {
        this.listener = listener;
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
