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

import io.questdb.client.std.Files;
import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/**
 * Background worker that trims every registered {@link SegmentRing}'s
 * segments after their frames have been ACK'd by the server, and runs the
 * disk barriers (periodic sync, the watermark sync before an unlink). The
 * {@code munmap + unlink + fsync} of a trim happen on this thread, never on
 * the user-thread / I/O-thread paths. The ring's next segment is created by
 * the producer itself at rotation ({@link SegmentProvisioner}).
 * <p>
 * One instance can serve many rings (typically all {@code Sender} instances
 * in a JVM). Polls each ring on a configurable tick (default 1 ms) — short
 * enough that a producer rarely sees {@link SegmentRing#BACKPRESSURE_NO_SPARE}
 * in the steady state, long enough that an idle JVM doesn't burn CPU.
 */
public final class SegmentManager implements QuietCloseable {

    public static final long UNLIMITED_TOTAL_BYTES = Long.MAX_VALUE;
    private static final int ENTRY_DEREGISTERED = 3;
    private static final int ENTRY_DEREGISTERED_IN_SERVICE = 2;
    private static final int ENTRY_IN_SERVICE = 1;
    private static final int ENTRY_REGISTERED = 0;
    private static final AtomicReferenceFieldUpdater<RingEntry, Runnable> ENTRY_CLEANUP_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(RingEntry.class, Runnable.class, "quiescenceCleanup");
    private static final AtomicIntegerFieldUpdater<RingEntry> ENTRY_STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(RingEntry.class, "state");
    private static final Logger LOG = LoggerFactory.getLogger(SegmentManager.class);
    private static final int MAX_TRIMS_PER_RING_PASS = 64;
    private static final long TRIM_RETRY_INITIAL_NANOS = 4_000_000L;
    private static final long TRIM_RETRY_MAX_NANOS = 1_024_000_000L;
    private static final int TRIM_RETRY_NONE = 0;
    private static final int TRIM_RETRY_POST_BARRIER = 3;
    private static final int TRIM_RETRY_PRE_BARRIER = 1;
    private static final int TRIM_RETRY_UNLINK = 2;
    private static final long WORKER_JOIN_TIMEOUT_MILLIS = 5_000L;

    // Every segment the registered rings own, plus their live side-file bytes,
    // is charged here. Private to this manager unless the caller hands the same
    // budget to several managers -- a sender pool does, so sf_max_total_bytes
    // caps all of its senders together.
    private final SegmentBudget budget;
    private final FilesFacade filesFacade;
    private final Object lock = new Object();
    // Reused by the worker thread each tick to snapshot `rings` under the
    // lock without per-tick allocation. Owned exclusively by workerLoop().
    private final ObjList<RingEntry> ringSnapshot = new ObjList<>();
    private final ObjList<RingEntry> rings = new ObjList<>();
    private final long segmentSizeBytes;
    // Reused by the manager worker while it checkpoints one ring. The entry
    // in-service state keeps these segment mappings alive until the pass ends.
    private final ObjList<MmapSegment> syncScratch = new ObjList<>();
    private final LongSupplier ticks;
    // Reused by the worker for one bounded trim quantum. Keeping the unlinked
    // prefix here until the post-unlink directory barrier succeeds avoids both
    // per-pass allocation and publishing logical removal before it is durable.
    private final MmapSegment[] trimBatch = new MmapSegment[MAX_TRIMS_PER_RING_PASS];
    // Handed to each ring: rotation and pin release may make a segment trimmable.
    private final Runnable workerWakeup = this::wakeWorker;
    // Test seam: runs after a deferred ring-pass cleanup returns. Null in
    // production; public sender/pool lifecycle tests use it to observe exact
    // callback completion without sleeps or polling.
    private volatile Runnable afterRingCleanupHook;
    // Test seam: runs at the top of deferUntilWorkerExit, before the
    // worker-liveness check. Null in production; registration-failure tests
    // throw from it to simulate an allocation failure (OOM building the
    // cleanup lambda or growing exitCleanups) while the worker is still
    // live. Callers must treat such a throw as "worker state unknown",
    // never as the exact false return meaning the worker loop has exited.
    private volatile Runnable beforeExitCleanupRegistrationHook;
    // Test seam: records entry into the per-ring quiescence wait. Null in
    // production; owned-engine close tests use it to prove they take only the
    // stronger whole-manager join path, not two sequential timeout budgets.
    private volatile Runnable beforeRingQuiescenceAwaitHook;
    // Test seam: runs on the worker thread just before the trim block's
    // synchronized(lock) entry. Null in production; only
    // SegmentManagerTrimDeregisterRaceTest installs it, to deterministically
    // inject a deregister(ring) call into the exact race window that the
    // entry-state check inside the trim block closes for budget accounting.
    private volatile Runnable beforeTrimSyncHook;
    // Test seam invoked exactly when a retry transition/recovery is logged.
    // Null in production; persistent-failure tests use it to prove log bounds
    // without binding to a particular SLF4J backend.
    private volatile Runnable retryLogHook;
    // Entry the worker is claiming or servicing, or null between passes.
    // Volatile publication lets teardown find the entry after deregister()
    // removes it from rings. RingEntry.state is the authoritative barrier:
    // the worker publishes this reference before its REGISTERED->IN_SERVICE
    // CAS and only touches ring resources after that CAS succeeds.
    private volatile RingEntry inService;
    // Cleanup actions handed to the worker's exit block by an owning engine
    // whose close() found the worker still mid service pass after the bounded
    // join timed out (see deferUntilWorkerExit). Guarded by {@link #lock};
    // consumed exactly once by the worker-loop finally, which runs them
    // OUTSIDE `lock`: they perform syscalls (munmap/unlink/flock release),
    // and no caller-facing lock may ever be held while running third-party
    // cleanup code.
    private ObjList<Runnable> exitCleanups;
    // A private manager belongs to exactly one CursorSendEngine. Its callback
    // is preallocated by that engine and stored directly here, so the critical
    // timed-out-close handoff never allocates an ObjList or grows a backing
    // array. Guarded by lock and consumed on worker-loop exit outside lock.
    private Runnable ownedEngineExitCleanup;
    private volatile boolean running;
    // Guarded by lock: set by the worker when it leaves its service loop.
    private boolean workerLoopExited;
    // volatile: read by awaitRingQuiescence() from arbitrary caller threads
    // while the @TestOnly setter may run on another.
    private volatile long workerJoinTimeoutMillis = WORKER_JOIN_TIMEOUT_MILLIS;
    // volatile because wakeWorker() reads workerThread without holding the
    // monitor; the synchronized start()/close() pair handles the
    // start-vs-close ordering.
    private volatile Thread workerThread;

    public SegmentManager(long segmentSizeBytes) {
        this(segmentSizeBytes, new SegmentBudget(UNLIMITED_TOTAL_BYTES));
    }

    /**
     * @param segmentSizeBytes per-segment file size in bytes
     * @param budget           charged for every segment the manager tracks, and
     *                         shareable between managers (a sender pool does, so
     *                         sf_max_total_bytes caps all of its senders
     *                         together). Its capacity is the upper bound on total
     *                         bytes across all registered rings — counts every segment
     *                         the ring owns (active + sealed), including bytes
     *                         already on disk at register-time (e.g. after
     *                         recovery or orphan adoption). When a rotation's
     *                         segment would exceed this, the producer stays in the
     *                         {@link SegmentRing#BACKPRESSURE_NO_SPARE} state
     *                         until ACK-driven trim frees space. Pass
     *                         {@link #UNLIMITED_TOTAL_BYTES} to disable. Must be
     *                         at least one {@code segmentSizeBytes}; a sensible
     *                         lower bound for a single ring is
     *                         {@code 2 × segmentSizeBytes}, the active segment
     *                         plus the one it rotates into.
     */
    public SegmentManager(long segmentSizeBytes, SegmentBudget budget) {
        this(segmentSizeBytes, budget, FilesFacade.INSTANCE, System::nanoTime);
    }

    @TestOnly
    public SegmentManager(long segmentSizeBytes, long maxTotalBytes, FilesFacade filesFacade) {
        this(segmentSizeBytes, new SegmentBudget(maxTotalBytes), filesFacade, System::nanoTime);
    }

    @TestOnly
    public SegmentManager(
            long segmentSizeBytes,
            long maxTotalBytes,
            FilesFacade filesFacade,
            LongSupplier ticks
    ) {
        this(segmentSizeBytes, new SegmentBudget(maxTotalBytes), filesFacade, ticks);
    }

    @TestOnly
    public SegmentManager(
            long segmentSizeBytes,
            SegmentBudget budget,
            FilesFacade filesFacade,
            LongSupplier ticks
    ) {
        if (segmentSizeBytes < MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1) {
            throw new IllegalArgumentException("segmentSizeBytes too small: " + segmentSizeBytes);
        }
        if (budget == null) {
            throw new IllegalArgumentException("budget must not be null");
        }
        if (budget.getCapacityBytes() < segmentSizeBytes) {
            throw new IllegalArgumentException(
                    "maxTotalBytes (" + budget.getCapacityBytes() + ") must allow at least one segment of "
                            + segmentSizeBytes + " bytes");
        }
        this.budget = budget;
        this.filesFacade = filesFacade;
        this.segmentSizeBytes = segmentSizeBytes;
        this.ticks = ticks;
    }

    FilesFacade filesFacade() {
        return filesFacade;
    }

    @Override
    public synchronized void close() {
        running = false;
        Thread t = workerThread;
        if (t != null) {
            LockSupport.unpark(t);
            // A pending interrupt on the caller makes Thread.join() throw at
            // once; clear it so the join actually reaps the worker (which
            // still owns segment files), then restore it for the rest of the
            // interrupted-teardown protocol.
            boolean interrupted = Thread.interrupted();
            long deadlineNanos = System.nanoTime() + workerJoinTimeoutMillis * 1_000_000L;
            try {
                while (t.isAlive()) {
                    long remainingMillis = (deadlineNanos - System.nanoTime()) / 1_000_000L;
                    if (remainingMillis <= 0) {
                        break;
                    }
                    try {
                        t.join(remainingMillis);
                    } catch (InterruptedException ignored) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            if (t.isAlive()) {
                synchronized (lock) {
                    if (!workerLoopExited) {
                        // workerThread stays set so isWorkerReaped() reports
                        // the incomplete shutdown and a later close() can
                        // retry the join.
                        LOG.warn("SegmentManager worker did not stop before close wait completed");
                        return;
                    }
                }
                // The worker has left its service loop (workerLoopExited) and
                // is now running only its finite exit cleanups -- the owning
                // engine's finishClose(), which releases the slot flock and
                // only THEN publishes closeCompleted. It can no longer wedge in
                // a service pass, so give it a second bounded join to finish
                // before reaping. Without this, a bounded-join timeout that
                // lands mid-finishClose reaps the worker (workerThread=null =>
                // isWorkerReaped()) while the flock release is still in flight,
                // so a caller reading isCloseCompleted() observes a stale false
                // and a spurious flock-release retry gets scheduled. This reuses
                // the same join-under-monitor pattern as the bounded join above
                // -- the worker uses a lock-free CAS for exactly-once cleanup
                // and never blocks on this monitor -- and still reaps on
                // timeout, so a pathologically slow cleanup cannot hang close().
                //
                // Budget with the fixed WORKER_JOIN_TIMEOUT_MILLIS, NOT the
                // tunable workerJoinTimeoutMillis: the first join bounds a
                // possibly-wedged SERVICE pass (tests shrink it to force the
                // timed-out path), but the exit cleanups are finite and must be
                // allowed to finish regardless of that tuning. In production
                // both values are equal, so this only matters under a shrunk
                // test override.
                boolean cleanupInterrupted = Thread.interrupted();
                long cleanupDeadlineNanos = System.nanoTime() + WORKER_JOIN_TIMEOUT_MILLIS * 1_000_000L;
                try {
                    while (t.isAlive()) {
                        long remainingMillis = (cleanupDeadlineNanos - System.nanoTime()) / 1_000_000L;
                        if (remainingMillis <= 0) {
                            break;
                        }
                        try {
                            t.join(remainingMillis);
                        } catch (InterruptedException ignored) {
                            cleanupInterrupted = true;
                        }
                    }
                } finally {
                    if (cleanupInterrupted) {
                        Thread.currentThread().interrupt();
                    }
                }
                // If the cleanups still have not finished (pathologically slow
                // syscall), fall through and reap anyway -- identical to the
                // best-effort behaviour before this second join existed.
            }
            workerThread = null;
        }
    }

    /**
     * Hands the single owning engine's preallocated close callback to the
     * worker exit block. Registration only assigns a reference under
     * {@link #lock}; it cannot allocate in the timed-out teardown path.
     * Returns {@code false} only after the worker loop has exited (or when it
     * never started), so the caller may then clean up inline safely.
     */
    public boolean deferOwnedEngineCloseUntilWorkerExit(Runnable cleanup) {
        synchronized (lock) {
            if (workerLoopExited || workerThread == null) {
                return false;
            }
            if (ownedEngineExitCleanup != null && ownedEngineExitCleanup != cleanup) {
                throw new IllegalStateException("owned manager already has an engine-exit cleanup");
            }
            ownedEngineExitCleanup = cleanup;
            return true;
        }
    }

    /**
     * Hands a cleanup action to the current service pass for {@code ring}.
     * The worker runs it outside the manager lock immediately after that pass
     * finishes. Returns {@code false} when no pass for the ring remains in
     * flight, in which case the caller already owns a quiescent ring and must
     * run the cleanup itself.
     * <p>
     * Registration and pass completion coordinate through the entry's atomic
     * state and callback fields, so there is no gap without an owner: either
     * this method attaches the cleanup while the pass remains active, the
     * worker claims an attached cleanup while completing, or this method
     * observes completed state and rejects the handoff. A repeated registration
     * for the same pass returns {@code true} without replacing its existing
     * owner.
     */
    public boolean deferUntilRingQuiescent(SegmentRing ring, Runnable cleanup) {
        RingEntry e = inService;
        if (e == null || e.ring != ring || !e.isInService()) {
            return false;
        }
        Runnable existing = ENTRY_CLEANUP_UPDATER.get(e);
        if (existing == null && ENTRY_CLEANUP_UPDATER.compareAndSet(e, null, cleanup)) {
            if (e.isInService()) {
                return true;
            }
            // Completion changed the state before observing the callback.
            // Remove our callback and clean inline, unless the worker already
            // claimed it (in which case that worker remains the owner).
            return !ENTRY_CLEANUP_UPDATER.compareAndSet(e, cleanup, null);
        }
        // A callback already attached to this pass is an owner. The sole
        // production caller always supplies the engine's preallocated bound
        // callback; duplicates intentionally do not replace it.
        return true;
    }

    /**
     * Hands a cleanup action to the worker thread's exit block, to run
     * strictly after the worker loop has finished its final service pass --
     * i.e. after the last point where the worker can create, write or unlink
     * anything under a registered ring's slot directory. Returns {@code false}
     * when the worker loop has already exited (or the worker never started):
     * the caller must run the cleanup itself, which is equally safe for the
     * same reason -- no further worker access to the slot is possible.
     * <p>
     * This is the slot-ownership transfer used by an owning engine's close()
     * when the bounded worker join timed out: instead of retiring the slot
     * until process exit, ring/watermark/flock release moves to the worker,
     * which is provably the last thread able to touch the slot directory.
     * The registration here and the exit block's {@code workerLoopExited}
     * flip share {@link #lock}, so the cleanup runs exactly once: either it
     * is registered before the flip and the worker runs it, or the flip won
     * and this method rejects the handoff.
     * <p>
     * May throw on allocation failure (the lambda at the call site, the
     * {@code ObjList}, or its growth). A throw carries NO liveness
     * information: the worker was never observed, so the caller must treat
     * it as "worker possibly still live" and retain resources — never as
     * the exact {@code false} return above.
     */
    public boolean deferUntilWorkerExit(Runnable cleanup) {
        Runnable hook = beforeExitCleanupRegistrationHook;
        if (hook != null) {
            hook.run();
        }
        synchronized (lock) {
            if (workerLoopExited || workerThread == null) {
                return false;
            }
            if (exitCleanups == null) {
                exitCleanups = new ObjList<>();
            }
            exitCleanups.add(cleanup);
            return true;
        }
    }

    /**
     * Quiescence barrier for {@link #deregister(SegmentRing)}. Blocks until
     * the worker thread is provably no longer executing a service pass for
     * {@code ring}, or the worker-join timeout elapses. After this returns
     * {@code true}, the worker will never again touch the ring, its
     * watermark, or path names under its slot directory: deregister has
     * removed the entry from the registry, a stale snapshot entry that has
     * not started its pass is skipped by the registration check at the top
     * of {@link #serviceRing(RingEntry)}, and this method has observed the
     * end of any in-flight pass. Only then may the caller release dependent
     * resources (ring, watermark, segment files, slot lock).
     * <p>
     * Returns {@code true} immediately when no worker is running or when
     * called from the worker thread itself (test hooks inject deregister
     * calls there; waiting would self-deadlock). Returns {@code false} when
     * the in-flight pass did not finish within the timeout — the caller must
     * treat the worker as still live and leak rather than release.
     * <p>
     * A pending caller interrupt is preserved but does not abort the wait,
     * mirroring {@link #close()}.
     */
    public boolean awaitRingQuiescence(SegmentRing ring) {
        Runnable hook = beforeRingQuiescenceAwaitHook;
        if (hook != null) {
            hook.run();
        }
        Thread t = workerThread;
        if (t == null || t == Thread.currentThread()) {
            return true;
        }
        long deadlineNanos = System.nanoTime() + workerJoinTimeoutMillis * 1_000_000L;
        boolean interrupted = Thread.interrupted();
        try {
            RingEntry e = inService;
            if (e == null || e.ring != ring || !e.isInService()) {
                return true;
            }
            synchronized (lock) {
                e.quiescenceWaiters++;
                try {
                    while (e.isInService()) {
                        long remainingNanos = deadlineNanos - System.nanoTime();
                        if (remainingNanos <= 0) {
                            return false;
                        }
                        try {
                            // Round up so a sub-millisecond remainder still waits
                            // instead of spinning through wait(0) == wait-forever.
                            lock.wait(Math.max(1L, remainingNanos / 1_000_000L));
                        } catch (InterruptedException ignored) {
                            interrupted = true;
                        }
                    }
                } finally {
                    e.quiescenceWaiters--;
                }
            }
            return true;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * True when no manager worker thread can be running: either
     * {@link #start()} was never called, or a {@link #close()} confirmed
     * worker termination and reaped the thread. Owners use this as a
     * stronger fallback barrier when {@link #awaitRingQuiescence(SegmentRing)}
     * times out but a subsequent {@code close()} join succeeded.
     */
    public synchronized boolean isWorkerReaped() {
        return workerThread == null;
    }

    /**
     * Stop tracking {@code ring}. Idempotent; safe to call from any thread.
     * <p>
     * Non-blocking: a worker service pass already in flight for this ring
     * may still be running when this returns. Callers about to release
     * resources the worker can reach (the ring itself, its watermark, its
     * segment files, or the slot lock guarding its directory) MUST follow
     * up with {@link #awaitRingQuiescence(SegmentRing)} and only release on
     * a {@code true} result.
     */
    public void deregister(SegmentRing ring) {
        synchronized (lock) {
            for (int i = 0, n = rings.size(); i < n; i++) {
                RingEntry e = rings.get(i);
                if (e.ring == ring) {
                    // Reverse the ring's charge to the budget: the seed from
                    // register() plus every segment the producer created
                    // since, minus the trims -- all of it is
                    // totalSegmentBytes() now, including a segment the
                    // provisioner holds after a failed rotation.
                    e.deregister();
                    budget.release(ring.totalSegmentBytes());
                    if (e.sideFileBytes != null) {
                        budget.removeSideFileGauge(e.sideFileBytes);
                    }
                    rings.remove(i);
                    return;
                }
            }
        }
    }

    /**
     * Register a ring for trim and disk barriers, and give it a
     * {@link SegmentProvisioner} that creates its segments in {@code dir}
     * ({@code null} = memory mode) on the producer thread, charged to this
     * manager's budget. The ring MUST already have its initial active segment
     * in place.
     * <p>
     * Also wires the ring's wakeup callback to {@link #wakeWorker()}, so a
     * rotation or a pin release that makes a segment trimmable wakes the
     * parked worker.
     */
    public void register(SegmentRing ring, String dir) {
        register(ring, dir, null, 0L);
    }

    /**
     * Same as {@link #register(SegmentRing, String)} but also wires the
     * {@link AckWatermark} the manager syncs before it unlinks a trimmed
     * segment. The watermark is owned and written by the caller (typically
     * {@link CursorSendEngine}, on every ACK); the manager only syncs it
     * and never closes it. {@code null} watermark falls back to the
     * legacy behaviour: no on-disk watermark, recovery seeds from
     * {@code lowestSurvivingBaseSeq - 1}.
     */
    public void register(SegmentRing ring, String dir, AckWatermark watermark) {
        register(ring, dir, watermark, 0L);
    }

    /**
     * Registers a ring with an optional periodic data-checkpoint interval.
     * A positive interval requires disk-backed store-and-forward mode.
     */
    public void register(SegmentRing ring, String dir, AckWatermark watermark, long syncIntervalNanos) {
        register(ring, dir, watermark, syncIntervalNanos, null);
    }

    /**
     * Same as {@link #register(SegmentRing, String, AckWatermark, long)} but
     * also wires a live gauge of the slot's {@code .symbol-dict} side-file
     * bytes. The manager reads the gauge at every provisioning cap check so
     * the dictionary counts against {@code sf_max_total_bytes} alongside the
     * {@code .sfa} segments; a {@code null} gauge contributes zero (memory
     * mode, degraded full-dict sessions).
     * <p>
     * The gauge is invoked under the {@link SegmentBudget}'s monitor, so it
     * must be wait-free and must not throw: a throwing gauge terminates the
     * worker of every manager charging that budget, and a blocking gauge
     * stalls register/deregister and provisioning for all of their slots.
     */
    public void register(SegmentRing ring, String dir, AckWatermark watermark, long syncIntervalNanos, LongSupplier sideFileBytes) {
        if (syncIntervalNanos < 0L) {
            throw new IllegalArgumentException("syncIntervalNanos must not be negative");
        }
        if (syncIntervalNanos > 0L && dir == null) {
            throw new IllegalArgumentException("periodic sync requires a segment directory");
        }
        if (ring.maxBytesPerSegment() != segmentSizeBytes) {
            // The ring's oversized-payload check assumes the segments this
            // manager's provisioner creates are maxBytesPerSegment large.
            throw new IllegalArgumentException("ring segment size " + ring.maxBytesPerSegment()
                    + " does not match the manager's " + segmentSizeBytes);
        }
        // Account for bytes the ring already owns when it joins. A recovered
        // ring (post-restart, orphan adoption) can come up at-or-above the cap;
        // without this seed, the budget stays at 0 and the per-tick cap check
        // at serviceRing would let the manager keep provisioning new spares on
        // top of the recovered set, effectively doubling the documented cap.
        long ringBytes = ring.totalSegmentBytes();
        // Start the file-generation counter past whatever's already on disk in
        // this slot, or on recovery the first rotation would try to create
        // sf-0000000000000000.sfa, collide with the recovered active (O_EXCL
        // refuses it) and backpressure the producer on every retry.
        // Memory-mode rings have no dir; nothing to scan.
        if (!ring.hasSegmentSource()) {
            long firstGeneration = dir == null ? 0L : scanMaxGeneration(dir) + 1L;
            ring.setSegmentSource(new SegmentProvisioner(ring, dir, firstGeneration, segmentSizeBytes, budget, filesFacade));
        }
        RingEntry e = new RingEntry(ring, dir, watermark, sideFileBytes, syncIntervalNanos, ticks.getAsLong());
        // ObjList.add either throws before storing e or makes the entry visible.
        // Once visible, only non-throwing state commits may remain, so the gauge
        // (whose list can also throw on growth) is added first and withdrawn if
        // the entry cannot be published.
        synchronized (lock) {
            if (sideFileBytes != null) {
                budget.addSideFileGauge(sideFileBytes);
            }
            try {
                rings.add(e);
            } catch (Throwable t) {
                if (sideFileBytes != null) {
                    budget.removeSideFileGauge(sideFileBytes);
                }
                throw t;
            }
            budget.charge(ringBytes);
            if (syncIntervalNanos > 0L) {
                ring.enablePeriodicSync();
            }
        }
        ring.setManagerWakeup(workerWakeup);
        // Nudge the worker so it picks up the new ring on its very next
        // iteration: a recovered ring may already hold trimmable segments,
        // and the first periodic-sync pass runs immediately. wakeWorker is
        // cheap (a single LockSupport.unpark) and a no-op when the worker has
        // not been started yet.
        wakeWorker();
    }

    @TestOnly
    public SegmentRing getInServiceRingForTesting() {
        RingEntry entry = inService;
        return entry == null ? null : entry.ring;
    }

    @TestOnly
    public long getCapAccountedBytesForTesting() {
        return budget.getAccountedBytes();
    }

    @TestOnly
    public long getTotalBytesForTesting() {
        return budget.getSegmentBytes();
    }

    @TestOnly
    public Thread getWorkerThreadForTesting() {
        return workerThread;
    }

    @TestOnly
    public boolean serviceRingForTesting(SegmentRing ring) {
        if (workerThread != null) {
            throw new IllegalStateException("test service requires a stopped manager");
        }
        RingEntry selected = null;
        synchronized (lock) {
            for (int i = 0, n = rings.size(); i < n; i++) {
                RingEntry candidate = rings.getQuick(i);
                if (candidate.ring == ring) {
                    selected = candidate;
                    break;
                }
            }
        }
        if (selected == null) {
            throw new IllegalArgumentException("ring is not registered");
        }
        return serviceRing(selected);
    }

    @TestOnly
    public void setAfterRingCleanupHook(Runnable hook) {
        this.afterRingCleanupHook = hook;
    }

    @TestOnly
    public void setBeforeExitCleanupRegistrationHook(Runnable hook) {
        this.beforeExitCleanupRegistrationHook = hook;
    }

    @TestOnly
    public void setBeforeRingQuiescenceAwaitHook(Runnable hook) {
        this.beforeRingQuiescenceAwaitHook = hook;
    }

    @TestOnly
    public void setBeforeTrimSyncHook(Runnable hook) {
        this.beforeTrimSyncHook = hook;
    }

    @TestOnly
    public void setRetryLogHook(Runnable hook) {
        this.retryLogHook = hook;
    }

    @TestOnly
    public void setWorkerJoinTimeoutMillis(long millis) {
        this.workerJoinTimeoutMillis = millis;
    }

    public synchronized void start() {
        if (workerThread != null) {
            throw new IllegalStateException("already started");
        }
        running = true;
        workerThread = new Thread(this::workerLoop, "qdb-sf-segment-manager");
        workerThread.setDaemon(true);
        workerThread.start();
    }

    /**
     * Unparks the worker thread out of its idle park so it processes
     * registered rings on the very next loop iteration. Cheap — a single
     * {@code LockSupport.unpark}; safe to call from any thread; idempotent
     * (multiple unparks coalesce into a single permit). No-op if the worker
     * hasn't been {@link #start()}'d yet.
     */
    public void wakeWorker() {
        Thread t = workerThread;
        if (t != null) {
            LockSupport.unpark(t);
        }
    }

    /**
     * Returns the highest hex-encoded generation across {@code sf-<gen>.sfa}
     * files in {@code dir}, or {@code -1} if none exist. Skips files that
     * don't match the pattern (e.g. the legacy {@code sf-initial.sfa}).
     */
    private long scanMaxGeneration(String dir) {
        long max = -1L;
        if (!filesFacade.exists(dir)) return max;
        long find = filesFacade.findFirst(dir);
        if (find < 0) {
            throw new SfOperationalException("could not enumerate SF segment directory " + dir);
        }
        if (find == 0) return max;
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(filesFacade.findName(find));
                rc = filesFacade.findNext(find);
                if (name == null || !name.startsWith("sf-") || !name.endsWith(".sfa")) {
                    continue;
                }
                String hex = name.substring(3, name.length() - 4);
                if (hex.length() != 16) continue;
                try {
                    long gen = Long.parseUnsignedLong(hex, 16);
                    if (gen > max) max = gen;
                } catch (NumberFormatException ignored) {
                    // sf-initial.sfa or non-hex — skip
                }
            }
            if (rc < 0) {
                throw new SfOperationalException("could not fully enumerate SF segment directory " + dir);
            }
        } finally {
            filesFacade.findClose(find);
        }
        return max;
    }

    private boolean serviceRing(RingEntry e) {
        // Publish before the CAS so deregister + await can always find the
        // entry. The state CAS is the ownership decision: if deregister won,
        // this stale snapshot pass skips the ring without touching it.
        inService = e;
        if (!ENTRY_STATE_UPDATER.compareAndSet(e, ENTRY_REGISTERED, ENTRY_IN_SERVICE)) {
            inService = null;
            return false;
        }
        boolean hasMoreTrimmable;
        try {
            hasMoreTrimmable = serviceRing0(e);
        } finally {
            e.finishService();
            Runnable cleanup = e.quiescenceCleanup == null
                    ? null
                    : ENTRY_CLEANUP_UPDATER.getAndSet(e, null);
            inService = null;
            // A normal pass performs only the two state CAS operations above.
            // The manager monitor is entered here only for an actual close
            // waiter; the recheck under lock prevents lost wakeups.
            if (e.quiescenceWaiters > 0) {
                synchronized (lock) {
                    if (e.quiescenceWaiters > 0) {
                        lock.notifyAll();
                    }
                }
            }
            if (cleanup != null) {
                try {
                    cleanup.run();
                } catch (Throwable t) {
                    LOG.error("deferred engine cleanup failed after manager-worker ring pass", t);
                } finally {
                    Runnable hook = afterRingCleanupHook;
                    if (hook != null) {
                        hook.run();
                    }
                }
            }
        }
        return hasMoreTrimmable;
    }

    private boolean serviceRing0(RingEntry e) {
        boolean memoryMode = e.dir == null;
        if (!memoryMode) {
            servicePeriodicSync(e, ticks.getAsLong());
        }
        return trimAcked(e, memoryMode);
    }

    private boolean trimAcked(RingEntry e, boolean memoryMode) {
        // Trim fully ACKed segments. The ring first transfers one bounded,
        //    unpinned prefix out of live traversal and into hidden pending
        //    ownership. Only then may this worker unmap it. Disk mode keeps the
        //    pending prefix until unlink + directory fsync are durable; memory
        //    mode commits each successfully freed prefix immediately. No
        //    syscall runs under the manager lock.
        Runnable hook = beforeTrimSyncHook;
        if (hook != null) {
            hook.run();
        }
        int pendingCount = e.ring.pendingTrimCount();
        MmapSegment first = pendingCount == 0 ? e.ring.firstTrimmable() : null;
        if (pendingCount == 0 && first == null) {
            return false;
        }

        if (memoryMode) {
            int batchSize = pendingCount > 0
                    ? e.ring.copyPendingTrims(trimBatch)
                    : e.ring.stagePendingTrims(
                            trimBatch, MAX_TRIMS_PER_RING_PASS, e.ring.ackedFsn());
            int closed = 0;
            Throwable closeFailure = null;
            while (closed < batchSize) {
                try {
                    trimBatch[closed].close();
                    closed++;
                } catch (Throwable t) {
                    closeFailure = t;
                    break;
                }
            }
            if (closed > 0) {
                synchronized (lock) {
                    long removedBytes = e.ring.commitPendingTrims(trimBatch, closed);
                    if (e.isRegistered()) {
                        budget.release(removedBytes);
                    }
                }
            }
            for (int i = 0; i < batchSize; i++) {
                trimBatch[i] = null;
            }
            if (closeFailure != null) {
                LOG.warn("Failed to trim memory segment", closeFailure);
                return false;
            }
            return e.ring.firstTrimmable() != null;
        }

        // A deferred disk retry does no sync, unlink, or logging work. Signed
        // subtraction is the standard wrap-safe deadline comparison because
        // the bounded delay is many orders of magnitude below half the long
        // range, even when the monotonic clock wraps.
        long now = ticks.getAsLong();
        if (e.trimRetryDelayNanos != 0 && now - e.trimRetryAtNanos < 0) {
            return false;
        }

        // Every attempt repeats the cheap covering barrier. The trim itself
        // is protected by the fsync'd manifest head, not by the watermark:
        // recovery skips files below the head and seeds at least headBase-1.
        // The sync only makes the watermark the I/O thread has written so far
        // (possibly one ACK behind durableAck, or torn by a racing write)
        // as durable as it can be, so recovery replays fewer duplicates, and
        // it preserves the same strict pre-unlink/post-unlink directory
        // ordering on pending retries.
        if (e.watermark == null) {
            if (!e.missingWatermarkLogged) {
                e.missingWatermarkLogged = true;
                LOG.warn("Cannot durably trim acknowledged segments in {} without an ack watermark", e.dir);
            }
            return false;
        }
        long durableAck;
        synchronized (lock) {
            if (!e.isRegistered()) {
                return false;
            }
            durableAck = e.ring.ackedFsn();
        }
        try {
            e.watermark.sync();
            if (filesFacade.fsyncDir(e.dir) != 0) {
                recordTrimFailure(e, TRIM_RETRY_PRE_BARRIER, now, null);
                return false;
            }
        } catch (Throwable t) {
            recordTrimFailure(e, TRIM_RETRY_PRE_BARRIER, now, t);
            return false;
        }

        int batchSize;
        if (pendingCount > 0) {
            batchSize = e.ring.copyPendingTrims(trimBatch);
        } else {
            try {
                // Under the ring monitor: advance the manifest past the last
                // eligible member, then atomically hide the batch. No I/O pin
                // can appear between the eligibility check and live removal.
                batchSize = e.ring.stagePendingTrims(
                        trimBatch, MAX_TRIMS_PER_RING_PASS, durableAck);
            } catch (Throwable t) {
                Arrays.fill(trimBatch, null);
                recordTrimFailure(e, TRIM_RETRY_UNLINK, now, t);
                return false;
            }
            if (batchSize == 0) {
                return false;
            }
        }

        boolean trimFailed = false;
        Throwable trimFailure = null;
        int unlinked = 0;
        while (unlinked < batchSize) {
            MmapSegment trimming = trimBatch[unlinked];
            String path = trimming.path();
            try {
                trimming.close();
                if (!filesFacade.remove(path) && filesFacade.exists(path)) {
                    trimFailed = true;
                    break;
                }
                unlinked++;
            } catch (Throwable t) {
                trimFailed = true;
                trimFailure = t;
                break;
            }
        }

        if (unlinked > 0) {
            try {
                if (filesFacade.fsyncDir(e.dir) != 0) {
                    for (int i = 0; i < batchSize; i++) {
                        trimBatch[i] = null;
                    }
                    recordTrimFailure(e, TRIM_RETRY_POST_BARRIER, now, null);
                    return false;
                }
            } catch (Throwable t) {
                for (int i = 0; i < batchSize; i++) {
                    trimBatch[i] = null;
                }
                recordTrimFailure(e, TRIM_RETRY_POST_BARRIER, now, t);
                return false;
            }
            try {
                synchronized (lock) {
                    long removedBytes = e.ring.commitPendingTrims(trimBatch, unlinked);
                    if (e.isRegistered()) {
                        budget.release(removedBytes);
                    }
                }
            } catch (Throwable t) {
                for (int i = 0; i < batchSize; i++) {
                    trimBatch[i] = null;
                }
                recordTrimFailure(e, TRIM_RETRY_POST_BARRIER, now, t);
                return false;
            }
        }
        for (int i = 0; i < batchSize; i++) {
            trimBatch[i] = null;
        }
        if (trimFailed) {
            recordTrimFailure(e, TRIM_RETRY_UNLINK, now, trimFailure);
            return false;
        }
        recordTrimSuccess(e);
        return e.ring.firstTrimmable() != null;
    }

    private void recordTrimFailure(RingEntry e, int failureKind, long now, Throwable failure) {
        long delay = e.trimRetryDelayNanos == 0
                ? TRIM_RETRY_INITIAL_NANOS
                : Math.min(e.trimRetryDelayNanos << 1, TRIM_RETRY_MAX_NANOS);
        e.trimRetryAtNanos = now + delay;
        e.trimRetryDelayNanos = delay;
        if (e.trimRetryFailureKind != failureKind) {
            e.trimRetryFailureKind = failureKind;
            if (failure == null) {
                LOG.warn("Durable segment trim failed in {} during {} (retry delayed)",
                        e.dir, trimFailureName(failureKind));
            } else {
                LOG.warn("Durable segment trim failed in {} during {} (retry delayed)",
                        e.dir, trimFailureName(failureKind), failure);
            }
            Runnable hook = retryLogHook;
            if (hook != null) {
                hook.run();
            }
        }
    }

    private void recordTrimSuccess(RingEntry e) {
        if (e.trimRetryDelayNanos != 0) {
            e.trimRetryAtNanos = 0;
            e.trimRetryDelayNanos = 0;
            e.trimRetryFailureKind = TRIM_RETRY_NONE;
            LOG.info("Durable segment trim recovered in {}", e.dir);
            Runnable hook = retryLogHook;
            if (hook != null) {
                hook.run();
            }
        }
    }

    private static String trimFailureName(int failureKind) {
        switch (failureKind) {
            case TRIM_RETRY_PRE_BARRIER:
                return "covering barrier";
            case TRIM_RETRY_UNLINK:
                return "segment unlink";
            default:
                return "directory commit barrier";
        }
    }

    private void servicePeriodicSync(RingEntry e, long now) {
        if (e.syncIntervalNanos <= 0L
                || (!e.ring.isSyncRequested() && now - e.nextDataSyncNanos < 0L)) {
            return;
        }
        try {
            e.ring.copyPendingSyncSegments(syncScratch);
            for (int i = 0, n = syncScratch.size(); i < n; i++) {
                syncScratch.getQuick(i).syncPublished();
            }
            e.ring.clearSyncRequestIfActiveDurable();
            // The pass above covered every not-yet-durable live range -- the
            // proven-durable sealed prefix is skipped precisely because its
            // syncPublished would early-return (see copyPendingSyncSegments),
            // so any latched failure necessarily belongs to a range we just
            // re-barriered. A failed barrier re-dirties its range under an
            // mlock pin (see MmapSegment.syncPublished), so a success here is a
            // genuine re-persist -- not a vacuous retry over pages a failed
            // writeback marked clean (fsyncgate). Unlatch so a transient disk
            // fault doesn't permanently brick the producer.
            e.ring.clearDurabilityFailure();
            e.nextDataSyncNanos = now + e.syncIntervalNanos;
            if (e.syncFailureLogged) {
                e.syncFailureLogged = false;
                LOG.info("Periodic SF data sync recovered for {}", e.dir);
            }
        } catch (Throwable failure) {
            e.ring.recordDurabilityFailure(failure);
            if (!e.syncFailureLogged) {
                e.syncFailureLogged = true;
                LOG.error("Periodic SF data sync failed for {}", e.dir, failure);
            }
            long retry = Math.min(e.syncIntervalNanos, 1_000_000_000L);
            e.nextDataSyncNanos = now + retry;
        } finally {
            syncScratch.clear();
        }
    }

    private void workerLoop() {
        try {
            while (running) {
                // Snapshot the rings under the lock so we don't hold it through the
                // (potentially slow) syscalls during unlink. ringSnapshot is a
                // thread-confined field — no per-tick allocation.
                ringSnapshot.clear();
                synchronized (lock) {
                    for (int i = 0, n = rings.size(); i < n; i++) {
                        ringSnapshot.add(rings.getQuick(i));
                    }
                }
                boolean hasMoreTrimmable = false;
                for (int i = 0, n = ringSnapshot.size(); i < n; i++) {
                    if (!running) break;
                    if (serviceRing(ringSnapshot.getQuick(i))) {
                        hasMoreTrimmable = true;
                    }
                }
                // Event-driven idle: every state change the worker services
                // wakes it (register/deregister, rotation, pin release, an ACK
                // that completes a sealed segment). Timed work: periodic sync
                // and a failed trim's retry. Otherwise park indefinitely.
                long parkNanos = Long.MAX_VALUE;
                if (!hasMoreTrimmable) {
                    long now = ticks.getAsLong();
                    for (int i = 0, n = ringSnapshot.size(); i < n; i++) {
                        RingEntry e = ringSnapshot.getQuick(i);
                        if (e.syncIntervalNanos > 0L) {
                            parkNanos = Math.min(parkNanos, Math.max(0L, e.nextDataSyncNanos - now));
                        }
                        if (e.trimRetryDelayNanos != 0L) {
                            parkNanos = Math.min(parkNanos, Math.max(0L, e.trimRetryAtNanos - now));
                        }
                    }
                }
                // Drop strong refs so a deregistered ring becomes collectable
                // before the next tick (otherwise the snapshot pins it until
                // the next wakeup).
                ringSnapshot.clear();
                if (!running) break;
                if (!hasMoreTrimmable) {
                    if (parkNanos == Long.MAX_VALUE) {
                        LockSupport.park(this);
                    } else if (parkNanos > 0L) {
                        LockSupport.parkNanos(this, parkNanos);
                    }
                }
            }
        } finally {
            ObjList<Runnable> cleanups;
            Runnable ownedEngineCleanup;
            synchronized (lock) {
                workerLoopExited = true;
                cleanups = exitCleanups;
                exitCleanups = null;
                ownedEngineCleanup = ownedEngineExitCleanup;
                ownedEngineExitCleanup = null;
            }
            // Deferred engine cleanups run OUTSIDE
            // `lock`: they perform syscalls (munmap, unlink, flock release)
            // and must never execute under a lock that close()/register/
            // deregister callers contend on. Running them after the loop body
            // is what makes the handoff safe: this thread can no longer touch
            // any slot path. They must also never block on a caller-held
            // monitor — a retried engine.close() joins this thread while
            // holding the engine monitor, which is why the engine side uses a
            // lock-free claim (CAS), not synchronization, for exactly-once.
            if (ownedEngineCleanup != null) {
                try {
                    ownedEngineCleanup.run();
                } catch (Throwable t) {
                    LOG.error("deferred owned-engine cleanup failed on manager-worker exit", t);
                }
            }
            if (cleanups != null) {
                for (int i = 0, n = cleanups.size(); i < n; i++) {
                    try {
                        cleanups.getQuick(i).run();
                    } catch (Throwable t) {
                        LOG.error("deferred engine cleanup failed on manager-worker exit", t);
                    }
                }
            }
        }
    }

    private static final class RingEntry {
        final String dir;
        final SegmentRing ring;
        // Live gauge of the slot's .symbol-dict side-file bytes, or null when
        // the slot has no dictionary (memory mode, degraded full-dict
        // sessions). Lent to the budget for its cap checks between register
        // and deregister -- never folded into its segments-only counter.
        final LongSupplier sideFileBytes;
        final long syncIntervalNanos;
        // Engine-owned ack watermark for this slot, or null in memory
        // mode and for callers that didn't supply one. The owner's I/O
        // thread writes it; the worker only syncs it before a trim and
        // never closes it.
        final AckWatermark watermark;
        // Prevents a legacy disk registration without a watermark from
        // flooding the log on every manager tick.
        boolean missingWatermarkLogged;
        long nextDataSyncNanos;
        boolean syncFailureLogged;
        // Zero-allocation manager-thread-only retry state. The deadline uses
        // the manager's monotonic clock and the delay doubles to a fixed cap.
        long trimRetryAtNanos;
        long trimRetryDelayNanos;
        int trimRetryFailureKind;
        // Updated lock-free by deferUntilRingQuiescent and pass completion.
        // The field updater avoids one AtomicReference allocation per ring.
        volatile Runnable quiescenceCleanup;
        // Waiters mutate this under SegmentManager.lock; pass completion reads
        // it before taking the otherwise-cold notification path.
        volatile int quiescenceWaiters;
        // REGISTERED -> IN_SERVICE -> REGISTERED on a normal pass.
        // Deregistration changes either registered state to its corresponding
        // deregistered state; completion then changes DEREGISTERED_IN_SERVICE
        // to DEREGISTERED. The field updater avoids per-entry allocation.
        volatile int state = ENTRY_REGISTERED;

        RingEntry(
                SegmentRing ring,
                String dir,
                AckWatermark watermark,
                LongSupplier sideFileBytes,
                long syncIntervalNanos,
                long now
        ) {
            this.ring = ring;
            this.dir = dir;
            this.watermark = watermark;
            this.sideFileBytes = sideFileBytes;
            this.syncIntervalNanos = syncIntervalNanos;
            // Run the first periodic pass immediately. This establishes a
            // durable baseline for segments recovered from MEMORY mode.
            this.nextDataSyncNanos = now;
        }

        void deregister() {
            while (true) {
                int current = state;
                int next = current == ENTRY_IN_SERVICE
                        ? ENTRY_DEREGISTERED_IN_SERVICE
                        : ENTRY_DEREGISTERED;
                if (current == ENTRY_DEREGISTERED || current == ENTRY_DEREGISTERED_IN_SERVICE
                        || ENTRY_STATE_UPDATER.compareAndSet(this, current, next)) {
                    return;
                }
            }
        }

        void finishService() {
            while (true) {
                int current = state;
                int next = current == ENTRY_DEREGISTERED_IN_SERVICE
                        ? ENTRY_DEREGISTERED
                        : ENTRY_REGISTERED;
                if (ENTRY_STATE_UPDATER.compareAndSet(this, current, next)) {
                    return;
                }
            }
        }

        boolean isInService() {
            int current = state;
            return current == ENTRY_IN_SERVICE || current == ENTRY_DEREGISTERED_IN_SERVICE;
        }

        boolean isRegistered() {
            int current = state;
            return current == ENTRY_REGISTERED || current == ENTRY_IN_SERVICE;
        }
    }
}
