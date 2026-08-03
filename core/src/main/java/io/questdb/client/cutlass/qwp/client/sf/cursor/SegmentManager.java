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
import io.questdb.client.std.Numbers;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.str.DirectUtf8Sink;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/**
 * Background worker that keeps every registered {@link SegmentRing} supplied
 * with a hot-spare segment and trims segments after their frames have been
 * ACK'd by the server. Off the user-thread / I/O-thread hot path entirely:
 * the expensive {@code openCleanRW + allocate + mmap} for spare creation and
 * {@code munmap + unlink} for trim happen on this thread, never on the
 * latency-sensitive paths.
 * <p>
 * One instance can serve many rings (typically all {@code Sender} instances
 * in a JVM). Polls each ring on a configurable tick (default 1 ms) — short
 * enough that a producer rarely sees {@link SegmentRing#BACKPRESSURE_NO_SPARE}
 * in the steady state, long enough that an idle JVM doesn't burn CPU.
 */
public final class SegmentManager implements QuietCloseable {

    public static final long DEFAULT_POLL_NANOS = 1_000_000L; // 1 ms
    public static final long DISK_FULL_LOG_THROTTLE_NANOS = 30_000_000_000L; // 30 s
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

    private final AtomicLong fileGeneration = new AtomicLong();
    private final FilesFacade filesFacade;
    private final Object lock = new Object();
    private final long maxTotalBytes;
    // Reused by the manager worker thread to build spare-segment paths
    // directly into native memory. Each rotation writes the path bytes plus
    // a trailing NUL terminator into the same buffer, and passes the
    // pointer to the long-ptr Files / MmapSegment overloads -- eliminating
    // the byte[] + native malloc pair that Files.pathPtr(String) would
    // otherwise allocate per call. Sized for typical SF directory paths;
    // grows on demand if a longer path is registered. Closed in close().
    private final DirectUtf8Sink pathScratch = new DirectUtf8Sink(256);
    private final long pollNanos;
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
    // Test seam: runs on the worker thread just before the install path's
    // synchronized(lock) entry (the one that performs installHotSpare + the
    // totalBytes += segmentSize commit). Null in production; tests use it to
    // pause after the worker has snapshotted a RingEntry and created a spare,
    // but before ownership/accounting commit. Callers may inject a deregister
    // or hold this stale worker snapshot while caller-side cleanup runs.
    private volatile Runnable beforeInstallSyncHook;
    // Test seam: records entry into the per-ring quiescence wait. Null in
    // production; owned-engine close tests use it to prove they take only the
    // stronger whole-manager join path, not two sequential timeout budgets.
    private volatile Runnable beforeRingQuiescenceAwaitHook;
    // Test seam: runs on the worker thread just before the trim block's
    // synchronized(lock) entry. Null in production; only
    // SegmentManagerTrimDeregisterRaceTest installs it, to deterministically
    // inject a deregister(ring) call into the exact race window that the
    // entry-state check inside the trim block closes for watermark writes and
    // totalBytes accounting.
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
    private long lastDiskFullLogNs;
    private volatile boolean running;
    // pathScratch free-exactly-once coordination between a timed-out close()
    // and the worker's exit path. All three are guarded by {@link #lock}.
    // When close() gives up on the join while the worker loop has not yet
    // exited, it hands scratch ownership to the worker
    // (scratchHandedToWorker=true) and the worker frees the buffer in its
    // exit block; in every other case close() frees it. Without the handoff,
    // a worker that outlives the bounded join leaks the native scratch
    // buffer forever, because nobody retries manager cleanup after close()
    // returns.
    private boolean scratchFreed;
    private boolean scratchHandedToWorker;
    private volatile long shortestSyncIntervalNanos = Long.MAX_VALUE;
    private boolean workerLoopExited;
    // Total bytes currently allocated across every segment owned by every
    // registered ring (active + sealed + hot-spare). Mutated by the manager
    // thread on provision/trim and by register/deregister callers under
    // {@link #lock}; the lock covers both paths so the counter stays
    // consistent across registration boundaries.
    private long totalBytes;
    // volatile: read by awaitRingQuiescence() from arbitrary caller threads
    // while the @TestOnly setter may run on another.
    private volatile long workerJoinTimeoutMillis = WORKER_JOIN_TIMEOUT_MILLIS;
    // volatile because wakeWorker() reads workerThread without holding the
    // monitor; the synchronized start()/close() pair handles the
    // start-vs-close ordering.
    private volatile Thread workerThread;

    public SegmentManager(long segmentSizeBytes) {
        this(segmentSizeBytes, DEFAULT_POLL_NANOS, UNLIMITED_TOTAL_BYTES, FilesFacade.INSTANCE, System::nanoTime);
    }

    public SegmentManager(long segmentSizeBytes, long pollNanos) {
        this(segmentSizeBytes, pollNanos, UNLIMITED_TOTAL_BYTES, FilesFacade.INSTANCE, System::nanoTime);
    }

    /**
     * Full constructor.
     *
     * @param segmentSizeBytes per-segment file size in bytes
     * @param pollNanos        how often the worker polls each registered ring;
     *                         default {@link #DEFAULT_POLL_NANOS}
     * @param maxTotalBytes    upper bound on total bytes the manager tracks
     *                         across all registered rings — counts every segment
     *                         the ring owns (initial active + sealed + hot
     *                         spare), including bytes already on disk at
     *                         register-time (e.g. after recovery or orphan
     *                         adoption). When provisioning a hot spare would
     *                         exceed this, the manager skips the install — the
     *                         requesting ring stays in the
     *                         {@link SegmentRing#BACKPRESSURE_NO_SPARE} state
     *                         until ACK-driven trim frees space. Pass
     *                         {@link #UNLIMITED_TOTAL_BYTES} to disable. Must be
     *                         at least one {@code segmentSizeBytes}; a sensible
     *                         lower bound for a single ring is
     *                         {@code 2 × segmentSizeBytes} so the manager can
     *                         hold an initial active plus one hot spare.
     */
    public SegmentManager(long segmentSizeBytes, long pollNanos, long maxTotalBytes) {
        this(segmentSizeBytes, pollNanos, maxTotalBytes, FilesFacade.INSTANCE, System::nanoTime);
    }

    @TestOnly
    public SegmentManager(long segmentSizeBytes, long pollNanos, long maxTotalBytes, FilesFacade filesFacade) {
        this(segmentSizeBytes, pollNanos, maxTotalBytes, filesFacade, System::nanoTime);
    }

    @TestOnly
    public SegmentManager(
            long segmentSizeBytes,
            long pollNanos,
            long maxTotalBytes,
            FilesFacade filesFacade,
            LongSupplier ticks
    ) {
        // The pathScratch field initializer has already allocated its native
        // buffer by the time this body runs, so a validation throw must free
        // it or every failed construction leaks 256 bytes of native memory
        // (e.g. a drainer retry loop hitting the same bad config).
        if (segmentSizeBytes < MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1) {
            pathScratch.close();
            throw new IllegalArgumentException("segmentSizeBytes too small: " + segmentSizeBytes);
        }
        if (maxTotalBytes < segmentSizeBytes) {
            pathScratch.close();
            throw new IllegalArgumentException(
                    "maxTotalBytes (" + maxTotalBytes + ") must allow at least one segment of "
                            + segmentSizeBytes + " bytes");
        }
        this.filesFacade = filesFacade;
        this.segmentSizeBytes = segmentSizeBytes;
        this.pollNanos = pollNanos;
        this.maxTotalBytes = maxTotalBytes;
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
                        // Hand pathScratch ownership to the worker: its exit
                        // block frees the buffer under the same lock, so the
                        // native allocation is reclaimed even though this
                        // close() could not confirm termination. workerThread
                        // stays set so isWorkerReaped() reports the incomplete
                        // shutdown and a later close() can retry the join.
                        scratchHandedToWorker = true;
                        LOG.warn("SegmentManager worker did not stop before close wait completed; "
                                + "worker frees its native scratch buffer on exit");
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
        // Free the rotation-path native scratch buffer only after worker
        // termination (or worker-loop exit) has been observed. The worker is
        // the only thread that touches the buffer; the scratchFreed flag
        // (shared with the worker's exit block) makes the free exactly-once
        // no matter which side runs last.
        synchronized (lock) {
            if (!scratchFreed) {
                scratchFreed = true;
                pathScratch.close();
            }
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
     * Stop tracking {@code ring}. Pending spares for the ring are NOT
     * created after this returns, but already-installed spares stay with
     * the ring (the ring closes them on its own {@link SegmentRing#close}).
     * Idempotent; safe to call from any thread.
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
                    // Reverse the ring's contribution to totalBytes —
                    // mirrors the seed in register(). Any spares the
                    // manager provisioned during the ring's lifetime
                    // are also part of totalSegmentBytes() now, so a
                    // single subtraction covers both the initial seed
                    // and the net manager activity (provisions minus
                    // trims) for this ring.
                    e.deregister();
                    totalBytes -= ring.totalSegmentBytes();
                    rings.remove(i);
                    return;
                }
            }
        }
    }

    /**
     * Register a ring for ongoing spare-creation + trim. {@code dir} is the
     * filesystem directory the ring's segments live in — used by the manager
     * both for creating spare files and unlinking trimmed ones. The ring
     * MUST already have its initial active segment in place.
     * <p>
     * Also wires the ring's "I need a spare" wakeup callback to
     * {@link #wakeWorker()}, so the producer thread can preempt the polling
     * tick the moment a rotation consumes the spare or the active crosses
     * the high-water mark — no waiting on the next tick.
     */
    public void register(SegmentRing ring, String dir) {
        register(ring, dir, null, 0L);
    }

    /**
     * Same as {@link #register(SegmentRing, String)} but also wires an
     * {@link AckWatermark} the manager will keep up to date on every
     * tick. The watermark is owned by the caller (typically
     * {@link CursorSendEngine}); the manager only writes through it
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
     * The gauge is invoked with the manager's internal lock held, so it must
     * be wait-free and must not throw: a throwing gauge terminates the
     * manager worker for every registered ring, and a blocking gauge stalls
     * register/deregister for all slots.
     */
    public void register(SegmentRing ring, String dir, AckWatermark watermark, long syncIntervalNanos, LongSupplier sideFileBytes) {
        if (syncIntervalNanos < 0L) {
            throw new IllegalArgumentException("syncIntervalNanos must not be negative");
        }
        if (syncIntervalNanos > 0L && dir == null) {
            throw new IllegalArgumentException("periodic sync requires a segment directory");
        }
        // Account for bytes the ring already owns when it joins. A recovered
        // ring (post-restart, orphan adoption) can come up at-or-above the cap;
        // without this seed, totalBytes stays at 0 and the per-tick cap check
        // at serviceRing would let the manager keep provisioning new spares on
        // top of the recovered set, effectively doubling the documented cap.
        long ringBytes = ring.totalSegmentBytes();
        // Skip the file-generation counter past whatever's already on disk in
        // this slot. Without this, on recovery the manager would mint a new
        // spare at sf-0000000000000000.sfa — and openCleanRW would truncate the
        // user's existing active file out from under the I/O loop, scrambling
        // the in-flight mmap. Memory-mode rings have no dir; nothing to scan.
        long minNextGeneration = dir == null ? -1L : scanMaxGeneration(dir) + 1L;
        Runnable managerWakeup = this::wakeWorker;
        RingEntry e = new RingEntry(ring, dir, watermark, sideFileBytes, syncIntervalNanos, ticks.getAsLong());
        // ObjList.add either throws before storing e or makes the entry visible.
        // Once visible, only non-throwing state commits may remain.
        synchronized (lock) {
            if (dir != null) {
                advanceFileGeneration(minNextGeneration);
            }
            rings.add(e);
            totalBytes += ringBytes;
            if (syncIntervalNanos > 0L) {
                ring.enablePeriodicSync();
                if (syncIntervalNanos < shortestSyncIntervalNanos) {
                    shortestSyncIntervalNanos = syncIntervalNanos;
                }
            }
        }
        ring.setManagerWakeup(managerWakeup);
        // Nudge the worker so it picks up the new ring on its very next
        // iteration. Without this, register-after-start has a race window:
        // start() schedules the worker thread, and if that thread reaches
        // workerLoop and takes `lock` before this method does, it observes
        // an empty `rings` snapshot, services nothing, then parkNanos
        // (potentially seconds). A new ring whose first append does not
        // cross the high-water mark fires no producer-side wakeup either,
        // leaving the ring without a spare for the full poll interval.
        // wakeWorker is cheap (a single LockSupport.unpark) and a no-op
        // when the worker has not been started yet.
        wakeWorker();
    }

    @TestOnly
    public SegmentRing getInServiceRingForTesting() {
        RingEntry entry = inService;
        return entry == null ? null : entry.ring;
    }

    // Callers must hold `lock` (the rings list is mutated under it). The
    // side-file bytes are read live from each slot's gauge instead of being
    // folded into the incremental totalBytes counter: the dictionary grows
    // out-of-band on producer threads, so an incremental mirror would
    // drift, while a live read cannot. Each gauge is WAIT-FREE and takes no
    // lock at all -- PersistedSymbolDict.appendedBytes() is a plain volatile
    // read -- and it must stay that way. This runs with `lock` held, on the
    // worker that drives provisioning and trim for every registered ring,
    // while a producer can hold that dictionary's monitor across ff.allocate
    // and mmap. A gauge that took the monitor would park the whole manager
    // behind one producer's append I/O.
    private long sideFileBytesLocked() {
        long total = 0L;
        for (int i = 0, n = rings.size(); i < n; i++) {
            LongSupplier gauge = rings.get(i).sideFileBytes;
            if (gauge != null) {
                total += gauge.getAsLong();
            }
        }
        return total;
    }

    @TestOnly
    public long getCapAccountedBytesForTesting() {
        synchronized (lock) {
            return totalBytes + sideFileBytesLocked();
        }
    }

    @TestOnly
    public long getTotalBytesForTesting() {
        synchronized (lock) {
            return totalBytes;
        }
    }

    @TestOnly
    public Thread getWorkerThreadForTesting() {
        return workerThread;
    }

    @TestOnly
    public boolean isPathScratchAllocatedForTesting() {
        synchronized (lock) {
            return !scratchFreed;
        }
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
    public void setBeforeInstallSyncHook(Runnable hook) {
        this.beforeInstallSyncHook = hook;
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
     * Unparks the worker thread out of its poll-park so it processes
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

    private void advanceFileGeneration(long minNext) {
        while (true) {
            long cur = fileGeneration.get();
            if (cur >= minNext) break;
            if (fileGeneration.compareAndSet(cur, minNext)) break;
        }
    }

    /**
     * Spare files are named with a JVM-wide monotonic generation counter
     * rather than a baseSeq-derived name, because the spare's baseSeq is
     * provisional at create time (SegmentRing.appendOrFsn rebases it at
     * rotation). Pattern: {@code <dir>/sf-<gen:016x>.sfa}. Recovery
     * discovers segments by extension + header magic, not by filename.
     * <p>
     * Builds the path bytes directly into {@link #pathScratch} and writes
     * a trailing NUL so the same buffer can be handed to the long-ptr
     * {@link FilesFacade} overloads (no per-rotation byte[] + native
     * malloc). The String returned is the same path without the NUL --
     * captured before terminating so it is suitable for {@link MmapSegment#path()}
     * and exception messages.
     */
    private String nextSparePath(String dir) {
        pathScratch.clear();
        pathScratch.putAscii(dir).putAscii("/sf-");
        Numbers.appendHex(pathScratch, fileGeneration.getAndIncrement(), true);
        pathScratch.putAscii(".sfa");
        String displayPath = pathScratch.toString();
        // Trailing NUL must be appended AFTER toString() captures the path
        // text -- DirectUtf8Sink.toString reads the full sink contents and
        // would otherwise include the terminator in the result. Use putAny
        // so the assertion in DirectUtf8Sink.put(byte) does not trip on a
        // non-negative byte.
        pathScratch.putAny((byte) 0);
        return displayPath;
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

        // 1. Provision a hot spare if the ring needs one AND we have headroom
        //    under the disk-total cap. Cap check is per-tick; if we're capped
        //    here, the ring stays in BACKPRESSURE_NO_SPARE until trim (step 2)
        //    on this or a subsequent tick frees space. Logged at most once per
        //    DISK_FULL_LOG_THROTTLE_NANOS so a sustained-disk-full state
        //    doesn't drown the log.
        if (e.ring.needsHotSpare()) {
            // Snapshot totalBytes under lock -- register/deregister can mutate
            // it from caller threads -- and add the live side-file bytes of
            // every registered slot, so .symbol-dict growth counts against the
            // cap. Heavy provisioning I/O happens outside the lock; the
            // post-install commit re-acquires it.
            long observedTotal;
            long observedSideFileBytes;
            synchronized (lock) {
                observedSideFileBytes = sideFileBytesLocked();
                observedTotal = totalBytes + observedSideFileBytes;
            }
            if (observedTotal + segmentSizeBytes > maxTotalBytes) {
                long now = System.nanoTime();
                if (now - lastDiskFullLogNs >= DISK_FULL_LOG_THROTTLE_NANOS) {
                    LOG.warn("SF {}: cannot provision spare in {} "
                                    + "(totalBytes={}, sideFileBytes={}, cap={}, segmentSize={}). "
                                    + "Producer is backpressured until ACK-driven trim frees segment "
                                    + "space; side-file bytes are not reclaimed by trim.",
                            memoryMode ? "memory cap reached" : "disk-full",
                            memoryMode ? "<memory>" : e.dir, observedTotal, observedSideFileBytes,
                            maxTotalBytes, segmentSizeBytes);
                    lastDiskFullLogNs = now;
                }
            } else {
                MmapSegment spare = null;
                String path = null;
                boolean installed = false;
                try {
                    // Null if the ring closed between this tick's snapshot and now:
                    // close() nulls it under the ring's own monitor with no
                    // coordination with the manager thread. A closed ring rejects
                    // installHotSpare regardless, so there is nothing to provision
                    // this tick; skip cleanly rather than provision a spare that will
                    // be abandoned.
                    MmapSegment active = e.ring.getActive();
                    if (active != null) {
                        // baseSeq is provisional -- SegmentRing.appendOrFsn calls
                        // rebaseSeq() at rotation time to pin the real value. We
                        // pass the manager's best guess (nextSeqHint at this
                        // instant), which is fine since it's overwritten anyway.
                        if (memoryMode) {
                            spare = MmapSegment.createInMemory(e.ring.nextSeqHint(), segmentSizeBytes);
                        } else {
                            path = nextSparePath(e.dir);
                            // Native path bytes (NUL-terminated) live in pathScratch
                            // from the call above. Hand them straight to MmapSegment.create
                            // via its long-ptr overload, bypassing the byte[] + native
                            // malloc that the String overload would incur on every
                            // rotation.
                            spare = MmapSegment.create(filesFacade,
                                    pathScratch.ptr(), path,
                                    e.ring.nextSeqHint(), segmentSizeBytes, true);
                        }
                        Runnable installHook = beforeInstallSyncHook;
                        if (installHook != null) {
                            installHook.run();
                        }
                        if (!memoryMode) {
                            spare.syncHeader();
                            if (filesFacade.fsyncDir(e.dir) != 0) {
                                throw new MmapSegmentException(
                                        "could not sync hot-spare directory " + e.dir);
                            }
                        }
                        // Install + commit atomically under the manager lock.
                        // If `e.ring` was deregistered between the snapshot
                        // above and now, abandoning the spare here is the only
                        // way to keep totalBytes consistent: deregister already
                        // subtracted ring.totalSegmentBytes() (without the
                        // spare, since it wasn't installed yet) so a commit at
                        // this point would inflate totalBytes by one segment
                        // with no future subtractor. By holding `lock` across
                        // installHotSpare AND the += commit AND the registration
                        // check, deregister is forced to either
                        // observe the spare in the ring (and subtract it) or
                        // run before installation (so no install happens).
                        synchronized (lock) {
                            if (e.isRegistered()) {
                                e.ring.installHotSpare(spare);
                                totalBytes += segmentSizeBytes;
                                installed = true;
                            }
                        }
                    }
                } catch (Throwable t) {
                    LOG.warn("Failed to provision hot spare in {} (will retry next tick)",
                            memoryMode ? "<memory>" : e.dir, t);
                }
                if (!installed) {
                    if (spare != null) {
                        try {
                            spare.close();
                        } catch (Throwable ignored) {
                        }
                    }
                    // Only remove the file when the spare object exists, i.e.
                    // MmapSegment.create succeeded and ownership is ours but
                    // installation was rejected (ring deregistered/closed).
                    // When create() itself threw, its catch already removed
                    // anything it put on disk -- and with exclusive create
                    // (O_EXCL) a failure can also mean the path was ALREADY
                    // occupied by a file some other lifecycle owns, which a
                    // blanket unlink here would destroy.
                    if (path != null && spare != null) {
                        filesFacade.remove(path);
                    }
                }
            }
        }

        // 2. Trim fully ACKed segments. The ring first transfers one bounded,
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
            // Preserve the cheap mmap-only watermark cadence for ACKs that do
            // not yet cover a complete, unpinned sealed segment.
            synchronized (lock) {
                if (e.isRegistered() && e.watermark != null) {
                    long currentAck = e.ring.ackedFsn();
                    if (currentAck > e.lastPersistedAck) {
                        e.watermark.write(currentAck);
                        e.lastPersistedAck = currentAck;
                    }
                }
            }
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
                        totalBytes -= removedBytes;
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

        // Every attempt repeats the cheap covering barrier. Besides keeping
        // the latest cumulative ACK durable, this preserves the same strict
        // pre-unlink/post-unlink directory ordering on pending retries.
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
            if (durableAck > e.lastPersistedAck) {
                e.watermark.write(durableAck);
                e.lastPersistedAck = durableAck;
            }
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
                        totalBytes -= removedBytes;
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
                // (potentially slow) syscalls during creation/unlink. ringSnapshot
                // is a thread-confined field — no per-tick allocation.
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
                // Drop strong refs so a deregistered ring becomes collectable
                // before the next tick (otherwise the snapshot pins it for up
                // to pollNanos after deregister).
                ringSnapshot.clear();
                if (!running) break;
                if (!hasMoreTrimmable) {
                    LockSupport.parkNanos(Math.min(pollNanos, shortestSyncIntervalNanos));
                }
            }
        } finally {
            // If a timed-out close() abandoned the reap, it handed
            // pathScratch ownership to this thread (see close()). Freeing it
            // here reclaims the native buffer even when the worker outlives
            // every close() attempt — nobody else retries manager cleanup.
            ObjList<Runnable> cleanups;
            Runnable ownedEngineCleanup;
            synchronized (lock) {
                workerLoopExited = true;
                if (scratchHandedToWorker && !scratchFreed) {
                    scratchFreed = true;
                    pathScratch.close();
                }
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
        // sessions). Read at the provisioning cap check only -- never folded
        // into totalBytes, which stays a segments-only counter.
        final LongSupplier sideFileBytes;
        final long syncIntervalNanos;
        // Engine-owned ack watermark for this slot, or null in memory
        // mode and for callers that didn't supply one. Manager-thread
        // only after register; never closed here (owner closes).
        final AckWatermark watermark;
        // Highest FSN this entry has written to its watermark. Manager-
        // thread only -- no concurrent access. Initialized to -1 so the
        // first observed acked FSN (>= 0) triggers the first write.
        // Survives across multiple serviceRing ticks and avoids a
        // write-storm when ackedFsn is steady.
        long lastPersistedAck = -1L;
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
