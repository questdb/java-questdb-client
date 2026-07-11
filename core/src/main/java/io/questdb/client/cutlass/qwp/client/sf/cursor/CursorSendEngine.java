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

import io.questdb.client.std.Compat;
import io.questdb.client.std.Files;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongConsumer;

/**
 * Facade that bundles a {@link SegmentRing} with a {@link SegmentManager} and
 * exposes the user-facing API the wire-send loop calls into. Keeps SF append
 * work on the user thread (where it belongs) and segment lifecycle work on
 * the manager thread (where it belongs).
 * <p>
 * <b>Responsibilities:</b>
 * <ul>
 *   <li>Owning the ring + manager lifecycle (open / close / startup recovery).</li>
 *   <li>Providing a user-thread append path that handles backpressure
 *       (spin briefly, then return — caller decides whether to retry).</li>
 *   <li>Exposing read accessors for the I/O thread: {@link #publishedFsn},
 *       {@link #activeSegment}, {@link #sealedSegments}.</li>
 *   <li>Routing server ACKs to the ring for trim.</li>
 * </ul>
 * <b>Not in scope:</b>
 * <ul>
 *   <li>Multi-producer support. Single producer (one user thread) only.</li>
 * </ul>
 */
public final class CursorSendEngine implements QuietCloseable {

    /**
     * Throttle the "producer is backpressured" WARN log to at most once per this interval.
     */
    public static final long BACKPRESSURE_LOG_THROTTLE_NANOS = 5_000_000_000L; // 5 s
    /**
     * Default deadline for {@link #appendBlocking}: 30 seconds.
     */
    public static final long DEFAULT_APPEND_DEADLINE_NANOS = 30_000_000_000L;
    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(CursorSendEngine.class);
    private static final ThreadFactory DEFAULT_FLOCK_RELEASE_RETRY_THREAD_FACTORY =
            runnable -> new Thread(runnable, "qdb-sf-flock-release-retry");
    private static final long FLOCK_RELEASE_RETRY_BASE_PARK_NANOS = 100_000_000L; // 100 ms
    private static final Object FLOCK_RELEASE_RETRY_LOCK = new Object();
    private static final long FLOCK_RELEASE_RETRY_MAX_PARK_NANOS = 5_000_000_000L; // 5 s
    private static final ArrayDeque<CursorSendEngine> FLOCK_RELEASE_RETRY_QUEUE = new ArrayDeque<>();
    private static volatile Runnable afterFlockReleaseRetryFailureHook;
    private static volatile Runnable beforeDeferredCloseCreationHook;
    private static volatile LongConsumer flockReleaseRetryParkOverride;
    private static Thread flockReleaseRetryThread;
    private static volatile ThreadFactory flockReleaseRetryThreadFactory =
            DEFAULT_FLOCK_RELEASE_RETRY_THREAD_FACTORY;
    private final long appendDeadlineNanos;
    // Number of times appendBlocking observed BACKPRESSURE_NO_SPARE on its first
    // ring.appendOrFsn attempt. One increment per blocking-call that had to wait
    // for the manager (or for ACKs) — not one per spin-park. Producer-thread
    // writer; volatile because the user may sample it from any thread.
    private final java.util.concurrent.atomic.AtomicLong backpressureStallCount =
            new java.util.concurrent.atomic.AtomicLong();
    // Constructed before an owned manager acquires its native path scratch, so
    // callback allocation failure cannot orphan manager resources. A timed-out
    // close can then hand it to either manager path without allocating.
    private final Runnable deferredClose;
    private final SegmentManager manager;
    // We own the manager iff the user constructed us with no manager — in that
    // case close() also stops the manager. When the manager is shared across
    // many engines (one per Sender), the caller owns and closes it.
    private final boolean ownsManager;
    private final SegmentRing ring;
    private final long segmentSizeBytes;
    private final String sfDir;
    // Held for the engine's lifetime in disk mode. {@code null} in memory
    // mode (no slot, no lock). Released by {@link #close()}; the kernel
    // also drops it on hard process exit.
    private final SlotLock slotLock;
    // True when the constructor recovered an existing on-disk slot rather
    // than starting fresh. Diagnostic accessor for tests and observability;
    // cursor frames are self-sufficient (every frame carries full schema +
    // full symbol-dict delta), so producer-side schema reset on recovery
    // is not required.
    private final boolean wasRecoveredFromDisk;
    // FSN of the last commit-bearing (non-FLAG_DEFER_COMMIT) frame found in a
    // ring recovered from disk, or -1 for fresh/memory rings and recovered
    // rings whose every frame is deferred. Frames above this FSN in the
    // recovered ring belong to a transaction whose commit frame was never
    // published; the server will never ack them until some later commit
    // covers them. Read by the sender's close-time drain to avoid waiting on
    // acks that cannot arrive.
    private long recoveredCommitBoundaryFsn = -1L;
    // FSN of the last frame of a recovered orphaned deferred tail, or -1 when
    // the recovered ring has no such tail. When >= 0, frames
    // [recoveredCommitBoundaryFsn + 1 .. recoveredOrphanTipFsn] all carry
    // FLAG_DEFER_COMMIT with no covering commit frame -- an aborted
    // transaction. The send loop must never transmit them; it retires the
    // range with a cumulative self-acknowledge once everything below is
    // server-acked (CursorWebSocketSendLoop.tryRetireOrphanTail).
    private long recoveredOrphanTipFsn = -1L;
    // Engine-owned mmap'd watermark file. {@code null} in memory mode and
    // in disk mode if open() failed (we proceed without it; recovery just
    // falls back to lowestBase - 1). Lifetime tied to the engine: opened
    // in the constructor, closed by {@link #close()}. The segment manager
    // writes through this on every tick where ackedFsn has advanced.
    private final AckWatermark watermark;
    // close() is publicly callable from any thread (Sender.close from a user
    // thread, JVM shutdown hooks, test cleanup). volatile + synchronized
    // close() makes the check-and-set atomic and gives readers a fence.
    private volatile boolean closed;
    // True once close() has run its full cleanup sequence INCLUDING a
    // CONFIRMED slot-flock release — finishClose() publishes this strictly
    // after SlotLock.release() reports success, never before. Pool threads
    // treat the flip as "the slot dir is reusable" and free the slot index
    // the moment they observe it (QwpWebSocketSender.isSlotLockReleased ->
    // SenderPool.reprobeRetiredSlots), so publishing before the release
    // would let a replacement engine's SlotLock.acquire collide with the
    // still-open fd. Stays false when a close attempt could not confirm
    // manager-worker quiescence (or the flock release itself failed) and had
    // to leak the ring/watermark/slot lock — in that case a later close()
    // call retries the cleanup (the worker may have exited by then).
    // volatile: latched by finishClose(), but read lock-free by
    // isCloseCompleted() from pool threads re-probing a retired slot (see the
    // getter for why it must not synchronize).
    private volatile boolean closeCompleted;
    // Invoked after closeCompleted publishes a confirmed flock release. Used
    // by an owning sender pool to wake capacity-starved borrowers immediately.
    private volatile Runnable slotLockReleaseListener;
    // Test-only hook run by finishClose() between the terminal cleanup and
    // the flock release. Lets a test park the releasing thread inside the
    // cleanup/release window and assert that closeCompleted stays false —
    // i.e. that completion is never observable while the flock is still
    // held. volatile: finishClose may run on the manager worker's exit
    // thread while the hook is installed from a test thread.
    private volatile Runnable beforeFlockReleaseHook;
    // Ensures this engine has at most one entry in the shared flock-release
    // retry driver. The error path only: ordinary closes never enqueue work.
    private final AtomicBoolean flockReleaseRetryStarted = new AtomicBoolean();
    // Published before deferredClose is registered. The manager lock provides
    // the callback handoff fence; volatile also covers a direct test/retry read.
    private volatile boolean fullyDrainedForDeferredClose;
    // Exactly-once claim on the terminal cleanup (finishClose). Contended by
    // close() and a worker-exit handoff (completeDeferredClose); whoever wins
    // the CAS runs the cleanup, the loser never touches ring/watermark/flock.
    // Deliberately NOT the engine monitor: a retried close() holds the
    // monitor while joining the manager worker, and the worker cannot die
    // until its exit-path cleanup finishes — monitor-based exclusion would
    // stall that close() for its full join budget (test-visible livelock).
    // With the CAS the worker's cleanup never blocks, so the join returns as
    // soon as the pass ends.
    private final AtomicBoolean terminalCleanupClaimed = new AtomicBoolean();
    // Published only after ring/watermark/unlink cleanup is finished. A close
    // that loses terminalCleanupClaimed may retry the flock only after this
    // becomes true, otherwise it could expose the slot while cleanup is live.
    private volatile boolean terminalResourcesCleaned;
    // Producer-thread-only: timestamp of the last "we're backpressured" log
    // line, used to throttle. Plain long is fine.
    private long lastBackpressureLogNs;

    /**
     * Creates an engine with a private, non-shared {@link SegmentManager},
     * unbounded total bytes (use only for tests / single-segment scenarios),
     * and the default append deadline.
     */
    public CursorSendEngine(String sfDir, long segmentSizeBytes) {
        this(sfDir, segmentSizeBytes, SegmentManager.UNLIMITED_TOTAL_BYTES,
                DEFAULT_APPEND_DEADLINE_NANOS);
    }

    /**
     * Creates an engine with a private, non-shared {@link SegmentManager}
     * capped at {@code maxTotalBytes} of cursor-allocated memory/disk
     * (active + spare + sealed). Producer's {@link #appendBlocking} blocks
     * up to {@code appendDeadlineNanos} when the cap is full and ACKs
     * haven't drained sealed segments; on deadline expiry it throws.
     */
    public CursorSendEngine(String sfDir, long segmentSizeBytes,
                            long maxTotalBytes, long appendDeadlineNanos) {
        this(sfDir, segmentSizeBytes, null, true, maxTotalBytes, appendDeadlineNanos);
    }

    /**
     * Creates an engine that shares the given {@link SegmentManager} (which
     * must already be {@link SegmentManager#start()}'d). The caller retains
     * ownership of the manager. Uses the default append deadline.
     */
    public CursorSendEngine(String sfDir, long segmentSizeBytes, SegmentManager manager) {
        this(sfDir, segmentSizeBytes, manager, false, DEFAULT_APPEND_DEADLINE_NANOS);
    }

    private CursorSendEngine(String sfDir, long segmentSizeBytes, SegmentManager manager,
                             boolean ownsManager, long appendDeadlineNanos) {
        this(sfDir, segmentSizeBytes, manager, ownsManager,
                SegmentManager.UNLIMITED_TOTAL_BYTES, appendDeadlineNanos);
    }

    private CursorSendEngine(String sfDir, long segmentSizeBytes, SegmentManager manager,
                             boolean ownsManager, long maxTotalBytes, long appendDeadlineNanos) {
        // Allocate the bound callback before constructing an owned manager.
        // Field initializers have completed, but no engine-owned native/disk
        // resource exists yet. If callback allocation throws, construction
        // stops without a manager whose native path scratch could be orphaned.
        this.deferredClose = createDeferredClose();
        if (ownsManager && manager == null) {
            manager = new SegmentManager(
                    segmentSizeBytes, SegmentManager.DEFAULT_POLL_NANOS, maxTotalBytes);
        }

        // sfDir == null  → memory-only mode (non-SF async ingest). Same
        //                  cursor architecture, no disk involvement; segments
        //                  live in malloc'd native memory.
        // sfDir != null  → store-and-forward mode. Segments are mmap'd files
        //                  under sfDir, recoverable across sender restarts.
        boolean memoryMode = sfDir == null;
        SlotLock acquiredLock = null;
        if (!memoryMode) {
            try {
                if (sfDir.isEmpty()) {
                    throw new IllegalArgumentException("sfDir must not be empty");
                }
                // Acquire the slot lock BEFORE we touch any *.sfa files. Two
                // engines pointed at the same slot would otherwise race on
                // recovery and create overlapping FSN ranges. SlotLock.acquire
                // also creates the slot dir if it doesn't exist yet — no
                // separate mkdir step needed here.
                acquiredLock = SlotLock.acquire(sfDir);
            } catch (Throwable t) {
                // Callback creation and owned-manager construction have already
                // completed. A slot-lock failure must close the owned manager's
                // native path scratch before propagating.
                if (ownsManager) {
                    try {
                        manager.close();
                    } catch (Throwable ignored) {
                    }
                }
                throw t;
            }
        }
        this.slotLock = acquiredLock;
        this.sfDir = sfDir;
        this.segmentSizeBytes = segmentSizeBytes;
        this.manager = manager;
        this.ownsManager = ownsManager;
        this.appendDeadlineNanos = appendDeadlineNanos;

        // Track the ring locally until every step succeeds — only commit it
        // to this.ring at the very end. If anything between ring allocation
        // and manager.register throws, the catch block closes the local
        // reference instead of orphaning the mmap'd segments + fds.
        SegmentRing ringInProgress = null;
        AckWatermark watermarkInProgress = null;
        try {
            // Disk mode: try to recover any *.sfa files left behind by a prior
            // session before deciding to start fresh. Without this the engine
            // would create a new sf-initial.sfa at baseSeq=0, overlapping FSNs
            // already on disk and corrupting ACK translation, trim, and replay.
            SegmentRing recovered = memoryMode ? null
                    : SegmentRing.openExisting(sfDir, segmentSizeBytes);
            this.wasRecoveredFromDisk = recovered != null;
            if (recovered != null) {
                ringInProgress = recovered;
                // Seed ackedFsn to one below the lowest segment's baseSeq.
                // We don't know what was actually acked before the prior
                // session crashed, but anything trimmed off the ring's
                // bottom must have been acked (trim is ack-driven). Without
                // this seed, ackedFsn stays at -1 and the I/O loop's
                // start-time positioning would walk to FSN 0 — which may
                // not exist on disk if earlier segments have been trimmed,
                // causing it to fall through to the active segment's tip
                // and skip the unacked sealed segments entirely.
                //
                // Then check the persisted ack watermark. If present and
                // larger than the segment-derived seed, prefer it: it
                // captures durable-acks that landed inside the lowest
                // surviving sealed segment before the previous sender
                // crashed. Without this, those already-acked frames would
                // be re-replayed on reconnect, producing row-level
                // duplicates unless the target table dedupes them.
                // max(lowestBase - 1, watermark) absorbs both write
                // orderings of the manager's "persist then trim" tick:
                //   - persist crashed before trim: segments still on disk
                //     are >= lowest, watermark is correct; max picks
                //     watermark.
                //   - trim ran before persist: segments are gone (so
                //     lowestBase is higher than watermark), watermark is
                //     stale; max picks lowestBase - 1.
                MmapSegment first = recovered.firstSealed();
                long lowestBase = first != null
                        ? first.baseSeq()
                        : recovered.getActive().baseSeq();
                // Open the watermark and use it (if present) to refine
                // the seed. The watermark may carry durable-acks the
                // previous sender received for frames inside the lowest
                // surviving sealed segment -- without it, those frames
                // get re-replayed across process restart, producing
                // row-level duplicates unless the target table dedupes
                // them. max(lowestBase - 1, watermark) absorbs both
                // write orderings of the manager's "persist then trim"
                // tick:
                //   - persist crashed before trim: segments still on
                //     disk are >= lowest, watermark is correct; max
                //     picks watermark.
                //   - trim ran before persist: segments are gone (so
                //     lowestBase is higher than watermark), watermark
                //     is stale; max picks lowestBase - 1.
                // open() returns null on any setup failure so a missing
                // mmap doesn't take down the engine -- we just fall
                // back to the bare lowestBase - 1 seed.
                watermarkInProgress = AckWatermark.open(sfDir);
                long baseSeed = lowestBase - 1;
                long watermarkFsn = watermarkInProgress != null
                        ? watermarkInProgress.read()
                        : AckWatermark.INVALID;
                // Reject watermarks past publishedFsn: a correctly
                // operating prior session cannot have produced one, so
                // a value above the on-disk frame ceiling is corruption
                // (torn write on a non-atomic filesystem, hardware
                // fault, manual edit). Trusting it would seed ackedFsn
                // = publishedFsn after ring.acknowledge clamps it, and
                // the cursor would position past every un-acked frame
                // -- silent data loss. Fall back to the segment-derived
                // seed so the un-acked tail still replays.
                long publishedFsn = recovered.publishedFsn();
                long candidate = Math.max(watermarkFsn, baseSeed);
                long seed = candidate > publishedFsn ? baseSeed : candidate;
                if (seed >= 0) {
                    recovered.acknowledge(seed);
                }
                // Locate the last commit-bearing frame below a potentially
                // orphaned FLAG_DEFER_COMMIT tail. A producer that crashed (or
                // closed) mid-transaction leaves deferred frames with no
                // covering commit frame at the top of the ring. The server
                // never acks uncommitted deferred frames, so (a) close-time
                // drains must not wait for them (see the sender's
                // drainOnClose), and (b) replaying them into a NEW session's
                // commit would resurrect half a transaction -- see the WARN
                // below. Computed before the I/O loop or producer append.
                this.recoveredCommitBoundaryFsn = recovered.findLastFsnWithoutPayloadFlag(
                        io.questdb.client.cutlass.qwp.protocol.QwpConstants.HEADER_OFFSET_FLAGS,
                        io.questdb.client.cutlass.qwp.protocol.QwpConstants.FLAG_DEFER_COMMIT,
                        io.questdb.client.cutlass.qwp.protocol.QwpConstants.MAGIC_MESSAGE,
                        io.questdb.client.cutlass.qwp.protocol.QwpConstants.HEADER_SIZE
                );
                if (publishedFsn >= 0 && recoveredCommitBoundaryFsn < publishedFsn) {
                    this.recoveredOrphanTipFsn = publishedFsn;
                    LOG.warn("recovered SF log ends with {} deferred frame(s) whose transaction was never "
                                    + "committed [commitBoundaryFsn={}, publishedFsn={}]. The tail belongs to an "
                                    + "aborted transaction: it will never be transmitted and its slots are "
                                    + "retired (trimmed) once every frame below it is server-acked.",
                            publishedFsn - Math.max(recoveredCommitBoundaryFsn, -1L),
                            recoveredCommitBoundaryFsn, publishedFsn);
                }
            } else {
                // Fresh start with no recovered segments. Any stale
                // watermark from a prior fully-drained session refers
                // to a lifecycle now gone -- unlink it before opening
                // so the new session's first read() correctly reports
                // INVALID (magic=0 on a freshly zero-filled file).
                if (!memoryMode) {
                    AckWatermark.removeOrphan(sfDir);
                    watermarkInProgress = AckWatermark.open(sfDir);
                }
                MmapSegment initial;
                String initialPath = null;
                if (memoryMode) {
                    initial = MmapSegment.createInMemory(0L, segmentSizeBytes);
                } else {
                    initialPath = sfDir + "/sf-initial.sfa";
                    initial = MmapSegment.create(initialPath, 0L, segmentSizeBytes);
                }
                try {
                    ringInProgress = new SegmentRing(initial, segmentSizeBytes);
                } catch (Throwable t) {
                    initial.close();
                    if (initialPath != null) {
                        Files.remove(initialPath);
                    }
                    throw t;
                }
            }

            if (ownsManager) {
                manager.start();
            }
            manager.register(ringInProgress, sfDir, watermarkInProgress);
            // All construction succeeded — commit the ring and
            // watermark references.
            this.ring = ringInProgress;
            this.watermark = watermarkInProgress;
        } catch (Throwable t) {
            // Stop an owned manager before freeing the ring and watermark it may
            // touch, then release the slot lock. Each cleanup is in its own
            // try/catch so a single failure doesn't strand later cleanups.
            // Closing an owned-but-never-started manager is safe (no worker to
            // join) and required: skipping it leaked the manager's native
            // path-scratch sink whenever construction failed before start().
            if (ownsManager) {
                try {
                    manager.close();
                } catch (Throwable ignored) {
                }
            }
            if (ringInProgress != null) {
                try {
                    ringInProgress.close();
                } catch (Throwable ignored) {
                }
            }
            if (watermarkInProgress != null) {
                try {
                    watermarkInProgress.close();
                } catch (Throwable ignored) {
                }
            }
            if (acquiredLock != null) {
                try {
                    acquiredLock.close();
                } catch (Throwable ignored) {
                }
            }
            throw t;
        }
    }

    /**
     * I/O thread accessor: highest FSN safe to send.
     */
    public long ackedFsn() {
        return ring.ackedFsn();
    }

    /**
     * Records a server ACK for cumulative FSN {@code seq}. Triggers
     * background trim of any sealed segments whose every frame is now
     * acknowledged. Idempotent and monotonic.
     *
     * @return {@code true} if the ack watermark actually advanced;
     * {@code false} on a no-op (idempotent re-ack or value clamped
     * at publishedFsn).
     */
    public boolean acknowledge(long seq) {
        return ring.acknowledge(seq);
    }

    /**
     * I/O thread accessor: the current active mmap'd segment.
     */
    public MmapSegment activeSegment() {
        return ring.getActive();
    }

    /**
     * Append the payload, blocking up to {@link #appendDeadlineNanos} when
     * the cursor ring is at its memory/disk cap and waiting for ACK-driven
     * trim to free space. Returns the assigned FSN on success.
     * <p>
     * Backpressure is surfaced two ways:
     * <ul>
     *   <li>{@link #getTotalBackpressureStalls()} counter — incremented once
     *       per blocking-call that had to wait for the manager.</li>
     *   <li>WARN log throttled to one line per
     *       {@link #BACKPRESSURE_LOG_THROTTLE_NANOS} of sustained
     *       backpressure, so ops can correlate slow flushes to the cap.</li>
     * </ul>
     * Throws {@link io.questdb.client.cutlass.line.LineSenderException} when
     * the deadline expires — silent unbounded blocking would mask "wire path
     * is wedged" failures (server down, slow disk, etc.) from the user.
     */
    public long appendBlocking(long payloadAddr, int payloadLen) {
        long fsn = ring.appendOrFsn(payloadAddr, payloadLen);
        if (fsn >= 0) return fsn;
        if (fsn == SegmentRing.PAYLOAD_TOO_LARGE) {
            throw new MmapSegmentException("payload too large for segment");
        }
        // First miss → record one stall (not one per spin) and start the
        // deadline clock.
        backpressureStallCount.incrementAndGet();
        long deadlineNs = System.nanoTime() + appendDeadlineNanos;
        while (true) {
            long now = System.nanoTime();
            if (now >= deadlineNs) {
                throw new io.questdb.client.cutlass.line.LineSenderException(
                        "cursor ring backpressured for ").put(appendDeadlineNanos / 1_000_000L)
                        .put(" ms — wire path is not draining (server slow / disconnected, or sf_max_total_bytes too small)");
            }
            if (now - lastBackpressureLogNs >= BACKPRESSURE_LOG_THROTTLE_NANOS) {
                lastBackpressureLogNs = now;
                LOG.warn("cursor producer backpressured ({} stalls so far); waiting for I/O drain — will throw after {} ms",
                        backpressureStallCount.get(), appendDeadlineNanos / 1_000_000L);
            }
            LockSupport.parkNanos(50_000L); // 50 µs
            fsn = ring.appendOrFsn(payloadAddr, payloadLen);
            if (fsn >= 0) return fsn;
            if (fsn == SegmentRing.PAYLOAD_TOO_LARGE) {
                throw new MmapSegmentException("payload too large for segment");
            }
        }
    }

    /**
     * User-thread append path. Spins briefly while waiting for the segment
     * manager to provision a hot spare; if backpressure persists past
     * {@code spinDeadlineNanos}, returns {@link SegmentRing#BACKPRESSURE_NO_SPARE}
     * so the caller can decide whether to {@code parkNanos} or surface the
     * pressure to the user.
     * <p>
     * Returns the assigned FSN on success, or one of the
     * {@code SegmentRing.BACKPRESSURE_*} / {@code PAYLOAD_*} sentinels.
     */
    public long appendOrFsn(long payloadAddr, int payloadLen, long spinDeadlineNanos) {
        long fsn = ring.appendOrFsn(payloadAddr, payloadLen);
        if (fsn >= 0) {
            return fsn;
        }
        if (fsn == SegmentRing.PAYLOAD_TOO_LARGE) {
            return fsn;
        }
        // Backpressure: spin briefly, then return so the caller decides.
        // The spin tightens the gap between manager-installs-spare and
        // producer-consumes-spare — usually a few µs on an idle manager thread.
        while (System.nanoTime() < spinDeadlineNanos) {
            Compat.onSpinWait();
            fsn = ring.appendOrFsn(payloadAddr, payloadLen);
            if (fsn >= 0 || fsn == SegmentRing.PAYLOAD_TOO_LARGE) {
                return fsn;
            }
        }
        return SegmentRing.BACKPRESSURE_NO_SPARE;
    }

    @Override
    public synchronized void close() {
        if (closed && closeCompleted) return;
        closed = true;
        // Capture drain state BEFORE closing the ring — once the ring is
        // closed, its accessors aren't safe to read. The active segment is
        // never trimmed by drainTrimmable (only sealed segments are), so
        // when everything published has been acked we have to unlink the
        // residual .sfa files here. Without this, the next sender (or a
        // drainer adopting this slot) would replay already-acked data
        // against potentially-fresh server state — duplicate writes when
        // the server has no dedup state for those messageSequences.
        // Memory mode has no files to unlink.
        //
        // Cleanup is gated on worker quiescence: releasing the ring,
        // watermark, segment files or the slot lock while the manager worker
        // is still mid service pass would let a replacement engine acquire
        // the slot and have its files unlinked by the stale worker —
        // store-and-forward data loss after restart. When the bounded worker
        // join times out on an owned manager, cleanup OWNERSHIP TRANSFERS to
        // the worker's exit path (deferUntilWorkerExit): the worker is
        // provably the last thread able to touch the slot directory, so it
        // releases everything the moment its in-flight pass finishes instead
        // of leaking the slot until process exit.
        //
        // "Fully drained" includes BOTH the obvious case (every published
        // FSN has been acked) AND the never-published case (publishedFsn
        // < 0). The latter matters because a drainer adopting an empty
        // orphan slot — segments filtered as empty by recovery, engine
        // recreates a fresh sf-initial.sfa — would otherwise leave that
        // fresh empty file behind, the next scanner finds it, adopts the
        // slot again, and the cycle repeats forever (M6).
        // Own try/catch so sabotaged/broken ring state cannot skip the
        // quiescence barrier below or the slot-lock release in finishClose.
        boolean drained = false;
        try {
            drained = sfDir != null
                    && (ring.publishedFsn() < 0
                    || ring.ackedFsn() >= ring.publishedFsn());
        } catch (Throwable ignored) {
        }
        final boolean fullyDrained = drained;
        fullyDrainedForDeferredClose = fullyDrained;
        // Each cleanup step in its own try/catch so a single failure
        // doesn't strand later cleanups — mirrors the constructor's
        // catch block. Without this, a throw from manager.deregister
        // or manager.close() would leave the ring mmap'd and any
        // residual .sfa files on disk, where the next sender can
        // adopt them and replay already-acked data.
        try {
            manager.deregister(ring);
        } catch (Throwable ignored) {
        }
        // Quiescence barrier. deregister alone only removes the entry
        // from the registry — the worker may still be mid service pass
        // for this ring (creating a spare file, trimming, unlinking).
        boolean workerQuiescent = false;
        if (ownsManager) {
            // Stopping and reaping a private manager is a stronger barrier
            // than waiting for this ring alone. Do it directly so a stuck
            // worker consumes at most one workerJoinTimeoutMillis budget,
            // rather than one here and a second one in manager.close().
            try {
                manager.close();
            } catch (Throwable ignored) {
            }
            try {
                workerQuiescent = manager.isWorkerReaped();
            } catch (Throwable ignored) {
            }
        } else {
            // A shared manager must keep serving its other rings, so wait
            // only for the deregistered ring's current pass to finish.
            try {
                workerQuiescent = manager.awaitRingQuiescence(ring);
            } catch (Throwable ignored) {
            }
        }
        if (!workerQuiescent && ownsManager) {
            // Ownership handoff: manager.close() already stopped the worker
            // loop (running=false), so the worker exits as soon as its
            // in-flight pass finishes — it is merely slow, or wedged in a
            // syscall. Either way it is the last thread able to touch the
            // slot, so hand it the terminal cleanup instead of leaking the
            // slot until process exit. completeDeferredClose and a retried
            // close() race through the terminalCleanupClaimed CAS, so the
            // cleanup runs exactly once and the worker never blocks on the
            // engine monitor a retried close() holds while joining it.
            boolean handedOff = false;
            boolean registrationFailed = false;
            try {
                handedOff = manager.deferOwnedEngineCloseUntilWorkerExit(deferredClose);
            } catch (Throwable ignored) {
                // Unexpected monitor/VM failure carries no worker-liveness
                // information. Ordinary handoff cannot allocate: both the
                // callback and the manager's single slot were preallocated.
                registrationFailed = true;
            }
            if (handedOff) {
                LOG.error("SF manager worker did not quiesce during engine close; ring, watermark "
                        + "and slot-lock release are handed to the worker's exit path and run the "
                        + "moment its in-flight service pass finishes. The slot stays locked (and "
                        + "isCloseCompleted() false) until then, so no replacement engine can race "
                        + "the stale worker on slot {}", sfDir == null ? "<memory>" : sfDir);
                return;
            }
            if (registrationFailed) {
                // The handoff never registered and the worker was never
                // observed — it must be presumed live and mid service pass.
                // Retain every worker-reachable resource (ring, watermark,
                // segment files, slot flock) and leave terminalCleanupClaimed
                // unclaimed and closeCompleted false, exactly like the
                // shared-manager leak branch: manager.close() above already
                // stopped the worker loop, so a retried close() converges via
                // isWorkerReaped() once the in-flight pass ends. The kernel
                // releases the slot flock on process exit regardless.
                LOG.error("SF worker-exit handoff registration failed during engine close; "
                        + "leaking the ring, watermark and slot lock so a possibly-live "
                        + "worker cannot corrupt a future engine on slot {}. close() may be "
                        + "invoked again to retry cleanup once the worker has exited.",
                        sfDir == null ? "<memory>" : sfDir);
                return;
            }
            // Handoff rejected: the worker loop exited between the failed
            // bounded join and the registration attempt (both sides share the
            // manager's lock, so this observation is exact). A worker past
            // its loop can never touch slot paths again — inline cleanup is
            // as safe as a reaped worker.
            workerQuiescent = true;
        }
        if (!workerQuiescent) {
            // A shared manager keeps serving sibling rings, so it cannot use
            // the whole-worker exit handoff above. Transfer cleanup to this
            // ring's current pass instead. Registration and pass completion
            // share the manager lock: true means the worker owns cleanup;
            // false means the pass already finished and inline cleanup is safe.
            boolean handedOff = false;
            boolean registrationFailed = false;
            try {
                handedOff = manager.deferUntilRingQuiescent(ring, deferredClose);
            } catch (Throwable ignored) {
                registrationFailed = true;
            }
            if (handedOff) {
                LOG.error("SF manager worker did not quiesce during engine close; ring, watermark "
                        + "and slot-lock release are handed to the current ring pass and run the "
                        + "moment that pass finishes. The slot stays locked (and "
                        + "isCloseCompleted() false) until then, so no replacement engine can "
                        + "race the stale worker on slot {}",
                        sfDir == null ? "<memory>" : sfDir);
                return;
            }
            if (registrationFailed) {
                LOG.error("SF ring-pass cleanup handoff registration failed during engine close; "
                        + "leaking the ring, watermark and slot lock so a possibly-live worker "
                        + "cannot corrupt a future engine on slot {}. The kernel releases the "
                        + "flock on process exit.", sfDir == null ? "<memory>" : sfDir);
                return;
            }
            workerQuiescent = true;
        }
        if (!terminalCleanupClaimed.compareAndSet(false, true)) {
            // A worker-exit handoff or earlier close owns the one-time
            // ring/watermark cleanup. Once that work is published complete,
            // this close may still retry an unconfirmed flock release.
            retryFlockReleaseIfReady();
            return;
        }
        finishClose(fullyDrained);
    }

    /**
     * Terminal cleanup: closes the ring and watermark, unlinks drained
     * segment files, releases the slot flock, and — only once the release
     * is <b>confirmed</b> — latches {@link #closeCompleted}. Publish order
     * is load-bearing: pools free the slot index the instant they observe
     * {@code closeCompleted} (via {@code isSlotLockReleased()}), so it must
     * never be visible while the flock is still held, or a replacement
     * engine races the release and fails acquisition on a live slot.
     * The caller must hold the engine monitor and
     * must have established that the manager worker can no longer touch the
     * slot directory (reaped, provably exited, or running this on its own
     * exit path) AND have won the {@link #terminalCleanupClaimed} CAS — the
     * claim token, not the engine monitor, is the exclusion between a
     * worker-exit handoff and a retried close(), so ring/watermark/flock are
     * never double-released and the worker's cleanup can never deadlock
     * against a close() that holds the monitor while joining the worker.
     */
    private void finishClose(boolean fullyDrained) {
        try {
            // On a fully-drained close, persist the final acked FSN through
            // the still-mapped watermark BEFORE closing the ring/watermark
            // and BEFORE unlinking any segment file. The manager persists
            // the watermark only on its own tick, so it may lag the final
            // ack. If the unlink below then fails (or the process dies
            // mid-unlink), residual acknowledged .sfa files without an
            // up-to-date watermark would seed the successor's recovery at
            // lowestBase - 1 and replay already-acknowledged rows --
            // duplicates on a non-DEDUP table. The write is a single mmap
            // store, so it succeeds even when the unlink is about to fail
            // (e.g. the slot dir turned read-only). Quiescence is already
            // established here, so no manager tick can race this write.
            if (fullyDrained && watermark != null) {
                try {
                    long finalAckedFsn = ring.ackedFsn();
                    if (finalAckedFsn >= 0) {
                        watermark.write(finalAckedFsn);
                    }
                } catch (Throwable ignored) {
                }
            }
            try {
                ring.close();
            } catch (Throwable ignored) {
            }
            // Close the watermark mmap/fd after the manager (which
            // writes through it) is gone but before the slot lock is
            // released. On fully-drained close, also unlink the file
            // -- a stale watermark with no segments behind it would
            // confuse a future recovery cycle if (it wouldn't actually
            // confuse current recovery, which only reads the watermark
            // when segments are present, but unlinking keeps the slot
            // dir clean and matches the "remove orphan" intent above).
            if (watermark != null) {
                try {
                    watermark.close();
                } catch (Throwable ignored) {
                }
            }
            if (fullyDrained) {
                boolean segmentsRemoved = false;
                try {
                    segmentsRemoved = unlinkAllSegmentFiles(sfDir);
                } catch (Throwable ignored) {
                }
                // Remove the watermark ONLY once every segment file is
                // confirmed gone. The watermark is what keeps residual
                // acknowledged segments inert to a successor's recovery;
                // removing it while any .sfa file survives would republish
                // those already-acknowledged rows.
                if (segmentsRemoved) {
                    try {
                        AckWatermark.removeOrphan(sfDir);
                    } catch (Throwable ignored) {
                    }
                } else {
                    LOG.warn("close-time segment cleanup incomplete on slot {}; retaining the ack "
                            + "watermark so residual acknowledged segments stay covered -- the next "
                            + "engine on this slot recovers them as fully acked and retries the "
                            + "unlink on its own close", sfDir);
                }
            }
        } finally {
            // Reaching finishClose at all requires established quiescence, so
            // releasing the flock is safe even if a step above threw. Leaking
            // it would strand the slot until process exit for no reason.
            //
            // ORDER MATTERS: explicitly release the flock, verify it, and
            // only then publish closeCompleted. Pools read isCloseCompleted()
            // as "the slot dir is reusable" and free the slot index the moment
            // it flips. SlotLock closes the fd once after confirmed unlock,
            // but that close result cannot safely control publication because
            // POSIX may consume the fd even when close reports failure.
            Runnable hook = beforeFlockReleaseHook;
            if (hook != null) {
                try {
                    hook.run();
                } catch (Throwable ignored) {
                    // test-only; must never block the release
                }
            }
            terminalResourcesCleaned = true;
            retryFlockReleaseIfReady();
        }
    }

    /**
     * Installs a hook invoked after each failed shared-driver flock release.
     * Test-only: makes persistent retry progress observable without sleeps.
     */
    @TestOnly
    public static void setAfterFlockReleaseRetryFailureHook(Runnable hook) {
        afterFlockReleaseRetryFailureHook = hook;
    }

    /**
     * Installs a constructor fault hook immediately before the bound deferred
     * close callback is created. Test-only: proves callback allocation failure
     * occurs before an owned manager acquires native resources.
     */
    @TestOnly
    public static void setBeforeDeferredCloseCreationHook(Runnable hook) {
        beforeDeferredCloseCreationHook = hook;
    }

    /**
     * Replaces the shared retry driver's inter-round park with a callback
     * receiving the park duration the driver would have used. Test-only:
     * makes the retry cadence inspectable and rounds coordinatable without
     * wall-clock waits.
     */
    @TestOnly
    public static void setFlockReleaseRetryParkOverride(LongConsumer override) {
        flockReleaseRetryParkOverride = override;
    }

    /**
     * Overrides creation of the single shared flock-release retry thread.
     * Test-only: makes thread creation/start failure and persistent retry
     * scalability deterministic without relying on scheduler timing.
     */
    @TestOnly
    public static void setFlockReleaseRetryThreadFactory(ThreadFactory factory) {
        synchronized (FLOCK_RELEASE_RETRY_LOCK) {
            if (flockReleaseRetryThread != null || !FLOCK_RELEASE_RETRY_QUEUE.isEmpty()) {
                throw new IllegalStateException("flock-release retry driver is active");
            }
            flockReleaseRetryThreadFactory = factory == null
                    ? DEFAULT_FLOCK_RELEASE_RETRY_THREAD_FACTORY
                    : factory;
        }
    }

    /**
     * Installs a hook that {@link #finishClose} runs between the terminal
     * cleanup and the slot-flock release. Test-only: makes the otherwise
     * microsecond-wide cleanup/release window deterministic so tests can
     * assert {@link #isCloseCompleted()} stays false until the release is
     * confirmed.
     */
    @TestOnly
    public void setBeforeFlockReleaseHook(Runnable hook) {
        this.beforeFlockReleaseHook = hook;
    }

    /**
     * Runs on a safe manager-worker handoff path when {@link #close()} moved
     * cleanup ownership to either a shared manager's ring-pass completion or
     * an owned manager's worker exit.
     * Deliberately does NOT take the engine monitor: a retried close() can
     * hold it while joining this very worker, and the thread cannot die until
     * this method returns — the {@link #terminalCleanupClaimed} CAS provides
     * the exactly-once exclusion instead, so the worker always exits promptly
     * and the racing close() converges via {@code isWorkerReaped()}.
     */
    private void completeDeferredClose() {
        if (!terminalCleanupClaimed.compareAndSet(false, true)) {
            // A retried close() (or an earlier duplicate handoff) already ran
            // the terminal cleanup.
            return;
        }
        finishClose(fullyDrainedForDeferredClose);
        LOG.info("deferred SF engine resource cleanup completed after manager-worker quiescence; "
                        + "slot release confirmed: {} [slot={}]",
                closeCompleted, sfDir == null ? "<memory>" : sfDir);
    }

    private Runnable createDeferredClose() {
        Runnable hook = beforeDeferredCloseCreationHook;
        if (hook != null) {
            hook.run();
        }
        return this::completeDeferredClose;
    }

    private boolean retryFlockReleaseIfReady() {
        if (closeCompleted || !terminalResourcesCleaned) {
            return closeCompleted;
        }
        boolean released;
        if (slotLock != null) {
            try {
                released = slotLock.release();
            } catch (Throwable ignored) {
                released = false;
            }
        } else {
            released = true;
        }
        if (released) {
            closeCompleted = true;
            Runnable listener = slotLockReleaseListener;
            if (listener != null) {
                try {
                    listener.run();
                } catch (Throwable ignored) {
                    // A notification failure must not invalidate a confirmed release.
                }
            }
            return true;
        }
        startFlockReleaseRetry();
        return false;
    }

    private static void runFlockReleaseRetryDriver() {
        // Capped exponential backoff: a persistent unlock failure must not
        // burn a fixed 10 rounds of native release syscalls per second
        // forever, but a transient failure must still recover promptly. The
        // ramp doubles per fully-failed round from 100 ms up to 5 s.
        long parkNanos = FLOCK_RELEASE_RETRY_BASE_PARK_NANOS;
        while (true) {
            final int roundSize;
            synchronized (FLOCK_RELEASE_RETRY_LOCK) {
                roundSize = FLOCK_RELEASE_RETRY_QUEUE.size();
                if (roundSize == 0) {
                    flockReleaseRetryThread = null;
                    return;
                }
            }
            boolean hasFailures = false;
            boolean hasSuccesses = false;
            for (int i = 0; i < roundSize; i++) {
                final CursorSendEngine engine;
                synchronized (FLOCK_RELEASE_RETRY_LOCK) {
                    engine = FLOCK_RELEASE_RETRY_QUEUE.pollFirst();
                }
                if (engine.retryFlockReleaseIfReady()) {
                    engine.flockReleaseRetryStarted.set(false);
                    hasSuccesses = true;
                } else {
                    synchronized (FLOCK_RELEASE_RETRY_LOCK) {
                        FLOCK_RELEASE_RETRY_QUEUE.addLast(engine);
                    }
                    hasFailures = true;
                    Runnable hook = afterFlockReleaseRetryFailureHook;
                    if (hook != null) {
                        hook.run();
                    }
                }
            }
            if (hasSuccesses) {
                // Progress: the failure condition is clearing, so retry the
                // remaining engines on the base cadence again.
                parkNanos = FLOCK_RELEASE_RETRY_BASE_PARK_NANOS;
            }
            if (hasFailures) {
                // Interruption must not abandon a retained flock, but clear
                // the flag so subsequent parks still throttle retries.
                Thread.interrupted();
                LongConsumer parkOverride = flockReleaseRetryParkOverride;
                if (parkOverride != null) {
                    parkOverride.accept(parkNanos);
                } else {
                    LockSupport.parkNanos(parkNanos);
                }
                parkNanos = Math.min(parkNanos * 2, FLOCK_RELEASE_RETRY_MAX_PARK_NANOS);
            }
        }
    }

    private void startFlockReleaseRetry() {
        if (!flockReleaseRetryStarted.compareAndSet(false, true)) {
            return;
        }
        Throwable startFailure = null;
        synchronized (FLOCK_RELEASE_RETRY_LOCK) {
            FLOCK_RELEASE_RETRY_QUEUE.addLast(this);
            if (flockReleaseRetryThread != null) {
                // The driver may be parked on a ramped backoff; wake it so a
                // freshly failed release gets its first driver retry promptly
                // instead of inheriting older engines' full backoff.
                LockSupport.unpark(flockReleaseRetryThread);
            } else {
                try {
                    Thread retryThread = flockReleaseRetryThreadFactory.newThread(
                            CursorSendEngine::runFlockReleaseRetryDriver);
                    if (retryThread == null) {
                        throw new IllegalStateException("retry thread factory returned null");
                    }
                    retryThread.setDaemon(true);
                    flockReleaseRetryThread = retryThread;
                    retryThread.start();
                } catch (Throwable t) {
                    startFailure = t;
                    flockReleaseRetryThread = null;
                    CursorSendEngine queued;
                    while ((queued = FLOCK_RELEASE_RETRY_QUEUE.pollFirst()) != null) {
                        queued.flockReleaseRetryStarted.set(false);
                    }
                }
            }
        }
        if (startFailure == null) {
            LOG.error("SF slot flock release failed during engine close; keeping "
                            + "closeCompleted=false and retrying on the shared driver so "
                            + "retired capacity recovers after the transient failure [slot={}]",
                    sfDir == null ? "<memory>" : sfDir);
        } else {
            // A later explicit close() or a pool's retired-slot probe
            // (ensureFlockReleaseRetryScheduled) can still retry without
            // repeating the one-time ring/watermark cleanup. The failed queue
            // is cleared so the driver does not retain engines it cannot
            // service.
            LOG.error("Could not start SF flock-release retry driver; a retried close() "
                            + "or pool re-probe re-arms the retry [slot={}, error={}]",
                    sfDir == null ? "<memory>" : sfDir, String.valueOf(startFailure));
        }
    }

    /**
     * Whether {@link #close()} completed all cleanup, including a
     * <b>confirmed</b> release of the SF slot lock — the flip is published
     * strictly after explicit unlock succeeds, so observing {@code true}
     * guarantees the slot dir is acquirable by a replacement engine. A false
     * value after close means manager-worker quiescence could not be
     * confirmed (or the flock release itself failed) and the
     * worker-reachable resources were retained deliberately — either handed to the worker's exit path (owned manager),
     * which flips this to true the moment the worker's in-flight pass
     * finishes, or leaked until a retried close() (shared manager). Owners
     * must not reuse the slot while this is false; pools may re-probe it to
     * recover a retired slot's capacity once it flips.
     * <p>
     * Deliberately unsynchronized ({@code closeCompleted} is volatile): pools
     * probe this under their own capacity lock, and the deferred cleanup can
     * hold the engine monitor through munmap/unlink syscalls — a synchronized
     * getter would stall the pool's hot borrow path behind them.
     */
    public boolean isCloseCompleted() {
        return closeCompleted;
    }

    /**
     * Registers a callback for confirmed SF slot-lock release. If release
     * already completed, invokes the callback before returning.
     */
    public void setSlotLockReleaseListener(Runnable listener) {
        slotLockReleaseListener = listener;
        if (listener != null && closeCompleted) {
            listener.run();
        }
    }

    /**
     * Re-arms the shared flock-release retry for an engine whose terminal
     * cleanup finished but whose confirmed flock release is still pending
     * and no longer scheduled — the retry driver thread failed to start when
     * the release first failed (e.g. OOM at thread creation).
     * {@code Sender.close()} is one-shot by contract, so pool probes
     * ({@code QwpWebSocketSender.isSlotLockReleased()}) call this to keep a
     * retired slot's capacity recoverable instead of lost until process
     * exit. Cheap unless the engine is in that orphan state (volatile reads,
     * then one failed CAS when the retry is already scheduled), so probes
     * may call it under their capacity lock.
     */
    public void ensureFlockReleaseRetryScheduled() {
        if (closeCompleted || !terminalResourcesCleaned) {
            return;
        }
        startFlockReleaseRetry();
    }

    /**
     * Pass-through to {@link SegmentRing#findSegmentContaining(long)}.
     */
    public MmapSegment findSegmentContaining(long fsn) {
        return ring.findSegmentContaining(fsn);
    }

    /**
     * Pass-through to {@link SegmentRing#firstSealed()}.
     */
    public MmapSegment firstSealed() {
        return ring.firstSealed();
    }

    /**
     * Number of times {@link #appendBlocking} hit
     * {@link SegmentRing#BACKPRESSURE_NO_SPARE} on its first attempt and
     * had to wait for the segment manager (or for ACKs) to free space.
     * One increment per blocking-call, not per spin-park. Cumulative.
     */
    public long getTotalBackpressureStalls() {
        return backpressureStallCount.get();
    }

    /**
     * Pass-through to {@link SegmentRing#nextSealedAfter(MmapSegment)}.
     */
    public MmapSegment nextSealedAfter(MmapSegment current) {
        return ring.nextSealedAfter(current);
    }

    /**
     * I/O thread accessor: highest FSN whose frame is fully written.
     */
    public long publishedFsn() {
        return ring.publishedFsn();
    }

    /**
     * I/O thread accessor: sealed segments waiting to drain. Direct view —
     * NOT thread-safe under producer-thread rotation. The I/O loop should
     * use {@link #sealedSegmentsSnapshot(MmapSegment[])} instead.
     */
    public ObjList<MmapSegment> sealedSegments() {
        return ring.getSealedSegments();
    }

    /**
     * Thread-safe snapshot pass-through to
     * {@link SegmentRing#snapshotSealedSegments(MmapSegment[])}. Returns
     * the count copied, or -1 if the buffer is too small.
     */
    public int sealedSegmentsSnapshot(MmapSegment[] target) {
        return ring.snapshotSealedSegments(target);
    }

    /**
     * Configured per-segment size in bytes.
     */
    public long segmentSizeBytes() {
        return segmentSizeBytes;
    }

    public String sfDir() {
        return sfDir;
    }

    /**
     * True when this engine opened against a pre-existing on-disk slot
     * (i.e. {@code SegmentRing.openExisting} returned a non-null ring at
     * construction). Memory-mode engines and fresh-disk engines return
     * false. Used by the sender to decide whether to mark schema state as
     * needing a reset before the first send.
     */
    public boolean wasRecoveredFromDisk() {
        return wasRecoveredFromDisk;
    }

    /**
     * FSN of the last commit-bearing frame in a disk-recovered ring, or
     * {@code -1} for fresh/memory rings. Frames above it are an orphaned
     * deferred tail (transaction never committed) that the server will not
     * ack until a later commit-bearing frame covers them.
     */
    public long recoveredCommitBoundaryFsn() {
        return recoveredCommitBoundaryFsn;
    }

    /**
     * FSN of the last frame of a recovered orphaned deferred tail, or
     * {@code -1} when none. See {@link #recoveredCommitBoundaryFsn()}: the
     * orphan range is {@code [recoveredCommitBoundaryFsn() + 1 .. this]}.
     */
    public long recoveredOrphanTipFsn() {
        return recoveredOrphanTipFsn;
    }

    /**
     * Ascending removal rank of a segment file name for
     * {@link #unlinkAllSegmentFiles(String)}. {@code sf-initial.sfa} is
     * always the fresh-start segment at baseSeq 0, so it ranks first.
     * Spare files carry a monotonic generation ({@code sf-<gen:016x>.sfa})
     * assigned in creation == rotation == baseSeq order, so the parsed
     * generation ranks them. Anything else with the extension is not a
     * live segment and ranks last.
     */
    private static long segmentCleanupRank(String name) {
        if ("sf-initial.sfa".equals(name)) {
            return Long.MIN_VALUE;
        }
        if (name.length() == 23 && name.startsWith("sf-")) {
            try {
                return Long.parseUnsignedLong(name.substring(3, 19), 16);
            } catch (NumberFormatException ignored) {
                // fall through to the unrecognized-name rank
            }
        }
        return Long.MAX_VALUE;
    }

    /**
     * Unlinks every {@code .sfa} file under {@code dir}. Called only on
     * clean shutdown when the ring confirms every published FSN has been
     * acked — at that moment the slot has no recoverable work and the
     * files are pure noise that would mislead the next sender's recovery.
     * <p>
     * Removal runs in ascending segment order and STOPS at the first
     * failure, so whatever survives (a failure here, or a crash mid-loop)
     * is always a contiguous top slice of the ring: recovery's
     * FSN-contiguity check still passes, and the retained ack watermark
     * (== the final acked FSN == the highest frame on disk) still covers
     * every surviving frame, so a successor replays nothing.
     *
     * @return {@code true} only when enumeration succeeded and every
     * {@code .sfa} file was confirmed removed — the caller keeps the ack
     * watermark on {@code false} so residual acknowledged segments stay
     * covered.
     */
    private static boolean unlinkAllSegmentFiles(String dir) {
        if (!io.questdb.client.std.Files.exists(dir)) return true;
        long find = io.questdb.client.std.Files.findFirst(dir);
        if (find < 0) {
            LOG.warn("close-time unlink could not enumerate {}; "
                    + "any residual sf-*.sfa files will be picked up by the next recovery", dir);
            return false;
        }
        if (find == 0) return true;
        ArrayList<String> names = new ArrayList<>();
        int rc = 1;
        try {
            while (rc > 0) {
                String name = io.questdb.client.std.Files.utf8ToString(
                        io.questdb.client.std.Files.findName(find));
                rc = io.questdb.client.std.Files.findNext(find);
                if (name != null && name.endsWith(".sfa")) {
                    names.add(name);
                }
            }
        } finally {
            io.questdb.client.std.Files.findClose(find);
        }
        if (rc < 0) {
            // A partial listing must not drive any unlink: removing only the
            // files we happened to see could delete the segment holding the
            // highest frame while a lower one survives, leaving residual
            // state the retained watermark can no longer vouch for.
            LOG.warn("close-time unlink could not fully enumerate {}; "
                    + "leaving all segment files for the next recovery", dir);
            return false;
        }
        names.sort((a, b) -> {
            int byRank = Long.compare(segmentCleanupRank(a), segmentCleanupRank(b));
            return byRank != 0 ? byRank : a.compareTo(b);
        });
        for (int i = 0, n = names.size(); i < n; i++) {
            String path = dir + "/" + names.get(i);
            if (!io.questdb.client.std.Files.remove(path)) {
                LOG.warn("Failed to unlink fully-acked segment {} on close; stopping so the "
                        + "residual files stay a contiguous, watermark-covered range", path);
                return false;
            }
        }
        return true;
    }
}
