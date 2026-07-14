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

import java.util.concurrent.locks.LockSupport;

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
    private final long appendDeadlineNanos;
    // Number of times appendBlocking observed BACKPRESSURE_NO_SPARE on its first
    // ring.appendOrFsn attempt. One increment per blocking-call that had to wait
    // for the manager (or for ACKs) — not one per spin-park. Producer-thread
    // writer; volatile because the user may sample it from any thread.
    private final java.util.concurrent.atomic.AtomicLong backpressureStallCount =
            new java.util.concurrent.atomic.AtomicLong();
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
        this(sfDir, segmentSizeBytes,
                new SegmentManager(segmentSizeBytes, SegmentManager.DEFAULT_POLL_NANOS, maxTotalBytes),
                true, appendDeadlineNanos);
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
                // The delegating constructors evaluate `new SegmentManager(...)`
                // BEFORE this body runs, so on a pre-try throw (e.g. slot lock
                // collision) an owned manager is already alive and would leak
                // its native path-scratch sink -- 256 bytes per failed
                // construction attempt. Close it before propagating.
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
        if (closed) return;
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
        // The whole close sequence runs under try/finally so the slot lock
        // is ALWAYS released, even if manager/ring close or unlink throws —
        // otherwise a kernel-held flock outlives the engine and the next
        // sender for the same slot collides on a lock the dead engine
        // never released.
        try {
            // "Fully drained" includes BOTH the obvious case (every published
            // FSN has been acked) AND the never-published case (publishedFsn
            // < 0). The latter matters because a drainer adopting an empty
            // orphan slot — segments filtered as empty by recovery, engine
            // recreates a fresh sf-initial.sfa — would otherwise leave that
            // fresh empty file behind, the next scanner finds it, adopts the
            // slot again, and the cycle repeats forever (M6).
            boolean fullyDrained = sfDir != null
                    && (ring.publishedFsn() < 0
                    || ring.ackedFsn() >= ring.publishedFsn());
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
            if (ownsManager) {
                try {
                    manager.close();
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
                try {
                    unlinkAllSegmentFiles(sfDir);
                } catch (Throwable ignored) {
                }
                try {
                    AckWatermark.removeOrphan(sfDir);
                } catch (Throwable ignored) {
                }
            }
        } finally {
            if (slotLock != null) {
                try {
                    slotLock.close();
                } catch (Throwable ignored) {
                    // best-effort; flock is also released by kernel on process exit
                }
            }
        }
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
     * Unlinks every {@code .sfa} file under {@code dir}. Called only on
     * clean shutdown when the ring confirms every published FSN has been
     * acked — at that moment the slot has no recoverable work and the
     * files are pure noise that would mislead the next sender's recovery.
     * Best-effort: logs and continues on failures, since we're already on
     * the close path.
     */
    private static void unlinkAllSegmentFiles(String dir) {
        if (!io.questdb.client.std.Files.exists(dir)) return;
        long find = io.questdb.client.std.Files.findFirst(dir);
        if (find < 0) {
            LOG.warn("close-time unlink could not enumerate {}; "
                    + "any residual sf-*.sfa files will be picked up by the next recovery", dir);
            return;
        }
        if (find == 0) return;
        try {
            int rc = 1;
            while (rc > 0) {
                String name = io.questdb.client.std.Files.utf8ToString(
                        io.questdb.client.std.Files.findName(find));
                rc = io.questdb.client.std.Files.findNext(find);
                if (name == null || !name.endsWith(".sfa")) continue;
                String path = dir + "/" + name;
                if (!io.questdb.client.std.Files.remove(path)) {
                    LOG.warn("Failed to unlink fully-acked segment {} on close", path);
                }
            }
        } finally {
            io.questdb.client.std.Files.findClose(find);
        }
    }
}
