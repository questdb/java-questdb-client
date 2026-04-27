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
import io.questdb.client.std.QuietCloseable;

import java.util.concurrent.locks.LockSupport;

/**
 * Facade that bundles a {@link SegmentRing} with a {@link SegmentManager} and
 * exposes the user-facing API that a wire-send loop will call into. This is
 * the integration point a future {@code QwpWebSocketSender} variant will use
 * in place of the legacy {@code SegmentLog} + {@code WebSocketSendQueue}
 * coupling — keeping the SF append work on the user thread (where it belongs)
 * and the segment lifecycle work on the manager thread (where it belongs).
 * <p>
 * <b>What this class is responsible for:</b>
 * <ul>
 *   <li>Owning the ring + manager lifecycle (open / close / startup recovery).</li>
 *   <li>Providing a user-thread append path that handles backpressure
 *       (spin briefly, then return — caller decides whether to retry).</li>
 *   <li>Exposing read accessors for the I/O thread: {@link #publishedFsn},
 *       {@link #activeSegment}, {@link #sealedSegments}.</li>
 *   <li>Routing server ACKs to the ring for trim.</li>
 * </ul>
 * <b>What this class is NOT yet responsible for (deferred follow-up):</b>
 * <ul>
 *   <li>Actually being wired into {@code QwpWebSocketSender}. Today the
 *       sender uses {@code WebSocketSendQueue + SegmentLog}; replacing those
 *       requires rewriting the I/O loop / ACK protocol / reconnect path.
 *       That's tracked separately.</li>
 *   <li>Recovery of segment ring from an existing {@code sf_dir} on startup.
 *       For now the engine starts fresh.</li>
 *   <li>Multi-producer support. Single producer (one user thread) only.</li>
 * </ul>
 */
public final class CursorSendEngine implements QuietCloseable {

    /** Default deadline for {@link #appendBlocking}: 30 seconds. */
    public static final long DEFAULT_APPEND_DEADLINE_NANOS = 30_000_000_000L;
    /** Throttle the "producer is backpressured" WARN log to at most once per this interval. */
    public static final long BACKPRESSURE_LOG_THROTTLE_NANOS = 5_000_000_000L; // 5 s
    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(CursorSendEngine.class);

    private final String sfDir;
    private final SegmentManager manager;
    // We own the manager iff the user constructed us with no manager — in that
    // case close() also stops the manager. When the manager is shared across
    // many engines (one per Sender), the caller owns and closes it.
    private final boolean ownsManager;
    // Held for the engine's lifetime in disk mode. {@code null} in memory
    // mode (no slot, no lock). Released by {@link #close()}; the kernel
    // also drops it on hard process exit.
    private final SlotLock slotLock;
    private final SegmentRing ring;
    private final long segmentSizeBytes;
    private final long appendDeadlineNanos;
    // True when the constructor recovered an existing on-disk slot rather
    // than starting fresh. Read by QwpWebSocketSender during connect to
    // decide whether to bump connectionGeneration so the first batch
    // re-publishes schema definitions (the server has no memory of FSNs
    // we recovered from disk).
    private final boolean recoveredFromDisk;
    // Number of times appendBlocking observed BACKPRESSURE_NO_SPARE on its first
    // ring.appendOrFsn attempt. One increment per blocking-call that had to wait
    // for the manager (or for ACKs) — not one per spin-park. Producer-thread
    // writer; volatile because the user may sample it from any thread.
    private final java.util.concurrent.atomic.AtomicLong backpressureStallCount =
            new java.util.concurrent.atomic.AtomicLong();
    // Producer-thread-only: timestamp of the last "we're backpressured" log
    // line, used to throttle. Plain long is fine.
    private long lastBackpressureLogNs;
    private boolean closed;

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
            if (sfDir.isEmpty()) {
                throw new IllegalArgumentException("sfDir must not be empty");
            }
            // Acquire the slot lock BEFORE we touch any *.sfa files. Two
            // engines pointed at the same slot would otherwise race on
            // recovery and create overlapping FSN ranges. SlotLock.acquire
            // also creates the slot dir if it doesn't exist yet — no
            // separate mkdir step needed here.
            acquiredLock = SlotLock.acquire(sfDir);
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
        boolean managerStarted = false;
        try {
            // Disk mode: try to recover any *.sfa files left behind by a prior
            // session before deciding to start fresh. Without this the engine
            // would create a new sf-initial.sfa at baseSeq=0, overlapping FSNs
            // already on disk and corrupting ACK translation, trim, and replay.
            SegmentRing recovered = memoryMode ? null
                    : SegmentRing.openExisting(sfDir, segmentSizeBytes);
            this.recoveredFromDisk = recovered != null;
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
                MmapSegment first = recovered.firstSealed();
                long lowestBase = first != null
                        ? first.baseSeq()
                        : recovered.getActive().baseSeq();
                if (lowestBase > 0) {
                    recovered.acknowledge(lowestBase - 1);
                }
            } else {
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
                managerStarted = true;
            }
            manager.register(ringInProgress, sfDir);
            // All construction succeeded — commit the ring reference.
            this.ring = ringInProgress;
        } catch (Throwable t) {
            // Order: ring first (releases mmap/fd), then manager (joins
            // worker thread, but only if we started it AND we own it),
            // then slot lock. Each in its own try/catch so a single
            // failure doesn't strand later cleanups.
            if (ringInProgress != null) {
                try {
                    ringInProgress.close();
                } catch (Throwable ignored) {
                }
            }
            if (ownsManager && managerStarted) {
                try {
                    manager.close();
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
     * Records a server ACK for cumulative FSN {@code seq}. Triggers
     * background trim of any sealed segments whose every frame is now
     * acknowledged. Idempotent and monotonic.
     */
    public void acknowledge(long seq) {
        ring.acknowledge(seq);
    }

    /** I/O thread accessor: highest FSN safe to send. */
    public long ackedFsn() {
        return ring.ackedFsn();
    }

    /** I/O thread accessor: the current active mmap'd segment. */
    public MmapSegment activeSegment() {
        return ring.getActive();
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
            Thread.onSpinWait();
            fsn = ring.appendOrFsn(payloadAddr, payloadLen);
            if (fsn >= 0 || fsn == SegmentRing.PAYLOAD_TOO_LARGE) {
                return fsn;
            }
        }
        return SegmentRing.BACKPRESSURE_NO_SPARE;
    }

    @Override
    public void close() {
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
        boolean fullyDrained = sfDir != null
                && ring.publishedFsn() >= 0
                && ring.ackedFsn() >= ring.publishedFsn();
        manager.deregister(ring);
        if (ownsManager) {
            manager.close();
        }
        ring.close();
        if (fullyDrained) {
            unlinkAllSegmentFiles(sfDir);
        }
        if (slotLock != null) {
            try {
                slotLock.close();
            } catch (Throwable ignored) {
                // best-effort; flock is also released by kernel on process exit
            }
        }
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

    /**
     * True when this engine opened against a pre-existing on-disk slot
     * (i.e. {@code SegmentRing.openExisting} returned a non-null ring at
     * construction). Memory-mode engines and fresh-disk engines return
     * false. Used by the sender to decide whether to mark schema state as
     * needing a reset before the first send.
     */
    public boolean wasRecoveredFromDisk() {
        return recoveredFromDisk;
    }

    /** I/O thread accessor: highest FSN whose frame is fully written. */
    public long publishedFsn() {
        return ring.publishedFsn();
    }

    /**
     * I/O thread accessor: sealed segments waiting to drain. Direct view —
     * NOT thread-safe under producer-thread rotation. The I/O loop should
     * use {@link #sealedSegmentsSnapshot(MmapSegment[])} instead.
     */
    public io.questdb.client.std.ObjList<MmapSegment> sealedSegments() {
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

    /** Pass-through to {@link SegmentRing#nextSealedAfter(MmapSegment)}. */
    public MmapSegment nextSealedAfter(MmapSegment current) {
        return ring.nextSealedAfter(current);
    }

    /** Pass-through to {@link SegmentRing#firstSealed()}. */
    public MmapSegment firstSealed() {
        return ring.firstSealed();
    }

    /** Pass-through to {@link SegmentRing#findSegmentContaining(long)}. */
    public MmapSegment findSegmentContaining(long fsn) {
        return ring.findSegmentContaining(fsn);
    }

    /** Configured per-segment size in bytes. */
    public long segmentSizeBytes() {
        return segmentSizeBytes;
    }

    public String sfDir() {
        return sfDir;
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
     * Number of times {@link #appendBlocking} hit
     * {@link SegmentRing#BACKPRESSURE_NO_SPARE} on its first attempt and
     * had to wait for the segment manager (or for ACKs) to free space.
     * One increment per blocking-call, not per spin-park. Cumulative.
     */
    public long getTotalBackpressureStalls() {
        return backpressureStallCount.get();
    }
}
