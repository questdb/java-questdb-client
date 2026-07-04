/*+*****************************************************************************
 * ___                 _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * <li>Owning the ring + manager lifecycle (open / close / startup recovery).</li>
 * <li>Providing a user-thread append path that handles backpressure
 * (spin briefly, then return — caller decides whether to retry).</li>
 * <li>Exposing read accessors for the I/O thread: {@link #publishedFsn},
 * {@link #activeSegment}, {@link #sealedSegments}.</li>
 * <li>Routing server ACKs to the ring for trim.</li>
 * </ul>
 * <b>Not in scope:</b>
 * <ul>
 * <li>Multi-producer support. Single producer (one user thread) only.</li>
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
        boolean memoryMode = sfDir == null;
        if (!memoryMode && sfDir.isEmpty()) {
            throw new IllegalArgumentException("sfDir must not be empty");
        }

        this.slotLock = memoryMode ? null : SlotLock.acquire(sfDir);
        this.sfDir = sfDir;
        this.segmentSizeBytes = segmentSizeBytes;
        this.manager = manager;
        this.ownsManager = ownsManager;
        this.appendDeadlineNanos = appendDeadlineNanos;

        SegmentRing ringInProgress = null;
        AckWatermark watermarkInProgress = null;
        boolean managerStarted = false;
        try {
            SegmentRing recovered = memoryMode ? null : SegmentRing.openExisting(sfDir, segmentSizeBytes);
            this.wasRecoveredFromDisk = recovered != null;

            if (recovered != null) {
                ringInProgress = recovered;
                watermarkInProgress = AckWatermark.open(sfDir);
                seedRecoveredAckWatermark(recovered, watermarkInProgress);
            } else {
                if (!memoryMode) {
                    AckWatermark.removeOrphan(sfDir);
                    watermarkInProgress = AckWatermark.open(sfDir);
                }
                ringInProgress = createFreshRing(memoryMode, sfDir, segmentSizeBytes);
            }

            if (ownsManager) {
                manager.start();
                managerStarted = true;
            }
            manager.register(ringInProgress, sfDir, watermarkInProgress);

            this.ring = ringInProgress;
            this.watermark = watermarkInProgress;
        } catch (Throwable t) {
            handleConstructionFailure(ownsManager, managerStarted, ringInProgress, watermarkInProgress, slotLock);
            throw t;
        }
    }

    private static void seedRecoveredAckWatermark(SegmentRing recovered, AckWatermark watermark) {
        MmapSegment first = recovered.firstSealed();
        long lowestBase = first != null ? first.baseSeq() : recovered.getActive().baseSeq();
        long baseSeed = lowestBase - 1;
        long watermarkFsn = watermark != null ? watermark.read() : AckWatermark.INVALID;

        long candidate = Math.max(watermarkFsn, baseSeed);
        long seed = candidate > recovered.publishedFsn() ? baseSeed : candidate;
        if (seed >= 0) {
            recovered.acknowledge(seed);
        }
    }

    private static SegmentRing createFreshRing(boolean memoryMode, String sfDir, long segmentSizeBytes) {
        MmapSegment initial;
        String initialPath = null;
        if (memoryMode) {
            initial = MmapSegment.createInMemory(0L, segmentSizeBytes);
        } else {
            initialPath = sfDir + "/sf-initial.sfa";
            initial = MmapSegment.create(initialPath, 0L, segmentSizeBytes);
        }
        try {
            return new SegmentRing(initial, segmentSizeBytes);
        } catch (Throwable t) {
            initial.close();
            if (initialPath != null) {
                Files.remove(initialPath);
            }
            throw t;
        }
    }

    private static void handleConstructionFailure(boolean ownsManager, boolean managerStarted,
                                                  SegmentRing ring, AckWatermark watermark, SlotLock lock) {
        if (ownsManager && managerStarted) {
            try {
                // Best-effort cleanup
            } catch (Throwable ignored) {
            }
        }
        closeQuietly(ring);
        closeQuietly(watermark);
        closeQuietly(lock);
    }

    private static void closeQuietly(QuietCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Throwable ignored) {
            }
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
     * <li>{@link #getTotalBackpressureStalls()} counter — incremented once
     * per blocking-call that had to wait for the manager.</li>
     * <li>WARN log throttled to one line per
     * {@link #BACKPRESSURE_LOG_THROTTLE_NANOS} of sustained
     * backpressure, so ops can correlate slow flushes to the cap.</li>
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
        try {
            boolean fullyDrained = sfDir != null
                    && (ring.publishedFsn() < 0 || ring.ackedFsn() >= ring.publishedFsn());

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
            unlinkEnumeratedFiles(find, dir);
        } finally {
            io.questdb.client.std.Files.findClose(find);
        }
    }

    private static void unlinkEnumeratedFiles(long find, String dir) {
        int rc = 1;
        while (rc > 0) {
            String name = io.questdb.client.std.Files.utf8ToString(io.questdb.client.std.Files.findName(find));
            rc = io.questdb.client.std.Files.findNext(find);

            if (name != null && name.endsWith(".sfa")) {
                String path = dir + "/" + name;
                if (!io.questdb.client.std.Files.remove(path)) {
                    LOG.warn("Failed to unlink fully-acked segment {} on close", path);
                }
            }
        }
    }
}