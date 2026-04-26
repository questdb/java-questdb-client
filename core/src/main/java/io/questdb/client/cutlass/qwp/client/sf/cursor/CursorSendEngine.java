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

    private final String sfDir;
    private final SegmentManager manager;
    // We own the manager iff the user constructed us with no manager — in that
    // case close() also stops the manager. When the manager is shared across
    // many engines (one per Sender), the caller owns and closes it.
    private final boolean ownsManager;
    private final SegmentRing ring;
    private final long segmentSizeBytes;
    private boolean closed;

    /**
     * Creates an engine with a private, non-shared {@link SegmentManager}.
     * Convenient for one-off senders / tests; for multi-Sender JVMs prefer
     * {@link #CursorSendEngine(String, long, SegmentManager)} with a shared
     * manager so all rings share one background thread.
     */
    public CursorSendEngine(String sfDir, long segmentSizeBytes) {
        this(sfDir, segmentSizeBytes, new SegmentManager(segmentSizeBytes), true);
    }

    /**
     * Creates an engine that shares the given {@link SegmentManager} (which
     * must already be {@link SegmentManager#start()}'d). The caller retains
     * ownership of the manager.
     */
    public CursorSendEngine(String sfDir, long segmentSizeBytes, SegmentManager manager) {
        this(sfDir, segmentSizeBytes, manager, false);
    }

    private CursorSendEngine(String sfDir, long segmentSizeBytes, SegmentManager manager,
                             boolean ownsManager) {
        if (sfDir == null || sfDir.isEmpty()) {
            throw new IllegalArgumentException("sfDir must not be empty");
        }
        if (!Files.exists(sfDir)) {
            int rc = Files.mkdir(sfDir, 0755);
            if (rc != 0) {
                throw new IllegalStateException("could not create sf_dir: " + sfDir + " rc=" + rc);
            }
        }
        this.sfDir = sfDir;
        this.segmentSizeBytes = segmentSizeBytes;
        this.manager = manager;
        this.ownsManager = ownsManager;

        // Create the initial active segment with baseSeq=0. (No on-disk
        // recovery in PR1 — assumes the directory is empty.) The manager will
        // immediately notice that the ring needs a hot spare and provision one.
        String initialPath = sfDir + "/sf-initial.sfa";
        MmapSegment initial = MmapSegment.create(initialPath, 0L, segmentSizeBytes);
        try {
            this.ring = new SegmentRing(initial, segmentSizeBytes);
        } catch (Throwable t) {
            initial.close();
            Files.remove(initialPath);
            throw t;
        }

        if (ownsManager) {
            manager.start();
        }
        manager.register(ring, sfDir);
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
        manager.deregister(ring);
        if (ownsManager) {
            manager.close();
        }
        ring.close();
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

    /** Configured per-segment size in bytes. */
    public long segmentSizeBytes() {
        return segmentSizeBytes;
    }

    public String sfDir() {
        return sfDir;
    }

    /**
     * Convenience overload: park-park-spin variant that retries indefinitely
     * (or until the engine is closed, in which case the caller will throw on
     * the next access). Use only when the producer is OK blocking — for
     * latency-sensitive paths, prefer
     * {@link #appendOrFsn(long, int, long)} with a real deadline.
     */
    public long appendBlocking(long payloadAddr, int payloadLen) {
        long fsn;
        while (true) {
            fsn = ring.appendOrFsn(payloadAddr, payloadLen);
            if (fsn >= 0) return fsn;
            if (fsn == SegmentRing.PAYLOAD_TOO_LARGE) {
                throw new MmapSegmentException("payload too large for segment");
            }
            LockSupport.parkNanos(50_000L); // 50 µs
        }
    }
}
