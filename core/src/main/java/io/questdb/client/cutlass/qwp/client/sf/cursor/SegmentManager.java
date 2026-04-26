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
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Background worker that keeps every registered {@link SegmentRing} supplied
 * with a hot-spare segment and trims segments after their frames have been
 * ACK'd by the server. Off the user-thread / I/O-thread hot path entirely:
 * the expensive {@code openCleanRW + truncate + mmap} for spare creation and
 * {@code munmap + unlink} for trim happen on this thread, never on the
 * latency-sensitive paths.
 * <p>
 * One instance can serve many rings (typically all {@code Sender} instances
 * in a JVM). Polls each ring on a configurable tick (default 1 ms) — short
 * enough that a producer rarely sees {@link SegmentRing#BACKPRESSURE_NO_SPARE}
 * in the steady state, long enough that an idle JVM doesn't burn CPU.
 * <p>
 * <b>baseSeq race window:</b> the spare is created with
 * {@code baseSeq = ring.nextSeqHint()} as observed by the manager. If the
 * producer thread appends more frames before the rotation actually fires,
 * the spare's baseSeq will be stale and {@link SegmentRing#appendOrFsn} will
 * throw on the mismatch check. In practice this is benign — by the time
 * {@link SegmentRing#needsHotSpare()} returns true the producer has very
 * little room left in the active segment, and the manager polls fast enough
 * to install before the producer fills the rest. Hardening to make the race
 * impossible (lazy header write at rotation time) is a separate refinement
 * deferred to PR2.
 */
public final class SegmentManager implements QuietCloseable {

    public static final long DEFAULT_POLL_NANOS = 1_000_000L; // 1 ms
    private static final Logger LOG = LoggerFactory.getLogger(SegmentManager.class);

    private final AtomicLong fileGeneration = new AtomicLong();
    private final Object lock = new Object();
    private final long pollNanos;
    private final ObjList<RingEntry> rings = new ObjList<>();
    private final long segmentSizeBytes;
    private volatile boolean running;
    private Thread workerThread;

    public SegmentManager(long segmentSizeBytes) {
        this(segmentSizeBytes, DEFAULT_POLL_NANOS);
    }

    public SegmentManager(long segmentSizeBytes, long pollNanos) {
        if (segmentSizeBytes < MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1) {
            throw new IllegalArgumentException("segmentSizeBytes too small: " + segmentSizeBytes);
        }
        this.segmentSizeBytes = segmentSizeBytes;
        this.pollNanos = pollNanos;
    }

    @Override
    public void close() {
        running = false;
        if (workerThread != null) {
            LockSupport.unpark(workerThread);
            try {
                workerThread.join(5_000);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            workerThread = null;
        }
    }

    /**
     * Stop tracking {@code ring}. Pending spares for the ring are NOT
     * created after this returns, but already-installed spares stay with
     * the ring (the ring closes them on its own {@link SegmentRing#close}).
     * Idempotent; safe to call from any thread.
     */
    public void deregister(SegmentRing ring) {
        synchronized (lock) {
            for (int i = 0, n = rings.size(); i < n; i++) {
                if (rings.get(i).ring == ring) {
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
     */
    public void register(SegmentRing ring, String dir) {
        synchronized (lock) {
            rings.add(new RingEntry(ring, dir));
        }
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

    private void serviceRing(RingEntry e) {
        // 1. Provision a hot spare if the ring needs one.
        if (e.ring.needsHotSpare()) {
            String path = nextSparePath(e.dir);
            try {
                // baseSeq is provisional — SegmentRing.appendOrFsn calls
                // rebaseSeq() at rotation time to pin the real value. We
                // pass the manager's best guess (nextSeqHint at this
                // instant), which is fine since it's overwritten anyway.
                MmapSegment spare = MmapSegment.create(path, e.ring.nextSeqHint(), segmentSizeBytes);
                try {
                    e.ring.installHotSpare(spare);
                } catch (Throwable t) {
                    spare.close();
                    Files.remove(path);
                    throw t;
                }
            } catch (Throwable t) {
                LOG.warn("Failed to provision hot spare in {} (will retry next tick)", e.dir, t);
            }
        }

        // 2. Trim any segments that the ring says are fully acked.
        ObjList<MmapSegment> trim = e.ring.drainTrimmable();
        if (trim != null) {
            for (int i = 0, n = trim.size(); i < n; i++) {
                MmapSegment s = trim.get(i);
                String path = s.path();
                try {
                    s.close();
                    if (!Files.remove(path)) {
                        LOG.warn("Failed to unlink trimmed segment {}", path);
                    }
                } catch (Throwable t) {
                    LOG.warn("Failed to trim segment {}", path, t);
                }
            }
        }
    }

    /**
     * Spare files are named with a JVM-wide monotonic generation counter
     * rather than a baseSeq-derived name, because the spare's baseSeq is
     * provisional at create time (SegmentRing.appendOrFsn rebases it at
     * rotation). Pattern: {@code <dir>/sf-<gen:016x>.sfa}. A recovery
     * scanner (cursor engine or legacy SegmentLog) discovers segments by
     * extension + header magic, not by name, so this is fine.
     */
    private String nextSparePath(String dir) {
        return dir + "/sf-" + String.format("%016x", fileGeneration.getAndIncrement()) + ".sfa";
    }

    private void workerLoop() {
        while (running) {
            // Snapshot the rings under the lock so we don't hold it through the
            // (potentially slow) syscalls during creation/unlink.
            int snapshotSize;
            RingEntry[] snapshot;
            synchronized (lock) {
                snapshotSize = rings.size();
                snapshot = new RingEntry[snapshotSize];
                for (int i = 0; i < snapshotSize; i++) {
                    snapshot[i] = rings.get(i);
                }
            }
            for (int i = 0; i < snapshotSize; i++) {
                if (!running) break;
                serviceRing(snapshot[i]);
            }
            if (!running) break;
            LockSupport.parkNanos(pollNanos);
        }
    }

    private static final class RingEntry {
        final String dir;
        final SegmentRing ring;

        RingEntry(SegmentRing ring, String dir) {
            this.ring = ring;
            this.dir = dir;
        }
    }
}
