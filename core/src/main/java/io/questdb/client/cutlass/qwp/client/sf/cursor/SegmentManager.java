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
    public static final long DISK_FULL_LOG_THROTTLE_NANOS = 30_000_000_000L; // 30 s
    public static final long UNLIMITED_TOTAL_BYTES = Long.MAX_VALUE;
    private static final Logger LOG = LoggerFactory.getLogger(SegmentManager.class);

    private final AtomicLong fileGeneration = new AtomicLong();
    private final Object lock = new Object();
    private final long maxTotalBytes;
    private final long pollNanos;
    private final ObjList<RingEntry> rings = new ObjList<>();
    private final long segmentSizeBytes;
    // Total bytes currently allocated across every segment owned by every
    // registered ring (active + sealed + hot-spare). Mutated by the manager
    // thread on provision/trim and by register/deregister callers under
    // {@link #lock}; the lock covers both paths so the counter stays
    // consistent across registration boundaries.
    private long totalBytes;
    private long lastDiskFullLogNs;
    private volatile boolean running;
    private Thread workerThread;

    public SegmentManager(long segmentSizeBytes) {
        this(segmentSizeBytes, DEFAULT_POLL_NANOS, UNLIMITED_TOTAL_BYTES);
    }

    public SegmentManager(long segmentSizeBytes, long pollNanos) {
        this(segmentSizeBytes, pollNanos, UNLIMITED_TOTAL_BYTES);
    }

    /**
     * Full constructor.
     *
     * @param segmentSizeBytes per-segment file size in bytes
     * @param pollNanos how often the worker polls each registered ring;
     *                  default {@link #DEFAULT_POLL_NANOS}
     * @param maxTotalBytes upper bound on total bytes the manager tracks
     *                      across all registered rings — counts every segment
     *                      the ring owns (initial active + sealed + hot
     *                      spare), including bytes already on disk at
     *                      register-time (e.g. after recovery or orphan
     *                      adoption). When provisioning a hot spare would
     *                      exceed this, the manager skips the install — the
     *                      requesting ring stays in the
     *                      {@link SegmentRing#BACKPRESSURE_NO_SPARE} state
     *                      until ACK-driven trim frees space. Pass
     *                      {@link #UNLIMITED_TOTAL_BYTES} to disable. Must be
     *                      at least one {@code segmentSizeBytes}; a sensible
     *                      lower bound for a single ring is
     *                      {@code 2 × segmentSizeBytes} so the manager can
     *                      hold an initial active plus one hot spare.
     */
    public SegmentManager(long segmentSizeBytes, long pollNanos, long maxTotalBytes) {
        if (segmentSizeBytes < MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1) {
            throw new IllegalArgumentException("segmentSizeBytes too small: " + segmentSizeBytes);
        }
        if (maxTotalBytes < segmentSizeBytes) {
            throw new IllegalArgumentException(
                    "maxTotalBytes (" + maxTotalBytes + ") must allow at least one segment of "
                            + segmentSizeBytes + " bytes");
        }
        this.segmentSizeBytes = segmentSizeBytes;
        this.pollNanos = pollNanos;
        this.maxTotalBytes = maxTotalBytes;
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
                    // Reverse the ring's contribution to totalBytes —
                    // mirrors the seed in register(). Any spares the
                    // manager provisioned during the ring's lifetime
                    // are also part of totalSegmentBytes() now, so a
                    // single subtraction covers both the initial seed
                    // and the net manager activity (provisions minus
                    // trims) for this ring.
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
        synchronized (lock) {
            rings.add(new RingEntry(ring, dir));
            // Account for bytes the ring already owns when it joins. A
            // recovered ring (post-restart, orphan adoption) can come up
            // at-or-above the cap; without this seed, totalBytes stays
            // at 0 and the per-tick cap check at serviceRing would let
            // the manager keep provisioning new spares on top of the
            // recovered set, effectively doubling the documented cap.
            totalBytes += ring.totalSegmentBytes();
            // Skip the file-generation counter past whatever's already on
            // disk in this slot. Without this, on recovery the manager
            // would mint a new spare at sf-0000000000000000.sfa — and
            // openCleanRW would truncate the user's existing active file
            // out from under the I/O loop, scrambling the in-flight mmap.
            // Memory-mode rings have no dir; nothing to scan there.
            if (dir != null) {
                long minNext = scanMaxGeneration(dir) + 1L;
                while (true) {
                    long cur = fileGeneration.get();
                    if (cur >= minNext) break;
                    if (fileGeneration.compareAndSet(cur, minNext)) break;
                }
            }
        }
        ring.setManagerWakeup(this::wakeWorker);
    }

    /**
     * Returns the highest hex-encoded generation across {@code sf-<gen>.sfa}
     * files in {@code dir}, or {@code -1} if none exist. Skips files that
     * don't match the pattern (e.g. the legacy {@code sf-initial.sfa}).
     */
    private static long scanMaxGeneration(String dir) {
        long max = -1L;
        if (!Files.exists(dir)) return max;
        long find = Files.findFirst(dir);
        if (find == 0) return max;
        try {
            int rc = 1;
            while (rc > 0) {
                String name = Files.utf8ToString(Files.findName(find));
                rc = Files.findNext(find);
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
        } finally {
            Files.findClose(find);
        }
        return max;
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
        // 1. Provision a hot spare if the ring needs one AND we have headroom
        //    under the disk-total cap. Cap check is per-tick; if we're capped
        //    here, the ring stays in BACKPRESSURE_NO_SPARE until trim (step 2)
        //    on this or a subsequent tick frees space. Logged at most once per
        //    DISK_FULL_LOG_THROTTLE_NANOS so a sustained-disk-full state
        //    doesn't drown the log.
        boolean memoryMode = e.dir == null;
        if (e.ring.needsHotSpare()) {
            // Snapshot totalBytes under lock — register/deregister can mutate
            // it from caller threads. Heavy provisioning I/O happens outside
            // the lock; the post-install commit re-acquires it.
            long observedTotal;
            synchronized (lock) {
                observedTotal = totalBytes;
            }
            if (observedTotal + segmentSizeBytes > maxTotalBytes) {
                long now = System.nanoTime();
                if (now - lastDiskFullLogNs >= DISK_FULL_LOG_THROTTLE_NANOS) {
                    LOG.warn("SF {}: cannot provision spare in {} "
                                    + "(totalBytes={}, cap={}, segmentSize={}). "
                                    + "Producer is backpressured until ACK-driven trim frees space.",
                            memoryMode ? "memory cap reached" : "disk-full",
                            memoryMode ? "<memory>" : e.dir, observedTotal, maxTotalBytes, segmentSizeBytes);
                    lastDiskFullLogNs = now;
                }
            } else {
                try {
                    // baseSeq is provisional — SegmentRing.appendOrFsn calls
                    // rebaseSeq() at rotation time to pin the real value. We
                    // pass the manager's best guess (nextSeqHint at this
                    // instant), which is fine since it's overwritten anyway.
                    MmapSegment spare;
                    String path;
                    if (memoryMode) {
                        spare = MmapSegment.createInMemory(e.ring.nextSeqHint(), segmentSizeBytes);
                        path = null;
                    } else {
                        path = nextSparePath(e.dir);
                        spare = MmapSegment.create(path, e.ring.nextSeqHint(), segmentSizeBytes);
                    }
                    try {
                        e.ring.installHotSpare(spare);
                        synchronized (lock) {
                            totalBytes += segmentSizeBytes;
                        }
                    } catch (Throwable t) {
                        spare.close();
                        if (path != null) {
                            Files.remove(path);
                        }
                        throw t;
                    }
                } catch (Throwable t) {
                    LOG.warn("Failed to provision hot spare in {} (will retry next tick)",
                            memoryMode ? "<memory>" : e.dir, t);
                }
            }
        }

        // 2. Trim any segments that the ring says are fully acked. For
        //    memory-mode rings, "trim" is just close() (Unsafe.free) — no
        //    file to unlink.
        ObjList<MmapSegment> trim = e.ring.drainTrimmable();
        if (trim != null) {
            for (int i = 0, n = trim.size(); i < n; i++) {
                MmapSegment s = trim.get(i);
                String path = s.path();
                long sz = s.sizeBytes();
                try {
                    s.close();
                    if (path != null && !Files.remove(path)) {
                        LOG.warn("Failed to unlink trimmed segment {}", path);
                    }
                    synchronized (lock) {
                        totalBytes -= sz;
                    }
                } catch (Throwable t) {
                    LOG.warn("Failed to trim segment {}", path == null ? "<memory>" : path, t);
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
