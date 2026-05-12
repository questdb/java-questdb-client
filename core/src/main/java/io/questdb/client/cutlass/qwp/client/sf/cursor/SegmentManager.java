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
import io.questdb.client.std.Numbers;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.str.StringSink;
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
    // Reused by the manager worker thread to build spare-segment paths
    // without per-rotation String.format / concat allocation.
    private final StringSink pathSink = new StringSink();
    private final long pollNanos;
    private final ObjList<RingEntry> rings = new ObjList<>();
    // Reused by the worker thread each tick to snapshot `rings` under the
    // lock without per-tick allocation. Owned exclusively by workerLoop().
    private final ObjList<RingEntry> ringSnapshot = new ObjList<>();
    private final long segmentSizeBytes;
    // Total bytes currently allocated across every segment owned by every
    // registered ring (active + sealed + hot-spare). Mutated by the manager
    // thread on provision/trim and by register/deregister callers under
    // {@link #lock}; the lock covers both paths so the counter stays
    // consistent across registration boundaries.
    private long totalBytes;
    private long lastDiskFullLogNs;
    // Test seam: runs on the worker thread just before the trim block's
    // synchronized(lock) entry. Null in production; only
    // SegmentManagerTrimDeregisterRaceTest installs it, to deterministically
    // inject a deregister(ring) call into the exact race window that the
    // stillRegistered guard inside the trim block closes.
    volatile Runnable beforeTrimSyncHook;
    private volatile boolean running;
    // volatile because wakeWorker() reads workerThread without holding the
    // monitor; the synchronized start()/close() pair handles the
    // start-vs-close ordering.
    private volatile Thread workerThread;

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
    public synchronized void close() {
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
        register(ring, dir, null);
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
        synchronized (lock) {
            rings.add(new RingEntry(ring, dir, watermark));
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
        if (find < 0) {
            LOG.warn("scanMaxGeneration could not enumerate {}; "
                    + "next spare may collide with an existing on-disk segment", dir);
            return max;
        }
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
                MmapSegment spare = null;
                String path = null;
                boolean installed = false;
                try {
                    // baseSeq is provisional — SegmentRing.appendOrFsn calls
                    // rebaseSeq() at rotation time to pin the real value. We
                    // pass the manager's best guess (nextSeqHint at this
                    // instant), which is fine since it's overwritten anyway.
                    if (memoryMode) {
                        spare = MmapSegment.createInMemory(e.ring.nextSeqHint(), segmentSizeBytes);
                    } else {
                        path = nextSparePath(e.dir);
                        spare = MmapSegment.create(path, e.ring.nextSeqHint(), segmentSizeBytes);
                    }
                    // Install + commit atomically under the manager lock.
                    // If `e.ring` was deregistered between the snapshot
                    // above and now, abandoning the spare here is the only
                    // way to keep totalBytes consistent: deregister already
                    // subtracted ring.totalSegmentBytes() (without the
                    // spare, since it wasn't installed yet) so a commit at
                    // this point would inflate totalBytes by one segment
                    // with no future subtractor. By holding `lock` across
                    // installHotSpare AND the += commit AND the still-
                    // registered check, deregister is forced to either
                    // observe the spare in the ring (and subtract it) or
                    // run before installation (so no install happens).
                    synchronized (lock) {
                        boolean stillRegistered = false;
                        for (int i = 0, n = rings.size(); i < n; i++) {
                            if (rings.get(i) == e) {
                                stillRegistered = true;
                                break;
                            }
                        }
                        if (stillRegistered) {
                            e.ring.installHotSpare(spare);
                            totalBytes += segmentSizeBytes;
                            installed = true;
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
                    // Remove the file even when spare is null (i.e. when
                    // MmapSegment.create itself threw): MmapSegment.create's
                    // catch already best-effort removes, but if anything
                    // before mmap (e.g. an exception thrown by the JVM
                    // between openCleanRW and the try block) leaves a file
                    // on disk, this is the second-line defense. Repeated
                    // unlink on an already-removed path is a harmless no-op.
                    if (path != null) {
                        Files.remove(path);
                    }
                }
            }
        }

        // 2. Trim any segments that the ring says are fully acked. For
        //    memory-mode rings, "trim" is just close() (Unsafe.free) — no
        //    file to unlink.
        //
        //    drainTrimmable + the totalBytes commit run together under
        //    `lock` so deregister cannot observe the intermediate state
        //    where segments have left ring.sealedSegments but the worker
        //    has not yet subtracted their bytes. Two interleavings would
        //    otherwise drift the counter:
        //      (a) deregister snapshots ring.totalSegmentBytes() before
        //          drainTrimmable mutates the ring → deregister subtracts
        //          including the about-to-be-drained bytes, then this
        //          loop subtracts them again. Drift: -drained.
        //      (b) drainTrimmable runs first (without holding lock) so
        //          ring.totalSegmentBytes() at deregister-time is already
        //          short by the drained bytes; if the worker then skips
        //          the subtract on a stillRegistered=false check, those
        //          bytes are never accounted. Drift: +drained.
        //    Atomic drain+commit under lock collapses both windows: any
        //    deregister sees either the pre-drain ring (with everything
        //    still counted) or the post-drain ring with the worker's
        //    subtraction already applied. Mirrors the spare-install
        //    path's stillRegistered guard above.
        //
        //    munmap + unlink stay outside the lock — they can be slow
        //    and shouldn't block register/deregister or sibling rings.
        // Persist the current ackedFsn watermark BEFORE the trim runs.
        // On host crash between the persist and the unlinks below, the
        // segments survive and the watermark is correct. On crash AFTER
        // the unlinks but before next tick, the segments are gone and
        // the watermark is stale, but recovery clamps with
        // max(lowestSurvivingBaseSeq - 1, watermark) so either ordering
        // is safe. Memory-mode rings (and callers that didn't supply a
        // watermark) skip the write.
        // Persist only on advance to avoid pointless mmap stores when
        // ackedFsn is steady. The store is a single 8-byte put against
        // an already-mapped region -- no syscall, no allocation -- but
        // the gate keeps the dirty-page footprint minimal under
        // steady-state load with no new acks arriving.
        if (e.watermark != null) {
            long currentAck = e.ring.ackedFsn();
            if (currentAck > e.lastPersistedAck) {
                e.watermark.write(currentAck);
                e.lastPersistedAck = currentAck;
            }
        }
        ObjList<MmapSegment> trim;
        Runnable hook = beforeTrimSyncHook;
        if (hook != null) {
            hook.run();
        }
        synchronized (lock) {
            trim = e.ring.drainTrimmable();
            if (trim != null) {
                boolean stillRegistered = false;
                for (int i = 0, n = rings.size(); i < n; i++) {
                    if (rings.get(i) == e) {
                        stillRegistered = true;
                        break;
                    }
                }
                if (stillRegistered) {
                    for (int i = 0, n = trim.size(); i < n; i++) {
                        totalBytes -= trim.get(i).sizeBytes();
                    }
                }
                // else: deregister already subtracted these bytes via
                // ring.totalSegmentBytes() (the drained segments were
                // still in sealedSegments at that read), so subtracting
                // again here would double-count. The segments are still
                // ours to close + unlink below — drainTrimmable has
                // already transferred ownership.
            }
        }
        if (trim != null) {
            for (int i = 0, n = trim.size(); i < n; i++) {
                MmapSegment s = trim.get(i);
                String path = s.path();
                try {
                    s.close();
                    if (path != null && !Files.remove(path)) {
                        LOG.warn("Failed to unlink trimmed segment {}", path);
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
     * rotation). Pattern: {@code <dir>/sf-<gen:016x>.sfa}. Recovery
     * discovers segments by extension + header magic, not by filename.
     */
    private String nextSparePath(String dir) {
        pathSink.clear();
        pathSink.put(dir).put("/sf-");
        Numbers.appendHex(pathSink, fileGeneration.getAndIncrement(), true);
        pathSink.put(".sfa");
        return pathSink.toString();
    }

    private void workerLoop() {
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
            for (int i = 0, n = ringSnapshot.size(); i < n; i++) {
                if (!running) break;
                serviceRing(ringSnapshot.getQuick(i));
            }
            // Drop strong refs so a deregistered ring becomes collectable
            // before the next tick (otherwise the snapshot pins it for up
            // to pollNanos after deregister).
            ringSnapshot.clear();
            if (!running) break;
            LockSupport.parkNanos(pollNanos);
        }
    }

    private static final class RingEntry {
        final String dir;
        final SegmentRing ring;
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

        RingEntry(SegmentRing ring, String dir, AckWatermark watermark) {
            this.ring = ring;
            this.dir = dir;
            this.watermark = watermark;
        }
    }
}
