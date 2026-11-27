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

import io.questdb.client.std.FilesFacade;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.str.DirectUtf8Sink;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.std.str.Utf8s;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * Supplies the segment a ring rotates into, charged to the
 * {@link SegmentBudget}. One per registered ring.
 * <p>
 * Disk mode creates {@code <dir>/sf-<gen:016x>.sfa}, makes its header and
 * directory entry durable, and only then hands it to the ring, whose manifest
 * update may name it. Memory mode allocates a heap-backed segment.
 * <p>
 * <b>Prefetch (disk mode):</b> after each rotation the {@link SegmentManager}
 * worker pre-creates the next segment off the producer thread
 * ({@link #prefetchWanted()}, {@link #createPrefetch}, {@link #installPrefetch})
 * and parks it in a one-slot handoff, so a rotation normally costs the
 * producer only the header rebase and the manifest update. The prefetch is an
 * optimisation, never a precondition: if the slot is empty at rotation, the
 * producer creates the segment itself in {@link #next} exactly as before. So a
 * prefetch the budget refuses, or whose create fails, waits for nothing and
 * schedules nothing: the worker retries a refused one on its next natural
 * wake-up and abandons a failed one until the next rotation asks again.
 * <p>
 * <b>Budget:</b> every charge is one atomic check-and-charge against
 * everything on the budget, side-file bytes included. A prefetch only ever
 * takes free room. The producer's own create is refused into backpressure until
 * ACK-driven trim releases segment bytes -- except below the liveness floor: a
 * ring holding fewer than {@link #MIN_LIVE_SEGMENTS} segments is charged past
 * the cap, because what fills the budget then is something no ACK of this ring
 * can shrink (its symbol dictionary, or the segments of a sibling sender whose
 * connection is down), and refusing would stall the slot for good. The cap
 * degrades by at most one segment per ring.
 * <p>
 * <b>Failure:</b> a failed producer create (disk full, I/O error) releases the
 * charge and backpressures the producer; the next attempt waits
 * {@link #CREATE_RETRY_NANOS}, so the producer's 50 µs backpressure poll does
 * not turn into a create storm. Logged at most once per
 * {@link #LOG_THROTTLE_NANOS}.
 */
public final class SegmentProvisioner implements SegmentSource {

    /** Wait between create attempts after a failed one. */
    public static final long CREATE_RETRY_NANOS = 10_000_000L; // 10 ms
    public static final long LOG_THROTTLE_NANOS = 30_000_000_000L; // 30 s
    // Active segment plus the one being rotated into: below this a producer
    // cannot advance at all.
    static final int MIN_LIVE_SEGMENTS = 2;
    // Prefetch slot state after close(): nothing can be installed any more.
    private static final Object CLOSED = new Object();
    private static final Logger LOG = LoggerFactory.getLogger(SegmentProvisioner.class);
    private static final AtomicReferenceFieldUpdater<SegmentProvisioner, Object> PREFETCHED =
            AtomicReferenceFieldUpdater.newUpdater(SegmentProvisioner.class, Object.class, "prefetched");
    private final SegmentBudget budget;
    // null = memory mode
    private final String dir;
    private final FilesFacade ff;
    // Shared by the producer's own creates and the worker's prefetches, so a
    // generation is never minted twice. Generations are unique, not ordered by
    // baseSeq: a prefetch minted before a producer fallback create is consumed
    // by the rotation after it.
    private final AtomicLong nextGeneration;
    // Producer-owned, sized at registration for the full UTF-8 filename and
    // NUL terminator. Ring close frees it after manager/I/O quiescence.
    private final DirectUtf8Sink pathScratch;
    private final SegmentRing ring;
    private final long segmentSizeBytes;
    private long lastLogNs;
    // Worker-thread only.
    private long lastPrefetchLogNs;
    // The one-slot handoff from the worker to the producer: null (empty), the
    // pre-created MmapSegment, or CLOSED. The worker installs only into an
    // empty slot and the producer takes only a segment, both by CAS; close()
    // swaps in CLOSED, so an install racing the ring's close can never leave
    // a mapping, fd or file behind. Still charged while it sits here, and
    // counted by heldBytes() so a deregister releases it.
    private volatile Object prefetched;
    // Set by the producer after each committed rotation (disk mode), cleared
    // by the worker when it starts a prefetch: the ring could use a
    // pre-created successor.
    private volatile boolean prefetchRequested;
    // A segment handed back by discard(): the rotation's manifest update
    // failed after its record may already have reached the page cache, so
    // the file must stay (recovery would otherwise find the manifest naming
    // a base with no segment and quarantine the slot). Still charged, and
    // counted by heldBytes() so a deregister releases it; the next rotation
    // at the same base reuses it. Volatile: deregister reads it from the
    // closing thread.
    private volatile MmapSegment retained;
    private long retryAtNanos;

    SegmentProvisioner(SegmentRing ring, String dir, long firstGeneration, long segmentSizeBytes,
                       SegmentBudget budget, FilesFacade ff) {
        this.ring = ring;
        this.dir = dir;
        this.nextGeneration = new AtomicLong(firstGeneration);
        this.segmentSizeBytes = segmentSizeBytes;
        this.budget = budget;
        this.ff = ff;
        this.pathScratch = dir == null ? null : new DirectUtf8Sink(Utf8s.utf8Bytes(dir) + 25L);
    }

    @Override
    public void close() {
        // Every segment it created belongs to the ring, except a retained or
        // prefetched one. A retained file stays: an empty segment at a base the
        // manifest may name, which recovery accepts either way. A prefetched
        // file is never named by the manifest, so it goes. Neither charge is
        // released here: SegmentManager.deregister already released both as
        // part of the ring's totalSegmentBytes() (see heldBytes()).
        try {
            Object p = PREFETCHED.getAndSet(this, CLOSED);
            if (p instanceof MmapSegment) {
                MmapSegment segment = (MmapSegment) p;
                try {
                    segment.close();
                } finally {
                    ff.remove(segment.path());
                }
            }
        } finally {
            try {
                if (retained != null) {
                    MmapSegment r = retained;
                    retained = null;
                    r.close();
                }
            } finally {
                if (pathScratch != null) {
                    pathScratch.close();
                }
            }
        }
    }

    @Override
    public void discard(MmapSegment segment) {
        retained = segment;
    }

    @Override
    public long heldBytes() {
        long held = 0L;
        MmapSegment r = retained;
        if (r != null) {
            held += r.sizeBytes();
        }
        Object p = prefetched;
        if (p instanceof MmapSegment) {
            held += ((MmapSegment) p).sizeBytes();
        }
        return held;
    }

    @Override
    public MmapSegment next(long baseSeq) {
        if (retained != null) {
            MmapSegment r = retained;
            retained = null;
            return r;
        }
        Object p = prefetched;
        if (p instanceof MmapSegment && PREFETCHED.compareAndSet(this, p, null)) {
            // Its base was a guess; the ring rebases it and makes the header
            // durable before the manifest names it.
            return (MmapSegment) p;
        }
        long now = System.nanoTime();
        if (retryAtNanos != 0L && now - retryAtNanos < 0L) {
            return null;
        }
        if (!budget.tryCharge(segmentSizeBytes)) {
            // O(1): this runs on every 50 us backpressure poll while the cap refuses.
            if (ring.liveSegmentCount() >= MIN_LIVE_SEGMENTS) {
                if (now - lastLogNs >= LOG_THROTTLE_NANOS) {
                    lastLogNs = now;
                    long sideFileBytes = budget.getSideFileBytes();
                    LOG.warn("SF {}: cannot create next segment in {} "
                                    + "(totalBytes={}, sideFileBytes={}, cap={}, segmentSize={}). "
                                    + "Producer is backpressured until ACK-driven trim frees segment "
                                    + "space; side-file bytes are not reclaimed by trim.",
                            dir == null ? "memory cap reached" : "disk-full",
                            dir == null ? "<memory>" : dir, budget.getSegmentBytes() + sideFileBytes,
                            sideFileBytes, budget.getCapacityBytes(), segmentSizeBytes);
                }
                return null;
            }
            budget.charge(segmentSizeBytes);
            if (now - lastLogNs >= LOG_THROTTLE_NANOS) {
                lastLogNs = now;
                long sideFileBytes = budget.getSideFileBytes();
                LOG.warn("SF {}: creating a segment past sf_max_total_bytes to keep the slot "
                                + "usable (totalBytes={}, sideFileBytes={}, cap={}, segmentSize={}). "
                                + "The rest of the budget -- symbol dictionaries, which trim cannot "
                                + "reclaim, and the segments of other senders sharing it -- leaves no "
                                + "room for this slot's active segment plus the next one; raise "
                                + "sf_max_total_bytes, or reduce symbol cardinality or the number of "
                                + "pooled senders.",
                        dir == null ? "<memory>" : dir, budget.getSegmentBytes() + sideFileBytes,
                        sideFileBytes, budget.getCapacityBytes(), segmentSizeBytes);
            }
        }
        MmapSegment segment = null;
        try {
            if (dir == null) {
                segment = MmapSegment.createInMemory(baseSeq, segmentSizeBytes);
            } else {
                // Generation-named, not baseSeq-named: recovery discovers
                // segments by extension and header magic, and a name that
                // never repeats within the directory is all unlink needs.
                long generation = nextGeneration.getAndIncrement();
                pathScratch.clear();
                pathScratch.put(dir).putAscii("/sf-");
                Numbers.appendHex(pathScratch, generation, true);
                pathScratch.putAscii(".sfa").putAscii('\0');
                segment = MmapSegment.createGenerated(ff, pathScratch.ptr(), dir, generation, baseSeq, segmentSizeBytes);
                // Header and directory entry durable before the ring's
                // manifest can name this segment.
                segment.syncHeader();
                pathScratch.clear();
                pathScratch.put(dir).putAscii('\0');
                if (ff.fsyncDir(pathScratch.ptr()) != 0) {
                    throw new MmapSegmentException("could not sync segment directory " + dir);
                }
            }
            retryAtNanos = 0L;
            return segment;
        } catch (Throwable t) {
            budget.release(segmentSizeBytes);
            if (segment != null) {
                try {
                    segment.close();
                } catch (Throwable ignored) {
                }
                // create() removes its own file when it throws; only a
                // segment that exists needs unlinking here.
                if (dir != null) {
                    ff.remove(segment.path());
                }
            }
            retryAtNanos = now + CREATE_RETRY_NANOS;
            if (now - lastLogNs >= LOG_THROTTLE_NANOS) {
                lastLogNs = now;
                LOG.warn("Failed to create next segment in {}; producer is backpressured, retrying every {} ms",
                        dir == null ? "<memory>" : dir, CREATE_RETRY_NANOS / 1_000_000L, t);
            }
            return null;
        }
    }

    /**
     * Producer thread, after a committed rotation: have the worker pre-create
     * the segment the next rotation needs. Not after a failed one, whose
     * segment is retained for the retry.
     */
    @Override
    public void rotated() {
        if (dir != null) {
            prefetchRequested = true;
        }
    }

    /**
     * Worker thread: releases a prefetched segment that could not be
     * installed (the ring was deregistered or closed meanwhile): its charge,
     * mapping and file.
     */
    void abandonPrefetch(MmapSegment segment) {
        budget.release(segmentSizeBytes);
        try {
            segment.close();
        } finally {
            ff.remove(segment.path());
        }
    }

    /**
     * Worker thread: creates the segment the next rotation needs, charged to
     * the budget, with its header and directory entry durable. Returns
     * {@code null} when the budget has no room (the request stays pending for
     * the worker's next wake-up) or the create failed (the request is dropped
     * until the next rotation; the producer creates the segment itself).
     * {@code provisionalBase} is only a guess: the ring rebases the segment at
     * rotation.
     */
    MmapSegment createPrefetch(long provisionalBase) {
        if (!budget.tryCharge(segmentSizeBytes)) {
            return null;
        }
        prefetchRequested = false;
        long generation = nextGeneration.getAndIncrement();
        // Allocating is fine here: this is the worker, once per segment.
        StringSink pathSink = new StringSink();
        pathSink.put(dir).put("/sf-");
        Numbers.appendHex(pathSink, generation, true);
        pathSink.put(".sfa");
        String path = pathSink.toString();
        MmapSegment segment = null;
        try {
            long pathPtr = ff.allocNativePath(path);
            try {
                segment = MmapSegment.createGenerated(ff, pathPtr, dir, generation, provisionalBase, segmentSizeBytes);
            } finally {
                ff.freeNativePath(pathPtr);
            }
            segment.syncHeader();
            if (ff.fsyncDir(dir) != 0) {
                throw new MmapSegmentException("could not sync segment directory " + dir);
            }
            return segment;
        } catch (Throwable t) {
            budget.release(segmentSizeBytes);
            if (segment != null) {
                try {
                    segment.close();
                } catch (Throwable ignored) {
                }
                ff.remove(path);
            }
            long now = System.nanoTime();
            if (now - lastPrefetchLogNs >= LOG_THROTTLE_NANOS) {
                lastPrefetchLogNs = now;
                LOG.warn("Failed to pre-create the next segment in {}; the producer creates it at rotation", dir, t);
            }
            return null;
        }
    }

    /**
     * Worker thread: hands a {@link #createPrefetch} result to the producer.
     * Returns {@code false}, leaving the segment with the caller, when the
     * slot is taken or the provisioner is closed.
     */
    boolean installPrefetch(MmapSegment segment) {
        return PREFETCHED.compareAndSet(this, null, segment);
    }

    /**
     * Worker thread: true when the ring has asked for a pre-created successor
     * and nothing occupies the slot. Never in memory mode, whose segments cost
     * a malloc, and never while a failed rotation's segment is retained: that
     * one is next in line.
     */
    boolean prefetchWanted() {
        return dir != null && prefetchRequested && prefetched == null && retained == null;
    }
}
