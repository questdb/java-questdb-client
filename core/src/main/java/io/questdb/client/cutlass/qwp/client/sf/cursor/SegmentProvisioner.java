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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a ring's next segment on the producer thread, at rotation, charged
 * to the {@link SegmentBudget}. One per registered ring; producer-thread only
 * after {@link SegmentManager#register}.
 * <p>
 * Disk mode creates {@code <dir>/sf-<gen:016x>.sfa} with its final
 * {@code baseSeq} in the header, makes the header and the directory entry
 * durable, and only then hands the segment to the ring, whose manifest update
 * may name it. Memory mode allocates a heap-backed segment.
 * <p>
 * <b>Budget:</b> the charge is one atomic check-and-charge against everything
 * on the budget, side-file bytes included. Refused, the producer is
 * backpressured until ACK-driven trim releases segment bytes -- except below
 * the liveness floor: a ring holding fewer than {@link #MIN_LIVE_SEGMENTS}
 * segments is charged past the cap, because what fills the budget then is
 * something no ACK of this ring can shrink (its symbol dictionary, or the
 * segments of a sibling sender whose connection is down), and refusing would
 * stall the slot for good. The cap degrades by at most one segment per ring.
 * <p>
 * <b>Failure:</b> a failed create (disk full, I/O error) releases the charge
 * and backpressures the producer; the next attempt waits
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
    private static final Logger LOG = LoggerFactory.getLogger(SegmentProvisioner.class);
    private final SegmentBudget budget;
    // null = memory mode
    private final String dir;
    private final FilesFacade ff;
    private final long floorBytes;
    private final SegmentRing ring;
    private final long segmentSizeBytes;
    private long lastLogNs;
    private long nextGeneration;
    private long retryAtNanos;

    SegmentProvisioner(SegmentRing ring, String dir, long firstGeneration, long segmentSizeBytes,
                       SegmentBudget budget, FilesFacade ff) {
        this.ring = ring;
        this.dir = dir;
        this.nextGeneration = firstGeneration;
        this.segmentSizeBytes = segmentSizeBytes;
        this.budget = budget;
        this.ff = ff;
        // Clamp: segmentSizeBytes is user-supplied and only bounded below, so
        // a pathological value would wrap the product negative and make the
        // floor test trivially false.
        this.floorBytes = segmentSizeBytes > Long.MAX_VALUE / MIN_LIVE_SEGMENTS
                ? Long.MAX_VALUE
                : segmentSizeBytes * MIN_LIVE_SEGMENTS;
    }

    @Override
    public void close() {
        // Holds no segments: every one it created belongs to the ring.
    }

    @Override
    public void discard(MmapSegment segment) {
        budget.release(segmentSizeBytes);
        String path = dir == null ? null : segment.path();
        try {
            segment.close();
        } finally {
            if (path != null) {
                ff.remove(path);
            }
        }
    }

    @Override
    public MmapSegment next(long baseSeq) {
        long now = System.nanoTime();
        if (retryAtNanos != 0L && now - retryAtNanos < 0L) {
            return null;
        }
        if (!budget.tryCharge(segmentSizeBytes)) {
            if (ring.totalSegmentBytes() >= floorBytes) {
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
        String path = null;
        try {
            if (dir == null) {
                segment = MmapSegment.createInMemory(baseSeq, segmentSizeBytes);
            } else {
                // Generation-named, not baseSeq-named: recovery discovers
                // segments by extension and header magic, and a name that
                // never repeats within the directory is all unlink needs.
                path = dir + "/sf-" + String.format("%016x", nextGeneration++) + ".sfa";
                segment = MmapSegment.create(ff, path, baseSeq, segmentSizeBytes, true);
                // Header and directory entry durable before the ring's
                // manifest can name this segment.
                segment.syncHeader();
                if (ff.fsyncDir(dir) != 0) {
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
                ff.remove(path);
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
}
