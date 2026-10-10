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

import io.questdb.client.std.ObjList;

import java.util.function.LongSupplier;

/**
 * The {@code sf_max_total_bytes} budget: an upper bound on the bytes the
 * cursor rings charged to it may hold -- every segment a ring owns (active,
 * sealed and hot spare), in memory or on disk, plus each slot's live
 * side-file bytes.
 * <p>
 * A {@code Sender} built on its own charges a private budget. A sender pool
 * hands one budget to every sender it builds, so {@code sf_max_total_bytes}
 * caps the pool as a whole: growing the pool adds connections, not buffer
 * memory. Each {@link SegmentManager} keeps its own worker thread and charges
 * the budget as it provisions and trims segments.
 * <p>
 * The cap is enforced when a manager provisions a hot spare; bytes a ring
 * already owns when it registers (a recovered slot) are charged even when
 * they exceed it. A ring below its minimum working set -- the active segment
 * plus one spare -- is provisioned past the cap, so a budget shared by
 * {@code n} rings can be exceeded by up to {@code n} such working sets.
 * <p>
 * Thread-safe. All state is guarded by this object's monitor, a leaf lock:
 * managers call in while holding their own lock, and nothing here calls back
 * into a manager. Side-file gauges are read under the monitor, so they must
 * be wait-free and must not throw.
 */
public final class SegmentBudget {

    private final long capacityBytes;
    // Live gauges of each charged slot's .symbol-dict side-file bytes. Read on
    // every cap check rather than folded into segmentBytes: a dictionary
    // grows out-of-band on producer threads, so an incremental mirror would
    // drift, while a live read cannot. Each gauge is a pair of volatile reads
    // (PersistedSymbolDict.occupiedDiskBytes()) and must stay wait-free: a
    // producer can hold its dictionary's monitor across ff.allocate and mmap,
    // and a gauge that took that monitor would stall every manager charging
    // this budget behind one producer's append I/O.
    private final ObjList<LongSupplier> sideFileGauges = new ObjList<>();
    private long segmentBytes;

    public SegmentBudget(long capacityBytes) {
        if (capacityBytes <= 0) {
            throw new IllegalArgumentException("capacityBytes must be positive: " + capacityBytes);
        }
        this.capacityBytes = capacityBytes;
    }

    /**
     * Segment bytes plus live side-file bytes currently charged.
     */
    public synchronized long getAccountedBytes() {
        return segmentBytes + sideFileBytes();
    }

    public long getCapacityBytes() {
        return capacityBytes;
    }

    public synchronized long getSegmentBytes() {
        return segmentBytes;
    }

    public synchronized long getSideFileBytes() {
        return sideFileBytes();
    }

    synchronized void addSideFileGauge(LongSupplier gauge) {
        sideFileGauges.add(gauge);
    }

    /**
     * Charges {@code bytes} unconditionally: segments a ring already owns when
     * it registers, and spares provisioned under the liveness floor.
     */
    synchronized void charge(long bytes) {
        segmentBytes += bytes;
    }

    synchronized void release(long bytes) {
        segmentBytes -= bytes;
    }

    // Identity, not equals(): each registration hands in its own gauge instance.
    synchronized void removeSideFileGauge(LongSupplier gauge) {
        for (int i = 0, n = sideFileGauges.size(); i < n; i++) {
            if (sideFileGauges.getQuick(i) == gauge) {
                sideFileGauges.remove(i);
                return;
            }
        }
    }

    /**
     * Charges {@code bytes} if they fit under the capacity together with
     * everything already charged and the live side-file bytes. Check and charge
     * are one atomic step, so managers sharing the budget can never both take
     * its last free segment.
     */
    synchronized boolean tryCharge(long bytes) {
        if (segmentBytes + sideFileBytes() + bytes > capacityBytes) {
            return false;
        }
        segmentBytes += bytes;
        return true;
    }

    private long sideFileBytes() {
        long total = 0L;
        for (int i = 0, n = sideFileGauges.size(); i < n; i++) {
            total += sideFileGauges.getQuick(i).getAsLong();
        }
        return total;
    }
}
