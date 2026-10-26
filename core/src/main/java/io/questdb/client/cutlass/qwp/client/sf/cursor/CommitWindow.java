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

import io.questdb.client.std.LongList;
import org.jetbrains.annotations.TestOnly;

/**
 * The commit window of the send loop: the bounds, the open commit group, the
 * pending close requests and the history of this connection's group spans.
 * <p>
 * Frames the sender marks as window members go out deferred, so the server
 * commits them together. The window closes, and the next frame goes out
 * committing, when the row bound or the age bound is reached, or when a
 * thread asks for a commit through a given FSN.
 * <p>
 * A group's span runs from its first member to the frame that committed it:
 * the member or unmarked frame that closed it, or the last frame sent before
 * a synthesised commit. A rejection arrives a round trip after its frame, when
 * that frame's group may have closed and later groups opened, so the spans
 * stay recorded until an acknowledgement settles them ({@link #onAcked}) and
 * {@link #groupStartBefore} answers from the history. The backing list
 * compacts as spans settle, so it stays bounded by the spans in flight.
 * <p>
 * Every method except {@link #configure} and {@link #requestCloseThrough} is
 * for the I/O thread only.
 */
public final class CommitWindow {
    public static final int CLOSED_BY_AGE = 1;
    public static final int CLOSED_BY_DEMAND = 2;
    public static final int CLOSED_BY_FRAME = 3;
    public static final int CLOSED_BY_ROWS = 4;
    public static final int DEFERRED = 5;
    public static final int NONE = 0;
    // Closed spans of this connection not yet settled, as (start, end) FSN pairs
    // in increasing order, live from index spanHead to spanSize. The open group's
    // span is (groupStartFsn, open) and is not stored.
    private final LongList spans = new LongList();
    private volatile long closeRequestFsn = -1;
    private long committedThroughFsn = -1;
    private long groupOpenedNanos;
    private long groupRows;
    private long groupStartFsn = -1;
    private volatile long intervalNanos;
    private boolean isForeignDeferOpen;
    private volatile int rowBound;
    private int spanHead;
    private int spanSize;

    public void configure(int rowBound, long intervalMillis) {
        if (rowBound < 0) {
            throw new IllegalArgumentException("rowBound must not be negative [rowBound=" + rowBound + ']');
        }
        if (intervalMillis < 0) {
            throw new IllegalArgumentException("intervalMillis must not be negative [intervalMillis=" + intervalMillis + ']');
        }
        this.rowBound = rowBound;
        this.intervalNanos = intervalMillis * 1_000_000L;
    }

    /**
     * The first FSN of the recorded group whose span holds {@code fsn} after its
     * first frame, so that an uncommitted frame of the group precedes it on
     * this connection: the start {@code s} of the span with
     * {@code s < fsn <= end}, the end being open while the group is. -1 when
     * no such span exists.
     */
    public long groupStartBefore(long fsn) {
        if (isGroupOpen() && groupStartFsn < fsn) {
            return groupStartFsn;
        }
        // Spans are disjoint and ordered, and the open group starts after all of
        // them: find the last closed span that starts below fsn.
        int lo = 0;
        int hi = (spanSize - spanHead) >> 1;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (spans.getQuick(spanHead + 2 * mid) < fsn) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo == 0) {
            return -1;
        }
        int start = spanHead + 2 * (lo - 1);
        return fsn <= spans.getQuick(start + 1) ? spans.getQuick(start) : -1;
    }

    public long groupStartFsn() {
        return groupStartFsn;
    }

    public boolean isActive() {
        return rowBound > 0 || intervalNanos > 0;
    }

    public boolean isGroupOpen() {
        return groupStartFsn >= 0;
    }

    /**
     * Drops the closed spans whose end is at or below {@code ackedFsn}: the
     * server committed them, so a later rejection cannot fall inside one.
     */
    public void onAcked(long ackedFsn) {
        int head = spanHead;
        while (head < spanSize && spans.getQuick(head + 1) <= ackedFsn) {
            head += 2;
        }
        if (head == spanHead) {
            return;
        }
        if (head == spanSize) {
            spans.clear();
            spanHead = 0;
            spanSize = 0;
        } else if (head >= spanSize - head) {
            // Once the settled prefix is at least as long as the live tail, move
            // the tail to the front: amortised O(1) per span, and the backing list
            // stays within twice the live count. LongList.clear() keeps the
            // backing array, and copying forward never overwrites an entry before
            // it is read.
            int live = spanSize - head;
            spans.clear();
            for (int i = 0; i < live; i++) {
                spans.add(spans.getQuick(head + i));
            }
            spanHead = 0;
            spanSize = live;
        } else {
            spanHead = head;
        }
    }

    public int onMember(long fsn, long rows, long nowNanos) {
        long total = groupRows + rows;
        int close = NONE;
        if (rowBound > 0 && total >= rowBound) {
            close = CLOSED_BY_ROWS;
        } else if (isGroupOpen() && intervalNanos > 0 && nowNanos - groupOpenedNanos >= intervalNanos) {
            close = CLOSED_BY_AGE;
        } else if (isDemandPending() && fsn >= closeRequestFsn) {
            close = CLOSED_BY_DEMAND;
        }
        isForeignDeferOpen = false;
        if (close != NONE) {
            closeGroup(fsn);
            committedThroughFsn = fsn;
            return close;
        }
        if (!isGroupOpen()) {
            groupStartFsn = fsn;
            groupOpenedNanos = nowNanos;
        }
        groupRows = total;
        return DEFERRED;
    }

    public int onUnmarked(long fsn, boolean isDeferred) {
        if (isDeferred) {
            isForeignDeferOpen = true;
            return NONE;
        }
        committedThroughFsn = fsn;
        isForeignDeferOpen = false;
        if (isGroupOpen()) {
            closeGroup(fsn);
            return CLOSED_BY_FRAME;
        }
        return NONE;
    }

    public int pollQuiet(long nowNanos, long lastSentFsn) {
        if (!isGroupOpen() || isForeignDeferOpen) {
            return NONE;
        }
        int close;
        if (intervalNanos > 0 && nowNanos - groupOpenedNanos >= intervalNanos) {
            close = CLOSED_BY_AGE;
        } else if (isDemandPending() && lastSentFsn >= closeRequestFsn) {
            close = CLOSED_BY_DEMAND;
        } else {
            return NONE;
        }
        closeGroup(lastSentFsn);
        committedThroughFsn = lastSentFsn;
        return close;
    }

    public synchronized void requestCloseThrough(long fsn) {
        // The producer and a thread waiting for acks may both ask; the lock keeps
        // the request monotonic. The I/O thread only reads the volatile.
        if (fsn > closeRequestFsn) {
            closeRequestFsn = fsn;
        }
    }

    public void resetForConnection(long committedThroughFsn) {
        clearGroup();
        isForeignDeferOpen = false;
        this.committedThroughFsn = committedThroughFsn;
        spans.clear();
        spanHead = 0;
        spanSize = 0;
    }

    /**
     * Closed spans the backing list holds: the unsettled ones plus settled ones
     * not yet compacted away.
     */
    @TestOnly
    public int spanBackingSize() {
        return spanSize >> 1;
    }

    private void clearGroup() {
        groupStartFsn = -1;
        groupRows = 0;
    }

    private void closeGroup(long endFsn) {
        if (isGroupOpen()) {
            spans.add(groupStartFsn);
            spans.add(endFsn);
            spanSize += 2;
        }
        clearGroup();
    }

    private boolean isDemandPending() {
        return closeRequestFsn > committedThroughFsn;
    }
}
