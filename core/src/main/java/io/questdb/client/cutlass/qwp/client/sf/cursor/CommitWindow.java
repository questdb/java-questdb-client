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

/**
 * The commit window of the send loop: the bounds, the open commit group and
 * the pending close requests.
 * <p>
 * Frames the sender marks as window members go out deferred, so the server
 * commits them together. The window closes, and the next frame goes out
 * committing, when the row bound or the age bound is reached, or when a
 * thread asks for a commit through a given FSN. Every method except
 * {@link #configure} and {@link #requestCloseThrough} is for the I/O thread
 * only.
 */
public final class CommitWindow {
    public static final int CLOSED_BY_AGE = 1;
    public static final int CLOSED_BY_DEMAND = 2;
    public static final int CLOSED_BY_FRAME = 3;
    public static final int CLOSED_BY_ROWS = 4;
    public static final int DEFERRED = 5;
    public static final int NONE = 0;
    private volatile long closeRequestFsn = -1;
    private long committedThroughFsn = -1;
    private long groupOpenedNanos;
    private long groupRows;
    private long groupStartFsn = -1;
    private volatile long intervalNanos;
    private boolean isForeignDeferOpen;
    private volatile int rowBound;

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

    public long groupStartFsn() {
        return groupStartFsn;
    }

    public boolean isActive() {
        return rowBound > 0 || intervalNanos > 0;
    }

    public boolean isGroupOpen() {
        return groupStartFsn >= 0;
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
            clearGroup();
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
            clearGroup();
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
        clearGroup();
        committedThroughFsn = lastSentFsn;
        return close;
    }

    public void requestCloseThrough(long fsn) {
        // a single producer per sender calls this, so the check-then-write is safe
        if (fsn > closeRequestFsn) {
            closeRequestFsn = fsn;
        }
    }

    public void resetForConnection(long committedThroughFsn) {
        clearGroup();
        isForeignDeferOpen = false;
        this.committedThroughFsn = committedThroughFsn;
    }

    private void clearGroup() {
        groupStartFsn = -1;
        groupRows = 0;
    }

    private boolean isDemandPending() {
        return closeRequestFsn > committedThroughFsn;
    }
}
