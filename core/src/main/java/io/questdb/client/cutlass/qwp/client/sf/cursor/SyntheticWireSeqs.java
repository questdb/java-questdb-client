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
 * Wire sequences of the commit-only frames the send loop synthesises.
 * <p>
 * A synthesised frame takes a wire sequence but belongs to no ring frame, so
 * the loop subtracts the count of such sequences at or below an acknowledged
 * wire sequence to keep the wire-sequence-to-FSN mapping exact. Sequences are
 * added in increasing order, and the entries an acknowledgement has passed
 * are trimmed while their count is kept; the backing list compacts as it
 * trims, so it stays bounded by the entries still in flight. Used by the I/O
 * thread only.
 */
public final class SyntheticWireSeqs {
    private final LongList seqs = new LongList();
    private int head;
    private long lastSeq = -1;
    private int size;
    private long trimmed;

    public void add(long wireSeq) {
        if (wireSeq <= lastSeq) {
            throw new IllegalStateException("synthetic wire seq must increase [last=" + lastSeq
                    + ", added=" + wireSeq + ']');
        }
        seqs.add(wireSeq);
        size++;
        lastSeq = wireSeq;
    }

    /**
     * Entries the backing list holds: the retained ones plus trimmed ones not
     * yet compacted away.
     */
    @TestOnly
    public int backingSize() {
        return size;
    }

    public void clear() {
        seqs.clear();
        head = 0;
        size = 0;
        trimmed = 0;
        lastSeq = -1;
    }

    public long countAtOrBelow(long wireSeq) {
        // first retained index whose value is above wireSeq
        int lo = head;
        int hi = size;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (seqs.getQuick(mid) <= wireSeq) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return trimmed + (lo - head);
    }

    @TestOnly
    public int retained() {
        return size - head;
    }

    public long total() {
        return trimmed + (size - head);
    }

    public void trimThrough(long wireSeq) {
        while (head < size && seqs.getQuick(head) <= wireSeq) {
            head++;
            trimmed++;
        }
        if (head == size) {
            seqs.clear();
            head = 0;
            size = 0;
        } else if (head >= size - head) {
            // An ack can always find the next synthesised commit in flight, so the
            // list may never empty. Once the trimmed prefix is at least as long as
            // the live tail, move the tail to the front: amortised O(1) per entry,
            // and the backing list stays within twice the live count.
            // LongList.clear() keeps the backing array, and copying forward never
            // overwrites an entry before it is read.
            int live = size - head;
            seqs.clear();
            for (int i = 0; i < live; i++) {
                seqs.add(seqs.getQuick(head + i));
            }
            head = 0;
            size = live;
        }
    }
}
