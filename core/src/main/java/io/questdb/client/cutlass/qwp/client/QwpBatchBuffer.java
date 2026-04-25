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

package io.questdb.client.cutlass.qwp.client;

import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;

/**
 * Pooled per-batch container owned by the client's I/O thread. A buffer holds a
 * native scratch region that a received {@code RESULT_BATCH} payload is memcpy'd
 * into, plus the per-column {@link QwpColumnLayout} pool used while decoding and
 * the {@link QwpColumnBatch} view that the user's handler sees.
 * <p>
 * Lifecycle: I/O thread takes a buffer from the free pool -> copies the frame
 * payload in -> hands the decoder the buffer -> pushes the resulting batch onto
 * the event queue. User thread pops, invokes the handler, releases the buffer
 * back to the pool. While the user thread owns the buffer the I/O thread is
 * free to take a different buffer and decode the next frame.
 */
public class QwpBatchBuffer implements QuietCloseable {

    final QwpColumnBatch batch = new QwpColumnBatch();
    /**
     * Per-column layout pool scoped to this buffer. Sized to the max column
     * count observed on this buffer across batches; layouts are reused.
     */
    final ObjList<QwpColumnLayout> layoutPool = new ObjList<>();
    private int payloadLen;
    private long scratchAddr;
    private int scratchCapacity;

    public QwpBatchBuffer(int initialCapacity) {
        this.scratchCapacity = initialCapacity;
        this.scratchAddr = Unsafe.malloc(initialCapacity, MemoryTag.NATIVE_DEFAULT);
    }

    @Override
    public void close() {
        if (scratchAddr != 0) {
            Unsafe.free(scratchAddr, scratchCapacity, MemoryTag.NATIVE_DEFAULT);
            scratchAddr = 0;
            scratchCapacity = 0;
        }
        // Layouts own native entries buffers in non-delta SYMBOL mode. Free them
        // before the buffer itself is discarded so the allocations don't leak.
        for (int i = 0, n = layoutPool.size(); i < n; i++) {
            layoutPool.getQuick(i).close();
        }
        layoutPool.clear();
    }

    /**
     * Copies {@code len} bytes starting at {@code srcAddr} into this buffer's
     * native scratch, growing if needed. Call once per incoming frame before
     * handing the buffer to the decoder.
     */
    public void copyFromPayload(long srcAddr, int len) {
        ensureCapacity(len);
        Unsafe.getUnsafe().copyMemory(srcAddr, scratchAddr, len);
        payloadLen = len;
    }

    public int getPayloadLen() {
        return payloadLen;
    }

    public long getScratchAddr() {
        return scratchAddr;
    }

    private void ensureCapacity(int required) {
        if (required < 0) {
            // A negative request cannot be honoured. Reject loudly rather than
            // silently wrapping through the doubling loop.
            throw new IllegalArgumentException("QwpBatchBuffer required capacity must be non-negative: " + required);
        }
        if (required <= scratchCapacity) return;
        // Start the doubling at max(current, 1) so a buffer constructed with
        // initialCapacity=0 can still grow (0 *= 2 would spin forever). Cap the
        // double step against Integer.MAX_VALUE because `newCap *= 2` wraps
        // negative once newCap passes 2^30, at which point the while loop
        // could never reach `required` and would spin indefinitely.
        long newCap = Math.max(scratchCapacity, 1);
        while (newCap < required) {
            newCap <<= 1;
            if (newCap > Integer.MAX_VALUE) {
                newCap = Integer.MAX_VALUE;
                break;
            }
        }
        if (newCap < required) {
            throw new OutOfMemoryError("QwpBatchBuffer required capacity " + required
                    + " exceeds Integer.MAX_VALUE");
        }
        int capped = (int) newCap;
        scratchAddr = Unsafe.realloc(scratchAddr, scratchCapacity, capped, MemoryTag.NATIVE_DEFAULT);
        scratchCapacity = capped;
    }
}
