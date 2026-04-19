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

    /**
     * Per-column layout pool scoped to this buffer. Sized to the max column
     * count observed on this buffer across batches; layouts are reused.
     */
    final ObjList<QwpColumnLayout> layoutPool = new ObjList<>();
    final QwpColumnBatch batch = new QwpColumnBatch();
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
        if (required <= scratchCapacity) return;
        int newCap = scratchCapacity;
        while (newCap < required) newCap *= 2;
        scratchAddr = Unsafe.realloc(scratchAddr, scratchCapacity, newCap, MemoryTag.NATIVE_DEFAULT);
        scratchCapacity = newCap;
    }
}
