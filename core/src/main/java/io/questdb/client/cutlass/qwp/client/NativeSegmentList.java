/*
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
 */

package io.questdb.client.cutlass.qwp.client;

import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;

final class NativeSegmentList implements QuietCloseable {
    static final int ENTRY_SIZE = 16;

    private int capacity;
    private long ptr;
    private int size;
    private long totalLength;

    NativeSegmentList() {
        this.capacity = 16;
        this.ptr = Unsafe.malloc((long) capacity * ENTRY_SIZE, MemoryTag.NATIVE_DEFAULT);
    }

    @Override
    public void close() {
        if (ptr != 0) {
            Unsafe.free(ptr, (long) capacity * ENTRY_SIZE, MemoryTag.NATIVE_DEFAULT);
            ptr = 0;
            capacity = 0;
            size = 0;
            totalLength = 0;
        }
    }

    private void ensureCapacity(int required) {
        if (required <= capacity) {
            return;
        }

        int newCapacity = capacity;
        while (newCapacity < required) {
            if (newCapacity > Integer.MAX_VALUE / 2) {
                newCapacity = required;
                break;
            }
            newCapacity *= 2;
        }

        ptr = Unsafe.realloc(
                ptr,
                (long) capacity * ENTRY_SIZE,
                (long) newCapacity * ENTRY_SIZE,
                MemoryTag.NATIVE_DEFAULT
        );
        capacity = newCapacity;
    }

    void add(long address, long length) {
        if (length <= 0) {
            return;
        }
        ensureCapacity(size + 1);
        long segmentPtr = ptr + (long) size * ENTRY_SIZE;
        Unsafe.getUnsafe().putLong(segmentPtr, address);
        Unsafe.getUnsafe().putLong(segmentPtr + 8, length);
        size++;
        totalLength += length;
    }

    void appendFrom(NativeSegmentList other) {
        if (other.size == 0) {
            return;
        }
        ensureCapacity(size + other.size);
        Unsafe.getUnsafe().copyMemory(
                other.ptr,
                ptr + (long) size * ENTRY_SIZE,
                (long) other.size * ENTRY_SIZE
        );
        size += other.size;
        totalLength += other.totalLength;
    }

    long getAddress() {
        return ptr;
    }

    int getSegmentCount() {
        return size;
    }

    long getTotalLength() {
        return totalLength;
    }

    void reset() {
        size = 0;
        totalLength = 0;
    }
}
