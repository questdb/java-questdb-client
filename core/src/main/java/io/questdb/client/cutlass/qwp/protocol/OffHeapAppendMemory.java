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

package io.questdb.client.cutlass.qwp.protocol;

import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;

/**
 * Lightweight append-only off-heap buffer for columnar data storage.
 * <p>
 * This buffer provides typed append operations (putByte, putShort, etc.) backed by
 * native memory allocated via {@link Unsafe}. Memory is tracked under
 * {@link MemoryTag#NATIVE_ILP_RSS} for precise accounting.
 * <p>
 * Growth strategy: capacity doubles on each resize via {@link Unsafe#realloc}.
 */
public class OffHeapAppendMemory implements QuietCloseable {

    private static final int DEFAULT_INITIAL_CAPACITY = 128;

    private long pageAddress;
    private long appendAddress;
    private long capacity;

    public OffHeapAppendMemory() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public OffHeapAppendMemory(long initialCapacity) {
        this.capacity = Math.max(initialCapacity, 8);
        this.pageAddress = Unsafe.malloc(this.capacity, MemoryTag.NATIVE_ILP_RSS);
        this.appendAddress = pageAddress;
    }

    /**
     * Returns the append offset (number of bytes written).
     */
    public long getAppendOffset() {
        return appendAddress - pageAddress;
    }

    /**
     * Returns the base address of the buffer.
     */
    public long pageAddress() {
        return pageAddress;
    }

    /**
     * Returns the address at the given byte offset from the start.
     */
    public long addressOf(long offset) {
        return pageAddress + offset;
    }

    /**
     * Resets the append position to 0 without freeing memory.
     */
    public void truncate() {
        appendAddress = pageAddress;
    }

    /**
     * Sets the append position to the given byte offset.
     * Used for truncateTo operations on column buffers.
     */
    public void jumpTo(long offset) {
        appendAddress = pageAddress + offset;
    }

    public void putByte(byte value) {
        ensureCapacity(1);
        Unsafe.getUnsafe().putByte(appendAddress, value);
        appendAddress++;
    }

    public void putBoolean(boolean value) {
        putByte(value ? (byte) 1 : (byte) 0);
    }

    public void putShort(short value) {
        ensureCapacity(2);
        Unsafe.getUnsafe().putShort(appendAddress, value);
        appendAddress += 2;
    }

    public void putInt(int value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putInt(appendAddress, value);
        appendAddress += 4;
    }

    public void putLong(long value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putLong(appendAddress, value);
        appendAddress += 8;
    }

    public void putFloat(float value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putFloat(appendAddress, value);
        appendAddress += 4;
    }

    public void putDouble(double value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putDouble(appendAddress, value);
        appendAddress += 8;
    }

    /**
     * Advances the append position by the given number of bytes without writing.
     */
    public void skip(long bytes) {
        ensureCapacity(bytes);
        appendAddress += bytes;
    }

    @Override
    public void close() {
        if (pageAddress != 0) {
            Unsafe.free(pageAddress, capacity, MemoryTag.NATIVE_ILP_RSS);
            pageAddress = 0;
            appendAddress = 0;
            capacity = 0;
        }
    }

    private void ensureCapacity(long needed) {
        long used = appendAddress - pageAddress;
        if (used + needed > capacity) {
            long newCapacity = Math.max(capacity * 2, used + needed);
            pageAddress = Unsafe.realloc(pageAddress, capacity, newCapacity, MemoryTag.NATIVE_ILP_RSS);
            capacity = newCapacity;
            appendAddress = pageAddress + used;
        }
    }
}
