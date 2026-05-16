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

package io.questdb.client.cutlass.qwp.protocol;

import io.questdb.client.cutlass.line.array.ArrayBufferAppender;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.Vect;

/**
 * Lightweight append-only off-heap buffer for columnar data storage.
 * <p>
 * This buffer provides typed append operations (putByte, putShort, etc.) backed by
 * native memory allocated via {@link Unsafe}. Memory is tracked under
 * {@link MemoryTag#NATIVE_ILP_RSS} for precise accounting.
 * <p>
 * Growth strategy: capacity doubles on each resize via {@link Unsafe#realloc}.
 */
public class OffHeapAppendMemory implements ArrayBufferAppender, QuietCloseable {

    private static final int DEFAULT_INITIAL_CAPACITY = 128;
    private long appendAddress;
    private long capacity;
    private long pageAddress;

    public OffHeapAppendMemory() {
        this(DEFAULT_INITIAL_CAPACITY);
    }

    public OffHeapAppendMemory(long initialCapacity) {
        this.capacity = Math.max(initialCapacity, 8);
        this.pageAddress = Unsafe.malloc(this.capacity, MemoryTag.NATIVE_ILP_RSS);
        this.appendAddress = pageAddress;
    }

    /**
     * Returns the address at the given byte offset from the start.
     */
    public long addressOf(long offset) {
        return pageAddress + offset;
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

    /**
     * Returns the append offset (number of bytes written).
     */
    public long getAppendOffset() {
        return appendAddress - pageAddress;
    }

    /**
     * Sets the append position to the given byte offset.
     * Used for truncateTo operations on column buffers.
     */
    public void jumpTo(long offset) {
        assert offset >= 0 && offset <= getAppendOffset();
        appendAddress = pageAddress + offset;
    }

    /**
     * Returns the base address of the buffer.
     */
    public long pageAddress() {
        return pageAddress;
    }

    public void putBoolean(boolean value) {
        putByte(value ? (byte) 1 : (byte) 0);
    }

    public void putByte(byte value) {
        ensureCapacity(1);
        Unsafe.getUnsafe().putByte(appendAddress, value);
        appendAddress++;
    }

    /**
     * Appends a slice of a byte array verbatim. {@code offset} and {@code len}
     * are not bounds-checked against the array; callers ensure validity.
     */
    public void putBytes(byte[] value, int offset, int len) {
        if (len <= 0) {
            return;
        }
        ensureCapacity(len);
        Unsafe.getUnsafe().copyMemory(
                value, Unsafe.BYTE_OFFSET + offset, null, appendAddress, len);
        appendAddress += len;
    }

    public void putBlockOfBytes(long from, long len) {
        if (len <= 0) {
            return;
        }
        ensureCapacity(len);
        Vect.memcpy(appendAddress, from, len);
        appendAddress += len;
    }

    public void putDouble(double value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putDouble(appendAddress, value);
        appendAddress += 8;
    }

    public void putFloat(float value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putFloat(appendAddress, value);
        appendAddress += 4;
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

    public void putShort(short value) {
        ensureCapacity(2);
        Unsafe.getUnsafe().putShort(appendAddress, value);
        appendAddress += 2;
    }

    /**
     * Encodes a Java String to UTF-8 directly into the off-heap buffer.
     * Pre-ensures worst-case capacity to avoid per-byte checks.
     */
    public void putUtf8(CharSequence value) {
        if (value == null || value.length() == 0) {
            return;
        }
        int len = value.length();
        ensureCapacity((long) len * 4); // worst case: all supplementary chars
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i);
            if (c < 0x80) {
                Unsafe.getUnsafe().putByte(appendAddress++, (byte) c);
            } else if (c < 0x800) {
                Unsafe.getUnsafe().putByte(appendAddress++, (byte) (0xC0 | (c >> 6)));
                Unsafe.getUnsafe().putByte(appendAddress++, (byte) (0x80 | (c & 0x3F)));
            } else if (c >= 0xD800 && c <= 0xDBFF && i + 1 < len) {
                char c2 = value.charAt(++i);
                if (Character.isLowSurrogate(c2)) {
                    int codePoint = 0x10000 + ((c - 0xD800) << 10) + (c2 - 0xDC00);
                    Unsafe.getUnsafe().putByte(appendAddress++, (byte) (0xF0 | (codePoint >> 18)));
                    Unsafe.getUnsafe().putByte(appendAddress++, (byte) (0x80 | ((codePoint >> 12) & 0x3F)));
                    Unsafe.getUnsafe().putByte(appendAddress++, (byte) (0x80 | ((codePoint >> 6) & 0x3F)));
                    Unsafe.getUnsafe().putByte(appendAddress++, (byte) (0x80 | (codePoint & 0x3F)));
                } else {
                    Unsafe.getUnsafe().putByte(appendAddress++, (byte) '?');
                    i--;
                }
            } else if (Character.isSurrogate(c)) {
                Unsafe.getUnsafe().putByte(appendAddress++, (byte) '?');
            } else {
                Unsafe.getUnsafe().putByte(appendAddress++, (byte) (0xE0 | (c >> 12)));
                Unsafe.getUnsafe().putByte(appendAddress++, (byte) (0x80 | ((c >> 6) & 0x3F)));
                Unsafe.getUnsafe().putByte(appendAddress++, (byte) (0x80 | (c & 0x3F)));
            }
        }
    }

    /**
     * Advances the append position by the given number of bytes without writing.
     */
    public void skip(long bytes) {
        ensureCapacity(bytes);
        appendAddress += bytes;
    }

    /**
     * Resets the append position to 0 without freeing memory.
     */
    public void truncate() {
        appendAddress = pageAddress;
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
