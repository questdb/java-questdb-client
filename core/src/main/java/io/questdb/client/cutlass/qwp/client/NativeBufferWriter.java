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
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.Utf8s;

/**
 * A simple native memory buffer writer for encoding QWP v1 messages.
 * <p>
 * This class provides write methods similar to HttpClient.Request but writes
 * to a native memory buffer that can be sent over WebSocket.
 * <p>
 * All multi-byte values are written in little-endian format unless otherwise specified.
 */
public class NativeBufferWriter implements QwpBufferWriter, QuietCloseable {

    private static final int DEFAULT_CAPACITY = 8192;

    private long bufferPtr;
    private int capacity;
    private int position;

    public NativeBufferWriter() {
        this(DEFAULT_CAPACITY);
    }

    public NativeBufferWriter(int initialCapacity) {
        this.capacity = initialCapacity;
        this.bufferPtr = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
        this.position = 0;
    }

    /**
     * Returns the UTF-8 encoded length of a string.
     *
     * @param s the string (may be null)
     * @return the number of bytes needed to encode the string as UTF-8
     */
    public static int utf8Length(CharSequence s) {
        return s == null ? 0 : Utf8s.utf8Bytes(s);
    }

    /**
     * Returns the number of bytes required to encode {@code value} as an
     * unsigned LEB128 varint.
     */
    public static int varintSize(long value) {
        if (value == 0) {
            return 1;
        }
        return (64 - Long.numberOfLeadingZeros(value) + 6) / 7;
    }

    @Override
    public void close() {
        if (bufferPtr != 0) {
            Unsafe.free(bufferPtr, capacity, MemoryTag.NATIVE_DEFAULT);
            bufferPtr = 0;
        }
    }

    /**
     * Ensures the buffer has at least the specified additional capacity.
     *
     * @param needed additional bytes needed beyond current position
     */
    @Override
    public void ensureCapacity(int needed) {
        if ((long) position + needed > capacity) {
            long required = (long) position + needed;
            long doubled = (long) capacity * 2;
            long newCapacity = Math.max(doubled, required);
            if (newCapacity > Integer.MAX_VALUE) {
                throw new OutOfMemoryError("NativeBufferWriter capacity overflow: " + newCapacity);
            }
            int cap = (int) newCapacity;
            bufferPtr = Unsafe.realloc(bufferPtr, capacity, cap, MemoryTag.NATIVE_DEFAULT);
            capacity = cap;
        }
    }

    /**
     * Returns the buffer pointer.
     */
    @Override
    public long getBufferPtr() {
        return bufferPtr;
    }

    /**
     * Returns the current buffer capacity.
     */
    @Override
    public int getCapacity() {
        return capacity;
    }

    /**
     * Returns the current write position (number of bytes written).
     */
    @Override
    public int getPosition() {
        return position;
    }

    @Override
    public long getWriteAddress() {
        return bufferPtr + position;
    }

    @Override
    public int getWritableBytes() {
        return capacity - position;
    }

    @Override
    public void patchByte(int offset, byte value) {
        Unsafe.getUnsafe().putByte(bufferPtr + offset, value);
    }

    /**
     * Patches an int value at the specified offset.
     * Used for updating length fields after writing content.
     */
    @Override
    public void patchInt(int offset, int value) {
        Unsafe.getUnsafe().putInt(bufferPtr + offset, value);
    }

    /**
     * Writes a block of bytes from native memory.
     */
    @Override
    public void putBlockOfBytes(long from, long len) {
        if (len < 0 || len > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("len exceeds int range: " + len);
        }
        int intLen = (int) len;
        ensureCapacity(intLen);
        Unsafe.getUnsafe().copyMemory(from, bufferPtr + position, intLen);
        position += intLen;
    }

    /**
     * Writes a single byte.
     */
    @Override
    public void putByte(byte value) {
        ensureCapacity(1);
        Unsafe.getUnsafe().putByte(bufferPtr + position, value);
        position++;
    }

    /**
     * Writes a double (8 bytes, little-endian).
     */
    @Override
    public void putDouble(double value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putDouble(bufferPtr + position, value);
        position += 8;
    }

    /**
     * Writes a float (4 bytes, little-endian).
     */
    @Override
    public void putFloat(float value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putFloat(bufferPtr + position, value);
        position += 4;
    }

    /**
     * Writes an int (4 bytes, little-endian).
     */
    @Override
    public void putInt(int value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putInt(bufferPtr + position, value);
        position += 4;
    }

    /**
     * Writes a long (8 bytes, little-endian).
     */
    @Override
    public void putLong(long value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putLong(bufferPtr + position, value);
        position += 8;
    }

    /**
     * Writes a short (2 bytes, little-endian).
     */
    @Override
    public void putShort(short value) {
        ensureCapacity(2);
        Unsafe.getUnsafe().putShort(bufferPtr + position, value);
        position += 2;
    }

    /**
     * Writes a length-prefixed UTF-8 string.
     */
    @Override
    public void putString(CharSequence value) {
        if (value == null || value.length() == 0) {
            putVarint(0);
            return;
        }

        int charLen = value.length();
        // Optimistic: assume ASCII (utf8Len == charLen).
        // Reserve varint(charLen) + charLen bytes.
        int varintLen = varintSize(charLen);
        ensureCapacity(varintLen + charLen);

        // Single-pass: write ASCII bytes directly after varint space
        long varintAddr = bufferPtr + position;
        long addr = varintAddr + varintLen;
        int i = 0;
        for (; i < charLen; i++) {
            char c = value.charAt(i);
            if (c >= 0x80) {
                break;
            }
            Unsafe.getUnsafe().putByte(addr++, (byte) c);
        }

        if (i == charLen) {
            // All ASCII — write varint prefix, done in a single pass
            writeVarintDirect(varintAddr, charLen);
            position += varintLen + charLen;
        } else {
            // Non-ASCII — fall back to two-pass
            int utf8Len = utf8Length(value);
            putVarint(utf8Len);
            ensureCapacity(utf8Len);
            encodeUtf8(value, utf8Len);
        }
    }

    /**
     * Writes UTF-8 bytes directly without length prefix.
     */
    @Override
    public void putUtf8(CharSequence value) {
        if (value == null || value.length() == 0) {
            return;
        }

        int charLen = value.length();
        ensureCapacity(charLen);

        // Single-pass: try ASCII encoding
        long addr = bufferPtr + position;
        int i = 0;
        for (; i < charLen; i++) {
            char c = value.charAt(i);
            if (c >= 0x80) {
                break;
            }
            Unsafe.getUnsafe().putByte(addr++, (byte) c);
        }

        if (i == charLen) {
            // All ASCII — done in a single pass
            position += charLen;
        } else {
            int utf8Len = Utf8s.utf8Bytes(value, i, charLen);
            ensureCapacity(i + utf8Len);
            position += i + Utf8s.strCpyUtf8(value, i, bufferPtr + position + i, utf8Len);
        }
    }

    /**
     * Writes a varint (unsigned LEB128).
     */
    @Override
    public void putVarint(long value) {
        ensureCapacity(10); // max varint bytes
        long addr = bufferPtr + position;
        while (value > 0x7F) {
            Unsafe.getUnsafe().putByte(addr++, (byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        Unsafe.getUnsafe().putByte(addr++, (byte) value);
        position = (int) (addr - bufferPtr);
    }

    /**
     * Resets the buffer for reuse.
     */
    @Override
    public void reset() {
        position = 0;
    }

    /**
     * Skips the specified number of bytes, advancing the position.
     * Used when data has been written directly to the buffer via getBufferPtr().
     *
     * @param bytes number of bytes to skip
     */
    @Override
    public void skip(int bytes) {
        ensureCapacity(bytes);
        position += bytes;
    }

    private static void writeVarintDirect(long addr, long value) {
        while (value > 0x7F) {
            Unsafe.getUnsafe().putByte(addr++, (byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        Unsafe.getUnsafe().putByte(addr, (byte) value);
    }

    private void encodeUtf8(CharSequence value, int utf8Len) {
        position += Utf8s.strCpyUtf8(value, bufferPtr + position, utf8Len);
    }
}
