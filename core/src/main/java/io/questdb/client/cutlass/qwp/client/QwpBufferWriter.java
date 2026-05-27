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

import io.questdb.client.cutlass.line.array.ArrayBufferAppender;

/**
 * Buffer writer interface for QWP v1 message encoding.
 * <p>
 * This interface extends {@link ArrayBufferAppender} with additional methods
 * required for encoding QWP v1 messages, including varint encoding, string
 * handling, and buffer manipulation.
 * <p>
 * Implementations include:
 * <ul>
 *   <li>{@link NativeBufferWriter} - standalone native memory buffer</li>
 *   <li>{@link io.questdb.client.cutlass.http.client.WebSocketSendBuffer} - WebSocket frame buffer</li>
 * </ul>
 * <p>
 * All multi-byte values are written in little-endian format.
 */
public interface QwpBufferWriter extends ArrayBufferAppender {

    /**
     * Ensures the buffer has capacity for at least the specified
     * additional bytes beyond the current position.
     *
     * @param additionalBytes number of additional bytes needed
     */
    void ensureCapacity(int additionalBytes);

    /**
     * Returns the native memory pointer to the buffer start.
     * <p>
     * The returned pointer is valid until the next buffer growth operation.
     * Use with care and only for reading completed data.
     */
    long getBufferPtr();

    /**
     * Returns the current buffer capacity in bytes.
     */
    int getCapacity();

    /**
     * Returns the current write position (number of bytes written).
     */
    int getPosition();

    /**
     * Returns the native address where the next write will go.
     * <p>
     * Unlike {@code getBufferPtr() + getPosition()}, this method returns
     * the correct write address for all buffer implementations, including
     * segmented buffers where {@link #getPosition()} is a global offset.
     * <p>
     * The returned pointer is valid until the next buffer growth or flush.
     */
    long getWriteAddress();

    /**
     * Returns the number of bytes available for writing at
     * {@link #getWriteAddress()}.
     */
    int getWritableBytes();

    /**
     * Patches a byte value at the specified offset in the buffer.
     *
     * @param offset the byte offset from buffer start
     * @param value  the byte value to write
     */
    void patchByte(int offset, byte value);

    /**
     * Patches an int value at the specified offset in the buffer.
     * <p>
     * Used for updating length fields after writing content.
     *
     * @param offset the byte offset from buffer start
     * @param value  the int value to write
     */
    void patchInt(int offset, int value);

    /**
     * Writes a float (4 bytes, little-endian).
     */
    void putFloat(float value);

    /**
     * Writes a short (2 bytes, little-endian).
     */
    void putShort(short value);

    /**
     * Writes a length-prefixed UTF-8 string.
     * <p>
     * Format: varint length + UTF-8 bytes
     *
     * @param value the string to write (may be null or empty)
     */
    void putString(CharSequence value);

    /**
     * Writes UTF-8 encoded bytes directly without length prefix.
     *
     * @param value the string to encode (may be null or empty)
     */
    void putUtf8(CharSequence value);

    /**
     * Writes an unsigned variable-length integer (LEB128 encoding).
     * <p>
     * Each byte contains 7 bits of data with the high bit indicating
     * whether more bytes follow.
     */
    void putVarint(long value);

    /**
     * Resets the buffer for reuse, setting the position to 0.
     * <p>
     * Does not deallocate memory.
     */
    void reset();

    /**
     * Skips the specified number of bytes, advancing the position.
     * <p>
     * Used when data has been written directly to the buffer via
     * {@link #getBufferPtr()}.
     *
     * @param bytes number of bytes to skip
     */
    void skip(int bytes);
}
