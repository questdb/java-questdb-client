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

package io.questdb.client.cutlass.http.client;

import io.questdb.client.cutlass.line.array.ArrayBufferAppender;
import io.questdb.client.cutlass.qwp.client.NativeBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.websocket.WebSocketFrameWriter;
import io.questdb.client.cutlass.qwp.websocket.WebSocketOpcode;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.SecureRnd;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.Vect;
import io.questdb.client.std.str.Utf8s;

/**
 * Zero-GC WebSocket send buffer that implements {@link ArrayBufferAppender} for direct
 * payload writing. Manages native memory with safe growth and handles WebSocket frame
 * building (reserve header -> write payload -> patch header -> mask).
 * <p>
 * Usage pattern:
 * <pre>
 * buffer.beginBinaryFrame();
 * // Write payload using ArrayBufferAppender methods
 * buffer.putLong(value);
 * buffer.putBlockOfBytes(ptr, len);
 * // Finish frame and get send info
 * FrameInfo frame = buffer.endBinaryFrame();
 * // Send frame using socket
 * socket.send(buffer.getBufferPtr() + frame.offset, frame.length);
 * buffer.reset();
 * </pre>
 * <p>
 * Thread safety: This class is NOT thread-safe. Each connection should have its own buffer.
 */
public class WebSocketSendBuffer implements QwpBufferWriter, QuietCloseable {

    private static final int DEFAULT_INITIAL_CAPACITY = 65536;
    private static final int MAX_BUFFER_SIZE = Integer.MAX_VALUE - 8; // Leave room for alignment
    // Maximum header size: 2 (base) + 8 (64-bit length) + 4 (mask key)
    private static final int MAX_HEADER_SIZE = 14;
    private final FrameInfo frameInfo = new FrameInfo();
    private final int maxBufferSize;
    private final SecureRnd rnd;
    private int bufCapacity;
    private long bufPtr;
    private int frameStartOffset;   // Where current frame's reserved header starts
    private int payloadStartOffset; // Where payload begins (frameStart + MAX_HEADER_SIZE)
    private int writePos;           // Current write position (offset from bufPtr)

    /**
     * Creates a new WebSocket send buffer with default initial capacity.
     */
    public WebSocketSendBuffer() {
        this(DEFAULT_INITIAL_CAPACITY, MAX_BUFFER_SIZE);
    }

    /**
     * Creates a new WebSocket send buffer with specified initial capacity.
     *
     * @param initialCapacity initial buffer size in bytes
     */
    public WebSocketSendBuffer(int initialCapacity) {
        this(initialCapacity, MAX_BUFFER_SIZE);
    }

    /**
     * Creates a new WebSocket send buffer with specified initial and max capacity.
     *
     * @param initialCapacity initial buffer size in bytes
     * @param maxBufferSize   maximum buffer size in bytes
     */
    public WebSocketSendBuffer(int initialCapacity, int maxBufferSize) {
        this.bufCapacity = Math.max(initialCapacity, MAX_HEADER_SIZE * 2);
        this.maxBufferSize = maxBufferSize;
        this.bufPtr = Unsafe.malloc(bufCapacity, MemoryTag.NATIVE_DEFAULT);
        try {
            this.rnd = new SecureRnd();
        } catch (Throwable t) {
            Unsafe.free(bufPtr, bufCapacity, MemoryTag.NATIVE_DEFAULT);
            bufPtr = 0;
            throw t;
        }
        this.writePos = 0;
        this.frameStartOffset = 0;
        this.payloadStartOffset = 0;
    }

    /**
     * Begins a new WebSocket frame. Reserves space for the maximum header size.
     * The opcode is specified later when ending the frame via {@link #endFrame(int)}.
     */
    public void beginFrame() {
        frameStartOffset = writePos;
        // Reserve maximum header space
        ensureCapacity(MAX_HEADER_SIZE);
        writePos += MAX_HEADER_SIZE;
        payloadStartOffset = writePos;
    }

    @Override
    public void close() {
        if (bufPtr != 0) {
            Unsafe.free(bufPtr, bufCapacity, MemoryTag.NATIVE_DEFAULT);
            bufPtr = 0;
            bufCapacity = 0;
        }
    }

    /**
     * Finishes the current binary frame, writing the header and applying masking.
     * Returns information about where to find the complete frame in the buffer.
     * <p>
     * IMPORTANT: Only call this after all payload writes are complete. The buffer
     * pointer is stable after this call (no more reallocations for this frame).
     *
     * @return frame info containing offset and length for sending
     */
    public FrameInfo endBinaryFrame() {
        return endFrame(WebSocketOpcode.BINARY);
    }

    /**
     * Finishes the current frame with the specified opcode.
     *
     * @param opcode the frame opcode
     * @return frame info containing offset and length for sending
     */
    public FrameInfo endFrame(int opcode) {
        int payloadLen = writePos - payloadStartOffset;

        // Calculate actual header size (with mask key for client frames)
        int actualHeaderSize = WebSocketFrameWriter.headerSize(payloadLen, true);
        int unusedSpace = MAX_HEADER_SIZE - actualHeaderSize;
        int actualFrameStart = frameStartOffset + unusedSpace;

        // Generate mask key
        int maskKey = rnd.nextInt();

        // Write header at actual position (after unused space)
        WebSocketFrameWriter.writeHeader(bufPtr + actualFrameStart, true, opcode, payloadLen, maskKey);

        // Apply mask to payload
        if (payloadLen > 0) {
            WebSocketFrameWriter.maskPayload(bufPtr + payloadStartOffset, payloadLen, maskKey);
        }

        return frameInfo.set(actualFrameStart, actualHeaderSize + payloadLen);
    }

    /**
     * Ensures the buffer has capacity for the specified number of additional bytes.
     * May reallocate the buffer if necessary.
     *
     * @param additionalBytes number of additional bytes needed
     */
    @Override
    public void ensureCapacity(int additionalBytes) {
        long requiredCapacity = (long) writePos + additionalBytes;
        if (requiredCapacity > bufCapacity) {
            grow(requiredCapacity);
        }
    }

    /**
     * Gets the buffer pointer. Only use this for reading after frame is complete.
     */
    public long getBufferPtr() {
        return bufPtr;
    }

    /**
     * Gets the current buffer capacity.
     */
    public int getCapacity() {
        return bufCapacity;
    }

    /**
     * Gets the current write position (number of bytes written).
     */
    @Override
    public int getPosition() {
        return writePos;
    }

    @Override
    public int getWritableBytes() {
        return bufCapacity - writePos;
    }

    @Override
    public long getWriteAddress() {
        return bufPtr + writePos;
    }

    /**
     * Gets the current write position (total bytes written since last reset).
     */
    public int getWritePos() {
        return writePos;
    }

    @Override
    public void patchByte(int offset, byte value) {
        Unsafe.getUnsafe().putByte(bufPtr + offset, value);
    }

    /**
     * Patches an int value at the specified offset.
     */
    @Override
    public void patchInt(int offset, int value) {
        Unsafe.getUnsafe().putInt(bufPtr + offset, value);
    }

    /**
     * Writes an ASCII string.
     */
    public void putAscii(CharSequence cs) {
        if (cs == null) {
            return;
        }
        int len = cs.length();
        ensureCapacity(len);
        for (int i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(bufPtr + writePos + i, (byte) cs.charAt(i));
        }
        writePos += len;
    }

    @Override
    public void putBlockOfBytes(long from, long len) {
        if (len <= 0) {
            return;
        }
        if (len > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("len exceeds int range: " + len);
        }
        int intLen = (int) len;
        ensureCapacity(intLen);
        Vect.memcpy(bufPtr + writePos, from, intLen);
        writePos += intLen;
    }

    @Override
    public void putByte(byte b) {
        ensureCapacity(1);
        Unsafe.getUnsafe().putByte(bufPtr + writePos, b);
        writePos++;
    }

    @Override
    public void putDouble(double value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putDouble(bufPtr + writePos, value);
        writePos += 8;
    }

    /**
     * Writes a float value.
     */
    public void putFloat(float value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putFloat(bufPtr + writePos, value);
        writePos += 4;
    }

    @Override
    public void putInt(int value) {
        ensureCapacity(4);
        Unsafe.getUnsafe().putInt(bufPtr + writePos, value);
        writePos += 4;
    }

    @Override
    public void putLong(long value) {
        ensureCapacity(8);
        Unsafe.getUnsafe().putLong(bufPtr + writePos, value);
        writePos += 8;
    }

    /**
     * Writes a short value in little-endian format.
     */
    public void putShort(short value) {
        ensureCapacity(2);
        Unsafe.getUnsafe().putShort(bufPtr + writePos, value);
        writePos += 2;
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
        int utf8Len = NativeBufferWriter.utf8Length(value);
        putVarint(utf8Len);
        putUtf8(value);
    }

    /**
     * Writes UTF-8 encoded bytes directly without length prefix.
     */
    @Override
    public void putUtf8(CharSequence value) {
        if (value == null || value.length() == 0) {
            return;
        }

        int charLen = value.length();
        ensureCapacity(charLen);

        // Single-pass for ASCII. Mixed strings keep the ASCII prefix and resume UTF-8 encoding.
        long addr = bufPtr + writePos;
        int i = 0;
        for (; i < charLen; i++) {
            char c = value.charAt(i);
            if (c >= 0x80) {
                break;
            }
            Unsafe.getUnsafe().putByte(addr++, (byte) c);
        }

        if (i == charLen) {
            writePos += charLen;
        } else {
            int utf8Len = Utf8s.utf8Bytes(value, i, charLen);
            ensureCapacity(i + utf8Len);
            writePos += i + Utf8s.strCpyUtf8(value, i, bufPtr + writePos + i, utf8Len);
        }
    }

    /**
     * Writes an unsigned variable-length integer (LEB128 encoding).
     */
    @Override
    public void putVarint(long value) {
        while (value > 0x7F) {
            putByte((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        putByte((byte) value);
    }

    /**
     * Resets the buffer for reuse. Does not deallocate memory.
     */
    public void reset() {
        writePos = 0;
        frameStartOffset = 0;
        payloadStartOffset = 0;
    }

    /**
     * Skips the specified number of bytes, advancing the position.
     */
    @Override
    public void skip(int bytes) {
        ensureCapacity(bytes);
        writePos += bytes;
    }

    /**
     * Writes a complete close frame.
     *
     * @param code   close status code (e.g., 1000 for normal closure)
     * @param reason optional reason string (may be null)
     * @return frame info for sending
     */
    public FrameInfo writeCloseFrame(int code, String reason) {
        int payloadLen = 2; // status code
        int reasonLen = 0;
        if (reason != null && !reason.isEmpty()) {
            reasonLen = Utf8s.utf8Bytes(reason);
            payloadLen += reasonLen;
        }

        if (payloadLen > 125) {
            throw new HttpClientException("Close payload too large [len=").put(payloadLen).put(']');
        }

        int frameStart = writePos;
        int headerSize = WebSocketFrameWriter.headerSize(payloadLen, true);
        ensureCapacity(headerSize + payloadLen);

        int maskKey = rnd.nextInt();
        int written = WebSocketFrameWriter.writeHeader(bufPtr + writePos, true, WebSocketOpcode.CLOSE, payloadLen, maskKey);
        writePos += written;

        // Write status code (big-endian)
        long payloadStart = bufPtr + writePos;
        Unsafe.getUnsafe().putByte(payloadStart, (byte) ((code >> 8) & 0xFF));
        Unsafe.getUnsafe().putByte(payloadStart + 1, (byte) (code & 0xFF));
        writePos += 2;

        // Write reason if present
        if (reasonLen > 0) {
            writePos += Utf8s.strCpyUtf8(reason, bufPtr + writePos, reasonLen);
        }

        // Mask the payload (including status code and reason)
        WebSocketFrameWriter.maskPayload(payloadStart, payloadLen, maskKey);

        return frameInfo.set(frameStart, headerSize + payloadLen);
    }

    /**
     * Writes a complete ping frame (control frame, no masking needed for server).
     * Note: Client frames MUST be masked per RFC 6455. This writes a masked ping.
     *
     * @return frame info for sending
     */
    public FrameInfo writePingFrame() {
        return writePingFrame(0, 0);
    }

    /**
     * Writes a complete ping frame with payload.
     *
     * @param payloadPtr pointer to ping payload
     * @param payloadLen length of payload (max 125 bytes for control frames)
     * @return frame info for sending
     */
    public FrameInfo writePingFrame(long payloadPtr, int payloadLen) {
        if (payloadLen > 125) {
            throw new HttpClientException("Ping payload too large [len=").put(payloadLen).put(']');
        }

        int frameStart = writePos;
        int headerSize = WebSocketFrameWriter.headerSize(payloadLen, true);
        ensureCapacity(headerSize + payloadLen);

        int maskKey = rnd.nextInt();
        int written = WebSocketFrameWriter.writeHeader(bufPtr + writePos, true, WebSocketOpcode.PING, payloadLen, maskKey);
        writePos += written;

        if (payloadLen > 0) {
            Vect.memcpy(bufPtr + writePos, payloadPtr, payloadLen);
            WebSocketFrameWriter.maskPayload(bufPtr + writePos, payloadLen, maskKey);
            writePos += payloadLen;
        }

        return frameInfo.set(frameStart, headerSize + payloadLen);
    }

    /**
     * Writes a complete pong frame.
     *
     * @param payloadPtr pointer to pong payload (should match received ping)
     * @param payloadLen length of payload
     * @return frame info for sending
     */
    public FrameInfo writePongFrame(long payloadPtr, int payloadLen) {
        if (payloadLen > 125) {
            throw new HttpClientException("Pong payload too large [len=").put(payloadLen).put(']');
        }

        int frameStart = writePos;
        int headerSize = WebSocketFrameWriter.headerSize(payloadLen, true);
        ensureCapacity(headerSize + payloadLen);

        int maskKey = rnd.nextInt();
        int written = WebSocketFrameWriter.writeHeader(bufPtr + writePos, true, WebSocketOpcode.PONG, payloadLen, maskKey);
        writePos += written;

        if (payloadLen > 0) {
            Vect.memcpy(bufPtr + writePos, payloadPtr, payloadLen);
            WebSocketFrameWriter.maskPayload(bufPtr + writePos, payloadLen, maskKey);
            writePos += payloadLen;
        }

        return frameInfo.set(frameStart, headerSize + payloadLen);
    }

    private void grow(long requiredCapacity) {
        if (requiredCapacity > maxBufferSize) {
            throw new HttpClientException("WebSocket buffer size exceeded maximum [required=")
                    .put(requiredCapacity)
                    .put(", max=")
                    .put(maxBufferSize)
                    .put(']');
        }
        int newCapacity = Math.min(
                Math.max(Numbers.ceilPow2((int) requiredCapacity), (int) requiredCapacity),
                maxBufferSize
        );
        bufPtr = Unsafe.realloc(bufPtr, bufCapacity, newCapacity, MemoryTag.NATIVE_DEFAULT);
        bufCapacity = newCapacity;
    }

    /**
     * Information about a completed WebSocket frame's location in the buffer.
     * This class is mutable and reused to avoid allocations. Callers must
     * extract values before calling any end*Frame() method again.
     */
    public static final class FrameInfo {
        /**
         * Total length of the frame (header + payload).
         */
        public int length;
        /**
         * Offset from buffer start where the frame begins.
         */
        public int offset;

        FrameInfo set(int offset, int length) {
            this.offset = offset;
            this.length = length;
            return this;
        }
    }
}
