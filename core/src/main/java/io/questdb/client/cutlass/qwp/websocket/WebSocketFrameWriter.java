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

package io.questdb.client.cutlass.qwp.websocket;

import io.questdb.client.std.Unsafe;

import java.nio.ByteOrder;

/**
 * Zero-allocation WebSocket frame writer.
 * Writes WebSocket frames according to RFC 6455.
 *
 * <p>All methods are static utilities that write directly to memory buffers.
 *
 * <p>Thread safety: This class is thread-safe as it contains no mutable state.
 */
public final class WebSocketFrameWriter {
    // Frame header bits
    private static final int FIN_BIT = 0x80;
    private static final boolean IS_BIG_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;
    private static final int MASK_BIT = 0x80;

    private WebSocketFrameWriter() {
        // Static utility class
    }

    /**
     * Calculates the header size for a given payload length and masking.
     *
     * @param payloadLength the payload length
     * @param masked        true if the payload will be masked
     * @return the header size in bytes
     */
    public static int headerSize(long payloadLength, boolean masked) {
        int size;
        if (payloadLength <= 125) {
            size = 2;
        } else if (payloadLength <= 65535) {
            size = 4;
        } else {
            size = 10;
        }
        return masked ? size + 4 : size;
    }

    /**
     * Masks payload data in place using XOR with the given mask key.
     *
     * @param buf     the payload buffer
     * @param len     the payload length
     * @param maskKey the 4-byte mask key
     */
    public static void maskPayload(long buf, long len, int maskKey) {
        // maskKey is in big-endian convention: MSB = wire byte 0 = mask byte for position 0.
        // For bulk XOR via getInt/getLong (native byte order), convert to native order
        // so that memory position 0 XORs with mask byte 0, position 1 with mask byte 1, etc.
        int nativeMask = IS_BIG_ENDIAN ? maskKey : Integer.reverseBytes(maskKey);
        long longMask = ((long) nativeMask << 32) | (nativeMask & 0xFFFFFFFFL);

        long i = 0;

        // Process 8-byte chunks
        while (i + 8 <= len) {
            long value = Unsafe.getUnsafe().getLong(buf + i);
            Unsafe.getUnsafe().putLong(buf + i, value ^ longMask);
            i += 8;
        }

        // Process 4-byte chunk if remaining
        if (i + 4 <= len) {
            int value = Unsafe.getUnsafe().getInt(buf + i);
            Unsafe.getUnsafe().putInt(buf + i, value ^ nativeMask);
            i += 4;
        }

        // Process remaining bytes - extract mask byte in big-endian order
        while (i < len) {
            byte b = Unsafe.getUnsafe().getByte(buf + i);
            int maskByte = (maskKey >>> ((3 - ((int) i & 3)) << 3)) & 0xFF;
            Unsafe.getUnsafe().putByte(buf + i, (byte) (b ^ maskByte));
            i++;
        }
    }

    /**
     * Writes a WebSocket frame header to the buffer.
     *
     * @param buf           the buffer to write to
     * @param fin           true if this is the final frame
     * @param opcode        the frame opcode
     * @param payloadLength the payload length
     * @param masked        true if the payload should be masked
     * @return the number of bytes written (header size)
     */
    public static int writeHeader(long buf, boolean fin, int opcode, long payloadLength, boolean masked) {
        int offset = 0;

        // First byte: FIN + opcode
        int byte0 = (fin ? FIN_BIT : 0) | (opcode & 0x0F);
        Unsafe.getUnsafe().putByte(buf + offset++, (byte) byte0);

        // Second byte: MASK + payload length
        int maskBit = masked ? MASK_BIT : 0;

        if (payloadLength <= 125) {
            Unsafe.getUnsafe().putByte(buf + offset++, (byte) (maskBit | payloadLength));
        } else if (payloadLength <= 65535) {
            Unsafe.getUnsafe().putByte(buf + offset++, (byte) (maskBit | 126));
            Unsafe.getUnsafe().putByte(buf + offset++, (byte) ((payloadLength >> 8) & 0xFF));
            Unsafe.getUnsafe().putByte(buf + offset++, (byte) (payloadLength & 0xFF));
        } else {
            Unsafe.getUnsafe().putByte(buf + offset++, (byte) (maskBit | 127));
            Unsafe.getUnsafe().putLong(buf + offset, Long.reverseBytes(payloadLength));
            offset += 8;
        }

        return offset;
    }

    /**
     * Writes a WebSocket frame header with optional mask key.
     *
     * @param buf           the buffer to write to
     * @param fin           true if this is the final frame
     * @param opcode        the frame opcode
     * @param payloadLength the payload length
     * @param maskKey       the mask key (only used if masked is true)
     * @return the number of bytes written (header size including mask key)
     */
    public static int writeHeader(long buf, boolean fin, int opcode, long payloadLength, int maskKey) {
        int offset = writeHeader(buf, fin, opcode, payloadLength, true);
        // Write mask key in network byte order (big-endian) per RFC 6455
        Unsafe.getUnsafe().putByte(buf + offset, (byte) (maskKey >>> 24));
        Unsafe.getUnsafe().putByte(buf + offset + 1, (byte) (maskKey >>> 16));
        Unsafe.getUnsafe().putByte(buf + offset + 2, (byte) (maskKey >>> 8));
        Unsafe.getUnsafe().putByte(buf + offset + 3, (byte) maskKey);
        return offset + 4;
    }
}
