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

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.std.Unsafe;
import org.jetbrains.annotations.TestOnly;

/**
 * Gorilla delta-of-delta encoder for timestamps in QWP v1 format.
 * <p>
 * This encoder is used by the WebSocket encoder to compress timestamp columns.
 * It uses delta-of-delta compression where:
 * <pre>
 * DoD = (t[n] - t[n-1]) - (t[n-1] - t[n-2])
 *
 * if DoD == 0:              write '0'              (1 bit)
 * elif DoD in [-64, 63]:    write '10' + 7-bit     (9 bits)
 * elif DoD in [-256, 255]:  write '110' + 9-bit    (12 bits)
 * elif DoD in [-2048, 2047]: write '1110' + 12-bit (16 bits)
 * else:                     write '1111' + 32-bit  (36 bits)
 * </pre>
 * <p>
 * The encoder writes first two timestamps uncompressed, then encodes
 * remaining timestamps using delta-of-delta compression.
 */
public class QwpGorillaEncoder {

    private static final int BUCKET_12BIT_MAX = 2047;
    private static final int BUCKET_12BIT_MIN = -2048;
    private static final int BUCKET_7BIT_MAX = 63;
    // Bucket boundaries (two's complement signed ranges)
    private static final int BUCKET_7BIT_MIN = -64;
    private static final int BUCKET_9BIT_MAX = 255;
    private static final int BUCKET_9BIT_MIN = -256;
    private final QwpBitWriter bitWriter = new QwpBitWriter();

    /**
     * Creates a new Gorilla encoder.
     */
    public QwpGorillaEncoder() {
    }

    /**
     * Calculates the encoded size in bytes for Gorilla-encoded timestamps stored off-heap.
     * <p>
     * Note: This does NOT include the encoding flag byte. Add 1 byte if
     * the encoding flag is needed.
     *
     * @param srcAddress source address of contiguous int64 timestamps in native memory
     * @param count      number of timestamps
     * @return encoded size in bytes (excluding encoding flag)
     */
    public static int calculateEncodedSize(long srcAddress, int count) {
        int size = calculateEncodedSizeIfSupported(srcAddress, count);
        assert size >= 0 : "caller must verify canUseGorilla() before calling calculateEncodedSize()";
        return size;
    }

    /**
     * Checks whether Gorilla encoding can be used and, if so, calculates the
     * encoded size in a single pass over the timestamp data. This replaces the
     * previous two-pass pattern of calling {@link #canUseGorilla} followed by
     * {@link #calculateEncodedSize}.
     *
     * @param srcAddress source address of contiguous int64 timestamps in native memory
     * @param count      number of timestamps
     * @return encoded size in bytes (excluding encoding flag), or -1 if Gorilla
     *         encoding cannot be used (a delta-of-delta exceeds int32 range)
     */
    public static int calculateEncodedSizeIfSupported(long srcAddress, int count) {
        if (count == 0) {
            return 0;
        }

        long size = 8; // first timestamp

        if (count == 1) {
            return (int) size;
        }

        size += 8; // second timestamp

        if (count == 2) {
            return (int) size;
        }

        // Single pass: validate int32 range AND calculate bits
        long prevTimestamp = Unsafe.getUnsafe().getLong(srcAddress + 8);
        long prevDelta = prevTimestamp - Unsafe.getUnsafe().getLong(srcAddress);
        long totalBits = 0;

        for (int i = 2; i < count; i++) {
            long ts = Unsafe.getUnsafe().getLong(srcAddress + (long) i * 8);
            long delta = ts - prevTimestamp;
            long deltaOfDelta = delta - prevDelta;

            if (deltaOfDelta < Integer.MIN_VALUE || deltaOfDelta > Integer.MAX_VALUE) {
                return -1;
            }

            totalBits += getBitsRequired(deltaOfDelta);

            prevDelta = delta;
            prevTimestamp = ts;
        }

        // Round up to bytes
        size += (totalBits + 7) / 8;

        return toIntChecked(size);
    }

    /**
     * Converts a long encoded size to int, throwing if it exceeds int range.
     */
    @TestOnly
    public static int toIntChecked(long size) {
        if (size > Integer.MAX_VALUE) {
            throw new LineSenderException("Gorilla encoded size exceeds int range");
        }
        return (int) size;
    }

    /**
     * Checks if Gorilla encoding can be used for timestamps stored off-heap.
     * <p>
     * Gorilla encoding uses 32-bit signed integers for delta-of-delta values,
     * so it cannot encode timestamps where the delta-of-delta exceeds the
     * 32-bit signed integer range.
     *
     * @param srcAddress source address of contiguous int64 timestamps in native memory
     * @param count      number of timestamps
     * @return true if Gorilla encoding can be used, false otherwise
     */
    public static boolean canUseGorilla(long srcAddress, int count) {
        return calculateEncodedSizeIfSupported(srcAddress, count) >= 0;
    }

    /**
     * Returns the number of bits required to encode a delta-of-delta value.
     *
     * @param deltaOfDelta the delta-of-delta value
     * @return bits required
     */
    public static int getBitsRequired(long deltaOfDelta) {
        int bucket = getBucket(deltaOfDelta);
        switch (bucket) {
            case 0:
                return 1;
            case 1:
                return 9;
            case 2:
                return 12;
            case 3:
                return 16;
            default:
                return 36;
        }
    }

    /**
     * Determines which bucket a delta-of-delta value falls into.
     *
     * @param deltaOfDelta the delta-of-delta value
     * @return bucket number (0 = 1-bit, 1 = 9-bit, 2 = 12-bit, 3 = 16-bit, 4 = 36-bit)
     */
    public static int getBucket(long deltaOfDelta) {
        if (deltaOfDelta == 0) {
            return 0; // 1-bit
        } else if (deltaOfDelta >= BUCKET_7BIT_MIN && deltaOfDelta <= BUCKET_7BIT_MAX) {
            return 1; // 9-bit (2 prefix + 7 value)
        } else if (deltaOfDelta >= BUCKET_9BIT_MIN && deltaOfDelta <= BUCKET_9BIT_MAX) {
            return 2; // 12-bit (3 prefix + 9 value)
        } else if (deltaOfDelta >= BUCKET_12BIT_MIN && deltaOfDelta <= BUCKET_12BIT_MAX) {
            return 3; // 16-bit (4 prefix + 12 value)
        } else {
            return 4; // 36-bit (4 prefix + 32 value)
        }
    }

    /**
     * Encodes a delta-of-delta value using bucket selection.
     * <p>
     * Prefix patterns are written LSB-first to match the decoder's read order:
     * <ul>
     *   <li>'0'    -> write bit 0</li>
     *   <li>'10'   -> write bit 1, then bit 0 (0b01 as 2-bit value)</li>
     *   <li>'110'  -> write bit 1, bit 1, bit 0 (0b011 as 3-bit value)</li>
     *   <li>'1110' -> write bit 1, bit 1, bit 1, bit 0 (0b0111 as 4-bit value)</li>
     *   <li>'1111' -> write bit 1, bit 1, bit 1, bit 1 (0b1111 as 4-bit value)</li>
     * </ul>
     *
     * @param deltaOfDelta the delta-of-delta value to encode
     */
    public void encodeDoD(long deltaOfDelta) {
        int bucket = getBucket(deltaOfDelta);
        switch (bucket) {
            case 0:
                bitWriter.writeBit(0);
                break;
            case 1:
                bitWriter.writeBits(0b01, 2);
                bitWriter.writeSigned(deltaOfDelta, 7);
                break;
            case 2:
                bitWriter.writeBits(0b011, 3);
                bitWriter.writeSigned(deltaOfDelta, 9);
                break;
            case 3:
                bitWriter.writeBits(0b0111, 4);
                bitWriter.writeSigned(deltaOfDelta, 12);
                break;
            default:
                bitWriter.writeBits(0b1111, 4);
                bitWriter.writeSigned(deltaOfDelta, 32);
                break;
        }
    }

    /**
     * Encodes timestamps from off-heap memory using Gorilla compression.
     * <p>
     * Format:
     * <pre>
     * - First timestamp: int64 (8 bytes, little-endian)
     * - Second timestamp: int64 (8 bytes, little-endian)
     * - Remaining timestamps: bit-packed delta-of-delta
     * </pre>
     * <p>
     * Precondition: the caller must verify that {@link #canUseGorilla(long, int)}
     * returns {@code true} before calling this method. The largest delta-of-delta
     * bucket uses 32-bit signed encoding, so values outside the {@code int} range
     * are silently truncated, producing corrupt output on decode.
     * <p>
     * Note: This method does NOT write the encoding flag byte. The caller is
     * responsible for writing the ENCODING_GORILLA flag before calling this method.
     *
     * @param destAddress destination address in native memory
     * @param capacity    maximum number of bytes to write
     * @param srcAddress  source address of contiguous int64 timestamps in native memory
     * @param count       number of timestamps to encode
     * @return number of bytes written
     */
    public int encodeTimestamps(long destAddress, long capacity, long srcAddress, int count) {
        if (count == 0) {
            return 0;
        }

        int pos;

        // Write first timestamp uncompressed
        if (capacity < 8) {
            throw new LineSenderException("Gorilla encoder buffer overflow");
        }
        long ts0 = Unsafe.getUnsafe().getLong(srcAddress);
        Unsafe.getUnsafe().putLong(destAddress, ts0);
        pos = 8;

        if (count == 1) {
            return pos;
        }

        // Write second timestamp uncompressed
        if (capacity < pos + 8) {
            throw new LineSenderException("Gorilla encoder buffer overflow");
        }
        long ts1 = Unsafe.getUnsafe().getLong(srcAddress + 8);
        Unsafe.getUnsafe().putLong(destAddress + pos, ts1);
        pos += 8;

        if (count == 2) {
            return pos;
        }

        // Encode remaining with delta-of-delta
        bitWriter.reset(destAddress + pos, capacity - pos);
        long prevTs = ts1;
        long prevDelta = ts1 - ts0;

        for (int i = 2; i < count; i++) {
            long ts = Unsafe.getUnsafe().getLong(srcAddress + (long) i * 8);
            long delta = ts - prevTs;
            long dod = delta - prevDelta;
            encodeDoD(dod);
            prevDelta = delta;
            prevTs = ts;
        }

        return pos + bitWriter.finish();
    }
}
