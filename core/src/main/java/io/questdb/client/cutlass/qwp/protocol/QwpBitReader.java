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

import io.questdb.client.cutlass.qwp.client.QwpDecodeException;
import io.questdb.client.std.Unsafe;

/**
 * Client-side bit-level reader for QWP v1 Gorilla-compressed columns. Mirrors
 * the server-side reader. Bits are read LSB-first; the buffer lazily pulls
 * bytes from the underlying native address as needed.
 * <p>
 * Overflow surfaces as {@link QwpDecodeException} so a malformed server frame
 * is reported to the user handler via {@code onError}, not an uncaught error.
 */
public class QwpBitReader {

    // Buffer for reading bits
    private long bitBuffer;
    // Number of bits currently available in the buffer (0-64)
    private int bitsInBuffer;
    private long currentAddress;
    private long endAddress;
    // Total bits available for reading (from reset)
    private long totalBitsAvailable;
    // Total bits already consumed
    private long totalBitsRead;

    public QwpBitReader() {
    }

    public long getBitPosition() {
        return totalBitsRead;
    }

    /**
     * Reads a single bit.
     */
    public int readBit() throws QwpDecodeException {
        if (totalBitsRead >= totalBitsAvailable) {
            throw new QwpDecodeException("QwpBitReader: read past end");
        }
        if (!ensureBits(1)) {
            throw new QwpDecodeException("QwpBitReader: read past end");
        }

        int bit = (int) (bitBuffer & 1);
        bitBuffer >>>= 1;
        bitsInBuffer--;
        totalBitsRead++;
        return bit;
    }

    /**
     * Reads multiple bits and returns them as a long (unsigned, LSB-aligned).
     */
    public long readBits(int numBits) throws QwpDecodeException {
        if (numBits <= 0) {
            return 0;
        }
        if (numBits > 64) {
            throw new AssertionError("Asked to read more than 64 bits into a long");
        }
        if (totalBitsRead + numBits > totalBitsAvailable) {
            throw new QwpDecodeException("QwpBitReader: read past end");
        }

        long result = 0;
        int bitsRemaining = numBits;
        int resultShift = 0;

        while (bitsRemaining > 0) {
            if (bitsInBuffer == 0) {
                if (!ensureBits(Math.min(bitsRemaining, 64))) {
                    throw new QwpDecodeException("QwpBitReader: read past end");
                }
            }

            int bitsToTake = Math.min(bitsRemaining, bitsInBuffer);
            long mask = bitsToTake == 64 ? -1L : (1L << bitsToTake) - 1;
            result |= (bitBuffer & mask) << resultShift;

            // Java masks the right operand of {@code >>>} by 0x3F for long, so
            // {@code bitBuffer >>>= 64} is a no-op and would leave the just-
            // consumed 64 bits in {@code bitBuffer}. The next ensureBits OR-fills
            // a fresh byte at bit 0 of that stale buffer, silently corrupting
            // every subsequent read. Special-case the full-width consume so
            // callers using readBits(64) (or readSigned(64)) are safe.
            if (bitsToTake == 64) {
                bitBuffer = 0L;
            } else {
                bitBuffer >>>= bitsToTake;
            }
            bitsInBuffer -= bitsToTake;
            bitsRemaining -= bitsToTake;
            resultShift += bitsToTake;
        }

        totalBitsRead += numBits;
        return result;
    }

    /**
     * Reads multiple bits and interprets them as a signed value (two's complement).
     */
    public long readSigned(int numBits) throws QwpDecodeException {
        long unsigned = readBits(numBits);
        if (numBits < 64 && (unsigned & (1L << (numBits - 1))) != 0) {
            unsigned |= -1L << numBits;
        }
        return unsigned;
    }

    /**
     * Resets the reader to read from {@code length} bytes starting at {@code address}.
     */
    public void reset(long address, long length) {
        this.currentAddress = address;
        this.endAddress = address + length;
        this.bitBuffer = 0;
        this.bitsInBuffer = 0;
        this.totalBitsAvailable = length * 8L;
        this.totalBitsRead = 0;
    }

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    private boolean ensureBits(int bitsNeeded) {
        while (bitsInBuffer < bitsNeeded && bitsInBuffer <= 56 && currentAddress < endAddress) {
            byte b = Unsafe.getUnsafe().getByte(currentAddress++);
            bitBuffer |= (long) (b & 0xFF) << bitsInBuffer;
            bitsInBuffer += 8;
        }
        return bitsInBuffer >= bitsNeeded;
    }
}
