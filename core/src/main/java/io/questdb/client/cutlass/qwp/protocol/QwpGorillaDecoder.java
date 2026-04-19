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

/**
 * Client-side Gorilla delta-of-delta decoder for timestamp columns in QWP
 * egress {@code RESULT_BATCH} frames. Mirrors the server-side decoder and
 * reads the bitstream produced by {@code QwpGorillaEncoder}.
 * <p>
 * Encoding buckets:
 * <pre>
 *   '0'                     -> DoD = 0                 (1 bit)
 *   '10' + 7-bit signed     -> DoD in [-64, 63]        (9 bits)
 *   '110' + 9-bit signed    -> DoD in [-256, 255]     (12 bits)
 *   '1110' + 12-bit signed  -> DoD in [-2048, 2047]   (16 bits)
 *   '1111' + 32-bit signed  -> any other DoD          (36 bits)
 * </pre>
 */
public class QwpGorillaDecoder {

    private final QwpBitReader bitReader = new QwpBitReader();
    private long prevDelta;
    private long prevTimestamp;

    public QwpGorillaDecoder() {
    }

    /**
     * Decodes the next timestamp from the bit stream.
     */
    public long decodeNext() throws QwpDecodeException {
        long deltaOfDelta = decodeDoD();
        long delta = prevDelta + deltaOfDelta;
        long timestamp = prevTimestamp + delta;

        prevDelta = delta;
        prevTimestamp = timestamp;
        return timestamp;
    }

    /**
     * Returns the current bit position (bits read since reset).
     */
    public long getBitPosition() {
        return bitReader.getBitPosition();
    }

    /**
     * Resets the decoder. First two timestamps are always shipped uncompressed
     * at the head of the column's wire bytes; the address + length here point
     * at the bitstream that follows them.
     */
    public void reset(long firstTimestamp, long secondTimestamp, long address, long length) {
        this.prevTimestamp = secondTimestamp;
        this.prevDelta = secondTimestamp - firstTimestamp;
        bitReader.reset(address, length);
    }

    private long decodeDoD() throws QwpDecodeException {
        int bit = bitReader.readBit();
        if (bit == 0) {
            return 0;
        }
        bit = bitReader.readBit();
        if (bit == 0) {
            return bitReader.readSigned(7);
        }
        bit = bitReader.readBit();
        if (bit == 0) {
            return bitReader.readSigned(9);
        }
        bit = bitReader.readBit();
        if (bit == 0) {
            return bitReader.readSigned(12);
        }
        return bitReader.readSigned(32);
    }
}
