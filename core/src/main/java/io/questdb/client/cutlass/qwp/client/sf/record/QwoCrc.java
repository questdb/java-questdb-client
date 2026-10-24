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

package io.questdb.client.cutlass.qwp.client.sf.record;

import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Crc32c;
import io.questdb.client.std.Unsafe;

/**
 * Accumulates the row CRC stream of a QWO1 block (spec 4.3), shared by the
 * writer and the recovery validator so both use one definition. Per row, per
 * column in descriptor order:
 * <ul>
 *     <li>a column with a null bitmap contributes one byte, 0 for null and 1 for
 *     present, then the value when present;</li>
 *     <li>a column without a bitmap contributes the value;</li>
 *     <li>a value is its bytes exactly as stored in the chunk (GEOHASH at wire
 *     width, SYMBOL as its varint, DECIMAL128/256 in wire word order); a BOOLEAN
 *     value is one byte 0 or 1; a variable-size value (VARCHAR, BINARY, arrays)
 *     is a little-endian {@code u32} length followed by its bytes.</li>
 * </ul>
 * The first row of a block starts from {@link Crc32c#INIT}; each later row
 * chains from the previous row's CRC. Each piece extends the CRC in place, with
 * no gathering copy: a flag byte, a length and a value of 1, 4 or 8 bytes go
 * through the register forms of {@link Crc32c}, any other value through
 * {@link Crc32c#updateUnsafe} at its chunk address; both give the CRC of the
 * same byte stream. The native {@code Crc32c.update} is never used on this
 * path.
 */
final class QwoCrc {
    private int crc;

    /**
     * Starts a row, chaining from {@code seed}: {@link Crc32c#INIT} for a
     * block's first row, the previous row's CRC otherwise.
     */
    void begin(int seed) {
        crc = seed;
    }

    /**
     * Ends the row and returns the CRC over every row so far.
     */
    int finish() {
        return crc;
    }

    /**
     * Appends one BOOLEAN column's contribution.
     */
    void putBoolean(boolean bitmap, boolean present, boolean value) {
        if (bitmap) {
            crc = Crc32c.updateByte(crc, present ? 1 : 0);
        }
        if (present) {
            crc = Crc32c.updateByte(crc, value ? 1 : 0);
        }
    }

    /**
     * Appends one column's contribution for any type but BOOLEAN; the value is
     * the {@code valueLen} bytes at {@code valueAddr} as stored in the chunk and
     * is ignored when {@code present} is false.
     */
    void putValue(byte type, boolean bitmap, boolean present, long valueAddr, int valueLen) {
        assert type != QwpConstants.TYPE_BOOLEAN;
        if (bitmap) {
            crc = Crc32c.updateByte(crc, present ? 1 : 0);
        }
        if (present) {
            if (QwoFormat.isVariableSize(type)) {
                crc = Crc32c.updateInt(crc, valueLen);
            }
            switch (valueLen) {
                case 1:
                    crc = Crc32c.updateByte(crc, Unsafe.getUnsafe().getByte(valueAddr));
                    break;
                case 4:
                    crc = Crc32c.updateInt(crc, Unsafe.getUnsafe().getInt(valueAddr));
                    break;
                case 8:
                    crc = Crc32c.updateLong(crc, Unsafe.getUnsafe().getLong(valueAddr));
                    break;
                default:
                    crc = Crc32c.updateUnsafe(crc, valueAddr, valueLen);
                    break;
            }
        }
    }
}
