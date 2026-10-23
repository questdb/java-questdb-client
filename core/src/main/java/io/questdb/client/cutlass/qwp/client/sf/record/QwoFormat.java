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

import io.questdb.client.cutlass.qwp.client.NativeBufferWriter;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Crc32c;
import io.questdb.client.std.Unsafe;

/**
 * Layout sheet for the QWO1 open record: a columnar record stored inside a
 * store-and-forward segment payload. All offsets are relative to the payload
 * start (header, descriptor) or to the start of the structure they belong to
 * (chunk). Multi-byte integers are little-endian.
 * <p>
 * Descriptor: after {@code D_NAME}'s varint-prefixed table name come
 * {@code colCount} column defs, each
 * {@code [varint nameLen][name][u8 type][u8 prefixKind][u8 prefix0][u8 prefix1][u8 bitmap][u32 firstChunkOffset]};
 * the {@code COL_*} offsets are relative to the type byte that follows the name.
 * {@code prefixKind} stays {@link #PREFIX_NONE} until the GEOHASH precision or
 * DECIMAL scale is known; the writer then stores the kind, the varint value and
 * the unchanged bitmap byte with one 4-byte store.
 * <p>
 * Chunk data after {@code C_DATA}: a bitmap of {@code ceil(rowCap/8)} bytes when
 * the column uses one, then the values (fixed: {@code rowCap * width}; BOOLEAN:
 * {@code ceil(rowCap/8)}; SYMBOL: {@code rowCap * 5}; variable:
 * {@code (rowCap + 1) * 4} offsets followed by {@code byteCap} bytes).
 */
public final class QwoFormat {
    public static final int COL_BITMAP = 4;
    public static final int COL_FIRST_CHUNK = 5;
    public static final int COL_FIXED_SIZE = 9;
    public static final int COL_PREFIX = 2;
    public static final int COL_PREFIX_KIND = 1;
    public static final int COL_TYPE = 0;
    public static final int C_BYTE_CAP = 8;
    public static final int C_DATA = 12;
    public static final int C_NEXT = 0;
    public static final int C_ROW_CAP = 4;
    public static final int DEFAULT_DICT_RESERVE = 4096;
    public static final int D_COL_COUNT = 8;
    public static final int D_CRC = 0;
    public static final int D_FLAGS = 10;
    public static final int D_NAME = 20;
    public static final int D_NEXT = 4;
    public static final int D_ROW_WORD = 12;
    public static final int HEADER_SIZE = 48;
    public static final int MAGIC_RECORD = 0x314F5751;
    public static final int MAX_CHUNK_ROWS = 4096;
    public static final int MAX_ROWS_PER_BLOCK = 1_000_000;
    public static final int MIN_RECORD_RESERVATION = 65536;
    public static final int OFF_BLOCK_COUNT = 6;
    public static final int OFF_CAPACITY = 8;
    public static final int OFF_DICT_CAPACITY = 24;
    public static final int OFF_DICT_REGION = 20;
    public static final int OFF_DICT_START_ID = 28;
    public static final int OFF_DICT_WORD = 32;
    public static final int OFF_FIRST_BLOCK = 16;
    public static final int OFF_FLAGS = 5;
    public static final int OFF_HEADER_CRC = 12;
    public static final int OFF_MAGIC = 0;
    public static final int OFF_RESERVED = 44;
    public static final int OFF_USED_BYTES = 40;
    public static final int OFF_VERSION = 4;
    public static final byte PREFIX_DECIMAL = 2;
    public static final byte PREFIX_GEOHASH = 1;
    public static final byte PREFIX_NONE = 0;
    public static final int STATE_OPEN = 0x4E45504F;
    public static final int STATE_SEALED = 0x4C414553;
    public static final int WIRE_MARGIN_FIXED = 32;
    public static final int WIRE_MARGIN_PER_BLOCK = 8;

    private QwoFormat() {
    }

    /**
     * Returns the size of a chunk after {@code C_DATA}: the optional null bitmap
     * followed by the column's values.
     *
     * @param prefix the GEOHASH precision in bits, ignored for every other type
     */
    public static int chunkDataBytes(byte type, int prefix, boolean bitmap, int rowCap, int byteCap) {
        int bitmapBytes = (rowCap + 7) >>> 3;
        int n = bitmap ? bitmapBytes : 0;
        if (type == QwpConstants.TYPE_BOOLEAN) {
            return n + bitmapBytes;
        }
        if (type == QwpConstants.TYPE_SYMBOL) {
            return n + rowCap * 5;
        }
        if (isVariableSize(type)) {
            return n + (rowCap + 1) * 4 + byteCap;
        }
        return n + rowCap * valueWidth(type, prefix);
    }

    public static int crcOf(long word) {
        return (int) word;
    }

    /**
     * CRC32C over the descriptor at {@code descAddr}, excluding the fields that
     * change after the descriptor is first published: {@code D_CRC} itself,
     * {@code D_NEXT}, {@code D_ROW_WORD} and the three prefix bytes
     * ({@code prefixKind}, {@code prefix0}, {@code prefix1}) of every column def.
     * It covers {@code D_COL_COUNT}, {@code D_FLAGS}, the table name and, per
     * column def, the name, the type, the bitmap byte and the first chunk offset.
     * The caller guarantees the descriptor lies in readable memory.
     */
    public static int descriptorCrc(long descAddr) {
        int colCount = Unsafe.getUnsafe().getShort(descAddr + D_COL_COUNT) & 0xFFFF;
        int crc = Crc32c.updateUnsafe(Crc32c.INIT, descAddr + D_COL_COUNT, D_ROW_WORD - D_COL_COUNT);
        long segment = descAddr + D_NAME;
        long p = skipString(segment);
        for (int i = 0; i < colCount; i++) {
            p = skipString(p) + COL_PREFIX_KIND;
            crc = Crc32c.updateUnsafe(crc, segment, p - segment);
            segment = p + (COL_BITMAP - COL_PREFIX_KIND);
            p = segment + (COL_FIXED_SIZE - COL_BITMAP);
        }
        return Crc32c.updateUnsafe(crc, segment, p - segment);
    }

    /**
     * CRC32C over the header fields {@code open()} writes and nothing changes
     * later: magic and version {@code [0,5)}, capacity {@code [8,12)}, the
     * dictionary region, its capacity and the start id {@code [20,32)}, and the
     * reserved word {@code [44,48)}. It excludes the flags (the seal may add
     * DEFER_COMMIT), blockCount, the CRC itself, firstBlock, dictWord and
     * usedBytes, so the CRC is stored once at open and never again.
     */
    public static int headerCrc(long payloadAddr) {
        int crc = Crc32c.updateUnsafe(Crc32c.INIT, payloadAddr, OFF_FLAGS);
        crc = Crc32c.updateUnsafe(crc, payloadAddr + OFF_CAPACITY, OFF_HEADER_CRC - OFF_CAPACITY);
        crc = Crc32c.updateUnsafe(crc, payloadAddr + OFF_DICT_REGION, OFF_DICT_WORD - OFF_DICT_REGION);
        return Crc32c.updateUnsafe(crc, payloadAddr + OFF_RESERVED, HEADER_SIZE - OFF_RESERVED);
    }

    public static boolean isRecordPayload(long payloadAddr, int payloadLen) {
        return payloadLen >= HEADER_SIZE && Unsafe.getUnsafe().getInt(payloadAddr) == MAGIC_RECORD;
    }

    public static boolean isVariableSize(byte type) {
        return type == QwpConstants.TYPE_VARCHAR
                || type == QwpConstants.TYPE_BINARY
                || type == QwpConstants.TYPE_DOUBLE_ARRAY
                || type == QwpConstants.TYPE_LONG_ARRAY;
    }

    /**
     * Writes {@code [varint utf8Len][utf8]} at {@code addr}, the same bytes as
     * {@link NativeBufferWriter#putString}. Returns the number of bytes written.
     */
    public static int putString(long addr, CharSequence s) {
        long p = NativeBufferWriter.writeVarint(addr, NativeBufferWriter.utf8Length(s));
        p = NativeBufferWriter.writeUtf8(p, s);
        return (int) (p - addr);
    }

    public static int rowCountOf(long word) {
        return (int) (word >>> 32);
    }

    public static long rowWord(int rowCount, int crc) {
        return ((long) rowCount << 32) | (crc & 0xFFFFFFFFL);
    }

    public static int stringBytes(CharSequence s) {
        int utf8Len = NativeBufferWriter.utf8Length(s);
        return NativeBufferWriter.varintSize(utf8Len) + utf8Len;
    }

    /**
     * Wire width of one value of a fixed-size type as stored in a chunk.
     *
     * @param prefix the GEOHASH precision in bits (ignored for other types); a
     *               non-positive precision (not yet known) yields 8 bytes
     */
    public static int valueWidth(byte type, int prefix) {
        switch (type) {
            case QwpConstants.TYPE_BYTE:
                return 1;
            case QwpConstants.TYPE_SHORT:
            case QwpConstants.TYPE_CHAR:
                return 2;
            case QwpConstants.TYPE_INT:
            case QwpConstants.TYPE_IPv4:
            case QwpConstants.TYPE_FLOAT:
                return 4;
            case QwpConstants.TYPE_LONG:
            case QwpConstants.TYPE_DATE:
            case QwpConstants.TYPE_DOUBLE:
            case QwpConstants.TYPE_TIMESTAMP:
            case QwpConstants.TYPE_TIMESTAMP_NANOS:
            case QwpConstants.TYPE_DECIMAL64:
                return 8;
            case QwpConstants.TYPE_UUID:
            case QwpConstants.TYPE_DECIMAL128:
                return 16;
            case QwpConstants.TYPE_LONG256:
            case QwpConstants.TYPE_DECIMAL256:
                return 32;
            case QwpConstants.TYPE_GEOHASH:
                return prefix <= 0 ? 8 : (prefix + 7) >>> 3;
            default:
                throw new IllegalArgumentException("not a fixed-size type: " + type);
        }
    }

    /**
     * Decodes an unsigned LEB128 varint of at most 5 bytes at {@code addr}.
     */
    static int readVarint(long addr) {
        int value = 0;
        for (int i = 0; i < 5; i++) {
            byte b = Unsafe.getUnsafe().getByte(addr + i);
            value |= (b & 0x7F) << (7 * i);
            if (b >= 0) {
                break;
            }
        }
        return value;
    }

    /**
     * Returns the address after the {@code [varint len][bytes]} string at {@code addr}.
     */
    static long skipString(long addr) {
        return addr + varintLength(addr) + readVarint(addr);
    }

    /**
     * Returns the encoded length, 1 to 5 bytes, of the varint at {@code addr}.
     */
    static int varintLength(long addr) {
        int n = 1;
        while (n < 5 && Unsafe.getUnsafe().getByte(addr + n - 1) < 0) {
            n++;
        }
        return n;
    }
}
