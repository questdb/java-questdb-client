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
import io.questdb.client.std.Unsafe;

/**
 * Walks one column of a QWO1 block over its chunk chain. It is the single
 * reading definition of the chunk layout, shared by {@link QwoMaterializer} and
 * the recovery validator.
 * <p>
 * After {@link #of} a caller drives one of two walks, and does not mix them in
 * one pass ({@link #rewind()} starts over):
 * <ul>
 *     <li>the chunk walk, {@link #nextChunk()} and the {@code chunk*} getters,
 *     hands out each chunk's null bitmap and value bytes for bulk copies;</li>
 *     <li>the row walk, {@link #next()} and the row getters, visits every row in
 *     order with its null flag, value address and length.</li>
 * </ul>
 * Only the block's first {@code rowCount} rows are visited. The chunks hold them
 * in row order, every chunk but the last one full, so a chunk holds
 * {@code min(rowCap, rows left)} of them. Nothing past those rows is read:
 * uncommitted values, bitmap bits of rolled-back rows and unreachable relocated
 * chunks are skipped. Chunks are found by following the def's first chunk and
 * {@code C_NEXT} links only; they need not be contiguous. The cursor trusts the
 * offsets it follows, so a caller reading untrusted memory checks the chain's
 * bounds first. A {@code rowCount} of 0 yields no chunk and no row. The cursor
 * allocates nothing.
 */
final class QwoColumnCursor {
    private long bitmapAddr;
    private boolean booleanValue;
    private long bytesAddr;
    private long bytesBefore;
    private int chunk;
    private int chunkRows;
    private int chunkValueCount;
    private int firstChunk;
    private boolean hasBitmap;
    private boolean isNull;
    private boolean isVariableSize;
    private int prefix;
    private QwoRecordReader reader;
    private int rowCount;
    private int rowInChunk;
    private int rowsBefore;
    private int slot;
    private long symbolAddr;
    private byte type;
    private long valueAddr;
    private int valueLen;
    private long valuesAddr;
    private int width;

    /**
     * Row walk: the current BOOLEAN row's value; false for a null row.
     */
    boolean booleanValue() {
        return booleanValue;
    }

    /**
     * Chunk walk: the current chunk's null bitmap (bit set = null, LSB first),
     * 0 when the column has none.
     */
    long chunkBitmapAddr() {
        return bitmapAddr;
    }

    /**
     * Chunk walk: start of the current chunk's value bytes as the wire carries
     * them back to back: the values for a fixed-size type, the packed bits for
     * BOOLEAN, the varints for SYMBOL, and the bytes after the offsets for a
     * variable-size type.
     */
    long chunkBytesAddr() {
        return bytesAddr;
    }

    /**
     * Chunk walk: length of the bytes at {@link #chunkBytesAddr()} that hold the
     * chunk's {@link #chunkValueCount()} values. A SYMBOL chunk is scanned to
     * find it.
     */
    int chunkBytesLen() {
        switch (type) {
            case QwpConstants.TYPE_BOOLEAN:
                return (chunkValueCount + 7) >>> 3;
            case QwpConstants.TYPE_SYMBOL: {
                long p = valuesAddr;
                for (int i = 0; i < chunkValueCount; i++) {
                    p += QwoFormat.varintLength(p);
                }
                return (int) (p - valuesAddr);
            }
            default:
                return isVariableSize ? valueOffset(chunkValueCount) : chunkValueCount * width;
        }
    }

    /**
     * Chunk walk: rows of the block the current chunk holds.
     */
    int chunkRows() {
        return chunkRows;
    }

    /**
     * Chunk walk: non-null rows of the current chunk, the values it stores.
     */
    int chunkValueCount() {
        return chunkValueCount;
    }

    /**
     * Chunk walk: the current chunk's values region after the bitmap: fixed
     * values, BOOLEAN bits, SYMBOL varints, or the {@code u32} offsets of a
     * variable-size type ({@code chunkValueCount() + 1} of them, the first 0).
     */
    long chunkValuesAddr() {
        return valuesAddr;
    }

    /**
     * Row walk: true when the current row is null; always false for a column
     * without a bitmap, whose nulls are stored as values.
     */
    boolean isNull() {
        return isNull;
    }

    /**
     * Row walk: advances to the next row of the block.
     *
     * @return false after the last row
     */
    boolean next() {
        if (rowInChunk + 1 >= chunkRows && !nextChunk()) {
            return false;
        }
        int row = ++rowInChunk;
        isNull = hasBitmap && (Unsafe.getUnsafe().getByte(bitmapAddr + (row >>> 3)) & (1 << (row & 7))) != 0;
        if (isNull) {
            booleanValue = false;
            valueAddr = 0;
            valueLen = 0;
            return true;
        }
        int s = slot++;
        switch (type) {
            case QwpConstants.TYPE_BOOLEAN:
                valueAddr = valuesAddr + (s >>> 3);
                valueLen = 0;
                booleanValue = (Unsafe.getUnsafe().getByte(valueAddr) & (1 << (s & 7))) != 0;
                break;
            case QwpConstants.TYPE_SYMBOL:
                valueAddr = symbolAddr;
                valueLen = QwoFormat.varintLength(symbolAddr);
                symbolAddr += valueLen;
                break;
            default:
                if (isVariableSize) {
                    int start = valueOffset(s);
                    valueAddr = bytesAddr + start;
                    valueLen = valueOffset(s + 1) - start;
                } else {
                    valueAddr = valuesAddr + (long) s * width;
                    valueLen = width;
                }
                break;
        }
        return true;
    }

    /**
     * Chunk walk: advances to the next chunk that holds rows of the block.
     *
     * @return false once the chunks so far hold every row, or when the chain
     * ends early
     */
    boolean nextChunk() {
        if (rowsBefore + chunkRows >= rowCount) {
            return false;
        }
        int next = chunk == 0 ? firstChunk : reader.nextChunk(chunk);
        if (next == 0) {
            return false;
        }
        if (isVariableSize && chunk != 0) {
            bytesBefore += valueOffset(chunkValueCount);
        }
        rowsBefore += chunkRows;
        chunk = next;
        int rowCap = reader.chunkRowCap(next);
        chunkRows = Math.min(rowCap, rowCount - rowsBefore);
        bitmapAddr = reader.chunkBitmapAddr(next, hasBitmap);
        valuesAddr = reader.chunkValuesAddr(next, type, hasBitmap);
        bytesAddr = isVariableSize ? reader.chunkBytesAddr(next, hasBitmap, rowCap) : valuesAddr;
        chunkValueCount = hasBitmap ? chunkRows - countNulls(bitmapAddr, chunkRows) : chunkRows;
        rowInChunk = -1;
        slot = 0;
        symbolAddr = valuesAddr;
        return true;
    }

    /**
     * Positions the cursor before the first row of the column whose def is at
     * offset {@code def}, in a block with {@code rowCount} committed rows.
     */
    void of(QwoRecordReader reader, int def, int rowCount) {
        this.reader = reader;
        this.rowCount = rowCount;
        type = reader.columnType(def);
        hasBitmap = reader.columnUsesBitmap(def);
        prefix = reader.prefix(def);
        isVariableSize = QwoFormat.isVariableSize(type);
        width = type == QwpConstants.TYPE_BOOLEAN || type == QwpConstants.TYPE_SYMBOL || isVariableSize
                ? 0
                : QwoFormat.valueWidth(type, prefix);
        firstChunk = reader.firstChunk(def);
        rewind();
    }

    /**
     * The column's GEOHASH precision or DECIMAL scale; -1 while the def carries
     * none (every value so far null) and for every other type.
     */
    int prefix() {
        return prefix;
    }

    /**
     * Goes back before the first row, so either walk can start over.
     */
    void rewind() {
        booleanValue = false;
        bytesBefore = 0;
        chunk = 0;
        chunkRows = 0;
        chunkValueCount = 0;
        isNull = false;
        rowInChunk = -1;
        rowsBefore = 0;
        valueAddr = 0;
        valueLen = 0;
    }

    byte type() {
        return type;
    }

    boolean usesBitmap() {
        return hasBitmap;
    }

    /**
     * Row walk: address of the current non-null row's value as stored in the
     * chunk: the fixed-size value (GEOHASH at wire width, DECIMAL128/256 in wire
     * word order), the SYMBOL varint, the variable-size bytes, or the byte that
     * holds a BOOLEAN bit. 0 for a null row.
     */
    long valueAddr() {
        return valueAddr;
    }

    /**
     * Row walk: for the current non-null row of a variable-size column, the end
     * offset of its bytes within the concatenation of every chunk's bytes, which
     * is the offset the wire carries for it.
     */
    long valueEnd() {
        return bytesBefore + (valueAddr - bytesAddr) + valueLen;
    }

    /**
     * Row walk: length of the bytes at {@link #valueAddr()}: the type's width, the
     * varint's length, or the variable-size value's length; 0 for BOOLEAN and for
     * a null row.
     */
    int valueLen() {
        return valueLen;
    }

    private static int countNulls(long bitmap, int rows) {
        int nulls = 0;
        int words = rows >>> 6;
        for (int i = 0; i < words; i++) {
            nulls += Long.bitCount(Unsafe.getUnsafe().getLong(bitmap + 8L * i));
        }
        int bit = words << 6;
        long p = bitmap + ((long) words << 3);
        for (; bit + 8 <= rows; bit += 8, p++) {
            nulls += Integer.bitCount(Unsafe.getUnsafe().getByte(p) & 0xFF);
        }
        if (bit < rows) {
            nulls += Integer.bitCount(Unsafe.getUnsafe().getByte(p) & ((1 << (rows - bit)) - 1));
        }
        return nulls;
    }

    private int valueOffset(int index) {
        return Unsafe.getUnsafe().getInt(valuesAddr + 4L * index);
    }
}
