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

import io.questdb.client.std.Unsafe;

import static io.questdb.client.cutlass.qwp.client.sf.record.QwoFormat.*;

/**
 * Reads a QWO1 record laid out by {@link QwoRecordWriter}, by offsets. Block,
 * column def and chunk handles are offsets from the payload start; the getters
 * do no bounds checking, so a caller reading untrusted memory (recovery)
 * validates offsets against {@link #usedBytes()} and the payload length first.
 * The reader holds only the payload address and length and allocates nothing.
 */
public final class QwoRecordReader {
    private long addr;
    private int len;

    public int blockCount() {
        return Unsafe.getUnsafe().getShort(addr + OFF_BLOCK_COUNT) & 0xFFFF;
    }

    public int capacity() {
        return Unsafe.getUnsafe().getInt(addr + OFF_CAPACITY);
    }

    /**
     * Address of the chunk's null bitmap (bit set = null), or 0 when the column
     * does not use one.
     */
    public long chunkBitmapAddr(int c, boolean bitmap) {
        return bitmap ? addr + c + C_DATA : 0;
    }

    public int chunkByteCap(int c) {
        return Unsafe.getUnsafe().getInt(addr + c + C_BYTE_CAP);
    }

    /**
     * Address of a variable-size chunk's value bytes, after its {@code rowCap + 1}
     * offsets.
     */
    public long chunkBytesAddr(int c, boolean bitmap, int rowCap) {
        return addr + c + C_DATA + (bitmap ? (rowCap + 7) >>> 3 : 0) + 4L * (rowCap + 1);
    }

    /**
     * Address of a variable-size chunk's {@code u32} offsets, relative to
     * {@link #chunkBytesAddr}; the first one is 0.
     */
    public long chunkOffsetsAddr(int c, boolean bitmap) {
        return addr + c + C_DATA + (bitmap ? (chunkRowCap(c) + 7) >>> 3 : 0);
    }

    public int chunkRowCap(int c) {
        return Unsafe.getUnsafe().getInt(addr + c + C_ROW_CAP);
    }

    /**
     * Address of the chunk's values, which follow the bitmap for every type:
     * non-null values in row order at the column's width, BOOLEAN bits packed by
     * value slot, SYMBOL varints back to back.
     */
    public long chunkValuesAddr(int c, byte type, boolean bitmap) {
        return addr + c + C_DATA + (bitmap ? (chunkRowCap(c) + 7) >>> 3 : 0);
    }

    public int colCount(int b) {
        return Unsafe.getUnsafe().getShort(addr + b + D_COL_COUNT) & 0xFFFF;
    }

    /**
     * Offset of the {@code col}-th column def of block {@code b}: the varint
     * length of the column name.
     */
    public int columnDef(int b, int col) {
        int def = (int) (skipString(addr + b + D_NAME) - addr);
        for (int i = 0; i < col; i++) {
            def = nextColumnDef(def);
        }
        return def;
    }

    public long columnNameAddr(int def) {
        return addr + def + varintLength(addr + def);
    }

    public int columnNameLen(int def) {
        return readVarint(addr + def);
    }

    public byte columnType(int def) {
        return Unsafe.getUnsafe().getByte(tailAddr(def) + COL_TYPE);
    }

    public boolean columnUsesBitmap(int def) {
        return Unsafe.getUnsafe().getByte(tailAddr(def) + COL_BITMAP) != 0;
    }

    public int dictCapacity() {
        return Unsafe.getUnsafe().getInt(addr + OFF_DICT_CAPACITY);
    }

    /**
     * Committed dictionary entries, from the dictionary word.
     */
    public int dictCount() {
        return (int) (Unsafe.getUnsafe().getLong(addr + OFF_DICT_WORD) >>> 32);
    }

    public long dictEntriesAddr() {
        return addr + Unsafe.getUnsafe().getInt(addr + OFF_DICT_REGION);
    }

    /**
     * Byte length of the {@link #dictCount()} committed entries, found by walking
     * their varint lengths; -1 when the region lies outside the payload or the
     * entries overrun it.
     */
    public int dictEntriesLen() {
        int region = Unsafe.getUnsafe().getInt(addr + OFF_DICT_REGION);
        int cap = dictCapacity();
        if (region < HEADER_SIZE || cap < 0 || (long) region + cap > len) {
            return -1;
        }
        long start = addr + region;
        long limit = start + cap;
        long p = start;
        for (int i = 0, n = dictCount(); i < n; i++) {
            if (p >= limit) {
                return -1;
            }
            int strLen = readVarint(p);
            long next = p + varintLength(p) + strLen;
            if (strLen < 0 || next > limit) {
                return -1;
            }
            p = next;
        }
        return (int) (p - start);
    }

    public int dictStartId() {
        return Unsafe.getUnsafe().getInt(addr + OFF_DICT_START_ID);
    }

    /**
     * Offset of the first block's descriptor, 0 when none is published.
     */
    public int firstBlock() {
        return Unsafe.getUnsafe().getInt(addr + OFF_FIRST_BLOCK);
    }

    public int firstChunk(int def) {
        return Unsafe.getUnsafe().getInt(tailAddr(def) + COL_FIRST_CHUNK);
    }

    public byte flags() {
        return Unsafe.getUnsafe().getByte(addr + OFF_FLAGS);
    }

    public int nextBlock(int b) {
        return Unsafe.getUnsafe().getInt(addr + b + D_NEXT);
    }

    public int nextChunk(int c) {
        return Unsafe.getUnsafe().getInt(addr + c + C_NEXT);
    }

    /**
     * Offset of the column def after {@code def}; after the last def of a block
     * it is the end of that block's descriptor.
     */
    public int nextColumnDef(int def) {
        return (int) (tailAddr(def) - addr) + COL_FIXED_SIZE;
    }

    public void of(long payloadAddr, int payloadLen) {
        this.addr = payloadAddr;
        this.len = payloadLen;
    }

    /**
     * Decoded GEOHASH precision or DECIMAL scale, or -1 while the def carries no
     * prefix ({@link QwoFormat#PREFIX_NONE}).
     */
    public int prefix(int def) {
        long tail = tailAddr(def);
        if (Unsafe.getUnsafe().getByte(tail + COL_PREFIX_KIND) == PREFIX_NONE) {
            return -1;
        }
        int b0 = Unsafe.getUnsafe().getByte(tail + COL_PREFIX) & 0xFF;
        if (b0 < 0x80) {
            return b0;
        }
        return (b0 & 0x7F) | ((Unsafe.getUnsafe().getByte(tail + COL_PREFIX + 1) & 0x7F) << 7);
    }

    public byte prefixKind(int def) {
        return Unsafe.getUnsafe().getByte(tailAddr(def) + COL_PREFIX_KIND);
    }

    /**
     * Committed rows of block {@code b}, from its row word.
     */
    public int rowCount(int b) {
        return rowCountOf(Unsafe.getUnsafe().getLong(addr + b + D_ROW_WORD));
    }

    public long tableNameAddr(int b) {
        return addr + b + D_NAME + varintLength(addr + b + D_NAME);
    }

    public int tableNameLen(int b) {
        return readVarint(addr + b + D_NAME);
    }

    public int usedBytes() {
        return Unsafe.getUnsafe().getInt(addr + OFF_USED_BYTES);
    }

    public byte version() {
        return Unsafe.getUnsafe().getByte(addr + OFF_VERSION);
    }

    private long tailAddr(int def) {
        return skipString(addr + def);
    }
}
