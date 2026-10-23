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
import io.questdb.client.std.IntHashSet;
import io.questdb.client.std.IntList;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Unsafe;

import java.nio.charset.StandardCharsets;

import static io.questdb.client.cutlass.qwp.client.sf.record.QwoFormat.*;

/**
 * Recovery-side validation of a QWO1 record found in untrusted memory, and the
 * in-place finalisation that lets recovery seal an open record.
 * <p>
 * {@link #validateSealed} accepts a sealed record only when all of it holds.
 * {@link #validateOpen} accepts an open record, possibly cut anywhere inside a
 * publish or a seal, and trusts only what its committed counts reach: blocks
 * not linked from {@code firstBlock}, rows past a block's row count, entries
 * past the dictionary count and chunks past the committed rows are ignored.
 * Both read nothing outside {@code [payloadAddr, payloadAddr + bound)} and check:
 * <ul>
 *     <li>header: magic, version {@link QwpConstants#VERSION}, the header CRC,
 *     flags with GORILLA and DELTA_SYMBOL_DICT set and nothing but them and
 *     DEFER_COMMIT, usedBytes in {@code [HEADER_SIZE, min(length, capacity)]};</li>
 *     <li>dictionary: the region inside {@code [HEADER_SIZE, bound)}, its
 *     dictCount entries parsing inside it, their CRC equal to dictWord's;</li>
 *     <li>blocks: firstBlock and every {@code D_NEXT} 0 or a descriptor inside
 *     {@code [HEADER_SIZE, bound)} whose {@code D_CRC} matches, no cycle, at most
 *     65535 of them, a row count in {@code [0, MAX_ROWS_PER_BLOCK]}; per column
 *     def a known type, a bitmap byte of 0 or 1 and a prefix in range (none for
 *     other types, a GEOHASH precision of 1 to 60, a DECIMAL scale of 0 or
 *     more);</li>
 *     <li>rows of a block with committed rows: each column's chunk chain inside
 *     {@code [HEADER_SIZE, bound)}, acyclic, every rowCap in
 *     {@code [1, MAX_CHUNK_ROWS]}, covering the rows; variable-size offsets
 *     starting at 0, ascending and within the chunk's byteCap; a GEOHASH or
 *     DECIMAL column with a committed non-null value carries its precision or
 *     scale; the row CRC recomputed by {@link QwoCrc} over a
 *     {@link QwoColumnCursor} walk equals the row word's.</li>
 * </ul>
 * A sealed record's bound is its usedBytes, and its blockCount must equal the
 * length of the block chain. An open record's bound is the smaller of the
 * reservation and the header's capacity; its blockCount is ignored (a cut
 * publish may leave it stale), and {@link #usedBytes()} reports the larger of
 * the stored usedBytes and the end of the structure the walk reached.
 * <p>
 * The structure is checked before the cursor follows it, because the cursor
 * trusts the offsets it reads. Validation allocates (recovery path) and frees
 * its off-heap scratch before returning. Not thread-safe.
 */
public final class QwoValidator {
    private static final int ALLOWED_FLAGS = QwpConstants.FLAG_DEFER_COMMIT
            | QwpConstants.FLAG_GORILLA
            | QwpConstants.FLAG_DELTA_SYMBOL_DICT;
    private static final int MAX_BLOCKS = 0xFFFF;
    private static final int MAX_GEOHASH_PRECISION = 60;
    private static final int REQUIRED_FLAGS = QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_DELTA_SYMBOL_DICT;
    private final IntList chunkByteCaps = new IntList();
    private final ObjList<QwoColumnCursor> cursors = new ObjList<>();
    private final QwoRecordReader reader = new QwoRecordReader();
    private final IntHashSet visitedBlocks = new IntHashSet();
    private final IntHashSet visitedChunks = new IntHashSet();
    private long addr;
    private int blockChainLength;
    private int bound;
    private int committedRows;
    private QwoCrc crc;
    private String detail;
    private long extent;
    private boolean isOpenValidated;
    private int usedBytes;

    /**
     * Committed rows across the record's blocks after a successful validation;
     * 0 after a failed one.
     */
    public int committedRows() {
        return committedRows;
    }

    /**
     * Why the last validation failed, naming the block, its table and the
     * column where one applies; null after a successful validation.
     */
    public String detail() {
        return detail;
    }

    /**
     * Prepares the open record the last call accepted for a recovery seal:
     * clears DEFER_COMMIT, so the recovered record commits any group opened
     * before it, stores blockCount as the length of the block chain and
     * usedBytes as {@link #usedBytes()}. None of these is under the header CRC,
     * so the header CRC stays valid. The record then passes
     * {@code validateSealed(payloadAddr, usedBytes())}; the envelope is the
     * caller's business.
     *
     * @throws IllegalStateException unless the last validation was a
     *                               successful {@link #validateOpen} of the record at {@code payloadAddr}
     */
    public void finalizeForRecovery(long payloadAddr) {
        if (!isOpenValidated || payloadAddr != addr) {
            throw new IllegalStateException("finalizeForRecovery needs a successful validateOpen of the same record first");
        }
        byte flags = Unsafe.getUnsafe().getByte(payloadAddr + OFF_FLAGS);
        Unsafe.getUnsafe().putByte(payloadAddr + OFF_FLAGS, (byte) (flags & ~QwpConstants.FLAG_DEFER_COMMIT));
        Unsafe.getUnsafe().putShort(payloadAddr + OFF_BLOCK_COUNT, (short) blockChainLength);
        Unsafe.getUnsafe().putInt(payloadAddr + OFF_USED_BYTES, usedBytes);
    }

    /**
     * Payload bytes the record occupies after a successful validation: a sealed
     * record's stored usedBytes; for an open record the larger of the stored
     * usedBytes and the end of the structure the walk reached. 0 after a failed
     * validation.
     */
    public int usedBytes() {
        return usedBytes;
    }

    /**
     * Validates the open record in {@code [payloadAddr, payloadAddr + capacity)},
     * trusting only its committed counts; see the class comment for the rules.
     *
     * @param capacity the reservation's payload bytes, all readable
     * @return true when the committed rows can be recovered; {@link #detail()}
     * says why not otherwise
     */
    public boolean validateOpen(long payloadAddr, int capacity) {
        return validate(payloadAddr, capacity, false);
    }

    /**
     * Validates the sealed record in {@code [payloadAddr, payloadAddr + payloadLen)};
     * see the class comment for the rules.
     *
     * @return true when every part of the record is valid; {@link #detail()}
     * says why not otherwise
     */
    public boolean validateSealed(long payloadAddr, int payloadLen) {
        return validate(payloadAddr, payloadLen, true);
    }

    private static String hex(int value) {
        return "0x" + Integer.toHexString(value);
    }

    private static boolean isKnownType(byte type) {
        switch (type) {
            case QwpConstants.TYPE_BOOLEAN:
            case QwpConstants.TYPE_BYTE:
            case QwpConstants.TYPE_SHORT:
            case QwpConstants.TYPE_INT:
            case QwpConstants.TYPE_LONG:
            case QwpConstants.TYPE_FLOAT:
            case QwpConstants.TYPE_DOUBLE:
            case QwpConstants.TYPE_SYMBOL:
            case QwpConstants.TYPE_TIMESTAMP:
            case QwpConstants.TYPE_DATE:
            case QwpConstants.TYPE_UUID:
            case QwpConstants.TYPE_LONG256:
            case QwpConstants.TYPE_GEOHASH:
            case QwpConstants.TYPE_VARCHAR:
            case QwpConstants.TYPE_TIMESTAMP_NANOS:
            case QwpConstants.TYPE_DOUBLE_ARRAY:
            case QwpConstants.TYPE_LONG_ARRAY:
            case QwpConstants.TYPE_DECIMAL64:
            case QwpConstants.TYPE_DECIMAL128:
            case QwpConstants.TYPE_DECIMAL256:
            case QwpConstants.TYPE_CHAR:
            case QwpConstants.TYPE_BINARY:
            case QwpConstants.TYPE_IPv4:
                return true;
            default:
                return false;
        }
    }

    /**
     * True when {@code count + 1} variable-size offsets at {@code offsets} start
     * at 0, never decrease and stay within {@code byteCap}.
     */
    private static boolean isOffsetsAscending(long offsets, int count, int byteCap) {
        int previous = Unsafe.getUnsafe().getInt(offsets);
        if (previous != 0) {
            return false;
        }
        for (int i = 1; i <= count; i++) {
            int offset = Unsafe.getUnsafe().getInt(offsets + 4L * i);
            if (offset < previous || offset > byteCap) {
                return false;
            }
            previous = offset;
        }
        return true;
    }

    /**
     * Ruling R4: no prefix for a type that has none, a GEOHASH precision of 1 to
     * 60, a DECIMAL scale of 0 or more; {@code prefix} is -1 for PREFIX_NONE.
     */
    private static boolean isPrefixInRange(byte type, byte kind, int prefix) {
        if (kind == PREFIX_NONE) {
            return true;
        }
        switch (type) {
            case QwpConstants.TYPE_GEOHASH:
                return kind == PREFIX_GEOHASH && prefix >= 1 && prefix <= MAX_GEOHASH_PRECISION;
            case QwpConstants.TYPE_DECIMAL64:
            case QwpConstants.TYPE_DECIMAL128:
            case QwpConstants.TYPE_DECIMAL256:
                return kind == PREFIX_DECIMAL && prefix >= 0;
            default:
                return false;
        }
    }

    private static boolean isPrefixType(byte type) {
        return type == QwpConstants.TYPE_GEOHASH
                || type == QwpConstants.TYPE_DECIMAL64
                || type == QwpConstants.TYPE_DECIMAL128
                || type == QwpConstants.TYPE_DECIMAL256;
    }

    private static String utf8(long addr, int len) {
        byte[] bytes = new byte[len];
        for (int i = 0; i < len; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(addr + i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private String blockLabel(int b, int index) {
        return "block " + index + " (table '" + utf8(reader.tableNameAddr(b), reader.tableNameLen(b)) + "')";
    }

    private String columnLabel(int b, int index, int col, int def) {
        return blockLabel(b, index) + " column " + col + " ('" + utf8(reader.columnNameAddr(def), reader.columnNameLen(def)) + "')";
    }

    private QwoColumnCursor cursorAt(int col) {
        while (cursors.size() <= col) {
            cursors.add(new QwoColumnCursor());
        }
        return cursors.getQuick(col);
    }

    /**
     * End offset of the descriptor at {@code b}, or -1 when its table name or a
     * column def runs past the bound. The caller checked {@code b < bound}.
     */
    private long descriptorEnd(int b) {
        long limit = addr + bound;
        long p = boundedSkipString(addr + b + D_NAME, limit);
        if (p < 0) {
            return -1;
        }
        for (int i = 0, n = reader.colCount(b); i < n; i++) {
            p = boundedSkipString(p, limit);
            if (p < 0 || p + COL_FIXED_SIZE > limit) {
                return -1;
            }
            p += COL_FIXED_SIZE;
        }
        return p - addr;
    }

    private boolean fail(String detail) {
        this.detail = detail;
        return false;
    }

    /**
     * Recomputes the row CRC of a block over the cursors {@link #validateChunks}
     * positioned, one per column, with the writer's stream definition.
     */
    private int rowCrc(int colCount, int rows) {
        int seed = Crc32c.INIT;
        for (int r = 0; r < rows; r++) {
            crc.begin(seed);
            for (int col = 0; col < colCount; col++) {
                QwoColumnCursor cursor = cursors.getQuick(col);
                boolean hasRow = cursor.next();
                assert hasRow;
                byte type = cursor.type();
                if (type == QwpConstants.TYPE_BOOLEAN) {
                    crc.putBoolean(cursor.usesBitmap(), !cursor.isNull(), cursor.booleanValue());
                } else {
                    crc.putValue(type, cursor.usesBitmap(), !cursor.isNull(), cursor.valueAddr(), cursor.valueLen());
                }
            }
            seed = crc.finish();
        }
        return seed;
    }

    private boolean validate(long payloadAddr, int len, boolean isSealed) {
        addr = payloadAddr;
        blockChainLength = 0;
        committedRows = 0;
        detail = null;
        isOpenValidated = false;
        usedBytes = 0;
        crc = new QwoCrc();
        try {
            if (validateRecord(len, isSealed)) {
                isOpenValidated = !isSealed;
                return true;
            }
            committedRows = 0;
            usedBytes = 0;
            return false;
        } finally {
            crc.close();
            crc = null;
        }
    }

    private boolean validateBlock(int b, int index) {
        if (b < HEADER_SIZE || b >= bound) {
            return fail("block " + index + " offset " + b + " outside [" + HEADER_SIZE + ", " + bound + ')');
        }
        if (!visitedBlocks.add(b)) {
            return fail("block chain cycles: block " + index + " links back to offset " + b);
        }
        long end = descriptorEnd(b);
        if (end < 0) {
            return fail("block " + index + " descriptor at offset " + b + " runs past the bound " + bound);
        }
        int storedCrc = Unsafe.getUnsafe().getInt(addr + b + D_CRC);
        int computedCrc = QwoFormat.descriptorCrc(addr + b);
        if (storedCrc != computedCrc) {
            return fail("block " + index + " descriptor crc mismatch: stored=" + hex(storedCrc) + ", computed=" + hex(computedCrc));
        }
        extent = Math.max(extent, end);
        long rowWord = Unsafe.getUnsafe().getLong(addr + b + D_ROW_WORD);
        int rows = rowCountOf(rowWord);
        if (rows < 0 || rows > MAX_ROWS_PER_BLOCK) {
            return fail(blockLabel(b, index) + " row count " + rows + " outside [0, " + MAX_ROWS_PER_BLOCK + ']');
        }
        int colCount = reader.colCount(b);
        int def = reader.columnDef(b, 0);
        for (int col = 0; col < colCount; col++) {
            if (!validateColumn(b, index, col, def, rows)) {
                return false;
            }
            def = reader.nextColumnDef(def);
        }
        if (rows > 0) {
            int storedRowCrc = crcOf(rowWord);
            int computedRowCrc = rowCrc(colCount, rows);
            if (storedRowCrc != computedRowCrc) {
                return fail(blockLabel(b, index) + " row crc mismatch: rows=" + rows
                        + ", stored=" + hex(storedRowCrc) + ", computed=" + hex(computedRowCrc));
            }
        }
        return true;
    }

    private boolean validateBlocks() {
        visitedBlocks.clear();
        long rows = 0;
        int index = 0;
        for (int b = reader.firstBlock(); b != 0; b = reader.nextBlock(b), index++) {
            if (index == MAX_BLOCKS) {
                return fail("block chain holds more than " + MAX_BLOCKS + " blocks");
            }
            if (!validateBlock(b, index)) {
                return false;
            }
            rows += reader.rowCount(b);
        }
        if (rows > Integer.MAX_VALUE) {
            return fail("committed rows overflow an int: " + rows);
        }
        blockChainLength = index;
        committedRows = (int) rows;
        return true;
    }

    /**
     * Checks the chunk chain that holds a column's {@code rows} committed rows
     * (offsets, capacities, extents, cycles, variable-size offsets) and leaves
     * the column's cursor positioned for the row walk.
     */
    private boolean validateChunks(int b, int index, int col, int def, int rows, byte type, boolean hasBitmap, int prefix) {
        boolean isVariableSize = isVariableSize(type);
        visitedChunks.clear();
        chunkByteCaps.clear();
        long covered = 0;
        int c = reader.firstChunk(def);
        while (covered < rows) {
            if (c == 0) {
                return fail(columnLabel(b, index, col, def) + " chunk chain ends after " + covered + " of " + rows + " committed rows");
            }
            if (c < HEADER_SIZE || (long) c + C_DATA > bound) {
                return fail(columnLabel(b, index, col, def) + " chunk offset " + c + " outside [" + HEADER_SIZE + ", " + bound + ')');
            }
            if (!visitedChunks.add(c)) {
                return fail(columnLabel(b, index, col, def) + " chunk chain cycles at offset " + c);
            }
            int rowCap = reader.chunkRowCap(c);
            if (rowCap < 1 || rowCap > MAX_CHUNK_ROWS) {
                return fail(columnLabel(b, index, col, def) + " chunk at offset " + c + " has rowCap " + rowCap
                        + " outside [1, " + MAX_CHUNK_ROWS + ']');
            }
            int byteCap = isVariableSize ? reader.chunkByteCap(c) : 0;
            if (byteCap < 0) {
                return fail(columnLabel(b, index, col, def) + " chunk at offset " + c + " has a negative byteCap " + byteCap);
            }
            long end = (long) c + C_DATA + chunkDataBytes(type, prefix, hasBitmap, rowCap, 0) + byteCap;
            if (end > bound) {
                return fail(columnLabel(b, index, col, def) + " chunk at offset " + c + " (rowCap " + rowCap
                        + ", byteCap " + byteCap + ") runs past the bound " + bound);
            }
            extent = Math.max(extent, end);
            chunkByteCaps.add(byteCap);
            covered += Math.min(rowCap, rows - covered);
            c = reader.nextChunk(c);
        }
        // the chain is in bounds, so the cursor's chunk walk visits exactly these chunks
        QwoColumnCursor cursor = cursorAt(col);
        cursor.of(reader, def, rows);
        long values = 0;
        for (int i = 0; cursor.nextChunk(); i++) {
            int valueCount = cursor.chunkValueCount();
            if (isVariableSize && !isOffsetsAscending(cursor.chunkValuesAddr(), valueCount, chunkByteCaps.getQuick(i))) {
                return fail(columnLabel(b, index, col, def) + " chunk " + i
                        + " has variable-size offsets that do not ascend from 0 within byteCap " + chunkByteCaps.getQuick(i));
            }
            values += valueCount;
        }
        cursor.rewind();
        if (values > 0 && prefix < 0 && isPrefixType(type)) {
            return fail(columnLabel(b, index, col, def) + " holds " + values + " non-null values but no "
                    + (type == QwpConstants.TYPE_GEOHASH ? "precision" : "scale"));
        }
        return true;
    }

    private boolean validateColumn(int b, int index, int col, int def, int rows) {
        byte type = reader.columnType(def);
        if (!isKnownType(type)) {
            return fail(columnLabel(b, index, col, def) + " has unknown type " + hex(type & 0xFF));
        }
        long tail = reader.columnNameAddr(def) + reader.columnNameLen(def);
        int bitmap = Unsafe.getUnsafe().getByte(tail + COL_BITMAP);
        if (bitmap != 0 && bitmap != 1) {
            return fail(columnLabel(b, index, col, def) + " has bitmap byte " + bitmap + ", not 0 or 1");
        }
        byte kind = reader.prefixKind(def);
        int prefix = reader.prefix(def);
        if (!isPrefixInRange(type, kind, prefix)) {
            return fail(columnLabel(b, index, col, def) + " has an out-of-range prefix: kind=" + kind + ", value=" + prefix);
        }
        return rows == 0 || validateChunks(b, index, col, def, rows, type, bitmap != 0, prefix);
    }

    private boolean validateDictionary() {
        int region = Unsafe.getUnsafe().getInt(addr + OFF_DICT_REGION);
        int cap = reader.dictCapacity();
        if (region < HEADER_SIZE || cap < 0 || (long) region + cap > bound) {
            return fail("dictionary region [" + region + ", " + ((long) region + cap) + ") outside ["
                    + HEADER_SIZE + ", " + bound + ')');
        }
        int count = reader.dictCount();
        int entriesLen = reader.dictEntriesLen();
        if (entriesLen < 0) {
            return fail("dictionary entries do not parse inside the region: count=" + count + ", region capacity=" + cap);
        }
        int storedCrc = crcOf(Unsafe.getUnsafe().getLong(addr + OFF_DICT_WORD));
        int computedCrc = Crc32c.updateUnsafe(Crc32c.INIT, reader.dictEntriesAddr(), entriesLen);
        if (storedCrc != computedCrc) {
            return fail("dictionary crc mismatch over " + count + " entries: stored=" + hex(storedCrc)
                    + ", computed=" + hex(computedCrc));
        }
        extent = Math.max(extent, (long) region + cap);
        return true;
    }

    private boolean validateRecord(int len, boolean isSealed) {
        if (len < HEADER_SIZE) {
            return fail((isSealed ? "payload length " : "capacity ") + len
                    + " is shorter than the record header (" + HEADER_SIZE + " bytes)");
        }
        reader.of(addr, len);
        int magic = Unsafe.getUnsafe().getInt(addr + OFF_MAGIC);
        if (magic != MAGIC_RECORD) {
            return fail("bad magic " + hex(magic));
        }
        int version = reader.version();
        if (version != QwpConstants.VERSION) {
            return fail("unsupported version " + version);
        }
        int storedHeaderCrc = Unsafe.getUnsafe().getInt(addr + OFF_HEADER_CRC);
        int computedHeaderCrc = QwoFormat.headerCrc(addr);
        if (storedHeaderCrc != computedHeaderCrc) {
            return fail("header crc mismatch: stored=" + hex(storedHeaderCrc) + ", computed=" + hex(computedHeaderCrc));
        }
        int flags = reader.flags() & 0xFF;
        if ((flags & REQUIRED_FLAGS) != REQUIRED_FLAGS || (flags & ~ALLOWED_FLAGS) != 0) {
            return fail("bad flags " + hex(flags));
        }
        int capacity = reader.capacity();
        int limit = Math.min(len, capacity);
        int stored = reader.usedBytes();
        if (stored < HEADER_SIZE || stored > limit) {
            return fail("usedBytes " + stored + " outside [" + HEADER_SIZE + ", " + limit + "] ("
                    + (isSealed ? "payload length " : "capacity ") + len + ", header capacity " + capacity + ')');
        }
        bound = isSealed ? stored : limit;
        reader.of(addr, bound);
        extent = HEADER_SIZE;
        if (!validateDictionary() || !validateBlocks()) {
            return false;
        }
        if (isSealed && reader.blockCount() != blockChainLength) {
            return fail("blockCount " + reader.blockCount() + " differs from the " + blockChainLength
                    + " blocks chained from firstBlock");
        }
        usedBytes = (int) Math.max(stored, extent);
        return true;
    }
}
