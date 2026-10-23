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

import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.NativeBufferWriter;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Crc32c;
import io.questdb.client.std.IntList;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import org.jetbrains.annotations.TestOnly;

import static io.questdb.client.cutlass.qwp.client.sf.record.QwoFormat.*;

/**
 * Appends rows into an open QWO1 record over a caller-provided memory range,
 * one staged row at a time, and makes them recoverable with {@link #publish()}.
 * <p>
 * The record starts with the {@link QwoFormat#HEADER_SIZE}-byte header and the
 * dictionary region; descriptors and column chunks follow from a bump pointer.
 * A table gets a block (a descriptor) on its first row in the record, and a new
 * block whenever its staged column list changes. Each column of a block is a
 * chain of chunks whose row capacities start at 1 and double up to
 * {@link QwoFormat#MAX_CHUNK_ROWS}, shrinking when the record runs short. A
 * variable-size chunk whose bytes run out while it still has free rows is
 * relocated: a larger copy replaces it in the chain and the old one becomes dead
 * space.
 * <p>
 * Crash safety follows program order of the stores. An append writes only into
 * memory no published structure reaches, or into the free slots of published
 * chunks beyond their committed rows; links into published structure are
 * deferred to {@link #publish()}, which stores, in order: {@code usedBytes}, new
 * structure (new descriptors' CRCs, chunk links, block links, firstBlock,
 * blockCount), the dictionary word, and one row word per touched block. A process
 * killed between any two of those stores leaves at least the previously
 * published rows recoverable. The header CRC covers only fields fixed at
 * {@link #open}, so neither publish nor seal rewrites it.
 * <p>
 * Working state lives on the heap in per-block and per-column state objects,
 * pooled across records; the append path allocates nothing once they exist.
 * Not thread-safe.
 */
public final class QwoRecordWriter implements QuietCloseable {
    public static final int APPEND_NO_SPACE = 1;
    public static final int APPEND_OK = 0;
    private static final long DOUBLE_ARRAY_BASE = Unsafe.getUnsafe().arrayBaseOffset(double[].class);
    private static final byte FLAGS_OPEN = (byte) (QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_DELTA_SYMBOL_DICT);
    private static final long LONG_ARRAY_BASE = Unsafe.getUnsafe().arrayBaseOffset(long[].class);
    private static final int MIN_BYTES_PER_VALUE = 16;
    private static final int PLAN_NEW_CHUNK = 1;
    private static final int PLAN_NONE = 0;
    private static final int PLAN_RELOCATE = 2;
    private final ObjList<BlockState> blocks = new ObjList<>();
    private final QwoCrc crc = new QwoCrc();
    private final IntList touchedBlocks = new IntList();
    private int blockCount;
    private int capacity;
    private int committedBlockCount;
    private int committedDictCount;
    private int committedDictCrc;
    private int committedDictUsed;
    private int committedRowCount;
    private int committedUsedBytes;
    private int dictCapacity;
    private int dictCount;
    private int dictCrc;
    private int dictStartId;
    private int dictUsed;
    private long epoch;
    private boolean isOpen;
    private int lastRefusedDictBytes;
    private int maxRowsPerBlock = MAX_ROWS_PER_BLOCK;
    private long payloadAddr;
    private int pendingLinkCount;
    private long[] pendingLinks = new long[16];
    private int rowCount;
    private int usedBytes;

    /**
     * Marks the record closed without touching its memory; the caller zeroes or
     * reuses the range.
     */
    public void abandon() {
        isOpen = false;
    }

    /**
     * Appends the single row staged in {@code staged} to the table's block,
     * after the dictionary entries for ids {@code (dictEndId(), batchMaxSymbolId]}.
     * The row stays uncommitted until {@link #publish()}.
     *
     * @return {@link #APPEND_OK}, or {@link #APPEND_NO_SPACE} with the record
     * unchanged when the dictionary region, the record, the block's row limit or,
     * for a row that needs a new block, the {@link QwoFormat#MAX_BLOCKS} limit
     * cannot take the row; {@link #lastRefusedDictBytes()} then tells whether the
     * dictionary region was the cause
     * @throws IllegalArgumentException when {@code staged} does not hold exactly
     *                                  one row, a SYMBOL column without a null bitmap holds a negative id, or
     *                                  a GEOHASH or DECIMAL column has no null bitmap
     */
    public int appendStagedRow(QwpTableBuffer staged, GlobalSymbolDictionary dict, int batchMaxSymbolId) {
        checkOpen();
        if (staged.getRowCount() != 1) {
            throw new IllegalArgumentException("staged buffer must hold exactly one row [rows=" + staged.getRowCount() + ']');
        }
        lastRefusedDictBytes = 0;
        int firstNewId = dictStartId + dictCount;
        long dictNeed = 0;
        for (int id = firstNewId; id <= batchMaxSymbolId; id++) {
            dictNeed += stringBytes(dict.getSymbol(id));
        }
        if (dictNeed > dictCapacity - dictUsed) {
            lastRefusedDictBytes = (int) Math.min(Integer.MAX_VALUE, dictNeed);
            return APPEND_NO_SPACE;
        }

        BlockState block = existingBlock(staged);
        boolean isNewBlock = block == null;
        if (isNewBlock) {
            if (blockCount == MAX_BLOCKS) {
                // the header's blockCount and the frame's table count are u16
                return APPEND_NO_SPACE;
            }
            block = prepareBlock(staged);
        } else if (block.rowCount >= maxRowsPerBlock) {
            return APPEND_NO_SPACE;
        }
        int maxNominalRowCap = 0;
        for (int i = 0, n = block.colCount; i < n; i++) {
            ColumnState cs = block.columns.getQuick(i);
            if (!planColumn(cs, staged.getColumn(i))) {
                return APPEND_NO_SPACE;
            }
            if (cs.planKind != PLAN_NONE) {
                maxNominalRowCap = Math.max(maxNominalRowCap, cs.planNominalRowCap);
            }
        }
        long budget = (long) capacity - usedBytes - WIRE_MARGIN_FIXED
                - (long) WIRE_MARGIN_PER_BLOCK * (blockCount + (isNewBlock ? 1 : 0))
                - (isNewBlock ? block.descriptorSize : 0);
        if (!fitChunks(block, budget, maxNominalRowCap)) {
            return APPEND_NO_SPACE;
        }

        if (isNewBlock) {
            int previous = staged.qwoEpoch() == epoch ? staged.qwoBlock() : -1;
            block.previousOwnerBlock = previous < blockCount ? previous : -1;
            writeDescriptor(block, staged);
            staged.qwoBlock(blockCount);
            staged.qwoEpoch(epoch);
            blockCount++;
        }
        if (firstNewId <= batchMaxSymbolId) {
            writeDictionaryEntries(dict, firstNewId, batchMaxSymbolId);
        }
        crc.begin(block.crc);
        for (int i = 0, n = block.colCount; i < n; i++) {
            ColumnState cs = block.columns.getQuick(i);
            if (cs.prefix < 0 && cs.planPrefix >= 0) {
                storePrefix(cs, cs.planPrefix);
            }
            if (cs.planKind == PLAN_NEW_CHUNK) {
                allocateChunk(cs);
            } else if (cs.planKind == PLAN_RELOCATE) {
                relocateChunk(cs);
            }
            writeValue(cs, staged.getColumn(i));
        }
        block.crc = crc.finish();
        block.rowCount++;
        rowCount++;
        if (!block.isTouched) {
            block.isTouched = true;
            touchedBlocks.add(block.index);
        }
        return APPEND_OK;
    }

    /**
     * Columns of block {@code block}: the first that many columns of the staged
     * buffer the block was laid out from. Valid for {@code block < blockCount()}.
     */
    public int blockColumnCount(int block) {
        return blocks.getQuick(block).colCount;
    }

    /**
     * Blocks laid out so far, including those not yet published.
     */
    public int blockCount() {
        return blockCount;
    }

    public int capacity() {
        return capacity;
    }

    @Override
    public void close() {
        isOpen = false;
        crc.close();
        for (int i = 0, n = blocks.size(); i < n; i++) {
            blocks.getQuick(i).forget();
        }
    }

    /**
     * Last global symbol id with a published entry; {@code dictStartId() - 1}
     * when none.
     */
    public int committedDictEndId() {
        return dictStartId + committedDictCount - 1;
    }

    public int committedRowCount() {
        return committedRowCount;
    }

    /**
     * Bytes reserved for dictionary entries right after the header.
     */
    public int dictCapacity() {
        return dictCapacity;
    }

    /**
     * Last global symbol id with an entry in the dictionary region, published or
     * not; {@code dictStartId() - 1} when none.
     */
    public int dictEndId() {
        return dictStartId + dictCount - 1;
    }

    public int dictStartId() {
        return dictStartId;
    }

    /**
     * Incremented by every {@link #open}; a table's {@code qwoBlock} is valid
     * only while its {@code qwoEpoch} equals this.
     */
    public long epoch() {
        return epoch;
    }

    public boolean hasCommittedRows() {
        return committedRowCount > 0;
    }

    public boolean hasUncommittedRows() {
        return rowCount > committedRowCount;
    }

    public boolean isOpen() {
        return isOpen;
    }

    /**
     * Encoded bytes of the dictionary entries the last refused row needed, which
     * the dictionary region could not take; 0 when the last append succeeded or
     * was refused for another reason.
     */
    public int lastRefusedDictBytes() {
        return lastRefusedDictBytes;
    }

    /**
     * Starts a record over {@code [payloadAddr, payloadAddr + capacity)}: writes
     * the header with the flags GORILLA and DELTA_SYMBOL_DICT and the header CRC,
     * and reserves {@code dictReserve} bytes for dictionary entries right after
     * it. The envelope state words are the segment's business.
     */
    public void open(long payloadAddr, int capacity, byte version, int dictStartId, int dictReserve) {
        if (isOpen) {
            throw new IllegalStateException("record already open");
        }
        if (dictReserve < 0 || (long) HEADER_SIZE + dictReserve > capacity) {
            throw new IllegalArgumentException("record too small [capacity=" + capacity + ", dictReserve=" + dictReserve + ']');
        }
        this.payloadAddr = payloadAddr;
        this.capacity = capacity;
        this.dictStartId = dictStartId;
        this.dictCapacity = dictReserve;
        usedBytes = HEADER_SIZE + dictReserve;
        Unsafe.getUnsafe().putInt(payloadAddr + OFF_MAGIC, MAGIC_RECORD);
        Unsafe.getUnsafe().putByte(payloadAddr + OFF_VERSION, version);
        Unsafe.getUnsafe().putByte(payloadAddr + OFF_FLAGS, FLAGS_OPEN);
        Unsafe.getUnsafe().putShort(payloadAddr + OFF_BLOCK_COUNT, (short) 0);
        Unsafe.getUnsafe().putInt(payloadAddr + OFF_CAPACITY, capacity);
        Unsafe.getUnsafe().putInt(payloadAddr + OFF_FIRST_BLOCK, 0);
        Unsafe.getUnsafe().putInt(payloadAddr + OFF_DICT_REGION, HEADER_SIZE);
        Unsafe.getUnsafe().putInt(payloadAddr + OFF_DICT_CAPACITY, dictReserve);
        Unsafe.getUnsafe().putInt(payloadAddr + OFF_DICT_START_ID, dictStartId);
        Unsafe.getUnsafe().putLong(payloadAddr + OFF_DICT_WORD, 0L);
        Unsafe.getUnsafe().putInt(payloadAddr + OFF_USED_BYTES, usedBytes);
        Unsafe.getUnsafe().putInt(payloadAddr + OFF_RESERVED, 0);
        Unsafe.getUnsafe().putInt(payloadAddr + OFF_HEADER_CRC, QwoFormat.headerCrc(payloadAddr));
        blockCount = 0;
        committedBlockCount = 0;
        dictCount = 0;
        dictCrc = Crc32c.INIT;
        dictUsed = 0;
        committedDictCount = 0;
        committedDictCrc = Crc32c.INIT;
        committedDictUsed = 0;
        rowCount = 0;
        committedRowCount = 0;
        committedUsedBytes = usedBytes;
        lastRefusedDictBytes = 0;
        pendingLinkCount = 0;
        touchedBlocks.clear();
        epoch++;
        isOpen = true;
    }

    public long payloadAddress() {
        return payloadAddr;
    }

    /**
     * Makes every row appended so far recoverable. Stores, in program order:
     * {@code usedBytes}; the structure new since the last publish (new
     * descriptors' CRCs, chunk links into published chunks, block links,
     * firstBlock, blockCount); the dictionary word; one row word per touched
     * block. Then moves the committed marks to the working state.
     */
    public void publish() {
        checkOpen();
        final long base = payloadAddr;
        Unsafe.getUnsafe().putInt(base + OFF_USED_BYTES, usedBytes);
        for (int i = committedBlockCount; i < blockCount; i++) {
            long desc = base + blocks.getQuick(i).descOffset;
            Unsafe.getUnsafe().putInt(desc + D_CRC, QwoFormat.descriptorCrc(desc));
        }
        for (int i = 0; i < pendingLinkCount; i++) {
            long link = pendingLinks[i];
            Unsafe.getUnsafe().putInt(base + (int) (link >>> 32), (int) link);
        }
        for (int i = committedBlockCount; i < blockCount; i++) {
            int linkOffset = i == 0 ? OFF_FIRST_BLOCK : blocks.getQuick(i - 1).descOffset + D_NEXT;
            Unsafe.getUnsafe().putInt(base + linkOffset, blocks.getQuick(i).descOffset);
        }
        if (blockCount != committedBlockCount) {
            Unsafe.getUnsafe().putShort(base + OFF_BLOCK_COUNT, (short) blockCount);
        }
        if (dictCount != committedDictCount) {
            Unsafe.getUnsafe().putLong(base + OFF_DICT_WORD, ((long) dictCount << 32) | (dictCrc & 0xFFFFFFFFL));
        }
        for (int i = 0, n = touchedBlocks.size(); i < n; i++) {
            BlockState block = blocks.getQuick(touchedBlocks.getQuick(i));
            Unsafe.getUnsafe().putLong(base + block.descOffset + D_ROW_WORD, rowWord(block.rowCount, block.crc));
            block.commit();
        }
        touchedBlocks.clear();
        pendingLinkCount = 0;
        committedBlockCount = blockCount;
        committedDictCount = dictCount;
        committedDictCrc = dictCrc;
        committedDictUsed = dictUsed;
        committedRowCount = rowCount;
        committedUsedBytes = usedBytes;
    }

    /**
     * Discards everything appended since the last publish: restores each touched
     * block's cursors, row count and CRC; forgets the blocks allocated since,
     * pointing their tables back at their previous block in this record (or none);
     * restores the bump pointer and the dictionary cursor. Uncommitted bytes stay
     * in memory, unreachable, and get overwritten by later appends.
     */
    public void rollback() {
        checkOpen();
        for (int i = 0, n = touchedBlocks.size(); i < n; i++) {
            int index = touchedBlocks.getQuick(i);
            if (index < committedBlockCount) {
                blocks.getQuick(index).restore();
            }
        }
        for (int i = blockCount - 1; i >= committedBlockCount; i--) {
            BlockState block = blocks.getQuick(i);
            block.owner.qwoBlock(block.previousOwnerBlock);
            block.forget();
        }
        touchedBlocks.clear();
        pendingLinkCount = 0;
        blockCount = committedBlockCount;
        dictCount = committedDictCount;
        dictCrc = committedDictCrc;
        dictUsed = committedDictUsed;
        rowCount = committedRowCount;
        usedBytes = committedUsedBytes;
    }

    /**
     * Rows appended so far, published or not, across all blocks.
     */
    public int rowCount() {
        return rowCount;
    }

    /**
     * Seals the published state: rolls back any unpublished rows, stores the
     * flags (GORILLA, DELTA_SYMBOL_DICT and, when asked, DEFER_COMMIT),
     * blockCount and usedBytes, zeroes {@code [usedBytes, capacity)} and closes
     * the record. The counters keep describing the sealed record.
     *
     * @return the payload bytes the sealed record occupies
     */
    public int seal(boolean deferCommit) {
        checkOpen();
        if (hasUncommittedRows()) {
            rollback();
        }
        byte flags = deferCommit ? (byte) (FLAGS_OPEN | QwpConstants.FLAG_DEFER_COMMIT) : FLAGS_OPEN;
        Unsafe.getUnsafe().putByte(payloadAddr + OFF_FLAGS, flags);
        Unsafe.getUnsafe().putShort(payloadAddr + OFF_BLOCK_COUNT, (short) committedBlockCount);
        Unsafe.getUnsafe().putInt(payloadAddr + OFF_USED_BYTES, committedUsedBytes);
        Unsafe.getUnsafe().setMemory(payloadAddr + committedUsedBytes, capacity - committedUsedBytes, (byte) 0);
        isOpen = false;
        return committedUsedBytes;
    }

    @TestOnly
    public void setMaxRowsPerBlockForTest(int maxRowsPerBlock) {
        this.maxRowsPerBlock = maxRowsPerBlock;
    }

    /**
     * Bump pointer: payload bytes in use, published or not, including the
     * header and the whole dictionary reserve.
     */
    public int usedBytes() {
        return usedBytes;
    }

    private static long bytesPerValue(ColumnState cs) {
        long values = cs.totalValues + (cs.isPresent ? 1 : 0);
        long avg = values > 0 ? (cs.totalBytes + (cs.isPresent ? cs.planLen : 0)) / values : 0;
        return Math.max(MIN_BYTES_PER_VALUE, avg);
    }

    private static long chunkSize(ColumnState cs, int rowCap, long byteCap) {
        return C_DATA + chunkDataBytes(cs.type, cs.planPrefix, cs.hasBitmap, rowCap, 0) + byteCap;
    }

    private static long plannedByteCap(ColumnState cs, int rowCap, boolean isMinimal) {
        if (!cs.isVariableSize) {
            return 0;
        }
        long valueLen = cs.isPresent ? cs.planLen : 0;
        if (cs.planKind == PLAN_RELOCATE) {
            long used = cs.chunkBytes + valueLen;
            if (isMinimal) {
                return used;
            }
            long byteCap = Math.max(used, (long) rowCap * bytesPerValue(cs));
            if (rowCap >= cs.chunkRowCap) {
                byteCap = Math.max(byteCap, 2L * cs.chunkByteCap);
            }
            return byteCap;
        }
        return isMinimal ? valueLen : Math.max(valueLen, (long) rowCap * bytesPerValue(cs));
    }

    private static int plannedRowCap(ColumnState cs, int limit, boolean isMinimal) {
        if (cs.planKind == PLAN_RELOCATE) {
            return isMinimal ? cs.chunkRows + 1 : Math.max(cs.chunkRows + 1, Math.min(cs.planNominalRowCap, limit));
        }
        return isMinimal ? 1 : Math.min(cs.planNominalRowCap, limit);
    }

    private static int stagedPrefix(byte type, QwpTableBuffer.ColumnBuffer col) {
        switch (type) {
            case QwpConstants.TYPE_GEOHASH:
                return col.getGeoHashPrecision();
            case QwpConstants.TYPE_DECIMAL64:
            case QwpConstants.TYPE_DECIMAL128:
            case QwpConstants.TYPE_DECIMAL256:
                return col.getDecimalScale();
            default:
                return -1;
        }
    }

    private static int symbolId(QwpTableBuffer.ColumnBuffer col) {
        long aux = col.getAuxDataAddress();
        return Unsafe.getUnsafe().getInt(aux != 0 ? aux : col.getDataAddress());
    }

    private static long totalChunkBytes(BlockState block, int limit, boolean isMinimal) {
        long total = 0;
        for (int i = 0, n = block.colCount; i < n; i++) {
            ColumnState cs = block.columns.getQuick(i);
            if (cs.planKind != PLAN_NONE) {
                int rowCap = plannedRowCap(cs, limit, isMinimal);
                total += chunkSize(cs, rowCap, plannedByteCap(cs, rowCap, isMinimal));
            }
        }
        return total;
    }

    private void allocateChunk(ColumnState cs) {
        int rowCap = cs.planRowCap;
        int chunk = usedBytes;
        long p = payloadAddr + chunk;
        Unsafe.getUnsafe().putInt(p + C_NEXT, 0);
        Unsafe.getUnsafe().putInt(p + C_ROW_CAP, rowCap);
        Unsafe.getUnsafe().putInt(p + C_BYTE_CAP, cs.planByteCap);
        long data = p + C_DATA;
        int bitsBytes = (rowCap + 7) >>> 3;
        long values = data;
        if (cs.hasBitmap) {
            Unsafe.getUnsafe().setMemory(data, bitsBytes, (byte) 0);
            values += bitsBytes;
        }
        if (cs.type == QwpConstants.TYPE_BOOLEAN) {
            Unsafe.getUnsafe().setMemory(values, bitsBytes, (byte) 0);
        } else if (cs.isVariableSize) {
            Unsafe.getUnsafe().putInt(values, 0);
        }
        int link = cs.chunk == 0 ? cs.defTail + COL_FIRST_CHUNK : cs.chunk + C_NEXT;
        storeLink(link, chunk);
        cs.chunkLink = link;
        cs.chunk = chunk;
        cs.chunkRowCap = rowCap;
        cs.chunkByteCap = cs.planByteCap;
        cs.chunkRows = 0;
        cs.chunkValues = 0;
        cs.chunkBytes = 0;
        usedBytes += cs.planSize;
    }

    private void checkOpen() {
        if (!isOpen) {
            throw new IllegalStateException("record is not open");
        }
    }

    private BlockState existingBlock(QwpTableBuffer staged) {
        if (staged.qwoEpoch() != epoch) {
            return null;
        }
        int index = staged.qwoBlock();
        if (index < 0 || index >= blockCount) {
            return null;
        }
        BlockState block = blocks.getQuick(index);
        if (block.owner != staged || block.colCount != staged.getColumnCount()) {
            return null;
        }
        for (int i = 0, n = block.colCount; i < n; i++) {
            ColumnState cs = block.columns.getQuick(i);
            QwpTableBuffer.ColumnBuffer col = staged.getColumn(i);
            if (cs.column != col) {
                return null;
            }
            if (cs.prefix >= 0) {
                int prefix = stagedPrefix(cs.type, col);
                if (prefix >= 0 && prefix != cs.prefix) {
                    return null;
                }
            }
        }
        return block;
    }

    /**
     * Sizes the chunks the row needs so they fit {@code budget} bytes: nominal
     * row capacities first, then the largest uniform cap on them that fits, then
     * a last resort of one row (or the rows a relocated chunk already holds plus
     * one) sized to the value. Stores the choice in each column's plan.
     */
    private boolean fitChunks(BlockState block, long budget, int maxNominalRowCap) {
        int limit = MAX_CHUNK_ROWS;
        boolean isMinimal = false;
        if (totalChunkBytes(block, limit, false) > budget) {
            int lo = 1;
            int hi = maxNominalRowCap - 1;
            limit = 0;
            while (lo <= hi) {
                int mid = (lo + hi) >>> 1;
                if (totalChunkBytes(block, mid, false) <= budget) {
                    limit = mid;
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }
            if (limit == 0) {
                if (totalChunkBytes(block, 1, true) > budget) {
                    return false;
                }
                isMinimal = true;
            }
        }
        for (int i = 0, n = block.colCount; i < n; i++) {
            ColumnState cs = block.columns.getQuick(i);
            if (cs.planKind != PLAN_NONE) {
                int rowCap = plannedRowCap(cs, limit, isMinimal);
                long byteCap = plannedByteCap(cs, rowCap, isMinimal);
                cs.planRowCap = rowCap;
                cs.planByteCap = (int) byteCap;
                cs.planSize = (int) chunkSize(cs, rowCap, byteCap);
            }
        }
        return true;
    }

    /**
     * Records the row's value length and the chunk work it needs in {@code cs}.
     * Returns false when the value alone cannot fit a record of this capacity.
     */
    private boolean planColumn(ColumnState cs, QwpTableBuffer.ColumnBuffer col) {
        boolean present = col.getValueCount() > 0;
        cs.isPresent = present;
        cs.planPrefix = cs.prefix >= 0 ? cs.prefix : stagedPrefix(cs.type, col);
        long len = 0;
        if (present) {
            switch (cs.type) {
                case QwpConstants.TYPE_BOOLEAN:
                    break;
                case QwpConstants.TYPE_SYMBOL: {
                    int id = symbolId(col);
                    if (id < 0) {
                        throw new IllegalArgumentException("negative symbol id in column '" + col.getName() + "': " + id);
                    }
                    cs.planSymbolId = id;
                    len = NativeBufferWriter.varintSize(id);
                    break;
                }
                case QwpConstants.TYPE_VARCHAR:
                case QwpConstants.TYPE_BINARY: {
                    long offsets = col.getStringOffsetsAddress();
                    len = Unsafe.getUnsafe().getInt(offsets + Integer.BYTES) - Unsafe.getUnsafe().getInt(offsets);
                    break;
                }
                case QwpConstants.TYPE_DOUBLE_ARRAY:
                case QwpConstants.TYPE_LONG_ARRAY: {
                    int nDims = col.getArrayDims()[0];
                    int[] shapes = col.getArrayShapes();
                    long elements = 1;
                    for (int d = 0; d < nDims; d++) {
                        elements *= shapes[d];
                    }
                    len = 1L + (long) Integer.BYTES * nDims + 8L * elements;
                    break;
                }
                default:
                    len = valueWidth(cs.type, cs.planPrefix);
                    break;
            }
            if (len > capacity) {
                return false;
            }
        }
        cs.planLen = (int) len;
        if (cs.chunk == 0 || cs.chunkRows == cs.chunkRowCap) {
            cs.planKind = PLAN_NEW_CHUNK;
            cs.planNominalRowCap = cs.chunk == 0 ? 1 : Math.min(cs.chunkRowCap * 2, MAX_CHUNK_ROWS);
        } else if (cs.isVariableSize && present && (long) cs.chunkBytes + len > cs.chunkByteCap) {
            cs.planKind = PLAN_RELOCATE;
            cs.planNominalRowCap = cs.chunkRowCap;
        } else {
            cs.planKind = PLAN_NONE;
        }
        return true;
    }

    /**
     * Takes the pooled block state at index {@link #blockCount} and sets it up
     * for {@code staged}; it becomes part of the record only when the append
     * goes ahead.
     */
    private BlockState prepareBlock(QwpTableBuffer staged) {
        BlockState block;
        if (blockCount < blocks.size()) {
            block = blocks.getQuick(blockCount);
        } else {
            block = new BlockState();
            blocks.add(block);
        }
        block.index = blockCount;
        block.owner = staged;
        int colCount = staged.getColumnCount();
        block.colCount = colCount;
        block.rowCount = 0;
        block.crc = Crc32c.INIT;
        block.committedRowCount = 0;
        block.committedCrc = Crc32c.INIT;
        block.isTouched = false;
        long size = D_NAME + stringBytes(staged.getTableName());
        for (int i = 0; i < colCount; i++) {
            ColumnState cs;
            if (i < block.columns.size()) {
                cs = block.columns.getQuick(i);
            } else {
                cs = new ColumnState();
                block.columns.add(cs);
            }
            QwpTableBuffer.ColumnBuffer col = staged.getColumn(i);
            cs.of(col);
            if (cs.prefixKind != PREFIX_NONE && !cs.hasBitmap) {
                // a null in a column without a bitmap is a sentinel value, which would count as a
                // value carrying no precision or scale; the sender creates these columns nullable
                throw new IllegalArgumentException("column '" + col.getName()
                        + "' needs a null bitmap: it is a GEOHASH or DECIMAL column [type=" + cs.type + ']');
            }
            size += stringBytes(col.getName()) + COL_FIXED_SIZE;
        }
        block.descriptorSize = size;
        return block;
    }

    private void relocateChunk(ColumnState cs) {
        int rowCap = cs.planRowCap;
        int chunk = usedBytes;
        long p = payloadAddr + chunk;
        Unsafe.getUnsafe().putInt(p + C_NEXT, 0);
        Unsafe.getUnsafe().putInt(p + C_ROW_CAP, rowCap);
        Unsafe.getUnsafe().putInt(p + C_BYTE_CAP, cs.planByteCap);
        long oldData = payloadAddr + cs.chunk + C_DATA;
        long data = p + C_DATA;
        long oldOffsets = oldData;
        long offsets = data;
        if (cs.hasBitmap) {
            int bitsBytes = (rowCap + 7) >>> 3;
            Unsafe.getUnsafe().setMemory(data, bitsBytes, (byte) 0);
            int rows = cs.chunkRows;
            int fullBytes = rows >>> 3;
            Unsafe.getUnsafe().copyMemory(oldData, data, fullBytes);
            if ((rows & 7) != 0) {
                int mask = (1 << (rows & 7)) - 1;
                Unsafe.getUnsafe().putByte(data + fullBytes, (byte) (Unsafe.getUnsafe().getByte(oldData + fullBytes) & mask));
            }
            oldOffsets += (cs.chunkRowCap + 7) >>> 3;
            offsets += bitsBytes;
        }
        Unsafe.getUnsafe().copyMemory(oldOffsets, offsets, (long) Integer.BYTES * (cs.chunkValues + 1));
        Unsafe.getUnsafe().copyMemory(
                oldOffsets + (long) Integer.BYTES * (cs.chunkRowCap + 1),
                offsets + (long) Integer.BYTES * (rowCap + 1),
                cs.chunkBytes
        );
        // A column's first chunk holds one row, so it is full before its bytes can run out: no
        // relocation rewrites the def's first-chunk offset, which the descriptor CRC covers.
        assert cs.chunkLink != cs.defTail + COL_FIRST_CHUNK : "the first chunk of a column is never relocated";
        storeLink(cs.chunkLink, chunk);
        cs.chunk = chunk;
        cs.chunkRowCap = rowCap;
        cs.chunkByteCap = cs.planByteCap;
        usedBytes += cs.planSize;
    }

    /**
     * Stores a link to new structure: directly when the link lives in memory no
     * published structure reaches, otherwise deferred to {@link #publish()}.
     */
    private void storeLink(int linkOffset, int target) {
        if (linkOffset >= committedUsedBytes) {
            Unsafe.getUnsafe().putInt(payloadAddr + linkOffset, target);
            return;
        }
        if (pendingLinkCount == pendingLinks.length) {
            long[] grown = new long[pendingLinks.length * 2];
            System.arraycopy(pendingLinks, 0, grown, 0, pendingLinkCount);
            pendingLinks = grown;
        }
        pendingLinks[pendingLinkCount++] = ((long) linkOffset << 32) | (target & 0xFFFFFFFFL);
    }

    /**
     * Stores {@code [prefixKind][prefix0][prefix1][bitmap]} with one 4-byte
     * store. The prefix bytes sit outside the descriptor CRC, so the store is
     * safe in a published descriptor, and it precedes the row word that commits
     * the column's first value. A rollback leaves it in place: the staged column
     * keeps its precision or scale. The value is a one-byte varint: a GEOHASH
     * precision is at most 60 and a DECIMAL scale at most 76.
     */
    private void storePrefix(ColumnState cs, int prefix) {
        assert prefix >= 0 && prefix <= 0x7F : "prefix " + prefix + " needs a two-byte varint";
        int word = (cs.prefixKind & 0xFF)
                | (prefix & 0x7F) << 8
                | (cs.hasBitmap ? 1 : 0) << 24;
        Unsafe.getUnsafe().putInt(payloadAddr + cs.defTail + COL_PREFIX_KIND, word);
        cs.prefix = prefix;
    }

    private void writeDescriptor(BlockState block, QwpTableBuffer staged) {
        int desc = usedBytes;
        long p = payloadAddr + desc;
        Unsafe.getUnsafe().putInt(p + D_CRC, 0);
        Unsafe.getUnsafe().putInt(p + D_NEXT, 0);
        Unsafe.getUnsafe().putShort(p + D_COL_COUNT, (short) block.colCount);
        Unsafe.getUnsafe().putShort(p + D_FLAGS, (short) 0);
        Unsafe.getUnsafe().putLong(p + D_ROW_WORD, 0L);
        long q = p + D_NAME;
        q += putString(q, staged.getTableName());
        for (int i = 0, n = block.colCount; i < n; i++) {
            ColumnState cs = block.columns.getQuick(i);
            q += putString(q, staged.getColumn(i).getName());
            cs.defTail = (int) (q - payloadAddr);
            Unsafe.getUnsafe().putByte(q + COL_TYPE, cs.type);
            Unsafe.getUnsafe().putByte(q + COL_PREFIX_KIND, PREFIX_NONE);
            Unsafe.getUnsafe().putShort(q + COL_PREFIX, (short) 0);
            Unsafe.getUnsafe().putByte(q + COL_BITMAP, cs.hasBitmap ? (byte) 1 : (byte) 0);
            Unsafe.getUnsafe().putInt(q + COL_FIRST_CHUNK, 0);
            q += COL_FIXED_SIZE;
        }
        block.descOffset = desc;
        usedBytes += (int) (q - p);
    }

    private void writeDictionaryEntries(GlobalSymbolDictionary dict, int fromId, int toId) {
        long region = payloadAddr + HEADER_SIZE;
        for (int id = fromId; id <= toId; id++) {
            long p = region + dictUsed;
            int n = putString(p, dict.getSymbol(id));
            dictCrc = Crc32c.updateUnsafe(dictCrc, p, n);
            dictUsed += n;
            dictCount++;
        }
    }

    private void writeValue(ColumnState cs, QwpTableBuffer.ColumnBuffer col) {
        long data = payloadAddr + cs.chunk + C_DATA;
        long values = data;
        boolean present = cs.isPresent;
        if (cs.hasBitmap) {
            int row = cs.chunkRows;
            long bitAddr = data + (row >>> 3);
            int mask = 1 << (row & 7);
            int bits = Unsafe.getUnsafe().getByte(bitAddr);
            Unsafe.getUnsafe().putByte(bitAddr, (byte) (present ? bits & ~mask : bits | mask));
            values += (cs.chunkRowCap + 7) >>> 3;
        }
        cs.chunkRows++;
        byte type = cs.type;
        if (!present) {
            if (type == QwpConstants.TYPE_BOOLEAN) {
                crc.putBoolean(cs.hasBitmap, false, false);
            } else {
                crc.putValue(type, cs.hasBitmap, false, 0, 0);
            }
            return;
        }
        int slot = cs.chunkValues++;
        int len = cs.planLen;
        long src = col.getDataAddress();
        long dst;
        switch (type) {
            case QwpConstants.TYPE_BOOLEAN: {
                boolean value = Unsafe.getUnsafe().getByte(src) != 0;
                long bitAddr = values + (slot >>> 3);
                int mask = 1 << (slot & 7);
                int bits = Unsafe.getUnsafe().getByte(bitAddr);
                Unsafe.getUnsafe().putByte(bitAddr, (byte) (value ? bits | mask : bits & ~mask));
                crc.putBoolean(cs.hasBitmap, true, value);
                return;
            }
            case QwpConstants.TYPE_SYMBOL:
                dst = values + cs.chunkBytes;
                NativeBufferWriter.writeVarint(dst, cs.planSymbolId);
                cs.chunkBytes += len;
                break;
            case QwpConstants.TYPE_VARCHAR:
            case QwpConstants.TYPE_BINARY: {
                long offsets = col.getStringOffsetsAddress();
                long from = col.getStringDataAddress() + Unsafe.getUnsafe().getInt(offsets);
                dst = writeVariable(cs, values, len);
                Unsafe.getUnsafe().copyMemory(from, dst, len);
                break;
            }
            case QwpConstants.TYPE_DOUBLE_ARRAY:
            case QwpConstants.TYPE_LONG_ARRAY: {
                dst = writeVariable(cs, values, len);
                int nDims = col.getArrayDims()[0];
                int[] shapes = col.getArrayShapes();
                Unsafe.getUnsafe().putByte(dst, (byte) nDims);
                long p = dst + 1;
                long elements = 1;
                for (int d = 0; d < nDims; d++) {
                    Unsafe.getUnsafe().putInt(p, shapes[d]);
                    elements *= shapes[d];
                    p += Integer.BYTES;
                }
                if (elements > 0) {
                    if (type == QwpConstants.TYPE_DOUBLE_ARRAY) {
                        Unsafe.getUnsafe().copyMemory(col.getDoubleArrayData(), DOUBLE_ARRAY_BASE, null, p, 8L * elements);
                    } else {
                        Unsafe.getUnsafe().copyMemory(col.getLongArrayData(), LONG_ARRAY_BASE, null, p, 8L * elements);
                    }
                }
                break;
            }
            case QwpConstants.TYPE_GEOHASH: {
                dst = values + (long) slot * len;
                long value = Unsafe.getUnsafe().getLong(src);
                for (int b = 0; b < len; b++) {
                    Unsafe.getUnsafe().putByte(dst + b, (byte) (value >>> (b << 3)));
                }
                break;
            }
            case QwpConstants.TYPE_DECIMAL128:
                // the column buffer holds hi, lo; the wire and the chunk hold lo, hi
                dst = values + (long) slot * 16;
                Unsafe.getUnsafe().putLong(dst, Unsafe.getUnsafe().getLong(src + 8));
                Unsafe.getUnsafe().putLong(dst + 8, Unsafe.getUnsafe().getLong(src));
                break;
            case QwpConstants.TYPE_DECIMAL256:
                // the column buffer holds hh, hl, lh, ll; the wire and the chunk hold ll, lh, hl, hh
                dst = values + (long) slot * 32;
                Unsafe.getUnsafe().putLong(dst, Unsafe.getUnsafe().getLong(src + 24));
                Unsafe.getUnsafe().putLong(dst + 8, Unsafe.getUnsafe().getLong(src + 16));
                Unsafe.getUnsafe().putLong(dst + 16, Unsafe.getUnsafe().getLong(src + 8));
                Unsafe.getUnsafe().putLong(dst + 24, Unsafe.getUnsafe().getLong(src));
                break;
            case QwpConstants.TYPE_BYTE:
                dst = values + slot;
                Unsafe.getUnsafe().putByte(dst, Unsafe.getUnsafe().getByte(src));
                break;
            case QwpConstants.TYPE_SHORT:
            case QwpConstants.TYPE_CHAR:
                dst = values + (long) slot * 2;
                Unsafe.getUnsafe().putShort(dst, Unsafe.getUnsafe().getShort(src));
                break;
            case QwpConstants.TYPE_INT:
            case QwpConstants.TYPE_IPv4:
            case QwpConstants.TYPE_FLOAT:
                dst = values + (long) slot * 4;
                Unsafe.getUnsafe().putInt(dst, Unsafe.getUnsafe().getInt(src));
                break;
            case QwpConstants.TYPE_LONG:
            case QwpConstants.TYPE_DATE:
            case QwpConstants.TYPE_DOUBLE:
            case QwpConstants.TYPE_TIMESTAMP:
            case QwpConstants.TYPE_TIMESTAMP_NANOS:
            case QwpConstants.TYPE_DECIMAL64:
                dst = values + (long) slot * 8;
                Unsafe.getUnsafe().putLong(dst, Unsafe.getUnsafe().getLong(src));
                break;
            default:
                // UUID (lo, hi) and LONG256 are stored in wire order already
                dst = values + (long) slot * len;
                Unsafe.getUnsafe().copyMemory(src, dst, len);
                break;
        }
        crc.putValue(type, cs.hasBitmap, true, dst, len);
    }

    /**
     * Reserves {@code len} bytes for the next variable-size value of the current
     * chunk, records its end offset and returns where the bytes go.
     */
    private long writeVariable(ColumnState cs, long offsets, int len) {
        long dst = offsets + (long) Integer.BYTES * (cs.chunkRowCap + 1) + cs.chunkBytes;
        cs.chunkBytes += len;
        Unsafe.getUnsafe().putInt(offsets + (long) Integer.BYTES * cs.chunkValues, cs.chunkBytes);
        cs.totalBytes += len;
        cs.totalValues++;
        return dst;
    }

    private static final class BlockState {
        final ObjList<ColumnState> columns = new ObjList<>();
        int colCount;
        int committedCrc;
        int committedRowCount;
        int crc;
        int descOffset;
        long descriptorSize;
        int index;
        boolean isTouched;
        QwpTableBuffer owner;
        int previousOwnerBlock;
        int rowCount;

        void commit() {
            committedRowCount = rowCount;
            committedCrc = crc;
            isTouched = false;
            for (int i = 0; i < colCount; i++) {
                columns.getQuick(i).commit();
            }
        }

        void forget() {
            owner = null;
            isTouched = false;
            for (int i = 0, n = columns.size(); i < n; i++) {
                columns.getQuick(i).column = null;
            }
        }

        void restore() {
            rowCount = committedRowCount;
            crc = committedCrc;
            isTouched = false;
            for (int i = 0; i < colCount; i++) {
                columns.getQuick(i).restore();
            }
        }
    }

    private static final class ColumnState {
        int chunk;
        int chunkByteCap;
        int chunkBytes;
        int chunkLink;
        int chunkRowCap;
        int chunkRows;
        int chunkValues;
        QwpTableBuffer.ColumnBuffer column;
        int committedChunk;
        int committedChunkByteCap;
        int committedChunkBytes;
        int committedChunkLink;
        int committedChunkRowCap;
        int committedChunkRows;
        int committedChunkValues;
        long committedTotalBytes;
        int committedTotalValues;
        int defTail;
        boolean hasBitmap;
        boolean isPresent;
        boolean isVariableSize;
        int planByteCap;
        int planKind;
        int planLen;
        int planNominalRowCap;
        int planPrefix;
        int planRowCap;
        int planSize;
        int planSymbolId;
        int prefix;
        byte prefixKind;
        long totalBytes;
        int totalValues;
        byte type;

        void commit() {
            committedChunk = chunk;
            committedChunkByteCap = chunkByteCap;
            committedChunkBytes = chunkBytes;
            committedChunkLink = chunkLink;
            committedChunkRowCap = chunkRowCap;
            committedChunkRows = chunkRows;
            committedChunkValues = chunkValues;
            committedTotalBytes = totalBytes;
            committedTotalValues = totalValues;
        }

        void of(QwpTableBuffer.ColumnBuffer col) {
            column = col;
            type = col.getType();
            hasBitmap = col.usesNullBitmap();
            isVariableSize = QwoFormat.isVariableSize(type);
            switch (type) {
                case QwpConstants.TYPE_GEOHASH:
                    prefixKind = PREFIX_GEOHASH;
                    break;
                case QwpConstants.TYPE_DECIMAL64:
                case QwpConstants.TYPE_DECIMAL128:
                case QwpConstants.TYPE_DECIMAL256:
                    prefixKind = PREFIX_DECIMAL;
                    break;
                default:
                    prefixKind = PREFIX_NONE;
                    break;
            }
            prefix = -1;
            chunk = 0;
            chunkByteCap = 0;
            chunkBytes = 0;
            chunkLink = 0;
            chunkRowCap = 0;
            chunkRows = 0;
            chunkValues = 0;
            totalBytes = 0;
            totalValues = 0;
            commit();
        }

        void restore() {
            chunk = committedChunk;
            chunkByteCap = committedChunkByteCap;
            chunkBytes = committedChunkBytes;
            chunkLink = committedChunkLink;
            chunkRowCap = committedChunkRowCap;
            chunkRows = committedChunkRows;
            chunkValues = committedChunkValues;
            totalBytes = committedTotalBytes;
            totalValues = committedTotalValues;
        }
    }
}
