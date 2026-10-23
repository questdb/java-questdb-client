/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Unsafe;

import java.util.Arrays;

/**
 * EXPERIMENT (local only, depth 3): merges consecutive QWP frames read from the
 * SF ring into ONE QWP message and collapses consecutive table blocks that carry
 * the same table name and the same column definitions into one block, so the
 * server decodes one message, one block of k rows, one ack and one commit.
 * <p>
 * Member frames are added with {@link #add(long, int)}, which parses the header
 * and the delta symbol-dictionary section and rejects a frame that cannot be
 * combined with the members already held (different non-commit flags, a
 * non-contiguous and non-superset delta). {@link #write(QwpBufferWriter)} then
 * emits the merged message: one header (flags of the last member), the merged
 * delta section, then the table blocks.
 * <p>
 * Column data layouts handled by the block merge (per column: null flag byte,
 * optional null bitmap of ceil(rows/8) bytes, LSB-first row order, then the
 * NON-NULL values only):
 * <ul>
 *   <li>fixed width (BYTE..LONG256 incl. DATE, UUID): values concatenated;</li>
 *   <li>TIMESTAMP/TIMESTAMP_NANOS: with FLAG_GORILLA the column carries an
 *   encoding byte; members use encoding 0 (raw) for up to two values, which is
 *   what one-row frames produce, and the merged column is written as encoding 0
 *   with the raw values concatenated. A member block with Gorilla-encoded data
 *   (more than two values) is not parsed and is copied verbatim;</li>
 *   <li>SYMBOL in delta-dictionary mode: global ids as varints, concatenated;</li>
 *   <li>VARCHAR/BINARY: offsets rebased by the preceding members' data sizes,
 *   bytes concatenated;</li>
 *   <li>BOOLEAN, GEOHASH, DECIMAL*, arrays: not parsed; the block and the rest
 *   of that member's blocks are copied verbatim.</li>
 * </ul>
 */
final class ExpFrameMerger {
    private static final int COL_BITMAP = 0;
    private static final int COL_STRIDE = 4;
    private static final int COL_VALUES = 1;
    private static final int COL_VALUES_LEN = 2;
    private static final int COL_VALUE_COUNT = 3;
    private static final int INITIAL_MEMBERS = 64;
    private int[] deltaRegionLen = new int[INITIAL_MEMBERS];
    // merged delta section: regions of [len varint][utf8] entries to concatenate
    private long[] deltaRegionPtr = new long[INITIAL_MEMBERS];
    private int deltaRegions;
    private byte firstFlags;
    private byte firstVersion;
    private boolean lastDeferred;
    private byte lastFlags;
    private long[] memberAddr = new long[INITIAL_MEMBERS];
    private int memberCount;
    private int[] memberLen = new int[INITIAL_MEMBERS];
    // per member: where the table blocks start and how many there are
    private int[] memberTableCount = new int[INITIAL_MEMBERS];
    private long[] memberTablesPtr = new long[INITIAL_MEMBERS];
    private long mergedDeltaCount;
    private long mergedDeltaStart;
    private int outputBlocks;
    // current run of mergeable blocks with identical table name + schema
    private long[] runCols = new long[INITIAL_MEMBERS * COL_STRIDE * 4];
    private int runColCount;
    private int runLen;
    private int runNameLen;
    private long runNamePtr;
    private int[] runRows = new int[INITIAL_MEMBERS];
    private int runSchemaLen;
    private long runSchemaPtr;
    private byte[] runTypes = new byte[16];
    // scratch for parsing one block
    private long[] scratchCols = new long[16 * COL_STRIDE];
    private byte[] scratchTypes = new byte[16];
    private long totalBlocksMerged;
    private int totalMemberBytes;

    /**
     * Adds a frame to the merge set. Returns false (and leaves the set unchanged)
     * when the frame is not a plain QWP message this merger can combine with the
     * members already held. The first member always fits.
     */
    boolean add(long addr, int len) {
        if (len < QwpConstants.HEADER_SIZE
                || Unsafe.getUnsafe().getInt(addr) != QwpConstants.MAGIC_MESSAGE) {
            return false;
        }
        byte version = Unsafe.getUnsafe().getByte(addr + 4);
        byte flags = Unsafe.getUnsafe().getByte(addr + QwpConstants.HEADER_OFFSET_FLAGS);
        int tableCount = Unsafe.getUnsafe().getShort(addr + 6) & 0xFFFF;
        int payloadLen = Unsafe.getUnsafe().getInt(addr + 8);
        if (payloadLen < 0 || QwpConstants.HEADER_SIZE + payloadLen != len) {
            return false;
        }
        if ((flags & QwpConstants.FLAG_ZSTD) != 0) {
            return false;
        }
        if (memberCount > 0) {
            if (version != firstVersion
                    || (flags & ~QwpConstants.FLAG_DEFER_COMMIT) != (firstFlags & ~QwpConstants.FLAG_DEFER_COMMIT)) {
                return false;
            }
        }
        long p = addr + QwpConstants.HEADER_SIZE;
        long end = addr + len;
        boolean delta = (flags & QwpConstants.FLAG_DELTA_SYMBOL_DICT) != 0;
        long dStart = 0;
        long dCount = 0;
        long entriesPtr = 0;
        int entriesLen = 0;
        if (delta) {
            long enc = readVarint(p, end);
            if (enc < 0) {
                return false;
            }
            dStart = enc >>> 3;
            p += enc & 7;
            enc = readVarint(p, end);
            if (enc < 0) {
                return false;
            }
            dCount = enc >>> 3;
            p += enc & 7;
            entriesPtr = p;
            for (long i = 0; i < dCount; i++) {
                enc = readVarint(p, end);
                if (enc < 0) {
                    return false;
                }
                p += (enc & 7) + (enc >>> 3);
                if (p > end) {
                    return false;
                }
            }
            entriesLen = (int) (p - entriesPtr);
        }
        // delta compatibility with the members already held
        int regionsBefore = deltaRegions;
        long startBefore = mergedDeltaStart;
        long countBefore = mergedDeltaCount;
        if (memberCount == 0) {
            mergedDeltaStart = dStart;
            mergedDeltaCount = 0;
            deltaRegions = 0;
            if (dCount > 0) {
                mergedDeltaCount = dCount;
                pushRegion(entriesPtr, entriesLen);
            }
        } else if (dCount == 0) {
            if (mergedDeltaCount == 0) {
                mergedDeltaStart = dStart; // every member passed the coverage guard; the last start is the tightest
            } else if (dStart > mergedDeltaStart + mergedDeltaCount) {
                return false;
            }
        } else if (mergedDeltaCount == 0) {
            if (dStart > mergedDeltaStart) {
                // a hole between the carried start and this member's start; keep it simple
                return false;
            }
            mergedDeltaStart = dStart;
            mergedDeltaCount = dCount;
            pushRegion(entriesPtr, entriesLen);
        } else if (dStart == mergedDeltaStart + mergedDeltaCount) {
            mergedDeltaCount += dCount;
            pushRegion(entriesPtr, entriesLen);
        } else if (dStart == mergedDeltaStart && dCount >= mergedDeltaCount) {
            // full-dictionary mode: every frame repeats the dictionary from the same start; keep the superset
            deltaRegions = 0;
            mergedDeltaCount = dCount;
            pushRegion(entriesPtr, entriesLen);
        } else {
            deltaRegions = regionsBefore;
            mergedDeltaStart = startBefore;
            mergedDeltaCount = countBefore;
            return false;
        }
        if (memberCount == memberAddr.length) {
            int n = memberCount * 2;
            memberAddr = Arrays.copyOf(memberAddr, n);
            memberLen = Arrays.copyOf(memberLen, n);
            memberTablesPtr = Arrays.copyOf(memberTablesPtr, n);
            memberTableCount = Arrays.copyOf(memberTableCount, n);
        }
        if (memberCount == 0) {
            firstFlags = flags;
            firstVersion = version;
        }
        memberAddr[memberCount] = addr;
        memberLen[memberCount] = len;
        memberTablesPtr[memberCount] = p;
        memberTableCount[memberCount] = tableCount;
        memberCount++;
        totalMemberBytes += len;
        lastFlags = flags;
        lastDeferred = (flags & QwpConstants.FLAG_DEFER_COMMIT) != 0;
        return true;
    }

    long blocksMerged() {
        return totalBlocksMerged;
    }

    boolean lastDeferred() {
        return lastDeferred;
    }

    int memberCount() {
        return memberCount;
    }

    int memberBytes() {
        return totalMemberBytes;
    }

    void reset() {
        memberCount = 0;
        totalMemberBytes = 0;
        deltaRegions = 0;
        mergedDeltaStart = 0;
        mergedDeltaCount = 0;
        lastDeferred = false;
        runLen = 0;
        outputBlocks = 0;
        totalBlocksMerged = 0;
    }

    /**
     * Writes the merged message. Returns the number of table blocks emitted.
     */
    int write(QwpBufferWriter w) {
        int hdr = w.getPosition();
        w.putInt(QwpConstants.MAGIC_MESSAGE);
        w.putByte(firstVersion);
        w.putByte(lastFlags);
        w.putShort((short) 0);
        w.putInt(0);
        if ((firstFlags & QwpConstants.FLAG_DELTA_SYMBOL_DICT) != 0) {
            w.putVarint(mergedDeltaStart);
            w.putVarint(mergedDeltaCount);
            for (int i = 0; i < deltaRegions; i++) {
                w.putBlockOfBytes(deltaRegionPtr[i], deltaRegionLen[i]);
            }
        }
        boolean gorilla = (firstFlags & QwpConstants.FLAG_GORILLA) != 0;
        boolean delta = (firstFlags & QwpConstants.FLAG_DELTA_SYMBOL_DICT) != 0;
        outputBlocks = 0;
        runLen = 0;
        for (int m = 0; m < memberCount; m++) {
            long p = memberTablesPtr[m];
            long end = memberAddr[m] + memberLen[m];
            int tables = memberTableCount[m];
            for (int t = 0; t < tables; t++) {
                long next = parseBlock(p, end, gorilla, delta);
                if (next < 0) {
                    // cannot parse: flush the run and copy the rest of this member verbatim
                    flushRun(w);
                    w.putBlockOfBytes(p, end - p);
                    outputBlocks += tables - t;
                    break;
                }
                appendToRun(w);
                p = next;
            }
        }
        flushRun(w);
        int payloadLen = w.getPosition() - hdr - QwpConstants.HEADER_SIZE;
        w.patchByte(hdr + 6, (byte) outputBlocks);
        w.patchByte(hdr + 6 + 1, (byte) (outputBlocks >>> 8));
        w.patchInt(hdr + 8, payloadLen);
        return outputBlocks;
    }

    private static int bitmapBytes(int rows) {
        return (rows + 7) >>> 3;
    }

    private static int countBits(long addr, int rows) {
        int count = 0;
        int full = rows >>> 3;
        for (int i = 0; i < full; i++) {
            count += Integer.bitCount(Unsafe.getUnsafe().getByte(addr + i) & 0xFF);
        }
        int rem = rows & 7;
        if (rem > 0) {
            count += Integer.bitCount(Unsafe.getUnsafe().getByte(addr + full) & ((1 << rem) - 1));
        }
        return count;
    }

    // same packing as CursorWebSocketSendLoop.readVarintAt: (value << 3) | bytes, -1 when malformed
    private static long readVarint(long p, long limit) {
        long value = 0;
        int shift = 0;
        int bytes = 0;
        while (p < limit && bytes < 6) {
            byte b = Unsafe.getUnsafe().getByte(p++);
            value |= (long) (b & 0x7F) << shift;
            bytes++;
            if ((b & 0x80) == 0) {
                return (value << 3) | bytes;
            }
            shift += 7;
        }
        return -1L;
    }

    // the scratch block (just parsed) joins the current run or starts a new one
    private void appendToRun(QwpBufferWriter w) {
        if (runLen > 0 && !sameIdentity()) {
            flushRun(w);
        }
        if (runLen == 0) {
            runNamePtr = scratchNamePtr;
            runNameLen = scratchNameLen;
            runSchemaPtr = scratchSchemaPtr;
            runSchemaLen = scratchSchemaLen;
            runColCount = scratchColCount;
            if (runTypes.length < runColCount) {
                runTypes = new byte[runColCount];
            }
            System.arraycopy(scratchTypes, 0, runTypes, 0, runColCount);
        }
        if (runLen == runRows.length) {
            runRows = Arrays.copyOf(runRows, runLen * 2);
        }
        int need = (runLen + 1) * runColCount * COL_STRIDE;
        if (runCols.length < need) {
            runCols = Arrays.copyOf(runCols, Math.max(need, runCols.length * 2));
        }
        runRows[runLen] = scratchRows;
        System.arraycopy(scratchCols, 0, runCols, runLen * runColCount * COL_STRIDE, runColCount * COL_STRIDE);
        runLen++;
    }

    private void flushRun(QwpBufferWriter w) {
        if (runLen == 0) {
            return;
        }
        int totalRows = 0;
        for (int i = 0; i < runLen; i++) {
            totalRows += runRows[i];
        }
        w.putVarint(runNameLen);
        w.putBlockOfBytes(runNamePtr, runNameLen);
        w.putVarint(totalRows);
        w.putVarint(runColCount);
        w.putBlockOfBytes(runSchemaPtr, runSchemaLen);
        boolean gorilla = (firstFlags & QwpConstants.FLAG_GORILLA) != 0;
        for (int c = 0; c < runColCount; c++) {
            boolean anyNulls = false;
            for (int i = 0; i < runLen; i++) {
                if (runCols[(i * runColCount + c) * COL_STRIDE + COL_BITMAP] != 0) {
                    anyNulls = true;
                    break;
                }
            }
            if (anyNulls) {
                w.putByte((byte) 1);
                int bytes = bitmapBytes(totalRows);
                w.ensureCapacity(bytes);
                long dst = w.getWriteAddress();
                for (int i = 0; i < bytes; i++) {
                    Unsafe.getUnsafe().putByte(dst + i, (byte) 0);
                }
                int bit = 0;
                for (int i = 0; i < runLen; i++) {
                    long bm = runCols[(i * runColCount + c) * COL_STRIDE + COL_BITMAP];
                    int rows = runRows[i];
                    if (bm != 0) {
                        for (int r = 0; r < rows; r++) {
                            if ((Unsafe.getUnsafe().getByte(bm + (r >>> 3)) & (1 << (r & 7))) != 0) {
                                int b = bit + r;
                                long a = dst + (b >>> 3);
                                Unsafe.getUnsafe().putByte(a, (byte) (Unsafe.getUnsafe().getByte(a) | (1 << (b & 7))));
                            }
                        }
                    }
                    bit += rows;
                }
                w.skip(bytes);
            } else {
                w.putByte((byte) 0);
            }
            byte type = runTypes[c];
            switch (type) {
                case QwpConstants.TYPE_VARCHAR:
                case QwpConstants.TYPE_BINARY: {
                    w.putInt(0);
                    int base = 0;
                    for (int i = 0; i < runLen; i++) {
                        int ix = (i * runColCount + c) * COL_STRIDE;
                        long offsets = runCols[ix + COL_VALUES];
                        int vc = (int) runCols[ix + COL_VALUE_COUNT];
                        for (int v = 1; v <= vc; v++) {
                            w.putInt(Unsafe.getUnsafe().getInt(offsets + (long) v * 4) + base);
                        }
                        base += Unsafe.getUnsafe().getInt(offsets + (long) vc * 4);
                    }
                    for (int i = 0; i < runLen; i++) {
                        int ix = (i * runColCount + c) * COL_STRIDE;
                        long offsets = runCols[ix + COL_VALUES];
                        int vc = (int) runCols[ix + COL_VALUE_COUNT];
                        int dataLen = Unsafe.getUnsafe().getInt(offsets + (long) vc * 4);
                        w.putBlockOfBytes(offsets + (long) (vc + 1) * 4, dataLen);
                    }
                    break;
                }
                case QwpConstants.TYPE_TIMESTAMP:
                case QwpConstants.TYPE_TIMESTAMP_NANOS:
                    if (gorilla) {
                        w.putByte((byte) 0); // uncompressed
                    }
                    // fall through
                default:
                    for (int i = 0; i < runLen; i++) {
                        int ix = (i * runColCount + c) * COL_STRIDE;
                        w.putBlockOfBytes(runCols[ix + COL_VALUES], runCols[ix + COL_VALUES_LEN]);
                    }
            }
        }
        if (runLen > 1) {
            totalBlocksMerged += runLen - 1;
        }
        outputBlocks++;
        runLen = 0;
    }

    private int scratchColCount;
    private int scratchNameLen;
    private long scratchNamePtr;
    private int scratchRows;
    private int scratchSchemaLen;
    private long scratchSchemaPtr;

    /**
     * Parses one table block at p into the scratch fields. Returns the address of
     * the next block, or -1 when the block cannot be parsed (unsupported column
     * type, Gorilla-encoded timestamps, malformed data).
     */
    private long parseBlock(long p, long end, boolean gorilla, boolean delta) {
        long enc = readVarint(p, end);
        if (enc < 0) {
            return -1;
        }
        int nameLen = (int) (enc >>> 3);
        p += enc & 7;
        scratchNamePtr = p;
        scratchNameLen = nameLen;
        p += nameLen;
        enc = readVarint(p, end);
        if (enc < 0) {
            return -1;
        }
        int rows = (int) (enc >>> 3);
        p += enc & 7;
        enc = readVarint(p, end);
        if (enc < 0) {
            return -1;
        }
        int cols = (int) (enc >>> 3);
        p += enc & 7;
        if (rows < 0 || cols < 0 || cols > QwpConstants.MAX_COLUMNS_PER_TABLE) {
            return -1;
        }
        if (scratchTypes.length < cols) {
            scratchTypes = new byte[cols];
            scratchCols = new long[cols * COL_STRIDE];
        }
        scratchSchemaPtr = p;
        for (int c = 0; c < cols; c++) {
            enc = readVarint(p, end);
            if (enc < 0) {
                return -1;
            }
            p += (enc & 7) + (enc >>> 3);
            if (p >= end) {
                return -1;
            }
            scratchTypes[c] = Unsafe.getUnsafe().getByte(p++);
        }
        scratchSchemaLen = (int) (p - scratchSchemaPtr);
        scratchRows = rows;
        scratchColCount = cols;
        for (int c = 0; c < cols; c++) {
            if (p >= end) {
                return -1;
            }
            int ix = c * COL_STRIDE;
            byte nullFlag = Unsafe.getUnsafe().getByte(p++);
            long bitmap = 0;
            int nulls = 0;
            if (nullFlag != 0) {
                bitmap = p;
                p += bitmapBytes(rows);
                if (p > end) {
                    return -1;
                }
                nulls = countBits(bitmap, rows);
            }
            int valueCount = rows - nulls;
            scratchCols[ix + COL_BITMAP] = bitmap;
            scratchCols[ix + COL_VALUE_COUNT] = valueCount;
            byte type = scratchTypes[c];
            long valuesPtr;
            long valuesLen;
            switch (type) {
                case QwpConstants.TYPE_TIMESTAMP:
                case QwpConstants.TYPE_TIMESTAMP_NANOS:
                    if (gorilla) {
                        if (p >= end) {
                            return -1;
                        }
                        byte encoding = Unsafe.getUnsafe().getByte(p++);
                        if (encoding != 0) {
                            return -1; // Gorilla bit stream: not merged in this experiment
                        }
                    }
                    valuesPtr = p;
                    valuesLen = (long) valueCount * 8;
                    break;
                case QwpConstants.TYPE_SYMBOL: {
                    if (!delta) {
                        return -1;
                    }
                    valuesPtr = p;
                    long q = p;
                    for (int v = 0; v < valueCount; v++) {
                        long e = readVarint(q, end);
                        if (e < 0) {
                            return -1;
                        }
                        q += e & 7;
                    }
                    valuesLen = q - p;
                    break;
                }
                case QwpConstants.TYPE_VARCHAR:
                case QwpConstants.TYPE_BINARY: {
                    valuesPtr = p;
                    long offsetsLen = (long) (valueCount + 1) * 4;
                    if (p + offsetsLen > end) {
                        return -1;
                    }
                    int dataLen = Unsafe.getUnsafe().getInt(p + (long) valueCount * 4);
                    if (dataLen < 0) {
                        return -1;
                    }
                    valuesLen = offsetsLen + dataLen;
                    break;
                }
                default: {
                    int size = QwpConstants.getFixedTypeSize(type);
                    if (size <= 0) {
                        return -1; // BOOLEAN, GEOHASH, arrays, decimals: not merged in this experiment
                    }
                    valuesPtr = p;
                    valuesLen = (long) valueCount * size;
                }
            }
            if (valuesPtr + valuesLen > end) {
                return -1;
            }
            scratchCols[ix + COL_VALUES] = valuesPtr;
            scratchCols[ix + COL_VALUES_LEN] = valuesLen;
            p = valuesPtr + valuesLen;
        }
        return p;
    }

    private void pushRegion(long ptr, int len) {
        if (len <= 0) {
            return;
        }
        if (deltaRegions == deltaRegionPtr.length) {
            deltaRegionPtr = Arrays.copyOf(deltaRegionPtr, deltaRegions * 2);
            deltaRegionLen = Arrays.copyOf(deltaRegionLen, deltaRegions * 2);
        }
        deltaRegionPtr[deltaRegions] = ptr;
        deltaRegionLen[deltaRegions] = len;
        deltaRegions++;
    }

    private boolean sameIdentity() {
        if (scratchNameLen != runNameLen || scratchSchemaLen != runSchemaLen || scratchColCount != runColCount) {
            return false;
        }
        for (int i = 0; i < runNameLen; i++) {
            if (Unsafe.getUnsafe().getByte(scratchNamePtr + i) != Unsafe.getUnsafe().getByte(runNamePtr + i)) {
                return false;
            }
        }
        for (int i = 0; i < runSchemaLen; i++) {
            if (Unsafe.getUnsafe().getByte(scratchSchemaPtr + i) != Unsafe.getUnsafe().getByte(runSchemaPtr + i)) {
                return false;
            }
        }
        return true;
    }
}
