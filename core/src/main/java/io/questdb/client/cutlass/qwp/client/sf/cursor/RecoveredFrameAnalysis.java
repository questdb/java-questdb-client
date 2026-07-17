/*******************************************************************************
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

package io.questdb.client.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.Utf8s;

/**
 * Engine-owned result of the one ordered frame walk performed during recovery.
 * It folds commit-boundary detection, delta extrema, gap detection and the raw
 * symbol suffix into the same pass. Running state is checkpointed only at a
 * commit-bearing frame, so a deferred orphan tail can be scanned without ever
 * leaking its metadata or symbols into the committed result.
 */
final class RecoveredFrameAnalysis implements QuietCloseable {

    private static final int MAX_RAW_BYTES = Integer.MAX_VALUE - 8;
    private final long ackedFsn;
    private final int baseline;
    private long committedBoundaryFsn = -1L;
    private long committedCoverage;
    private boolean committedGap;
    private long committedMaxDeltaEnd;
    private long committedMaxDeltaStart;
    private int committedRawCount;
    private int committedRawLen;
    private long framesVisited;
    private boolean runningGap;
    private boolean runningUnackedGap;
    private long runningMaxDeltaEnd;
    private long runningMaxDeltaStart;
    private long symbolEntriesVisited;
    private long rawAddr;
    private int rawCapacity;
    private int runningRawCount;
    private int runningRawLen;
    private long runningCoverage;

    RecoveredFrameAnalysis(int baseline, long ackedFsn) {
        this.baseline = baseline;
        this.ackedFsn = ackedFsn;
        this.runningCoverage = baseline;
        this.committedCoverage = baseline;
    }

    void accept(long fsn, long payload, int payloadLen) {
        framesVisited++;
        boolean isQwp = payloadLen >= QwpConstants.HEADER_SIZE
                && payloadLen > QwpConstants.HEADER_OFFSET_FLAGS
                && Unsafe.getUnsafe().getInt(payload) == QwpConstants.MAGIC_MESSAGE;
        byte flags = isQwp
                ? Unsafe.getUnsafe().getByte(payload + QwpConstants.HEADER_OFFSET_FLAGS)
                : 0;
        if (isQwp && (flags & QwpConstants.FLAG_DELTA_SYMBOL_DICT) != 0) {
            foldDelta(fsn, payload + QwpConstants.HEADER_SIZE, payload + payloadLen);
        }

        // Only a positively identified deferred QWP frame can belong to an
        // uncommitted tail. Short, foreign or otherwise non-QWP payloads remain
        // retirement barriers, matching findLastFrameFsnWithoutPayloadFlag.
        if (!isQwp || (flags & QwpConstants.FLAG_DEFER_COMMIT) == 0) {
            committedBoundaryFsn = fsn;
            committedCoverage = runningCoverage;
            committedGap = runningGap;
            committedMaxDeltaEnd = runningMaxDeltaEnd;
            committedMaxDeltaStart = runningMaxDeltaStart;
            committedRawLen = runningRawLen;
            committedRawCount = runningRawCount;
        }
    }

    void appendDecodedSymbols(ObjList<String> out) {
        decodeSymbols(null, out);
    }

    void addDecodedSymbolsTo(GlobalSymbolDictionary target) {
        decodeSymbols(target, null);
    }

    private void decodeSymbols(GlobalSymbolDictionary target, ObjList<String> out) {
        long p = rawAddr;
        long limit = rawAddr + committedRawLen;
        for (int i = 0; i < committedRawCount; i++) {
            long encoded = readVarint(p, limit);
            if (encoded < 0L) {
                throw new IllegalStateException("malformed cached symbol dictionary suffix");
            }
            int varintLen = (int) (encoded & 7L);
            long symbolLen = encoded >>> 3;
            p += varintLen;
            if (symbolLen > limit - p) {
                throw new IllegalStateException("truncated cached symbol dictionary suffix");
            }
            String symbol = Utf8s.stringFromUtf8Bytes(p, p + symbolLen);
            if (target != null) {
                target.addRecoveredSymbol(symbol);
            } else {
                out.add(symbol);
            }
            p += symbolLen;
        }
        if (p != limit) {
            throw new IllegalStateException("overfilled cached symbol dictionary suffix");
        }
    }

    int baseline() {
        return baseline;
    }

    long commitBoundaryFsn() {
        return committedBoundaryFsn;
    }

    long coverage() {
        return committedGap ? -1L : committedCoverage;
    }

    long framesVisited() {
        return framesVisited;
    }

    /**
     * Discards native bytes accumulated only while scanning an uncommitted
     * deferred tail. Call exactly once after the ordered recovery walk.
     */
    void finish() {
        runningRawLen = committedRawLen;
        runningRawCount = committedRawCount;
        if (rawCapacity == committedRawLen) {
            return;
        }
        if (committedRawLen == 0) {
            Unsafe.free(rawAddr, rawCapacity, MemoryTag.NATIVE_DEFAULT);
            rawAddr = 0L;
            rawCapacity = 0;
            return;
        }
        rawAddr = Unsafe.realloc(
                rawAddr,
                rawCapacity,
                committedRawLen,
                MemoryTag.NATIVE_DEFAULT);
        rawCapacity = committedRawLen;
    }

    long maxDeltaEnd() {
        return committedMaxDeltaEnd;
    }

    long maxDeltaStart() {
        return committedMaxDeltaStart;
    }

    long rawAddr() {
        return rawAddr;
    }

    int rawCount() {
        return committedRawCount;
    }

    int rawLen() {
        return committedRawLen;
    }

    int rawCapacity() {
        return rawCapacity;
    }

    long symbolEntriesVisited() {
        return symbolEntriesVisited;
    }

    @Override
    public void close() {
        if (rawAddr != 0L) {
            Unsafe.free(rawAddr, rawCapacity, MemoryTag.NATIVE_DEFAULT);
            rawAddr = 0L;
            rawCapacity = 0;
            runningRawLen = 0;
            committedRawLen = 0;
        }
    }

    private void appendRaw(long entryStart, int entryLen) {
        long required = (long) runningRawLen + entryLen;
        if (required > MAX_RAW_BYTES) {
            throw new IllegalStateException("recovered symbol dictionary suffix exceeds maximum size "
                    + "[required=" + required + ", max=" + MAX_RAW_BYTES + ']');
        }
        if (required > rawCapacity) {
            long newCapacity = Math.max(required, Math.max(4_096L, (long) rawCapacity * 2L));
            if (newCapacity > MAX_RAW_BYTES) {
                newCapacity = MAX_RAW_BYTES;
            }
            rawAddr = Unsafe.realloc(rawAddr, rawCapacity, (int) newCapacity, MemoryTag.NATIVE_DEFAULT);
            rawCapacity = (int) newCapacity;
        }
        Unsafe.getUnsafe().copyMemory(entryStart, rawAddr + runningRawLen, entryLen);
        runningRawLen += entryLen;
        runningRawCount++;
    }

    private void foldDelta(long fsn, long p, long limit) {
        long encodedStart = readVarint(p, limit);
        if (encodedStart < 0L) {
            markGap(fsn);
            return;
        }
        int startLen = (int) (encodedStart & 7L);
        long deltaStart = encodedStart >>> 3;
        p += startLen;
        if (deltaStart > runningMaxDeltaStart) {
            runningMaxDeltaStart = deltaStart;
        }

        long encodedCount = readVarint(p, limit);
        if (encodedCount < 0L) {
            markGap(fsn);
            return;
        }
        int countLen = (int) (encodedCount & 7L);
        long deltaCount = encodedCount >>> 3;
        p += countLen;
        long deltaEnd = deltaCount > Long.MAX_VALUE - deltaStart
                ? Long.MAX_VALUE
                : deltaStart + deltaCount;
        if (deltaEnd > runningMaxDeltaEnd) {
            runningMaxDeltaEnd = deltaEnd;
        }
        if (runningGap) {
            // A full dictionary is a new self-sufficient epoch, but it may only
            // repair a gap that is entirely behind the durable ACK watermark.
            // If any gapped frame will replay first, accepting this reset would
            // hide the unsafe wire-order gap and let the server observe missing
            // ids before it reaches the full frame.
            if (deltaStart != 0L || runningUnackedGap) {
                return;
            }
            runningGap = false;
            runningCoverage = baseline;
            runningRawLen = 0;
            runningRawCount = 0;
        }
        if (deltaStart > runningCoverage) {
            markGap(fsn);
            return;
        }
        // The segment scan already CRC-validated this frame. When its entire
        // range is covered, entry parsing cannot extend recovery state, so skip
        // the cardinality-proportional payload walk.
        if (deltaEnd <= runningCoverage) {
            return;
        }

        long id = deltaStart;
        for (long i = 0; i < deltaCount; i++, id++) {
            symbolEntriesVisited++;
            long entryStart = p;
            long encodedLen = readVarint(p, limit);
            if (encodedLen < 0L) {
                markGap(fsn);
                return;
            }
            int varintLen = (int) (encodedLen & 7L);
            long symbolLen = encodedLen >>> 3;
            p += varintLen;
            if (symbolLen > limit - p) {
                markGap(fsn);
                return;
            }
            p += symbolLen;
            if (id >= runningCoverage) {
                appendRaw(entryStart, (int) (p - entryStart));
            }
        }
        if (deltaEnd > runningCoverage) {
            runningCoverage = deltaEnd;
        }
    }

    private void markGap(long fsn) {
        runningGap = true;
        if (fsn > ackedFsn) {
            runningUnackedGap = true;
        }
    }

    /**
     * Returns {@code (value << 3) | encodedByteCount}, or {@code -1} for an
     * unterminated/oversized 32-bit protocol varint.
     */
    private static long readVarint(long p, long limit) {
        long value = 0L;
        int shift = 0;
        int bytes = 0;
        while (p < limit && bytes < 5) {
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
}
