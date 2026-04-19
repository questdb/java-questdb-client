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

package io.questdb.client.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.DirectUtf8String;

import java.nio.charset.StandardCharsets;

/**
 * Zero-alloc (after warmup) decoder for inbound QWP egress {@code RESULT_BATCH} frames.
 * <p>
 * The decoder parses the payload in-place -- no values are copied out of the
 * WebSocket receive buffer. It maintains pooled {@link QwpColumnLayout} slots,
 * per-column {@code int[]} index arrays, and per-column {@link DirectUtf8String}
 * dict entries that are reused across batches. After the connection has seen
 * its peak schema width and row count, decoding a batch allocates nothing on
 * the JVM heap.
 * <p>
 * The produced {@link QwpColumnBatch} is valid only during the surrounding
 * {@code onBatch} callback because its pointers refer into the caller's native
 * payload buffer.
 */
public class QwpResultBatchDecoder {

    /**
     * Cap on per-row ARRAY element count. 8 bytes per element x this ~ 256 MB max payload,
     * which fits in {@code int} once {@code rowEnd - p} is computed. A malicious or buggy
     * server cannot push a negative or wrap-around length past this guard.
     */
    private static final long MAX_ARRAY_ELEMENTS = (Integer.MAX_VALUE - 1024) / 8L;
    /**
     * Hard cap on {@code row_count} per batch. Matches the server's MAX_ROWS_PER_BATCH.
     * A hostile server could otherwise encode row_count = Integer.MAX_VALUE; ensureIntArray
     * would then try to allocate an {@code int[Integer.MAX_VALUE]} (~8 GB) before any
     * wire-length bounds check fires. Cap two orders of magnitude above the batch size
     * to leave head-room for future server-side batch enlargement without breaking clients.
     */
    private static final int MAX_ROWS_PER_BATCH = 1_048_576;
    /**
     * Hard cap on registered schema ids per connection. Matches
     * {@code QwpConstants.DEFAULT_MAX_SCHEMAS_PER_CONNECTION} on the server side.
     * Capping protects the client from a hostile or buggy server that could
     * otherwise force unbounded {@code schemaRegistry} growth (or AIOOBE on a
     * negative schema id) by encoding {@code schemaId = Integer.MAX_VALUE} (or
     * a negative varint that long-to-int casts negative).
     */
    private static final int MAX_SCHEMAS_PER_CONNECTION = 65_535;
    // Connection-scoped state (safe to share across buffers -- reused across batches
    // of the same query and across queries on the same connection).
    // Registry indexed by schemaId. null = not registered. Schema ids are server-assigned
    // and small (monotonic from 0).
    private final ObjList<ObjList<QwpEgressColumnInfo>> schemaRegistry = new ObjList<>();
    // Reusable varint decode state: value in varintValue, new position in varintPos.
    // Instance-level so no {@code long[2]} scratch is allocated per call.
    private long varintPos;
    private long varintValue;

    /**
     * Decodes the RESULT_BATCH frame whose payload has been copied into {@code buffer}.
     * Populates {@code buffer.batch} and {@code buffer.layoutPool}. The resulting
     * batch view stays valid as long as the buffer is not reused.
     */
    public void decode(QwpBatchBuffer buffer) throws QwpDecodeException {
        decodePayload(buffer, buffer.getScratchAddr(), buffer.getPayloadLen());
    }

    /**
     * In-place decode: parses the frame whose bytes live at {@code payloadPtr} (e.g. the
     * WebSocket recv buffer) without copying into {@code buffer}'s native scratch.
     * {@code buffer} contributes only its reusable layout pool and batch view; all
     * column pointers produced reference {@code payloadPtr}, so the caller must keep
     * those bytes stable until it's done reading the {@link QwpColumnBatch}.
     */
    public void decode(QwpBatchBuffer buffer, long payloadPtr, int payloadLen) throws QwpDecodeException {
        decodePayload(buffer, payloadPtr, payloadLen);
    }

    // Pool helpers

    private static long advanceFixed(QwpColumnLayout layout, long p, long limit, int sizeBytes) throws QwpDecodeException {
        layout.valuesAddr = p;
        long total = (long) sizeBytes * layout.nonNullCount;
        if (p + total > limit) throw new QwpDecodeException("truncated fixed-width column");
        return p + total;
    }

    private static QwpColumnLayout borrowLayout(ObjList<QwpColumnLayout> layoutPool, int colIdx) {
        while (layoutPool.size() <= colIdx) {
            layoutPool.add(new QwpColumnLayout());
        }
        return layoutPool.getQuick(colIdx);
    }

    private static int[] ensureIntArray(int[] current, int size) {
        if (current != null && current.length >= size) return current;
        return new int[Math.max(size, current == null ? 16 : current.length * 2)];
    }

    private static long[] ensureLongArray(long[] current, int size) {
        if (current != null && current.length >= size) return current;
        return new long[Math.max(size, current == null ? 16 : current.length * 2)];
    }

    // Varint / string helpers

    /**
     * STRING / VARCHAR: the offsets array is (nonNullCount+1) x uint32 starting at {@code p},
     * followed by the concatenated UTF-8 bytes.
     */
    private static long parseStringColumn(QwpColumnLayout layout, long p, long limit) throws QwpDecodeException {
        int nonNull = layout.nonNullCount;
        long offsetsSize = 4L * (nonNull + 1);
        if (p + offsetsSize > limit) throw new QwpDecodeException("truncated string offsets");
        layout.valuesAddr = p;
        layout.stringBytesAddr = p + offsetsSize;
        int totalBytes = nonNull == 0 ? 0 : Unsafe.getUnsafe().getInt(p + 4L * nonNull);
        // totalBytes is signed int32 read from the wire. A negative value passes the
        // "addr + totalBytes > limit" check (the sum stays below limit) and would
        // return a position before stringBytesAddr -- subsequent column parsing would
        // then read native memory backwards. Reject it explicitly.
        if (totalBytes < 0 || layout.stringBytesAddr + totalBytes > limit) {
            throw new QwpDecodeException("invalid string column total bytes: " + totalBytes);
        }
        return layout.stringBytesAddr + totalBytes;
    }

    private static String readUtf8(long p, long len) {
        byte[] bytes = new byte[(int) len];
        for (int i = 0; i < len; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(p + i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    // Per-column parse: advances through wire bytes, populates layout pointers,
    // precomputes nonNullIdx for O(1) per-row access.

    private void decodePayload(QwpBatchBuffer buffer, long payload, int payloadLen) throws QwpDecodeException {
        if (payloadLen < QwpConstants.HEADER_SIZE + 10) {
            throw new QwpDecodeException("RESULT_BATCH payload too short: " + payloadLen);
        }
        // Message header
        int magic = Unsafe.getUnsafe().getInt(payload);
        if (magic != QwpConstants.MAGIC_MESSAGE) {
            throw new QwpDecodeException("bad magic 0x" + Integer.toHexString(magic));
        }
        byte version = Unsafe.getUnsafe().getByte(payload + 4);
        if (version != QwpConstants.VERSION_1) {
            throw new QwpDecodeException("unsupported version " + (version & 0xFF));
        }
        long p = payload + QwpConstants.HEADER_SIZE;
        long limit = payload + payloadLen;

        byte msgKind = Unsafe.getUnsafe().getByte(p++);
        if (msgKind != (byte) 0x11) {
            throw new QwpDecodeException("expected RESULT_BATCH (0x11), got 0x" + Integer.toHexString(msgKind & 0xFF));
        }
        if (p + 8 > limit) throw new QwpDecodeException("truncated request_id");
        long requestId = Unsafe.getUnsafe().getLong(p);
        p += 8;
        decodeVarint(p, limit);
        long batchSeq = varintValue;
        p = varintPos;

        // Table block: name_length, name, row_count, column_count, schema, columns
        decodeVarint(p, limit);
        long nameLen = varintValue;
        p = varintPos;
        if (p + nameLen > limit) throw new QwpDecodeException("truncated table name");
        p += nameLen;

        decodeVarint(p, limit);
        // Reject row counts that would force multi-GB allocations in ensureIntArray/ensureLongArray
        // before the per-column bounds checks fire. A hostile varint with the high bit set also
        // casts negative, which would silently flip bitmapBytes = (rowCount + 7) >>> 3 into a huge
        // positive int via unsigned shift.
        if (varintValue < 0 || varintValue > MAX_ROWS_PER_BATCH) {
            throw new QwpDecodeException("row_count out of range: " + varintValue);
        }
        int rowCount = (int) varintValue;
        p = varintPos;
        decodeVarint(p, limit);
        if (varintValue < 0 || varintValue > QwpConstants.MAX_COLUMNS_PER_TABLE) {
            throw new QwpDecodeException("column_count out of range: " + varintValue);
        }
        int columnCount = (int) varintValue;
        p = varintPos;

        // Schema section
        if (p >= limit) throw new QwpDecodeException("truncated schema mode");
        byte schemaMode = Unsafe.getUnsafe().getByte(p++);
        decodeVarint(p, limit);
        // Reject schema ids that wouldn't fit in our registry (or that cast negative
        // from a hostile high varint). Without this guard, ensureSchemaSlot would
        // either OOM appending billions of nulls or AIOOBE on a negative index.
        if (varintValue < 0 || varintValue >= MAX_SCHEMAS_PER_CONNECTION) {
            throw new QwpDecodeException("schema_id out of range: " + varintValue);
        }
        int schemaId = (int) varintValue;
        p = varintPos;

        ObjList<QwpEgressColumnInfo> columns;
        if (schemaMode == QwpConstants.SCHEMA_MODE_FULL) {
            columns = ensureSchemaSlot(schemaId, columnCount);
            for (int i = 0; i < columnCount; i++) {
                decodeVarint(p, limit);
                int colNameLen = (int) varintValue;
                p = varintPos;
                if (p + colNameLen + 1 > limit) throw new QwpDecodeException("truncated column def");
                String colName = readUtf8(p, colNameLen);
                p += colNameLen;
                byte wireType = Unsafe.getUnsafe().getByte(p++);
                columns.getQuick(i).of(colName, wireType, 0, 0);
            }
        } else if (schemaMode == QwpConstants.SCHEMA_MODE_REFERENCE) {
            if (schemaId >= schemaRegistry.size() || schemaRegistry.getQuick(schemaId) == null) {
                throw new QwpDecodeException("schema id " + schemaId + " not registered on this connection");
            }
            columns = schemaRegistry.getQuick(schemaId);
            if (columns.size() != columnCount) {
                throw new QwpDecodeException("schema id " + schemaId + " column count mismatch");
            }
        } else {
            throw new QwpDecodeException("unknown schema mode 0x" + Integer.toHexString(schemaMode & 0xFF));
        }

        // Reset batch view and parse columns into per-column layouts owned by the buffer.
        resetBatch(buffer, requestId, batchSeq, rowCount, columnCount, columns, payload, limit);
        for (int ci = 0; ci < columnCount; ci++) {
            QwpColumnLayout layout = borrowLayout(buffer.layoutPool, ci);
            layout.clear();
            layout.info = columns.getQuick(ci);
            p = parseColumn(layout, rowCount, p, limit);
        }
    }

    /**
     * Decodes a varint starting at {@code p}. Stores the decoded value in
     * {@link #varintValue} and the position just past the varint in
     * {@link #varintPos}. Caller reads both before issuing the next varint call.
     */
    private void decodeVarint(long p, long limit) throws QwpDecodeException {
        long value = 0;
        int shift = 0;
        long cur = p;
        while (true) {
            if (cur >= limit) throw new QwpDecodeException("truncated varint");
            byte b = Unsafe.getUnsafe().getByte(cur++);
            value |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
            if (shift > 63) throw new QwpDecodeException("varint overflow");
        }
        varintValue = value;
        varintPos = cur;
    }

    private ObjList<QwpEgressColumnInfo> ensureSchemaSlot(int schemaId, int columnCount) {
        while (schemaRegistry.size() <= schemaId) {
            schemaRegistry.add(null);
        }
        ObjList<QwpEgressColumnInfo> slot = schemaRegistry.getQuick(schemaId);
        if (slot == null) {
            slot = new ObjList<>();
            schemaRegistry.setQuick(schemaId, slot);
        }
        int currentPos = slot.size();
        if (columnCount > currentPos) {
            slot.setPos(columnCount);
            for (int i = currentPos; i < columnCount; i++) {
                if (slot.getQuick(i) == null) {
                    slot.setQuick(i, new QwpEgressColumnInfo());
                }
            }
        } else {
            slot.setPos(columnCount);
        }
        return slot;
    }

    private long parseArrayColumn(QwpColumnLayout layout, int rowCount, long p, long limit) throws QwpDecodeException {
        layout.arrayRowAddr = ensureLongArray(layout.arrayRowAddr, rowCount);
        layout.arrayRowLen = ensureIntArray(layout.arrayRowLen, rowCount);
        layout.valuesAddr = p;
        for (int i = 0; i < rowCount; i++) {
            if (layout.nonNullIdx[i] < 0) {
                layout.arrayRowAddr[i] = 0;
                layout.arrayRowLen[i] = 0;
                continue;
            }
            if (p + 1 > limit) throw new QwpDecodeException("truncated ARRAY header");
            int nDims = Unsafe.getUnsafe().getByte(p) & 0xFF;
            long headerEnd = p + 1 + 4L * nDims;
            if (headerEnd > limit) throw new QwpDecodeException("truncated ARRAY dims");
            long elements = 1;
            for (int d = 0; d < nDims; d++) {
                int dl = Unsafe.getUnsafe().getInt(p + 1 + 4L * d);
                if (dl < 0) throw new QwpDecodeException("ARRAY dim " + d + " is negative: " + dl);
                elements *= dl;
                if (elements > MAX_ARRAY_ELEMENTS) {
                    throw new QwpDecodeException("ARRAY element count exceeds limit ("
                            + elements + " > " + MAX_ARRAY_ELEMENTS + ")");
                }
            }
            long rowEnd = headerEnd + 8L * elements;
            if (rowEnd > limit) throw new QwpDecodeException("truncated ARRAY payload");
            layout.arrayRowAddr[i] = p;
            layout.arrayRowLen[i] = (int) (rowEnd - p);
            p = rowEnd;
        }
        return p;
    }

    private long parseColumn(QwpColumnLayout layout, int rowCount, long p, long limit) throws QwpDecodeException {
        p = parseNullSection(layout, rowCount, p, limit);
        byte wt = layout.info.wireType;
        if (wt == QwpConstants.TYPE_BOOLEAN) {
            layout.valuesAddr = p;
            int bytes = (layout.nonNullCount + 7) >>> 3;
            if (p + bytes > limit) throw new QwpDecodeException("truncated BOOLEAN");
            return p + bytes;
        }
        if (wt == QwpConstants.TYPE_BYTE) return advanceFixed(layout, p, limit, 1);
        if (wt == QwpConstants.TYPE_SHORT || wt == QwpConstants.TYPE_CHAR) return advanceFixed(layout, p, limit, 2);
        if (wt == QwpConstants.TYPE_INT || wt == QwpConstants.TYPE_FLOAT
                || wt == QwpConstants.TYPE_IPv4) return advanceFixed(layout, p, limit, 4);
        if (wt == QwpConstants.TYPE_LONG || wt == QwpConstants.TYPE_DOUBLE
                || wt == QwpConstants.TYPE_DATE
                || wt == QwpConstants.TYPE_TIMESTAMP || wt == QwpConstants.TYPE_TIMESTAMP_NANOS) {
            return advanceFixed(layout, p, limit, 8);
        }
        if (wt == QwpConstants.TYPE_DECIMAL64) {
            if (p >= limit) throw new QwpDecodeException("truncated DECIMAL64 scale");
            layout.info.scale = Unsafe.getUnsafe().getByte(p++) & 0xFF;
            return advanceFixed(layout, p, limit, 8);
        }
        if (wt == QwpConstants.TYPE_UUID) return advanceFixed(layout, p, limit, 16);
        if (wt == QwpConstants.TYPE_DECIMAL128) {
            if (p >= limit) throw new QwpDecodeException("truncated DECIMAL128 scale");
            layout.info.scale = Unsafe.getUnsafe().getByte(p++) & 0xFF;
            return advanceFixed(layout, p, limit, 16);
        }
        if (wt == QwpConstants.TYPE_LONG256) return advanceFixed(layout, p, limit, 32);
        if (wt == QwpConstants.TYPE_DECIMAL256) {
            if (p >= limit) throw new QwpDecodeException("truncated DECIMAL256 scale");
            layout.info.scale = Unsafe.getUnsafe().getByte(p++) & 0xFF;
            return advanceFixed(layout, p, limit, 32);
        }
        if (wt == QwpConstants.TYPE_STRING || wt == QwpConstants.TYPE_VARCHAR
                || wt == QwpConstants.TYPE_BINARY) {
            // STRING/VARCHAR/BINARY all share the (N+1) x uint32 offsets + concatenated bytes layout.
            // BINARY differs only in that the bytes are opaque (no UTF-8 contract).
            return parseStringColumn(layout, p, limit);
        }
        if (wt == QwpConstants.TYPE_SYMBOL) {
            return parseSymbolColumn(layout, rowCount, p, limit);
        }
        if (wt == QwpConstants.TYPE_GEOHASH) {
            decodeVarint(p, limit);
            layout.info.precisionBits = (int) varintValue;
            p = varintPos;
            int bytesPerValue = (layout.info.precisionBits + 7) >>> 3;
            layout.valuesAddr = p;
            long total = (long) bytesPerValue * layout.nonNullCount;
            if (p + total > limit) throw new QwpDecodeException("truncated GEOHASH");
            return p + total;
        }
        if (wt == QwpConstants.TYPE_DOUBLE_ARRAY || wt == QwpConstants.TYPE_LONG_ARRAY) {
            return parseArrayColumn(layout, rowCount, p, limit);
        }
        throw new QwpDecodeException("unsupported wire type 0x" + Integer.toHexString(wt & 0xFF));
    }

    /**
     * Reads the null flag and bitmap, populates {@code layout.nullBitmapAddr} and
     * {@code layout.nonNullCount}, and fills {@code layout.nonNullIdx[0..rowCount)}
     * with dense indices (or -1 for NULL rows). Returns the position just past
     * the null section.
     */
    private long parseNullSection(QwpColumnLayout layout, int rowCount, long p, long limit) throws QwpDecodeException {
        if (p >= limit) throw new QwpDecodeException("truncated null flag");
        byte flag = Unsafe.getUnsafe().getByte(p++);
        layout.nonNullIdx = ensureIntArray(layout.nonNullIdx, rowCount);
        if (flag == 0) {
            layout.nullBitmapAddr = 0;
            layout.nonNullCount = rowCount;
            for (int i = 0; i < rowCount; i++) layout.nonNullIdx[i] = i;
            return p;
        }
        int bitmapBytes = (rowCount + 7) >>> 3;
        if (p + bitmapBytes > limit) throw new QwpDecodeException("truncated null bitmap");
        layout.nullBitmapAddr = p;
        int denseIdx = 0;
        for (int i = 0; i < rowCount; i++) {
            int bi = i >>> 3;
            int bit = i & 7;
            byte bm = Unsafe.getUnsafe().getByte(p + bi);
            if ((bm & (1 << bit)) != 0) {
                layout.nonNullIdx[i] = -1;
            } else {
                layout.nonNullIdx[i] = denseIdx++;
            }
        }
        layout.nonNullCount = denseIdx;
        return p + bitmapBytes;
    }

    /**
     * SYMBOL: per-table dictionary (dict_size varint, then len+bytes per entry),
     * then per-non-null-row varint indices into the dict.
     */
    private long parseSymbolColumn(QwpColumnLayout layout, int rowCount, long p, long limit) throws QwpDecodeException {
        decodeVarint(p, limit);
        int dictSize = (int) varintValue;
        p = varintPos;
        // Ensure pool size
        while (layout.symbolDict.size() < dictSize) {
            layout.symbolDict.add(new DirectUtf8String());
        }
        for (int e = 0; e < dictSize; e++) {
            decodeVarint(p, limit);
            int entryLen = (int) varintValue;
            p = varintPos;
            if (p + entryLen > limit) throw new QwpDecodeException("truncated symbol entry");
            layout.symbolDict.getQuick(e).of(p, p + entryLen);
            p += entryLen;
        }
        layout.symbolDictSize = dictSize;
        // Materialise per-row IDs into int[rowCount] so random access is O(1).
        layout.symbolRowIds = ensureIntArray(layout.symbolRowIds, rowCount);
        for (int i = 0; i < rowCount; i++) {
            int denseIdx = layout.nonNullIdx[i];
            if (denseIdx < 0) continue; // NULL row; leave slot stale
            decodeVarint(p, limit);
            p = varintPos;
            int id = (int) varintValue;
            if (id < 0 || id >= dictSize) {
                throw new QwpDecodeException("symbol index out of range: " + id);
            }
            layout.symbolRowIds[i] = id;
        }
        layout.valuesAddr = 0; // Not applicable; accessors use symbolRowIds + symbolDict.
        return p;
    }

    // Batch reset

    private void resetBatch(
            QwpBatchBuffer buffer,
            long requestId,
            long batchSeq,
            int rowCount,
            int columnCount,
            ObjList<QwpEgressColumnInfo> columns,
            long payloadAddr,
            long payloadLimit
    ) {
        QwpColumnBatch batch = buffer.batch;
        batch.requestId = requestId;
        batch.batchSeq = batchSeq;
        batch.rowCount = rowCount;
        batch.columnCount = columnCount;
        batch.columns = columns;
        batch.payloadAddr = payloadAddr;
        batch.payloadLimit = payloadLimit;
        // Surface the buffer-owned layouts to the batch view
        while (batch.columnLayouts.size() < columnCount) {
            batch.columnLayouts.add(null);
        }
        for (int i = 0; i < columnCount; i++) {
            batch.columnLayouts.setQuick(i, borrowLayout(buffer.layoutPool, i));
        }
    }
}
