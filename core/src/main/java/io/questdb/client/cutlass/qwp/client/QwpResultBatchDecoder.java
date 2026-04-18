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

import java.nio.charset.StandardCharsets;

/**
 * Stateful decoder for inbound QWP egress frames (server → client).
 * <p>
 * Reusable across batches on a single connection. Holds the connection-scoped
 * schema registry so schema-reference batches (mode 0x01) can be resolved back
 * to the full column list. Decoded values are materialised onto the heap for
 * Phase-1 simplicity; see the public getters on {@link QwpColumnBatch}.
 */
public class QwpResultBatchDecoder {

    /**
     * Sentinel used as the per-row "NULL" marker in the decoded value arrays.
     */
    public static final Object NULL = new Object();

    private final QwpColumnBatch batch = new QwpColumnBatch();
    // Registry indexed by schemaId. null = not registered. Non-null ObjList is the column list
    // for that schema. Schema ids are server-assigned and small (monotonic from 0).
    private final ObjList<ObjList<QwpEgressColumnInfo>> schemaRegistry = new ObjList<>();

    /**
     * Clears the per-connection schema registry. Call when reconnecting.
     */
    public void clearRegistry() {
        schemaRegistry.clear();
    }

    /**
     * Decodes a RESULT_BATCH frame payload (starting with msg_kind=0x11 at {@code payload}).
     * After success, {@link #getBatch()} returns a view over the decoded data valid until
     * the next {@code decode()} call.
     */
    public void decode(long payload, int payloadLen) throws QwpDecodeException {
        if (payloadLen < QwpConstants.HEADER_SIZE + 10 /* msg_kind + reqId + min varint */) {
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
        // flags and table_count informational for Phase 1
        long p = payload + QwpConstants.HEADER_SIZE;
        long limit = payload + payloadLen;

        byte msgKind = Unsafe.getUnsafe().getByte(p++);
        if (msgKind != (byte) 0x11) {
            throw new QwpDecodeException("expected RESULT_BATCH (0x11), got 0x" + Integer.toHexString(msgKind & 0xFF));
        }
        if (p + 8 > limit) throw new QwpDecodeException("truncated request_id");
        long requestId = Unsafe.getUnsafe().getLong(p);
        p += 8;
        long[] varint = new long[2]; // [value, nextPos]
        decodeVarint(p, limit, varint);
        long batchSeq = varint[0];
        p = varint[1];

        // Table block: name_length, name, row_count, column_count, schema, columns
        decodeVarint(p, limit, varint);
        long nameLen = varint[0];
        p = varint[1];
        if (p + nameLen > limit) throw new QwpDecodeException("truncated table name");
        // Skip name — result sets carry empty names.
        p += nameLen;

        decodeVarint(p, limit, varint);
        int rowCount = (int) varint[0];
        p = varint[1];
        decodeVarint(p, limit, varint);
        int columnCount = (int) varint[0];
        p = varint[1];

        // Schema section
        if (p >= limit) throw new QwpDecodeException("truncated schema mode");
        byte schemaMode = Unsafe.getUnsafe().getByte(p++);
        decodeVarint(p, limit, varint);
        int schemaId = (int) varint[0];
        p = varint[1];

        ObjList<QwpEgressColumnInfo> columns;
        if (schemaMode == QwpConstants.SCHEMA_MODE_FULL) {
            columns = ensureSchemaSlot(schemaId, columnCount);
            for (int i = 0; i < columnCount; i++) {
                decodeVarint(p, limit, varint);
                int colNameLen = (int) varint[0];
                p = varint[1];
                if (p + colNameLen + 1 > limit) throw new QwpDecodeException("truncated column def");
                String colName = readUtf8(p, colNameLen);
                p += colNameLen;
                byte wireType = Unsafe.getUnsafe().getByte(p++);
                // Scale/precision are NOT in the schema — they're wire-level prefixes
                // inside each column's data block. Placeholders here.
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

        // Column data
        batch.reset(requestId, batchSeq, rowCount, columnCount, columns);
        for (int ci = 0; ci < columnCount; ci++) {
            QwpEgressColumnInfo info = columns.getQuick(ci);
            p = decodeColumn(p, limit, info, rowCount, batch.columnValues(ci));
        }
    }

    /**
     * Returns the view over the most recently decoded batch. Valid until the next
     * {@link #decode(long, int)} call.
     */
    public QwpColumnBatch getBatch() {
        return batch;
    }

    // -----------------------------------------------------------------------------
    // Varint / bitmap helpers
    // -----------------------------------------------------------------------------

    private static void decodeVarint(long p, long limit, long[] out) throws QwpDecodeException {
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
        out[0] = value;
        out[1] = cur;
    }

    private static String readUtf8(long p, long len) {
        byte[] bytes = new byte[(int) len];
        for (int i = 0; i < len; i++) {
            bytes[i] = Unsafe.getUnsafe().getByte(p + i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Reads the null flag byte and (if present) the bitmap. Populates the given boolean
     * array (length = rowCount) with row-is-null flags. Returns the new position.
     */
    private static long readNullBitmap(long p, long limit, int rowCount, boolean[] nullFlags) throws QwpDecodeException {
        if (p >= limit) throw new QwpDecodeException("truncated null flag");
        byte flag = Unsafe.getUnsafe().getByte(p++);
        java.util.Arrays.fill(nullFlags, 0, rowCount, false);
        if (flag == 0) return p;
        int bytes = (rowCount + 7) >>> 3;
        if (p + bytes > limit) throw new QwpDecodeException("truncated null bitmap");
        for (int i = 0; i < rowCount; i++) {
            int bi = i >>> 3;
            int bit = i & 7;
            byte bm = Unsafe.getUnsafe().getByte(p + bi);
            if ((bm & (1 << bit)) != 0) {
                nullFlags[i] = true;
            }
        }
        return p + bytes;
    }

    // -----------------------------------------------------------------------------
    // Per-column decoders
    // -----------------------------------------------------------------------------

    private long decodeColumn(long p, long limit, QwpEgressColumnInfo info, int rowCount, Object[] values)
            throws QwpDecodeException {
        boolean[] nullFlags = new boolean[rowCount];
        p = readNullBitmap(p, limit, rowCount, nullFlags);
        byte wt = info.wireType;
        if (wt == QwpConstants.TYPE_BOOLEAN) {
            int nonNull = 0;
            for (int i = 0; i < rowCount; i++) if (!nullFlags[i]) nonNull++;
            int bytes = (nonNull + 7) >>> 3;
            if (p + bytes > limit) throw new QwpDecodeException("truncated BOOLEAN");
            int bitIdx = 0;
            for (int i = 0; i < rowCount; i++) {
                if (nullFlags[i]) {
                    values[i] = null;
                    continue;
                }
                byte bm = Unsafe.getUnsafe().getByte(p + (bitIdx >>> 3));
                values[i] = ((bm & (1 << (bitIdx & 7))) != 0) ? Boolean.TRUE : Boolean.FALSE;
                bitIdx++;
            }
            return p + bytes;
        }
        if (wt == QwpConstants.TYPE_BYTE) return decodeFixed(p, limit, rowCount, 1, nullFlags, values);
        if (wt == QwpConstants.TYPE_SHORT || wt == QwpConstants.TYPE_CHAR) {
            return decodeFixed(p, limit, rowCount, 2, nullFlags, values);
        }
        if (wt == QwpConstants.TYPE_INT) return decodeFixed(p, limit, rowCount, 4, nullFlags, values);
        if (wt == QwpConstants.TYPE_FLOAT) return decodeFloat(p, limit, rowCount, nullFlags, values);
        if (wt == QwpConstants.TYPE_LONG || wt == QwpConstants.TYPE_DATE
                || wt == QwpConstants.TYPE_TIMESTAMP || wt == QwpConstants.TYPE_TIMESTAMP_NANOS) {
            return decodeFixed(p, limit, rowCount, 8, nullFlags, values);
        }
        if (wt == QwpConstants.TYPE_DOUBLE) return decodeDouble(p, limit, rowCount, nullFlags, values);
        if (wt == QwpConstants.TYPE_STRING || wt == QwpConstants.TYPE_VARCHAR) {
            return decodeString(p, limit, rowCount, nullFlags, values, wt == QwpConstants.TYPE_STRING);
        }
        if (wt == QwpConstants.TYPE_SYMBOL) return decodeSymbol(p, limit, rowCount, nullFlags, values);
        if (wt == QwpConstants.TYPE_UUID) return decodeFixedPair(p, limit, rowCount, nullFlags, values);
        if (wt == QwpConstants.TYPE_LONG256) return decodeFixedQuad(p, limit, rowCount, nullFlags, values);
        if (wt == QwpConstants.TYPE_GEOHASH) {
            long[] varint = new long[2];
            decodeVarint(p, limit, varint);
            info.precisionBits = (int) varint[0];
            p = varint[1];
            int bytesPerValue = (info.precisionBits + 7) >>> 3;
            for (int i = 0; i < rowCount; i++) {
                if (nullFlags[i]) {
                    values[i] = null;
                    continue;
                }
                if (p + bytesPerValue > limit) throw new QwpDecodeException("truncated GEOHASH");
                long bits = 0;
                for (int b = 0; b < bytesPerValue; b++) {
                    bits |= ((long) (Unsafe.getUnsafe().getByte(p + b) & 0xFF)) << (b * 8);
                }
                values[i] = bits;
                p += bytesPerValue;
            }
            return p;
        }
        if (wt == QwpConstants.TYPE_DECIMAL64) {
            if (p >= limit) throw new QwpDecodeException("truncated DECIMAL64 scale");
            info.scale = Unsafe.getUnsafe().getByte(p++) & 0xFF;
            return decodeFixed(p, limit, rowCount, 8, nullFlags, values);
        }
        if (wt == QwpConstants.TYPE_DECIMAL128) {
            if (p >= limit) throw new QwpDecodeException("truncated DECIMAL128 scale");
            info.scale = Unsafe.getUnsafe().getByte(p++) & 0xFF;
            return decodeFixedPair(p, limit, rowCount, nullFlags, values);
        }
        if (wt == QwpConstants.TYPE_DECIMAL256) {
            if (p >= limit) throw new QwpDecodeException("truncated DECIMAL256 scale");
            info.scale = Unsafe.getUnsafe().getByte(p++) & 0xFF;
            return decodeFixedQuad(p, limit, rowCount, nullFlags, values);
        }
        if (wt == QwpConstants.TYPE_DOUBLE_ARRAY || wt == QwpConstants.TYPE_LONG_ARRAY) {
            return decodeArray(p, limit, rowCount, nullFlags, values);
        }
        throw new QwpDecodeException("unsupported wire type 0x" + Integer.toHexString(wt & 0xFF));
    }

    private long decodeFixed(long p, long limit, int rowCount, int sizeBytes, boolean[] nullFlags, Object[] values)
            throws QwpDecodeException {
        for (int i = 0; i < rowCount; i++) {
            if (nullFlags[i]) {
                values[i] = null;
                continue;
            }
            if (p + sizeBytes > limit) throw new QwpDecodeException("truncated fixed column");
            long v;
            switch (sizeBytes) {
                case 1: v = Unsafe.getUnsafe().getByte(p); break;
                case 2: v = Unsafe.getUnsafe().getShort(p); break;
                case 4: v = Unsafe.getUnsafe().getInt(p); break;
                case 8: v = Unsafe.getUnsafe().getLong(p); break;
                default: throw new IllegalStateException();
            }
            values[i] = v;
            p += sizeBytes;
        }
        return p;
    }

    private long decodeFloat(long p, long limit, int rowCount, boolean[] nullFlags, Object[] values)
            throws QwpDecodeException {
        for (int i = 0; i < rowCount; i++) {
            if (nullFlags[i]) {
                values[i] = null;
                continue;
            }
            if (p + 4 > limit) throw new QwpDecodeException("truncated FLOAT");
            values[i] = Float.intBitsToFloat(Unsafe.getUnsafe().getInt(p));
            p += 4;
        }
        return p;
    }

    private long decodeDouble(long p, long limit, int rowCount, boolean[] nullFlags, Object[] values)
            throws QwpDecodeException {
        for (int i = 0; i < rowCount; i++) {
            if (nullFlags[i]) {
                values[i] = null;
                continue;
            }
            if (p + 8 > limit) throw new QwpDecodeException("truncated DOUBLE");
            values[i] = Double.longBitsToDouble(Unsafe.getUnsafe().getLong(p));
            p += 8;
        }
        return p;
    }

    private long decodeFixedPair(long p, long limit, int rowCount, boolean[] nullFlags, Object[] values)
            throws QwpDecodeException {
        for (int i = 0; i < rowCount; i++) {
            if (nullFlags[i]) {
                values[i] = null;
                continue;
            }
            if (p + 16 > limit) throw new QwpDecodeException("truncated 16-byte value");
            long lo = Unsafe.getUnsafe().getLong(p);
            long hi = Unsafe.getUnsafe().getLong(p + 8);
            values[i] = new long[]{lo, hi};
            p += 16;
        }
        return p;
    }

    private long decodeFixedQuad(long p, long limit, int rowCount, boolean[] nullFlags, Object[] values)
            throws QwpDecodeException {
        for (int i = 0; i < rowCount; i++) {
            if (nullFlags[i]) {
                values[i] = null;
                continue;
            }
            if (p + 32 > limit) throw new QwpDecodeException("truncated 32-byte value");
            values[i] = new long[]{
                    Unsafe.getUnsafe().getLong(p),
                    Unsafe.getUnsafe().getLong(p + 8),
                    Unsafe.getUnsafe().getLong(p + 16),
                    Unsafe.getUnsafe().getLong(p + 24)
            };
            p += 32;
        }
        return p;
    }

    private long decodeString(long p, long limit, int rowCount, boolean[] nullFlags, Object[] values, boolean utf16)
            throws QwpDecodeException {
        int nonNull = 0;
        for (int i = 0; i < rowCount; i++) if (!nullFlags[i]) nonNull++;
        int offsetBytes = 4 * (nonNull + 1);
        if (p + offsetBytes > limit) throw new QwpDecodeException("truncated string offsets");
        long offsetsAddr = p;
        long bytesStart = p + offsetBytes;

        int nonNullIdx = 0;
        for (int i = 0; i < rowCount; i++) {
            if (nullFlags[i]) {
                values[i] = null;
                continue;
            }
            int startOff = Unsafe.getUnsafe().getInt(offsetsAddr + 4L * nonNullIdx);
            int endOff = Unsafe.getUnsafe().getInt(offsetsAddr + 4L * (nonNullIdx + 1));
            int len = endOff - startOff;
            if (bytesStart + endOff > limit || len < 0) throw new QwpDecodeException("truncated string bytes");
            if (utf16) {
                values[i] = readUtf8(bytesStart + startOff, len);
            } else {
                byte[] raw = new byte[len];
                for (int b = 0; b < len; b++) {
                    raw[b] = Unsafe.getUnsafe().getByte(bytesStart + startOff + b);
                }
                values[i] = raw;
            }
            nonNullIdx++;
        }
        int totalStringBytes = nonNull == 0 ? 0 : Unsafe.getUnsafe().getInt(offsetsAddr + 4L * nonNull);
        return bytesStart + totalStringBytes;
    }

    private long decodeSymbol(long p, long limit, int rowCount, boolean[] nullFlags, Object[] values)
            throws QwpDecodeException {
        long[] varint = new long[2];
        decodeVarint(p, limit, varint);
        int dictSize = (int) varint[0];
        p = varint[1];
        String[] dict = new String[dictSize];
        for (int e = 0; e < dictSize; e++) {
            decodeVarint(p, limit, varint);
            int entryLen = (int) varint[0];
            p = varint[1];
            if (p + entryLen > limit) throw new QwpDecodeException("truncated symbol entry");
            dict[e] = readUtf8(p, entryLen);
            p += entryLen;
        }
        for (int i = 0; i < rowCount; i++) {
            if (nullFlags[i]) {
                values[i] = null;
                continue;
            }
            decodeVarint(p, limit, varint);
            int idx = (int) varint[0];
            p = varint[1];
            if (idx < 0 || idx >= dictSize) throw new QwpDecodeException("symbol index out of range: " + idx);
            values[i] = dict[idx];
        }
        return p;
    }

    private long decodeArray(long p, long limit, int rowCount, boolean[] nullFlags, Object[] values)
            throws QwpDecodeException {
        for (int i = 0; i < rowCount; i++) {
            if (nullFlags[i]) {
                values[i] = null;
                continue;
            }
            if (p + 1 > limit) throw new QwpDecodeException("truncated ARRAY");
            int nDims = Unsafe.getUnsafe().getByte(p) & 0xFF;
            long headerEnd = p + 1 + 4L * nDims;
            if (headerEnd > limit) throw new QwpDecodeException("truncated ARRAY dims");
            int elements = 1;
            for (int d = 0; d < nDims; d++) {
                int dl = Unsafe.getUnsafe().getInt(p + 1 + 4L * d);
                elements *= dl;
            }
            long payloadEnd = headerEnd + 8L * elements;
            if (payloadEnd > limit) throw new QwpDecodeException("truncated ARRAY payload");
            int totalLen = (int) (payloadEnd - p);
            byte[] raw = new byte[totalLen];
            for (int b = 0; b < totalLen; b++) {
                raw[b] = Unsafe.getUnsafe().getByte(p + b);
            }
            values[i] = raw;
            p = payloadEnd;
        }
        return p;
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
}
