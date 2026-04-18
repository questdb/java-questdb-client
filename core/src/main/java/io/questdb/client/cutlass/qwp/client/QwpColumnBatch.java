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
import io.questdb.client.std.str.DirectUtf8Sequence;
import io.questdb.client.std.str.DirectUtf8String;

import java.nio.charset.StandardCharsets;

/**
 * Column-major view over one decoded {@code RESULT_BATCH}. Valid only during the
 * {@link QwpColumnBatchHandler#onBatch} callback; becomes stale once control returns
 * to the decoder.
 * <p>
 * Accessors are designed for zero-allocation on the hot path: fixed-width values
 * are read straight from the native WebSocket payload buffer, and string/varchar
 * access returns a reusable {@link DirectUtf8Sequence} view ({@link #getStrA} /
 * {@link #getStrB}) that re-points at the underlying bytes with each call.
 * <p>
 * Convenience accessors that materialise heap objects ({@link #getString},
 * {@link #getVarchar}, {@link #getLongArray}, {@link #getValue}) are provided for
 * ergonomics but allocate; use the native-view accessors on the hot path.
 */
public class QwpColumnBatch {

    final ObjList<QwpColumnLayout> columnLayouts = new ObjList<>();
    long batchSeq;
    int columnCount;
    ObjList<QwpEgressColumnInfo> columns;
    long payloadAddr;
    long payloadLimit;
    long requestId;
    int rowCount;
    // Reusable views for zero-alloc UTF-8 access. strA and strB are dual views
    // (same pattern as QuestDB Record.getStrA/getStrB) so callers can compare
    // two cells without one overwriting the other.
    private final DirectUtf8String strA = new DirectUtf8String();
    private final DirectUtf8String strB = new DirectUtf8String();
    private final DirectUtf8String varcharA = new DirectUtf8String();
    private final DirectUtf8String varcharB = new DirectUtf8String();

    public long batchSeq() {
        return batchSeq;
    }

    public boolean getBool(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return false;
        int denseIdx = l.nonNullIdx[row];
        // Bit-packed: 8 values per byte, LSB-first
        byte b = Unsafe.getUnsafe().getByte(l.valuesAddr + (denseIdx >>> 3));
        return (b & (1 << (denseIdx & 7))) != 0;
    }

    /**
     * Returns a single BYTE value without the type-dispatch branch in {@link #getLong}.
     * The caller must know the column is BYTE.
     */
    public byte getByteValue(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0;
        return Unsafe.getUnsafe().getByte(l.valuesAddr + l.nonNullIdx[row]);
    }

    /**
     * Returns a single CHAR value. Caller must know the column is CHAR.
     */
    public char getCharValue(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0;
        return (char) Unsafe.getUnsafe().getShort(l.valuesAddr + 2L * l.nonNullIdx[row]);
    }

    public int getColumnCount() {
        return columnCount;
    }

    public String getColumnName(int col) {
        return columns.getQuick(col).name;
    }

    public byte getColumnWireType(int col) {
        return columns.getQuick(col).wireType;
    }

    public int getDecimalScale(int col) {
        return columns.getQuick(col).scale;
    }

    /**
     * Returns the high 64 bits of a DECIMAL128 value. Combine with {@link #getDecimal128Low}.
     */
    public long getDecimal128High(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0L;
        return Unsafe.getUnsafe().getLong(l.valuesAddr + 16L * l.nonNullIdx[row] + 8L);
    }

    /**
     * Returns the low 64 bits of a DECIMAL128 value.
     */
    public long getDecimal128Low(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0L;
        return Unsafe.getUnsafe().getLong(l.valuesAddr + 16L * l.nonNullIdx[row]);
    }

    public double getDouble(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return Double.NaN;
        return Unsafe.getUnsafe().getDouble(l.valuesAddr + 8L * l.nonNullIdx[row]);
    }

    public float getFloat(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return Float.NaN;
        return Unsafe.getUnsafe().getFloat(l.valuesAddr + 4L * l.nonNullIdx[row]);
    }

    /**
     * Returns a single INT value without type dispatch. Caller must know the
     * column is INT or IPv4. Returns 0 for NULL rows.
     */
    public int getIntValue(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0;
        return Unsafe.getUnsafe().getInt(l.valuesAddr + 4L * l.nonNullIdx[row]);
    }

    public int getGeohashPrecisionBits(int col) {
        return columns.getQuick(col).precisionBits;
    }

    /**
     * Returns a LONG / INT / SHORT / BYTE / CHAR / TIMESTAMP / DATE / DECIMAL64 / GEOHASH value,
     * dispatching by the column's wire type. Convenience for schema-agnostic code.
     * Hot loops should call the type-specific accessors ({@link #getLongValue},
     * {@link #getIntValue}, {@link #getShortValue}, {@link #getByteValue}, {@link #getCharValue})
     * to skip the branch chain.
     * Returns 0 for NULL rows; use {@link #isNull} first when that matters.
     */
    public long getLong(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0L;
        byte wt = l.info.wireType;
        int denseIdx = l.nonNullIdx[row];
        if (wt == QwpConstants.TYPE_LONG || wt == QwpConstants.TYPE_DATE
                || wt == QwpConstants.TYPE_TIMESTAMP || wt == QwpConstants.TYPE_TIMESTAMP_NANOS
                || wt == QwpConstants.TYPE_DECIMAL64) {
            return Unsafe.getUnsafe().getLong(l.valuesAddr + 8L * denseIdx);
        }
        if (wt == QwpConstants.TYPE_INT) {
            return Unsafe.getUnsafe().getInt(l.valuesAddr + 4L * denseIdx);
        }
        if (wt == QwpConstants.TYPE_SHORT || wt == QwpConstants.TYPE_CHAR) {
            return Unsafe.getUnsafe().getShort(l.valuesAddr + 2L * denseIdx);
        }
        if (wt == QwpConstants.TYPE_BYTE) {
            return Unsafe.getUnsafe().getByte(l.valuesAddr + denseIdx);
        }
        if (wt == QwpConstants.TYPE_GEOHASH) {
            int precBits = l.info.precisionBits;
            int bytesPerValue = (precBits + 7) >>> 3;
            long p = l.valuesAddr + (long) bytesPerValue * denseIdx;
            long bits = 0;
            for (int b = 0; b < bytesPerValue; b++) {
                bits |= ((long) (Unsafe.getUnsafe().getByte(p + b) & 0xFF)) << (b * 8);
            }
            return bits;
        }
        throw new IllegalStateException("getLong() not applicable for wire type 0x"
                + Integer.toHexString(wt & 0xFF));
    }

    /**
     * Returns an 8-byte LONG / TIMESTAMP / DATE / DECIMAL64 value without type dispatch.
     * Caller must know the column is a LONG-family type. Returns 0 for NULL rows.
     */
    public long getLongValue(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0L;
        return Unsafe.getUnsafe().getLong(l.valuesAddr + 8L * l.nonNullIdx[row]);
    }

    /**
     * Returns a SHORT value without type dispatch. Caller must know the column is SHORT.
     */
    public short getShortValue(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0;
        return Unsafe.getUnsafe().getShort(l.valuesAddr + 2L * l.nonNullIdx[row]);
    }

    /**
     * Convenience: returns a length-{@code N} long array with the components of a UUID,
     * LONG256, DECIMAL128, or DECIMAL256 value. Allocates — avoid on the hot path;
     * use {@link #getUuidLo}/{@link #getUuidHi} or {@link #getLong256Word} instead.
     */
    public long[] getLongArray(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return null;
        byte wt = l.info.wireType;
        int denseIdx = l.nonNullIdx[row];
        if (wt == QwpConstants.TYPE_UUID || wt == QwpConstants.TYPE_DECIMAL128) {
            long base = l.valuesAddr + 16L * denseIdx;
            return new long[]{Unsafe.getUnsafe().getLong(base), Unsafe.getUnsafe().getLong(base + 8)};
        }
        if (wt == QwpConstants.TYPE_LONG256 || wt == QwpConstants.TYPE_DECIMAL256) {
            long base = l.valuesAddr + 32L * denseIdx;
            return new long[]{
                    Unsafe.getUnsafe().getLong(base),
                    Unsafe.getUnsafe().getLong(base + 8),
                    Unsafe.getUnsafe().getLong(base + 16),
                    Unsafe.getUnsafe().getLong(base + 24)
            };
        }
        throw new IllegalStateException("getLongArray() not applicable for wire type 0x"
                + Integer.toHexString(wt & 0xFF));
    }

    /**
     * Returns one of the four 64-bit words of a LONG256 or DECIMAL256 value.
     * {@code wordIndex} 0 is least significant, 3 is most significant.
     */
    public long getLong256Word(int col, int row, int wordIndex) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0L;
        return Unsafe.getUnsafe().getLong(l.valuesAddr + 32L * l.nonNullIdx[row] + 8L * wordIndex);
    }

    // =============================================================================
    // Raw column-address API — for zero-branch hot inner loops.
    //
    // Typical usage:
    //   long base = batch.valuesAddr(col);
    //   int[] idx = batch.nonNullIndex(col);
    //   for (int r = 0; r < rowCount; r++) {
    //       if (idx[r] < 0) continue;              // NULL
    //       long v = Unsafe.getLong(base + 8L * idx[r]);
    //       ...
    //   }
    //
    // All four accessors return constant-time views; no allocation.
    // =============================================================================

    /**
     * Number of non-null rows in this column, i.e. the count of entries in the
     * dense values array pointed to by {@link #valuesAddr(int)}.
     */
    public int nonNullCount(int col) {
        return columnLayouts.getQuick(col).nonNullCount;
    }

    /**
     * Per-row lookup table. {@code result[row]} is the dense index within the
     * column's non-null values, or -1 if the row is NULL. Array length equals
     * {@link #getRowCount()}. Valid only during the current {@code onBatch}
     * callback; do not retain.
     */
    public int[] nonNullIndex(int col) {
        return columnLayouts.getQuick(col).nonNullIdx;
    }

    /**
     * Address of the column's null bitmap, or {@code 0} if the column has no NULL rows.
     * Bitmap is {@code ceil(rowCount / 8)} bytes, LSB-first; bit = 1 means NULL.
     */
    public long nullBitmapAddr(int col) {
        return columnLayouts.getQuick(col).nullBitmapAddr;
    }

    /**
     * Address of the column's packed non-null values in the payload buffer.
     * Layout depends on the wire type:
     * <ul>
     *   <li>Fixed-width (LONG, INT, DOUBLE, UUID, LONG256, etc.): contiguous values, index by {@code nonNullIndex(col)[row] * sizeBytes}.</li>
     *   <li>BOOLEAN: bit-packed, 8 values per byte, LSB-first; index by {@code nonNullIndex(col)[row]}.</li>
     *   <li>STRING / VARCHAR: points to the (N+1) × uint32 offsets array; use {@link #stringBytesAddr(int)} for the bytes region.</li>
     *   <li>GEOHASH: {@code ceil(precisionBits / 8)} bytes per value; index by {@code nonNullIndex(col)[row] * bytesPerValue}.</li>
     *   <li>DECIMAL64/128/256: the scale byte has already been consumed; this is the first unscaled value.</li>
     *   <li>SYMBOL: not meaningful — use {@link #getStrA} / {@link #getStrB} instead.</li>
     *   <li>ARRAY: not meaningful — use the per-row {@code arrayRowAddr} accessors (forthcoming).</li>
     * </ul>
     */
    public long valuesAddr(int col) {
        return columnLayouts.getQuick(col).valuesAddr;
    }

    /**
     * For STRING / VARCHAR columns, the address of the concatenated UTF-8 bytes
     * (immediately after the offsets array pointed to by {@link #valuesAddr}).
     * Combined with the offsets array, lets you read every string without
     * going through {@link #getStrA}.
     */
    public long stringBytesAddr(int col) {
        return columnLayouts.getQuick(col).stringBytesAddr;
    }

    public int getRowCount() {
        return rowCount;
    }

    /**
     * Zero-allocation UTF-8 view over the STRING / VARCHAR / SYMBOL value at
     * {@code (col, row)}. The returned view is invalidated by the next call to
     * {@code getStrA} / {@code getStrB} / {@code getVarcharA} / {@code getVarcharB}
     * or once the enclosing {@code onBatch} callback returns.
     */
    public DirectUtf8Sequence getStrA(int col, int row) {
        return lookupStringBytes(col, row, strA);
    }

    /**
     * Dual of {@link #getStrA}; use when you need to hold two string views concurrently.
     */
    public DirectUtf8Sequence getStrB(int col, int row) {
        return lookupStringBytes(col, row, strB);
    }

    /**
     * Heap-allocating convenience. Returns a {@link String} for STRING / SYMBOL
     * columns, or the UTF-8 bytes as a {@link String} for VARCHAR. Returns {@code null}
     * for NULL rows. Allocates; on the hot path prefer {@link #getStrA}.
     */
    public String getString(int col, int row) {
        DirectUtf8Sequence v = lookupStringBytes(col, row, strA);
        if (v == null) return null;
        int size = v.size();
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = v.byteAt(i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Returns the high 64 bits of a UUID value.
     */
    public long getUuidHi(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0L;
        return Unsafe.getUnsafe().getLong(l.valuesAddr + 16L * l.nonNullIdx[row] + 8L);
    }

    /**
     * Returns the low 64 bits of a UUID value.
     */
    public long getUuidLo(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0L;
        return Unsafe.getUnsafe().getLong(l.valuesAddr + 16L * l.nonNullIdx[row]);
    }

    /**
     * Heap-allocating convenience: returns the boxed raw value using the same rules
     * as the legacy API (Boolean, Long, Float, Double, String, byte[], long[]).
     * Allocates per call. Prefer the typed accessors.
     */
    public Object getValue(int col, int row) {
        if (isNull(col, row)) return null;
        QwpColumnLayout l = columnLayouts.getQuick(col);
        byte wt = l.info.wireType;
        if (wt == QwpConstants.TYPE_BOOLEAN) return getBool(col, row);
        if (wt == QwpConstants.TYPE_FLOAT) return getFloat(col, row);
        if (wt == QwpConstants.TYPE_DOUBLE) return getDouble(col, row);
        if (wt == QwpConstants.TYPE_STRING || wt == QwpConstants.TYPE_SYMBOL) return getString(col, row);
        if (wt == QwpConstants.TYPE_VARCHAR) return getVarchar(col, row);
        if (wt == QwpConstants.TYPE_UUID || wt == QwpConstants.TYPE_DECIMAL128
                || wt == QwpConstants.TYPE_LONG256 || wt == QwpConstants.TYPE_DECIMAL256) {
            return getLongArray(col, row);
        }
        return getLong(col, row);
    }

    /**
     * Heap-allocating convenience. Returns the raw UTF-8 bytes of a VARCHAR value,
     * or {@code null} for NULL rows. Allocates; on the hot path prefer
     * {@link #getVarcharA}.
     */
    public byte[] getVarchar(int col, int row) {
        DirectUtf8Sequence v = lookupStringBytes(col, row, varcharA);
        if (v == null) return null;
        int size = v.size();
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = v.byteAt(i);
        }
        return bytes;
    }

    /**
     * Zero-allocation VARCHAR view. Semantically identical to {@link #getStrA} on
     * VARCHAR columns but conventionally paired with {@link #getVarcharB}.
     */
    public DirectUtf8Sequence getVarcharA(int col, int row) {
        return lookupStringBytes(col, row, varcharA);
    }

    public DirectUtf8Sequence getVarcharB(int col, int row) {
        return lookupStringBytes(col, row, varcharB);
    }

    public boolean isNull(int col, int row) {
        return isLayoutNull(columnLayouts.getQuick(col), row);
    }

    /**
     * Fast null check once the layout is in hand. Inlining pattern used by all the
     * typed accessors: load layout once, check bitmap, read value. Eliminates the
     * second {@code ObjList.getQuick(col)} that separate {@code isNull(col,row)} would cost.
     */
    private static boolean isLayoutNull(QwpColumnLayout l, int row) {
        if (l.nullBitmapAddr == 0) return false;
        byte bm = Unsafe.getUnsafe().getByte(l.nullBitmapAddr + (row >>> 3));
        return (bm & (1 << (row & 7))) != 0;
    }

    public long requestId() {
        return requestId;
    }

    /**
     * Resolves the {@code (col, row)} cell for STRING / VARCHAR / SYMBOL columns and
     * points the supplied view at the underlying bytes in the payload buffer.
     * Returns {@code null} for NULL rows or unsupported wire types.
     */
    private DirectUtf8Sequence lookupStringBytes(int col, int row, DirectUtf8String view) {
        if (isNull(col, row)) return null;
        QwpColumnLayout l = columnLayouts.getQuick(col);
        byte wt = l.info.wireType;
        int denseIdx = l.nonNullIdx[row];
        if (wt == QwpConstants.TYPE_STRING || wt == QwpConstants.TYPE_VARCHAR) {
            int startOff = Unsafe.getUnsafe().getInt(l.valuesAddr + 4L * denseIdx);
            int endOff = Unsafe.getUnsafe().getInt(l.valuesAddr + 4L * (denseIdx + 1));
            return view.of(l.stringBytesAddr + startOff, l.stringBytesAddr + endOff);
        }
        if (wt == QwpConstants.TYPE_SYMBOL) {
            int dictIdx = l.symbolRowIds[row];
            DirectUtf8String entry = l.symbolDict.getQuick(dictIdx);
            return view.of(entry.ptr(), entry.ptr() + entry.size());
        }
        return null;
    }
}
