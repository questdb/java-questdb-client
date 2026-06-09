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
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.Long256Sink;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.Uuid;
import io.questdb.client.std.bytes.DirectByteSequence;
import io.questdb.client.std.str.CharSink;
import io.questdb.client.std.str.DirectUtf8Sequence;

/**
 * Column-pinned view over a {@link QwpColumnBatch}. Resolves the column layout
 * once at {@link #of(int)} time, then exposes per-row accessors that take only
 * the row index. A column-first scan over N rows therefore pays one layout
 * lookup instead of N:
 * <pre>
 *   ColumnView prices = batch.column(2);
 *   for (int r = 0; r &lt; batch.getRowCount(); r++) {
 *       sum += prices.getDoubleValue(r);
 *   }
 * </pre>
 * <p>
 * <strong>Lifetime.</strong> Owned by the batch, reused across rows of the
 * pinned column, and valid only for the duration of the surrounding
 * {@code onBatch} call. The batch keeps one {@code ColumnView} per column index
 * so two views over different columns can be held concurrently; calling
 * {@link QwpColumnBatch#column(int)} a second time with the same {@code col}
 * returns the same instance and keeps prior bindings of other columns intact.
 * <p>
 * <strong>Direct-address surface.</strong> The {@code valuesAddr},
 * {@code nullBitmapAddr}, {@code stringBytesAddr}, {@code symbolDict*}, and
 * {@code arrayRowAddr} accessors expose raw native pointers into the WebSocket
 * payload buffer for SIMD or JNI consumers. The layout of the bytes behind
 * these addresses follows the QWP wire format (see
 * {@code https://questdb.com/docs/connect/wire-protocols/qwp-egress-websocket/});
 * these methods are an expert API and may shift as the wire format evolves.
 * <p>
 * <strong>Type contract / NULL handling.</strong> Each typed accessor delegates
 * to the same read path as the corresponding {@code QwpColumnBatch}
 * {@code (col, row)} primitive; the contracts in {@link QwpColumnBatch} apply
 * unchanged.
 */
public class ColumnView {

    private final QwpColumnBatch batch;
    private int bytesPerValue;
    private int col;
    private QwpColumnLayout layout;

    ColumnView(QwpColumnBatch batch) {
        this.batch = batch;
    }

    /**
     * Address (or 0) of a single ARRAY value's packed bytes. Format is the QWP
     * array encoding: 1 byte ndims, ndims x int32 dim sizes, then the flat
     * row-major elements. Returns 0 if the row is NULL. Caller must know the
     * column is an ARRAY type.
     */
    public long arrayRowAddr(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return 0L;
        return layout.arrayRowAddr[row];
    }

    /**
     * Returns the parent batch. Use it to read column-level metadata not
     * exposed on this view ({@link QwpColumnBatch#getColumnName},
     * {@link QwpColumnBatch#getDecimalScale}, etc.) or to switch to row-first
     * iteration without capturing an extra reference.
     */
    public QwpColumnBatch batch() {
        return batch;
    }

    /**
     * Per-value stride in bytes for fixed-width types, including GEOHASH (where
     * stride is {@code ceil(precisionBits / 8)}). Returns 0 for BOOLEAN
     * (bit-packed, 8 values per byte) and -1 for variable-width types
     * (STRING, VARCHAR, BINARY, SYMBOL, ARRAY, DOUBLE_ARRAY, LONG_ARRAY).
     * Combine with {@link #valuesAddr()} and {@link #nonNullCount()} to walk
     * the dense values array directly.
     */
    public int bytesPerValue() {
        return bytesPerValue;
    }

    /**
     * Index of the column this view is pinned to.
     */
    public int getColumnIndex() {
        return col;
    }

    /**
     * Wire type code for the pinned column. Same value as
     * {@link QwpColumnBatch#getColumnWireType(int)}.
     */
    public byte getColumnWireType() {
        return layout.info.wireType;
    }

    /**
     * @see QwpColumnBatch#getArrayNDims(int, int)
     */
    public int getArrayNDims(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return 0;
        return Unsafe.getUnsafe().getByte(layout.arrayRowAddr[row]) & 0xFF;
    }

    /**
     * @see QwpColumnBatch#getBinary(int, int)
     */
    public byte[] getBinary(int row) {
        return batch.getBinary(col, row);
    }

    /**
     * @see QwpColumnBatch#getBinaryA(int, int)
     */
    public DirectByteSequence getBinaryA(int row) {
        return batch.getBinaryA(col, row);
    }

    /**
     * @see QwpColumnBatch#getBinaryB(int, int)
     */
    public DirectByteSequence getBinaryB(int row) {
        return batch.getBinaryB(col, row);
    }

    /**
     * @see QwpColumnBatch#getBoolValue(int, int)
     */
    public boolean getBoolValue(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return false;
        int denseIdx = layout.denseIndex(row);
        byte b = Unsafe.getUnsafe().getByte(layout.valuesAddr + (denseIdx >>> 3));
        return (b & (1 << (denseIdx & 7))) != 0;
    }

    /**
     * @see QwpColumnBatch#getByteValue(int, int)
     */
    public byte getByteValue(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return 0;
        return Unsafe.getUnsafe().getByte(layout.valuesAddr + layout.denseIndex(row));
    }

    /**
     * @see QwpColumnBatch#getCharValue(int, int)
     */
    public char getCharValue(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return 0;
        return (char) Unsafe.getUnsafe().getShort(layout.valuesAddr + 2L * layout.denseIndex(row));
    }

    /**
     * @see QwpColumnBatch#getDecimal128(int, int, Decimal128)
     */
    public boolean getDecimal128(int row, Decimal128 sink) {
        return batch.getDecimal128(col, row, sink);
    }

    /**
     * @see QwpColumnBatch#getDecimal128High(int, int)
     */
    public long getDecimal128High(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return 0L;
        return Unsafe.getUnsafe().getLong(layout.valuesAddr + 16L * layout.denseIndex(row) + 8L);
    }

    /**
     * @see QwpColumnBatch#getDecimal128Low(int, int)
     */
    public long getDecimal128Low(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return 0L;
        return Unsafe.getUnsafe().getLong(layout.valuesAddr + 16L * layout.denseIndex(row));
    }

    /**
     * @see QwpColumnBatch#getDecimal256(int, int, Decimal256)
     */
    public boolean getDecimal256(int row, Decimal256 sink) {
        return batch.getDecimal256(col, row, sink);
    }

    /**
     * @see QwpColumnBatch#getDecimal64(int, int, Decimal64)
     */
    public boolean getDecimal64(int row, Decimal64 sink) {
        return batch.getDecimal64(col, row, sink);
    }

    /**
     * @see QwpColumnBatch#getDoubleArrayElements(int, int)
     */
    public double[] getDoubleArrayElements(int row) {
        return batch.getDoubleArrayElements(col, row);
    }

    /**
     * @see QwpColumnBatch#getDoubleValue(int, int)
     */
    public double getDoubleValue(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return Double.NaN;
        return Unsafe.getUnsafe().getDouble(layout.valuesAddr + 8L * layout.denseIndex(row));
    }

    /**
     * @see QwpColumnBatch#getFloatValue(int, int)
     */
    public float getFloatValue(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return Float.NaN;
        return Unsafe.getUnsafe().getFloat(layout.valuesAddr + 4L * layout.denseIndex(row));
    }

    /**
     * @see QwpColumnBatch#getGeohashValue(int, int)
     */
    public long getGeohashValue(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return 0L;
        int denseIdx = layout.denseIndex(row);
        int bpv = bytesPerValue;
        long p = layout.valuesAddr + (long) bpv * denseIdx;
        long bits = 0;
        for (int b = 0; b < bpv; b++) {
            bits |= ((long) (Unsafe.getUnsafe().getByte(p + b) & 0xFF)) << (b * 8);
        }
        return bits;
    }

    /**
     * @see QwpColumnBatch#getIntValue(int, int)
     */
    public int getIntValue(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return 0;
        return Unsafe.getUnsafe().getInt(layout.valuesAddr + 4L * layout.denseIndex(row));
    }

    /**
     * @see QwpColumnBatch#getLong256(int, int, Long256Sink)
     */
    public boolean getLong256(int row, Long256Sink sink) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return false;
        sink.fromAddress(layout.valuesAddr + 32L * layout.denseIndex(row));
        return true;
    }

    /**
     * @see QwpColumnBatch#getLong256Word(int, int, int)
     */
    public long getLong256Word(int row, int wordIndex) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return 0L;
        return Unsafe.getUnsafe().getLong(layout.valuesAddr + 32L * layout.denseIndex(row) + 8L * wordIndex);
    }

    /**
     * @see QwpColumnBatch#getLongValue(int, int)
     */
    public long getLongValue(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return 0L;
        return Unsafe.getUnsafe().getLong(layout.valuesAddr + 8L * layout.denseIndex(row));
    }

    /**
     * @see QwpColumnBatch#getShortValue(int, int)
     */
    public short getShortValue(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return 0;
        return Unsafe.getUnsafe().getShort(layout.valuesAddr + 2L * layout.denseIndex(row));
    }

    /**
     * @see QwpColumnBatch#getStrA(int, int)
     */
    public DirectUtf8Sequence getStrA(int row) {
        return batch.getStrA(col, row);
    }

    /**
     * @see QwpColumnBatch#getStrB(int, int)
     */
    public DirectUtf8Sequence getStrB(int row) {
        return batch.getStrB(col, row);
    }

    /**
     * @see QwpColumnBatch#getString(int, int)
     */
    public String getString(int row) {
        return batch.getString(col, row);
    }

    /**
     * @see QwpColumnBatch#getString(int, int, CharSink)
     */
    public boolean getString(int row, CharSink<?> sink) {
        return batch.getString(col, row, sink);
    }

    /**
     * @see QwpColumnBatch#getSymbol(int, int)
     */
    public String getSymbol(int row) {
        return batch.getSymbol(col, row);
    }

    /**
     * @see QwpColumnBatch#getSymbolId(int, int)
     */
    public int getSymbolId(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return -1;
        return layout.symbolRowIds[row];
    }

    /**
     * @see QwpColumnBatch#getUuid(int, int, Uuid)
     */
    public boolean getUuid(int row, Uuid sink) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return false;
        sink.fromAddress(layout.valuesAddr + 16L * layout.denseIndex(row));
        return true;
    }

    /**
     * @see QwpColumnBatch#getUuidHi(int, int)
     */
    public long getUuidHi(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return 0L;
        return Unsafe.getUnsafe().getLong(layout.valuesAddr + 16L * layout.denseIndex(row) + 8L);
    }

    /**
     * @see QwpColumnBatch#getUuidLo(int, int)
     */
    public long getUuidLo(int row) {
        if (QwpColumnBatch.isLayoutNull(layout, row)) return 0L;
        return Unsafe.getUnsafe().getLong(layout.valuesAddr + 16L * layout.denseIndex(row));
    }

    /**
     * @see QwpColumnBatch#isNull(int, int)
     */
    public boolean isNull(int row) {
        return QwpColumnBatch.isLayoutNull(layout, row);
    }

    /**
     * Dense index of {@code row} into the column's packed non-null values, or
     * undefined for NULL rows (call {@link #isNull(int)} first). Equivalent to
     * looking up {@code QwpColumnBatch.nonNullIndex(col)[row]} but skips the
     * per-call lookup of the column array.
     */
    public int nonNullIndex(int row) {
        return layout.denseIndex(row);
    }

    /**
     * Number of non-null rows for the pinned column in this batch. Same as
     * {@link QwpColumnBatch#nonNullCount(int)}; exposed here so SIMD consumers
     * iterating raw {@link #valuesAddr()} bytes have the trip count without a
     * second lookup.
     */
    public int nonNullCount() {
        return layout.nonNullCount;
    }

    /**
     * Address of the per-row null bitmap. {@code 0} when the column has no
     * nulls in this batch, in which case row index equals dense index for all
     * rows. Otherwise the bitmap is little-endian byte-packed, 8 rows per byte,
     * LSB-first: row {@code r} is NULL iff {@code (bitmap[r >>> 3] & (1 << (r &amp; 7))) != 0}.
     * The bitmap is sized to {@code ceil(rowCount / 8)} bytes.
     */
    public long nullBitmapAddr() {
        return layout.nullBitmapAddr;
    }

    /**
     * Re-points this flyweight at column {@code col} of the parent batch.
     * Resolves the column layout once and caches it; subsequent per-row
     * accessors avoid the {@code ObjList.getQuick(col)} that the
     * {@link QwpColumnBatch} two-arg primitives pay per call.
     */
    public ColumnView of(int col) {
        this.col = col;
        this.layout = batch.columnLayouts.getQuick(col);
        this.bytesPerValue = QwpConstants.getFixedTypeSize(layout.info.wireType);
        if (this.bytesPerValue == -1 && layout.info.wireType == QwpConstants.TYPE_GEOHASH) {
            this.bytesPerValue = (layout.info.precisionBits + 7) >>> 3;
        }
        return this;
    }

    /**
     * Address of the SYMBOL dictionary's packed entries array. Each entry is
     * an 8-byte pair {@code (offset:i32 | length:i32<<32)} relative to
     * {@link #symbolDictHeapAddr()}. Returns 0 for non-SYMBOL columns.
     */
    public long symbolDictEntriesAddr() {
        return layout.symbolDictEntriesAddr;
    }

    /**
     * Address of the SYMBOL dictionary's UTF-8 bytes heap holding all dict
     * entries. Returns 0 for non-SYMBOL columns.
     */
    public long symbolDictHeapAddr() {
        return layout.symbolDictHeapAddr;
    }

    /**
     * Number of valid entries in the SYMBOL dictionary for this batch.
     * Returns 0 for non-SYMBOL columns.
     */
    public int symbolDictSize() {
        return layout.symbolDictSize;
    }

    /**
     * Per-row dictionary IDs for a SYMBOL column. {@code result[row]} is the
     * dict id for that row; values for NULL rows are unspecified -- check
     * {@link #isNull(int)} or {@link #nullBitmapAddr()} first. Returns
     * {@code null} for non-SYMBOL columns. Array is owned by the decoder and
     * valid only for the surrounding {@code onBatch} call.
     */
    public int[] symbolRowIds() {
        return layout.symbolRowIds;
    }

    /**
     * Address of the concatenated UTF-8 bytes for STRING / VARCHAR / BINARY
     * values. The {@link #valuesAddr()} for these columns points at the
     * {@code (N+1) x int32} offsets array; the bytes themselves live at this
     * address. Returns 0 for non-string columns.
     */
    public long stringBytesAddr() {
        return layout.stringBytesAddr;
    }

    /**
     * Base address of the column's packed non-null values in the payload
     * buffer. Same value as {@link QwpColumnBatch#valuesAddr(int)}; see that
     * method's Javadoc for the per-type layout (fixed-width stride, bit-packed
     * BOOLEAN, STRING/VARCHAR offsets array, etc.). Combine with
     * {@link #bytesPerValue()}, {@link #nonNullCount()}, and
     * {@link #nullBitmapAddr()} to drive a SIMD scan.
     */
    public long valuesAddr() {
        return layout.valuesAddr;
    }
}
