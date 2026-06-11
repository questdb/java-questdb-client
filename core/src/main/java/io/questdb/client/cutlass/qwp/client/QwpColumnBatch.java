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
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.Uuid;
import io.questdb.client.std.bytes.DirectByteSequence;
import io.questdb.client.std.bytes.DirectByteSlice;
import io.questdb.client.std.str.CharSink;
import io.questdb.client.std.str.DirectUtf8Sequence;
import io.questdb.client.std.str.DirectUtf8String;

import java.nio.charset.StandardCharsets;

/**
 * Column-major view over one decoded {@code RESULT_BATCH}. Handed to
 * {@link QwpColumnBatchHandler#onBatch} and valid only for the duration of that
 * callback; all pointers and row indices become stale once control returns to
 * the decoder. To retain a value past the callback, copy it out.
 * <p>
 * <strong>Threading:</strong> not thread-safe. The batch is produced on the
 * client's I/O thread and consumed on whichever thread calls
 * {@code onBatch}. Dispatching work to another thread requires copying values
 * first.
 * <p>
 * <strong>Indexing:</strong> {@code col} and {@code row} are 0-based. Out-of-range
 * values surface as {@link ArrayIndexOutOfBoundsException}.
 * <p>
 * <strong>Type contract:</strong> each typed accessor is valid only for a specific
 * wire type (or small family of compatible wire types). Cross-check with
 * {@link #getColumnWireType(int)} before dispatching. Calling an accessor whose
 * type doesn't match the column's wire type yields <em>undefined</em> results --
 * the underlying native read uses a stride that matches the declared type, so
 * you will silently get bytes at the wrong offset.
 * <p>
 * <strong>NULL handling:</strong> call {@link #isNull(int, int)} when NULL vs.
 * value must be distinguished. When a typed accessor is called on a NULL row
 * the returned value is a type-dependent zero: {@code 0} / {@code 0L} for
 * integer and boolean accessors, {@code Double.NaN} / {@code Float.NaN} for
 * floating-point, {@code null} for reference types, {@code false} for sink
 * accessors. Sink accessors leave the sink's previous contents untouched on
 * NULL. Per-accessor NULL behaviour is not repeated in method docs; this
 * paragraph is the contract.
 * <p>
 * <strong>Five zero-allocation idioms</strong> -- pick whichever matches the
 * wire type of the column:
 * <ul>
 *   <li><b>Primitive accessors</b> ({@link #getLongValue}, {@link #getIntValue},
 *       {@link #getDoubleValue}, {@link #getBoolValue}, etc.) read fixed-width
 *       values straight from the payload buffer with one native load.</li>
 *   <li><b>UTF-8 views</b> ({@link #getStrA}, {@link #getStrB}) return reusable
 *       {@link DirectUtf8Sequence} flyweights pointing into the payload (or into
 *       the symbol dict heap for SYMBOL columns). Each call re-points the view,
 *       so hold at most two live views at once via the A/B pair.</li>
 *   <li><b>Sink accessors</b> ({@link #getLong256(int, int, Long256Sink)},
 *       {@link #getUuid(int, int, Uuid)},
 *       {@link #getString(int, int, CharSink)}) copy multi-word or variable-width
 *       values into a caller-owned sink in a single call. Reuse the sink across
 *       rows.</li>
 *   <li><b>Symbol cache</b> ({@link #getSymbol}, {@link #getSymbolForId},
 *       {@link #getSymbolId}) materialises each distinct SYMBOL dict entry into
 *       a {@link String} at most once per batch, regardless of how many rows
 *       reference it.</li>
 *   <li><b>Row / column views</b> ({@link #row(int)}, {@link #column(int)},
 *       {@link #forEachRow(RowCallback)}) wrap the {@code (col, row)} primitives
 *       with single-arg accessors. {@link RowView} pins the current row;
 *       {@link ColumnView} pins the column once and exposes a direct-address
 *       surface ({@link ColumnView#valuesAddr()}, {@link ColumnView#nullBitmapAddr()},
 *       etc.) for SIMD or JNI consumers. Both flyweights are batch-owned and
 *       reused.</li>
 * </ul>
 * The heap-allocating convenience accessors {@link #getString(int, int)},
 * {@link #getBinary(int, int)}, and {@link #getDoubleArrayElements(int, int)}
 * allocate per call and are meant for low-volume access only.
 */
public class QwpColumnBatch {

    final ObjList<QwpColumnLayout> columnLayouts = new ObjList<>();
    // BINARY views -- re-pointed per call, never re-allocated.
    private final DirectByteSlice binaryA = new DirectByteSlice();
    private final DirectByteSlice binaryB = new DirectByteSlice();
    // One ColumnView per column index, lazily created by column(int). Slots are
    // re-pointed in place across batches; the list grows but never shrinks so a
    // wide-then-narrow query sequence keeps reusing the same instances.
    private final ObjList<ColumnView> columnViews = new ObjList<>();
    // Reusable views for zero-alloc UTF-8 access. strA and strB are dual views
    // (same pattern as QuestDB Record.getStrA/getStrB) so callers can compare
    // two cells without one overwriting the other.
    private final DirectUtf8String strA = new DirectUtf8String();
    private final DirectUtf8String strB = new DirectUtf8String();
    long batchSeq;
    int columnCount;
    ObjList<QwpEgressColumnInfo> columns;
    long payloadAddr;
    long payloadLimit;
    long requestId;
    int rowCount;
    // Lazily created on first row(int) / forEachRow call; reused for the batch lifetime.
    private RowView rowView;

    /**
     * Server-assigned monotonic sequence number for this batch within the query.
     * First batch is 0. Useful for cross-referencing client-side timings with
     * server logs or for detecting batch gaps.
     */
    public long batchSeq() {
        return batchSeq;
    }

    /**
     * Returns the {@link ColumnView} for {@code col}, pinned and ready to read.
     * The view is owned by the batch and cached per column index, so calling
     * {@code column(0)} and {@code column(1)} returns two distinct instances
     * that can be held side-by-side. Calling {@code column(c)} a second time
     * with the same {@code c} returns the same instance with its layout
     * pointer refreshed against the current batch state.
     */
    public ColumnView column(int col) {
        if (columnViews.size() <= col) {
            columnViews.setPos(col + 1);
        }
        ColumnView view = columnViews.getQuick(col);
        if (view == null) {
            view = new ColumnView(this);
            columnViews.setQuick(col, view);
        }
        return view.of(col);
    }

    /**
     * Iterates rows in this batch and invokes {@code callback} for each one.
     * The {@link RowView} handed to the callback is re-pointed in place across
     * iterations -- do not retain it past {@link RowCallback#onRow(RowView)}.
     * Throwing from the callback aborts iteration and propagates the exception.
     */
    public void forEachRow(RowCallback callback) {
        if (rowView == null) {
            rowView = new RowView(this);
        }
        int n = rowCount;
        for (int r = 0; r < n; r++) {
            rowView.of(r);
            callback.onRow(rowView);
        }
    }

    /**
     * Returns the dimensionality of the ARRAY value at {@code (col, row)}, or 0 if
     * the row is null. Caller must know the column is an ARRAY type.
     */
    public int getArrayNDims(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0;
        return Unsafe.getUnsafe().getByte(l.arrayRowAddr[row]) & 0xFF;
    }

    /**
     * Heap-allocating convenience. Returns the raw bytes of a BINARY value. Allocates
     * a new {@code byte[]} per call; on the hot path prefer {@link #getBinaryA} which
     * returns a reusable native view.
     */
    public byte[] getBinary(int col, int row) {
        io.questdb.client.std.bytes.DirectByteSequence v = lookupBinaryBytes(col, row, binaryA);
        if (v == null) return null;
        int size = v.size();
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = v.byteAt(i);
        }
        return bytes;
    }

    /**
     * Zero-allocation BINARY view. Returns a {@link DirectByteSequence}
     * pointing into the WebSocket payload buffer. The view is invalidated by the next call to
     * {@link #getBinaryB} on this batch or once the enclosing
     * {@code onBatch} callback returns.
     */
    public DirectByteSequence getBinaryA(int col, int row) {
        return lookupBinaryBytes(col, row, binaryA);
    }

    /**
     * Dual of {@link #getBinaryA}; use when you need to hold two binary views concurrently.
     */
    public io.questdb.client.std.bytes.DirectByteSequence getBinaryB(int col, int row) {
        return lookupBinaryBytes(col, row, binaryB);
    }

    /**
     * Returns a single BOOLEAN value. Caller must know the column is BOOLEAN.
     * Values are bit-packed on the wire (8 per byte).
     */
    public boolean getBoolValue(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return false;
        int denseIdx = l.denseIndex(row);
        // Bit-packed: 8 values per byte, LSB-first
        byte b = Unsafe.getUnsafe().getByte(l.valuesAddr + (denseIdx >>> 3));
        return (b & (1 << (denseIdx & 7))) != 0;
    }

    /**
     * Returns a single BYTE value. The caller must know the column is BYTE.
     */
    public byte getByteValue(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0;
        return Unsafe.getUnsafe().getByte(l.valuesAddr + l.denseIndex(row));
    }

    /**
     * Returns a single CHAR value. Caller must know the column is CHAR.
     */
    public char getCharValue(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0;
        return (char) Unsafe.getUnsafe().getShort(l.valuesAddr + 2L * l.denseIndex(row));
    }

    /**
     * Number of columns in the result schema. Matches the projection of the SELECT.
     */
    public int getColumnCount() {
        return columnCount;
    }

    /**
     * Column name as declared by the schema (e.g. the SELECT alias or underlying
     * column identifier). Same across every batch of the query.
     */
    public String getColumnName(int col) {
        return columns.getQuick(col).name;
    }

    /**
     * QWP wire type code for the column, used to choose the right typed accessor.
     * See {@link io.questdb.client.cutlass.qwp.protocol.QwpConstants} for the
     * {@code TYPE_*} constants.
     */
    public byte getColumnWireType(int col) {
        return columns.getQuick(col).wireType;
    }

    /**
     * Zero-allocation read of a DECIMAL128 value into a caller-supplied
     * {@link Decimal128} sink. Sets the sink's high, low, and scale in a single
     * call. Returns {@code true} on a hit, {@code false} for NULL rows (the
     * sink is left untouched).
     */
    public boolean getDecimal128(int col, int row, Decimal128 sink) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return false;
        long base = l.valuesAddr + 16L * l.denseIndex(row);
        sink.of(
                Unsafe.getUnsafe().getLong(base + 8L),
                Unsafe.getUnsafe().getLong(base),
                columns.getQuick(col).scale
        );
        return true;
    }

    /**
     * Returns the high 64 bits of a DECIMAL128 value. Combine with {@link #getDecimal128Low}.
     */
    public long getDecimal128High(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0L;
        return Unsafe.getUnsafe().getLong(l.valuesAddr + 16L * l.denseIndex(row) + 8L);
    }

    /**
     * Returns the low 64 bits of a DECIMAL128 value.
     */
    public long getDecimal128Low(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0L;
        return Unsafe.getUnsafe().getLong(l.valuesAddr + 16L * l.denseIndex(row));
    }

    /**
     * Zero-allocation read of a DECIMAL256 value into a caller-supplied
     * {@link Decimal256} sink. Sets all four 64-bit words and the scale in a
     * single call. Returns {@code true} on a hit, {@code false} for NULL rows
     * (the sink is left untouched).
     */
    public boolean getDecimal256(int col, int row, Decimal256 sink) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return false;
        long base = l.valuesAddr + 32L * l.denseIndex(row);
        sink.of(
                Unsafe.getUnsafe().getLong(base + 24L),
                Unsafe.getUnsafe().getLong(base + 16L),
                Unsafe.getUnsafe().getLong(base + 8L),
                Unsafe.getUnsafe().getLong(base),
                columns.getQuick(col).scale
        );
        return true;
    }

    /**
     * Zero-allocation read of a DECIMAL64 value into a caller-supplied
     * {@link Decimal64} sink. Sets the unscaled value and scale in a single
     * call. Returns {@code true} on a hit, {@code false} for NULL rows (the
     * sink is left untouched).
     */
    public boolean getDecimal64(int col, int row, Decimal64 sink) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return false;
        long value = Unsafe.getUnsafe().getLong(l.valuesAddr + 8L * l.denseIndex(row));
        sink.of(value, columns.getQuick(col).scale);
        return true;
    }

    /**
     * Scale (number of fractional digits) for DECIMAL64 / DECIMAL128 / DECIMAL256
     * columns. Same across every row in the batch. Undefined for non-DECIMAL columns.
     */
    public int getDecimalScale(int col) {
        return columns.getQuick(col).scale;
    }

    /**
     * Returns the flattened elements of a DOUBLE_ARRAY value in row-major order.
     * Heap-allocating convenience; use {@link #getArrayNDims} to discover
     * dimensionality separately if you need it.
     */
    public double[] getDoubleArrayElements(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return null;
        long rowAddr = l.arrayRowAddr[row];
        int nDims = Unsafe.getUnsafe().getByte(rowAddr) & 0xFF;
        int elements = 1;
        for (int d = 0; d < nDims; d++) {
            elements *= Unsafe.getUnsafe().getInt(rowAddr + 1 + 4L * d);
        }
        double[] out = new double[elements];
        long base = rowAddr + 1 + 4L * nDims;
        for (int i = 0; i < elements; i++) {
            out[i] = Unsafe.getUnsafe().getDouble(base + 8L * i);
        }
        return out;
    }

    /**
     * Returns a single DOUBLE value. Caller must know the column is DOUBLE.
     */
    public double getDoubleValue(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return Double.NaN;
        return Unsafe.getUnsafe().getDouble(l.valuesAddr + 8L * l.denseIndex(row));
    }

    /**
     * Returns a single FLOAT value. Caller must know the column is FLOAT.
     */
    public float getFloatValue(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return Float.NaN;
        return Unsafe.getUnsafe().getFloat(l.valuesAddr + 4L * l.denseIndex(row));
    }

    /**
     * Precision (in bits) of a GEOHASH column, in the range 1..60. Same across
     * every row in the batch. Undefined for non-GEOHASH columns.
     */
    public int getGeohashPrecisionBits(int col) {
        return columns.getQuick(col).precisionBits;
    }

    /**
     * Returns a GEOHASH value packed into a long (up to 60 bits of precision).
     * Caller must know the column is GEOHASH; use {@link #getGeohashPrecisionBits}
     * to retrieve the bit width.
     */
    public long getGeohashValue(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0L;
        int denseIdx = l.denseIndex(row);
        int bytesPerValue = (l.info.precisionBits + 7) >>> 3;
        long p = l.valuesAddr + (long) bytesPerValue * denseIdx;
        long bits = 0;
        for (int b = 0; b < bytesPerValue; b++) {
            bits |= ((long) (Unsafe.getUnsafe().getByte(p + b) & 0xFF)) << (b * 8);
        }
        return bits;
    }

    /**
     * Returns a single INT value. Caller must know the column is INT or IPv4.
     */
    public int getIntValue(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0;
        return Unsafe.getUnsafe().getInt(l.valuesAddr + 4L * l.denseIndex(row));
    }

    /**
     * Zero-allocation read of a LONG256 or DECIMAL256 value. Copies all four
     * 64-bit words into {@code sink} in a single call. Returns {@code true}
     * on a hit, {@code false} for NULL rows (the sink is left untouched).
     * Prefer this over four {@link #getLong256Word} calls on the hot path --
     * one virtual dispatch instead of four, one address computation instead
     * of four.
     */
    public boolean getLong256(int col, int row, Long256Sink sink) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return false;
        sink.fromAddress(l.valuesAddr + 32L * l.denseIndex(row));
        return true;
    }

    /**
     * Returns one of the four 64-bit words of a LONG256 or DECIMAL256 value.
     * {@code wordIndex} 0 is least significant, 3 is most significant. For
     * bulk reads prefer {@link #getLong256(int, int, Long256Sink)}.
     */
    public long getLong256Word(int col, int row, int wordIndex) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0L;
        return Unsafe.getUnsafe().getLong(l.valuesAddr + 32L * l.denseIndex(row) + 8L * wordIndex);
    }

    /**
     * Returns an 8-byte LONG / TIMESTAMP / DATE / DECIMAL64 value.
     * Caller must know the column is a LONG-family type.
     */
    public long getLongValue(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0L;
        return Unsafe.getUnsafe().getLong(l.valuesAddr + 8L * l.denseIndex(row));
    }

    /**
     * Number of rows in this batch, including NULL rows. The server caps this
     * to its {@code max_batch_rows} setting (typically 4096), so a query that
     * returns more rows arrives split across multiple {@code onBatch} calls.
     */
    public int getRowCount() {
        return rowCount;
    }

    /**
     * Returns a SHORT value without type dispatch. Caller must know the column is SHORT.
     */
    public short getShortValue(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0;
        return Unsafe.getUnsafe().getShort(l.valuesAddr + 2L * l.denseIndex(row));
    }

    /**
     * Zero-allocation UTF-8 view over the STRING / VARCHAR / SYMBOL value at
     * {@code (col, row)}. For STRING / VARCHAR the view points into the payload
     * buffer; for SYMBOL it points into the per-batch symbol dictionary heap
     * (same lifetime either way -- both are invalidated when {@code onBatch}
     * returns). The returned view is also invalidated by the next call to
     * {@code getStrA} on this batch; use {@link #getStrB} for the dual slot
     * when two concurrent views are needed.
     */
    public DirectUtf8Sequence getStrA(int col, int row) {
        return lookupStringBytes(col, row, strA);
    }

    /**
     * Dual of {@link #getStrA}; use when you need to hold two string views
     * concurrently (e.g. row-vs-row comparisons within one batch).
     */
    public DirectUtf8Sequence getStrB(int col, int row) {
        return lookupStringBytes(col, row, strB);
    }

    /**
     * Heap-allocating convenience. Returns a {@link String} for STRING / SYMBOL /
     * VARCHAR columns. For STRING / VARCHAR each call allocates a new String; on
     * the hot path prefer {@link #getStrA} or {@link #getString(int, int, CharSink)}.
     * For SYMBOL the result is cached per dict entry (see {@link #getSymbol}) so
     * repeated calls against rows sharing a dict entry return the same String
     * instance.
     */
    public String getString(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return null;
        if (l.info.wireType == QwpConstants.TYPE_SYMBOL) {
            return lookupCachedSymbol(l, l.symbolRowIds[row]);
        }
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
     * Zero-allocation variant of {@link #getString(int, int)}. Writes the STRING /
     * SYMBOL / VARCHAR value at {@code (col, row)} into {@code sink} -- UTF-8 bytes
     * pass straight through to a {@link io.questdb.client.std.str.Utf8Sink}; a
     * {@link io.questdb.client.std.str.Utf16Sink} transcodes to UTF-16 en route.
     * Returns {@code true} when a value was written, {@code false} when the row is
     * NULL (the sink is left untouched).
     */
    public boolean getString(int col, int row, CharSink<?> sink) {
        DirectUtf8Sequence v = lookupStringBytes(col, row, strA);
        if (v == null) return false;
        sink.put(v);
        return true;
    }

    /**
     * Materialises the SYMBOL value at {@code (col, row)} as a {@link String}.
     * The result is cached per dict entry on the batch, so a scan over N rows
     * that share K distinct symbols allocates at most K Strings -- the same
     * String instance is returned for every row pointing at the same dict
     * entry.
     */
    public String getSymbol(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return null;
        return lookupCachedSymbol(l, l.symbolRowIds[row]);
    }

    /**
     * Number of distinct entries in the SYMBOL column's dictionary for this batch.
     * Caller must know the column is SYMBOL. In delta mode this equals the size
     * of the connection-scoped dictionary at the time the batch was decoded.
     */
    public int getSymbolDictSize(int col) {
        return columnLayouts.getQuick(col).symbolDictSize;
    }

    /**
     * Materialises the dict entry at {@code dictId} as a {@link String}, with the
     * same per-entry caching as {@link #getSymbol}. Use together with
     * {@link #getSymbolId} to walk rows by id instead of value -- e.g. to key a
     * {@code HashMap<int, ...>} on dict id without ever allocating a String.
     * Caller must know the column is SYMBOL.
     */
    public String getSymbolForId(int col, int dictId) {
        return lookupCachedSymbol(columnLayouts.getQuick(col), dictId);
    }

    /**
     * Dict id for the SYMBOL value at {@code (col, row)}, or {@code -1} for NULL
     * rows. Ids are stable across a batch and allow id-based processing (see
     * {@link #getSymbolForId}). Caller must know the column is SYMBOL.
     */
    public int getSymbolId(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return -1;
        return l.symbolRowIds[row];
    }

    /**
     * Zero-allocation read of a UUID value. Copies both 64-bit words into
     * {@code sink} in a single call. Returns {@code true} on a hit,
     * {@code false} for NULL rows (the sink is left untouched). Prefer this
     * over paired {@link #getUuidLo} / {@link #getUuidHi} calls on the hot
     * path -- one virtual dispatch instead of two, one address computation
     * instead of two.
     */
    public boolean getUuid(int col, int row, Uuid sink) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return false;
        sink.fromAddress(l.valuesAddr + 16L * l.denseIndex(row));
        return true;
    }

    /**
     * Returns the high 64 bits of a UUID value. For bulk reads prefer
     * {@link #getUuid(int, int, Uuid)}.
     */
    public long getUuidHi(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0L;
        return Unsafe.getUnsafe().getLong(l.valuesAddr + 16L * l.denseIndex(row) + 8L);
    }

    /**
     * Returns the low 64 bits of a UUID value.
     */
    public long getUuidLo(int col, int row) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (isLayoutNull(l, row)) return 0L;
        return Unsafe.getUnsafe().getLong(l.valuesAddr + 16L * l.denseIndex(row));
    }

    /**
     * True if the cell is NULL on the wire.
     * <p>
     * Note on type-specific sentinels (see
     * {@code https://questdb.com/docs/connect/wire-protocols/qwp-egress-websocket/}):
     * QuestDB stores NULL as a sentinel value for several types -- {@code Long.MIN_VALUE}
     * for LONG/INT/etc., {@code 0.0.0.0} for IPv4, {@code -1} for GEOHASH, and crucially
     * {@code NaN} for FLOAT and DOUBLE. Egress preserves these conventions: a row carrying
     * NaN in a DOUBLE column will return {@code true} from this method. Callers who need
     * to distinguish "real NaN" from "explicit NULL" cannot do so over the wire -- both
     * map to the same null bitmap bit.
     */
    public boolean isNull(int col, int row) {
        return isLayoutNull(columnLayouts.getQuick(col), row);
    }

    /**
     * Number of non-null rows in the column for this batch. Equal to
     * {@link #getRowCount()} minus the NULL-row count.
     */
    public int nonNullCount(int col) {
        return columnLayouts.getQuick(col).nonNullCount;
    }

    /**
     * Per-row lookup table. {@code result[row]} is the dense index within the
     * column's non-null values, or -1 if the row is NULL. Array length equals
     * {@link #getRowCount()}. Valid only during the current {@code onBatch}
     * callback; do not retain.
     * <p>
     * Most callers don't need this -- the typed accessors
     * ({@link #getLongValue}, {@link #getDoubleValue}, etc.) already resolve
     * dense indices internally. Use only when writing custom readers against
     * the raw column-address API.
     * <p>
     * For columns with no nulls the decoder skips populating this array, so
     * this accessor lazy-materialises an identity mapping on demand; the
     * result is cached until the next batch.
     */
    public int[] nonNullIndex(int col) {
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (l.nullBitmapAddr == 0 && l.nonNullIdx == null) {
            int[] arr = new int[rowCount];
            for (int i = 0; i < rowCount; i++) arr[i] = i;
            l.nonNullIdx = arr;
        }
        return l.nonNullIdx;
    }

    /**
     * Starting address of the RESULT_BATCH payload this view was decoded from.
     * Equal to the WebSocket payload pointer when in-place decode is in use.
     * Intended for accounting (byte counters) rather than data access.
     */
    public long payloadAddr() {
        return payloadAddr;
    }

    /**
     * Exclusive upper bound of the RESULT_BATCH payload bytes. See
     * {@link #payloadAddr()}.
     */
    public long payloadLimit() {
        return payloadLimit;
    }

    /**
     * Client-assigned request id this batch belongs to. Matches the id the I/O
     * thread attached to the {@code QUERY_REQUEST} frame. Useful for correlating
     * batches with {@code execute} calls in diagnostics.
     */
    public long requestId() {
        return requestId;
    }

    /**
     * Returns the cached {@link RowView} pointed at {@code row}. Use as an
     * escape hatch from {@link #forEachRow(RowCallback)} when you need to
     * drive iteration manually (e.g., interleaved with another data source).
     * Single shared instance per batch; two calls overwrite each other.
     */
    public RowView row(int row) {
        if (rowView == null) {
            rowView = new RowView(this);
        }
        return rowView.of(row);
    }

    /**
     * Address of the column's packed non-null values in the payload buffer.
     * Layout depends on the wire type:
     * <ul>
     *   <li>Fixed-width (LONG, INT, DOUBLE, UUID, LONG256, etc.): contiguous values, index by {@code nonNullIndex(col)[row] * sizeBytes}.</li>
     *   <li>BOOLEAN: bit-packed, 8 values per byte, LSB-first; index by {@code nonNullIndex(col)[row]}.</li>
     *   <li>STRING / VARCHAR: points to the (N+1) x uint32 offsets array; use  for the bytes region.</li>
     *   <li>GEOHASH: {@code ceil(precisionBits / 8)} bytes per value; index by {@code nonNullIndex(col)[row] * bytesPerValue}.</li>
     *   <li>DECIMAL64/128/256: the scale byte has already been consumed; this is the first unscaled value.</li>
     *   <li>SYMBOL: not meaningful -- use {@link #getStrA} / {@link #getStrB} instead.</li>
     *   <li>ARRAY: not meaningful -- use the per-row {@code arrayRowAddr} accessors (forthcoming).</li>
     * </ul>
     */
    public long valuesAddr(int col) {
        return columnLayouts.getQuick(col).valuesAddr;
    }

    /**
     * Resolves the {@code (col, row)} cell for a BINARY column and points the supplied
     * slice at the underlying bytes in the payload buffer. Returns {@code null} for NULL
     * rows or if the column is not BINARY.
     */
    private io.questdb.client.std.bytes.DirectByteSequence lookupBinaryBytes(
            int col, int row, io.questdb.client.std.bytes.DirectByteSlice view) {
        if (isNull(col, row)) return null;
        QwpColumnLayout l = columnLayouts.getQuick(col);
        if (l.info.wireType != QwpConstants.TYPE_BINARY) return null;
        int denseIdx = l.denseIndex(row);
        int startOff = Unsafe.getUnsafe().getInt(l.valuesAddr + 4L * denseIdx);
        int endOff = Unsafe.getUnsafe().getInt(l.valuesAddr + 4L * (denseIdx + 1));
        return view.of(l.stringBytesAddr + startOff, endOff - startOff);
    }

    /**
     * Lazily materialises and caches the String for a SYMBOL dict entry. The
     * cache lives on the {@link QwpColumnLayout} and survives across batches in
     * delta mode, where {@code dictId} resolves to the same bytes for the life
     * of the connection-scoped dict. When the decoder re-stamps
     * {@link QwpColumnLayout#symbolDictVersion} (non-delta batch, or CACHE_RESET
     * bump of the dict generation), this method wipes the cache on the first
     * lookup and catches {@link QwpColumnLayout#symbolCacheVersion} up to the
     * current version.
     */
    private String lookupCachedSymbol(QwpColumnLayout l, int dictId) {
        ObjList<String> cache = l.symbolStringCache;
        if (l.symbolCacheVersion != l.symbolDictVersion) {
            cache.clear();
            l.symbolCacheVersion = l.symbolDictVersion;
        }
        if (cache.size() < l.symbolDictSize) {
            cache.setPos(l.symbolDictSize);
        }
        String s = cache.getQuick(dictId);
        if (s == null) {
            long packed = Unsafe.getUnsafe().getLong(l.symbolDictEntriesAddr + ((long) dictId << 3));
            long start = l.symbolDictHeapAddr + (packed & 0xFFFFFFFFL);
            int len = (int) (packed >>> 32);
            byte[] bytes = new byte[len];
            for (int i = 0; i < len; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(start + i);
            }
            s = new String(bytes, StandardCharsets.UTF_8);
            cache.setQuick(dictId, s);
        }
        return s;
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
        int denseIdx = l.denseIndex(row);
        if (wt == QwpConstants.TYPE_VARCHAR) {
            int startOff = Unsafe.getUnsafe().getInt(l.valuesAddr + 4L * denseIdx);
            int endOff = Unsafe.getUnsafe().getInt(l.valuesAddr + 4L * (denseIdx + 1));
            return view.of(l.stringBytesAddr + startOff, l.stringBytesAddr + endOff);
        }
        if (wt == QwpConstants.TYPE_SYMBOL) {
            int dictIdx = l.symbolRowIds[row];
            // Single 64-bit load: low 32 bits = offset into dict heap, high 32 = length.
            // No ObjList.getQuick, no DirectUtf8String deref -- pure pointer arithmetic.
            long packed = Unsafe.getUnsafe().getLong(l.symbolDictEntriesAddr + ((long) dictIdx << 3));
            long start = l.symbolDictHeapAddr + (packed & 0xFFFFFFFFL);
            long end = start + (packed >>> 32);
            return view.of(start, end);
        }
        return null;
    }

    /**
     * Fast null check once the layout is in hand. Inlining pattern used by all the
     * typed accessors: load layout once, check bitmap, read value. Eliminates the
     * second {@code ObjList.getQuick(col)} that separate {@code isNull(col,row)} would cost.
     * Also reused by {@link ColumnView} so it can keep the layout pointer cached.
     */
    static boolean isLayoutNull(QwpColumnLayout l, int row) {
        if (l.nullBitmapAddr == 0) return false;
        byte bm = Unsafe.getUnsafe().getByte(l.nullBitmapAddr + (row >>> 3));
        return (bm & (1 << (row & 7))) != 0;
    }
}
