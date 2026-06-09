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

package io.questdb.client.cutlass.qwp.protocol;

import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cairo.TableUtils;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.array.ArrayBufferAppender;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.std.CharSequenceIntHashMap;
import io.questdb.client.std.Chars;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.Decimals;
import io.questdb.client.std.LowerCaseCharSequenceIntHashMap;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.NumericException;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.Vect;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.std.str.Utf8s;

import java.util.Arrays;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Buffers rows for a single table in columnar format.
 * <p>
 * Fixed-width column data is stored off-heap via {@link OffHeapAppendMemory} for zero-GC
 * buffering and bulk copy to network buffers. Variable-width data (strings, symbol
 * dictionaries, arrays) remains on-heap.
 */
public class QwpTableBuffer implements QuietCloseable {

    private static final int MAX_COLUMN_NAME_LENGTH = 127;
    private final LowerCaseCharSequenceIntHashMap columnNameToIndex;
    private final ObjList<ColumnBuffer> columns;
    private final QwpWebSocketSender sender;
    private final String tableName;
    private QwpColumnDef[] cachedColumnDefs;
    private int columnAccessCursor; // tracks expected next column index
    private boolean columnDefsCacheValid;
    private int committedColumnCount; // columns that existed at last nextRow()
    private ColumnBuffer[] fastColumns; // plain array for O(1) sequential access
    private int inProgressColumnCount;
    private int rowCount;

    public QwpTableBuffer(String tableName) {
        this(tableName, null);
    }

    /**
     * Use this constructor overload to allow writing to a symbol column.
     * {@link ColumnBuffer#addSymbol(CharSequence)} needs the sender to
     * call {@link QwpWebSocketSender#getOrAddGlobalSymbol(CharSequence)}, registering
     * the symbol in the global dictionary shared with the server.
     */
    public QwpTableBuffer(String tableName, QwpWebSocketSender sender) {
        this.tableName = tableName;
        this.sender = sender;
        this.columns = new ObjList<>();
        this.columnNameToIndex = new LowerCaseCharSequenceIntHashMap();
        this.rowCount = 0;
        this.columnDefsCacheValid = false;
    }

    /**
     * Cancels the current in-progress row.
     * <p>
     * This removes any column values added since the last {@link #nextRow()} call.
     * If no values have been added for the current row, this is a no-op.
     */
    public void cancelCurrentRow() {
        columnAccessCursor = 0;
        inProgressColumnCount = 0;
        for (int i = 0, n = columns.size(); i < n; i++) {
            ColumnBuffer col = fastColumns[i];
            if (i >= committedColumnCount) {
                // Column was created during the in-progress row. Remove all data.
                col.truncateTo(0);
            } else if (col.size > rowCount) {
                // Pre-existing column was set for the in-progress row.
                // Truncate to committed state.
                col.truncateTo(rowCount);
            }
            // else: pre-existing column wasn't touched this row. No-op.
        }
    }

    /**
     * Clears the buffer completely, including column definitions.
     * Frees all off-heap memory.
     */
    public void clear() {
        for (int i = 0, n = columns.size(); i < n; i++) {
            columns.get(i).close();
        }
        columns.clear();
        columnNameToIndex.clear();
        fastColumns = null;
        columnAccessCursor = 0;
        committedColumnCount = 0;
        inProgressColumnCount = 0;
        rowCount = 0;
        columnDefsCacheValid = false;
        cachedColumnDefs = null;
    }

    @Override
    public void close() {
        clear();
    }

    /**
     * Returns the total bytes buffered across all columns.
     * This queries actual buffer sizes, not estimates.
     */
    public long getBufferedBytes() {
        long bytes = 0;
        for (int i = 0, n = columns.size(); i < n; i++) {
            bytes += fastColumns[i].getBufferedBytes();
        }
        return bytes;
    }

    /**
     * Returns the column at the given index.
     */
    public ColumnBuffer getColumn(int index) {
        return columns.get(index);
    }

    /**
     * Returns the number of columns.
     */
    public int getColumnCount() {
        return columns.size();
    }

    /**
     * Returns the column definitions (cached for efficiency).
     */
    public QwpColumnDef[] getColumnDefs() {
        if (!columnDefsCacheValid || cachedColumnDefs == null || cachedColumnDefs.length != columns.size()) {
            cachedColumnDefs = new QwpColumnDef[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                ColumnBuffer col = columns.get(i);
                cachedColumnDefs[i] = new QwpColumnDef(col.name, col.type);
            }
            columnDefsCacheValid = true;
        }
        return cachedColumnDefs;
    }

    /**
     * Returns an existing column with the given name and type, or {@code null} if absent.
     * <p>
     * Uses the same sequential access optimization as {@link #getOrCreateColumn(CharSequence, byte, boolean)}.
     * When the next expected column is accessed in order, the internal cursor advances without a hash lookup.
     */
    public ColumnBuffer getExistingColumn(CharSequence name, byte type) {
        return lookupColumn(name, type);
    }

    /**
     * Gets or creates a column with the given name and type.
     * <p>
     * Optimized for the common case where columns are accessed in the same
     * order every row: a sequential cursor avoids hash map lookups entirely.
     * <p>
     * Returns {@code null} when the column has already been written in the current
     * (uncommitted) row.  Callers must treat a {@code null} return as "duplicate
     * column in this row — skip the write", matching the ILP first-value-wins
     * semantics.  The check is a single field comparison on the hot path and has
     * no measurable cost.
     */
    public ColumnBuffer getOrCreateColumn(CharSequence name, byte type, boolean useNullBitmap) {
        if (name == null || name.length() == 0) {
            throw new LineSenderException("column name cannot be empty");
        }
        ColumnBuffer existing = lookupColumn(name, type);
        if (existing != null) {
            // col.size > rowCount means this column already received a value
            // for the in-progress row.  Silently ignore the duplicate (first
            // value wins, same as the ILP server behaviour).
            if (existing.size > rowCount) {
                return null;
            }
            inProgressColumnCount++;
            return existing;
        }
        if (TableUtils.isValidColumnName(name, MAX_COLUMN_NAME_LENGTH)) {
            ColumnBuffer col = createColumn(name, type, useNullBitmap);
            inProgressColumnCount++;
            return col;
        }
        throw new LineSenderException(
                name.length() > MAX_COLUMN_NAME_LENGTH ? "column name too long [maxLength=" + MAX_COLUMN_NAME_LENGTH + "]"
                        : "column name contains illegal characters: " + name
        );
    }

    public ColumnBuffer getOrCreateDesignatedTimestampColumn(byte type) {
        ColumnBuffer existing = lookupColumn("", type);
        if (existing != null) {
            inProgressColumnCount++;
            return existing;
        }
        ColumnBuffer col = createColumn("", type, true);
        inProgressColumnCount++;
        return col;
    }

    /**
     * Returns the number of rows buffered.
     */
    public int getRowCount() {
        return rowCount;
    }

    /**
     * Returns the table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns true if a row is partially built (columns written but not yet committed
     * via {@link #nextRow()}).
     */
    public boolean hasInProgressRow() {
        return inProgressColumnCount > 0;
    }

    /**
     * Advances to the next row.
     * <p>
     * This should be called after all column values for the current row have been set.
     */
    public void nextRow() {
        // Reset sequential access cursor for the next row
        columnAccessCursor = 0;
        inProgressColumnCount = 0;
        // Ensure all columns have the same row count
        for (int i = 0, n = columns.size(); i < n; i++) {
            ColumnBuffer col = fastColumns[i];
            // If column wasn't set for this row, add a null
            while (col.size < rowCount + 1) {
                col.addNull();
            }
        }
        rowCount++;
        committedColumnCount = columns.size();
    }

    /**
     * Advances to the next row using a prepared list of columns that need null padding.
     * <p>
     * This avoids rescanning every column when the caller has already identified
     * which columns were omitted in the current row.
     */
    public void nextRow(ColumnBuffer[] missingColumns, int missingColumnCount) {
        columnAccessCursor = 0;
        inProgressColumnCount = 0;
        for (int i = 0; i < missingColumnCount; i++) {
            ColumnBuffer col = missingColumns[i];
            while (col.size < rowCount + 1) {
                col.addNull();
            }
        }
        rowCount++;
        committedColumnCount = columns.size();
    }

    /**
     * Resets the buffer for reuse. Keeps column definitions and allocated memory.
     */
    public void reset() {
        for (int i = 0, n = columns.size(); i < n; i++) {
            fastColumns[i].reset();
        }
        columnAccessCursor = 0;
        committedColumnCount = columns.size();
        inProgressColumnCount = 0;
        rowCount = 0;
    }

    public void retainInProgressRow(
            int[] sizeBefore,
            int[] valueCountBefore,
            int[] arrayShapeOffsetBefore,
            int[] arrayDataOffsetBefore
    ) {
        columnAccessCursor = 0;
        for (int i = 0, n = columns.size(); i < n; i++) {
            ColumnBuffer col = fastColumns[i];
            if (sizeBefore[i] > -1) {
                col.retainTailRow(
                        sizeBefore[i],
                        valueCountBefore[i],
                        arrayShapeOffsetBefore[i],
                        arrayDataOffsetBefore[i]
                );
            } else {
                col.clearToEmptyFast();
            }
        }
        rowCount = 0;
        committedColumnCount = columns.size();
    }

    public void rollbackUncommittedColumns() {
        if (columns.size() <= committedColumnCount) {
            return;
        }

        for (int i = columns.size() - 1; i >= committedColumnCount; i--) {
            ColumnBuffer col = columns.getQuick(i);
            if (col != null) {
                col.close();
            }
            columns.remove(i);
        }
        rebuildColumnAccessStructures();
    }

    private static void assertColumnType(CharSequence name, byte type, ColumnBuffer column) {
        if (column.type != type) {
            throw new LineSenderException(
                    "Column type mismatch for column '" + name + "': columnType="
                            + column.type + ", sentType=" + type
            );
        }
    }

    private ColumnBuffer createColumn(CharSequence name, byte type, boolean useNullBitmap) {
        if (columns.size() >= MAX_COLUMNS_PER_TABLE) {
            throw new LineSenderException("column count exceeds maximum: " + (columns.size() + 1) +
                    " (max " + MAX_COLUMNS_PER_TABLE + ")"
            );
        }
        ColumnBuffer col = new ColumnBuffer(Chars.toString(name), type, useNullBitmap);
        try {
            col.sender = sender;
            int index = columns.size();
            col.index = index;
            columns.add(col);
            columnNameToIndex.put(name, index);
            // Update fast access array
            if (fastColumns == null || index >= fastColumns.length) {
                int newLen = Math.max(8, index + 4);
                ColumnBuffer[] newArr = new ColumnBuffer[newLen];
                if (fastColumns != null) {
                    System.arraycopy(fastColumns, 0, newArr, 0, index);
                }
                fastColumns = newArr;
            }
            fastColumns[index] = col;
            // Pre-pad with nulls for already-committed rows so the next
            // value the caller adds lands at the correct row position.
            for (int r = 0; r < rowCount; r++) {
                col.addNull();
            }
            columnDefsCacheValid = false;
        } catch (Throwable t) {
            col.close();
            throw t;
        }
        return col;
    }

    private ColumnBuffer lookupColumn(CharSequence name, byte type) {
        // Fast path: predict next column in sequence
        int n = columns.size();
        if (columnAccessCursor < n) {
            ColumnBuffer candidate = fastColumns[columnAccessCursor];
            if (Chars.equalsIgnoreCase(candidate.name, name)) {
                columnAccessCursor++;
                assertColumnType(name, type, candidate);
                return candidate;
            }
        }

        // Slow path: hash map lookup
        int idx = columnNameToIndex.get(name);
        if (idx >= 0) {
            ColumnBuffer existing = columns.get(idx);
            assertColumnType(name, type, existing);
            return existing;
        }

        return null;
    }

    private void rebuildColumnAccessStructures() {
        columnNameToIndex.clear();

        int columnCount = columns.size();
        int minCapacity = Math.max(8, columnCount + 4);
        if (fastColumns == null || fastColumns.length < minCapacity) {
            fastColumns = new ColumnBuffer[minCapacity];
        } else {
            Arrays.fill(fastColumns, null);
        }

        for (int i = 0; i < columnCount; i++) {
            ColumnBuffer col = columns.getQuick(i);
            col.index = i;
            fastColumns[i] = col;
            columnNameToIndex.put(col.name, i);
        }

        columnDefsCacheValid = false;
        cachedColumnDefs = null;
    }

    /**
     * Returns the in-memory buffer element stride in bytes. This is the size used
     * to store each value in the client's off-heap {@link OffHeapAppendMemory} buffer.
     * This is different from element size on the wire.
     * <p>
     * For example, BOOLEAN is stored as 1 byte per value here (for easy indexed access)
     * but bit-packed on the wire; GEOHASH is stored as 8-byte longs here but uses
     * variable-width encoding on the wire.
     * <p>
     * Returns 0 for variable-width types (string, arrays) that do not use a fixed-stride
     * data buffer.
     *
     * @see QwpConstants#getFixedTypeSize(byte) for wire-format sizes
     */
    static int elementSizeInBuffer(byte type) {
        switch (type) {
            case TYPE_BOOLEAN:
            case TYPE_BYTE:
                return 1;
            case TYPE_SHORT:
            case TYPE_CHAR:
                return 2;
            case TYPE_INT:
            case TYPE_IPv4:
            case TYPE_SYMBOL:
            case TYPE_FLOAT:
                return 4;
            case TYPE_GEOHASH:
            case TYPE_LONG:
            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
            case TYPE_DATE:
            case TYPE_DECIMAL64:
            case TYPE_DOUBLE:
                return 8;
            case TYPE_UUID:
            case TYPE_DECIMAL128:
                return 16;
            case TYPE_LONG256:
            case TYPE_DECIMAL256:
                return 32;
            default:
                return 0;
        }
    }

    /**
     * Helper class to capture array data from DoubleArray/LongArray.appendToBufPtr().
     */
    private static class ArrayCapture implements ArrayBufferAppender {
        final int[] shape = new int[32];
        double[] doubleData;
        int doubleDataOffset;
        long[] longData;
        int longDataOffset;
        byte nDims;
        private boolean forLong;
        private int shapeIndex;

        @Override
        public void putBlockOfBytes(long from, long len) {
            int count = (int) (len / 8);
            if (forLong) {
                if (longData == null || longData.length < count) {
                    longData = new long[Math.max(count, longData == null ? 256 : longData.length * 2)];
                }
                for (int i = 0; i < count; i++) {
                    longData[longDataOffset++] = Unsafe.getUnsafe().getLong(from + i * 8L);
                }
            } else {
                if (doubleData == null || doubleData.length < count) {
                    doubleData = new double[Math.max(count, doubleData == null ? 256 : doubleData.length * 2)];
                }
                for (int i = 0; i < count; i++) {
                    doubleData[doubleDataOffset++] = Unsafe.getUnsafe().getDouble(from + i * 8L);
                }
            }
        }

        @Override
        public void putByte(byte b) {
            if (shapeIndex == 0) {
                nDims = b;
            }
        }

        @Override
        public void putDouble(double value) {
            if (doubleData == null || doubleDataOffset >= doubleData.length) {
                throw new LineSenderException("array data overflow: more double values than declared in shape");
            }
            doubleData[doubleDataOffset++] = value;
        }

        @Override
        public void putInt(int value) {
            if (shapeIndex < nDims) {
                shape[shapeIndex++] = value;
                if (shapeIndex == nDims) {
                    long product = 1;
                    for (int i = 0; i < nDims; i++) {
                        product *= shape[i];
                    }
                    if (product > Integer.MAX_VALUE) {
                        throw new LineSenderException("array too large: total element count exceeds int range");
                    }
                    int totalElements = (int) product;
                    if (forLong) {
                        if (longData == null || longData.length < totalElements) {
                            longData = new long[Math.max(totalElements, longData == null ? 256 : longData.length * 2)];
                        }
                    } else {
                        if (doubleData == null || doubleData.length < totalElements) {
                            doubleData = new double[Math.max(totalElements, doubleData == null ? 256 : doubleData.length * 2)];
                        }
                    }
                }
            }
        }

        @Override
        public void putLong(long value) {
            if (longData == null || longDataOffset >= longData.length) {
                throw new LineSenderException("array data overflow: more long values than declared in shape");
            }
            longData[longDataOffset++] = value;
        }

        void reset(boolean forLong) {
            this.forLong = forLong;
            shapeIndex = 0;
            nDims = 0;
            doubleDataOffset = 0;
            longDataOffset = 0;
        }
    }

    /**
     * Column buffer for a single column.
     * <p>
     * Fixed-width data is stored off-heap in {@link OffHeapAppendMemory} for zero-GC
     * operation and efficient bulk copy to network buffers.
     */
    public static class ColumnBuffer implements QuietCloseable {
        private static final long DOUBLE_ARRAY_BASE_OFFSET = Unsafe.getUnsafe().arrayBaseOffset(double[].class);
        final int elemSize;
        final String name;
        final byte type;
        final boolean useNullBitmap;
        private final Decimal256 rescaleTemp = new Decimal256();
        private ArrayCapture arrayCapture;
        private int arrayDataOffset;
        // Array storage (double/long arrays - variable length per row)
        private byte[] arrayDims;
        private int arrayShapeOffset;
        private int[] arrayShapes;
        // Optional auxiliary buffer used by symbol encoders that need sideband IDs.
        private OffHeapAppendMemory auxBuffer;
        // Off-heap data buffer for fixed-width types
        private OffHeapAppendMemory dataBuffer;
        // Decimal storage
        private byte decimalScale = -1;
        private double[] doubleArrayData;
        // GeoHash precision (number of bits, 1-60)
        private int geohashPrecision = -1;
        private boolean hasNulls;
        private int index;
        private long[] longArrayData;
        private int maxGlobalSymbolId = -1;
        private int nullBufCapRows;
        // Off-heap null bitmap (bit-packed, 1 bit per row)
        private long nullBufPtr;
        private QwpWebSocketSender sender;
        private int size;         // Total row count (including nulls)
        // Symbol specific (dictionary stays on-heap)
        private boolean storeGlobalSymbolIdsOnly;
        private OffHeapAppendMemory stringData;
        // Off-heap storage for string/varchar column data
        private OffHeapAppendMemory stringOffsets;
        private CharSequenceIntHashMap symbolDict;
        private ObjList<String> symbolList;
        private StringSink symbolLookupSink;
        private int valueCount;   // Actual stored values (excludes nulls)

        public ColumnBuffer(String name, byte type, boolean useNullBitmap) {
            this.name = name;
            this.type = type;
            this.useNullBitmap = useNullBitmap;
            this.elemSize = elementSizeInBuffer(type);
            this.size = 0;
            this.valueCount = 0;
            this.hasNulls = false;

            try {
                allocateStorage(type);
                if (useNullBitmap) {
                    nullBufCapRows = 64; // multiple of 64
                    long sizeBytes = (long) nullBufCapRows >>> 3;
                    nullBufPtr = Unsafe.calloc(sizeBytes, MemoryTag.NATIVE_ILP_RSS);
                }
            } catch (Throwable t) {
                close();
                throw t;
            }
        }

        /**
         * Adds a BINARY value. The bytes are appended verbatim to the column's data
         * buffer and an offset entry is pushed. Shares the VARCHAR wire layout
         * (offsets + concatenated bytes) but with no UTF-8 contract on the bytes.
         * A null reference is rejected; callers should use the null bitmap instead.
         */
        public void addBinary(byte[] value) {
            if (value == null) {
                if (useNullBitmap) {
                    ensureNullBitmapCapacity(size + 1);
                    markNull(size);
                    size++;
                    return;
                }
                throw new LineSenderException(
                        "BINARY value cannot be null; mark the row null via the null bitmap instead");
            }
            if (value.length > 0) {
                stringData.putBytes(value, 0, value.length);
            }
            stringOffsets.putInt(checkedStringOffset(stringData.getAppendOffset()));
            valueCount++;
            size++;
        }

        /**
         * Adds a BINARY value from native memory at {@code [ptr, ptr + len)}.
         * Bytes are copied into the column's data buffer; the caller's memory
         * need only stay valid for the duration of this call.
         */
        public void addBinary(long ptr, long len) {
            if (len < 0) {
                throw new LineSenderException(
                        "BINARY length must be non-negative; mark the row null via the null bitmap instead");
            }
            if (len > 0 && ptr == 0) {
                throw new LineSenderException("BINARY pointer cannot be 0 for a non-empty value");
            }
            if (len > 0) {
                stringData.putBlockOfBytes(ptr, len);
            }
            stringOffsets.putInt(checkedStringOffset(stringData.getAppendOffset()));
            valueCount++;
            size++;
        }

        public void addBoolean(boolean value) {
            dataBuffer.putByte(value ? (byte) 1 : (byte) 0);
            valueCount++;
            size++;
        }

        public void addByte(byte value) {
            dataBuffer.putByte(value);
            valueCount++;
            size++;
        }

        public void addDecimal128(Decimal128 value) {
            if (value == null || value.isNull()) {
                addNull();
                return;
            }
            if (decimalScale == -1) {
                decimalScale = (byte) value.getScale();
            } else if (decimalScale != value.getScale()) {
                rescaleTemp.ofRaw(value.getHigh(), value.getLow());
                rescaleTemp.setScale(value.getScale());
                try {
                    rescaleTemp.rescale(decimalScale);
                } catch (NumericException e) {
                    throw new LineSenderException("column '" + name + "' cannot rescale decimal from scale "
                            + value.getScale() + " to " + decimalScale + " without precision loss", e);
                }
                if (!rescaleTemp.fitsInStorageSizePow2(4)) {
                    throw new LineSenderException("Decimal128 overflow: rescaling from scale "
                            + value.getScale() + " to " + decimalScale + " exceeds 128-bit capacity");
                }
                dataBuffer.putLong(rescaleTemp.getLh());
                dataBuffer.putLong(rescaleTemp.getLl());
                valueCount++;
                size++;
                return;
            }
            dataBuffer.putLong(value.getHigh());
            dataBuffer.putLong(value.getLow());
            valueCount++;
            size++;
        }

        public void addDecimal256(Decimal256 value) {
            if (value == null || value.isNull()) {
                addNull();
                return;
            }
            Decimal256 src = value;
            if (decimalScale == -1) {
                decimalScale = (byte) value.getScale();
            } else if (decimalScale != value.getScale()) {
                rescaleTemp.copyFrom(value);
                try {
                    rescaleTemp.rescale(decimalScale);
                } catch (NumericException e) {
                    throw new LineSenderException("column '" + name + "' cannot rescale decimal from scale "
                            + value.getScale() + " to " + decimalScale + " without precision loss", e);
                }
                src = rescaleTemp;
            }
            dataBuffer.putLong(src.getHh());
            dataBuffer.putLong(src.getHl());
            dataBuffer.putLong(src.getLh());
            dataBuffer.putLong(src.getLl());
            valueCount++;
            size++;
        }

        public void addDecimal64(Decimal64 value) {
            if (value == null || value.isNull()) {
                addNull();
                return;
            }
            if (decimalScale == -1) {
                decimalScale = (byte) value.getScale();
                dataBuffer.putLong(value.getValue());
            } else if (decimalScale != value.getScale()) {
                rescaleTemp.ofRaw(value.getValue());
                rescaleTemp.setScale(value.getScale());
                try {
                    rescaleTemp.rescale(decimalScale);
                } catch (NumericException e) {
                    throw new LineSenderException("column '" + name + "' cannot rescale decimal from scale "
                            + value.getScale() + " to " + decimalScale + " without precision loss", e);
                }
                if (!rescaleTemp.fitsInStorageSizePow2(3)) {
                    throw new LineSenderException("Decimal64 overflow: rescaling from scale "
                            + value.getScale() + " to " + decimalScale + " exceeds 64-bit capacity");
                }
                dataBuffer.putLong(rescaleTemp.getLl());
            } else {
                dataBuffer.putLong(value.getValue());
            }
            valueCount++;
            size++;
        }

        public void addDouble(double value) {
            dataBuffer.putDouble(value);
            valueCount++;
            size++;
        }

        public void addDoubleArray(double[] values) {
            if (values == null) {
                addNull();
                return;
            }
            ensureArrayCapacity(1, values.length);
            arrayDims[valueCount] = 1;
            arrayShapes[arrayShapeOffset++] = values.length;
            System.arraycopy(values, 0, doubleArrayData, arrayDataOffset, values.length);
            arrayDataOffset += values.length;
            valueCount++;
            size++;
        }

        public void addDoubleArray(double[][] values) {
            if (values == null) {
                addNull();
                return;
            }
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            for (int i = 1; i < dim0; i++) {
                if (values[i].length != dim1) {
                    throw new LineSenderException("irregular array shape");
                }
            }
            int elemCount = checkedElementCount((long) dim0 * dim1);
            ensureArrayCapacity(2, elemCount);
            arrayDims[valueCount] = 2;
            arrayShapes[arrayShapeOffset++] = dim0;
            arrayShapes[arrayShapeOffset++] = dim1;
            for (int i = 0, n = values.length; i < n; i++) {
                double[] row = values[i];
                System.arraycopy(row, 0, doubleArrayData, arrayDataOffset, row.length);
                arrayDataOffset += row.length;
            }
            valueCount++;
            size++;
        }

        public void addDoubleArray(double[][][] values) {
            if (values == null) {
                addNull();
                return;
            }
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            int dim2 = dim0 > 0 && dim1 > 0 ? values[0][0].length : 0;
            for (int i = 0; i < dim0; i++) {
                if (values[i].length != dim1) {
                    throw new LineSenderException("irregular array shape");
                }
                for (int j = 0; j < dim1; j++) {
                    if (values[i][j].length != dim2) {
                        throw new LineSenderException("irregular array shape");
                    }
                }
            }
            int elemCount = checkedElementCount((long) dim0 * dim1 * dim2);
            ensureArrayCapacity(3, elemCount);
            arrayDims[valueCount] = 3;
            arrayShapes[arrayShapeOffset++] = dim0;
            arrayShapes[arrayShapeOffset++] = dim1;
            arrayShapes[arrayShapeOffset++] = dim2;
            for (int i = 0, ni = values.length; i < ni; i++) {
                double[][] plane = values[i];
                for (int j = 0, nj = plane.length; j < nj; j++) {
                    double[] row = plane[j];
                    System.arraycopy(row, 0, doubleArrayData, arrayDataOffset, row.length);
                    arrayDataOffset += row.length;
                }
            }
            valueCount++;
            size++;
        }

        public void addDoubleArray(DoubleArray array) {
            if (array == null) {
                addNull();
                return;
            }
            arrayCapture.reset(false);
            array.appendToBufPtr(arrayCapture);

            ensureArrayCapacity(arrayCapture.nDims, arrayCapture.doubleDataOffset);
            arrayDims[valueCount] = arrayCapture.nDims;
            for (int i = 0; i < arrayCapture.nDims; i++) {
                arrayShapes[arrayShapeOffset++] = arrayCapture.shape[i];
            }
            for (int i = 0; i < arrayCapture.doubleDataOffset; i++) {
                doubleArrayData[arrayDataOffset++] = arrayCapture.doubleData[i];
            }
            valueCount++;
            size++;
        }

        public void addDoubleArrayPayload(long ptr, long len) {
            appendArrayPayload(ptr, len);
        }

        public void addFloat(float value) {
            dataBuffer.putFloat(value);
            valueCount++;
            size++;
        }

        /**
         * Adds a geohash value with the given precision.
         *
         * @param value     the geohash value (bit-packed)
         * @param precision number of bits (1-60)
         */
        public void addGeoHash(long value, int precision) {
            if (precision < 1 || precision > 60) {
                throw new LineSenderException("invalid GeoHash precision: " + precision + " (must be 1-60)");
            }
            if (geohashPrecision == -1) {
                geohashPrecision = precision;
            } else if (geohashPrecision != precision) {
                throw new LineSenderException(
                        "GeoHash precision mismatch: column has " + geohashPrecision + " bits, got " + precision
                );
            }
            dataBuffer.putLong(value);
            valueCount++;
            size++;
        }

        public void addInt(int value) {
            dataBuffer.putInt(value);
            valueCount++;
            size++;
        }

        /**
         * Adds a packed IPv4 address (4 bytes, little-endian on the wire). The bit
         * pattern 0 (i.e. 0.0.0.0) is reserved by QuestDB as the IPv4 NULL sentinel
         * and surfaces as NULL on read regardless of the null bitmap.
         */
        public void addIPv4(int address) {
            dataBuffer.putInt(address);
            valueCount++;
            size++;
        }

        public void addLong(long value) {
            dataBuffer.putLong(value);
            valueCount++;
            size++;
        }

        public void addLong256(long l0, long l1, long l2, long l3) {
            dataBuffer.putLong(l0);
            dataBuffer.putLong(l1);
            dataBuffer.putLong(l2);
            dataBuffer.putLong(l3);
            valueCount++;
            size++;
        }

        public void addLongArray(long[] values) {
            if (values == null) {
                addNull();
                return;
            }
            ensureArrayCapacity(1, values.length);
            arrayDims[valueCount] = 1;
            arrayShapes[arrayShapeOffset++] = values.length;
            System.arraycopy(values, 0, longArrayData, arrayDataOffset, values.length);
            arrayDataOffset += values.length;
            valueCount++;
            size++;
        }

        public void addLongArray(long[][] values) {
            if (values == null) {
                addNull();
                return;
            }
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            for (int i = 1; i < dim0; i++) {
                if (values[i].length != dim1) {
                    throw new LineSenderException("irregular array shape");
                }
            }
            int elemCount = checkedElementCount((long) dim0 * dim1);
            ensureArrayCapacity(2, elemCount);
            arrayDims[valueCount] = 2;
            arrayShapes[arrayShapeOffset++] = dim0;
            arrayShapes[arrayShapeOffset++] = dim1;
            for (int i = 0; i < dim0; i++) {
                long[] row = values[i];
                for (int j = 0, rowLength = row.length; j < rowLength; j++) {
                    longArrayData[arrayDataOffset++] = row[j];
                }
            }
            valueCount++;
            size++;
        }

        public void addLongArray(long[][][] values) {
            if (values == null) {
                addNull();
                return;
            }
            int dim0 = values.length;
            int dim1 = dim0 > 0 ? values[0].length : 0;
            int dim2 = dim0 > 0 && dim1 > 0 ? values[0][0].length : 0;
            for (int i = 0; i < dim0; i++) {
                if (values[i].length != dim1) {
                    throw new LineSenderException("irregular array shape");
                }
                for (int j = 0; j < dim1; j++) {
                    if (values[i][j].length != dim2) {
                        throw new LineSenderException("irregular array shape");
                    }
                }
            }
            int elemCount = checkedElementCount((long) dim0 * dim1 * dim2);
            ensureArrayCapacity(3, elemCount);
            arrayDims[valueCount] = 3;
            arrayShapes[arrayShapeOffset++] = dim0;
            arrayShapes[arrayShapeOffset++] = dim1;
            arrayShapes[arrayShapeOffset++] = dim2;
            for (int i = 0, ni = values.length; i < ni; i++) {
                long[][] plane = values[i];
                for (int j = 0, nj = plane.length; j < nj; j++) {
                    long[] row = plane[j];
                    System.arraycopy(row, 0, longArrayData, arrayDataOffset, row.length);
                    arrayDataOffset += row.length;
                }
            }
            valueCount++;
            size++;
        }

        public void addLongArray(LongArray array) {
            if (array == null) {
                addNull();
                return;
            }
            arrayCapture.reset(true);
            array.appendToBufPtr(arrayCapture);

            ensureArrayCapacity(arrayCapture.nDims, arrayCapture.longDataOffset);
            arrayDims[valueCount] = arrayCapture.nDims;
            for (int i = 0; i < arrayCapture.nDims; i++) {
                arrayShapes[arrayShapeOffset++] = arrayCapture.shape[i];
            }
            for (int i = 0; i < arrayCapture.longDataOffset; i++) {
                longArrayData[arrayDataOffset++] = arrayCapture.longData[i];
            }
            valueCount++;
            size++;
        }

        public void addNull() {
            if (useNullBitmap) {
                ensureNullBitmapCapacity(size + 1);
                markNull(size);
            } else {
                // For non-nullable columns, store a sentinel/default value
                switch (type) {
                    case TYPE_BOOLEAN:
                    case TYPE_BYTE:
                        dataBuffer.putByte((byte) 0);
                        break;
                    case TYPE_SHORT:
                    case TYPE_CHAR:
                        dataBuffer.putShort((short) 0);
                        break;
                    case TYPE_INT:
                        dataBuffer.putInt(0);
                        break;
                    case TYPE_IPv4:
                        // QuestDB convention: 0.0.0.0 (bit pattern 0) is the NULL sentinel.
                        dataBuffer.putInt(0);
                        break;
                    case TYPE_GEOHASH:
                        dataBuffer.putLong(-1L);
                        break;
                    case TYPE_LONG:
                    case TYPE_TIMESTAMP:
                    case TYPE_TIMESTAMP_NANOS:
                    case TYPE_DATE:
                        dataBuffer.putLong(Long.MIN_VALUE);
                        break;
                    case TYPE_FLOAT:
                        dataBuffer.putFloat(Float.NaN);
                        break;
                    case TYPE_DOUBLE:
                        dataBuffer.putDouble(Double.NaN);
                        break;
                    case TYPE_VARCHAR:
                    case TYPE_BINARY:
                        stringOffsets.putInt(checkedStringOffset(stringData.getAppendOffset()));
                        break;
                    case TYPE_SYMBOL:
                        dataBuffer.putInt(-1);
                        break;
                    case TYPE_UUID:
                        dataBuffer.putLong(Long.MIN_VALUE);
                        dataBuffer.putLong(Long.MIN_VALUE);
                        break;
                    case TYPE_LONG256:
                        dataBuffer.putLong(Long.MIN_VALUE);
                        dataBuffer.putLong(Long.MIN_VALUE);
                        dataBuffer.putLong(Long.MIN_VALUE);
                        dataBuffer.putLong(Long.MIN_VALUE);
                        break;
                    case TYPE_DECIMAL64:
                        dataBuffer.putLong(Decimals.DECIMAL64_NULL);
                        break;
                    case TYPE_DECIMAL128:
                        dataBuffer.putLong(Decimals.DECIMAL128_HI_NULL);
                        dataBuffer.putLong(Decimals.DECIMAL128_LO_NULL);
                        break;
                    case TYPE_DECIMAL256:
                        dataBuffer.putLong(Decimals.DECIMAL256_HH_NULL);
                        dataBuffer.putLong(Decimals.DECIMAL256_HL_NULL);
                        dataBuffer.putLong(Decimals.DECIMAL256_LH_NULL);
                        dataBuffer.putLong(Decimals.DECIMAL256_LL_NULL);
                        break;
                    case TYPE_DOUBLE_ARRAY:
                    case TYPE_LONG_ARRAY:
                        ensureArrayCapacity(1, 0);
                        arrayDims[valueCount] = 1;
                        arrayShapes[arrayShapeOffset++] = 0;
                        break;
                }
                valueCount++;
            }
            size++;
        }

        public void addShort(short value) {
            dataBuffer.putShort(value);
            valueCount++;
            size++;
        }

        public void addString(CharSequence value) {
            if (value == null && useNullBitmap) {
                ensureNullBitmapCapacity(size + 1);
                markNull(size);
            } else {
                if (value != null) {
                    stringData.putUtf8(value);
                }
                stringOffsets.putInt(checkedStringOffset(stringData.getAppendOffset()));
                valueCount++;
            }
            size++;
        }

        public void addSymbol(CharSequence value) {
            if (value == null) {
                addNull();
                return;
            }
            if (sender != null) {
                int globalId = sender.getOrAddGlobalSymbol(value);
                addSymbolWithGlobalId(value, globalId);
                return;
            }
            if (storeGlobalSymbolIdsOnly) {
                throw new LineSenderException("column '" + name + "' cannot mix global symbol IDs with local symbol dictionary values");
            }
            int idx = getOrAddLocalSymbol(value);
            dataBuffer.putInt(idx);
            valueCount++;
            size++;
        }

        public void addSymbolUtf8(long ptr, int len) {
            if (len < 0) {
                addNull();
                return;
            }
            StringSink lookupSink = symbolLookupSink;
            if (lookupSink == null) {
                symbolLookupSink = lookupSink = new StringSink(Math.max(16, len));
            } else {
                lookupSink.clear();
            }
            if (!Utf8s.utf8ToUtf16(ptr, ptr + len, lookupSink)) {
                // Reuse the existing error path with the same diagnostic payload.
                Utf8s.stringFromUtf8Bytes(ptr, ptr + len);
                throw new AssertionError("unreachable");
            }
            if (sender != null) {
                int globalId = sender.getOrAddGlobalSymbol(lookupSink);
                addSymbolWithGlobalId(lookupSink, globalId);
                return;
            }
            if (storeGlobalSymbolIdsOnly) {
                throw new LineSenderException("column '" + name + "' cannot mix global symbol IDs with local symbol dictionary values");
            }
            int idx = getOrAddLocalSymbol(lookupSink);
            dataBuffer.putInt(idx);
            valueCount++;
            size++;
        }

        public void addSymbolWithGlobalId(CharSequence value, int globalId) {
            if (value == null) {
                addNull();
                return;
            }
            if (!storeGlobalSymbolIdsOnly) {
                if (symbolList != null && symbolList.size() > 0) {
                    int localIdx = getOrAddLocalSymbol(value);
                    dataBuffer.putInt(localIdx);
                    if (auxBuffer == null) {
                        auxBuffer = new OffHeapAppendMemory(64);
                    }
                    auxBuffer.putInt(globalId);
                    if (globalId > maxGlobalSymbolId) {
                        maxGlobalSymbolId = globalId;
                    }
                    valueCount++;
                    size++;
                    return;
                }
                storeGlobalSymbolIdsOnly = true;
            }
            dataBuffer.putInt(globalId);

            if (globalId > maxGlobalSymbolId) {
                maxGlobalSymbolId = globalId;
            }

            valueCount++;
            size++;
        }

        public void addUuid(long high, long low) {
            // Store in wire order: lo first, hi second
            dataBuffer.putLong(low);
            dataBuffer.putLong(high);
            valueCount++;
            size++;
        }

        @Override
        public void close() {
            if (dataBuffer != null) {
                dataBuffer.close();
                dataBuffer = null;
            }
            if (auxBuffer != null) {
                auxBuffer.close();
                auxBuffer = null;
            }
            if (stringOffsets != null) {
                stringOffsets.close();
                stringOffsets = null;
            }
            if (stringData != null) {
                stringData.close();
                stringData = null;
            }
            if (nullBufPtr != 0) {
                Unsafe.free(nullBufPtr, (long) nullBufCapRows >>> 3, MemoryTag.NATIVE_ILP_RSS);
                nullBufPtr = 0;
                nullBufCapRows = 0;
            }
        }

        public void ensureNullBitmapCapacity(int minRows) {
            if (nullBufPtr == 0 || nullBufCapRows >= minRows) {
                return;
            }
            int newCapRows = Math.max(nullBufCapRows * 2, ((minRows + 63) >>> 6) << 6);
            long newSizeBytes = (long) newCapRows >>> 3;
            long oldSizeBytes = (long) nullBufCapRows >>> 3;
            nullBufPtr = Unsafe.realloc(nullBufPtr, oldSizeBytes, newSizeBytes, MemoryTag.NATIVE_ILP_RSS);
            Vect.memset(nullBufPtr + oldSizeBytes, newSizeBytes - oldSizeBytes, 0);
            nullBufCapRows = newCapRows;
        }

        public int getArrayDataOffset() {
            return arrayDataOffset;
        }

        public byte[] getArrayDims() {
            return arrayDims;
        }

        public int getArrayShapeOffset() {
            return arrayShapeOffset;
        }

        public int[] getArrayShapes() {
            return arrayShapes;
        }

        /**
         * Returns the off-heap address of the auxiliary data buffer (global symbol IDs).
         * Returns 0 if no auxiliary data exists.
         */
        public long getAuxDataAddress() {
            return auxBuffer != null ? auxBuffer.pageAddress() : 0;
        }

        /**
         * Returns the total bytes buffered in this column's storage.
         */
        public long getBufferedBytes() {
            long bytes = 0;
            if (dataBuffer != null) {
                bytes += dataBuffer.getAppendOffset();
            }
            if (auxBuffer != null) {
                bytes += auxBuffer.getAppendOffset();
            }
            if (stringData != null) {
                bytes += stringData.getAppendOffset();
            }
            if (stringOffsets != null) {
                bytes += stringOffsets.getAppendOffset();
            }
            if (doubleArrayData != null) {
                bytes += (long) arrayDataOffset * Double.BYTES;
            }
            if (longArrayData != null) {
                bytes += (long) arrayDataOffset * Long.BYTES;
            }
            return bytes;
        }

        /**
         * Returns the off-heap address of the column data buffer.
         */
        public long getDataAddress() {
            return dataBuffer != null ? dataBuffer.pageAddress() : 0;
        }

        public byte getDecimalScale() {
            return decimalScale;
        }

        public double[] getDoubleArrayData() {
            return doubleArrayData;
        }

        public int getGeoHashPrecision() {
            return geohashPrecision;
        }

        public int getIndex() {
            return index;
        }

        public long[] getLongArrayData() {
            return longArrayData;
        }

        public int getMaxGlobalSymbolId() {
            return maxGlobalSymbolId;
        }

        /**
         * Returns the off-heap address of the null bitmap.
         * Returns 0 for non-nullable columns.
         */
        public long getNullBitmapAddress() {
            return nullBufPtr;
        }

        public int getSize() {
            return size;
        }

        public long getStringDataAddress() {
            return stringData != null ? stringData.pageAddress() : 0;
        }

        public long getStringDataSize() {
            return stringData != null ? stringData.getAppendOffset() : 0;
        }

        public long getStringOffsetsAddress() {
            return stringOffsets != null ? stringOffsets.pageAddress() : 0;
        }

        public String[] getSymbolDictionary() {
            if (symbolList == null) {
                return new String[0];
            }
            String[] dict = new String[symbolList.size()];
            for (int i = 0; i < symbolList.size(); i++) {
                dict[i] = symbolList.get(i);
            }
            return dict;
        }

        public int getSymbolDictionarySize() {
            if (storeGlobalSymbolIdsOnly) {
                return 0;
            }
            return symbolList != null ? symbolList.size() : 0;
        }

        public CharSequence getSymbolValue(int index) {
            return symbolList != null ? symbolList.getQuick(index) : null;
        }

        public byte getType() {
            return type;
        }

        public int getValueCount() {
            return valueCount;
        }

        public boolean hasSymbol(CharSequence value) {
            return symbolDict != null && symbolDict.get(value) != CharSequenceIntHashMap.NO_ENTRY_VALUE;
        }

        public boolean isNull(int index) {
            if (nullBufPtr == 0 || index >= nullBufCapRows) {
                return false;
            }
            long longAddr = nullBufPtr + ((long) (index >>> 6)) * 8;
            int bitIndex = index & 63;
            return (Unsafe.getUnsafe().getLong(longAddr) & (1L << bitIndex)) != 0;
        }

        public void reset() {
            size = 0;
            valueCount = 0;
            hasNulls = false;
            if (dataBuffer != null) {
                dataBuffer.truncate();
            }
            if (auxBuffer != null) {
                auxBuffer.truncate();
            }
            if (stringOffsets != null) {
                stringOffsets.truncate();
                stringOffsets.putInt(0); // re-seed initial 0 offset
            }
            if (stringData != null) {
                stringData.truncate();
            }
            if (nullBufPtr != 0) {
                Vect.memset(nullBufPtr, (long) nullBufCapRows >>> 3, 0);
            }
            if (symbolDict != null) {
                symbolDict.clear();
                symbolList.clear();
            }
            storeGlobalSymbolIdsOnly = false;
            maxGlobalSymbolId = -1;
            arrayShapeOffset = 0;
            arrayDataOffset = 0;
            decimalScale = -1;
            // geohashPrecision is intentionally NOT cleared here. It is a
            // schema property, locked on first write and matched by the
            // server's auto-created GEOHASH(Nb) type, so preserving it across
            // batches lets a later all-null batch encode the column with the
            // correct varint instead of falling back to precision=1.
        }

        public void retainTailRow(
                int sizeBefore,
                int valueCountBefore,
                int arrayShapeOffsetBefore,
                int arrayDataOffsetBefore
        ) {
            assert size == sizeBefore + 1;

            compactNullBitmap(sizeBefore);

            if (valueCount == valueCountBefore) {
                clearValuePayload();
                size = 1;
                valueCount = 0;
                return;
            }

            switch (type) {
                case TYPE_VARCHAR:
                case TYPE_BINARY:
                    retainStringValue(valueCountBefore);
                    break;
                case TYPE_SYMBOL:
                    retainSymbolValue(valueCountBefore);
                    break;
                case TYPE_DOUBLE_ARRAY:
                case TYPE_LONG_ARRAY:
                    retainArrayValue(valueCountBefore, arrayShapeOffsetBefore, arrayDataOffsetBefore);
                    break;
                default:
                    retainFixedWidthValue(valueCountBefore);
                    break;
            }

            size = 1;
            valueCount = 1;
        }

        public void truncateTo(int newSize) {
            if (newSize >= size) {
                return;
            }

            int newValueCount = 0;
            if (useNullBitmap && nullBufPtr != 0) {
                for (int i = 0; i < newSize; i++) {
                    if (!isNull(i)) {
                        newValueCount++;
                    }
                }
                // Clear null bits for truncated rows
                for (int i = newSize; i < Math.min(size, nullBufCapRows); i++) {
                    long longAddr = nullBufPtr + ((long) (i >>> 6)) * 8;
                    int bitIndex = i & 63;
                    long current = Unsafe.getUnsafe().getLong(longAddr);
                    Unsafe.getUnsafe().putLong(longAddr, current & ~(1L << bitIndex));
                }
                hasNulls = false;
                for (int i = 0; i < newSize && !hasNulls; i++) {
                    if (isNull(i)) {
                        hasNulls = true;
                    }
                }
            } else {
                newValueCount = newSize;
            }

            size = newSize;
            valueCount = newValueCount;

            // Rewind off-heap data buffer
            if (dataBuffer != null && elemSize > 0) {
                dataBuffer.jumpTo((long) newValueCount * elemSize);
            }

            // Rewind string buffers
            if (stringOffsets != null) {
                int dataOffset = Unsafe.getUnsafe().getInt(stringOffsets.pageAddress() + (long) newValueCount * 4);
                stringData.jumpTo(dataOffset);
                stringOffsets.jumpTo((long) (newValueCount + 1) * 4);
            }

            // Rewind aux buffer (symbol global IDs)
            if (auxBuffer != null) {
                auxBuffer.jumpTo((long) newValueCount * 4);
            }

            // Rewind array offsets by walking the retained values
            if (arrayDims != null) {
                int newShapeOffset = 0;
                int newDataOffset = 0;
                for (int i = 0; i < newValueCount; i++) {
                    int nDims = arrayDims[i];
                    int elemCount = 1;
                    for (int d = 0; d < nDims; d++) {
                        elemCount *= arrayShapes[newShapeOffset++];
                    }
                    newDataOffset += elemCount;
                }
                arrayShapeOffset = newShapeOffset;
                arrayDataOffset = newDataOffset;
            }

            // When all values are removed, reset type-specific metadata so the
            // column behaves as freshly created. truncateTo is reached from
            // cancelCurrentRow, where newly-added in-progress columns must be
            // free to re-acquire a different precision/scale on the next row
            // (the server has not seen them yet). The between-batch path goes
            // through ColumnBuffer.reset() and intentionally preserves
            // geohashPrecision there to keep its server-locked value.
            if (newValueCount == 0) {
                decimalScale = -1;
                geohashPrecision = -1;
                maxGlobalSymbolId = -1;
                storeGlobalSymbolIdsOnly = false;
                if (symbolDict != null) {
                    symbolDict.clear();
                    symbolList.clear();
                }
            }
        }

        public boolean usesNullBitmap() {
            return useNullBitmap;
        }

        private static int checkedElementCount(long product) {
            if (product > Integer.MAX_VALUE) {
                throw new LineSenderException("array too large: total element count exceeds int range");
            }
            return (int) product;
        }

        private static int checkedStringOffset(long offset) {
            if (offset > Integer.MAX_VALUE) {
                throw new LineSenderException("string column data exceeds 2 GiB per batch, flush more frequently");
            }
            return (int) offset;
        }

        private void allocateStorage(byte type) {
            switch (type) {
                case TYPE_BOOLEAN:
                case TYPE_BYTE:
                    dataBuffer = new OffHeapAppendMemory(16);
                    break;
                case TYPE_SHORT:
                case TYPE_CHAR:
                    dataBuffer = new OffHeapAppendMemory(32);
                    break;
                case TYPE_INT:
                case TYPE_IPv4:
                case TYPE_FLOAT:
                    dataBuffer = new OffHeapAppendMemory(64);
                    break;
                case TYPE_GEOHASH:
                case TYPE_LONG:
                case TYPE_TIMESTAMP:
                case TYPE_TIMESTAMP_NANOS:
                case TYPE_DATE:
                case TYPE_DECIMAL64:
                case TYPE_DOUBLE:
                    dataBuffer = new OffHeapAppendMemory(128);
                    break;
                case TYPE_UUID:
                case TYPE_DECIMAL128:
                    dataBuffer = new OffHeapAppendMemory(256);
                    break;
                case TYPE_LONG256:
                case TYPE_DECIMAL256:
                    dataBuffer = new OffHeapAppendMemory(512);
                    break;
                case TYPE_VARCHAR:
                case TYPE_BINARY:
                    stringOffsets = new OffHeapAppendMemory(64);
                    try {
                        stringOffsets.putInt(0); // seed initial 0 offset
                        stringData = new OffHeapAppendMemory(256);
                    } catch (Throwable t) {
                        stringOffsets.close();
                        stringOffsets = null;
                        throw t;
                    }
                    break;
                case TYPE_SYMBOL:
                    dataBuffer = new OffHeapAppendMemory(64);
                    symbolDict = new CharSequenceIntHashMap();
                    symbolList = new ObjList<>();
                    break;
                case TYPE_DOUBLE_ARRAY:
                case TYPE_LONG_ARRAY:
                    arrayDims = new byte[16];
                    arrayCapture = new ArrayCapture();
                    break;
            }
        }

        private void appendArrayPayload(long ptr, long len) {
            if (len < 0) {
                addNull();
                return;
            }
            if (len == 0) {
                throw new LineSenderException("invalid array payload: empty payload");
            }

            int nDims = Unsafe.getUnsafe().getByte(ptr) & 0xFF;
            if (nDims < 1 || nDims > ColumnType.ARRAY_NDIMS_LIMIT) {
                throw new LineSenderException("invalid array payload: bad dimensionality " + nDims);
            }

            long cursor = ptr + 1;
            long headerBytes = 1L + (long) nDims * Integer.BYTES;
            if (len < headerBytes) {
                throw new LineSenderException("invalid array payload: truncated shape header");
            }

            int elemCount = 1;
            for (int d = 0; d < nDims; d++) {
                int dimLen = Unsafe.getUnsafe().getInt(cursor);
                if (dimLen < 0) {
                    throw new LineSenderException("invalid array payload: negative dimension length");
                }
                elemCount = checkedElementCount((long) elemCount * dimLen);
                cursor += Integer.BYTES;
            }

            long dataBytes = (long) elemCount * Double.BYTES;
            if (len != headerBytes + dataBytes) {
                throw new LineSenderException("invalid array payload: length mismatch");
            }

            ensureArrayCapacity(nDims, elemCount);
            arrayDims[valueCount] = (byte) nDims;

            cursor = ptr + 1;
            for (int d = 0; d < nDims; d++) {
                arrayShapes[arrayShapeOffset++] = Unsafe.getUnsafe().getInt(cursor);
                cursor += Integer.BYTES;
            }

            if (dataBytes > 0) {
                Unsafe.getUnsafe().copyMemory(
                        null,
                        cursor,
                        doubleArrayData,
                        DOUBLE_ARRAY_BASE_OFFSET + (long) arrayDataOffset * Double.BYTES,
                        dataBytes
                );
            }

            arrayDataOffset += elemCount;
            valueCount++;
            size++;
        }

        private void clearToEmptyFast() {
            int sizeBefore = size;
            clearValuePayload();
            if (nullBufPtr != 0 && sizeBefore > 0) {
                int rowsToClear = Math.min(sizeBefore, nullBufCapRows);
                long usedLongs = ((long) rowsToClear + 63) >>> 6;
                Vect.memset(nullBufPtr, usedLongs * Long.BYTES, 0);
            }
            size = 0;
            valueCount = 0;
            hasNulls = false;
        }

        private void clearValuePayload() {
            if (dataBuffer != null && elemSize > 0) {
                dataBuffer.jumpTo(0);
            }
            if (auxBuffer != null) {
                auxBuffer.truncate();
            }
            if (stringOffsets != null) {
                stringOffsets.truncate();
                stringOffsets.putInt(0);
            }
            if (stringData != null) {
                stringData.truncate();
            }
            arrayShapeOffset = 0;
            arrayDataOffset = 0;
            resetEmptyMetadata();
        }

        private void compactNullBitmap(int sourceRow) {
            if (nullBufPtr == 0) {
                return;
            }

            boolean retainedNull = isNull(sourceRow);
            int rowsToClear = Math.min(size, nullBufCapRows);
            long usedLongs = ((long) rowsToClear + 63) >>> 6;
            Vect.memset(nullBufPtr, usedLongs * Long.BYTES, 0);
            if (retainedNull) {
                Unsafe.getUnsafe().putLong(nullBufPtr, 1L);
            }
            hasNulls = retainedNull;
        }

        private void ensureArrayCapacity(int nDims, int dataElements) {
            // Ensure per-row array dims capacity
            if (valueCount >= arrayDims.length) {
                arrayDims = Arrays.copyOf(arrayDims, arrayDims.length * 2);
            }

            // Ensure shape array capacity
            int requiredShapeCapacity = arrayShapeOffset + nDims;
            if (arrayShapes == null) {
                arrayShapes = new int[Math.max(64, requiredShapeCapacity)];
            } else if (requiredShapeCapacity > arrayShapes.length) {
                arrayShapes = Arrays.copyOf(arrayShapes, Math.max(arrayShapes.length * 2, requiredShapeCapacity));
            }

            // Ensure data array capacity
            int requiredDataCapacity = arrayDataOffset + dataElements;
            if (type == TYPE_DOUBLE_ARRAY) {
                if (doubleArrayData == null) {
                    doubleArrayData = new double[Math.max(256, requiredDataCapacity)];
                } else if (requiredDataCapacity > doubleArrayData.length) {
                    doubleArrayData = Arrays.copyOf(doubleArrayData, Math.max(doubleArrayData.length * 2, requiredDataCapacity));
                }
            } else if (type == TYPE_LONG_ARRAY) {
                if (longArrayData == null) {
                    longArrayData = new long[Math.max(256, requiredDataCapacity)];
                } else if (requiredDataCapacity > longArrayData.length) {
                    longArrayData = Arrays.copyOf(longArrayData, Math.max(longArrayData.length * 2, requiredDataCapacity));
                }
            }
        }

        private int getOrAddLocalSymbol(CharSequence value) {
            int idx = symbolDict.get(value);
            if (idx == CharSequenceIntHashMap.NO_ENTRY_VALUE) {
                String symbol = Chars.toString(value);
                idx = symbolList.size();
                symbolDict.put(symbol, idx);
                symbolList.add(symbol);
            }
            return idx;
        }

        private void markNull(int index) {
            long longAddr = nullBufPtr + ((long) (index >>> 6)) * 8;
            int bitIndex = index & 63;
            long current = Unsafe.getUnsafe().getLong(longAddr);
            Unsafe.getUnsafe().putLong(longAddr, current | (1L << bitIndex));
            hasNulls = true;
        }

        private void resetEmptyMetadata() {
            decimalScale = -1;
            geohashPrecision = -1;
            maxGlobalSymbolId = -1;
            storeGlobalSymbolIdsOnly = false;
            if (symbolDict != null) {
                symbolDict.clear();
                symbolList.clear();
            }
        }

        private void retainArrayValue(int valueIndex, int shapeOffsetBefore, int dataOffsetBefore) {
            int nDims = arrayDims[valueIndex] & 0xFF;
            arrayDims[0] = (byte) nDims;

            int shapeCount = arrayShapeOffset - shapeOffsetBefore;
            if (shapeCount > 0 && shapeOffsetBefore > 0) {
                System.arraycopy(arrayShapes, shapeOffsetBefore, arrayShapes, 0, shapeCount);
            }
            arrayShapeOffset = shapeCount;

            int dataCount = arrayDataOffset - dataOffsetBefore;
            if (dataCount > 0 && dataOffsetBefore > 0) {
                if (type == TYPE_LONG_ARRAY) {
                    System.arraycopy(longArrayData, dataOffsetBefore, longArrayData, 0, dataCount);
                } else {
                    System.arraycopy(doubleArrayData, dataOffsetBefore, doubleArrayData, 0, dataCount);
                }
            }
            arrayDataOffset = dataCount;
        }

        private void retainFixedWidthValue(int valueIndex) {
            if (dataBuffer == null || elemSize == 0) {
                return;
            }

            long srcOffset = (long) valueIndex * elemSize;
            long dataAddress = dataBuffer.pageAddress();
            if (srcOffset > 0) {
                Vect.memmove(dataAddress, dataAddress + srcOffset, elemSize);
            }
            dataBuffer.jumpTo(elemSize);

            if (auxBuffer != null) {
                long auxAddress = auxBuffer.pageAddress();
                long auxOffset = (long) valueIndex * Integer.BYTES;
                if (auxOffset > 0) {
                    Vect.memmove(auxAddress, auxAddress + auxOffset, Integer.BYTES);
                }
                auxBuffer.jumpTo(Integer.BYTES);
                maxGlobalSymbolId = Unsafe.getUnsafe().getInt(auxAddress);
            }
        }

        private void retainStringValue(int valueIndex) {
            long offsetsAddress = stringOffsets.pageAddress();
            int start = Unsafe.getUnsafe().getInt(offsetsAddress + (long) valueIndex * Integer.BYTES);
            int end = Unsafe.getUnsafe().getInt(offsetsAddress + (long) (valueIndex + 1) * Integer.BYTES);
            int len = end - start;

            if (len > 0 && start > 0) {
                Vect.memmove(stringData.pageAddress(), stringData.pageAddress() + start, len);
            }

            stringData.jumpTo(len);
            stringOffsets.truncate();
            stringOffsets.putInt(0);
            stringOffsets.putInt(len);
        }

        private void retainSymbolValue(int valueIndex) {
            retainFixedWidthValue(valueIndex);

            if (storeGlobalSymbolIdsOnly) {
                maxGlobalSymbolId = Unsafe.getUnsafe().getInt(dataBuffer.pageAddress());
                return;
            }

            int localIndex = Unsafe.getUnsafe().getInt(dataBuffer.pageAddress());
            String symbol = symbolList.get(localIndex);

            symbolDict.clear();
            symbolList.clear();
            symbolList.add(symbol);
            symbolDict.put(symbol, 0);
            Unsafe.getUnsafe().putInt(dataBuffer.pageAddress(), 0);
        }
    }
}
