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

package io.questdb.client.cutlass.qwp.protocol;

import io.questdb.client.cairo.ColumnType;
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

    private final CharSequenceIntHashMap columnNameToIndex;
    private final ObjList<ColumnBuffer> columns;
    private final QwpWebSocketSender sender;
    private final String tableName;
    private QwpColumnDef[] cachedColumnDefs;
    private int columnAccessCursor; // tracks expected next column index
    private boolean columnDefsCacheValid;
    private int committedColumnCount; // columns that existed at last nextRow()
    private ColumnBuffer[] fastColumns; // plain array for O(1) sequential access
    private int rowCount;
    private long schemaHash;
    private boolean schemaHashComputed;

    public QwpTableBuffer(String tableName) {
        this(tableName, null);
    }

    /**
     * Use this constructor overload to allow writing to a symbol column.
     * {@link ColumnBuffer#addSymbol(CharSequence)} needs the sender to
     * call {@link QwpWebSocketSender#getOrAddGlobalSymbol(String)}, registering
     * the symbol in the global dictionary shared with the server.
     */
    public QwpTableBuffer(String tableName, QwpWebSocketSender sender) {
        this.tableName = tableName;
        this.sender = sender;
        this.columns = new ObjList<>();
        this.columnNameToIndex = new CharSequenceIntHashMap();
        this.rowCount = 0;
        this.schemaHash = 0;
        this.schemaHashComputed = false;
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
        rowCount = 0;
        schemaHash = 0;
        schemaHashComputed = false;
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
                cachedColumnDefs[i] = new QwpColumnDef(col.name, col.type, col.useNullBitmap);
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
     */
    public ColumnBuffer getOrCreateColumn(CharSequence name, byte type, boolean nullable) {
        ColumnBuffer existing = lookupColumn(name, type);
        if (existing != null) {
            return existing;
        }

        return createColumn(name, type, nullable);
    }

    /**
     * Returns the number of rows buffered.
     */
    public int getRowCount() {
        return rowCount;
    }

    /**
     * Returns the schema hash for this table.
     * <p>
     * The hash is computed to match what QwpSchema.computeSchemaHash() produces:
     * - Uses wire type codes (with nullable bit)
     * - Hash is over name bytes + type code for each column
     */
    public long getSchemaHash() {
        if (!schemaHashComputed) {
            // Compute hash directly from column buffers without intermediate arrays
            schemaHash = QwpSchemaHash.computeSchemaHashDirect(columns);
            schemaHashComputed = true;
        }
        return schemaHash;
    }

    /**
     * Returns the table name.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Advances to the next row.
     * <p>
     * This should be called after all column values for the current row have been set.
     */
    public void nextRow() {
        // Reset sequential access cursor for the next row
        columnAccessCursor = 0;
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

    private ColumnBuffer createColumn(CharSequence name, byte type, boolean nullable) {
        ColumnBuffer col = new ColumnBuffer(Chars.toString(name), type, nullable);
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
        schemaHashComputed = false;
        columnDefsCacheValid = false;
        return col;
    }

    private ColumnBuffer lookupColumn(CharSequence name, byte type) {
        // Fast path: predict next column in sequence
        int n = columns.size();
        if (columnAccessCursor < n) {
            ColumnBuffer candidate = fastColumns[columnAccessCursor];
            if (Chars.equals(candidate.name, name)) {
                columnAccessCursor++;
                assertColumnType(name, type, candidate);
                return candidate;
            }
        }

        // Slow path: hash map lookup
        int idx = columnNameToIndex.get(name);
        if (idx != CharSequenceIntHashMap.NO_ENTRY_VALUE) {
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

        schemaHashComputed = false;
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
        return switch (type) {
            case TYPE_BOOLEAN, TYPE_BYTE -> 1;
            case TYPE_SHORT, TYPE_CHAR -> 2;
            case TYPE_INT, TYPE_SYMBOL, TYPE_FLOAT -> 4;
            case TYPE_GEOHASH, TYPE_LONG, TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS,
                 TYPE_DATE, TYPE_DECIMAL64, TYPE_DOUBLE -> 8;
            case TYPE_UUID, TYPE_DECIMAL128 -> 16;
            case TYPE_LONG256, TYPE_DECIMAL256 -> 32;
            default -> 0;
        };
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
                    longData = new long[count];
                }
                for (int i = 0; i < count; i++) {
                    longData[longDataOffset++] = Unsafe.getUnsafe().getLong(from + i * 8L);
                }
            } else {
                if (doubleData == null || doubleData.length < count) {
                    doubleData = new double[count];
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
            if (doubleData != null && doubleDataOffset < doubleData.length) {
                doubleData[doubleDataOffset++] = value;
            }
        }

        @Override
        public void putInt(int value) {
            if (shapeIndex < nDims) {
                shape[shapeIndex++] = value;
                if (shapeIndex == nDims) {
                    int totalElements = 1;
                    for (int i = 0; i < nDims; i++) {
                        totalElements *= shape[i];
                    }
                    if (forLong) {
                        if (longData == null || longData.length < totalElements) {
                            longData = new long[totalElements];
                        }
                    } else {
                        if (doubleData == null || doubleData.length < totalElements) {
                            doubleData = new double[totalElements];
                        }
                    }
                }
            }
        }

        @Override
        public void putLong(long value) {
            if (longData != null && longDataOffset < longData.length) {
                longData[longDataOffset++] = value;
            }
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
        // Off-heap auxiliary buffer for global symbol IDs (SYMBOL type only)
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
        private OffHeapAppendMemory stringData;
        // Off-heap storage for string/varchar column data
        private OffHeapAppendMemory stringOffsets;
        // Symbol specific (dictionary stays on-heap)
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

        public void addBoolean(boolean value) {
            ensureNullBitmapCapacity();
            dataBuffer.putByte(value ? (byte) 1 : (byte) 0);
            valueCount++;
            size++;
        }

        public void addByte(byte value) {
            ensureNullBitmapCapacity();
            dataBuffer.putByte(value);
            valueCount++;
            size++;
        }

        public void addDecimal128(Decimal128 value) {
            if (value == null || value.isNull()) {
                addNull();
                return;
            }
            ensureNullBitmapCapacity();
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
            ensureNullBitmapCapacity();
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
            ensureNullBitmapCapacity();
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
            ensureNullBitmapCapacity();
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
            for (double v : values) {
                doubleArrayData[arrayDataOffset++] = v;
            }
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
            for (double[] row : values) {
                for (double v : row) {
                    doubleArrayData[arrayDataOffset++] = v;
                }
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
            for (double[][] plane : values) {
                for (double[] row : plane) {
                    for (double v : row) {
                        doubleArrayData[arrayDataOffset++] = v;
                    }
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
            ensureNullBitmapCapacity();
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
            ensureNullBitmapCapacity();
            dataBuffer.putLong(value);
            valueCount++;
            size++;
        }

        public void addInt(int value) {
            ensureNullBitmapCapacity();
            dataBuffer.putInt(value);
            valueCount++;
            size++;
        }

        public void addLong(long value) {
            ensureNullBitmapCapacity();
            dataBuffer.putLong(value);
            valueCount++;
            size++;
        }

        public void addLong256(long l0, long l1, long l2, long l3) {
            ensureNullBitmapCapacity();
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
            for (long v : values) {
                longArrayData[arrayDataOffset++] = v;
            }
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
            for (long[] row : values) {
                for (long v : row) {
                    longArrayData[arrayDataOffset++] = v;
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
            for (long[][] plane : values) {
                for (long[] row : plane) {
                    for (long v : row) {
                        longArrayData[arrayDataOffset++] = v;
                    }
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
                ensureNullBitmapCapacity();
                markNull(size);
            } else {
                // For non-nullable columns, store a sentinel/default value
                switch (type) {
                    case TYPE_BOOLEAN, TYPE_BYTE -> dataBuffer.putByte((byte) 0);
                    case TYPE_SHORT, TYPE_CHAR -> dataBuffer.putShort((short) 0);
                    case TYPE_INT -> dataBuffer.putInt(0);
                    case TYPE_GEOHASH -> dataBuffer.putLong(-1L);
                    case TYPE_LONG, TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS, TYPE_DATE ->
                            dataBuffer.putLong(Long.MIN_VALUE);
                    case TYPE_FLOAT -> dataBuffer.putFloat(Float.NaN);
                    case TYPE_DOUBLE -> dataBuffer.putDouble(Double.NaN);
                    case TYPE_STRING, TYPE_VARCHAR -> stringOffsets.putInt((int) stringData.getAppendOffset());
                    case TYPE_SYMBOL -> dataBuffer.putInt(-1);
                    case TYPE_UUID -> {
                        dataBuffer.putLong(Long.MIN_VALUE);
                        dataBuffer.putLong(Long.MIN_VALUE);
                    }
                    case TYPE_LONG256 -> {
                        dataBuffer.putLong(Long.MIN_VALUE);
                        dataBuffer.putLong(Long.MIN_VALUE);
                        dataBuffer.putLong(Long.MIN_VALUE);
                        dataBuffer.putLong(Long.MIN_VALUE);
                    }
                    case TYPE_DECIMAL64 -> dataBuffer.putLong(Decimals.DECIMAL64_NULL);
                    case TYPE_DECIMAL128 -> {
                        dataBuffer.putLong(Decimals.DECIMAL128_HI_NULL);
                        dataBuffer.putLong(Decimals.DECIMAL128_LO_NULL);
                    }
                    case TYPE_DECIMAL256 -> {
                        dataBuffer.putLong(Decimals.DECIMAL256_HH_NULL);
                        dataBuffer.putLong(Decimals.DECIMAL256_HL_NULL);
                        dataBuffer.putLong(Decimals.DECIMAL256_LH_NULL);
                        dataBuffer.putLong(Decimals.DECIMAL256_LL_NULL);
                    }
                    case TYPE_DOUBLE_ARRAY, TYPE_LONG_ARRAY -> {
                        ensureArrayCapacity(1, 0);
                        arrayDims[valueCount] = 1;
                        arrayShapes[arrayShapeOffset++] = 0;
                    }
                }
                valueCount++;
            }
            size++;
        }

        public void addShort(short value) {
            ensureNullBitmapCapacity();
            dataBuffer.putShort(value);
            valueCount++;
            size++;
        }

        public void addString(CharSequence value) {
            if (value == null && useNullBitmap) {
                ensureNullBitmapCapacity();
                markNull(size);
            } else {
                ensureNullBitmapCapacity();
                if (value != null) {
                    stringData.putUtf8(value);
                }
                stringOffsets.putInt((int) stringData.getAppendOffset());
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
                String symbolValue = value.toString();
                int globalId = sender.getOrAddGlobalSymbol(symbolValue);
                addSymbolWithGlobalId(symbolValue, globalId);
                return;
            }
            ensureNullBitmapCapacity();
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
                String symbolValue = lookupSink.toString();
                int globalId = sender.getOrAddGlobalSymbol(symbolValue);
                addSymbolWithGlobalId(symbolValue, globalId);
                return;
            }
            ensureNullBitmapCapacity();
            int idx = getOrAddLocalSymbol(lookupSink);
            dataBuffer.putInt(idx);
            valueCount++;
            size++;
        }

        public void addSymbolWithGlobalId(String value, int globalId) {
            if (value == null) {
                addNull();
                return;
            }
            ensureNullBitmapCapacity();
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
        }

        public void addUuid(long high, long low) {
            ensureNullBitmapCapacity();
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

        public String getName() {
            return name;
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
            if (nullBufPtr == 0) {
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
            maxGlobalSymbolId = -1;
            arrayShapeOffset = 0;
            arrayDataOffset = 0;
            decimalScale = -1;
            geohashPrecision = -1;
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
                case TYPE_STRING, TYPE_VARCHAR -> retainStringValue(valueCountBefore);
                case TYPE_SYMBOL -> retainSymbolValue(valueCountBefore);
                case TYPE_DOUBLE_ARRAY, TYPE_LONG_ARRAY ->
                        retainArrayValue(valueCountBefore, arrayShapeOffsetBefore, arrayDataOffsetBefore);
                default -> retainFixedWidthValue(valueCountBefore);
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
                for (int i = newSize; i < size; i++) {
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
            // column behaves as freshly created (matches what reset() does).
            if (newValueCount == 0) {
                decimalScale = -1;
                geohashPrecision = -1;
                maxGlobalSymbolId = -1;
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
                case TYPE_STRING:
                case TYPE_VARCHAR:
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
                long usedLongs = ((long) sizeBefore + 63) >>> 6;
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
            long usedLongs = ((long) size + 63) >>> 6;
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

            // Ensure null bitmap capacity
            if (useNullBitmap) {
                ensureNullBitmapCapacity();
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

        private void ensureNullBitmapCapacity() {
            if (nullBufPtr == 0 || nullBufCapRows > size) {
                return;
            }
            int newCapRows = Math.max(nullBufCapRows * 2, ((size + 64) >>> 6) << 6);
            long newSizeBytes = (long) newCapRows >>> 3;
            long oldSizeBytes = (long) nullBufCapRows >>> 3;
            nullBufPtr = Unsafe.realloc(nullBufPtr, oldSizeBytes, newSizeBytes, MemoryTag.NATIVE_ILP_RSS);
            Vect.memset(nullBufPtr + oldSizeBytes, newSizeBytes - oldSizeBytes, 0);
            nullBufCapRows = newCapRows;
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
