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

package io.questdb.client.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.client.cutlass.qwp.protocol.QwpGorillaEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Encodes ILP v4 messages for WebSocket transport.
 * <p>
 * This encoder reads column data from off-heap {@link io.questdb.client.cutlass.qwp.protocol.OffHeapAppendMemory}
 * buffers in {@link QwpTableBuffer.ColumnBuffer} and uses bulk {@code putBlockOfBytes} for fixed-width
 * types where wire format matches native byte order.
 * <p>
 * Types that use bulk copy (native byte-order on wire):
 * BYTE, SHORT, INT, LONG, FLOAT, DOUBLE, DATE, UUID, LONG256
 * <p>
 * Types that require element-by-element encoding:
 * BOOLEAN (bit-packed on wire), TIMESTAMP (Gorilla), DECIMAL64/128/256 (big-endian on wire)
 */
public class QwpWebSocketEncoder implements QuietCloseable {

    public static final byte ENCODING_GORILLA = 0x01;
    public static final byte ENCODING_UNCOMPRESSED = 0x00;
    private final QwpGorillaEncoder gorillaEncoder = new QwpGorillaEncoder();
    private QwpBufferWriter buffer;
    private byte flags;
    private NativeBufferWriter ownedBuffer;

    public QwpWebSocketEncoder() {
        this.ownedBuffer = new NativeBufferWriter();
        this.buffer = ownedBuffer;
        this.flags = 0;
    }

    public QwpWebSocketEncoder(int bufferSize) {
        this.ownedBuffer = new NativeBufferWriter(bufferSize);
        this.buffer = ownedBuffer;
        this.flags = 0;
    }

    @Override
    public void close() {
        if (ownedBuffer != null) {
            ownedBuffer.close();
            ownedBuffer = null;
        }
    }

    public int encode(QwpTableBuffer tableBuffer, boolean useSchemaRef) {
        buffer.reset();
        writeHeader(1, 0);
        int payloadStart = buffer.getPosition();
        encodeTable(tableBuffer, useSchemaRef);
        int payloadLength = buffer.getPosition() - payloadStart;
        buffer.patchInt(8, payloadLength);
        return buffer.getPosition();
    }

    public int encodeWithDeltaDict(
            QwpTableBuffer tableBuffer,
            GlobalSymbolDictionary globalDict,
            int confirmedMaxId,
            int batchMaxId,
            boolean useSchemaRef
    ) {
        buffer.reset();
        int deltaStart = confirmedMaxId + 1;
        int deltaCount = Math.max(0, batchMaxId - confirmedMaxId);
        byte savedFlags = flags;
        flags |= FLAG_DELTA_SYMBOL_DICT;
        writeHeader(1, 0);
        int payloadStart = buffer.getPosition();
        buffer.putVarint(deltaStart);
        buffer.putVarint(deltaCount);
        for (int id = deltaStart; id < deltaStart + deltaCount; id++) {
            String symbol = globalDict.getSymbol(id);
            buffer.putString(symbol);
        }
        encodeTableWithGlobalSymbols(tableBuffer, useSchemaRef);
        int payloadLength = buffer.getPosition() - payloadStart;
        buffer.patchInt(8, payloadLength);
        flags = savedFlags;
        return buffer.getPosition();
    }

    public QwpBufferWriter getBuffer() {
        return buffer;
    }

    public boolean isDeltaSymbolDictEnabled() {
        return (flags & FLAG_DELTA_SYMBOL_DICT) != 0;
    }

    public boolean isGorillaEnabled() {
        return (flags & FLAG_GORILLA) != 0;
    }

    public boolean isUsingExternalBuffer() {
        return buffer != ownedBuffer;
    }

    public void reset() {
        if (!isUsingExternalBuffer()) {
            buffer.reset();
        }
    }

    public void setBuffer(QwpBufferWriter externalBuffer) {
        this.buffer = externalBuffer != null ? externalBuffer : ownedBuffer;
    }

    public void setDeltaSymbolDictEnabled(boolean enabled) {
        if (enabled) {
            flags |= FLAG_DELTA_SYMBOL_DICT;
        } else {
            flags &= ~FLAG_DELTA_SYMBOL_DICT;
        }
    }

    public void setGorillaEnabled(boolean enabled) {
        if (enabled) {
            flags |= FLAG_GORILLA;
        } else {
            flags &= ~FLAG_GORILLA;
        }
    }

    public void writeHeader(int tableCount, int payloadLength) {
        buffer.putByte((byte) 'I');
        buffer.putByte((byte) 'L');
        buffer.putByte((byte) 'P');
        buffer.putByte((byte) '4');
        buffer.putByte(VERSION_1);
        buffer.putByte(flags);
        buffer.putShort((short) tableCount);
        buffer.putInt(payloadLength);
    }

    private void encodeColumn(QwpTableBuffer.ColumnBuffer col, QwpColumnDef colDef, int rowCount, boolean useGorilla) {
        int valueCount = col.getValueCount();
        long dataAddr = col.getDataAddress();

        if (colDef.isNullable()) {
            writeNullBitmap(col, rowCount);
        }

        switch (col.getType()) {
            case TYPE_BOOLEAN:
                writeBooleanColumn(dataAddr, valueCount);
                break;
            case TYPE_BYTE:
                buffer.putBlockOfBytes(dataAddr, valueCount);
                break;
            case TYPE_SHORT:
            case TYPE_CHAR:
                buffer.putBlockOfBytes(dataAddr, (long) valueCount * 2);
                break;
            case TYPE_INT:
                buffer.putBlockOfBytes(dataAddr, (long) valueCount * 4);
                break;
            case TYPE_LONG:
                buffer.putBlockOfBytes(dataAddr, (long) valueCount * 8);
                break;
            case TYPE_FLOAT:
                buffer.putBlockOfBytes(dataAddr, (long) valueCount * 4);
                break;
            case TYPE_DOUBLE:
                buffer.putBlockOfBytes(dataAddr, (long) valueCount * 8);
                break;
            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
                writeTimestampColumn(dataAddr, valueCount, useGorilla);
                break;
            case TYPE_DATE:
                buffer.putBlockOfBytes(dataAddr, (long) valueCount * 8);
                break;
            case TYPE_STRING:
            case TYPE_VARCHAR:
                writeStringColumn(col.getStringValues(), valueCount);
                break;
            case TYPE_SYMBOL:
                writeSymbolColumn(col, valueCount);
                break;
            case TYPE_UUID:
                // Stored as lo+hi contiguously, matching wire order
                buffer.putBlockOfBytes(dataAddr, (long) valueCount * 16);
                break;
            case TYPE_LONG256:
                // Stored as 4 contiguous longs per value
                buffer.putBlockOfBytes(dataAddr, (long) valueCount * 32);
                break;
            case TYPE_DOUBLE_ARRAY:
                writeDoubleArrayColumn(col, valueCount);
                break;
            case TYPE_LONG_ARRAY:
                writeLongArrayColumn(col, valueCount);
                break;
            case TYPE_DECIMAL64:
                writeDecimal64Column(col.getDecimalScale(), dataAddr, valueCount);
                break;
            case TYPE_DECIMAL128:
                writeDecimal128Column(col.getDecimalScale(), dataAddr, valueCount);
                break;
            case TYPE_DECIMAL256:
                writeDecimal256Column(col.getDecimalScale(), dataAddr, valueCount);
                break;
            default:
                throw new IllegalStateException("Unknown column type: " + col.getType());
        }
    }

    private void encodeColumnWithGlobalSymbols(QwpTableBuffer.ColumnBuffer col, QwpColumnDef colDef, int rowCount, boolean useGorilla) {
        int valueCount = col.getValueCount();

        if (colDef.isNullable()) {
            writeNullBitmap(col, rowCount);
        }

        if (col.getType() == TYPE_SYMBOL) {
            writeSymbolColumnWithGlobalIds(col, valueCount);
        } else {
            // Delegate to standard encoding for all other types
            long dataAddr = col.getDataAddress();
            switch (col.getType()) {
                case TYPE_BOOLEAN:
                    writeBooleanColumn(dataAddr, valueCount);
                    break;
                case TYPE_BYTE:
                    buffer.putBlockOfBytes(dataAddr, valueCount);
                    break;
                case TYPE_SHORT:
                case TYPE_CHAR:
                    buffer.putBlockOfBytes(dataAddr, (long) valueCount * 2);
                    break;
                case TYPE_INT:
                    buffer.putBlockOfBytes(dataAddr, (long) valueCount * 4);
                    break;
                case TYPE_LONG:
                    buffer.putBlockOfBytes(dataAddr, (long) valueCount * 8);
                    break;
                case TYPE_FLOAT:
                    buffer.putBlockOfBytes(dataAddr, (long) valueCount * 4);
                    break;
                case TYPE_DOUBLE:
                    buffer.putBlockOfBytes(dataAddr, (long) valueCount * 8);
                    break;
                case TYPE_TIMESTAMP:
                case TYPE_TIMESTAMP_NANOS:
                    writeTimestampColumn(dataAddr, valueCount, useGorilla);
                    break;
                case TYPE_DATE:
                    buffer.putBlockOfBytes(dataAddr, (long) valueCount * 8);
                    break;
                case TYPE_STRING:
                case TYPE_VARCHAR:
                    writeStringColumn(col.getStringValues(), valueCount);
                    break;
                case TYPE_UUID:
                    buffer.putBlockOfBytes(dataAddr, (long) valueCount * 16);
                    break;
                case TYPE_LONG256:
                    buffer.putBlockOfBytes(dataAddr, (long) valueCount * 32);
                    break;
                case TYPE_DOUBLE_ARRAY:
                    writeDoubleArrayColumn(col, valueCount);
                    break;
                case TYPE_LONG_ARRAY:
                    writeLongArrayColumn(col, valueCount);
                    break;
                case TYPE_DECIMAL64:
                    writeDecimal64Column(col.getDecimalScale(), dataAddr, valueCount);
                    break;
                case TYPE_DECIMAL128:
                    writeDecimal128Column(col.getDecimalScale(), dataAddr, valueCount);
                    break;
                case TYPE_DECIMAL256:
                    writeDecimal256Column(col.getDecimalScale(), dataAddr, valueCount);
                    break;
                default:
                    throw new IllegalStateException("Unknown column type: " + col.getType());
            }
        }
    }

    private void encodeTable(QwpTableBuffer tableBuffer, boolean useSchemaRef) {
        QwpColumnDef[] columnDefs = tableBuffer.getColumnDefs();
        int rowCount = tableBuffer.getRowCount();

        if (useSchemaRef) {
            writeTableHeaderWithSchemaRef(
                    tableBuffer.getTableName(),
                    rowCount,
                    tableBuffer.getSchemaHash(),
                    columnDefs.length
            );
        } else {
            writeTableHeaderWithSchema(tableBuffer.getTableName(), rowCount, columnDefs);
        }

        boolean useGorilla = isGorillaEnabled();
        for (int i = 0; i < tableBuffer.getColumnCount(); i++) {
            QwpTableBuffer.ColumnBuffer col = tableBuffer.getColumn(i);
            QwpColumnDef colDef = columnDefs[i];
            encodeColumn(col, colDef, rowCount, useGorilla);
        }
    }

    private void encodeTableWithGlobalSymbols(QwpTableBuffer tableBuffer, boolean useSchemaRef) {
        QwpColumnDef[] columnDefs = tableBuffer.getColumnDefs();
        int rowCount = tableBuffer.getRowCount();

        if (useSchemaRef) {
            writeTableHeaderWithSchemaRef(
                    tableBuffer.getTableName(),
                    rowCount,
                    tableBuffer.getSchemaHash(),
                    columnDefs.length
            );
        } else {
            writeTableHeaderWithSchema(tableBuffer.getTableName(), rowCount, columnDefs);
        }

        boolean useGorilla = isGorillaEnabled();
        for (int i = 0; i < tableBuffer.getColumnCount(); i++) {
            QwpTableBuffer.ColumnBuffer col = tableBuffer.getColumn(i);
            QwpColumnDef colDef = columnDefs[i];
            encodeColumnWithGlobalSymbols(col, colDef, rowCount, useGorilla);
        }
    }

    /**
     * Writes boolean column data (bit-packed on wire).
     * Reads individual bytes from off-heap and packs into bits.
     */
    private void writeBooleanColumn(long addr, int count) {
        int packedSize = (count + 7) / 8;
        for (int i = 0; i < packedSize; i++) {
            byte b = 0;
            for (int bit = 0; bit < 8; bit++) {
                int idx = i * 8 + bit;
                if (idx < count && Unsafe.getUnsafe().getByte(addr + idx) != 0) {
                    b |= (1 << bit);
                }
            }
            buffer.putByte(b);
        }
    }

    /**
     * Writes Decimal128 values in big-endian wire format.
     * Reads hi/lo pairs from off-heap (stored as hi, lo per value).
     */
    private void writeDecimal128Column(byte scale, long addr, int count) {
        buffer.putByte(scale);
        for (int i = 0; i < count; i++) {
            long offset = (long) i * 16;
            long hi = Unsafe.getUnsafe().getLong(addr + offset);
            long lo = Unsafe.getUnsafe().getLong(addr + offset + 8);
            buffer.putLongBE(hi);
            buffer.putLongBE(lo);
        }
    }

    /**
     * Writes Decimal256 values in big-endian wire format.
     * Reads hh/hl/lh/ll quads from off-heap (stored contiguously per value).
     */
    private void writeDecimal256Column(byte scale, long addr, int count) {
        buffer.putByte(scale);
        for (int i = 0; i < count; i++) {
            long offset = (long) i * 32;
            buffer.putLongBE(Unsafe.getUnsafe().getLong(addr + offset));
            buffer.putLongBE(Unsafe.getUnsafe().getLong(addr + offset + 8));
            buffer.putLongBE(Unsafe.getUnsafe().getLong(addr + offset + 16));
            buffer.putLongBE(Unsafe.getUnsafe().getLong(addr + offset + 24));
        }
    }

    /**
     * Writes Decimal64 values in big-endian wire format.
     * Reads longs from off-heap.
     */
    private void writeDecimal64Column(byte scale, long addr, int count) {
        buffer.putByte(scale);
        for (int i = 0; i < count; i++) {
            buffer.putLongBE(Unsafe.getUnsafe().getLong(addr + (long) i * 8));
        }
    }

    private void writeDoubleArrayColumn(QwpTableBuffer.ColumnBuffer col, int count) {
        byte[] dims = col.getArrayDims();
        int[] shapes = col.getArrayShapes();
        double[] data = col.getDoubleArrayData();

        int shapeIdx = 0;
        int dataIdx = 0;
        for (int row = 0; row < count; row++) {
            int nDims = dims[row];
            buffer.putByte((byte) nDims);

            int elemCount = 1;
            for (int d = 0; d < nDims; d++) {
                int dimLen = shapes[shapeIdx++];
                buffer.putInt(dimLen);
                elemCount *= dimLen;
            }

            for (int e = 0; e < elemCount; e++) {
                buffer.putDouble(data[dataIdx++]);
            }
        }
    }

    private void writeLongArrayColumn(QwpTableBuffer.ColumnBuffer col, int count) {
        byte[] dims = col.getArrayDims();
        int[] shapes = col.getArrayShapes();
        long[] data = col.getLongArrayData();

        int shapeIdx = 0;
        int dataIdx = 0;
        for (int row = 0; row < count; row++) {
            int nDims = dims[row];
            buffer.putByte((byte) nDims);

            int elemCount = 1;
            for (int d = 0; d < nDims; d++) {
                int dimLen = shapes[shapeIdx++];
                buffer.putInt(dimLen);
                elemCount *= dimLen;
            }

            for (int e = 0; e < elemCount; e++) {
                buffer.putLong(data[dataIdx++]);
            }
        }
    }

    /**
     * Writes a null bitmap from off-heap memory.
     * On little-endian platforms, the byte layout of the long-packed bitmap
     * in memory matches the wire format, enabling bulk copy.
     */
    private void writeNullBitmap(QwpTableBuffer.ColumnBuffer col, int rowCount) {
        long nullAddr = col.getNullBitmapAddress();
        if (nullAddr != 0) {
            int bitmapSize = (rowCount + 7) / 8;
            buffer.putBlockOfBytes(nullAddr, bitmapSize);
        } else {
            // Non-nullable column shouldn't reach here, but write zeros as fallback
            int bitmapSize = (rowCount + 7) / 8;
            for (int i = 0; i < bitmapSize; i++) {
                buffer.putByte((byte) 0);
            }
        }
    }

    private void writeStringColumn(String[] strings, int count) {
        int totalDataLen = 0;
        for (int i = 0; i < count; i++) {
            if (strings[i] != null) {
                totalDataLen += QwpBufferWriter.utf8Length(strings[i]);
            }
        }

        int runningOffset = 0;
        buffer.putInt(0);
        for (int i = 0; i < count; i++) {
            if (strings[i] != null) {
                runningOffset += QwpBufferWriter.utf8Length(strings[i]);
            }
            buffer.putInt(runningOffset);
        }

        for (int i = 0; i < count; i++) {
            if (strings[i] != null) {
                buffer.putUtf8(strings[i]);
            }
        }
    }

    /**
     * Writes a symbol column with dictionary.
     * Reads local symbol indices from off-heap data buffer.
     */
    private void writeSymbolColumn(QwpTableBuffer.ColumnBuffer col, int count) {
        long dataAddr = col.getDataAddress();
        String[] dictionary = col.getSymbolDictionary();

        buffer.putVarint(dictionary.length);
        for (String symbol : dictionary) {
            buffer.putString(symbol);
        }

        for (int i = 0; i < count; i++) {
            int idx = Unsafe.getUnsafe().getInt(dataAddr + (long) i * 4);
            buffer.putVarint(idx);
        }
    }

    /**
     * Writes a symbol column using global IDs (for delta dictionary mode).
     * Reads from auxiliary data buffer if available, otherwise falls back to local indices.
     */
    private void writeSymbolColumnWithGlobalIds(QwpTableBuffer.ColumnBuffer col, int count) {
        long auxAddr = col.getAuxDataAddress();
        if (auxAddr == 0) {
            // Fall back to local indices
            long dataAddr = col.getDataAddress();
            for (int i = 0; i < count; i++) {
                int idx = Unsafe.getUnsafe().getInt(dataAddr + (long) i * 4);
                buffer.putVarint(idx);
            }
        } else {
            for (int i = 0; i < count; i++) {
                int globalId = Unsafe.getUnsafe().getInt(auxAddr + (long) i * 4);
                buffer.putVarint(globalId);
            }
        }
    }

    private void writeTableHeaderWithSchema(String tableName, int rowCount, QwpColumnDef[] columns) {
        buffer.putString(tableName);
        buffer.putVarint(rowCount);
        buffer.putVarint(columns.length);
        buffer.putByte(SCHEMA_MODE_FULL);
        for (QwpColumnDef col : columns) {
            buffer.putString(col.getName());
            buffer.putByte(col.getWireTypeCode());
        }
    }

    private void writeTableHeaderWithSchemaRef(String tableName, int rowCount, long schemaHash, int columnCount) {
        buffer.putString(tableName);
        buffer.putVarint(rowCount);
        buffer.putVarint(columnCount);
        buffer.putByte(SCHEMA_MODE_REFERENCE);
        buffer.putLong(schemaHash);
    }

    /**
     * Writes a timestamp column with optional Gorilla compression.
     * Reads longs directly from off-heap — zero heap allocation.
     */
    private void writeTimestampColumn(long addr, int count, boolean useGorilla) {
        if (useGorilla && count > 2) {
            if (QwpGorillaEncoder.canUseGorilla(addr, count)) {
                buffer.putByte(ENCODING_GORILLA);
                int encodedSize = QwpGorillaEncoder.calculateEncodedSize(addr, count);
                buffer.ensureCapacity(encodedSize);
                int bytesWritten = gorillaEncoder.encodeTimestamps(
                        buffer.getBufferPtr() + buffer.getPosition(),
                        buffer.getCapacity() - buffer.getPosition(),
                        addr,
                        count
                );
                buffer.skip(bytesWritten);
            } else {
                buffer.putByte(ENCODING_UNCOMPRESSED);
                buffer.putBlockOfBytes(addr, (long) count * 8);
            }
        } else {
            if (useGorilla) {
                buffer.putByte(ENCODING_UNCOMPRESSED);
            }
            buffer.putBlockOfBytes(addr, (long) count * 8);
        }
    }
}
