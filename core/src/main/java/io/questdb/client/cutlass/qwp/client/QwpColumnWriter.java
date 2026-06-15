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

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.client.cutlass.qwp.protocol.QwpGorillaEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Unsafe;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Transport-agnostic column encoder for QWP v1 table data.
 * <p>
 * Reads column data from {@link QwpTableBuffer.ColumnBuffer} and writes encoded
 * bytes to a {@link QwpBufferWriter}. Both {@link QwpWebSocketEncoder} and
 * {@link QwpUdpSender} delegate to this class for column encoding.
 */
class QwpColumnWriter {

    private static final byte ENCODING_GORILLA = 0x01;
    private static final byte ENCODING_UNCOMPRESSED = 0x00;
    private final QwpGorillaEncoder gorillaEncoder = new QwpGorillaEncoder();
    private QwpBufferWriter buffer;

    private void encodeColumn(
            QwpTableBuffer.ColumnBuffer col,
            int rowCount,
            int valueCount,
            long stringDataSize,
            int symbolDictionarySize,
            boolean useGorilla,
            boolean useGlobalSymbols
    ) {
        long dataAddr = col.getDataAddress();

        writeNullHeader(col, rowCount, rowCount - valueCount);

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
            case TYPE_IPv4:
            case TYPE_FLOAT:
                buffer.putBlockOfBytes(dataAddr, (long) valueCount * 4);
                break;
            case TYPE_LONG:
            case TYPE_DATE:
            case TYPE_DOUBLE:
                buffer.putBlockOfBytes(dataAddr, (long) valueCount * 8);
                break;
            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
                writeTimestampColumn(dataAddr, valueCount, useGorilla);
                break;
            case TYPE_GEOHASH:
                writeGeoHashColumn(dataAddr, valueCount, col.getGeoHashPrecision());
                break;
            case TYPE_VARCHAR:
            case TYPE_BINARY:
                // BINARY shares VARCHAR's wire layout (offsets + concatenated bytes);
                // only the byte-stream contract differs (opaque vs UTF-8).
                writeStringColumn(col, valueCount, stringDataSize);
                break;
            case TYPE_SYMBOL:
                if (useGlobalSymbols) {
                    writeSymbolColumnWithGlobalIds(col, valueCount);
                } else {
                    writeSymbolColumn(col, valueCount, symbolDictionarySize);
                }
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
                throw new LineSenderException("Unknown column type: " + col.getType());
        }
    }

    private void writeBooleanColumn(long addr, int count) {
        int packedSize = (count + 7) / 8;
        for (int i = 0; i < packedSize; i++) {
            byte b = 0;
            for (int bit = 0; bit < 8; bit++) {
                int idx = i * 8 + bit;
                if (idx < count && Unsafe.getUnsafe().getByte(addr + idx) != 0) {
                    b |= (byte) (1 << bit);
                }
            }
            buffer.putByte(b);
        }
    }

    private void writeDecimal128Column(byte scale, long addr, int count) {
        buffer.putByte(scale);
        for (int i = 0; i < count; i++) {
            long offset = (long) i * 16;
            long hi = Unsafe.getUnsafe().getLong(addr + offset);
            long lo = Unsafe.getUnsafe().getLong(addr + offset + 8);
            buffer.putLong(lo);
            buffer.putLong(hi);
        }
    }

    private void writeDecimal256Column(byte scale, long addr, int count) {
        buffer.putByte(scale);
        for (int i = 0; i < count; i++) {
            long offset = (long) i * 32;
            buffer.putLong(Unsafe.getUnsafe().getLong(addr + offset + 24));
            buffer.putLong(Unsafe.getUnsafe().getLong(addr + offset + 16));
            buffer.putLong(Unsafe.getUnsafe().getLong(addr + offset + 8));
            buffer.putLong(Unsafe.getUnsafe().getLong(addr + offset));
        }
    }

    private void writeDecimal64Column(byte scale, long addr, int count) {
        buffer.putByte(scale);
        for (int i = 0; i < count; i++) {
            buffer.putLong(Unsafe.getUnsafe().getLong(addr + (long) i * 8));
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
                elemCount = Math.multiplyExact(elemCount, dimLen);
            }

            for (int e = 0; e < elemCount; e++) {
                buffer.putDouble(data[dataIdx++]);
            }
        }
    }

    private void writeGeoHashColumn(long addr, int count, int precision) {
        if (precision < 1) {
            precision = 1;
        }
        buffer.putVarint(precision);
        int valueSize = (precision + 7) / 8;
        for (int i = 0; i < count; i++) {
            long value = Unsafe.getUnsafe().getLong(addr + (long) i * 8);
            for (int b = 0; b < valueSize; b++) {
                buffer.putByte((byte) (value >>> (b * 8)));
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
                elemCount = Math.multiplyExact(elemCount, dimLen);
            }

            for (int e = 0; e < elemCount; e++) {
                buffer.putLong(data[dataIdx++]);
            }
        }
    }

    private void writeNullHeader(QwpTableBuffer.ColumnBuffer col, int rowCount, int nullCount) {
        if (nullCount > 0) {
            buffer.putByte((byte) 1);
            col.ensureNullBitmapCapacity(rowCount);
            long nullAddr = col.getNullBitmapAddress();
            int bitmapSize = (rowCount + 7) / 8;
            buffer.putBlockOfBytes(nullAddr, bitmapSize);
        } else {
            buffer.putByte((byte) 0);
        }
    }

    private void writeStringColumn(QwpTableBuffer.ColumnBuffer col, int valueCount, long stringDataSize) {
        buffer.putBlockOfBytes(col.getStringOffsetsAddress(), (long) (valueCount + 1) * 4);
        buffer.putBlockOfBytes(col.getStringDataAddress(), stringDataSize);
    }

    private void writeSymbolColumn(QwpTableBuffer.ColumnBuffer col, int count, int dictionarySize) {
        long dataAddr = col.getDataAddress();
        buffer.putVarint(dictionarySize);
        for (int i = 0; i < dictionarySize; i++) {
            buffer.putString(col.getSymbolValue(i));
        }

        for (int i = 0; i < count; i++) {
            int idx = Unsafe.getUnsafe().getInt(dataAddr + (long) i * 4);
            buffer.putVarint(idx);
        }
    }

    private void writeSymbolColumnWithGlobalIds(QwpTableBuffer.ColumnBuffer col, int count) {
        long auxAddr = col.getAuxDataAddress();
        if (auxAddr == 0) {
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

    private void writeTableHeader(String tableName, int rowCount, QwpColumnDef[] columns) {
        buffer.putString(tableName);
        buffer.putVarint(rowCount);
        buffer.putVarint(columns.length);
        for (QwpColumnDef col : columns) {
            buffer.putString(col.getName());
            buffer.putByte(col.getWireTypeCode());
        }
    }

    private void writeTimestampColumn(long addr, int count, boolean useGorilla) {
        if (useGorilla && count > 2) {
            // Single pass: check feasibility and compute encoded size together
            int encodedSize = QwpGorillaEncoder.calculateEncodedSizeIfSupported(addr, count);
            if (encodedSize >= 0) {
                buffer.putByte(ENCODING_GORILLA);
                buffer.ensureCapacity(encodedSize);
                int bytesWritten = gorillaEncoder.encodeTimestamps(
                        buffer.getWriteAddress(),
                        buffer.getWritableBytes(),
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

    void encodeTable(QwpTableBuffer tableBuffer, boolean useGlobalSymbols, boolean useGorilla) {
        encodeTable(tableBuffer, tableBuffer.getRowCount(), null, null, null, useGlobalSymbols, useGorilla);
    }

    void encodeTable(
            QwpTableBuffer tableBuffer,
            int rowCount,
            int[] limitedValueCounts,
            long[] limitedStringDataSizes,
            int[] limitedSymbolDictionarySizes,
            boolean useGlobalSymbols,
            boolean useGorilla
    ) {
        QwpColumnDef[] columnDefs = tableBuffer.getColumnDefs();

        writeTableHeader(tableBuffer.getTableName(), rowCount, columnDefs);

        for (int i = 0; i < tableBuffer.getColumnCount(); i++) {
            QwpTableBuffer.ColumnBuffer col = tableBuffer.getColumn(i);
            int valueCount = col.getValueCount();
            long stringDataSize = col.getStringDataSize();
            int symbolDictionarySize = col.getSymbolDictionarySize();

            if (limitedValueCounts != null && limitedValueCounts[i] > -1) {
                valueCount = limitedValueCounts[i];
                stringDataSize = limitedStringDataSizes[i];
                symbolDictionarySize = limitedSymbolDictionarySizes[i];
            }

            encodeColumn(col, rowCount, valueCount, stringDataSize, symbolDictionarySize, useGorilla, useGlobalSymbols);
        }
    }

    void setBuffer(QwpBufferWriter buffer) {
        this.buffer = buffer;
    }
}
