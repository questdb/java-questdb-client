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
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;

/**
 * Estimates the encoded datagram size for a {@link QwpTableBuffer}.
 * <p>
 * The estimate mirrors the encoding in {@link QwpWebSocketEncoder#encode}
 * with {@code useSchemaRef=false} and Gorilla disabled. The estimate is
 * always >= the actual encoded size.
 */
public final class QwpDatagramSizeEstimator {

    private QwpDatagramSizeEstimator() {
    }

    /**
     * Estimates the encoded datagram size in bytes.
     * <p>
     * The {@code rowCount} parameter is separate from {@code tableBuffer.getRowCount()}
     * so the caller can pass {@code rowCount + 1} for a tentative pre-commit estimate.
     *
     * @param tableBuffer the table buffer to estimate
     * @param rowCount    the number of rows to estimate for
     * @return the estimated encoded size in bytes (always >= actual)
     */
    public static long estimate(QwpTableBuffer tableBuffer, int rowCount) {
        long size = 0;

        // Header: ILP4 (4) + version (1) + flags (1) + tableCount (2) + payloadLength (4) = 12
        size += HEADER_SIZE;

        // Table name
        String tableName = tableBuffer.getTableName();
        int tableNameUtf8Len = NativeBufferWriter.utf8Length(tableName);
        size += varintSize(tableNameUtf8Len) + tableNameUtf8Len;

        // Row count varint
        size += varintSize(rowCount);

        int columnCount = tableBuffer.getColumnCount();
        // Column count varint
        size += varintSize(columnCount);

        // Schema mode byte
        size += 1;

        QwpColumnDef[] columnDefs = tableBuffer.getColumnDefs();

        // Per-column schema: name + wire type code
        for (int i = 0; i < columnCount; i++) {
            QwpColumnDef colDef = columnDefs[i];
            int nameUtf8Len = NativeBufferWriter.utf8Length(colDef.getName());
            size += varintSize(nameUtf8Len) + nameUtf8Len;
            size += 1; // wire type code
        }

        // Per-column data
        for (int i = 0; i < columnCount; i++) {
            QwpTableBuffer.ColumnBuffer col = tableBuffer.getColumn(i);
            QwpColumnDef colDef = columnDefs[i];
            int valueCount = col.getValueCount();

            // Nullable bitmap
            if (colDef.isNullable()) {
                size += (rowCount + 7) / 8;
            }

            // Safety margin: if this column has fewer values than rows,
            // nextRow() will pad it -- account for one extra element
            if (col.getSize() < rowCount) {
                size += elementWireSize(col.getType());
            }

            size += estimateColumnData(col, valueCount);
        }

        // Fixed safety margin
        size += 8;

        return size;
    }

    public static int varintSize(long value) {
        if (value == 0) {
            return 1;
        }
        return (64 - Long.numberOfLeadingZeros(value) + 6) / 7;
    }

    private static long estimateArrayColumn(QwpTableBuffer.ColumnBuffer col, int valueCount, int elemBytes) {
        long size = 0;
        byte[] dims = col.getArrayDims();
        int[] shapes = col.getArrayShapes();
        if (dims == null || shapes == null) {
            return size;
        }

        int shapeIdx = 0;
        for (int row = 0; row < valueCount; row++) {
            int nDims = dims[row];
            // nDims byte
            size += 1;
            int elemCount = 1;
            for (int d = 0; d < nDims; d++) {
                // dim length int32
                size += 4;
                elemCount *= shapes[shapeIdx++];
            }
            // elements
            size += (long) elemCount * elemBytes;
        }
        return size;
    }

    private static long estimateColumnData(QwpTableBuffer.ColumnBuffer col, int valueCount) {
        return switch (col.getType()) {
            case TYPE_BOOLEAN -> (valueCount + 7) / 8;
            case TYPE_BYTE -> valueCount;
            case TYPE_SHORT, TYPE_CHAR -> (long) valueCount * 2;
            case TYPE_INT, TYPE_FLOAT -> (long) valueCount * 4;
            case TYPE_LONG, TYPE_DOUBLE, TYPE_DATE -> (long) valueCount * 8;
            case TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS ->
                // Gorilla is disabled for UDP, so no encoding byte -- just raw longs
                    (long) valueCount * 8;
            case TYPE_UUID -> (long) valueCount * 16;
            case TYPE_LONG256 -> (long) valueCount * 32;
            case TYPE_DECIMAL64 -> 1 + (long) valueCount * 8;
            case TYPE_DECIMAL128 -> 1 + (long) valueCount * 16;
            case TYPE_DECIMAL256 -> 1 + (long) valueCount * 32;
            case TYPE_STRING, TYPE_VARCHAR ->
                    (long) (valueCount + 1) * 4 + col.getStringDataSize();
            case TYPE_SYMBOL -> estimateSymbolColumn(col, valueCount);
            case TYPE_GEOHASH -> estimateGeoHashColumn(col, valueCount);
            case TYPE_DOUBLE_ARRAY -> estimateArrayColumn(col, valueCount, 8);
            case TYPE_LONG_ARRAY -> estimateArrayColumn(col, valueCount, 8);
            default -> 0;
        };
    }

    private static long estimateGeoHashColumn(QwpTableBuffer.ColumnBuffer col, int valueCount) {
        int precision = col.getGeoHashPrecision();
        if (precision < 1) {
            precision = 1;
        }
        long size = varintSize(precision);
        int valueSize = (precision + 7) / 8;
        size += (long) valueCount * valueSize;
        return size;
    }

    private static long estimateSymbolColumn(QwpTableBuffer.ColumnBuffer col, int valueCount) {
        String[] dictionary = col.getSymbolDictionary();
        int dictSize = dictionary.length;

        long size = varintSize(dictSize);
        for (String symbol : dictionary) {
            int utf8Len = NativeBufferWriter.utf8Length(symbol);
            size += varintSize(utf8Len) + utf8Len;
        }

        // Per-value index varints. Maximum index is dictSize - 1.
        int maxIndex = Math.max(0, dictSize - 1);
        size += (long) valueCount * varintSize(maxIndex);

        return size;
    }

    private static int elementWireSize(byte type) {
        return switch (type) {
            case TYPE_BOOLEAN -> 1;
            case TYPE_BYTE -> 1;
            case TYPE_SHORT, TYPE_CHAR -> 2;
            case TYPE_INT, TYPE_FLOAT -> 4;
            case TYPE_LONG, TYPE_DOUBLE, TYPE_DATE, TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS -> 8;
            case TYPE_UUID -> 16;
            case TYPE_LONG256 -> 32;
            case TYPE_DECIMAL64 -> 8;
            case TYPE_DECIMAL128 -> 16;
            case TYPE_DECIMAL256 -> 32;
            case TYPE_STRING, TYPE_VARCHAR -> 4; // one offset entry
            case TYPE_SYMBOL -> 1; // one varint index (at least 1 byte)
            case TYPE_GEOHASH -> 8;
            case TYPE_DOUBLE_ARRAY, TYPE_LONG_ARRAY -> 5; // 1 dim byte + 4 shape int
            default -> 0;
        };
    }
}
