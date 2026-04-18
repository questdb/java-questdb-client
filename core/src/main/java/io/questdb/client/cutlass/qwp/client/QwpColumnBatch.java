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

import io.questdb.client.std.ObjList;

/**
 * Column-major view over one decoded {@code RESULT_BATCH}. The view becomes
 * stale after the next {@code decode()} call on the owning
 * {@link QwpResultBatchDecoder}.
 * <p>
 * Values are returned boxed for Phase-1 simplicity. Primitive accessors
 * ({@link #getLong}, {@link #getDouble}, etc.) unbox with {@link #isNull}
 * returning {@code true} for NULL rows (primitives return 0/NaN in that case).
 */
public class QwpColumnBatch {

    ObjList<QwpEgressColumnInfo> columns;
    /**
     * columnValues[columnIndex][rowIndex] = boxed value (null for NULL rows).
     * Visibility: package-private so {@link QwpResultBatchDecoder} populates directly.
     */
    final ObjList<Object[]> columnValues = new ObjList<>();
    long batchSeq;
    int columnCount;
    long requestId;
    int rowCount;

    public int getColumnCount() {
        return columnCount;
    }

    public String getColumnName(int col) {
        return columns.getQuick(col).name;
    }

    public byte getColumnWireType(int col) {
        return columns.getQuick(col).wireType;
    }

    public double getDouble(int col, int row) {
        Object v = columnValues.getQuick(col)[row];
        return v == null ? Double.NaN : (Double) v;
    }

    public float getFloat(int col, int row) {
        Object v = columnValues.getQuick(col)[row];
        return v == null ? Float.NaN : (Float) v;
    }

    public int getGeohashPrecisionBits(int col) {
        return columns.getQuick(col).precisionBits;
    }

    public long getLong(int col, int row) {
        Object v = columnValues.getQuick(col)[row];
        return v == null ? 0L : (Long) v;
    }

    public long[] getLongArray(int col, int row) {
        return (long[]) columnValues.getQuick(col)[row];
    }

    public int getRowCount() {
        return rowCount;
    }

    public String getString(int col, int row) {
        return (String) columnValues.getQuick(col)[row];
    }

    public Object getValue(int col, int row) {
        return columnValues.getQuick(col)[row];
    }

    public byte[] getVarchar(int col, int row) {
        return (byte[]) columnValues.getQuick(col)[row];
    }

    public boolean isNull(int col, int row) {
        return columnValues.getQuick(col)[row] == null;
    }

    public long requestId() {
        return requestId;
    }

    public long batchSeq() {
        return batchSeq;
    }

    void reset(long requestId, long batchSeq, int rowCount, int columnCount, ObjList<QwpEgressColumnInfo> columns) {
        this.requestId = requestId;
        this.batchSeq = batchSeq;
        this.rowCount = rowCount;
        this.columnCount = columnCount;
        this.columns = columns;
        while (columnValues.size() < columnCount) {
            columnValues.add(new Object[0]);
        }
        for (int ci = 0; ci < columnCount; ci++) {
            Object[] arr = columnValues.getQuick(ci);
            if (arr.length < rowCount) {
                arr = new Object[rowCount];
                columnValues.setQuick(ci, arr);
            } else {
                // Clear trailing slots from previous batches
                for (int r = rowCount; r < arr.length; r++) arr[r] = null;
            }
        }
    }

    Object[] columnValues(int col) {
        return columnValues.getQuick(col);
    }
}
