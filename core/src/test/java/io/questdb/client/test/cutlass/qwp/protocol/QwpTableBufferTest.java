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

package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import org.junit.Test;

import static org.junit.Assert.*;

public class QwpTableBufferTest {

    /**
     * Simulates the encoder's walk over array data — the same logic as
     * QwpWebSocketEncoder.writeDoubleArrayColumn(). Returns the flat
     * double values the encoder would serialize for the given column.
     */
    private static double[] readDoubleArraysLikeEncoder(QwpTableBuffer.ColumnBuffer col) {
        byte[] dims = col.getArrayDims();
        int[] shapes = col.getArrayShapes();
        double[] data = col.getDoubleArrayData();
        int count = col.getValueCount();

        // First pass: count total elements
        int totalElements = 0;
        int shapeIdx = 0;
        for (int row = 0; row < count; row++) {
            int nDims = dims[row];
            int elemCount = 1;
            for (int d = 0; d < nDims; d++) {
                elemCount *= shapes[shapeIdx++];
            }
            totalElements += elemCount;
        }

        // Second pass: collect values
        double[] result = new double[totalElements];
        shapeIdx = 0;
        int dataIdx = 0;
        int resultIdx = 0;
        for (int row = 0; row < count; row++) {
            int nDims = dims[row];
            int elemCount = 1;
            for (int d = 0; d < nDims; d++) {
                elemCount *= shapes[shapeIdx++];
            }
            for (int e = 0; e < elemCount; e++) {
                result[resultIdx++] = data[dataIdx++];
            }
        }
        return result;
    }

    /**
     * Same as above but for long arrays (mirrors QwpWebSocketEncoder.writeLongArrayColumn()).
     */
    private static long[] readLongArraysLikeEncoder(QwpTableBuffer.ColumnBuffer col) {
        byte[] dims = col.getArrayDims();
        int[] shapes = col.getArrayShapes();
        long[] data = col.getLongArrayData();
        int count = col.getValueCount();

        int totalElements = 0;
        int shapeIdx = 0;
        for (int row = 0; row < count; row++) {
            int nDims = dims[row];
            int elemCount = 1;
            for (int d = 0; d < nDims; d++) {
                elemCount *= shapes[shapeIdx++];
            }
            totalElements += elemCount;
        }

        long[] result = new long[totalElements];
        shapeIdx = 0;
        int dataIdx = 0;
        int resultIdx = 0;
        for (int row = 0; row < count; row++) {
            int nDims = dims[row];
            int elemCount = 1;
            for (int d = 0; d < nDims; d++) {
                elemCount *= shapes[shapeIdx++];
            }
            for (int e = 0; e < elemCount; e++) {
                result[resultIdx++] = data[dataIdx++];
            }
        }
        return result;
    }

    @Test
    public void testCancelRowRewindsDoubleArrayOffsets() {
        try (QwpTableBuffer table = new QwpTableBuffer("test")) {
            // Row 0: committed with [1.0, 2.0]
            QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false);
            col.addDoubleArray(new double[]{1.0, 2.0});
            table.nextRow();

            // Row 1: committed with [3.0, 4.0]
            col = table.getOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false);
            col.addDoubleArray(new double[]{3.0, 4.0});
            table.nextRow();

            // Start row 2 with [5.0, 6.0] — then cancel it
            col = table.getOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false);
            col.addDoubleArray(new double[]{5.0, 6.0});
            table.cancelCurrentRow();

            // Add replacement row 2 with [7.0, 8.0]
            col = table.getOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false);
            col.addDoubleArray(new double[]{7.0, 8.0});
            table.nextRow();

            assertEquals(3, table.getRowCount());
            assertEquals(3, col.getValueCount());

            // Walk the arrays exactly as the encoder would
            double[] encoded = readDoubleArraysLikeEncoder(col);
            assertArrayEquals(
                    new double[]{1.0, 2.0, 3.0, 4.0, 7.0, 8.0},
                    encoded,
                    0.0
            );
        }
    }

    @Test
    public void testCancelRowRewindsLongArrayOffsets() {
        try (QwpTableBuffer table = new QwpTableBuffer("test")) {
            // Row 0: committed with [10, 20]
            QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false);
            col.addLongArray(new long[]{10, 20});
            table.nextRow();

            // Start row 1 with [30, 40] — then cancel it
            col = table.getOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false);
            col.addLongArray(new long[]{30, 40});
            table.cancelCurrentRow();

            // Add replacement row 1 with [50, 60]
            col = table.getOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false);
            col.addLongArray(new long[]{50, 60});
            table.nextRow();

            assertEquals(2, table.getRowCount());
            assertEquals(2, col.getValueCount());

            long[] encoded = readLongArraysLikeEncoder(col);
            assertArrayEquals(new long[]{10, 20, 50, 60}, encoded);
        }
    }

    @Test
    public void testCancelRowRewindsMultiDimArrayOffsets() {
        try (QwpTableBuffer table = new QwpTableBuffer("test")) {
            // Row 0: committed 2D array [[1.0, 2.0], [3.0, 4.0]]
            QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false);
            col.addDoubleArray(new double[][]{{1.0, 2.0}, {3.0, 4.0}});
            table.nextRow();

            // Start row 1 with 2D array [[5.0, 6.0], [7.0, 8.0]] — cancel
            col = table.getOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false);
            col.addDoubleArray(new double[][]{{5.0, 6.0}, {7.0, 8.0}});
            table.cancelCurrentRow();

            // Replacement row 1 with [[9.0, 10.0], [11.0, 12.0]]
            col = table.getOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false);
            col.addDoubleArray(new double[][]{{9.0, 10.0}, {11.0, 12.0}});
            table.nextRow();

            assertEquals(2, table.getRowCount());
            assertEquals(2, col.getValueCount());

            // Verify shapes are correct (2 dims per row, each [2, 2])
            int[] shapes = col.getArrayShapes();
            byte[] dims = col.getArrayDims();
            assertEquals(2, dims[0]);
            assertEquals(2, dims[1]);
            // Row 0 shapes: [2, 2]
            assertEquals(2, shapes[0]);
            assertEquals(2, shapes[1]);
            // Row 1 shapes must be the replacement [2, 2], not stale data
            assertEquals(2, shapes[2]);
            assertEquals(2, shapes[3]);

            double[] encoded = readDoubleArraysLikeEncoder(col);
            assertArrayEquals(
                    new double[]{1.0, 2.0, 3.0, 4.0, 9.0, 10.0, 11.0, 12.0},
                    encoded,
                    0.0
            );
        }
    }
}
