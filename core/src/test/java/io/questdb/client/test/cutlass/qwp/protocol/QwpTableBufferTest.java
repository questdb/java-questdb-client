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

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal64;
import org.junit.Test;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.*;

public class QwpTableBufferTest {

    @Test
    public void testAddDecimal128PrecisionLoss() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("d", QwpConstants.TYPE_DECIMAL128, true);
                // First row sets decimalScale = 2
                col.addDecimal128(Decimal128.fromLong(100, 2));
                table.nextRow();
                // Second row at scale 4 with trailing fractional digits that
                // cannot be represented at scale 2 without rounding
                try {
                    col.addDecimal128(Decimal128.fromLong(12345, 4));
                    fail("Expected LineSenderException for precision loss");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("precision loss"));
                }
            }
        });
    }

    @Test
    public void testAddDecimal128RescaleOverflow() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("d", QwpConstants.TYPE_DECIMAL128, true);
                // First row sets decimalScale = 10
                col.addDecimal128(Decimal128.fromLong(1, 10));
                table.nextRow();
                // Second row at scale 0 with a large value — rescaling to scale 10
                // multiplies by 10^10, which exceeds 128-bit capacity
                try {
                    col.addDecimal128(new Decimal128(Long.MAX_VALUE / 2, Long.MAX_VALUE, 0));
                    fail("Expected LineSenderException for 128-bit overflow");
                } catch (LineSenderException e) {
                    assertEquals("Decimal128 overflow: rescaling from scale 0 to 10 exceeds 128-bit capacity", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAddDecimal64PrecisionLoss() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("d", QwpConstants.TYPE_DECIMAL64, true);
                // First row sets decimalScale = 2
                col.addDecimal64(Decimal64.fromLong(100, 2));
                table.nextRow();
                // Second row at scale 4 with trailing fractional digits that
                // cannot be represented at scale 2 without rounding
                try {
                    col.addDecimal64(Decimal64.fromLong(12345, 4));
                    fail("Expected LineSenderException for precision loss");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("precision loss"));
                }
            }
        });
    }

    @Test
    public void testAddDecimal64RescaleOverflow() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("d", QwpConstants.TYPE_DECIMAL64, true);
                // First row sets decimalScale = 5
                col.addDecimal64(Decimal64.fromLong(1, 5));
                table.nextRow();
                // Second row at scale 0 with a large value — rescaling to scale 5
                // multiplies by 10^5 = 100_000, which exceeds 64-bit capacity
                // Long.MAX_VALUE / 10 ≈ 9.2 * 10^17, * 10^5 ≈ 9.2 * 10^22 >> 2^63
                try {
                    col.addDecimal64(Decimal64.fromLong(Long.MAX_VALUE / 10, 0));
                    fail("Expected LineSenderException for 64-bit overflow");
                } catch (LineSenderException e) {
                    assertEquals("Decimal64 overflow: rescaling from scale 0 to 5 exceeds 64-bit capacity", e.getMessage());
                }
            }
        });
    }

    @Test
    public void testAddDoubleArrayNullOnNonNullableColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false);

                // Row 0: real array
                col.addDoubleArray(new double[]{1.0, 2.0});
                table.nextRow();

                // Row 1: null on non-nullable — must write empty array metadata
                col.addDoubleArray((double[]) null);
                table.nextRow();

                // Row 2: real array
                col.addDoubleArray(new double[]{3.0, 4.0});
                table.nextRow();

                assertEquals(3, table.getRowCount());
                assertEquals(3, col.getValueCount());
                assertEquals(col.getSize(), col.getValueCount());

                // Encoder walk must not corrupt — row 1 is an empty array
                double[] encoded = readDoubleArraysLikeEncoder(col);
                assertArrayEquals(new double[]{1.0, 2.0, 3.0, 4.0}, encoded, 0.0);

                byte[] dims = col.getArrayDims();
                int[] shapes = col.getArrayShapes();
                assertEquals(1, dims[0]);
                assertEquals(2, shapes[0]);
                assertEquals(1, dims[1]);  // null row: 1D empty
                assertEquals(0, shapes[1]); // null row: 0 elements
                assertEquals(1, dims[2]);
                assertEquals(2, shapes[2]);
            }
        });
    }

    @Test
    public void testAddLongArrayNullOnNonNullableColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false);

                // Row 0: real array
                col.addLongArray(new long[]{10, 20});
                table.nextRow();

                // Row 1: null on non-nullable — must write empty array metadata
                col.addLongArray((long[]) null);
                table.nextRow();

                // Row 2: real array
                col.addLongArray(new long[]{30, 40});
                table.nextRow();

                assertEquals(3, table.getRowCount());
                assertEquals(3, col.getValueCount());
                assertEquals(col.getSize(), col.getValueCount());

                // Encoder walk must not corrupt — row 1 is an empty array
                long[] encoded = readLongArraysLikeEncoder(col);
                assertArrayEquals(new long[]{10, 20, 30, 40}, encoded);

                byte[] dims = col.getArrayDims();
                int[] shapes = col.getArrayShapes();
                assertEquals(1, dims[0]);
                assertEquals(2, shapes[0]);
                assertEquals(1, dims[1]);  // null row: 1D empty
                assertEquals(0, shapes[1]); // null row: 0 elements
                assertEquals(1, dims[2]);
                assertEquals(2, shapes[2]);
            }
        });
    }

    @Test
    public void testAddSymbolNullOnNonNullableColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, false);
                col.addSymbol("server1");
                table.nextRow();

                // Null on a non-nullable column must write a sentinel value,
                // keeping size and valueCount in sync
                col.addSymbol(null);
                table.nextRow();

                col.addSymbol("server2");
                table.nextRow();

                assertEquals(3, table.getRowCount());
                // For non-nullable columns, every row must have a physical value
                assertEquals(col.getSize(), col.getValueCount());
            }
        });
    }

    @Test
    public void testCancelRowRewindsDoubleArrayOffsets() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testCancelRowRewindsLongArrayOffsets() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testCancelRowRewindsMultiDimArrayOffsets() throws Exception {
        assertMemoryLeak(() -> {
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
        });
    }

    @Test
    public void testDoubleArrayWrapperMultipleRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test");
                 DoubleArray arr = new DoubleArray(3)) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false);

                arr.append(1.0).append(2.0).append(3.0);
                col.addDoubleArray(arr);
                table.nextRow();

                // DoubleArray auto-wraps, so just append next row's data
                arr.append(4.0).append(5.0).append(6.0);
                col.addDoubleArray(arr);
                table.nextRow();

                arr.append(7.0).append(8.0).append(9.0);
                col.addDoubleArray(arr);
                table.nextRow();

                assertEquals(3, col.getValueCount());
                double[] encoded = readDoubleArraysLikeEncoder(col);
                assertArrayEquals(
                        new double[]{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0},
                        encoded,
                        0.0
                );

                byte[] dims = col.getArrayDims();
                int[] shapes = col.getArrayShapes();
                for (int i = 0; i < 3; i++) {
                    assertEquals(1, dims[i]);
                    assertEquals(3, shapes[i]);
                }
            }
        });
    }

    @Test
    public void testDoubleArrayWrapperShrinkingSize() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false);

                // Row 0: large array (5 elements)
                try (DoubleArray big = new DoubleArray(5)) {
                    big.append(1.0).append(2.0).append(3.0).append(4.0).append(5.0);
                    col.addDoubleArray(big);
                    table.nextRow();
                }

                // Row 1: smaller array (2 elements) — must not see leftover data from row 0
                try (DoubleArray small = new DoubleArray(2)) {
                    small.append(10.0).append(20.0);
                    col.addDoubleArray(small);
                    table.nextRow();
                }

                assertEquals(2, col.getValueCount());
                double[] encoded = readDoubleArraysLikeEncoder(col);
                assertArrayEquals(
                        new double[]{1.0, 2.0, 3.0, 4.0, 5.0, 10.0, 20.0},
                        encoded,
                        0.0
                );

                int[] shapes = col.getArrayShapes();
                assertEquals(5, shapes[0]);
                assertEquals(2, shapes[1]);
            }
        });
    }

    @Test
    public void testDoubleArrayWrapperVaryingDimensionality() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false);

                // Row 0: 2D array (2x2)
                try (DoubleArray matrix = new DoubleArray(2, 2)) {
                    matrix.append(1.0).append(2.0).append(3.0).append(4.0);
                    col.addDoubleArray(matrix);
                    table.nextRow();
                }

                // Row 1: 1D array (3 elements) — different dimensionality
                try (DoubleArray vec = new DoubleArray(3)) {
                    vec.append(10.0).append(20.0).append(30.0);
                    col.addDoubleArray(vec);
                    table.nextRow();
                }

                assertEquals(2, col.getValueCount());

                byte[] dims = col.getArrayDims();
                assertEquals(2, dims[0]);
                assertEquals(1, dims[1]);

                int[] shapes = col.getArrayShapes();
                // Row 0: shape [2, 2]
                assertEquals(2, shapes[0]);
                assertEquals(2, shapes[1]);
                // Row 1: shape [3]
                assertEquals(3, shapes[2]);

                double[] encoded = readDoubleArraysLikeEncoder(col);
                assertArrayEquals(
                        new double[]{1.0, 2.0, 3.0, 4.0, 10.0, 20.0, 30.0},
                        encoded,
                        0.0
                );
            }
        });
    }

    @Test
    public void testGetOrCreateColumnConflictingTypeFastPath() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                // First call creates the column as LONG
                table.getOrCreateColumn("x", QwpConstants.TYPE_LONG, false).addLong(1L);
                table.nextRow();

                // Second call with the same name but a different type hits the fast path
                // (sequential cursor matches the column name) and must throw
                try {
                    table.getOrCreateColumn("x", QwpConstants.TYPE_DOUBLE, false);
                    fail("Expected LineSenderException for column type mismatch");
                } catch (LineSenderException e) {
                    assertEquals(
                            "Column type mismatch for column 'x': columnType=" + QwpConstants.TYPE_LONG + ", sentType=" + QwpConstants.TYPE_DOUBLE,
                            e.getMessage()
                    );
                }
            }
        });
    }

    @Test
    public void testGetOrCreateColumnConflictingTypeSlowPath() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                // Create two columns so the fast-path cursor can be defeated
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(1L);
                table.getOrCreateColumn("b", QwpConstants.TYPE_STRING, true).addString("v");
                table.nextRow();

                // Access column "b" first — cursor now expects "a" at index 0,
                // but we ask for "b", so the fast path misses and falls through
                // to the hash-map lookup, which must detect the type conflict
                try {
                    table.getOrCreateColumn("b", QwpConstants.TYPE_LONG, false);
                    fail("Expected LineSenderException for column type mismatch");
                } catch (LineSenderException e) {
                    assertEquals(
                            "Column type mismatch for column 'b': columnType=" + QwpConstants.TYPE_STRING + ", sentType=" + QwpConstants.TYPE_LONG,
                            e.getMessage()
                    );
                }
            }
        });
    }

    @Test
    public void testLongArrayMultipleRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false);

                col.addLongArray(new long[]{10, 20, 30});
                table.nextRow();

                col.addLongArray(new long[]{40, 50, 60});
                table.nextRow();

                col.addLongArray(new long[]{70, 80, 90});
                table.nextRow();

                assertEquals(3, col.getValueCount());
                long[] encoded = readLongArraysLikeEncoder(col);
                assertArrayEquals(
                        new long[]{10, 20, 30, 40, 50, 60, 70, 80, 90},
                        encoded
                );

                byte[] dims = col.getArrayDims();
                int[] shapes = col.getArrayShapes();
                for (int i = 0; i < 3; i++) {
                    assertEquals(1, dims[i]);
                    assertEquals(3, shapes[i]);
                }
            }
        });
    }

    @Test
    public void testLongArrayShrinkingSize() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false);

                // Row 0: large array (4 elements)
                col.addLongArray(new long[]{100, 200, 300, 400});
                table.nextRow();

                // Row 1: smaller array (2 elements) — must not see leftover data from row 0
                col.addLongArray(new long[]{10, 20});
                table.nextRow();

                assertEquals(2, col.getValueCount());
                long[] encoded = readLongArraysLikeEncoder(col);
                assertArrayEquals(new long[]{100, 200, 300, 400, 10, 20}, encoded);

                int[] shapes = col.getArrayShapes();
                assertEquals(4, shapes[0]);
                assertEquals(2, shapes[1]);
            }
        });
    }

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
}
