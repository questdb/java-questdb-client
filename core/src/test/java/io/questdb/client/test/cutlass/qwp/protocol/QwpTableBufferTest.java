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
    public void testCancelRowTruncatesLateAddedColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                // Commit 3 rows with columns "a" (LONG, non-nullable) and "b" (STRING, nullable)
                for (int i = 0; i < 3; i++) {
                    table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(i);
                    table.getOrCreateColumn("b", QwpConstants.TYPE_STRING, true).addString("v" + i);
                    table.nextRow();
                }

                // Start row 4: set "a" and "b", then create a NEW column "c"
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(3);
                table.getOrCreateColumn("b", QwpConstants.TYPE_STRING, true).addString("v3");
                QwpTableBuffer.ColumnBuffer colC = table.getOrCreateColumn("c", QwpConstants.TYPE_STRING, true);
                colC.addString("stale");

                // Cancel the in-progress row
                table.cancelCurrentRow();

                // Column "c" was created during the in-progress row, so it must be fully cleared
                assertEquals(0, colC.getSize());
                assertEquals(0, colC.getValueCount());

                // Start row 4 again: set "a" and "b" only (not "c")
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(3);
                table.getOrCreateColumn("b", QwpConstants.TYPE_STRING, true).addString("v3");
                table.nextRow();

                // Column "c" should now have size == 4 (padded with nulls) and valueCount == 0
                assertEquals(4, colC.getSize());
                assertEquals(0, colC.getValueCount());

                // All 4 rows of column "c" should be null
                for (int i = 0; i < 4; i++) {
                    assertTrue("row " + i + " of column c should be null", colC.isNull(i));
                }
            }
        });
    }

    @Test
    public void testCancelRowTruncatesLateAddedColumnWhenSizeEqualsRowCount() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                // Commit exactly 1 row so rowCount == 1
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(0);
                table.nextRow();

                // Start row 2: set "a", then create NEW column "c" with one value
                // col_c.size will be 1, which equals rowCount — the edge case
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(1);
                QwpTableBuffer.ColumnBuffer colC = table.getOrCreateColumn("c", QwpConstants.TYPE_STRING, true);
                colC.addString("stale");

                // Cancel the in-progress row
                table.cancelCurrentRow();

                // Column "c" had size == rowCount (1 == 1) but was still late-added
                assertEquals(0, colC.getSize());
                assertEquals(0, colC.getValueCount());

                // Start row 2 again without setting "c"
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(1);
                table.nextRow();

                // Column "c" should have 2 null rows
                assertEquals(2, colC.getSize());
                assertEquals(0, colC.getValueCount());
                assertTrue(colC.isNull(0));
                assertTrue(colC.isNull(1));
            }
        });
    }

    @Test
    public void testNextRowWithPreparedMissingColumnsPadsListedColumns() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer colA = table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false);
                QwpTableBuffer.ColumnBuffer colB = table.getOrCreateColumn("b", QwpConstants.TYPE_STRING, true);

                colA.addLong(10);
                colB.addString("x");
                table.nextRow();

                colA.addLong(20);
                table.nextRow(new QwpTableBuffer.ColumnBuffer[]{colB}, 1);

                assertEquals(2, colA.getSize());
                assertEquals(2, colA.getValueCount());
                assertEquals(2, colB.getSize());
                assertEquals(1, colB.getValueCount());
                assertFalse(colB.isNull(0));
                assertTrue(colB.isNull(1));
            }
        });
    }

    @Test
    public void testCancelRowResetsDecimalScaleOnLateAddedColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(0);
                table.nextRow();

                // Start row 2: create a decimal column with scale 5
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(1);
                QwpTableBuffer.ColumnBuffer colD = table.getOrCreateColumn("d", QwpConstants.TYPE_DECIMAL64, true);
                colD.addDecimal64(Decimal64.fromLong(100, 5));
                table.cancelCurrentRow();

                // After cancel, decimalScale must be reset. Adding a value at scale 3
                // should succeed and use scale 3 as the column's scale.
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(1);
                colD.addDecimal64(Decimal64.fromLong(42, 3));
                table.nextRow();

                assertEquals(2, colD.getSize());
                assertEquals(1, colD.getValueCount());
            }
        });
    }

    @Test
    public void testCancelRowResetsGeohashPrecisionOnLateAddedColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(0);
                table.nextRow();

                // Start row 2: create a geohash column with 20-bit precision
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(1);
                QwpTableBuffer.ColumnBuffer colG = table.getOrCreateColumn("g", QwpConstants.TYPE_GEOHASH, true);
                colG.addGeoHash(123L, 20);
                table.cancelCurrentRow();

                // After cancel, geohashPrecision must be reset. Adding a value at
                // 30-bit precision should succeed without a precision mismatch error.
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(1);
                colG.addGeoHash(456L, 30);
                table.nextRow();

                assertEquals(2, colG.getSize());
                assertEquals(1, colG.getValueCount());
            }
        });
    }

    @Test
    public void testCancelRowResetsSymbolDictOnLateAddedColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(0);
                table.nextRow();

                // Start row 2: create a symbol column with value "stale"
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(1);
                QwpTableBuffer.ColumnBuffer colS = table.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true);
                colS.addSymbol("stale");
                table.cancelCurrentRow();

                // After cancel, symbol dictionary must be empty.
                // "fresh" should get local ID 0, not 1.
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(1);
                colS.addSymbol("fresh");
                table.nextRow();

                assertEquals(2, colS.getSize());
                assertEquals(1, colS.getValueCount());
                String[] dict = colS.getSymbolDictionary();
                assertEquals(1, dict.length);
                assertEquals("fresh", dict[0]);
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

    @Test
    public void testGetExistingColumnReturnsOrderedColumnsAcrossRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer colA = table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false);
                QwpTableBuffer.ColumnBuffer colB = table.getOrCreateColumn("b", QwpConstants.TYPE_STRING, true);
                colA.addLong(1);
                colB.addString("x");
                table.nextRow();

                QwpTableBuffer.ColumnBuffer existingA = table.getExistingColumn("a", QwpConstants.TYPE_LONG);
                QwpTableBuffer.ColumnBuffer existingB = table.getExistingColumn("b", QwpConstants.TYPE_STRING);

                assertSame(colA, existingA);
                assertSame(colB, existingB);

                existingA.addLong(2);
                existingB.addString("y");
                table.nextRow();

                assertEquals(2, table.getRowCount());
                assertEquals(2, colA.getSize());
                assertEquals(2, colA.getValueCount());
                assertEquals(2, colB.getSize());
                assertEquals(2, colB.getValueCount());
            }
        });
    }

    @Test
    public void testGetExistingColumnReturnsOutOfOrderColumns() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer colA = table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false);
                QwpTableBuffer.ColumnBuffer colB = table.getOrCreateColumn("b", QwpConstants.TYPE_STRING, true);
                colA.addLong(1);
                colB.addString("x");
                table.nextRow();

                QwpTableBuffer.ColumnBuffer existingB = table.getExistingColumn("b", QwpConstants.TYPE_STRING);
                QwpTableBuffer.ColumnBuffer existingA = table.getExistingColumn("a", QwpConstants.TYPE_LONG);

                assertSame(colB, existingB);
                assertSame(colA, existingA);

                existingB.addString("y");
                existingA.addLong(2);
                table.nextRow();

                assertEquals(2, table.getRowCount());
                assertEquals(2, colA.getSize());
                assertEquals(2, colA.getValueCount());
                assertEquals(2, colB.getSize());
                assertEquals(2, colB.getValueCount());
            }
        });
    }

    @Test
    public void testGetExistingColumnReturnsNullWithoutCreatingColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer colA = table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false);
                colA.addLong(1);
                table.nextRow();

                assertNull(table.getExistingColumn("missing", QwpConstants.TYPE_STRING));
                assertEquals(1, table.getColumnCount());

                QwpTableBuffer.ColumnBuffer colB = table.getOrCreateColumn("b", QwpConstants.TYPE_STRING, true);
                assertNotNull(colB);
                assertEquals(2, table.getColumnCount());
            }
        });
    }

    @Test
    public void testGetExistingColumnTypeMismatchOnOrderedPathThrows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer colA = table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false);
                table.getOrCreateColumn("b", QwpConstants.TYPE_STRING, true);
                colA.addLong(1);
                table.nextRow();

                try {
                    table.getExistingColumn("a", QwpConstants.TYPE_STRING);
                    fail("Expected LineSenderException for ordered-path type mismatch");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("Column type mismatch"));
                    assertTrue(e.getMessage().contains("column 'a'"));
                }
            }
        });
    }

    @Test
    public void testGetExistingColumnTypeMismatchOnHashPathThrows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer colA = table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false);
                QwpTableBuffer.ColumnBuffer colB = table.getOrCreateColumn("b", QwpConstants.TYPE_STRING, true);
                colA.addLong(1);
                colB.addString("x");
                table.nextRow();

                try {
                    table.getExistingColumn("b", QwpConstants.TYPE_LONG);
                    fail("Expected LineSenderException for hash-path type mismatch");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("Column type mismatch"));
                    assertTrue(e.getMessage().contains("column 'b'"));
                }
            }
        });
    }

    @Test
    public void testGetExistingColumnWorksAfterReset() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer colA = table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false);
                QwpTableBuffer.ColumnBuffer colB = table.getOrCreateColumn("b", QwpConstants.TYPE_STRING, true);
                colA.addLong(1);
                colB.addString("x");
                table.nextRow();

                table.reset();

                QwpTableBuffer.ColumnBuffer existingA = table.getExistingColumn("a", QwpConstants.TYPE_LONG);
                QwpTableBuffer.ColumnBuffer existingB = table.getExistingColumn("b", QwpConstants.TYPE_STRING);

                assertSame(colA, existingA);
                assertSame(colB, existingB);

                existingA.addLong(2);
                existingB.addString("y");
                table.nextRow();

                assertEquals(1, table.getRowCount());
                assertEquals(1, colA.getSize());
                assertEquals(1, colA.getValueCount());
                assertEquals(1, colB.getSize());
                assertEquals(1, colB.getValueCount());
            }
        });
    }

    @Test
    public void testGetExistingColumnWorksForLateAddedColumnAfterCancelRow() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(1);
                table.nextRow();

                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(2);
                QwpTableBuffer.ColumnBuffer late = table.getOrCreateColumn("late", QwpConstants.TYPE_STRING, true);
                late.addString("stale");
                table.cancelCurrentRow();

                QwpTableBuffer.ColumnBuffer existingLate = table.getExistingColumn("late", QwpConstants.TYPE_STRING);
                assertSame(late, existingLate);
                assertEquals(0, existingLate.getSize());
                assertEquals(0, existingLate.getValueCount());

                table.getExistingColumn("a", QwpConstants.TYPE_LONG).addLong(2);
                table.nextRow();

                assertEquals(2, table.getRowCount());
                assertEquals(2, existingLate.getSize());
                assertEquals(0, existingLate.getValueCount());
                assertTrue(existingLate.isNull(0));
                assertTrue(existingLate.isNull(1));
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
