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

package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.cairo.CairoException;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.cutlass.qwp.protocol.OffHeapAppendMemory;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.*;

public class QwpTableBufferTest {

    @Test
    public void testAddBinaryEmptyArrayIsZeroLengthValue() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("b", QwpConstants.TYPE_BINARY, true);
                col.addBinary(new byte[0]);
                table.nextRow();

                assertEquals(1, col.getSize());
                assertEquals(1, col.getValueCount());
                assertFalse(col.isNull(0));
                // Zero bytes appended, so the cumulative offset for value[1] is still 0.
                assertEquals(0L, col.getStringDataSize());
                // Offset array seeded with [0, 0] -- 8 bytes total.
                long offsetsAddr = col.getStringOffsetsAddress();
                assertEquals(0, Unsafe.getUnsafe().getInt(offsetsAddr));
                assertEquals(0, Unsafe.getUnsafe().getInt(offsetsAddr + 4));
            }
        });
    }

    @Test
    public void testAddBinaryRejectsNullOnNonNullableColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("b", QwpConstants.TYPE_BINARY, false);
                try {
                    col.addBinary(null);
                    fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("BINARY value cannot be null"));
                }
            }
        });
    }

    @Test
    public void testAddBinaryRoundTripsBytesAndOffsets() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("b", QwpConstants.TYPE_BINARY, true);
                byte[] r0 = {0x00, 0x7F, (byte) 0x80, (byte) 0xFF};
                byte[] r1 = {(byte) 0xCA, (byte) 0xFE};
                col.addBinary(r0);
                table.nextRow();
                col.addBinary(r1);
                table.nextRow();

                assertEquals(2, col.getSize());
                assertEquals(2, col.getValueCount());
                assertEquals(r0.length + r1.length, col.getStringDataSize());

                long offsetsAddr = col.getStringOffsetsAddress();
                assertEquals(0, Unsafe.getUnsafe().getInt(offsetsAddr));
                assertEquals(r0.length, Unsafe.getUnsafe().getInt(offsetsAddr + 4));
                assertEquals(r0.length + r1.length, Unsafe.getUnsafe().getInt(offsetsAddr + 8));

                long dataAddr = col.getStringDataAddress();
                for (int i = 0; i < r0.length; i++) {
                    assertEquals(r0[i], Unsafe.getUnsafe().getByte(dataAddr + i));
                }
                for (int i = 0; i < r1.length; i++) {
                    assertEquals(r1[i], Unsafe.getUnsafe().getByte(dataAddr + r0.length + i));
                }
            }
        });
    }

    @Test
    public void testAddBinaryFromNativePointer() throws Exception {
        assertMemoryLeak(() -> {
            byte[] payload = {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
            long ptr = copyToNative(payload);
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("b", QwpConstants.TYPE_BINARY, true);
                col.addBinary(ptr, payload.length);
                table.nextRow();

                assertEquals(1, col.getSize());
                assertEquals(1, col.getValueCount());
                assertEquals(payload.length, col.getStringDataSize());

                long offsetsAddr = col.getStringOffsetsAddress();
                assertEquals(0, Unsafe.getUnsafe().getInt(offsetsAddr));
                assertEquals(payload.length, Unsafe.getUnsafe().getInt(offsetsAddr + 4));

                long dataAddr = col.getStringDataAddress();
                for (int i = 0; i < payload.length; i++) {
                    assertEquals(payload[i], Unsafe.getUnsafe().getByte(dataAddr + i));
                }
            } finally {
                Unsafe.free(ptr, payload.length, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testAddBinaryFromNativePointerRejectsBadArgs() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("b", QwpConstants.TYPE_BINARY, true);
                try {
                    col.addBinary(0L, -1L);
                    fail("Expected LineSenderException for negative length");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("non-negative"));
                }
                try {
                    col.addBinary(0L, 5L);
                    fail("Expected LineSenderException for null pointer with non-zero length");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("pointer cannot be 0"));
                }
                // ptr == 0 && len == 0 is fine: writes a zero-length entry.
                col.addBinary(0L, 0L);
                table.nextRow();
                assertEquals(1, col.getValueCount());
            }
        });
    }

    @Test
    public void testAddBinaryWritesNullViaBitmap() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("b", QwpConstants.TYPE_BINARY, true);
                col.addBinary(new byte[]{1, 2, 3});
                table.nextRow();
                col.addBinary(null);
                table.nextRow();
                col.addBinary(new byte[]{4});
                table.nextRow();

                assertEquals(3, col.getSize());
                assertEquals(2, col.getValueCount());
                assertFalse(col.isNull(0));
                assertTrue(col.isNull(1));
                assertFalse(col.isNull(2));

                long offsetsAddr = col.getStringOffsetsAddress();
                assertEquals(0, Unsafe.getUnsafe().getInt(offsetsAddr));
                assertEquals(3, Unsafe.getUnsafe().getInt(offsetsAddr + 4));
                assertEquals(4, Unsafe.getUnsafe().getInt(offsetsAddr + 8));
            }
        });
    }

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
    public void testAddDoubleArrayPayloadSupportsHigherDimensionalShape() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test");
                 DoubleArray array = new DoubleArray(2, 1, 1, 2);
                 OffHeapAppendMemory payload = new OffHeapAppendMemory(128)) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("arr", QwpConstants.TYPE_DOUBLE_ARRAY, false);

                array.append(1.0).append(2.0).append(3.0).append(4.0);
                array.appendToBufPtr(payload);

                col.addDoubleArrayPayload(payload.pageAddress(), payload.getAppendOffset());
                table.nextRow();

                assertEquals(1, col.getValueCount());

                byte[] dims = col.getArrayDims();
                int[] shapes = col.getArrayShapes();
                assertEquals(4, dims[0]);
                assertEquals(2, shapes[0]);
                assertEquals(1, shapes[1]);
                assertEquals(1, shapes[2]);
                assertEquals(2, shapes[3]);

                assertArrayEquals(new double[]{1.0, 2.0, 3.0, 4.0}, readDoubleArraysLikeEncoder(col), 0.0);
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
    public void testAddSymbolUtf8CancelRowRewindsDictionary() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(0);
                table.nextRow();

                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(1);
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true);
                addSymbolUtf8(col, "stale");
                table.cancelCurrentRow();

                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(1);
                addSymbolUtf8(col, "fresh");
                table.nextRow();

                assertEquals(2, col.getSize());
                assertEquals(1, col.getValueCount());
                assertArrayEquals(new String[]{"fresh"}, col.getSymbolDictionary());
                assertEquals(0, Unsafe.getUnsafe().getInt(col.getDataAddress()));
            }
        });
    }

    @Test
    public void testAddSymbolUtf8RejectsInvalidUtf8() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, true);
                byte[] invalid = {(byte) 0xC3, 0x28};
                long ptr = copyToNative(invalid);
                try {
                    try {
                        col.addSymbolUtf8(ptr, invalid.length);
                        fail("Expected CairoException");
                    } catch (CairoException ex) {
                        assertTrue(ex.getFlyweightMessage().toString().contains("cannot convert invalid UTF-8 sequence"));
                    }
                    assertEquals(0, col.getSize());
                    assertEquals(0, col.getValueCount());
                    assertEquals(0, col.getSymbolDictionarySize());
                } finally {
                    Unsafe.free(ptr, invalid.length, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testAddSymbolUtf8ReusesExistingDictionaryEntry() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, true);
                addSymbolUtf8(col, "東京");
                table.nextRow();

                addSymbolUtf8(col, "東京");
                table.nextRow();

                addSymbolUtf8(col, "Αθηνα");
                table.nextRow();

                assertEquals(3, col.getSize());
                assertEquals(3, col.getValueCount());
                assertEquals(2, col.getSymbolDictionarySize());
                assertArrayEquals(new String[]{"東京", "Αθηνα"}, col.getSymbolDictionary());

                long dataAddress = col.getDataAddress();
                assertEquals(0, Unsafe.getUnsafe().getInt(dataAddress));
                assertEquals(0, Unsafe.getUnsafe().getInt(dataAddress + 4));
                assertEquals(1, Unsafe.getUnsafe().getInt(dataAddress + 8));
            }
        });
    }

    @Test
    public void testAddSymbolWithGlobalIdStoresOnlyGlobalIds() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, true);
                col.addSymbolWithGlobalId("alpha", 7);
                table.nextRow();
                col.addSymbolWithGlobalId("beta", 11);
                table.nextRow();

                assertEquals(2, col.getSize());
                assertEquals(2, col.getValueCount());
                assertEquals(0, col.getSymbolDictionarySize());
                assertEquals(0, col.getAuxDataAddress());
                assertEquals(11, col.getMaxGlobalSymbolId());

                long dataAddress = col.getDataAddress();
                assertEquals(7, Unsafe.getUnsafe().getInt(dataAddress));
                assertEquals(11, Unsafe.getUnsafe().getInt(dataAddress + Integer.BYTES));
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
    public void testCancelRowRetainsGlobalSymbolIdWithoutLocalDictionary() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(0);
                table.nextRow();

                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(1);
                QwpTableBuffer.ColumnBuffer colS = table.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true);
                colS.addSymbolWithGlobalId("stale", 4);
                table.cancelCurrentRow();

                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(1);
                colS.addSymbolWithGlobalId("fresh", 9);
                table.nextRow();

                assertEquals(2, colS.getSize());
                assertEquals(1, colS.getValueCount());
                assertEquals(0, colS.getSymbolDictionarySize());
                assertEquals(9, colS.getMaxGlobalSymbolId());
                assertEquals(9, Unsafe.getUnsafe().getInt(colS.getDataAddress()));
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
    public void testCancelRowTruncatesLateAddedColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                // Commit 3 rows with columns "a" (LONG, non-nullable) and "b" (STRING, nullable)
                for (int i = 0; i < 3; i++) {
                    table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(i);
                    table.getOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true).addString("v" + i);
                    table.nextRow();
                }

                // Start row 4: set "a" and "b", then create a NEW column "c"
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(3);
                table.getOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true).addString("v3");
                QwpTableBuffer.ColumnBuffer colC = table.getOrCreateColumn("c", QwpConstants.TYPE_VARCHAR, true);
                colC.addString("stale");

                // Cancel the in-progress row
                table.cancelCurrentRow();

                // Column "c" was created during the in-progress row, so it must be fully cleared
                assertEquals(0, colC.getSize());
                assertEquals(0, colC.getValueCount());

                // Start row 4 again: set "a" and "b" only (not "c")
                table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false).addLong(3);
                table.getOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true).addString("v3");
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
                QwpTableBuffer.ColumnBuffer colC = table.getOrCreateColumn("c", QwpConstants.TYPE_VARCHAR, true);
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
    public void testGetExistingColumnReturnsNullWithoutCreatingColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer colA = table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false);
                colA.addLong(1);
                table.nextRow();

                assertNull(table.getExistingColumn("missing", QwpConstants.TYPE_VARCHAR));
                assertEquals(1, table.getColumnCount());

                QwpTableBuffer.ColumnBuffer colB = table.getOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true);
                assertNotNull(colB);
                assertEquals(2, table.getColumnCount());
            }
        });
    }

    @Test
    public void testGetExistingColumnReturnsOrderedColumnsAcrossRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer colA = table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false);
                QwpTableBuffer.ColumnBuffer colB = table.getOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true);
                colA.addLong(1);
                colB.addString("x");
                table.nextRow();

                QwpTableBuffer.ColumnBuffer existingA = table.getExistingColumn("a", QwpConstants.TYPE_LONG);
                QwpTableBuffer.ColumnBuffer existingB = table.getExistingColumn("b", QwpConstants.TYPE_VARCHAR);

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
                QwpTableBuffer.ColumnBuffer colB = table.getOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true);
                colA.addLong(1);
                colB.addString("x");
                table.nextRow();

                QwpTableBuffer.ColumnBuffer existingB = table.getExistingColumn("b", QwpConstants.TYPE_VARCHAR);
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
    public void testGetExistingColumnTypeMismatchOnHashPathThrows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer colA = table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false);
                QwpTableBuffer.ColumnBuffer colB = table.getOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true);
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
    public void testGetExistingColumnTypeMismatchOnOrderedPathThrows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer colA = table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false);
                table.getOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true);
                colA.addLong(1);
                table.nextRow();

                try {
                    table.getExistingColumn("a", QwpConstants.TYPE_VARCHAR);
                    fail("Expected LineSenderException for ordered-path type mismatch");
                } catch (LineSenderException e) {
                    assertTrue(e.getMessage().contains("Column type mismatch"));
                    assertTrue(e.getMessage().contains("column 'a'"));
                }
            }
        });
    }

    @Test
    public void testGetExistingColumnWorksAfterReset() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer colA = table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false);
                QwpTableBuffer.ColumnBuffer colB = table.getOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true);
                colA.addLong(1);
                colB.addString("x");
                table.nextRow();

                table.reset();

                QwpTableBuffer.ColumnBuffer existingA = table.getExistingColumn("a", QwpConstants.TYPE_LONG);
                QwpTableBuffer.ColumnBuffer existingB = table.getExistingColumn("b", QwpConstants.TYPE_VARCHAR);

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
                QwpTableBuffer.ColumnBuffer late = table.getOrCreateColumn("late", QwpConstants.TYPE_VARCHAR, true);
                late.addString("stale");
                table.cancelCurrentRow();

                QwpTableBuffer.ColumnBuffer existingLate = table.getExistingColumn("late", QwpConstants.TYPE_VARCHAR);
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
                table.getOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true).addString("v");
                table.nextRow();

                // Access column "b" first — cursor now expects "a" at index 0,
                // but we ask for "b", so the fast path misses and falls through
                // to the hash-map lookup, which must detect the type conflict
                try {
                    table.getOrCreateColumn("b", QwpConstants.TYPE_LONG, false);
                    fail("Expected LineSenderException for column type mismatch");
                } catch (LineSenderException e) {
                    assertEquals(
                            "Column type mismatch for column 'b': columnType=" + QwpConstants.TYPE_VARCHAR + ", sentType=" + QwpConstants.TYPE_LONG,
                            e.getMessage()
                    );
                }
            }
        });
    }

    @Test
    public void testGetOrCreateColumnThrowsWhenExceedingMaxColumnCount() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                for (int i = 0; i < QwpConstants.MAX_COLUMNS_PER_TABLE; i++) {
                    table.getOrCreateColumn("c" + i, QwpConstants.TYPE_LONG, false);
                }
                assertEquals(QwpConstants.MAX_COLUMNS_PER_TABLE, table.getColumnCount());
                try {
                    table.getOrCreateColumn("overflow", QwpConstants.TYPE_LONG, false);
                    fail("Expected LineSenderException for exceeding max column count");
                } catch (LineSenderException e) {
                    assertEquals(
                            "column count exceeds maximum: " + (QwpConstants.MAX_COLUMNS_PER_TABLE + 1)
                                    + " (max " + QwpConstants.MAX_COLUMNS_PER_TABLE + ")",
                            e.getMessage()
                    );
                }
                assertEquals(QwpConstants.MAX_COLUMNS_PER_TABLE, table.getColumnCount());
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
    public void testLongArrayWrapperMultipleRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test");
                 LongArray arr = new LongArray(3)) {
                QwpTableBuffer.ColumnBuffer col = table.getOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, false);

                arr.append(10).append(20).append(30);
                col.addLongArray(arr);
                table.nextRow();

                arr.append(40).append(50).append(60);
                col.addLongArray(arr);
                table.nextRow();

                arr.append(70).append(80).append(90);
                col.addLongArray(arr);
                table.nextRow();

                assertEquals(3, col.getValueCount());
                long[] encoded = readLongArraysLikeEncoder(col);
                assertArrayEquals(new long[]{10, 20, 30, 40, 50, 60, 70, 80, 90}, encoded);

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
    public void testNextRowWithPreparedMissingColumnsPadsListedColumns() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer colA = table.getOrCreateColumn("a", QwpConstants.TYPE_LONG, false);
                QwpTableBuffer.ColumnBuffer colB = table.getOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true);
                QwpTableBuffer.ColumnBuffer colC = table.getOrCreateColumn("c", QwpConstants.TYPE_LONG, false);

                colA.addLong(10);
                colB.addString("x");
                colC.addLong(100);
                table.nextRow();

                colA.addLong(20);
                table.nextRow(new QwpTableBuffer.ColumnBuffer[]{colB, colC}, 2);

                assertEquals(2, colA.getSize());
                assertEquals(2, colA.getValueCount());
                assertEquals(2, colB.getSize());
                assertEquals(1, colB.getValueCount());
                assertFalse(colB.isNull(0));
                assertTrue(colB.isNull(1));
                assertEquals(2, colC.getSize());
                assertEquals(2, colC.getValueCount());
                assertEquals(100L, Unsafe.getUnsafe().getLong(colC.getDataAddress()));
                assertEquals(Long.MIN_VALUE, Unsafe.getUnsafe().getLong(colC.getDataAddress() + Long.BYTES));
            }
        });
    }

    @Test
    public void testNonAsciiColumnNameCaseInsensitive() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                // Row 1: create columns with non-ASCII names (Cyrillic)
                QwpTableBuffer.ColumnBuffer col1 = table.getOrCreateColumn(
                        "Температура",
                        QwpConstants.TYPE_DOUBLE, true);
                col1.addDouble(42.0);

                QwpTableBuffer.ColumnBuffer col2 = table.getOrCreateColumn(
                        "Straße", QwpConstants.TYPE_LONG, true);
                col2.addLong(100);

                table.nextRow();

                // Row 2: same column names in different case should resolve
                // to the same columns via the hash map slow path
                QwpTableBuffer.ColumnBuffer col3 = table.getOrCreateColumn(
                        "температура",
                        QwpConstants.TYPE_DOUBLE, true);
                assertSame(col1, col3);
                col3.addDouble(99.0);

                QwpTableBuffer.ColumnBuffer col4 = table.getOrCreateColumn(
                        "straße", QwpConstants.TYPE_LONG, true);
                assertSame(col2, col4);
                col4.addLong(200);

                table.nextRow();

                assertEquals(2, table.getColumnCount());
                assertEquals(2, table.getRowCount());
            }
        });
    }

    @Test
    public void testRetainInProgressRowFastClearsUnstagedNullableColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer table = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer keep = table.getOrCreateColumn("keep", QwpConstants.TYPE_LONG, false);
                QwpTableBuffer.ColumnBuffer drop = table.getOrCreateColumn("drop", QwpConstants.TYPE_VARCHAR, true);

                for (int i = 0; i < 130; i++) {
                    keep.addLong(i);
                    if ((i & 1) == 0) {
                        drop.addString("v" + i);
                    } else {
                        drop.addNull();
                    }
                    table.nextRow();
                }

                int keepSizeBefore = keep.getSize();
                int keepValueCountBefore = keep.getValueCount();
                int keepArrayShapeOffsetBefore = keep.getArrayShapeOffset();
                int keepArrayDataOffsetBefore = keep.getArrayDataOffset();
                int keepIndex = keep.getIndex();

                keep.addLong(130);

                int[] sizeBefore = {-1, -1};
                int[] valueCountBefore = {-1, -1};
                int[] arrayShapeOffsetBefore = new int[2];
                int[] arrayDataOffsetBefore = new int[2];

                sizeBefore[keepIndex] = keepSizeBefore;
                valueCountBefore[keepIndex] = keepValueCountBefore;
                arrayShapeOffsetBefore[keepIndex] = keepArrayShapeOffsetBefore;
                arrayDataOffsetBefore[keepIndex] = keepArrayDataOffsetBefore;

                table.retainInProgressRow(
                        sizeBefore,
                        valueCountBefore,
                        arrayShapeOffsetBefore,
                        arrayDataOffsetBefore
                );

                assertEquals(0, table.getRowCount());

                assertEquals(1, keep.getSize());
                assertEquals(1, keep.getValueCount());
                assertEquals(130L, Unsafe.getUnsafe().getLong(keep.getDataAddress()));

                assertEquals(0, drop.getSize());
                assertEquals(0, drop.getValueCount());
                assertEquals(0, drop.getStringDataSize());
                assertFalse(drop.isNull(0));
                assertFalse(drop.isNull(63));
                assertFalse(drop.isNull(64));
                assertFalse(drop.isNull(129));
                assertEquals(0, Unsafe.getUnsafe().getLong(drop.getNullBitmapAddress()));
                assertEquals(0, Unsafe.getUnsafe().getLong(drop.getNullBitmapAddress() + Long.BYTES));
                assertEquals(0, Unsafe.getUnsafe().getLong(drop.getNullBitmapAddress() + 2L * Long.BYTES));
                assertEquals(0, Unsafe.getUnsafe().getInt(drop.getStringOffsetsAddress()));
            }
        });
    }

    private static void addSymbolUtf8(QwpTableBuffer.ColumnBuffer col, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        long ptr = copyToNative(bytes);
        try {
            col.addSymbolUtf8(ptr, bytes.length);
        } finally {
            Unsafe.free(ptr, bytes.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static long copyToNative(byte[] bytes) {
        long ptr = Unsafe.malloc(bytes.length, MemoryTag.NATIVE_DEFAULT);
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(ptr + i, bytes[i]);
        }
        return ptr;
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
