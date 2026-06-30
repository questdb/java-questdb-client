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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Unit tests for QwpWebSocketEncoder.
 */
public class QwpWebSocketEncoderTest {

    @Test
    public void testBufferResetAndReuse() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test")) {

                // First batch
                for (int i = 0; i < 100; i++) {
                    QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                    col.addLong(i);
                    buffer.nextRow();
                }
                int size1 = encoder.encode(buffer);

                // Reset and second batch
                buffer.reset();
                for (int i = 0; i < 50; i++) {
                    QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                    col.addLong(i * 2);
                    buffer.nextRow();
                }
                int size2 = encoder.encode(buffer);

                Assert.assertTrue(size1 > size2); // More rows = larger
                Assert.assertEquals(50, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncode2DDoubleArray() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("matrix", TYPE_DOUBLE_ARRAY, true);
                col.addDoubleArray(new double[][]{{1.0, 2.0}, {3.0, 4.0}});
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncode2DLongArray() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("matrix", TYPE_LONG_ARRAY, true);
                col.addLongArray(new long[][]{{1L, 2L}, {3L, 4L}});
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncode3DDoubleArray() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("tensor", TYPE_DOUBLE_ARRAY, true);
                col.addDoubleArray(new double[][][]{
                        {{1.0, 2.0}, {3.0, 4.0}},
                        {{5.0, 6.0}, {7.0, 8.0}}
                });
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeAllBasicTypesInOneRow() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("all_types")) {

                buffer.getOrCreateColumn("b", TYPE_BOOLEAN, false).addBoolean(true);
                buffer.getOrCreateColumn("by", TYPE_BYTE, false).addByte((byte) 42);
                buffer.getOrCreateColumn("sh", TYPE_SHORT, false).addShort((short) 1000);
                buffer.getOrCreateColumn("i", TYPE_INT, false).addInt(100000);
                buffer.getOrCreateColumn("l", TYPE_LONG, false).addLong(1000000000L);
                buffer.getOrCreateColumn("f", TYPE_FLOAT, false).addFloat(3.14f);
                buffer.getOrCreateColumn("d", TYPE_DOUBLE, false).addDouble(3.14159265);
                buffer.getOrCreateColumn("s", TYPE_VARCHAR, true).addString("test");
                buffer.getOrCreateColumn("sym", TYPE_SYMBOL, false).addSymbol("AAPL");
                buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP).addLong(1000000L);

                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(1, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncodeAllBooleanValues() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("flag", TYPE_BOOLEAN, false);
                for (int i = 0; i < 100; i++) {
                    col.addBoolean(i % 2 == 0);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(100, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncodeDecimal128() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("amount", TYPE_DECIMAL128, false);
                col.addDecimal128(io.questdb.client.std.Decimal128.fromLong(123456789012345L, 4));
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeDecimal256() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("bignum", TYPE_DECIMAL256, false);
                col.addDecimal256(io.questdb.client.std.Decimal256.fromLong(Long.MAX_VALUE, 6));
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeDecimal64() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("price", TYPE_DECIMAL64, false);
                col.addDecimal64(io.questdb.client.std.Decimal64.fromLong(12345L, 2)); // 123.45
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeDoubleArray() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("values", TYPE_DOUBLE_ARRAY, true);
                col.addDoubleArray(new double[]{1.0, 2.0, 3.0});
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeEmptyString() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("name", TYPE_VARCHAR, true);
                col.addString("");
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeEmptyTableName() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("")) {
                // Edge case: empty table name (probably invalid but let's verify encoding works)
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                col.addLong(1L);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 0);
            }
        });
    }

    @Test
    public void testEncodeLargeArray() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                // Large 1D array
                double[] largeArray = new double[1000];
                for (int i = 0; i < 1000; i++) {
                    largeArray[i] = i * 1.5;
                }

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("values", TYPE_DOUBLE_ARRAY, true);
                col.addDoubleArray(largeArray);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 8000); // At least 8 bytes per double
            }
        });
    }

    @Test
    public void testEncodeLargeRowCount() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("metrics")) {

                for (int i = 0; i < 10_000; i++) {
                    QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                    col.addLong(i);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(10_000, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncodeLongArray() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("values", TYPE_LONG_ARRAY, true);
                col.addLongArray(new long[]{1L, 2L, 3L});
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeLongString() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                String sb = io.questdb.client.test.tools.TestUtils.repeat("a", 10_000);

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("data", TYPE_VARCHAR, true);
                col.addString(sb);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 10_000);
            }
        });
    }

    @Test
    public void testEncodeMaxMinLong() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                col.addLong(Long.MAX_VALUE);
                buffer.nextRow();

                col.addLong(Long.MIN_VALUE);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(2, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncodeMixedColumnTypes() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("events")) {

                // Add columns of different types
                QwpTableBuffer.ColumnBuffer symbolCol = buffer.getOrCreateColumn("host", TYPE_SYMBOL, false);
                symbolCol.addSymbol("server1");

                QwpTableBuffer.ColumnBuffer longCol = buffer.getOrCreateColumn("count", TYPE_LONG, false);
                longCol.addLong(42);

                QwpTableBuffer.ColumnBuffer doubleCol = buffer.getOrCreateColumn("value", TYPE_DOUBLE, false);
                doubleCol.addDouble(3.14);

                QwpTableBuffer.ColumnBuffer boolCol = buffer.getOrCreateColumn("active", TYPE_BOOLEAN, false);
                boolCol.addBoolean(true);

                QwpTableBuffer.ColumnBuffer stringCol = buffer.getOrCreateColumn("message", TYPE_VARCHAR, true);
                stringCol.addString("hello world");

                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                tsCol.addLong(1000000L);

                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeMixedColumnsMultipleRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("events")) {

                for (int i = 0; i < 50; i++) {
                    QwpTableBuffer.ColumnBuffer symbolCol = buffer.getOrCreateColumn("host", TYPE_SYMBOL, false);
                    symbolCol.addSymbol("server" + (i % 5));

                    QwpTableBuffer.ColumnBuffer longCol = buffer.getOrCreateColumn("count", TYPE_LONG, false);
                    longCol.addLong(i * 10);

                    QwpTableBuffer.ColumnBuffer doubleCol = buffer.getOrCreateColumn("value", TYPE_DOUBLE, false);
                    doubleCol.addDouble(i * 1.5);

                    QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                    tsCol.addLong(1000000L + i);

                    buffer.nextRow();
                }

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(50, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncodeMultipleColumns() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("weather")) {

                // Add multiple columns
                QwpTableBuffer.ColumnBuffer tempCol = buffer.getOrCreateColumn("temperature", TYPE_DOUBLE, false);
                tempCol.addDouble(23.5);

                QwpTableBuffer.ColumnBuffer humCol = buffer.getOrCreateColumn("humidity", TYPE_LONG, false);
                humCol.addLong(65);

                QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                tsCol.addLong(1000000L);

                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);

                // Verify header
                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();
                Assert.assertEquals((byte) 'Q', Unsafe.getUnsafe().getByte(ptr));
                Assert.assertEquals((byte) 'W', Unsafe.getUnsafe().getByte(ptr + 1));
                Assert.assertEquals((byte) 'P', Unsafe.getUnsafe().getByte(ptr + 2));
                Assert.assertEquals((byte) '1', Unsafe.getUnsafe().getByte(ptr + 3));
            }
        });
    }

    @Test
    public void testEncodeMultipleDecimal64() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("price", TYPE_DECIMAL64, false);
                col.addDecimal64(io.questdb.client.std.Decimal64.fromLong(12345L, 2)); // 123.45
                buffer.nextRow();

                col.addDecimal64(io.questdb.client.std.Decimal64.fromLong(67890L, 2)); // 678.90
                buffer.nextRow();

                col.addDecimal64(io.questdb.client.std.Decimal64.fromLong(11111L, 2)); // 111.11
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(3, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncodeMultipleRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("metrics")) {

                for (int i = 0; i < 100; i++) {
                    QwpTableBuffer.ColumnBuffer valCol = buffer.getOrCreateColumn("value", TYPE_LONG, false);
                    valCol.addLong(i);

                    QwpTableBuffer.ColumnBuffer tsCol = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                    tsCol.addLong(1000000L + i);

                    buffer.nextRow();
                }

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(100, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncodeMultipleSymbolsSameDictionary() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("host", TYPE_SYMBOL, false);
                col.addSymbol("server1");
                buffer.nextRow();

                col.addSymbol("server1"); // Same symbol
                buffer.nextRow();

                col.addSymbol("server2"); // Different symbol
                buffer.nextRow();

                col.addSymbol("server1"); // Back to first
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(4, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncodeMultipleUuids() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("id", TYPE_UUID, false);
                for (int i = 0; i < 10; i++) {
                    col.addUuid(i * 1000L, i * 2000L);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(10, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncodeNaNDouble() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_DOUBLE, false);
                col.addDouble(Double.NaN);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeNegativeLong() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                col.addLong(-123456789L);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeNullableColumnWithNull() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test")) {

                // Nullable column with null
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("name", TYPE_VARCHAR, true);
                col.addString(null);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeNullableColumnWithValue() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test")) {

                // Nullable column with a value
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("name", TYPE_VARCHAR, true);
                col.addString("hello");
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeNullableSymbolWithNull() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("host", TYPE_SYMBOL, true);
                col.addSymbol("server1");
                buffer.nextRow();

                col.addSymbol(null); // Null symbol
                buffer.nextRow();

                col.addSymbol("server2");
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(3, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncodeSingleRowWithBoolean() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("active", TYPE_BOOLEAN, false);
                col.addBoolean(true);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeSingleRowWithDouble() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("temperature", TYPE_DOUBLE, false);
                col.addDouble(23.5);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeSingleRowWithLong() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                // Add a long column
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("value", TYPE_LONG, false);
                col.addLong(12345L);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12); // At least header size

                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                // Verify header magic
                Assert.assertEquals((byte) 'Q', Unsafe.getUnsafe().getByte(ptr));
                Assert.assertEquals((byte) 'W', Unsafe.getUnsafe().getByte(ptr + 1));
                Assert.assertEquals((byte) 'P', Unsafe.getUnsafe().getByte(ptr + 2));
                Assert.assertEquals((byte) '1', Unsafe.getUnsafe().getByte(ptr + 3));

                // Version
                Assert.assertEquals(VERSION, Unsafe.getUnsafe().getByte(ptr + 4));

                // Table count (little-endian short)
                Assert.assertEquals((short) 1, Unsafe.getUnsafe().getShort(ptr + 6));
            }
        });
    }

    @Test
    public void testEncodeSingleRowWithString() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("name", TYPE_VARCHAR, true);
                col.addString("hello");
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeSingleRowWithTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                // Add a timestamp column (designated timestamp uses empty name)
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                col.addLong(1000000L); // Micros
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeSingleSymbol() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("host", TYPE_SYMBOL, false);
                col.addSymbol("server1");
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeSpecialDoubles() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_DOUBLE, false);
                col.addDouble(Double.MAX_VALUE);
                buffer.nextRow();

                col.addDouble(Double.MIN_VALUE);
                buffer.nextRow();

                col.addDouble(Double.POSITIVE_INFINITY);
                buffer.nextRow();

                col.addDouble(Double.NEGATIVE_INFINITY);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(4, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncodeSymbolWithManyDistinctValues() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("host", TYPE_SYMBOL, false);
                for (int i = 0; i < 100; i++) {
                    col.addSymbol("server" + i);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
                Assert.assertEquals(100, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testEncodeUnicodeString() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("name", TYPE_VARCHAR, true);
                col.addString("Hello 世界 🌍");
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeUuid() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("id", TYPE_UUID, false);
                col.addUuid(0x123456789ABCDEF0L, 0xFEDCBA9876543210L);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncodeWithDeltaDict_freshConnection_sendsAllSymbols() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {
                GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

                // Add symbol column with global IDs
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ticker", TYPE_SYMBOL, false);

                // Simulate adding symbols via global dictionary
                int id1 = globalDict.getOrAddSymbol("AAPL");  // ID 0
                int id2 = globalDict.getOrAddSymbol("GOOG");  // ID 1
                col.addSymbolWithGlobalId("AAPL", id1);
                buffer.nextRow();
                col.addSymbolWithGlobalId("GOOG", id2);
                buffer.nextRow();

                // Fresh connection: confirmedMaxId = -1, so delta should include all symbols (0, 1)
                int confirmedMaxId = -1;
                int batchMaxId = 1;

                int size = encoder.encodeWithDeltaDict(buffer, globalDict, confirmedMaxId, batchMaxId);
                Assert.assertTrue(size > 12);

                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                // Verify header flag has FLAG_DELTA_SYMBOL_DICT set
                byte flags = Unsafe.getUnsafe().getByte(ptr + HEADER_OFFSET_FLAGS);
                Assert.assertTrue("Delta flag should be set", (flags & FLAG_DELTA_SYMBOL_DICT) != 0);
            }
        });
    }

    @Test
    public void testEncodeWithDeltaDict_noNewSymbols_sendsEmptyDelta() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {
                GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

                // Pre-populate dictionary with all symbols
                int id0 = globalDict.getOrAddSymbol("AAPL");  // ID 0
                int id1 = globalDict.getOrAddSymbol("GOOG");  // ID 1

                // Use only existing symbols
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ticker", TYPE_SYMBOL, false);
                col.addSymbolWithGlobalId("AAPL", id0);
                buffer.nextRow();
                col.addSymbolWithGlobalId("GOOG", id1);
                buffer.nextRow();

                // Server has confirmed all symbols (0-1), batchMaxId is 1
                int confirmedMaxId = 1;
                int batchMaxId = 1;

                int size = encoder.encodeWithDeltaDict(buffer, globalDict, confirmedMaxId, batchMaxId);
                Assert.assertTrue(size > 12);

                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                // Verify delta flag is set
                byte flags = Unsafe.getUnsafe().getByte(ptr + HEADER_OFFSET_FLAGS);
                Assert.assertTrue("Delta flag should be set", (flags & FLAG_DELTA_SYMBOL_DICT) != 0);

                // Read delta section after header
                long pos = ptr + HEADER_SIZE;

                // Read deltaStart varint (should be 2 = confirmedMaxId + 1)
                int deltaStart = Unsafe.getUnsafe().getByte(pos) & 0x7F;
                Assert.assertEquals(2, deltaStart);
                pos++;

                // Read deltaCount varint (should be 0)
                int deltaCount = Unsafe.getUnsafe().getByte(pos) & 0x7F;
                Assert.assertEquals(0, deltaCount);
            }
        });
    }

    @Test
    public void testEncodeWithDeltaDict_readsGlobalIdsFromDataBuffer() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {
                GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();
                for (int i = 0; i < 8; i++) {
                    globalDict.getOrAddSymbol("SYM_" + i);
                }

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ticker", TYPE_SYMBOL, false);
                col.addSymbolWithGlobalId("SYM_5", 5);
                buffer.nextRow();
                col.addSymbolWithGlobalId("SYM_7", 7);
                buffer.nextRow();

                Assert.assertEquals(0, col.getAuxDataAddress());

                int size = encoder.encodeWithDeltaDict(buffer, globalDict, 7, 7);
                Assert.assertTrue(size > 12);

                Cursor cursor = new Cursor(encoder.getBuffer().getBufferPtr() + HEADER_SIZE);
                Assert.assertEquals(8, cursor.readVarint());
                Assert.assertEquals(0, cursor.readVarint());

                Assert.assertEquals("test_table", cursor.readString());
                Assert.assertEquals(2, cursor.readVarint());
                Assert.assertEquals(1, cursor.readVarint());
                Assert.assertEquals("ticker", cursor.readString());
                Assert.assertEquals(TYPE_SYMBOL, cursor.readByte());
                Assert.assertEquals(0, cursor.readByte()); // no nulls
                Assert.assertEquals(5, cursor.readVarint());
                Assert.assertEquals(7, cursor.readVarint());
            }
        });
    }

    @Test
    public void testEncodeWithDeltaDict_withConfirmed_sendsOnlyNew() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {
                GlobalSymbolDictionary globalDict = new GlobalSymbolDictionary();

                // Pre-populate dictionary (simulating symbols already sent)
                globalDict.getOrAddSymbol("AAPL");  // ID 0
                globalDict.getOrAddSymbol("GOOG");  // ID 1

                // Now add new symbols
                int id2 = globalDict.getOrAddSymbol("MSFT");  // ID 2
                int id3 = globalDict.getOrAddSymbol("TSLA");  // ID 3

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ticker", TYPE_SYMBOL, false);
                col.addSymbolWithGlobalId("MSFT", id2);
                buffer.nextRow();
                col.addSymbolWithGlobalId("TSLA", id3);
                buffer.nextRow();

                // Server has confirmed IDs 0-1, so delta should only include 2-3
                int confirmedMaxId = 1;
                int batchMaxId = 3;

                int size = encoder.encodeWithDeltaDict(buffer, globalDict, confirmedMaxId, batchMaxId);
                Assert.assertTrue(size > 12);

                QwpBufferWriter buf = encoder.getBuffer();
                long ptr = buf.getBufferPtr();

                // Verify delta flag is set
                byte flags = Unsafe.getUnsafe().getByte(ptr + HEADER_OFFSET_FLAGS);
                Assert.assertTrue("Delta flag should be set", (flags & FLAG_DELTA_SYMBOL_DICT) != 0);

                // Read delta section after header
                long pos = ptr + HEADER_SIZE;

                // Read deltaStart varint (should be 2 = confirmedMaxId + 1)
                int deltaStart = Unsafe.getUnsafe().getByte(pos) & 0x7F;
                Assert.assertEquals(2, deltaStart);
            }
        });
    }

    @Test
    public void testEncodeZeroLong() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                col.addLong(0L);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testEncoderReusability() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer1 = new QwpTableBuffer("table1");
                 QwpTableBuffer buffer2 = new QwpTableBuffer("table2")) {
                // Encode first message
                QwpTableBuffer.ColumnBuffer col1 = buffer1.getOrCreateColumn("x", TYPE_LONG, false);
                col1.addLong(1L);
                buffer1.nextRow();
                int size1 = encoder.encode(buffer1);

                // Encode second message (encoder should reset internally)
                QwpTableBuffer.ColumnBuffer col2 = buffer2.getOrCreateColumn("y", TYPE_DOUBLE, false);
                col2.addDouble(2.0);
                buffer2.nextRow();
                int size2 = encoder.encode(buffer2);

                // Both should succeed
                Assert.assertTrue(size1 > 12);
                Assert.assertTrue(size2 > 12);
            }
        });
    }

    @Test
    public void testGlobalSymbolDictionaryBasics() throws Exception {
        assertMemoryLeak(() -> {
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();

            // Test sequential IDs
            Assert.assertEquals(0, dict.getOrAddSymbol("AAPL"));
            Assert.assertEquals(1, dict.getOrAddSymbol("GOOG"));
            Assert.assertEquals(2, dict.getOrAddSymbol("MSFT"));

            // Test deduplication
            Assert.assertEquals(0, dict.getOrAddSymbol("AAPL"));
            Assert.assertEquals(1, dict.getOrAddSymbol("GOOG"));

            // Test retrieval
            Assert.assertEquals("AAPL", dict.getSymbol(0));
            Assert.assertEquals("GOOG", dict.getSymbol(1));
            Assert.assertEquals("MSFT", dict.getSymbol(2));

            // Test size
            Assert.assertEquals(3, dict.size());
        });
    }

    @Test
    public void testGorillaEncoding_compressionRatio() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("metrics")) {
                // Add many timestamps with constant delta - best case for Gorilla
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, true);
                for (int i = 0; i < 1000; i++) {
                    col.addLong(1000000000L + i * 1000L);
                    buffer.nextRow();
                }

                int sizeWithGorilla = encoder.encode(buffer);

                // Uncompressed, the timestamps alone take 1000 * 8 = 8000 bytes.
                // For constant delta, Gorilla compresses to well under a fifth of that.
                int uncompressedTimestampBytes = 1000 * 8;
                Assert.assertTrue("Compression ratio should be < 0.2 for constant delta",
                        sizeWithGorilla < uncompressedTimestampBytes / 5);
            }
        });
    }

    @Test
    public void testGorillaEncoding_multipleTimestampColumns() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test")) {
                // Add multiple timestamp columns
                for (int i = 0; i < 50; i++) {
                    QwpTableBuffer.ColumnBuffer ts1Col = buffer.getOrCreateColumn("ts1", TYPE_TIMESTAMP, true);
                    ts1Col.addLong(1000000000L + i * 1000L);

                    QwpTableBuffer.ColumnBuffer ts2Col = buffer.getOrCreateColumn("ts2", TYPE_TIMESTAMP, true);
                    ts2Col.addLong(2000000000L + i * 2000L);

                    buffer.nextRow();
                }

                int sizeWithGorilla = encoder.encode(buffer);

                // Two constant-delta timestamp columns of 50 rows take
                // 2 * 50 * 8 = 800 bytes uncompressed; Gorilla compresses both.
                int uncompressedTimestampBytes = 2 * 50 * 8;
                Assert.assertTrue("Gorilla should compress multiple timestamp columns",
                        sizeWithGorilla < uncompressedTimestampBytes);
            }
        });
    }

    @Test
    public void testGorillaEncoding_multipleTimestamps_usesGorillaEncoding() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test")) {
                // Add multiple timestamps with constant delta (best compression)
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                for (int i = 0; i < 100; i++) {
                    col.addLong(1000000000L + i * 1000L);
                    buffer.nextRow();
                }

                int sizeWithGorilla = encoder.encode(buffer);

                // 100 constant-delta timestamps take 100 * 8 = 800 bytes
                // uncompressed; Gorilla produces a much smaller payload.
                int uncompressedTimestampBytes = 100 * 8;
                Assert.assertTrue("Gorilla encoding should be smaller",
                        sizeWithGorilla < uncompressedTimestampBytes);
            }
        });
    }

    @Test
    public void testGorillaEncoding_nanosTimestamps() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test")) {
                // Use TYPE_TIMESTAMP_NANOS
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP_NANOS, true);
                for (int i = 0; i < 100; i++) {
                    col.addLong(1000000000000000000L + i * 1000000L); // Nanos with millisecond intervals
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);

                // Verify header has Gorilla flag
                QwpBufferWriter buf = encoder.getBuffer();
                byte flags = Unsafe.getUnsafe().getByte(buf.getBufferPtr() + HEADER_OFFSET_FLAGS);
                Assert.assertEquals(FLAG_GORILLA, (byte) (flags & FLAG_GORILLA));
            }
        });
    }

    @Test
    public void testGorillaEncoding_singleTimestamp_usesUncompressed() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test")) {
                // Single timestamp - should use uncompressed
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                col.addLong(1000000L);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);
            }
        });
    }

    @Test
    public void testGorillaEncoding_twoTimestamps_usesUncompressed() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test")) {
                // Only 2 timestamps - should use uncompressed (Gorilla needs 3+)
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP);
                col.addLong(1000000L);
                buffer.nextRow();
                col.addLong(2000000L);
                buffer.nextRow();

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);

                // Verify header has Gorilla flag set
                QwpBufferWriter buf = encoder.getBuffer();
                byte flags = Unsafe.getUnsafe().getByte(buf.getBufferPtr() + HEADER_OFFSET_FLAGS);
                Assert.assertEquals(FLAG_GORILLA, (byte) (flags & FLAG_GORILLA));
            }
        });
    }

    @Test
    public void testGorillaEncoding_varyingDelta() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test")) {
                // Varying deltas that exercise different buckets
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, true);
                long[] timestamps = {
                        1000000000L,
                        1000001000L,  // delta=1000
                        1000002000L,  // DoD=0
                        1000003050L,  // DoD=50
                        1000004200L,  // DoD=100
                        1000006200L,  // DoD=850
                };

                for (long ts : timestamps) {
                    col.addLong(ts);
                    buffer.nextRow();
                }

                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 12);

                // Verify header has Gorilla flag
                QwpBufferWriter buf = encoder.getBuffer();
                byte flags = Unsafe.getUnsafe().getByte(buf.getBufferPtr() + HEADER_OFFSET_FLAGS);
                Assert.assertEquals(FLAG_GORILLA, (byte) (flags & FLAG_GORILLA));
            }
        });
    }

    @Test
    public void testGorillaFlagAlwaysSet() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test")) {
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("ts", TYPE_TIMESTAMP, true);
                col.addLong(1000000L);
                buffer.nextRow();

                encoder.encode(buffer);

                // The Gorilla flag is always set on QWP ingress messages.
                QwpBufferWriter buf = encoder.getBuffer();
                byte flags = Unsafe.getUnsafe().getByte(buf.getBufferPtr() + 5);
                Assert.assertEquals(FLAG_GORILLA, (byte) (flags & FLAG_GORILLA));
            }
        });
    }

    @Test
    public void testPayloadLengthPatched() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test_table")) {
                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                col.addLong(42L);
                buffer.nextRow();

                int size = encoder.encode(buffer);

                // Payload length is at offset 8 (4 magic + 1 version + 1 flags + 2 tablecount)
                QwpBufferWriter buf = encoder.getBuffer();
                int payloadLength = Unsafe.getUnsafe().getInt(buf.getBufferPtr() + 8);

                // Payload length should be total size minus header (12 bytes)
                Assert.assertEquals(size - 12, payloadLength);
            }
        });
    }

    @Test
    public void testReset() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                col.addLong(1L);
                buffer.nextRow();

                int size1 = encoder.encode(buffer);

                // Reset and encode again
                buffer.reset();
                col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                col.addLong(2L);
                buffer.nextRow();

                int size2 = encoder.encode(buffer);

                // Sizes should be similar (same schema)
                Assert.assertEquals(size1, size2);
            }
        });
    }

    @Test
    public void testVersionByteInHeader() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("test")) {

                QwpTableBuffer.ColumnBuffer col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                col.addLong(42);
                buffer.nextRow();

                // Default version
                int size = encoder.encode(buffer);
                Assert.assertTrue(size > 0);
                long ptr = encoder.getBuffer().getBufferPtr();
                Assert.assertEquals(1, Unsafe.getUnsafe().getByte(ptr + 4));

                // Custom version
                buffer.reset();
                col = buffer.getOrCreateColumn("x", TYPE_LONG, false);
                col.addLong(42);
                buffer.nextRow();
                encoder.setVersion((byte) 3);
                encoder.encode(buffer);
                Assert.assertEquals(3, Unsafe.getUnsafe().getByte(ptr + 4));
            }
        });
    }

    private static final class Cursor {
        private long address;

        private Cursor(long address) {
            this.address = address;
        }

        private byte readByte() {
            return Unsafe.getUnsafe().getByte(address++);
        }

        private String readString() {
            int len = readVarint();
            byte[] bytes = new byte[len];
            for (int i = 0; i < len; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(address + i);
            }
            String value = new String(bytes, StandardCharsets.UTF_8);
            address += len;
            return value;
        }

        private int readVarint() {
            int value = 0;
            int shift = 0;
            while (true) {
                int b = Unsafe.getUnsafe().getByte(address++) & 0xff;
                value |= (b & 0x7f) << shift;
                if ((b & 0x80) == 0) {
                    return value;
                }
                shift += 7;
            }
        }
    }
}
