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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.client.QwpDatagramSizeEstimator;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpDatagramSizeEstimatorTest {

    @Test
    public void testBooleanColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_BOOLEAN, false).addBoolean(true);
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testByteColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_BYTE, false).addByte((byte) 42);
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testCharColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_CHAR, false).addShort((short) 'A');
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testDateColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_DATE, false).addLong(1_700_000_000_000L);
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testDecimal128Column() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_DECIMAL128, true).addDecimal128(new Decimal128(0, 12345, 2));
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testDecimal256Column() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_DECIMAL256, true).addDecimal256(new Decimal256(0, 0, 0, 12345, 2));
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testDecimal64Column() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_DECIMAL64, true).addDecimal64(new Decimal64(12345, 2));
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testDoubleArray2DColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_DOUBLE_ARRAY, true).addDoubleArray(
                        new double[][]{{1.0, 2.0, 3.0}, {4.0, 5.0, 6.0}}
                );
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testDoubleArray3DColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_DOUBLE_ARRAY, true).addDoubleArray(
                        new double[][][]{{{1.0, 2.0}, {3.0, 4.0}}, {{5.0, 6.0}, {7.0, 8.0}}}
                );
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testDoubleArrayColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_DOUBLE_ARRAY, true).addDoubleArray(new double[]{1.0, 2.0, 3.0});
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testDoubleColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_DOUBLE, false).addDouble(3.14);
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testFloatColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_FLOAT, false).addFloat(3.14f);
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testGeoHashColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_GEOHASH, true).addGeoHash(0x1234L, 20);
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testIntColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_INT, false).addInt(42);
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testLong256Column() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_LONG256, false).addLong256(1L, 2L, 3L, 4L);
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testLongArray2DColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_LONG_ARRAY, true).addLongArray(
                        new long[][]{{10L, 20L, 30L}, {40L, 50L, 60L}}
                );
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testLongArrayColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_LONG_ARRAY, true).addLongArray(new long[]{10L, 20L, 30L});
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testLongColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_LONG, false).addLong(42L);
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testMultiByteUtf8() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("\u6e2c\u5b9a")) { // 測定 (3 bytes per char)
                buf.getOrCreateColumn("\u6e29\u5ea6", TYPE_DOUBLE, false).addDouble(22.5); // 温度
                buf.getOrCreateColumn("\u30e1\u30e2", TYPE_STRING, true).addString("\u3053\u3093\u306b\u3061\u306f"); // メモ, こんにちは
                buf.getOrCreateColumn("\u5730\u57df", TYPE_SYMBOL, true).addSymbol("\u6771\u4eac"); // 地域, 東京
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testMultiRowDoubleTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            for (int rowCount : new int[]{1, 5, 10, 50}) {
                try (QwpTableBuffer buf = new QwpTableBuffer("measurements")) {
                    for (int i = 0; i < rowCount; i++) {
                        buf.getOrCreateColumn("value", TYPE_DOUBLE, false).addDouble(i * 1.1);
                        buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L + i);
                        buf.nextRow();
                    }
                    assertEstimateAccuracy(buf, rowCount);
                }
            }
        });
    }

    @Test
    public void testNullableColumnMixedNullNonNull() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                QwpTableBuffer.ColumnBuffer idCol = buf.getOrCreateColumn("id", TYPE_LONG, false);
                QwpTableBuffer.ColumnBuffer valCol = buf.getOrCreateColumn("val", TYPE_DOUBLE, true);
                QwpTableBuffer.ColumnBuffer tsCol = buf.getOrCreateColumn("", TYPE_TIMESTAMP, true);

                // Row 1: has value
                idCol.addLong(1L);
                valCol.addDouble(10.0);
                tsCol.addLong(1_000_000L);
                buf.nextRow();

                // Row 2: null (skip val)
                idCol.addLong(2L);
                tsCol.addLong(2_000_000L);
                buf.nextRow();

                // Row 3: has value
                idCol.addLong(3L);
                valCol.addDouble(30.0);
                tsCol.addLong(3_000_000L);
                buf.nextRow();

                // Row 4: null
                idCol.addLong(4L);
                tsCol.addLong(4_000_000L);
                buf.nextRow();

                // Row 5: has value
                idCol.addLong(5L);
                valCol.addDouble(50.0);
                tsCol.addLong(5_000_000L);
                buf.nextRow();

                assertEstimateAccuracy(buf, 5);
            }
        });
    }

    @Test
    public void testShortColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_SHORT, false).addShort((short) 42);
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testStringColumnEmpty() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_STRING, true).addString("");
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testStringColumnLong() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_STRING, true).addString("a]".repeat(500));
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testStringColumnShort() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_STRING, true).addString("hello");
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testSymbolWith100DistinctValues() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                for (int i = 0; i < 100; i++) {
                    buf.getOrCreateColumn("sym", TYPE_SYMBOL, true).addSymbol("val-" + i);
                    buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L + i);
                    buf.nextRow();
                }
                assertEstimateAccuracy(buf, 100);
            }
        });
    }

    @Test
    public void testSymbolWith10DistinctValues() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                for (int i = 0; i < 10; i++) {
                    buf.getOrCreateColumn("sym", TYPE_SYMBOL, true).addSymbol("value-" + i);
                    buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L + i);
                    buf.nextRow();
                }
                assertEstimateAccuracy(buf, 10);
            }
        });
    }

    @Test
    public void testSymbolWith1DistinctValue() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                for (int i = 0; i < 5; i++) {
                    buf.getOrCreateColumn("sym", TYPE_SYMBOL, true).addSymbol("only-one");
                    buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L + i);
                    buf.nextRow();
                }
                assertEstimateAccuracy(buf, 5);
            }
        });
    }

    @Test
    public void testTimestampColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_TIMESTAMP, true).addLong(1_700_000_000L);
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testTimestampNanosColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_TIMESTAMP_NANOS, true).addLong(1_700_000_000_000L);
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testUuidColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_UUID, false).addUuid(123L, 456L);
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testRandomSchemas() throws Exception {
        assertMemoryLeak(() -> {
            Random rng = new Random(42);
            byte[] fixedTypes = {
                    TYPE_BOOLEAN, TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG,
                    TYPE_FLOAT, TYPE_DOUBLE, TYPE_DATE, TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS,
                    TYPE_UUID, TYPE_LONG256
            };

            for (int trial = 0; trial < 100; trial++) {
                int numCols = 1 + rng.nextInt(5);
                int numRows = 1 + rng.nextInt(20);

                try (QwpTableBuffer buf = new QwpTableBuffer("random_" + trial)) {
                    byte[] colTypes = new byte[numCols];
                    for (int c = 0; c < numCols; c++) {
                        int pick = rng.nextInt(fixedTypes.length + 2);
                        if (pick < fixedTypes.length) {
                            colTypes[c] = fixedTypes[pick];
                        } else if (pick == fixedTypes.length) {
                            colTypes[c] = TYPE_STRING;
                        } else {
                            colTypes[c] = TYPE_SYMBOL;
                        }
                    }

                    for (int row = 0; row < numRows; row++) {
                        for (int c = 0; c < numCols; c++) {
                            boolean isNullable = colTypes[c] == TYPE_STRING || colTypes[c] == TYPE_SYMBOL
                                    || colTypes[c] == TYPE_TIMESTAMP;
                            QwpTableBuffer.ColumnBuffer col = buf.getOrCreateColumn(
                                    "c" + c, colTypes[c], isNullable
                            );
                            addRandomValue(col, colTypes[c], rng);
                        }
                        buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L + row);
                        buf.nextRow();
                    }
                    assertEstimateAccuracy(buf, numRows);
                }
            }
        });
    }

    @Test
    public void testVarcharColumn() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buf = new QwpTableBuffer("t")) {
                buf.getOrCreateColumn("v", TYPE_VARCHAR, true).addString("varchar value");
                buf.getOrCreateColumn("", TYPE_TIMESTAMP, true).addLong(1_000_000L);
                buf.nextRow();
                assertEstimateAccuracy(buf, 1);
            }
        });
    }

    @Test
    public void testVarintSize() {
        Assert.assertEquals(1, QwpDatagramSizeEstimator.varintSize(0));
        Assert.assertEquals(1, QwpDatagramSizeEstimator.varintSize(1));
        Assert.assertEquals(1, QwpDatagramSizeEstimator.varintSize(127));
        Assert.assertEquals(2, QwpDatagramSizeEstimator.varintSize(128));
        Assert.assertEquals(2, QwpDatagramSizeEstimator.varintSize(16383));
        Assert.assertEquals(3, QwpDatagramSizeEstimator.varintSize(16384));
    }

    private static void addRandomValue(QwpTableBuffer.ColumnBuffer col, byte type, Random rng) {
        switch (type) {
            case TYPE_BOOLEAN -> col.addBoolean(rng.nextBoolean());
            case TYPE_BYTE -> col.addByte((byte) rng.nextInt());
            case TYPE_SHORT -> col.addShort((short) rng.nextInt());
            case TYPE_INT -> col.addInt(rng.nextInt());
            case TYPE_LONG -> col.addLong(rng.nextLong());
            case TYPE_FLOAT -> col.addFloat(rng.nextFloat());
            case TYPE_DOUBLE -> col.addDouble(rng.nextDouble());
            case TYPE_DATE -> col.addLong(rng.nextLong());
            case TYPE_TIMESTAMP, TYPE_TIMESTAMP_NANOS -> col.addLong(Math.abs(rng.nextLong()));
            case TYPE_UUID -> col.addUuid(rng.nextLong(), rng.nextLong());
            case TYPE_LONG256 -> col.addLong256(rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong());
            case TYPE_STRING -> col.addString("str" + rng.nextInt(1000));
            case TYPE_SYMBOL -> col.addSymbol("sym" + rng.nextInt(20));
        }
    }

    private static void assertEstimateAccuracy(QwpTableBuffer buf, int rowCount) {
        long estimate = QwpDatagramSizeEstimator.estimate(buf, rowCount);

        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            encoder.setGorillaEnabled(false);
            int actual = encoder.encode(buf, false);

            Assert.assertTrue(
                    "estimate (" + estimate + ") < actual (" + actual + ")",
                    estimate >= actual
            );
            Assert.assertTrue(
                    "estimate (" + estimate + ") - actual (" + actual + ") = " + (estimate - actual) + " >= 32",
                    estimate - actual < 32
            );
        }
    }
}
