/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 *******************************************************************************/
package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.assertReason;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.binding;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.column;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.missing;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.parameterizedColumn;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.tableHeader;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpSchemaBindingNativeDecimalTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/native-decimal-conversions.tsv";
    private static final String HEADER = "# case_id\tsource_type\tsource_scale\tsource_ll_hex\tsource_lh_hex\tsource_hl_hex\tsource_hh_hex\ttarget_type\ttarget_precision\ttarget_scale\toutcome\texpected_ll_hex\texpected_lh_hex\texpected_hl_hex\texpected_hh_hex\texpected_sql";

    @Test
    public void testCorpusUsesExactTargetDecimalWireAndPreservesCaller() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingNativeDecimalTest.class.getResourceAsStream(CORPUS);
            Assert.assertNotNull(CORPUS, stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                Assert.assertEquals(HEADER, lines.readLine());
                String line;
                while ((line = lines.readLine()) != null) {
                    Assert.assertFalse(line.isEmpty());
                    String[] f = line.split("\t", -1);
                    Assert.assertEquals(line, 16, f.length);
                    Assert.assertTrue("VALUE".equals(f[10]) || "INVALID".equals(f[10]));
                    assertTargetTag(f[7], Integer.parseInt(f[8]));
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer, -1,
                                column("value", ColumnType.getDecimalType(Integer.parseInt(f[8]), Integer.parseInt(f[9]))));
                        Object value = decimal(f);
                        long[] before = limbs(value);
                        int scale = scale(value);
                        if ("INVALID".equals(f[10])) {
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                                    () -> append(binding, value));
                            buffer.cancelCurrentRow();
                            buffer.rollbackUncommittedColumns();
                            Assert.assertEquals(0, buffer.getColumnCount());
                        } else {
                            append(binding, value);
                            buffer.nextRow();
                            int size = encoder.encodeSchema(buffer);
                            QwpTestWireReader reader = tableReader(encoder, size, wireType(f[7]));
                            Assert.assertEquals(0, reader.u8());
                            Assert.assertEquals(Integer.parseInt(f[9]), reader.u8());
                            assertExpectedLimbs(reader, f);
                            Assert.assertEquals(size, reader.position());
                        }
                        Assert.assertEquals(scale, scale(value));
                        Assert.assertArrayEquals(before, limbs(value));
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + f[0] + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertEquals(60, count);
        });
    }

    @Test
    public void testDecimalTextOverloadUsesEveryExactTargetWidth() throws Exception {
        assertMemoryLeak(() -> {
            int[] precisions = {2, 4, 9, 18, 38, 76};
            for (int precision : precisions) {
                try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    QwpSchemaBinding binding = binding(buffer, -1,
                            column("value", ColumnType.getDecimalType(precision, 1)));
                    binding.decimalColumn("value", "1.2", new Decimal256());
                    buffer.nextRow();

                    int size = encoder.encodeSchema(buffer);
                    QwpTestWireReader reader = tableReader(encoder, size, wireTypeForPrecision(precision));
                    Assert.assertEquals(0, reader.u8());
                    Assert.assertEquals(1, reader.u8());
                    Assert.assertEquals(12, reader.i64());
                    int limbs = precision <= 18 ? 1 : precision <= 38 ? 2 : 4;
                    for (int i = 1; i < limbs; i++) {
                        Assert.assertEquals(0, reader.i64());
                    }
                    Assert.assertEquals(size, reader.position());
                }
            }
        });
    }

    @Test
    public void testDecimalTextOverloadSuppressesDuplicatesAndRollsBackInvalidRows() throws Exception {
        assertMemoryLeak(() -> {
            int decimal = ColumnType.getDecimalType(5, 1);
            Decimal256 scratch = new Decimal256();
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("value", decimal), column("only_b", decimal));

                binding.decimalColumn("value", "1.2", scratch)
                        .decimalColumn("value", "not-a-decimal", scratch);
                buffer.nextRow();

                binding.decimalColumn("only_b", "2", scratch);
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.decimalColumn("value", "not-a-decimal", scratch));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();

                binding.decimalColumn("value", "NaN", scratch)
                        .decimalColumn("value", "still-not-a-decimal", scratch);
                buffer.nextRow();
                binding.decimalColumn("value", (CharSequence) null, scratch)
                        .decimalColumn("value", "", scratch);
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableHeader(encoder, size, 3, 1);
                decimalDefinition(reader, "value", QwpConstants.TYPE_DECIMAL64);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(0x06, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(12, reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testDecimalTextOverloadValidatesTargetAfterDuplicateBeforeParsing() throws Exception {
        assertMemoryLeak(() -> {
            Decimal256 scratch = new Decimal256();
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.UUID));

                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.decimalColumn("value", "not-a-decimal", scratch));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();

                binding.uuidColumn("value", 1, 2)
                        .decimalColumn("value", "not-a-decimal", scratch);
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableHeader(encoder, size, 1, 1);
                Assert.assertEquals("value", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_UUID, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(1, reader.i64());
                Assert.assertEquals(2, reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testDecimalTextOverloadMissingInfersDecimal256AtNaturalScale() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
                Decimal256 scratch = new Decimal256();
                binding.decimalColumn("value", "NaN", scratch);
                buffer.nextRow();
                binding.decimalColumn("value", "12.3400", scratch);
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = unknownTableReader(encoder, size, QwpConstants.TYPE_DECIMAL256, 2);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(2, reader.u8());
                Assert.assertEquals(1234, reader.i64());
                Assert.assertEquals(0, reader.i64());
                Assert.assertEquals(0, reader.i64());
                Assert.assertEquals(0, reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testDecimalTextOverloadMissingSpecialInfersDecimal256ScaleZeroNull() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
                binding.decimalColumn("value", "-Infinity", new Decimal256());
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = unknownTableReader(encoder, size, QwpConstants.TYPE_DECIMAL256);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(size, reader.position());

                buffer.reset();
                binding.decimalColumn("value", "5.678", new Decimal256());
                buffer.nextRow();
                int resetSize = encoder.encodeSchema(buffer);
                QwpTestWireReader reset = unknownTableReader(encoder, resetSize, QwpConstants.TYPE_DECIMAL256);
                Assert.assertEquals(0, reset.u8());
                Assert.assertEquals(3, reset.u8());
                Assert.assertEquals(5678, reset.i64());
                Assert.assertEquals(0, reset.i64());
                Assert.assertEquals(0, reset.i64());
                Assert.assertEquals(0, reset.i64());
                Assert.assertEquals(resetSize, reset.position());
            }
        });
    }

    @Test
    public void testDuplicateRollbackResetOmissionAndRetainedTargetScale() throws Exception {
        assertMemoryLeak(() -> {
            int decimal = ColumnType.getDecimalType(18, 4);
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("a", decimal), column("bad", ColumnType.getDecimalType(2, 0)), column("c", decimal));
                binding.decimalColumn("a", new Decimal64(15, 1)).binaryColumn("a", new byte[]{1});
                buffer.nextRow();
                binding.decimalColumn("a", new Decimal64(2, 0));
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.decimalColumn("bad", new Decimal128(0, 1000, 0)));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.decimalColumn("c", new Decimal256(0, 0, 0, 25, 2));
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableHeader(encoder, size, 2, 2);
                decimalDefinition(reader, "a", QwpConstants.TYPE_DECIMAL64);
                decimalDefinition(reader, "c", QwpConstants.TYPE_DECIMAL64);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(2, reader.u8());
                Assert.assertEquals(4, reader.u8());
                Assert.assertEquals(15000, reader.i64());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(4, reader.u8());
                Assert.assertEquals(2500, reader.i64());
                Assert.assertEquals(size, reader.position());

                buffer.reset();
                binding.decimalColumn("c", new Decimal64(3, 0));
                buffer.nextRow();
                int resetSize = encoder.encodeSchema(buffer);
                QwpTestWireReader reset = tableHeader(encoder, resetSize, 1, 2);
                decimalDefinition(reset, "a", QwpConstants.TYPE_DECIMAL64);
                decimalDefinition(reset, "c", QwpConstants.TYPE_DECIMAL64);
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(4, reset.u8());
                Assert.assertEquals(0, reset.u8());
                Assert.assertEquals(4, reset.u8());
                Assert.assertEquals(30000, reset.i64());
                Assert.assertEquals(resetSize, reset.position());
            }
        });
    }

    @Test
    public void testMissingInfersNativeDecimalWidthAndScale() throws Exception {
        assertMemoryLeak(() -> {
            Object[] values = {
                    new Decimal64(12, 1),
                    new Decimal128(3, 4, 2),
                    new Decimal256(5, 6, 7, 8, 3)
            };
            byte[] types = {QwpConstants.TYPE_DECIMAL64, QwpConstants.TYPE_DECIMAL128, QwpConstants.TYPE_DECIMAL256};
            for (int i = 0; i < values.length; i++) {
                try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
                    append(binding, values[i]);
                    buffer.nextRow();
                    int size = encoder.encodeSchema(buffer);
                    QwpTestWireReader reader = unknownTableReader(encoder, size, types[i]);
                    Assert.assertEquals(0, reader.u8());
                    Assert.assertEquals(scale(values[i]), reader.u8());
                    for (long limb : wireLimbs(values[i])) {
                        Assert.assertEquals(limb, reader.i64());
                    }
                    Assert.assertEquals(size, reader.position());
                }
            }
        });
    }

    @Test
    public void testMalformedParameterizedAndDesignatedTargetsRemainRejected() throws Exception {
        assertMemoryLeak(() -> {
            Decimal64 value = new Decimal64(1, 0);
            int[] malformed = {
                    ColumnType.DECIMAL64,
                    ColumnType.getDecimalType(18, 4) | 0x10000,
                    ColumnType.getDecimalType(18, 4) | 0x40000000
            };
            for (int type : malformed) {
                try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    QwpSchemaBinding binding = binding(buffer, -1, column("value", type));
                    assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                            () -> binding.decimalColumn("value", value));
                }
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, 0,
                        column("value", ColumnType.getDecimalType(18, 4)));
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.decimalColumn("value", value));
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        parameterizedColumn("value", ColumnType.getDecimalType(18, 4)));
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.decimalColumn("value", value));
            }
        });
    }

    @Test
    public void testLegacyBufferStillChoosesScaleFromFirstValueInEachBatch() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpTableBuffer.ColumnBuffer column = buffer.getOrCreateColumn(
                        "value", QwpConstants.TYPE_DECIMAL64, true);
                column.addDecimal64(new Decimal64(12, 2));
                buffer.nextRow();
                int firstSize = encoder.encode(buffer);
                QwpTestWireReader first = legacyTableReader(encoder, firstSize);
                Assert.assertEquals(0, first.u8());
                Assert.assertEquals(2, first.u8());
                Assert.assertEquals(12, first.i64());
                Assert.assertEquals(firstSize, first.position());

                buffer.reset();
                column.addDecimal64(new Decimal64(34, 3));
                buffer.nextRow();
                int secondSize = encoder.encode(buffer);
                QwpTestWireReader second = legacyTableReader(encoder, secondSize);
                Assert.assertEquals(0, second.u8());
                Assert.assertEquals(3, second.u8());
                Assert.assertEquals(34, second.i64());
                Assert.assertEquals(secondSize, second.position());
            }
        });
    }

    @Test
    public void testLegacyNullOnlyDecimalUsesWireScaleZeroWithoutLockingNextBatch() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpTableBuffer.ColumnBuffer column = buffer.getOrCreateColumn(
                        "value", QwpConstants.TYPE_DECIMAL256, true);
                column.addDecimal256(Decimal256.NULL_VALUE);
                buffer.nextRow();

                int nullSize = encoder.encode(buffer);
                QwpTestWireReader nullOnly = legacyTableReader(encoder, nullSize, QwpConstants.TYPE_DECIMAL256);
                Assert.assertEquals(1, nullOnly.u8());
                Assert.assertEquals(1, nullOnly.u8());
                Assert.assertEquals(0, nullOnly.u8());
                Assert.assertEquals(nullSize, nullOnly.position());

                buffer.reset();
                column.addDecimal256(new Decimal256(0, 0, 0, 1234, 4));
                buffer.nextRow();
                int valueSize = encoder.encode(buffer);
                QwpTestWireReader value = legacyTableReader(encoder, valueSize, QwpConstants.TYPE_DECIMAL256);
                Assert.assertEquals(0, value.u8());
                Assert.assertEquals(4, value.u8());
                Assert.assertEquals(1234, value.i64());
                Assert.assertEquals(0, value.i64());
                Assert.assertEquals(0, value.i64());
                Assert.assertEquals(0, value.i64());
                Assert.assertEquals(valueSize, value.position());
            }
        });
    }

    private static void append(QwpSchemaBinding binding, Object value) {
        if (value instanceof Decimal64) {
            binding.decimalColumn("value", (Decimal64) value);
        } else if (value instanceof Decimal128) {
            binding.decimalColumn("value", (Decimal128) value);
        } else {
            binding.decimalColumn("value", (Decimal256) value);
        }
    }

    private static void assertExpectedLimbs(QwpTestWireReader reader, String[] f) {
        int count = wireLongCount(f[7]);
        for (int i = 0; i < count; i++) {
            Assert.assertEquals(parseHex(f[11 + i]), reader.i64());
        }
        for (int i = count; i < 4; i++) {
            Assert.assertEquals("-", f[11 + i]);
        }
    }

    private static void assertTargetTag(String name, int precision) {
        String expected;
        if (precision <= 2) {
            expected = "DECIMAL8";
        } else if (precision <= 4) {
            expected = "DECIMAL16";
        } else if (precision <= 9) {
            expected = "DECIMAL32";
        } else if (precision <= 18) {
            expected = "DECIMAL64";
        } else if (precision <= 38) {
            expected = "DECIMAL128";
        } else {
            expected = "DECIMAL256";
        }
        Assert.assertEquals(expected, name);
    }

    private static Object decimal(String[] f) {
        int scale = Integer.parseInt(f[2]);
        long ll = parseHex(f[3]);
        switch (f[1]) {
            case "DECIMAL64":
                return new Decimal64(ll, scale);
            case "DECIMAL128":
                Decimal128 decimal = new Decimal128();
                decimal.of(parseHex(f[4]), ll, scale);
                return decimal;
            case "DECIMAL256":
                return new Decimal256(parseHex(f[6]), parseHex(f[5]), parseHex(f[4]), ll, scale);
            default:
                throw new AssertionError("unknown source type " + f[1]);
        }
    }

    private static void decimalDefinition(QwpTestWireReader reader, String name, byte type) {
        Assert.assertEquals(name, reader.string());
        Assert.assertEquals(type, reader.u8());
    }

    private static long[] limbs(Object value) {
        if (value instanceof Decimal64) {
            return new long[]{((Decimal64) value).getValue()};
        }
        if (value instanceof Decimal128) {
            return new long[]{((Decimal128) value).getHigh(), ((Decimal128) value).getLow()};
        }
        Decimal256 decimal = (Decimal256) value;
        return new long[]{decimal.getHh(), decimal.getHl(), decimal.getLh(), decimal.getLl()};
    }

    private static QwpTestWireReader legacyTableReader(QwpWebSocketEncoder encoder, int size) {
        return legacyTableReader(encoder, size, QwpConstants.TYPE_DECIMAL64);
    }

    private static QwpTestWireReader legacyTableReader(QwpWebSocketEncoder encoder, int size, byte type) {
        QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        Assert.assertEquals("t", reader.string());
        Assert.assertEquals(1, reader.varint());
        Assert.assertEquals(1, reader.varint());
        decimalDefinition(reader, "value", type);
        return reader;
    }

    private static long parseHex(String value) {
        Assert.assertNotEquals("-", value);
        return Long.parseUnsignedLong(value, 16);
    }

    private static int scale(Object value) {
        if (value instanceof Decimal64) {
            return ((Decimal64) value).getScale();
        }
        if (value instanceof Decimal128) {
            return ((Decimal128) value).getScale();
        }
        return ((Decimal256) value).getScale();
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, byte type) {
        QwpTestWireReader reader = tableHeader(encoder, size, 1, 1);
        decimalDefinition(reader, "value", type);
        return reader;
    }

    private static QwpTestWireReader unknownTableReader(QwpWebSocketEncoder encoder, int size, byte type) {
        return unknownTableReader(encoder, size, type, 1);
    }

    private static QwpTestWireReader unknownTableReader(QwpWebSocketEncoder encoder, int size, byte type, int rows) {
        QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        Assert.assertEquals("t", reader.string());
        Assert.assertEquals(0, reader.u8());
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(1, reader.varint());
        decimalDefinition(reader, "value", type);
        return reader;
    }

    private static byte wireType(String target) {
        if ("DECIMAL128".equals(target)) {
            return QwpConstants.TYPE_DECIMAL128;
        }
        if ("DECIMAL256".equals(target)) {
            return QwpConstants.TYPE_DECIMAL256;
        }
        return QwpConstants.TYPE_DECIMAL64;
    }

    private static int wireLongCount(String target) {
        if ("DECIMAL128".equals(target)) {
            return 2;
        }
        if ("DECIMAL256".equals(target)) {
            return 4;
        }
        return 1;
    }

    private static byte wireTypeForPrecision(int precision) {
        if (precision <= 18) {
            return QwpConstants.TYPE_DECIMAL64;
        }
        if (precision <= 38) {
            return QwpConstants.TYPE_DECIMAL128;
        }
        return QwpConstants.TYPE_DECIMAL256;
    }

    private static long[] wireLimbs(Object value) {
        if (value instanceof Decimal64) {
            return new long[]{((Decimal64) value).getValue()};
        }
        if (value instanceof Decimal128) {
            Decimal128 decimal = (Decimal128) value;
            return new long[]{decimal.getLow(), decimal.getHigh()};
        }
        Decimal256 decimal = (Decimal256) value;
        return new long[]{decimal.getLl(), decimal.getLh(), decimal.getHl(), decimal.getHh()};
    }
}
