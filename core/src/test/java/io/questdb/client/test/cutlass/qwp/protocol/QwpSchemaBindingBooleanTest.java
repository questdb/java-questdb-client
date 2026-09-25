/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.assertReason;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.binding;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.column;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.skipTablePrefix;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpSchemaBindingBooleanTest {
    private static final String BOOLEAN_CORPUS = "/io/questdb/client/cutlass/qwp/boolean-to-target.tsv";
    private static final String STRING_CORPUS = "/io/questdb/client/cutlass/qwp/string-to-boolean.tsv";

    @Test
    public void testBooleanCorpusUsesExactTargetWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingBooleanTest.class.getResourceAsStream(BOOLEAN_CORPUS);
            Assert.assertNotNull(BOOLEAN_CORPUS, stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') {
                        continue;
                    }
                    String[] fields = line.split("\\t", -1);
                    Assert.assertEquals(line, 6, fields.length);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer, -1, column("value", targetType(fields[2])));
                        Assert.assertTrue(fields[1], "TRUE".equals(fields[1]) || "FALSE".equals(fields[1]));
                        binding.boolColumn("value", "TRUE".equals(fields[1]));
                        buffer.nextRow();
                        QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), wireType(fields[3]), 1);
                        Assert.assertEquals(0, reader.u8());
                        assertValue(reader, fields[3], fields[4]);
                        Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertEquals(18, count);
        });
    }

    @Test
    public void testStringCorpusParsesStrictAsciiAndUsesBooleanWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingBooleanTest.class.getResourceAsStream(STRING_CORPUS);
            Assert.assertNotNull(STRING_CORPUS, stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') {
                        continue;
                    }
                    String[] fields = line.split("\\t", -1);
                    Assert.assertEquals(line, 4, fields.length);
                    CharSequence value = "<NULL>".equals(fields[1]) ? null : utf16(fields[1]);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.BOOLEAN));
                        if ("<INVALID>".equals(fields[2])) {
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, () -> binding.stringColumn("value", value));
                        } else {
                            binding.stringColumn("value", value);
                            buffer.nextRow();
                            QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), QwpConstants.TYPE_BOOLEAN, 1);
                            if (value == null) {
                                Assert.assertEquals(1, reader.u8());
                                Assert.assertEquals(1, reader.u8());
                            } else {
                                Assert.assertEquals(0, reader.u8());
                                Assert.assertEquals(Integer.parseInt(fields[2]), reader.u8());
                            }
                            Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                        }
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertEquals(80, count);
        });
    }

    @Test
    public void testBooleanPackingPastEightAndSixtyFourWithOmissions() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.BOOLEAN));
                for (int row = 0; row < 137; row++) {
                    if ((row & 1) == 0) {
                        binding.boolColumn("value", (row & 3) == 0);
                    }
                    buffer.nextRow();
                }
                QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), QwpConstants.TYPE_BOOLEAN, 137);
                Assert.assertEquals(1, reader.u8());
                byte[] nulls = new byte[18];
                for (int i = 0; i < 17; i++) nulls[i] = (byte) 0xaa;
                Assert.assertArrayEquals(nulls, reader.bytes(nulls.length));
                byte[] expected = new byte[9];
                for (int i = 0; i < 8; i++) expected[i] = 0x55;
                expected[8] = 0x15;
                Assert.assertArrayEquals(expected, reader.bytes(expected.length));
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());

                buffer.reset();
                for (int row = 0; row < 70; row++) buffer.nextRow();
                QwpTestWireReader allNull = tableReader(encoder, encoder.encodeSchema(buffer), QwpConstants.TYPE_BOOLEAN, 70);
                Assert.assertEquals(1, allNull.u8());
                byte[] allNulls = new byte[9];
                for (int i = 0; i < 8; i++) allNulls[i] = (byte) 0xff;
                allNulls[8] = 0x3f;
                Assert.assertArrayEquals(allNulls, allNull.bytes(allNulls.length));
                Assert.assertEquals(encoder.getBuffer().getPosition(), allNull.position());
            }
        });
    }

    @Test
    public void testDuplicateRollbackFailedRowAndLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("a", ColumnType.BOOLEAN), column("bad", ColumnType.BOOLEAN), column("c", ColumnType.INT));
                binding.boolColumn("a", true);
                buffer.nextRow();
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.stringColumn("bad", "invalid"));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.boolColumn("c", true);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
                reader.skip(QwpConstants.HEADER_SIZE);
                skipTablePrefix(reader, 2, 2);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_BOOLEAN, reader.u8());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_INT, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(2, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.i32());
                Assert.assertEquals(size, reader.position());

                buffer.reset();
                StringBuilder mutable = new StringBuilder("TrUe");
                binding.stringColumn("a", mutable).boolColumn("a", false);
                mutable.replace(0, mutable.length(), "false");
                buffer.nextRow();
                int resetSize = encoder.encodeSchema(buffer);
                QwpTestWireReader reset = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), resetSize);
                reset.skip(QwpConstants.HEADER_SIZE);
                skipTablePrefix(reset, 1, 2);
                Assert.assertEquals("a", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_BOOLEAN, reset.u8());
                Assert.assertEquals("c", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_INT, reset.u8());
                Assert.assertEquals(0, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(resetSize, reset.position());
                buffer.clear();
                assertIllegalState(() -> binding.boolColumn("a", true));
                assertIllegalState(() -> binding.stringColumn("a", "true"));
            }
        });
    }

    @Test
    public void testDuplicatePrecedesUnsupportedCrossSetterAndInvalidParse() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("flag", ColumnType.BOOLEAN), column("sym", ColumnType.SYMBOL));
                binding.boolColumn("flag", true).stringColumn("flag", "invalid");
                binding.stringColumn("sym", "first").boolColumn("sym", false);
                buffer.nextRow();
                Assert.assertTrue(encoder.encodeSchema(buffer) > 0);
            }
        });
    }

    @Test
    public void testUnsupportedParameterizedUnknownAndDesignatedTargets() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, -1,
                    column("sym", ColumnType.SYMBOL), column("future", ColumnType.BOOLEAN, new byte[]{1}),
                    column("flagged", ColumnType.BOOLEAN | 0x10000), column("plain", ColumnType.BOOLEAN));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.boolColumn("sym", true));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.symbol("plain", null));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.longColumn("plain", Long.MIN_VALUE));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.boolColumn("future", true));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.stringColumn("flagged", "true"));
            rollback(buffer);
            binding.boolColumn("missing", true);
            Assert.assertEquals(QwpConstants.TYPE_BOOLEAN,
                    buffer.getColumnDefs()[buffer.getColumnCount() - 1].getTypeCode());
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, 0, column("d", ColumnType.BOOLEAN));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.boolColumn("d", true));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.stringColumn("d", null));
        }
    }

    private static void assertValue(QwpTestWireReader reader, String type, String expected) {
        switch (type) {
            case "BOOLEAN": Assert.assertEquals(Integer.parseInt(expected), reader.u8()); break;
            case "BYTE": Assert.assertEquals(Integer.parseInt(expected), reader.u8()); break;
            case "SHORT": Assert.assertEquals(Integer.parseInt(expected), reader.i16()); break;
            case "INT": Assert.assertEquals(Integer.parseInt(expected), reader.i32()); break;
            case "LONG": Assert.assertEquals(Long.parseLong(expected), reader.i64()); break;
            case "FLOAT": Assert.assertEquals(Float.floatToRawIntBits(Float.parseFloat(expected)), Float.floatToRawIntBits(reader.f32())); break;
            case "DOUBLE": Assert.assertEquals(Double.doubleToRawLongBits(Double.parseDouble(expected)), Double.doubleToRawLongBits(reader.f64())); break;
            case "VARCHAR":
                byte[] bytes = hex(expected);
                Assert.assertEquals(0, reader.i32());
                Assert.assertEquals(bytes.length, reader.i32());
                Assert.assertArrayEquals(bytes, reader.bytes(bytes.length));
                break;
            default: throw new AssertionError("unknown wire type: " + type);
        }
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, byte type, int rows) {
        QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        skipTablePrefix(reader, rows, 1);
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(type, reader.u8());
        return reader;
    }

    private static int targetType(String value) {
        switch (value) {
            case "BOOLEAN": return ColumnType.BOOLEAN;
            case "BYTE": return ColumnType.BYTE;
            case "SHORT": return ColumnType.SHORT;
            case "INT": return ColumnType.INT;
            case "LONG": return ColumnType.LONG;
            case "FLOAT": return ColumnType.FLOAT;
            case "DOUBLE": return ColumnType.DOUBLE;
            case "STRING": return ColumnType.STRING;
            case "VARCHAR": return ColumnType.VARCHAR;
            default: throw new AssertionError("unknown target type: " + value);
        }
    }

    private static byte wireType(String value) {
        switch (value) {
            case "BOOLEAN": return QwpConstants.TYPE_BOOLEAN;
            case "BYTE": return QwpConstants.TYPE_BYTE;
            case "SHORT": return QwpConstants.TYPE_SHORT;
            case "INT": return QwpConstants.TYPE_INT;
            case "LONG": return QwpConstants.TYPE_LONG;
            case "FLOAT": return QwpConstants.TYPE_FLOAT;
            case "DOUBLE": return QwpConstants.TYPE_DOUBLE;
            case "VARCHAR": return QwpConstants.TYPE_VARCHAR;
            default: throw new AssertionError("unknown wire type: " + value);
        }
    }

    private static byte[] hex(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) bytes[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        return bytes;
    }

    private static String utf16(String value) {
        Assert.assertEquals(0, value.length() & 3);
        char[] chars = new char[value.length() / 4];
        for (int i = 0; i < chars.length; i++) chars[i] = (char) Integer.parseInt(value.substring(i * 4, i * 4 + 4), 16);
        return new String(chars);
    }

    private static void rollback(QwpTableBuffer buffer) { buffer.cancelCurrentRow(); buffer.rollbackUncommittedColumns(); }
    private static void assertIllegalState(Runnable action) { Assert.assertThrows(IllegalStateException.class, action::run); }
}
