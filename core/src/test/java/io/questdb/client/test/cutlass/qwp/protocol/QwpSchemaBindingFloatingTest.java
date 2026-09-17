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

public class QwpSchemaBindingFloatingTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/floating-to-numeric.tsv";

    @Test
    public void testConformanceCorpusUsesExactTargetWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingFloatingTest.class.getResourceAsStream(CORPUS);
            Assert.assertNotNull(CORPUS, stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') continue;
                    String[] f = line.split("\\t", -1);
                    Assert.assertEquals(line, 7, f.length);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer, -1, column("value", targetType(f[3])));
                        Runnable append = () -> append(binding, f[1], f[2]);
                        if ("<INVALID>".equals(f[5])) {
                            Assert.assertEquals(f[0], "<INVALID>", f[6]);
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, append);
                        } else {
                            append.run();
                            buffer.nextRow();
                            QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), wireType(f[4]), 1, 1);
                            if ("<NULL>".equals(f[5])) {
                                Assert.assertEquals(f[0],
                                        "BYTE".equals(f[3]) || "SHORT".equals(f[3]) ? "0" : "NULL", f[6]);
                                Assert.assertEquals(1, reader.u8());
                                Assert.assertEquals(1, reader.u8());
                            } else {
                                Assert.assertEquals(0, reader.u8());
                                Assert.assertArrayEquals(f[0], hex(f[5]), reader.bytes(hex(f[5]).length));
                            }
                            Assert.assertEquals(f[0], encoder.getBuffer().getPosition(), reader.position());
                        }
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + f[0] + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertEquals(350, count);
        });
    }

    @Test
    public void testNaNIsMissingWithOtherNullAndPresentValues() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.DOUBLE));
                binding.floatColumn("value", Float.intBitsToFloat(0x7fc12345));
                buffer.nextRow();
                buffer.nextRow();
                binding.doubleColumn("value", -0.0d);
                buffer.nextRow();
                QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), QwpConstants.TYPE_DOUBLE, 3, 1);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(3, reader.u8());
                Assert.assertEquals(0x8000000000000000L, reader.i64());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
            }
        });
    }

    @Test
    public void testDuplicatePrecedesInvalidAndUnsupportedConversion() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("n", ColumnType.LONG), column("uuid", ColumnType.UUID));
                binding.floatColumn("n", 1).doubleColumn("n", Double.POSITIVE_INFINITY);
                binding.uuidColumn("uuid", 1, 2).floatColumn("uuid", Float.NaN);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
                reader.skip(QwpConstants.HEADER_SIZE);
                skipTablePrefix(reader, 1, 2);
                Assert.assertEquals("n", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_LONG, reader.u8());
                Assert.assertEquals("uuid", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_UUID, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(1, reader.i64());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(1, reader.i64());
                Assert.assertEquals(2, reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testFailureRollbackRemovesFailedOnlyColumnAndPreservesRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("a", ColumnType.FLOAT), column("only_b", ColumnType.DOUBLE),
                        column("bad", ColumnType.BYTE), column("c", ColumnType.LONG));
                binding.doubleColumn("a", 1.5);
                buffer.nextRow();
                binding.floatColumn("only_b", 2);
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.doubleColumn("bad", 128));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.floatColumn("c", 3);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
                reader.skip(QwpConstants.HEADER_SIZE);
                skipTablePrefix(reader, 2, 2);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_FLOAT, reader.u8());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_LONG, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(2, reader.u8());
                Assert.assertEquals(Float.floatToRawIntBits(1.5f), reader.i32());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(3, reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testResetClearAndStaleBinding() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.FLOAT));
                binding.doubleColumn("value", 9.25);
                buffer.nextRow();
                buffer.reset();
                binding.floatColumn("value", -0.0f);
                buffer.nextRow();
                QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), QwpConstants.TYPE_FLOAT, 1, 1);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0x80000000, reader.i32());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                buffer.clear();
                assertIllegalState(() -> binding.floatColumn("value", 1));
                assertIllegalState(() -> binding.doubleColumn("value", 1));
            }
        });
    }

    @Test
    public void testUnsupportedParameterizedUnknownAndDesignatedTargets() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, -1,
                    column("uuid", ColumnType.UUID), column("future", ColumnType.FLOAT, new byte[]{1}),
                    column("flagged", ColumnType.DOUBLE | 0x10000), column("bool", ColumnType.BOOLEAN),
                    column("timestamp", ColumnType.TIMESTAMP), column("binary", ColumnType.BINARY),
                    column("byte", ColumnType.BYTE), column("short", ColumnType.SHORT));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.floatColumn("uuid", Float.NaN));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.doubleColumn("future", Double.NaN));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.floatColumn("flagged", 1));
            rollback(buffer);
            binding.doubleColumn("missing", 1);
            Assert.assertEquals(QwpConstants.TYPE_DOUBLE,
                    buffer.getColumnDefs()[buffer.getColumnCount() - 1].getTypeCode());
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.floatColumn("bool", Float.NaN));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.doubleColumn("timestamp", Double.NaN));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.floatColumn("binary", Float.NaN));
            rollback(buffer);
            assertInvalidContext(binding, buffer, "byte", true);
            assertInvalidContext(binding, buffer, "short", false);
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, 0, column("d", ColumnType.DOUBLE));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.floatColumn("d", Float.NaN));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.doubleColumn("d", Double.NaN));
        }
    }

    private static void append(QwpSchemaBinding binding, String inputType, String bits) {
        if ("FLOAT".equals(inputType)) {
            binding.floatColumn("value", Float.intBitsToFloat((int) Long.parseLong(bits, 16)));
        } else if ("DOUBLE".equals(inputType)) {
            binding.doubleColumn("value", Double.longBitsToDouble(Long.parseUnsignedLong(bits, 16)));
        } else {
            throw new AssertionError("unknown input type: " + inputType);
        }
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, byte type, int rows, int columns) {
        QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        skipTablePrefix(reader, rows, columns);
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(type, reader.u8());
        return reader;
    }

    private static int targetType(String value) {
        switch (value) {
            case "BYTE": return ColumnType.BYTE; case "SHORT": return ColumnType.SHORT;
            case "INT": return ColumnType.INT; case "LONG": return ColumnType.LONG;
            case "FLOAT": return ColumnType.FLOAT; case "DOUBLE": return ColumnType.DOUBLE;
            default: throw new AssertionError("unknown target type: " + value);
        }
    }

    private static byte wireType(String value) {
        switch (value) {
            case "BYTE": return QwpConstants.TYPE_BYTE; case "SHORT": return QwpConstants.TYPE_SHORT;
            case "INT": return QwpConstants.TYPE_INT; case "LONG": return QwpConstants.TYPE_LONG;
            case "FLOAT": return QwpConstants.TYPE_FLOAT; case "DOUBLE": return QwpConstants.TYPE_DOUBLE;
            default: throw new AssertionError("unknown wire type: " + value);
        }
    }

    private static byte[] hex(String value) {
        Assert.assertEquals(value, 0, value.length() & 1); byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) bytes[i] = (byte) Integer.parseInt(value.substring((bytes.length - 1 - i) * 2, (bytes.length - i) * 2), 16);
        return bytes;
    }

    private static void rollback(QwpTableBuffer buffer) { buffer.cancelCurrentRow(); buffer.rollbackUncommittedColumns(); }
    private static void assertInvalidContext(QwpSchemaBinding binding, QwpTableBuffer buffer, String column, boolean floatInput) {
        LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class,
                () -> { if (floatInput) binding.floatColumn(column, 128); else binding.doubleColumn(column, Double.POSITIVE_INFINITY); });
        Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
        Assert.assertFalse(error.isRetryable());
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("column=" + column));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("inputType=" + (floatInput ? "FLOAT" : "DOUBLE")));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("targetType=" + column.toUpperCase()));
        rollback(buffer);
    }
    private static void assertIllegalState(Runnable action) { Assert.assertThrows(IllegalStateException.class, action::run); }
}
