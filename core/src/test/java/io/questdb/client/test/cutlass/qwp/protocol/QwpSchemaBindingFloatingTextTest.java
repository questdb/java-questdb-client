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
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.missing;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpSchemaBindingFloatingTextTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/floating-to-text.tsv";

    @Test
    public void testCorpusUsesExactTargetTextWire() throws Exception {
        assertMemoryLeak(
                () -> {
                    InputStream stream =
                            QwpSchemaBindingFloatingTextTest.class.getResourceAsStream(CORPUS);
                    Assert.assertNotNull(CORPUS, stream);
                    int count = 0;
                    try (BufferedReader lines =
                            new BufferedReader(
                                    new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                        String line;
                        while ((line = lines.readLine()) != null) {
                            if (line.isEmpty() || line.charAt(0) == '#') {
                                continue;
                            }
                            String[] fields = line.split("\t", -1);
                            Assert.assertEquals(line, 7, fields.length);
                            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                                    QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                                boolean symbol = "SYMBOL".equals(fields[3]);
                                QwpSchemaBinding binding =
                                        binding(buffer, column("value", targetType(fields[3])));
                                append(binding, fields[1], fields[2], "value");
                                buffer.nextRow();
                                int size = encoder.encodeSchema(buffer);
                                QwpTestWireReader reader =
                                        tableReader(
                                                encoder,
                                                size,
                                                symbol
                                                        ? QwpConstants.TYPE_SYMBOL
                                                        : QwpConstants.TYPE_VARCHAR,
                                                1);
                                boolean missing = "<NULL>".equals(fields[5]);
                                Assert.assertEquals(missing, "<NULL>".equals(fields[6]));
                                Assert.assertEquals(missing ? 1 : 0, reader.u8());
                                if (missing) {
                                    Assert.assertEquals(1, reader.u8());
                                    if (symbol) {
                                        Assert.assertEquals(0, reader.varint());
                                    } else {
                                        Assert.assertEquals(0, reader.i32());
                                    }
                                } else {
                                    byte[] expected = hex(fields[5]);
                                    Assert.assertEquals(
                                            fields[6],
                                            new String(expected, StandardCharsets.UTF_8));
                                    if (symbol) {
                                        Assert.assertEquals(1, reader.varint());
                                        Assert.assertArrayEquals(expected, reader.stringBytes());
                                        Assert.assertEquals(0, reader.varint());
                                    } else {
                                        Assert.assertEquals(0, reader.i32());
                                        Assert.assertEquals(expected.length, reader.i32());
                                        Assert.assertArrayEquals(
                                                expected, reader.bytes(expected.length));
                                    }
                                }
                                Assert.assertEquals(size, reader.position());
                            } catch (AssertionError e) {
                                throw new AssertionError(
                                        "case_id=" + fields[0] + ": " + e.getMessage(), e);
                            }
                            count++;
                        }
                    }
                    Assert.assertEquals(48, count);
                });
    }

    @Test
    public void testNaNDuplicateOmissionRollbackAndReset() throws Exception {
        assertMemoryLeak(
                () -> {
                    for (int target :
                            new int[] {ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL}) {
                        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                                QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                            byte wireType =
                                    target == ColumnType.SYMBOL
                                            ? QwpConstants.TYPE_SYMBOL
                                            : QwpConstants.TYPE_VARCHAR;
                            QwpSchemaBinding binding = binding(buffer, column("value", target));
                            binding.floatColumn("value", Float.intBitsToFloat(0x7fc00002))
                                    .binaryColumn("value", new byte[] {1});
                            buffer.nextRow();
                            binding.doubleColumn("value", -0.0);
                            buffer.nextRow();
                            buffer.nextRow();
                            int size = encoder.encodeSchema(buffer);
                            QwpTestWireReader reader = tableReader(encoder, size, wireType, 3);
                            Assert.assertEquals(1, reader.u8());
                            Assert.assertEquals(5, reader.u8());
                            if (wireType == QwpConstants.TYPE_SYMBOL) {
                                Assert.assertEquals(1, reader.varint());
                                Assert.assertEquals("-0.0", reader.string());
                                Assert.assertEquals(0, reader.varint());
                            } else {
                                Assert.assertEquals(0, reader.i32());
                                Assert.assertEquals(4, reader.i32());
                                Assert.assertEquals("-0.0", reader.ascii(4));
                            }
                            Assert.assertEquals(size, reader.position());
                        }
                    }

                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                            QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding =
                                binding(
                                        buffer,
                                        column("a", ColumnType.STRING),
                                        column("bad", ColumnType.UUID),
                                        column("c", ColumnType.VARCHAR));
                        binding.doubleColumn("a", 1e23);
                        buffer.nextRow();
                        binding.floatColumn("a", 0.1f);
                        assertReason(
                                LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                                () -> binding.doubleColumn("bad", 2.0));
                        buffer.cancelCurrentRow();
                        buffer.rollbackUncommittedColumns();
                        binding.floatColumn("c", Float.intBitsToFloat(1));
                        buffer.nextRow();
                        buffer.nextRow();
                        int size = encoder.encodeSchema(buffer);
                        QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
                        reader.skip(QwpConstants.HEADER_SIZE);
                        Assert.assertEquals("t", reader.string());
                        Assert.assertEquals(1, reader.u8());
                        Assert.assertEquals(1, reader.i32());
                        Assert.assertEquals(1, reader.i64());
                        Assert.assertEquals(3, reader.varint());
                        Assert.assertEquals(2, reader.varint());
                        Assert.assertEquals("a", reader.string());
                        Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.u8());
                        Assert.assertEquals("c", reader.string());
                        Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.u8());
                        assertVarchar(reader, 6, "1.0E23");
                        assertVarchar(reader, 5, "1.401298464324817E-45");
                        Assert.assertEquals(size, reader.position());

                        buffer.reset();
                        binding.doubleColumn("a", Double.NEGATIVE_INFINITY);
                        buffer.nextRow();
                        int resetSize = encoder.encodeSchema(buffer);
                        QwpTestWireReader reset = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), resetSize);
                        reset.skip(QwpConstants.HEADER_SIZE);
                        Assert.assertEquals("t", reset.string());
                        Assert.assertEquals(1, reset.u8());
                        Assert.assertEquals(1, reset.i32());
                        Assert.assertEquals(1, reset.i64());
                        Assert.assertEquals(1, reset.varint());
                        Assert.assertEquals(2, reset.varint());
                        Assert.assertEquals("a", reset.string());
                        Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reset.u8());
                        Assert.assertEquals("c", reset.string());
                        Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reset.u8());
                        assertVarchar(reset, 0, "-Infinity");
                        Assert.assertEquals(1, reset.u8());
                        Assert.assertEquals(1, reset.u8());
                        Assert.assertEquals(0, reset.i32());
                        Assert.assertEquals(resetSize, reset.position());
                    }
                });
    }

    @Test
    public void testSymbolFormatterScratchIsCopied() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, column("value", ColumnType.SYMBOL));
            binding.doubleColumn("value", 42.0);
            buffer.nextRow();
            binding.doubleColumn("value", 43.0);
            buffer.nextRow();
            binding.doubleColumn("value", 42.0);
            buffer.nextRow();
            int size = encoder.encodeSchema(buffer);
            QwpTestWireReader reader = tableReader(encoder, size, QwpConstants.TYPE_SYMBOL, 3);
            Assert.assertEquals(0, reader.u8());
            Assert.assertEquals(2, reader.varint());
            Assert.assertEquals("42.0", reader.string());
            Assert.assertEquals("43.0", reader.string());
            Assert.assertEquals(0, reader.varint());
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals(0, reader.varint());
            Assert.assertEquals(size, reader.position());
        }
    }

    @Test
    public void testNativeInferenceAndUnsupportedTargets() {
        assertNative("FLOAT", "value", 0x80000000L);
        assertNative("DOUBLE", "value", 0x8000000000000000L);
        for (int target : new int[] {ColumnType.TIMESTAMP, ColumnType.UUID, ColumnType.BOOLEAN}) {
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, column("value", target));
                assertReason(
                        LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () ->
                                binding.doubleColumn(
                                        "value", Double.longBitsToDouble(0x7ff8000000000002L)));
            }
        }
    }

    private static void assertNative(String inputType, String name, long bits) {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
            append(
                    binding,
                    inputType,
                    inputType.equals("FLOAT")
                            ? String.format("%08x", bits)
                            : String.format("%016x", bits),
                    name);
            buffer.nextRow();
            int size = encoder.encodeSchema(buffer);
            QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
            reader.skip(QwpConstants.HEADER_SIZE);
            Assert.assertEquals("t", reader.string());
            Assert.assertEquals(0, reader.u8());
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals(name, reader.string());
            Assert.assertEquals(
                    inputType.equals("FLOAT") ? QwpConstants.TYPE_FLOAT : QwpConstants.TYPE_DOUBLE,
                    reader.u8());
            Assert.assertEquals(0, reader.u8());
            if (inputType.equals("FLOAT")) {
                Assert.assertEquals((int) bits, reader.i32());
            } else {
                Assert.assertEquals(bits, reader.i64());
            }
            Assert.assertEquals(size, reader.position());
        }
    }

    private static void append(QwpSchemaBinding binding, String type, String bits, String name) {
        if ("FLOAT".equals(type))
            binding.floatColumn(name, Float.intBitsToFloat((int) Long.parseUnsignedLong(bits, 16)));
        else binding.doubleColumn(name, Double.longBitsToDouble(Long.parseUnsignedLong(bits, 16)));
    }

    private static void assertVarchar(QwpTestWireReader reader, int bitmap, String expected) {
        if (bitmap == 0) {
            Assert.assertEquals(0, reader.u8());
        } else {
            Assert.assertEquals(1, reader.u8());
            Assert.assertEquals(bitmap, reader.u8());
        }
        Assert.assertEquals(0, reader.i32());
        Assert.assertEquals(expected.length(), reader.i32());
        Assert.assertEquals(expected, reader.ascii(expected.length()));
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, byte type, int rows) {
        QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        Assert.assertEquals("t", reader.string());
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(1, reader.i32());
        Assert.assertEquals(1, reader.i64());
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(1, reader.varint());
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(type, reader.u8());
        return reader;
    }

    private static int targetType(String value) {
        if ("STRING".equals(value)) {
            return ColumnType.STRING;
        }
        if ("VARCHAR".equals(value)) {
            return ColumnType.VARCHAR;
        }
        if ("SYMBOL".equals(value)) {
            return ColumnType.SYMBOL;
        }
        throw new AssertionError(value);
    }

    private static byte[] hex(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++)
            bytes[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        return bytes;
    }
}
