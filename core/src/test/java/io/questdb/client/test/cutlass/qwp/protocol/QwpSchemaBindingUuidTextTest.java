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
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
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
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.known;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.missing;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.parameterizedColumn;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpSchemaBindingUuidTextTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/uuid-to-text.tsv";

    @Test
    public void testCorpusUsesCanonicalVarcharWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingUuidTextTest.class.getResourceAsStream(CORPUS);
            Assert.assertNotNull(CORPUS, stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') {
                        continue;
                    }
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 7, fields.length);
                    Assert.assertEquals("VARCHAR", fields[4]);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer, column("value", targetType(fields[3])));
                        binding.uuidColumn("value", Long.parseLong(fields[1]), Long.parseLong(fields[2]));
                        buffer.nextRow();
                        int size = encoder.encodeSchema(buffer);
                        QwpTestWireReader reader = tableReader(encoder, size, 1);
                        if ("<NULL>".equals(fields[5])) {
                            Assert.assertEquals("<NULL>", fields[6]);
                            Assert.assertEquals(1, reader.u8());
                            Assert.assertEquals(1, reader.u8());
                            Assert.assertEquals(0, reader.i32());
                        } else {
                            byte[] expected = hex(fields[5]);
                            Assert.assertEquals(fields[6], new String(expected, StandardCharsets.UTF_8));
                            Assert.assertEquals(0, reader.u8());
                            Assert.assertEquals(0, reader.i32());
                            Assert.assertEquals(expected.length, reader.i32());
                            Assert.assertArrayEquals(expected, reader.bytes(expected.length));
                        }
                        Assert.assertEquals(size, reader.position());
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertEquals(16, count);
        });
    }

    @Test
    public void testDuplicateNullOmissionRollbackAndReset() throws Exception {
        assertMemoryLeak(() -> {
            for (int targetType : new int[]{ColumnType.STRING, ColumnType.VARCHAR}) {
                try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    QwpSchemaBinding binding = binding(buffer, column("value", targetType));
                    binding.uuidColumn("value", Long.MIN_VALUE, Long.MIN_VALUE)
                            .binaryColumn("value", new byte[]{1})
                            .longColumn("value", 42);
                    buffer.nextRow();
                    binding.uuidColumn("value", 1, 0);
                    buffer.nextRow();
                    buffer.nextRow();
                    int size = encoder.encodeSchema(buffer);
                    QwpTestWireReader reader = tableReader(encoder, size, 3);
                    Assert.assertEquals(1, reader.u8());
                    Assert.assertEquals(5, reader.u8());
                    Assert.assertEquals(0, reader.i32());
                    Assert.assertEquals(36, reader.i32());
                    Assert.assertEquals("00000000-0000-0000-0000-000000000001", reader.ascii(36));
                    Assert.assertEquals(size, reader.position());
                }
            }
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer,
                        column("a", ColumnType.STRING), column("bad", ColumnType.LONG), column("c", ColumnType.VARCHAR));
                binding.uuidColumn("a", 1, 0);
                buffer.nextRow();
                binding.uuidColumn("a", 2, 0);
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.uuidColumn("bad", 3, 0));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.uuidColumn("c", 4, 0);
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
                assertVarcharValues(reader, 6, "00000000-0000-0000-0000-000000000001");
                assertVarcharValues(reader, 5, "00000000-0000-0000-0000-000000000004");
                Assert.assertEquals(size, reader.position());

                buffer.reset();
                binding.uuidColumn("a", 5, 0);
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
                Assert.assertEquals(0, reset.u8());
                Assert.assertEquals(0, reset.i32());
                Assert.assertEquals(36, reset.i32());
                Assert.assertEquals("00000000-0000-0000-0000-000000000005", reset.ascii(36));
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(0, reset.i32());
                Assert.assertEquals(resetSize, reset.position());
            }
        });
    }

    @Test
    public void testNativeInferenceAndNativeBothMinRemainUuidBytes() {
        assertNativeBothMin(known(column("value", ColumnType.UUID)), true);
        assertNativeBothMin(missing(), false);
    }

    private static void assertNativeBothMin(QwpSchemaResponse schema, boolean known) {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, schema);
            binding.uuidColumn("value", Long.MIN_VALUE, Long.MIN_VALUE);
            buffer.nextRow();
            int size = encoder.encodeSchema(buffer);
            QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
            reader.skip(QwpConstants.HEADER_SIZE);
            Assert.assertEquals("t", reader.string());
            Assert.assertEquals(known ? 1 : 0, reader.u8());
            if (known) {
                Assert.assertEquals(1, reader.i32());
                Assert.assertEquals(1, reader.i64());
            }
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals("value", reader.string());
            Assert.assertEquals(QwpConstants.TYPE_UUID, reader.u8());
            Assert.assertEquals(0, reader.u8());
            Assert.assertEquals(Long.MIN_VALUE, reader.i64());
            Assert.assertEquals(Long.MIN_VALUE, reader.i64());
            Assert.assertEquals(size, reader.position());
        }
    }

    @Test
    public void testSymbolParameterizedAndDesignatedTargetsRemainUnsupported() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding ordinary = binding(buffer,
                    column("sym", ColumnType.SYMBOL), parameterizedColumn("text", ColumnType.STRING),
                    column("ts", ColumnType.TIMESTAMP));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> ordinary.uuidColumn("sym", Long.MIN_VALUE, Long.MIN_VALUE));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> ordinary.uuidColumn("text", 1, 0));
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding designated = new QwpSchemaBinding(buffer, known(0, column("ts", ColumnType.TIMESTAMP)));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> designated.uuidColumn("ts", 1, 0));
        }
    }

    private static void assertVarcharValues(QwpTestWireReader reader, int bitmap, String value) {
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(bitmap, reader.u8());
        Assert.assertEquals(0, reader.i32());
        Assert.assertEquals(36, reader.i32());
        Assert.assertEquals(value, reader.ascii(36));
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, int rows) {
        return tableReader(encoder, size, rows, "value");
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, int rows, String column) {
        QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        Assert.assertEquals("t", reader.string());
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(1, reader.i32());
        Assert.assertEquals(1, reader.i64());
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(1, reader.varint());
        Assert.assertEquals(column, reader.string());
        Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.u8());
        return reader;
    }

    private static int targetType(String value) {
        if ("STRING".equals(value)) return ColumnType.STRING;
        if ("VARCHAR".equals(value)) return ColumnType.VARCHAR;
        throw new AssertionError(value);
    }

    private static byte[] hex(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) bytes[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        return bytes;
    }
}
