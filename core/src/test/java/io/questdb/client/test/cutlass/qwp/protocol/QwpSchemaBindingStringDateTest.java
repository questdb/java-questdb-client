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
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.tableHeader;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpSchemaBindingStringDateTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/string-to-date.tsv";

    @Test
    public void testCorpusUsesExactDateWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingStringDateTest.class.getResourceAsStream(CORPUS);
            Assert.assertNotNull(CORPUS, stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                Assert.assertEquals("case_id\tinput_kind\tinput\toutcome\texpected_raw\tfeature", lines.readLine());
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') {
                        continue;
                    }
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 6, fields.length);
                    CharSequence value = input(fields[1], fields[2]);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.DATE));
                        if ("INVALID".equals(fields[3])) {
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                                    () -> binding.stringColumn("value", value));
                            Assert.assertEquals(0, buffer.getRowCount());
                        } else {
                            Assert.assertTrue(line, "VALID".equals(fields[3]) || "NULL".equals(fields[3]));
                            binding.stringColumn("value", value);
                            buffer.nextRow();
                            int size = encoder.encodeSchema(buffer);
                            QwpTestWireReader reader = tableReader(encoder, size, 1, "value", QwpConstants.TYPE_DATE);
                            if ("NULL".equals(fields[3])) {
                                Assert.assertEquals(1, reader.u8());
                                Assert.assertEquals(1, reader.u8());
                            } else {
                                Assert.assertEquals(0, reader.u8());
                                Assert.assertEquals(Long.parseLong(fields[4]), reader.i64());
                            }
                            Assert.assertEquals(size, reader.position());
                        }
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertEquals(53, count);
        });
    }

    @Test
    public void testNullDuplicateOmissionRollbackAndReset() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("a", ColumnType.DATE), column("bad", ColumnType.UUID), column("c", ColumnType.DATE));
                binding.stringColumn("a", null).binaryColumn("a", new byte[]{1});
                buffer.nextRow();
                binding.stringColumn("a", "1970-01-02");
                buffer.nextRow();
                buffer.nextRow();
                binding.stringColumn("a", "1970-01-03");
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.stringColumn("bad", "not-a-uuid"));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.stringColumn("c", "1969-12-31");
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableHeader(encoder, size, 4, 2);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_DATE, reader.u8());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_DATE, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(13, reader.u8());
                Assert.assertEquals(86_400_000L, reader.i64());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(7, reader.u8());
                Assert.assertEquals(-86_400_000L, reader.i64());
                Assert.assertEquals(size, reader.position());

                buffer.reset();
                binding.stringColumn("a", "1970-01-01 00:00:00.001UTC");
                buffer.nextRow();
                int resetSize = encoder.encodeSchema(buffer);
                QwpTestWireReader reset = tableHeader(encoder, resetSize, 1, 2);
                Assert.assertEquals("a", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_DATE, reset.u8());
                Assert.assertEquals("c", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_DATE, reset.u8());
                Assert.assertEquals(0, reset.u8());
                Assert.assertEquals(1, reset.i64());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(resetSize, reset.position());
            }
        });
    }

    @Test
    public void testNativeInferenceParameterizedAndDesignatedBehavior() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
            binding.stringColumn("value", "1970-01-01");
            buffer.nextRow();
            int size = encoder.encodeSchema(buffer);
            QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
            reader.skip(QwpConstants.HEADER_SIZE);
            Assert.assertEquals("t", reader.string());
            Assert.assertEquals(0, reader.u8());
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals("value", reader.string());
            Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.u8());
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.DATE, new byte[]{1}));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.stringColumn("value", "1970-01-01"));
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, 0, column("value", ColumnType.DATE));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.stringColumn("value", "1970-01-01"));
        }
    }

    private static CharSequence input(String kind, String value) {
        Assert.assertTrue(kind, "TEXT".equals(kind) || "UTF16_HEX".equals(kind));
        if ("<NULL>".equals(value)) {
            return null;
        }
        if ("<EMPTY>".equals(value)) {
            return "";
        }
        if ("TEXT".equals(kind)) {
            return value;
        }
        String[] units = value.split(",");
        char[] chars = new char[units.length];
        for (int i = 0; i < units.length; i++) {
            chars[i] = (char) Integer.parseInt(units[i], 16);
        }
        return new String(chars);
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, int rows, String name, byte type) {
        QwpTestWireReader reader = tableHeader(encoder, size, rows, 1);
        Assert.assertEquals(name, reader.string());
        Assert.assertEquals(type, reader.u8());
        return reader;
    }
}
