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
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.known;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.missing;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.parameterizedColumn;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpSchemaBindingStringLong256Test {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/string-long256-conversions.tsv";
    private static final String HEADER = "# case_id\tinput\toutcome\tl0_hex\tl1_hex\tl2_hex\tl3_hex";

    @Test
    public void testCorpusUsesExactLong256Wire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingStringLong256Test.class.getResourceAsStream(CORPUS);
            Assert.assertNotNull(CORPUS, stream);
            int count = 0;
            boolean headerSeen = false;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty()) {
                        continue;
                    }
                    if (line.charAt(0) == '#') {
                        Assert.assertFalse("duplicate corpus header", headerSeen);
                        Assert.assertEquals(HEADER, line);
                        headerSeen = true;
                        continue;
                    }
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 7, fields.length);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer, column("value", ColumnType.LONG256));
                        if ("VALUE".equals(fields[2])) {
                            binding.stringColumn("value", fields[1]);
                            buffer.nextRow();
                            int size = encoder.encodeSchema(buffer);
                            QwpTestWireReader reader = tableReader(encoder, size, 1);
                            Assert.assertEquals(0, reader.u8());
                            for (int i = 3; i < 7; i++) {
                                Assert.assertEquals(parseHexLong(fields[i]), reader.i64());
                            }
                            Assert.assertEquals(size, reader.position());
                        } else if ("NULL".equals(fields[2])) {
                            Assert.assertEquals("<NULL>", fields[1]);
                            binding.stringColumn("value", null);
                            buffer.nextRow();
                            int size = encoder.encodeSchema(buffer);
                            QwpTestWireReader reader = tableReader(encoder, size, 1);
                            Assert.assertEquals(1, reader.u8());
                            Assert.assertEquals(1, reader.u8());
                            Assert.assertEquals(size, reader.position());
                        } else if ("INVALID".equals(fields[2])) {
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                                    () -> binding.stringColumn("value", fields[1]));
                            buffer.cancelCurrentRow();
                            buffer.rollbackUncommittedColumns();
                            Assert.assertEquals(0, buffer.getColumnDefs().length);
                        } else {
                            throw new AssertionError("unknown outcome: " + fields[2]);
                        }
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertTrue("missing corpus header", headerSeen);
            Assert.assertEquals(23, count);
        });
    }

    @Test
    public void testDuplicateNullOmissionRollbackAndReset() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer,
                        column("a", ColumnType.LONG256), column("bad", ColumnType.LONG), column("c", ColumnType.LONG256));
                binding.stringColumn("a", null).binaryColumn("a", new byte[]{1});
                buffer.nextRow();
                binding.stringColumn("a", "0x01").stringColumn("a", "invalid duplicate");
                buffer.nextRow();
                buffer.nextRow();
                binding.stringColumn("a", "0x02");
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.stringColumn("bad", "0x03"));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.stringColumn("c", "0x04");
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
                reader.skip(QwpConstants.HEADER_SIZE);
                Assert.assertEquals("t", reader.string());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.i32());
                Assert.assertEquals(1, reader.i64());
                Assert.assertEquals(4, reader.varint());
                Assert.assertEquals(2, reader.varint());
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_LONG256, reader.u8());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_LONG256, reader.u8());
                assertLong256Values(reader, 13, 1);
                assertLong256Values(reader, 7, 4);
                Assert.assertEquals(size, reader.position());

                buffer.reset();
                binding.stringColumn("a", "0x05");
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
                Assert.assertEquals(QwpConstants.TYPE_LONG256, reset.u8());
                Assert.assertEquals("c", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_LONG256, reset.u8());
                Assert.assertEquals(0, reset.u8());
                Assert.assertEquals(5, reset.i64());
                Assert.assertEquals(0, reset.i64());
                Assert.assertEquals(0, reset.i64());
                Assert.assertEquals(0, reset.i64());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(resetSize, reset.position());
            }
        });
    }

    @Test
    public void testMissingStringInferenceRemainsVarchar() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
            binding.stringColumn("value", "0x00");
            buffer.nextRow();
            Assert.assertEquals(QwpConstants.TYPE_VARCHAR, buffer.getColumnDefs()[0].getTypeCode());
        }
    }

    @Test
    public void testParameterizedDesignatedAndOtherTargetsRemainUnsupported() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer,
                    parameterizedColumn("value", ColumnType.LONG256), column("other", ColumnType.UUID));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.stringColumn("value", "0x00"));
            buffer.cancelCurrentRow();
            buffer.rollbackUncommittedColumns();
            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                    () -> binding.stringColumn("other", "0x00"));
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer,
                    known(0, column("ts", ColumnType.TIMESTAMP_MICRO)));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.stringColumn("ts", "0x00"));
        }
    }

    private static void assertLong256Values(QwpTestWireReader reader, int bitmap, long value) {
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(bitmap, reader.u8());
        Assert.assertEquals(value, reader.i64());
        Assert.assertEquals(0, reader.i64());
        Assert.assertEquals(0, reader.i64());
        Assert.assertEquals(0, reader.i64());
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
        Assert.assertEquals(QwpConstants.TYPE_LONG256, reader.u8());
        return reader;
    }

    private static long parseHexLong(String value) {
        return Long.parseUnsignedLong(value, 16);
    }
}
