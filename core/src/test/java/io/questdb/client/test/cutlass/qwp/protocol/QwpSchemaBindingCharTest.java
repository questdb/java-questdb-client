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
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.column;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.known;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.missing;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.tableHeader;

public class QwpSchemaBindingCharTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/char-conversions.tsv";
    private static final String HEADER = "# case_id\tinput_kind\tutf16_hex\texpected_char\texpected_null";

    @Test
    public void testCharToTextUsesTargetNativeUtf8() {
        for (int targetType : new int[]{ColumnType.STRING, ColumnType.VARCHAR}) {
            try (QwpTableBuffer buffer = new QwpTableBuffer("t");
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpSchemaBinding binding = new QwpSchemaBinding(buffer, known(-1, column("value", targetType)));
                binding.charColumn("value", 'A');
                buffer.nextRow();
                binding.charColumn("value", '\u03a9');
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableHeader(encoder, size, 2, 1);
                Assert.assertEquals("value", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0, reader.i32());
                Assert.assertEquals(1, reader.i32());
                Assert.assertEquals(3, reader.i32());
                Assert.assertEquals('A', reader.u8());
                Assert.assertEquals(0xce, reader.u8());
                Assert.assertEquals(0xa9, reader.u8());
                Assert.assertEquals(size, reader.position());
            }
        }
    }

    @Test
    public void testCorpusUsesExactNativeCharWire() throws Exception {
        InputStream stream = getClass().getResourceAsStream(CORPUS);
        Assert.assertNotNull(CORPUS, stream);
        boolean headerSeen = false;
        int count = 0;
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
                Assert.assertEquals(line, 5, fields.length);
                Assert.assertTrue("true".equals(fields[4]) || "false".equals(fields[4]));
                Assert.assertEquals(Integer.parseInt(fields[3]) == 0, Boolean.parseBoolean(fields[4]));
                try (QwpTableBuffer buffer = new QwpTableBuffer("t");
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                    if ("CHAR".equals(fields[1])) {
                        QwpSchemaBinding binding = new QwpSchemaBinding(buffer, known(-1, column("value", ColumnType.CHAR)));
                        String input = fromUtf16Hex(fields[2]);
                        Assert.assertEquals(1, input.length());
                        binding.charColumn("value", input.charAt(0));
                    } else if ("STRING".equals(fields[1])) {
                        QwpSchemaBinding binding = new QwpSchemaBinding(buffer, known(-1, column("value", ColumnType.CHAR)));
                        binding.stringColumn("value", "<NULL>".equals(fields[2]) ? null : fromUtf16Hex(fields[2]));
                    } else {
                        throw new AssertionError("unknown input kind: " + fields[1]);
                    }
                    buffer.nextRow();
                    int size = encoder.encodeSchema(buffer);
                    QwpTestWireReader reader = tableReader(encoder, size, QwpConstants.TYPE_CHAR);
                    if ("<NULL>".equals(fields[2])) {
                        Assert.assertEquals(1, reader.u8());
                        Assert.assertEquals(1, reader.u8());
                    } else {
                        Assert.assertEquals(0, reader.u8());
                        Assert.assertEquals(Integer.parseInt(fields[3]), reader.u16());
                    }
                    Assert.assertEquals(size, reader.position());
                } catch (AssertionError e) {
                    throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                }
                count++;
            }
        }
        Assert.assertTrue("missing corpus header", headerSeen);
        Assert.assertEquals(23, count);
    }

    @Test
    public void testDuplicateOmissionRollbackResetAndInference() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t");
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, known(-1,
                    column("a", ColumnType.CHAR), column("bad", ColumnType.UUID), column("c", ColumnType.CHAR)));
            binding.stringColumn("a", "A").binaryColumn("a", new byte[]{1});
            buffer.nextRow();
            buffer.nextRow();
            binding.stringColumn("a", "B");
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.charColumn("bad", 'C'));
            buffer.cancelCurrentRow();
            buffer.rollbackUncommittedColumns();
            binding.stringColumn("c", "Z");
            buffer.nextRow();
            int size = encoder.encodeSchema(buffer);
            QwpTestWireReader reader = tableHeader(encoder, size, 3, 2);
            Assert.assertEquals("a", reader.string());
            Assert.assertEquals(QwpConstants.TYPE_CHAR, reader.u8());
            Assert.assertEquals("c", reader.string());
            Assert.assertEquals(QwpConstants.TYPE_CHAR, reader.u8());
            Assert.assertEquals(1, reader.u8());
            Assert.assertEquals(6, reader.u8());
            Assert.assertEquals('A', reader.u16());
            Assert.assertEquals(1, reader.u8());
            Assert.assertEquals(3, reader.u8());
            Assert.assertEquals('Z', reader.u16());
            Assert.assertEquals(size, reader.position());

            buffer.reset();
            binding.stringColumn("a", "Q");
            buffer.nextRow();
            int resetSize = encoder.encodeSchema(buffer);
            QwpTestWireReader reset = tableHeader(encoder, resetSize, 1, 2);
            Assert.assertEquals("a", reset.string());
            Assert.assertEquals(QwpConstants.TYPE_CHAR, reset.u8());
            Assert.assertEquals("c", reset.string());
            Assert.assertEquals(QwpConstants.TYPE_CHAR, reset.u8());
            Assert.assertEquals(0, reset.u8());
            Assert.assertEquals('Q', reset.u16());
            Assert.assertEquals(1, reset.u8());
            Assert.assertEquals(1, reset.u8());
            Assert.assertEquals(resetSize, reset.position());
        }

        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
            binding.stringColumn("value", "A");
            buffer.nextRow();
            Assert.assertEquals(QwpConstants.TYPE_VARCHAR, buffer.getColumnDefs()[0].getTypeCode());
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
            binding.charColumn("value", 'A');
            buffer.nextRow();
            Assert.assertEquals(QwpConstants.TYPE_CHAR, buffer.getColumnDefs()[0].getTypeCode());
        }
    }

    @Test
    public void testUnsupportedAndDesignatedTargetsRemainRejected() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, known(-1,
                    column("value", ColumnType.TIMESTAMP_MICRO)));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.charColumn("value", 'A'));
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, known(0, column("ts", ColumnType.TIMESTAMP_MICRO)));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.charColumn("ts", 'A'));
        }
        for (int targetType : new int[]{ColumnType.STRING, ColumnType.VARCHAR}) {
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(buffer, known(-1, column("value", targetType)));
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.charColumn("value", '\ud800'));
            }
        }
    }

    private static String fromUtf16Hex(String hex) {
        StringBuilder value = new StringBuilder(hex.length() / 4);
        for (int i = 0; i < hex.length(); i += 4) {
            value.append((char) Integer.parseInt(hex.substring(i, i + 4), 16));
        }
        return value.toString();
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, byte type) {
        QwpTestWireReader reader = tableHeader(encoder, size, 1, 1);
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(type, reader.u8());
        return reader;
    }
}
