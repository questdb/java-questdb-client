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
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.assertReason;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.binding;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.column;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.missing;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.tableHeader;

public class QwpSchemaBindingTimestampTextTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/timestamp-to-text.tsv";

    @Test
    public void testCorpusUsesExactVarcharBytes() throws Exception {
        InputStream stream = getClass().getResourceAsStream(CORPUS);
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
                    Assert.assertEquals(
                            "# case_id\tinput_kind\tunit\tvalue\tnanos\toutcome\texpected_text", line);
                    headerSeen = true;
                    continue;
                }
                String[] fields = line.split("\t", -1);
                Assert.assertEquals(line, 7, fields.length);
                Assert.assertTrue("LONG".equals(fields[1]) || "INSTANT".equals(fields[1]));
                Assert.assertTrue("VALUE".equals(fields[5]) || "INVALID".equals(fields[5]));
                for (int target : new int[]{ColumnType.STRING, ColumnType.VARCHAR}) {
                    try (QwpTableBuffer buffer = new QwpTableBuffer("t");
                         QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                        QwpSchemaBinding binding = binding(buffer, -1, column("value", target));
                        Runnable append = () -> append(binding, fields);
                        if ("INVALID".equals(fields[5])) {
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, append);
                            buffer.cancelCurrentRow();
                            buffer.rollbackUncommittedColumns();
                            continue;
                        }
                        append.run();
                        buffer.nextRow();
                        QwpTestWireReader reader = tableReader(
                                encoder, encoder.encodeSchema(buffer), 1, QwpConstants.TYPE_VARCHAR);
                        String expectedText = "<EMPTY>".equals(fields[6]) ? "" : fields[6];
                        byte[] expected = expectedText.getBytes(StandardCharsets.UTF_8);
                        Assert.assertEquals(0, reader.u8());
                        Assert.assertEquals(0, reader.i32());
                        Assert.assertEquals(expected.length, reader.i32());
                        Assert.assertArrayEquals(expected, reader.bytes(expected.length));
                        Assert.assertEquals(reader.limit(), reader.position());
                    } catch (AssertionError e) {
                        throw new AssertionError(
                                "case_id=" + fields[0] + ", target=" + target + ": " + e.getMessage(), e);
                    }
                }
                count++;
            }
        }
        Assert.assertTrue("missing corpus header", headerSeen);
        Assert.assertEquals(63, count);
    }

    private static void append(QwpSchemaBinding binding, String[] fields) {
        if ("INSTANT".equals(fields[1])) {
            binding.timestampColumn("value", Instant.ofEpochSecond(
                    Long.parseLong(fields[3]), Integer.parseInt(fields[4])));
        } else if ("LONG".equals(fields[1])) {
            binding.timestampColumn("value", Long.parseLong(fields[3]), ChronoUnit.valueOf(fields[2]));
        } else {
            throw new AssertionError("unknown input kind: " + fields[1]);
        }
    }

    @Test
    public void testUnitsInstantNullDuplicateOmissionRollbackAndReset() {
        for (int target : new int[]{ColumnType.STRING, ColumnType.VARCHAR}) {
            try (QwpTableBuffer buffer = new QwpTableBuffer("t");
                 QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("a", target), column("bad", ColumnType.UUID), column("c", target));
                binding.timestampColumn("a", Long.MIN_VALUE, ChronoUnit.MICROS)
                        .binaryColumn("a", new byte[]{1});
                buffer.nextRow();
                binding.timestampColumn("a", -1, ChronoUnit.NANOS);
                buffer.nextRow();
                buffer.nextRow();
                binding.timestampColumn("a", Instant.ofEpochSecond(-1, 999_999_999));
                buffer.nextRow();
                binding.timestampColumn("a", 1, ChronoUnit.DAYS);
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.timestampColumn("bad", 1, ChronoUnit.MICROS));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.timestampColumn("c", 1000, ChronoUnit.NANOS);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableHeader(encoder, size, 5, 2);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.u8());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.u8());
                assertVarchar(reader, 0x14, "", "1970-01-01T00:00:00.000Z", "1969-12-31T23:59:59.999Z");
                assertVarchar(reader, 0x0f, "1970-01-01T00:00:00.000Z");
                Assert.assertEquals(size, reader.position());

                buffer.reset();
                binding.timestampColumn("a", 0, ChronoUnit.SECONDS);
                buffer.nextRow();
                int resetSize = encoder.encodeSchema(buffer);
                QwpTestWireReader reset = tableHeader(encoder, resetSize, 1, 2);
                Assert.assertEquals("a", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reset.u8());
                Assert.assertEquals("c", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reset.u8());
                assertVarchar(reset, -1, "1970-01-01T00:00:00.000Z");
                assertVarchar(reset, 0x01);
                Assert.assertEquals(resetSize, reset.position());
            }
        }
    }

    @Test
    public void testInferenceDesignatedAndRangeErrors() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
            binding.timestampColumn("value", Long.MIN_VALUE, ChronoUnit.NANOS);
            buffer.nextRow();
            Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP_NANOS, buffer.getColumnDefs()[0].getTypeCode());
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, 0, column("ts", ColumnType.TIMESTAMP_MICRO));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.timestampColumn("ts", 1, ChronoUnit.MICROS));
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.VARCHAR));
            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                    () -> binding.timestampColumn("value", Long.MAX_VALUE, ChronoUnit.DAYS));
            buffer.cancelCurrentRow();
            buffer.rollbackUncommittedColumns();
            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                    () -> binding.timestampColumn("value", (Instant) null));
            buffer.cancelCurrentRow();
            buffer.rollbackUncommittedColumns();
            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                    () -> binding.timestampColumn("value", 1, null));
            buffer.cancelCurrentRow();
            buffer.rollbackUncommittedColumns();
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.timestampColumn("value", 1, ChronoUnit.MONTHS));
            buffer.cancelCurrentRow();
            buffer.rollbackUncommittedColumns();
            binding.timestampColumn("value", 1, ChronoUnit.MICROS);
            binding.timestampColumn("value", Long.MAX_VALUE, ChronoUnit.DAYS);
            buffer.nextRow();
        }
    }

    private static void assertVarchar(QwpTestWireReader reader, int bitmap, String... values) {
        if (bitmap < 0) {
            Assert.assertEquals(0, reader.u8());
        } else {
            Assert.assertEquals(1, reader.u8());
            Assert.assertEquals(bitmap, reader.u8());
        }
        int total = 0;
        StringBuilder joined = new StringBuilder();
        Assert.assertEquals(0, reader.i32());
        for (String value : values) {
            total += value.getBytes(StandardCharsets.UTF_8).length;
            Assert.assertEquals(total, reader.i32());
            joined.append(value);
        }
        Assert.assertArrayEquals(joined.toString().getBytes(StandardCharsets.UTF_8), reader.bytes(total));
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, int rows, byte type) {
        QwpTestWireReader reader = tableHeader(encoder, size, rows, 1);
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(type, reader.u8());
        return reader;
    }
}
