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

public class QwpSchemaBindingLongTimestampTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/long-to-timestamp.tsv";

    @Test
    public void testCorpusUsesExactTargetUnitWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingLongTimestampTest.class.getResourceAsStream(CORPUS);
            Assert.assertNotNull(CORPUS, stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') {
                        continue;
                    }
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 6, fields.length);
                    int target = targetType(fields[2]);
                    byte wireType = wireType(fields[3]);
                    long value = Long.parseLong(fields[1]);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer, -1, column("value", target));
                        binding.longColumn("value", value);
                        buffer.nextRow();
                        int size = encoder.encodeSchema(buffer);
                        QwpTestWireReader reader = tableReader(encoder, size, 1, "value", wireType);
                        if ("<NULL>".equals(fields[4])) {
                            Assert.assertEquals("true", fields[5]);
                            Assert.assertEquals(1, reader.u8());
                            Assert.assertEquals(1, reader.u8());
                            Assert.assertEquals(0, reader.u8());
                        } else {
                            Assert.assertEquals("false", fields[5]);
                            Assert.assertEquals(0, reader.u8());
                            Assert.assertEquals(0, reader.u8());
                            Assert.assertEquals(Long.parseLong(fields[4]), reader.i64());
                        }
                        Assert.assertEquals(size, reader.position());
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertEquals(12, count);
        });
    }

    @Test
    public void testNullDuplicateOmissionRollbackAndReset() throws Exception {
        assertMemoryLeak(() -> {
            for (int target : new int[]{ColumnType.TIMESTAMP_MICRO, ColumnType.TIMESTAMP_NANO}) {
                byte wireType = target == ColumnType.TIMESTAMP_MICRO
                        ? QwpConstants.TYPE_TIMESTAMP
                        : QwpConstants.TYPE_TIMESTAMP_NANOS;
                try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    QwpSchemaBinding binding = binding(buffer, -1, column("value", target));
                    binding.longColumn("value", Long.MIN_VALUE).binaryColumn("value", new byte[]{1});
                    buffer.nextRow();
                    binding.longColumn("value", Long.MIN_VALUE + 1);
                    buffer.nextRow();
                    buffer.nextRow();
                    int size = encoder.encodeSchema(buffer);
                    QwpTestWireReader reader = tableReader(encoder, size, 3, "value", wireType);
                    Assert.assertEquals(1, reader.u8());
                    Assert.assertEquals(5, reader.u8());
                    Assert.assertEquals(0, reader.u8());
                    Assert.assertEquals(Long.MIN_VALUE + 1, reader.i64());
                    Assert.assertEquals(size, reader.position());

                    buffer.reset();
                    binding.longColumn("value", Long.MAX_VALUE);
                    buffer.nextRow();
                    int resetSize = encoder.encodeSchema(buffer);
                    QwpTestWireReader reset = tableReader(encoder, resetSize, 1, "value", wireType);
                    Assert.assertEquals(0, reset.u8());
                    Assert.assertEquals(0, reset.u8());
                    Assert.assertEquals(Long.MAX_VALUE, reset.i64());
                    Assert.assertEquals(resetSize, reset.position());
                }
            }

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("a", ColumnType.TIMESTAMP_MICRO),
                        column("bad", ColumnType.UUID),
                        column("c", ColumnType.TIMESTAMP_NANO));
                binding.longColumn("a", -1);
                buffer.nextRow();
                binding.longColumn("a", 2);
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.longColumn("bad", 3));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.longColumn("c", 4);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableHeader(encoder, size, 2, 2);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP, reader.u8());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP_NANOS, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(2, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(-1, reader.i64());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(4, reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testNativeInferenceAndNamedDesignatedRejection() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
            binding.longColumn("value", Long.MIN_VALUE);
            buffer.nextRow();
            int size = encoder.encodeSchema(buffer);
            QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
            reader.skip(QwpConstants.HEADER_SIZE);
            Assert.assertEquals("t", reader.string());
            Assert.assertEquals(0, reader.u8());
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals("value", reader.string());
            Assert.assertEquals(QwpConstants.TYPE_LONG, reader.u8());
            Assert.assertEquals(1, reader.u8());
            Assert.assertEquals(1, reader.u8());
            Assert.assertEquals(size, reader.position());
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, 0, column("ts", ColumnType.TIMESTAMP_MICRO));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.longColumn("ts", 1));
        }
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, int rows, String name, byte type) {
        QwpTestWireReader reader = tableHeader(encoder, size, rows, 1);
        Assert.assertEquals(name, reader.string());
        Assert.assertEquals(type, reader.u8());
        return reader;
    }

    private static int targetType(String value) {
        return "TIMESTAMP".equals(value) ? ColumnType.TIMESTAMP_MICRO : ColumnType.TIMESTAMP_NANO;
    }

    private static byte wireType(String value) {
        return "TIMESTAMP".equals(value) ? QwpConstants.TYPE_TIMESTAMP : QwpConstants.TYPE_TIMESTAMP_NANOS;
    }
}
