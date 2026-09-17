/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpColumnDef;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.std.Numbers;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.assertReason;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.column;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.known;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.schemaResult;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpSchemaBindingTest {
    @Test
    public void testTimestampInputConformanceCorpus() throws Exception {
        InputStream stream = QwpSchemaBindingTest.class.getResourceAsStream(
                "/io/questdb/client/cutlass/qwp/timestamp-inputs.tsv");
        Assert.assertNotNull(stream);
        try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = lines.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] fields = line.split("\t", -1);
                Assert.assertEquals(line, 7, fields.length);
                try {
                    int targetType = timestampColumnType(fields[4]);
                    byte wireType = timestampWireType(fields[4]);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding rows = rows(buffer, column("ts", targetType));
                        Runnable write;
                        if ("LONG".equals(fields[1])) {
                            long input = Long.parseLong(fields[2]);
                            ChronoUnit unit = ChronoUnit.valueOf(fields[3]);
                            write = () -> rows.timestampColumn("ts", input, unit);
                        } else if ("INSTANT".equals(fields[1])) {
                            Instant input = Instant.parse(fields[2]);
                            Assert.assertEquals("-", fields[3]);
                            write = () -> rows.timestampColumn("ts", input);
                        } else {
                            throw new AssertionError("unknown input_kind: " + fields[1]);
                        }
                        if ("<INVALID>".equals(fields[5])) {
                            Assert.assertEquals("<INVALID>", fields[6]);
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, write);
                            continue;
                        }
                        write.run();
                        buffer.nextRow();
                        QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1, wireType);
                        Assert.assertEquals("supplied timestamp must be bitmap-present", 0, reader.u8());
                        Assert.assertEquals("short timestamp columns use raw encoding", 0, reader.u8());
                        Assert.assertEquals(Long.parseLong(fields[5]), reader.i64());
                        Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                    }
                } catch (AssertionError e) {
                    throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                }
            }
        }
    }

    @Test
    public void testTimestampOverloadValidationAndDuplicateSuppression() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer, column("ts", ColumnType.TIMESTAMP_NANO));
                rows.timestampColumn("ts", Instant.ofEpochSecond(0, 123))
                        .timestampColumn("ts", (Instant) null)
                        .timestampColumn("ts", Long.MAX_VALUE, ChronoUnit.WEEKS);
                buffer.nextRow();
                QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1,
                        QwpConstants.TYPE_TIMESTAMP_NANOS);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(123, reader.i64());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer, column("ts", ColumnType.TIMESTAMP_MICRO));
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> rows.timestampColumn("ts", (Instant) null));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.timestampColumn("ts", 1, ChronoUnit.WEEKS));
            }
            for (int targetType : new int[]{ColumnType.LONG, ColumnType.TIMESTAMP_NANO | 0x10000}) {
                try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    QwpSchemaBinding rows = rows(buffer, column("ts", targetType));
                    assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                            () -> rows.timestampColumn("ts", (Instant) null));
                }
            }
        });
    }

    @Test
    public void testTimestampInstantFailureRollsBackOnlyCurrentRowColumns() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer,
                        column("micro", ColumnType.TIMESTAMP_MICRO),
                        column("late", ColumnType.TIMESTAMP_NANO));
                rows.timestampColumn("micro", Instant.ofEpochSecond(1));
                buffer.nextRow();
                rows.timestampColumn("micro", Instant.ofEpochSecond(2));
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> rows.timestampColumn("late", Instant.MAX));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                rows.timestampColumn("micro", Instant.ofEpochSecond(3))
                        .timestampColumn("late", Instant.ofEpochSecond(0, 4));
                buffer.nextRow();
                QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 2,
                        QwpConstants.TYPE_TIMESTAMP, QwpConstants.TYPE_TIMESTAMP_NANOS);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(1_000_000, reader.i64());
                Assert.assertEquals(3_000_000, reader.i64());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(4, reader.i64());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
            }
        });
    }

    @Test
    public void testTimestampUnitConformanceCorpus() throws Exception {
        InputStream stream = QwpSchemaBindingTest.class.getResourceAsStream(
                "/io/questdb/client/cutlass/qwp/timestamp-units.tsv");
        Assert.assertNotNull(stream);
        try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = lines.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] fields = line.split("\t", -1);
                Assert.assertEquals(line, 6, fields.length);
                try {
                    long input = Long.parseLong(fields[1]);
                    ChronoUnit unit = ChronoUnit.valueOf(fields[2]);
                    int targetType = timestampColumnType(fields[3]);
                    byte wireType = timestampWireType(fields[3]);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding rows = rows(buffer, column("ts", targetType));
                        if ("<INVALID>".equals(fields[4])) {
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                                    () -> rows.timestampColumn("ts", input, unit));
                            continue;
                        }
                        rows.timestampColumn("ts", input, unit);
                        buffer.nextRow();
                        QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1, wireType);
                        Assert.assertEquals("supplied timestamp must be bitmap-present", 0, reader.u8());
                        Assert.assertEquals("short timestamp columns use raw encoding", 0, reader.u8());
                        Assert.assertEquals(Long.parseLong(fields[4]), reader.i64());
                        Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                    }
                } catch (AssertionError e) {
                    throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                }
            }
        }
    }

    @Test
    public void testTimestampValidationDuplicateAndRollback() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer,
                        column("micro", ColumnType.TIMESTAMP_MICRO),
                        column("nano", ColumnType.TIMESTAMP_NANO));
                rows.timestampColumn("micro", 7, ChronoUnit.MICROS)
                        .timestampColumn("micro", Long.MAX_VALUE, null)
                        .timestampColumn("micro", Long.MAX_VALUE, ChronoUnit.SECONDS);
                buffer.nextRow();

                rows.timestampColumn("micro", 8, ChronoUnit.MICROS);
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> rows.timestampColumn("nano", Long.MAX_VALUE, ChronoUnit.MICROS));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();

                rows.timestampColumn("micro", 9, ChronoUnit.MICROS)
                        .timestampColumn("nano", 10, ChronoUnit.NANOS);
                buffer.nextRow();
                QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 2,
                        QwpConstants.TYPE_TIMESTAMP, QwpConstants.TYPE_TIMESTAMP_NANOS);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(7, reader.i64());
                Assert.assertEquals(9, reader.i64());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(10, reader.i64());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
            }
        });
    }

    @Test
    public void testTimestampNullUnsupportedAndBindingLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer,
                        column("micro", ColumnType.TIMESTAMP_MICRO),
                        column("nano", ColumnType.TIMESTAMP_NANO));
                rows.stringColumn("micro", null).stringColumn("nano", null);
                buffer.nextRow();
                rows.stringColumn("micro", null);
                buffer.nextRow();
                QwpTestWireReader nulls = tableReader(encoder, encoder.encodeSchema(buffer), 2,
                        QwpConstants.TYPE_TIMESTAMP, QwpConstants.TYPE_TIMESTAMP_NANOS);
                Assert.assertEquals(1, nulls.u8());
                Assert.assertEquals(3, nulls.u8());
                Assert.assertEquals(0, nulls.u8());
                Assert.assertEquals(1, nulls.u8());
                Assert.assertEquals(3, nulls.u8());
                Assert.assertEquals(0, nulls.u8());
                Assert.assertEquals(encoder.getBuffer().getPosition(), nulls.position());

                buffer.reset();
                rows.timestampColumn("nano", 11, ChronoUnit.NANOS);
                buffer.nextRow();
                buffer.clear();
                assertIllegalState(() -> rows.timestampColumn("nano", 12, ChronoUnit.NANOS));
                assertIllegalState(() -> rows.timestampColumn("nano", Instant.EPOCH));
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer, column("x", ColumnType.LONG));
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.timestampColumn("x", Long.MIN_VALUE, ChronoUnit.MICROS));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                rows.timestampColumn("missing", 1, ChronoUnit.MICROS);
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer, column("ts", ColumnType.TIMESTAMP_MICRO));
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> rows.timestampColumn("ts", 1, null));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.timestampColumn("ts", 1, ChronoUnit.WEEKS));
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer, column("ts", ColumnType.TIMESTAMP_MICRO, new byte[]{1}));
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.timestampColumn("ts", 1, ChronoUnit.MICROS));
            }
            for (int targetType : new int[]{ColumnType.TIMESTAMP_MICRO | 0x10000, ColumnType.TIMESTAMP_NANO | 0x10000}) {
                try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    QwpSchemaBinding rows = rows(buffer, column("ts", targetType));
                    assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                            () -> rows.timestampColumn("ts", 1, ChronoUnit.MICROS));
                }
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(buffer,
                        known(0, column("ts", ColumnType.TIMESTAMP_MICRO)));
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.timestampColumn("ts", 1, ChronoUnit.MICROS));
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.timestampColumn("ts", Instant.EPOCH));
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.stringColumn("ts", null));
            }
        });
    }
    @Test
    public void testClassifiesSchemaLessResponses() throws Exception {
        assertCtorReason(QwpSchemaProtocol.RESULT_DENIED, LineSenderSchemaException.Reason.ACCESS_DENIED, "access denied");
        assertCtorReason(QwpSchemaProtocol.RESULT_UNAVAILABLE, LineSenderSchemaException.Reason.SCHEMA_UNAVAILABLE, "temporarily unavailable");
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            new QwpSchemaBinding(buffer, schemaResult(QwpSchemaProtocol.RESULT_MISSING)).longColumn("x", 1);
        }
        assertCtorReason(QwpSchemaProtocol.RESULT_TOO_LARGE, LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, "response limit");
    }

    @Test
    public void testInfersNativeTargetsForMissingSchemaAndColumns() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(buffer, schemaResult(QwpSchemaProtocol.RESULT_MISSING));
                rows.longColumn("l", 1)
                        .stringColumn("s", "x")
                        .symbol("sym", "y")
                        .boolColumn("b", true)
                        .floatColumn("f", 1)
                        .doubleColumn("d", 2)
                        .uuidColumn("u", 3, 4)
                        .binaryColumn("bin", new byte[]{5})
                        .timestampColumn("tm", 6, ChronoUnit.MICROS)
                        .timestampColumn("tn", 7, ChronoUnit.NANOS)
                        .timestampColumn("ti", Instant.EPOCH);
                buffer.nextRow();
                byte[] expected = {
                        QwpConstants.TYPE_LONG, QwpConstants.TYPE_VARCHAR, QwpConstants.TYPE_SYMBOL,
                        QwpConstants.TYPE_BOOLEAN, QwpConstants.TYPE_FLOAT, QwpConstants.TYPE_DOUBLE,
                        QwpConstants.TYPE_UUID, QwpConstants.TYPE_BINARY, QwpConstants.TYPE_TIMESTAMP,
                        QwpConstants.TYPE_TIMESTAMP_NANOS, QwpConstants.TYPE_TIMESTAMP
                };
                QwpColumnDef[] definitions = buffer.getColumnDefs();
                Assert.assertEquals(expected.length, definitions.length);
                for (int i = 0; i < expected.length; i++) {
                    Assert.assertEquals("column " + i, expected[i], definitions[i].getTypeCode());
                }
            }

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(buffer, schemaResult(QwpSchemaProtocol.RESULT_MISSING));
                rows.uuidColumn("u", 0x0102030405060708L, 0x1112131415161718L)
                        .floatColumn("f", Float.intBitsToFloat(0x3fc00000))
                        .stringColumn("s", "quest");
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, -1, -1, 1,
                        QwpConstants.TYPE_UUID, QwpConstants.TYPE_FLOAT, QwpConstants.TYPE_VARCHAR);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0x0102030405060708L, reader.i64());
                Assert.assertEquals(0x1112131415161718L, reader.i64());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0x3fc00000, reader.i32());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0, reader.i32());
                Assert.assertEquals(5, reader.i32());
                Assert.assertEquals("quest", reader.ascii(5));
                Assert.assertEquals(size, reader.position());
            }

            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer, column("known", ColumnType.LONG));
                rows.longColumn("missing", 1)
                        .unsupportedColumn("missing", "INT")
                        .uuidColumn("missing", 2, 3);
                buffer.nextRow();
                LineSenderSchemaException conflict = Assert.assertThrows(
                        LineSenderSchemaException.class,
                        () -> rows.uuidColumn("missing", 4, 5)
                );
                Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, conflict.getReason());
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
            }

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(buffer, schemaResult(QwpSchemaProtocol.RESULT_MISSING));
                rows.longColumn("a", 1);
                buffer.nextRow();
                try {
                    rows.stringColumn("failed_only", new CharSequence() {
                        @Override
                        public char charAt(int index) {
                            if (index == 1) {
                                throw new IllegalStateException("fixture failure");
                            }
                            return 'x';
                        }

                        @Override
                        public int length() {
                            return 2;
                        }

                        @Override
                        public CharSequence subSequence(int start, int end) {
                            throw new UnsupportedOperationException();
                        }
                    });
                    Assert.fail("expected fixture failure");
                } catch (IllegalStateException e) {
                    Assert.assertEquals("fixture failure", e.getMessage());
                }
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                rows.longColumn("a", 3);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, -1, -1, 2, QwpConstants.TYPE_LONG);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(1, reader.i64());
                Assert.assertEquals(3, reader.i64());
                Assert.assertEquals(size, reader.position());
            }

            try (QwpTableBuffer micros = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(micros, schemaResult(QwpSchemaProtocol.RESULT_MISSING));
                rows.designatedTimestamp(1, ChronoUnit.MICROS);
                Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP, micros.getColumnDefs()[0].getTypeCode());
                micros.nextRow();
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.designatedTimestamp(2, ChronoUnit.NANOS));
                micros.cancelCurrentRow();
                micros.rollbackUncommittedColumns();
            }
            try (QwpTableBuffer nanos = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(nanos, schemaResult(QwpSchemaProtocol.RESULT_MISSING));
                rows.designatedTimestamp(1, ChronoUnit.NANOS);
                Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP_NANOS, nanos.getColumnDefs()[0].getTypeCode());
            }
        });
    }

    @Test
    public void testDuplicateUsesFirstValueAcrossInputSetters() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                 QwpSchemaBinding rows = rows(buffer, column("u", ColumnType.UUID), column("l", ColumnType.LONG));
                rows.uuidColumn("u", 1, 2).longColumn("u", 3);
                rows.longColumn("l", 4).stringColumn("l", "not-a-long");
                buffer.nextRow();
                rows.stringColumn("u", "01234567-89ab-cdef-0123-456789abcdef")
                        .stringColumn("u", "malformed");
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 2, QwpConstants.TYPE_UUID, QwpConstants.TYPE_LONG);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(1, reader.i64());
                Assert.assertEquals(2, reader.i64());
                Assert.assertEquals(0x0123456789abcdefL, reader.i64());
                Assert.assertEquals(0x0123456789abcdefL, reader.i64());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(2, reader.u8());
                Assert.assertEquals(4, reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testEmptyAndIncompleteEncodeContracts() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                 QwpSchemaBinding rows = rows(buffer, column("l", ColumnType.LONG));
                Assert.assertEquals(0, encoder.encodeSchema(buffer));
                rows.longColumn("l", 1);
                assertIllegalState(() -> encoder.encodeSchema(buffer));
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                assertLongFrame(encoder, size, 1);
                int position = encoder.getBuffer().getPosition();
                byte[] before = copyFrame(encoder, position);
                buffer.reset();
                Assert.assertEquals(0, encoder.encodeSchema(buffer));
                Assert.assertEquals(position, encoder.getBuffer().getPosition());
                Assert.assertArrayEquals(before, copyFrame(encoder, position));
            }
        });
    }

    @Test
    public void testLifecycleAndNameValidation() throws Exception {
        try (QwpTableBuffer invalid = new QwpTableBuffer("bad\nname")) {
            assertIllegalArgument(() -> new QwpSchemaBinding(invalid, known(column("x", ColumnType.LONG))));
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding rows = rows(buffer, column("x", ColumnType.LONG));
            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, () -> rows.longColumn("bad\nname", 1));
            rows.longColumn("missing", 1);
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding stale = rows(buffer, column("x", ColumnType.LONG));
            buffer.clear();
            assertIllegalState(() -> stale.longColumn("x", 1));
            assertIllegalState(() -> stale.timestampColumn("x", Instant.EPOCH));
        }
    }

    @Test
    public void testSmallIntegerNumericConformanceCorpus() throws Exception {
        InputStream stream = QwpSchemaBindingTest.class.getResourceAsStream(
                "/io/questdb/client/cutlass/qwp/small-integer-to-numeric.tsv");
        Assert.assertNotNull(stream);
        int count = 0;
        try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = lines.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#') {
                    continue;
                }
                String[] fields = line.split("\t", -1);
                Assert.assertEquals(line, 5, fields.length);
                try {
                    long input = Long.parseLong(fields[2]);
                    int targetType = numericColumnType(fields[3]);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding rows = rows(buffer, column("n", targetType));
                        Runnable append = () -> appendSmallInteger(rows, fields[1], input, "n");
                        if ("<INVALID>".equals(fields[4])) {
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, append);
                        } else {
                            append.run();
                            buffer.nextRow();
                            QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1,
                                    numericWireType(fields[3]));
                            if ("<NULL>".equals(fields[4])) {
                                assertNumericNull(reader, fields[3]);
                            } else {
                                Assert.assertEquals(0, reader.u8());
                                assertNumericValue(reader, fields[3], fields[4]);
                            }
                            Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                        }
                    }
                } catch (AssertionError e) {
                    throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                }
                count++;
            }
        }
        Assert.assertEquals(64, count);
    }

    @Test
    public void testSmallIntegerTextConformanceCorpusUsesTargetWire() throws Exception {
        InputStream stream = QwpSchemaBindingTest.class.getResourceAsStream(
                "/io/questdb/client/cutlass/qwp/small-integer-to-text.tsv");
        Assert.assertNotNull(stream);
        int count = 0;
        try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = lines.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#') {
                    continue;
                }
                String[] fields = line.split("\t", -1);
                Assert.assertEquals(line, 6, fields.length);
                try {
                    long input = Long.parseLong(fields[2]);
                    byte wireType = textWireType(fields[3]);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding rows = rows(buffer, column("value", textColumnType(fields[3])));
                        appendSmallInteger(rows, fields[1], input, "value");
                        buffer.nextRow();
                        QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1, wireType);
                        if ("<NULL>".equals(fields[4])) {
                            Assert.assertEquals("<NULL>", fields[5]);
                            Assert.assertEquals("NULL bitmap must be present", 1, reader.u8());
                            Assert.assertEquals("row zero must be NULL", 1, reader.u8());
                            if (wireType == QwpConstants.TYPE_SYMBOL) {
                                Assert.assertEquals(0, reader.varint());
                            } else {
                                Assert.assertEquals(0, reader.i32());
                            }
                        } else {
                            byte[] expected = hexBytes(fields[4]);
                            Assert.assertEquals(fields[5], new String(expected, StandardCharsets.UTF_8));
                            Assert.assertEquals("value must be bitmap-present", 0, reader.u8());
                            if (wireType == QwpConstants.TYPE_SYMBOL) {
                                Assert.assertEquals(1, reader.varint());
                                Assert.assertEquals(fields[5], reader.string());
                                Assert.assertEquals(0, reader.varint());
                            } else {
                                Assert.assertEquals(0, reader.i32());
                                Assert.assertEquals(expected.length, reader.i32());
                                for (byte value : expected) {
                                    Assert.assertEquals(value & 0xff, reader.u8());
                                }
                            }
                        }
                        Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                    }
                } catch (AssertionError e) {
                    throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                }
                count++;
            }
        }
        Assert.assertEquals(30, count);
    }

    @Test
    public void testSmallIntegerDecimalConformanceCorpusUsesTargetWire() throws Exception {
        InputStream stream = QwpSchemaBindingTest.class.getResourceAsStream(
                "/io/questdb/client/cutlass/qwp/small-integer-to-decimal.tsv");
        Assert.assertNotNull(stream);
        boolean[][] sourceTargets = new boolean[3][6];
        int count = 0;
        try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            Assert.assertEquals(
                    "# case_id\tinput_type\tinput\ttarget_type\ttarget_precision\ttarget_scale\toutcome"
                            + "\texpected_ll_hex\texpected_lh_hex\texpected_hl_hex\texpected_hh_hex\texpected_sql",
                    lines.readLine()
            );
            String line;
            while ((line = lines.readLine()) != null) {
                String[] fields = line.split("\t", -1);
                Assert.assertEquals(line, 12, fields.length);
                try {
                    long input = Long.parseLong(fields[2]);
                    int precision = Integer.parseInt(fields[4]);
                    int scale = Integer.parseInt(fields[5]);
                    Assert.assertEquals(decimalStorageType(precision), fields[3]);
                    sourceTargets[smallIntegerSourceIndex(fields[1])][decimalStorageIndex(fields[3])] = true;
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding rows = rows(
                                buffer,
                                column("value", ColumnType.getDecimalType(precision, scale))
                        );
                        Runnable append = () -> appendSmallInteger(rows, fields[1], input, "value");
                        if ("INVALID".equals(fields[6])) {
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, append);
                            rollbackCurrentRow(buffer);
                        } else {
                            append.run();
                            buffer.nextRow();
                            int size = encoder.encodeSchema(buffer);
                            QwpTestWireReader reader = tableReader(
                                    encoder,
                                    size,
                                    1,
                                    decimalWireType(fields[3])
                            );
                            if ("NULL".equals(fields[6])) {
                                Assert.assertEquals("NULL", fields[11]);
                                Assert.assertEquals(1, reader.u8());
                                Assert.assertEquals(1, reader.u8());
                            } else {
                                Assert.assertEquals("VALUE", fields[6]);
                                Assert.assertEquals(0, reader.u8());
                            }
                            Assert.assertEquals(scale, reader.u8());
                            if ("VALUE".equals(fields[6])) {
                                int limbs = decimalWireLongCount(fields[3]);
                                for (int i = 0; i < limbs; i++) {
                                    Assert.assertEquals(parseHexLong(fields[7 + i]), reader.i64());
                                }
                                for (int i = limbs; i < 4; i++) {
                                    Assert.assertEquals("-", fields[7 + i]);
                                }
                            }
                            Assert.assertEquals(size, reader.position());
                        }
                    }
                } catch (AssertionError e) {
                    throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                }
                count++;
            }
        }
        Assert.assertEquals(30, count);
        for (int source = 0; source < sourceTargets.length; source++) {
            for (int target = 0; target < sourceTargets[source].length; target++) {
                Assert.assertTrue("missing source/target coverage " + source + '/' + target,
                        sourceTargets[source][target]);
            }
        }
    }

    @Test
    public void testIntegerTemporalConformanceCorpusUsesTargetWire() throws Exception {
        InputStream stream = QwpSchemaBindingTest.class.getResourceAsStream(
                "/io/questdb/client/cutlass/qwp/integer-temporal-conversions.tsv");
        Assert.assertNotNull(stream);
        int count = 0;
        try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = lines.readLine()) != null) {
                if (line.isEmpty() || line.charAt(0) == '#') {
                    continue;
                }
                String[] fields = line.split("\t", -1);
                Assert.assertEquals(line, 5, fields.length);
                try {
                    long input = Long.parseLong(fields[2]);
                    int targetType = temporalColumnType(fields[3]);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding rows = rows(buffer, column("value", targetType));
                        appendIntegerTemporal(rows, fields[1], input, "value");
                        buffer.nextRow();
                        QwpTestWireReader reader = tableReader(
                                encoder,
                                encoder.encodeSchema(buffer),
                                1,
                                temporalWireType(fields[3])
                        );
                        if ("<NULL>".equals(fields[4])) {
                            Assert.assertEquals("NULL bitmap must be present", 1, reader.u8());
                            Assert.assertEquals("row zero must be NULL", 1, reader.u8());
                        } else {
                            Assert.assertEquals("value must be bitmap-present", 0, reader.u8());
                        }
                        if (!"DATE".equals(fields[3])) {
                            Assert.assertEquals("short timestamp columns use raw encoding", 0, reader.u8());
                        }
                        if (!"<NULL>".equals(fields[4])) {
                            Assert.assertEquals(Long.parseLong(fields[4]), reader.i64());
                        }
                        Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                    }
                } catch (AssertionError e) {
                    throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                }
                count++;
            }
        }
        Assert.assertEquals(24, count);
    }

    @Test
    public void testSmallIntegerDuplicateFirstAndFailureRollback() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer,
                        column("a", ColumnType.LONG),
                        column("only_b", ColumnType.SHORT),
                        column("bad", ColumnType.BYTE),
                        column("c", ColumnType.DOUBLE));
                rows.byteColumn("a", (byte) -1).intColumn("a", Integer.MIN_VALUE);
                buffer.nextRow();

                rows.shortColumn("only_b", (short) 5);
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> rows.shortColumn("bad", (short) 128));
                rollbackCurrentRow(buffer);

                rows.intColumn("c", 3);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 2,
                        QwpConstants.TYPE_LONG, QwpConstants.TYPE_DOUBLE);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(2, reader.u8());
                Assert.assertEquals(-1, reader.i64());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(Double.doubleToRawLongBits(3), reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testIntMinIsNullWithCompanionMissingRowsAcrossNumericTargets() throws Exception {
        assertMemoryLeak(() -> {
            for (String target : new String[]{"BYTE", "SHORT", "INT", "LONG", "FLOAT", "DOUBLE"}) {
                try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    QwpSchemaBinding rows = rows(buffer, column("n", numericColumnType(target)));
                    rows.intColumn("n", Integer.MIN_VALUE);
                    buffer.nextRow();
                    buffer.nextRow();
                    rows.intColumn("n", 7);
                    buffer.nextRow();
                    int size = encoder.encodeSchema(buffer);
                    QwpTestWireReader reader = tableReader(encoder, size, 3, numericWireType(target));
                    Assert.assertEquals(target, 1, reader.u8());
                    Assert.assertEquals(target, 3, reader.u8());
                    if ("FLOAT".equals(target)) {
                        assertNumericValue(reader, target, "0x40e00000");
                    } else if ("DOUBLE".equals(target)) {
                        assertNumericValue(reader, target, "0x401c000000000000");
                    } else {
                        assertNumericValue(reader, target, "7");
                    }
                    Assert.assertEquals(size, reader.position());
                }
            }
        });
    }

    @Test
    public void testIntToIpv4UsesTargetNativeWireAndNulls() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer, column("ip", ColumnType.IPv4));
                rows.intColumn("ip", Integer.MIN_VALUE);
                buffer.nextRow();
                rows.intColumn("ip", 0);
                buffer.nextRow();
                rows.intColumn("ip", 1);
                buffer.nextRow();
                rows.intColumn("ip", Integer.MIN_VALUE + 1);
                buffer.nextRow();
                rows.intColumn("ip", Integer.MAX_VALUE);
                buffer.nextRow();
                rows.intColumn("ip", -1);
                buffer.nextRow();
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 7, QwpConstants.TYPE_IPv4);
                Assert.assertEquals("NULL bitmap must be present", 1, reader.u8());
                Assert.assertEquals("INT null, IPv4 zero and omission must be NULL", 0x43, reader.u8());
                Assert.assertEquals(1, reader.i32());
                Assert.assertEquals(Integer.MIN_VALUE + 1, reader.i32());
                Assert.assertEquals(Integer.MAX_VALUE, reader.i32());
                Assert.assertEquals(-1, reader.i32());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testSmallIntegerMissingSchemaUsesNativeWireTypes() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(buffer, schemaResult(QwpSchemaProtocol.RESULT_MISSING));
                rows.byteColumn("b", (byte) -1).shortColumn("s", (short) 2).intColumn("i", 3);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, -1, -1, 1,
                        QwpConstants.TYPE_BYTE, QwpConstants.TYPE_SHORT, QwpConstants.TYPE_INT);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0xff, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(2, reader.u16());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(3, reader.i32());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testSmallIntegerUnsupportedParameterizedDesignatedAndStaleTargets() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding rows = rows(buffer,
                    column("uuid", ColumnType.UUID),
                    column("ip", ColumnType.IPv4),
                    column("future", ColumnType.INT, new byte[]{1}),
                    column("future_date", ColumnType.DATE, new byte[]{1}),
                    column("future_text", ColumnType.VARCHAR, new byte[]{1}),
                    column("future_decimal", ColumnType.getDecimalType(18, 2), new byte[]{1}),
                    column("future_ipv4", ColumnType.IPv4, new byte[]{1}));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.byteColumn("uuid", (byte) 1));
            rollbackCurrentRow(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.byteColumn("ip", (byte) 1));
            rollbackCurrentRow(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.shortColumn("ip", (short) 2));
            rollbackCurrentRow(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.shortColumn("future", (short) 2));
            rollbackCurrentRow(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.byteColumn("future_date", (byte) 3));
            rollbackCurrentRow(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.intColumn("future_text", 4));
            rollbackCurrentRow(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.shortColumn("future_decimal", (short) 5));
            rollbackCurrentRow(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.intColumn("future_ipv4", 6));
            rollbackCurrentRow(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.intColumn("uuid", Integer.MIN_VALUE));
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding rows = new QwpSchemaBinding(buffer,
                    known(0, column("designated", ColumnType.TIMESTAMP_MICRO)));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.intColumn("designated", 1));
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding stale = rows(buffer, column("n", ColumnType.LONG));
            buffer.clear();
            assertIllegalState(() -> stale.byteColumn("n", (byte) 1));
            assertIllegalState(() -> stale.shortColumn("n", (short) 2));
            assertIllegalState(() -> stale.intColumn("n", 3));
        }
    }

    @Test
    public void testLongNumericConformanceCorpus() throws Exception {
        InputStream stream = QwpSchemaBindingTest.class.getResourceAsStream(
                "/io/questdb/client/cutlass/qwp/long-to-numeric.tsv");
        Assert.assertNotNull(stream);
        try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = lines.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] fields = line.split("\t", -1);
                Assert.assertEquals(line, 4, fields.length);
                try {
                    long input = Long.parseLong(fields[1]);
                    int targetType = numericColumnType(fields[2]);
                    byte wireType = numericWireType(fields[2]);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                         QwpSchemaBinding rows = rows(buffer, column("n", targetType));
                        if ("<INVALID>".equals(fields[3])) {
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                                    () -> rows.longColumn("n", input));
                            continue;
                        }
                        rows.longColumn("n", input);
                buffer.nextRow();
                        int size = encoder.encodeSchema(buffer);
                        QwpTestWireReader reader = tableReader(encoder, size, 1, wireType);
                        if ("<NULL>".equals(fields[3])) {
                            if (input == Long.MIN_VALUE) {
                                assertNumericNull(reader, fields[2]);
                            } else {
                                Assert.assertEquals("INT sentinel is carried as a non-null wire value", 0, reader.u8());
                                Assert.assertEquals(Integer.MIN_VALUE, reader.i32());
                            }
                        } else {
                            Assert.assertEquals(0, reader.u8());
                            assertNumericValue(reader, fields[2], fields[3]);
                        }
                        Assert.assertEquals(size, reader.position());
                    }
                } catch (AssertionError e) {
                    throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                }
            }
        }
    }

    @Test
    public void testLongNumericDuplicateFirstAndFailureRollback() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                 QwpSchemaBinding rows = rows(buffer, column("b", ColumnType.BYTE), column("i", ColumnType.INT));
                rows.longColumn("b", 7).longColumn("b", 128).stringColumn("b", "unsupported");
                rows.longColumn("i", 1);
                buffer.nextRow();
                rows.longColumn("i", 2);
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, () -> rows.longColumn("b", 128));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                rows.longColumn("b", 3).longColumn("i", 4);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 2, QwpConstants.TYPE_BYTE, QwpConstants.TYPE_INT);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(7, reader.u8());
                Assert.assertEquals(3, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(1, reader.i32());
                Assert.assertEquals(4, reader.i32());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testLongRangeDiagnosticIsReadableAndDoesNotEchoValue() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding rows = rows(buffer, column("b", ColumnType.BYTE));
            try {
                rows.longColumn("b", 9_876_543_210L);
                Assert.fail("expected range failure");
            } catch (LineSenderSchemaException e) {
                Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, e.getReason());
                Assert.assertFalse(e.isRetryable());
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("table=t"));
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("column=b"));
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("inputType=LONG"));
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("targetType=BYTE(" + ColumnType.BYTE + ")"));
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("outside target type range"));
                Assert.assertFalse(e.getMessage(), e.getMessage().contains("9876543210"));
            }
        }
    }

    @Test
    public void testLongNullRejectsUuidUnlessDuplicate() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                 QwpSchemaBinding rows = rows(buffer, column("u", ColumnType.UUID));
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.longColumn("u", Long.MIN_VALUE));
                rows.uuidColumn("u", 1, 2).longColumn("u", Long.MIN_VALUE);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 1, QwpConstants.TYPE_UUID);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(1, reader.i64());
                Assert.assertEquals(2, reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testLongNumericDuplicateFirstAcrossEveryTarget() throws Exception {
        assertMemoryLeak(() -> {
            byte[][] schema = {
                    column("b", ColumnType.BYTE), column("s", ColumnType.SHORT),
                    column("i", ColumnType.INT), column("l", ColumnType.LONG),
                    column("f", ColumnType.FLOAT), column("d", ColumnType.DOUBLE)};
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                 QwpSchemaBinding rows = rows(buffer, schema);
                String[] names = {"b", "s", "i", "l", "f", "d"};
                for (String name : names) {
                    rows.longColumn(name, 7).stringColumn(name, "not-numeric").uuidColumn(name, 1, 2);
                }
                rows.longColumn("b", 128).longColumn("s", 32768).longColumn("i", 2147483648L);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 1, QwpConstants.TYPE_BYTE, QwpConstants.TYPE_SHORT,
                        QwpConstants.TYPE_INT, QwpConstants.TYPE_LONG, QwpConstants.TYPE_FLOAT, QwpConstants.TYPE_DOUBLE);
                Assert.assertEquals(0, reader.u8()); Assert.assertEquals(7, reader.u8());
                Assert.assertEquals(0, reader.u8()); Assert.assertEquals(7, reader.u16());
                Assert.assertEquals(0, reader.u8()); Assert.assertEquals(7, reader.i32());
                Assert.assertEquals(0, reader.u8()); Assert.assertEquals(7, reader.i64());
                Assert.assertEquals(0, reader.u8()); Assert.assertEquals(Float.floatToRawIntBits(7), reader.i32());
                Assert.assertEquals(0, reader.u8()); Assert.assertEquals(Double.doubleToRawLongBits(7), reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testNumericFailureRemovesColumnsIntroducedOnlyInFailedRow() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                 QwpSchemaBinding rows = rows(buffer, column("a", ColumnType.LONG), column("only_b", ColumnType.BYTE),
                         column("bad", ColumnType.SHORT));
                rows.longColumn("a", 1);
                buffer.nextRow();
                rows.longColumn("only_b", 5);
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, () -> rows.longColumn("bad", 32768));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                rows.longColumn("a", 3);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 2, QwpConstants.TYPE_LONG);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(1, reader.i64());
                Assert.assertEquals(3, reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testNumericExtensionRejectedOnlyWhenUsed() throws Exception {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding rows = rows(buffer,
                column("future_float", ColumnType.FLOAT, new byte[]{1}), column("plain", ColumnType.BYTE));
            rows.longColumn("plain", 1);
                buffer.nextRow();
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.longColumn("future_float", 2));
        }
    }

    @Test
    public void testNullStringSupportsAllNumericTargets() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                 QwpSchemaBinding rows = rows(buffer,
                         column("b", ColumnType.BYTE), column("s", ColumnType.SHORT),
                         column("i", ColumnType.INT), column("l", ColumnType.LONG),
                         column("f", ColumnType.FLOAT), column("d", ColumnType.DOUBLE));
                rows.stringColumn("b", null).stringColumn("s", null).stringColumn("i", null)
                        .stringColumn("l", null).stringColumn("f", null).stringColumn("d", null);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 1, QwpConstants.TYPE_BYTE, QwpConstants.TYPE_SHORT,
                        QwpConstants.TYPE_INT, QwpConstants.TYPE_LONG, QwpConstants.TYPE_FLOAT, QwpConstants.TYPE_DOUBLE);
                assertNumericNull(reader, "BYTE");
                assertNumericNull(reader, "SHORT");
                assertNumericNull(reader, "INT");
                assertNumericNull(reader, "LONG");
                assertNumericNull(reader, "FLOAT");
                assertNumericNull(reader, "DOUBLE");
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testNullStringSupportsLongAndUuidTargets() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                 QwpSchemaBinding rows = rows(buffer, column("u", ColumnType.UUID), column("l", ColumnType.LONG));
                rows.stringColumn("u", null).stringColumn("l", null);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 1, QwpConstants.TYPE_UUID, QwpConstants.TYPE_LONG);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testParameterExtensionRejectedOnlyWhenColumnUsed() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                 QwpSchemaBinding rows = rows(buffer, column("future", ColumnType.LONG, new byte[]{1}), column("plain", ColumnType.LONG));
                rows.longColumn("plain", 1);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                assertLongFrame(encoder, size, 1);
                buffer.reset();
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> rows.longColumn("future", 2));
            }
        });
    }

    @Test
    public void testUnknownFullTypeFlagsRejectedOnlyWhenUsed() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                 QwpSchemaBinding rows = rows(buffer, column("future", ColumnType.LONG | 0x10000), column("plain", ColumnType.LONG));
                rows.longColumn("plain", 7);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                assertLongFrame(encoder, size, 7);
                buffer.reset();
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> rows.longColumn("future", 2));
            }
        });
    }

    @Test
    public void testCancelAndResetRemoveOnlyIntendedRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                 QwpSchemaBinding rows = rows(buffer, column("l", ColumnType.LONG));
                rows.longColumn("l", 1);
                buffer.nextRow();
                rows.longColumn("l", 2);
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                rows.longColumn("l", 3);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                assertLongFrame(encoder, size, 1, 3);
                buffer.reset();
                rows.longColumn("l", 4);
                buffer.nextRow();
                size = encoder.encodeSchema(buffer);
                assertLongFrame(encoder, size, 4);
            }
        });
    }

    @Test
    public void testOwnerRollbackAfterSetterFailurePreservesCompletedRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                 QwpSchemaBinding rows = rows(buffer, column("u", ColumnType.UUID), column("l", ColumnType.LONG));
                rows.longColumn("l", 1);
                buffer.nextRow();
                rows.longColumn("l", 2);
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> rows.stringColumn("u", "not-a-uuid"));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                rows.stringColumn("u", "01234567-89ab-cdef-0123-456789abcdef");
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 2, QwpConstants.TYPE_LONG, QwpConstants.TYPE_UUID);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(2, reader.u8());
                Assert.assertEquals(1, reader.i64());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(0x0123456789abcdefL, reader.i64());
                Assert.assertEquals(0x0123456789abcdefL, reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testAttachmentLifecycleAndStaleBindingGuards() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaResponse schema = known(column("u", ColumnType.UUID), column("l", ColumnType.LONG));
                QwpSchemaBinding old = new QwpSchemaBinding(buffer, schema);
                assertIllegalState(() -> new QwpSchemaBinding(buffer, schema));
                buffer.reset();
                old.longColumn("l", 1);
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                buffer.clear();

                QwpSchemaBinding current = new QwpSchemaBinding(buffer, schema);
                current.longColumn("l", 7);
                assertIllegalState(() -> old.longColumn("l", 8));
                assertIllegalState(() -> old.stringColumn("u", "01234567-89ab-cdef-0123-456789abcdef"));
                assertIllegalState(() -> old.uuidColumn("u", 1, 2));
                buffer.nextRow();
                try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                    int size = encoder.encodeSchema(buffer);
                    QwpTestWireReader reader = tableReader(encoder, size, 1, QwpConstants.TYPE_LONG);
                    Assert.assertEquals(0, reader.u8());
                    Assert.assertEquals(7, reader.i64());
                    Assert.assertEquals(size, reader.position());
                }
            }
        });
    }

    @Test
    public void testBindingRequiresEmptyLayout() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            buffer.getOrCreateColumn("l", QwpConstants.TYPE_LONG, true).addLong(1);
            buffer.nextRow();
            buffer.reset();
            Assert.assertEquals(1, buffer.getColumnCount());
            assertIllegalState(() -> new QwpSchemaBinding(buffer, known(column("l", ColumnType.LONG))));
        }
    }

    @Test
    public void testBoundEncodingGuardsDoNotMutateEncoder() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer sentinel = new QwpTableBuffer("s");
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                sentinel.getOrCreateColumn("x", QwpConstants.TYPE_LONG, true).addLong(9);
                sentinel.nextRow();
                encoder.encode(sentinel);
                QwpSchemaBinding binding = rows(buffer, column("l", ColumnType.LONG));
                assertEncoderUnchanged(encoder, () -> encoder.encode(buffer));
                assertEncoderUnchanged(encoder, () -> encoder.encodeWithDeltaDict(
                        buffer, new GlobalSymbolDictionary(), -1, -1));
                assertEncoderUnchangedIllegalArgument(encoder, () -> encoder.encodeSchema(
                        buffer, binding.getTableId() + 1, binding.getMetadataVersion()));
                binding.longColumn("l", 1);
                assertEncoderUnchanged(encoder, () -> encoder.encodeSchema(buffer));
                buffer.nextRow();
                int derivedSize = encoder.encodeSchema(buffer);
                byte[] derived = copyFrame(encoder, derivedSize);
                int explicitSize = encoder.encodeSchema(buffer, binding.getTableId(), binding.getMetadataVersion());
                Assert.assertEquals(derivedSize, explicitSize);
                Assert.assertArrayEquals(derived, copyFrame(encoder, explicitSize));

                encoder.beginMessage(1, new GlobalSymbolDictionary(), -1, -1);
                assertEncoderUnchanged(encoder, () -> encoder.addTable(buffer));
            }
        });
    }

    @Test
    public void testEmptyBoundEncodingContracts() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = rows(buffer, column("l", ColumnType.LONG));
                Assert.assertEquals(0, encoder.encodeSchema(buffer));
                Assert.assertEquals(0, encoder.getBuffer().getPosition());
                encoder.beginSchemaMessage(1, new GlobalSymbolDictionary(), -1, -1);
                assertEncoderUnchanged(encoder, () -> encoder.addSchemaTable(
                        buffer, binding.getTableId(), binding.getMetadataVersion()));
            }
        });
    }

    @Test
    public void testUuidConformanceCorpus() throws Exception {
        InputStream stream = QwpSchemaBindingTest.class.getResourceAsStream(
                "/io/questdb/client/cutlass/qwp/uuid-string-conformance.tsv");
        Assert.assertNotNull(stream);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] fields = line.split("\t", -1);
                Assert.assertEquals(line, 3, fields.length);
                CharSequence input = "<NULL>".equals(fields[1]) ? null : fields[1];
                try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding rows = rows(buffer, column("u", ColumnType.UUID));
                    if ("<INVALID>".equals(fields[2])) {
                        assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, () -> rows.stringColumn("u", input));
                    } else {
                        rows.stringColumn("u", input);
                buffer.nextRow();
                    }
                }
            }
        }
    }

    @Test
    public void testStringAndTypedUuidEncodeSameLimbs() throws Exception {
        assertMemoryLeak(() -> {
            long hi = 0x0123456789abcdefL;
            long lo = 0xfedcba9876543210L;
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                 QwpSchemaBinding rows = rows(buffer, column("u", ColumnType.UUID));
                rows.stringColumn("u", "01234567-89ab-cdef-fedc-ba9876543210");
                buffer.nextRow();
                rows.uuidColumn("u", lo, hi);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 2, QwpConstants.TYPE_UUID);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(lo, reader.i64());
                Assert.assertEquals(hi, reader.i64());
                Assert.assertEquals(lo, reader.i64());
                Assert.assertEquals(hi, reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testIpv4ConformanceCorpusUsesExactTargetWire() throws Exception {
        InputStream stream = QwpSchemaBindingTest.class.getResourceAsStream(
                "/io/questdb/client/cutlass/qwp/ipv4-conversions.tsv");
        Assert.assertNotNull(stream);
        int count = 0;
        try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            Assert.assertEquals("# case_id\tinput_kind\tinput\toutcome\texpected_hex\texpected_text", lines.readLine());
            String line;
            while ((line = lines.readLine()) != null) {
                String[] fields = line.split("\t", -1);
                Assert.assertEquals(line, 6, fields.length);
                for (int targetType : new int[]{ColumnType.IPv4, ColumnType.STRING, ColumnType.VARCHAR}) {
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding rows = rows(buffer, column("ip", targetType));
                        Runnable append = "INT".equals(fields[1])
                                ? () -> rows.ipv4Column("ip", Integer.parseInt(fields[2]))
                                : () -> rows.ipv4Column("ip", fields[2]);
                        if ("INVALID".equals(fields[3])) {
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, append);
                            continue;
                        }
                        append.run();
                        buffer.nextRow();
                        byte wireType = targetType == ColumnType.IPv4
                                ? QwpConstants.TYPE_IPv4
                                : QwpConstants.TYPE_VARCHAR;
                        QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1, wireType);
                        if ("NULL".equals(fields[3])) {
                            Assert.assertEquals(1, reader.u8());
                            Assert.assertEquals(1, reader.u8());
                            if (wireType == QwpConstants.TYPE_VARCHAR) {
                                Assert.assertEquals(0, reader.i32());
                            }
                        } else {
                            Assert.assertEquals("VALUE", fields[3]);
                            Assert.assertEquals(0, reader.u8());
                            if (wireType == QwpConstants.TYPE_IPv4) {
                                Assert.assertEquals((int) Long.parseUnsignedLong(fields[4], 16), reader.i32());
                            } else {
                                byte[] expected = fields[5].getBytes(StandardCharsets.UTF_8);
                                Assert.assertEquals(0, reader.i32());
                                Assert.assertEquals(expected.length, reader.i32());
                                Assert.assertEquals(fields[5], reader.ascii(expected.length));
                            }
                        }
                        Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + fields[0] + ", target="
                                + ColumnType.nameOf(targetType) + ": " + e.getMessage(), e);
                    }
                }
                count++;
            }
        }
        Assert.assertEquals(16, count);
    }

    @Test
    public void testIpv4DuplicateRollbackNullOmissionAndInference() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer,
                        column("ip", ColumnType.IPv4), column("bad", ColumnType.LONG));
                rows.ipv4Column("ip", 0x01020304).ipv4Column("ip", "not-an-ip");
                buffer.nextRow();

                rows.ipv4Column("ip", 0x05060708);
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.ipv4Column("bad", 0x090a0b0c));
                rollbackCurrentRow(buffer);

                rows.ipv4Column("ip", 0);
                buffer.nextRow();
                buffer.nextRow();
                rows.ipv4Column("ip", ".0.0.0.0.");
                buffer.nextRow();
                rows.ipv4Column("ip", 0x0d0e0f10);
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 5, QwpConstants.TYPE_IPv4);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(0x0e, reader.u8());
                Assert.assertEquals(0x01020304, reader.i32());
                Assert.assertEquals(0x0d0e0f10, reader.i32());
                Assert.assertEquals(size, reader.position());
            }

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(buffer, schemaResult(QwpSchemaProtocol.RESULT_MISSING));
                rows.ipv4Column("ignored", (CharSequence) null)
                        .ipv4Column("ip", "192.168.1.1");
                buffer.nextRow();
                Assert.assertEquals(1, buffer.getColumnDefs().length);
                Assert.assertEquals(QwpConstants.TYPE_IPv4, buffer.getColumnDefs()[0].getTypeCode());
                QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), -1, -1, 1,
                        QwpConstants.TYPE_IPv4);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0xc0a80101, reader.i32());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
            }

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(buffer, schemaResult(QwpSchemaProtocol.RESULT_MISSING));
                rows.ipv4Column("ip", 0);
                buffer.nextRow();
                Assert.assertEquals(1, buffer.getColumnDefs().length);
                Assert.assertEquals(QwpConstants.TYPE_IPv4, buffer.getColumnDefs()[0].getTypeCode());
                QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), -1, -1, 1,
                        QwpConstants.TYPE_IPv4);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
            }
        });
    }

    @Test
    public void testIpv4RejectsUnsupportedParameterizedAndDesignatedTargets() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding rows = rows(buffer,
                    column("long_value", ColumnType.LONG),
                    column("parameterized", ColumnType.IPv4, new byte[]{1}));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.ipv4Column("long_value", 1));
            rollbackCurrentRow(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.ipv4Column("long_value", "not-an-ip"));
            rollbackCurrentRow(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.ipv4Column("parameterized", "1.2.3.4"));
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding rows = new QwpSchemaBinding(buffer,
                    known(0, column("ts", ColumnType.TIMESTAMP_MICRO)));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.ipv4Column("ts", 1));
        }
    }

    @Test
    public void testLong256ConformanceCorpusUsesExactTargetWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingTest.class.getResourceAsStream(
                    "/io/questdb/client/cutlass/qwp/long256-conversions.tsv");
            Assert.assertNotNull(stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                Assert.assertEquals("# case_id\tinput_l0\tinput_l1\tinput_l2\tinput_l3\toutcome\texpected_text", lines.readLine());
                String line;
                while ((line = lines.readLine()) != null) {
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 7, fields.length);
                    long l0 = Long.parseLong(fields[1]);
                    long l1 = Long.parseLong(fields[2]);
                    long l2 = Long.parseLong(fields[3]);
                    long l3 = Long.parseLong(fields[4]);
                    for (int targetType : new int[]{ColumnType.LONG256, ColumnType.STRING, ColumnType.VARCHAR}) {
                        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                             QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                            QwpSchemaBinding rows = rows(buffer, column("value", targetType));
                            rows.long256Column("value", l0, l1, l2, l3);
                            buffer.nextRow();
                            byte wireType = targetType == ColumnType.LONG256
                                    ? QwpConstants.TYPE_LONG256
                                    : QwpConstants.TYPE_VARCHAR;
                            int size = encoder.encodeSchema(buffer);
                            QwpTestWireReader reader = tableReader(encoder, size, 1, wireType);
                            if ("NULL".equals(fields[5])) {
                                Assert.assertEquals("-", fields[6]);
                                Assert.assertEquals(1, reader.u8());
                                Assert.assertEquals(1, reader.u8());
                                if (wireType == QwpConstants.TYPE_VARCHAR) {
                                    Assert.assertEquals(0, reader.i32());
                                }
                            } else {
                                Assert.assertEquals("VALUE", fields[5]);
                                Assert.assertEquals(0, reader.u8());
                                if (wireType == QwpConstants.TYPE_LONG256) {
                                    Assert.assertEquals(l0, reader.i64());
                                    Assert.assertEquals(l1, reader.i64());
                                    Assert.assertEquals(l2, reader.i64());
                                    Assert.assertEquals(l3, reader.i64());
                                } else {
                                    Assert.assertEquals(0, reader.i32());
                                    Assert.assertEquals(fields[6].length(), reader.i32());
                                    Assert.assertEquals(fields[6], reader.ascii(fields[6].length()));
                                }
                            }
                            Assert.assertEquals(size, reader.position());
                        } catch (AssertionError e) {
                            throw new AssertionError("case_id=" + fields[0] + ", target="
                                    + ColumnType.nameOf(targetType) + ": " + e.getMessage(), e);
                        }
                    }
                    count++;
                }
            }
            Assert.assertEquals(8, count);
        });
    }

    @Test
    public void testLong256DuplicateNullOmissionRollbackAndMissingInference() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer,
                        column("value", ColumnType.LONG256), column("bad", ColumnType.UUID));
                rows.long256Column("value", 1, 2, 3, 4)
                        .long256Column("value", Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
                buffer.nextRow();
                buffer.nextRow();
                rows.long256Column("value", 5, 6, 7, 8);
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.long256Column("bad", 9, 10, 11, 12));
                rollbackCurrentRow(buffer);
                rows.long256Column("value", Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 3, QwpConstants.TYPE_LONG256);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(0x06, reader.u8());
                Assert.assertEquals(1, reader.i64());
                Assert.assertEquals(2, reader.i64());
                Assert.assertEquals(3, reader.i64());
                Assert.assertEquals(4, reader.i64());
                Assert.assertEquals(size, reader.position());
            }

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(buffer, schemaResult(QwpSchemaProtocol.RESULT_MISSING));
                rows.long256Column("value", Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE);
                buffer.nextRow();
                Assert.assertEquals(1, buffer.getColumnDefs().length);
                Assert.assertEquals(QwpConstants.TYPE_LONG256, buffer.getColumnDefs()[0].getTypeCode());
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, -1, -1, 1, QwpConstants.TYPE_LONG256);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testLong256DuplicatePrecedesUnsupportedAndRejectsParameterizedDesignatedTargets() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding rows = rows(buffer,
                    column("bad", ColumnType.UUID),
                    column("parameterized", ColumnType.LONG256, new byte[]{1}));
            rows.uuidColumn("bad", 1, 2)
                    .long256Column("bad", 3, 4, 5, 6);
            buffer.nextRow();
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.long256Column("bad", 3, 4, 5, 6));
            rollbackCurrentRow(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.long256Column("parameterized", 1, 2, 3, 4));
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding rows = new QwpSchemaBinding(buffer,
                    known(0, column("ts", ColumnType.TIMESTAMP_MICRO)));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.long256Column("ts", 1, 2, 3, 4));
        }
    }

    @Test
    public void testNativeGeoHashConformanceCorpusUsesExactTargetWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingTest.class.getResourceAsStream(
                    "/io/questdb/client/cutlass/qwp/native-geohash-conversions.tsv");
            Assert.assertNotNull(stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                Assert.assertEquals("# case_id\tinput_bits\tprecision_bits\texpected_value_hex\texpected_text", lines.readLine());
                String line;
                while ((line = lines.readLine()) != null) {
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 5, fields.length);
                    long input = Long.parseLong(fields[1]);
                    int precision = Integer.parseInt(fields[2]);
                    long expected = Long.parseUnsignedLong(fields[3], 16);
                    int geoType = ColumnType.getGeoHashTypeWithBits(precision);
                    for (int targetType : new int[]{geoType, ColumnType.STRING, ColumnType.VARCHAR}) {
                        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                             QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                            QwpSchemaBinding rows = rows(buffer, column("value", targetType));
                            rows.geoHashColumn("value", input, precision);
                            buffer.nextRow();
                            byte wireType = ColumnType.isGeoHash(targetType)
                                    ? QwpConstants.TYPE_GEOHASH
                                    : QwpConstants.TYPE_VARCHAR;
                            int size = encoder.encodeSchema(buffer);
                            QwpTestWireReader reader = tableReader(encoder, size, 1, wireType);
                            if (wireType == QwpConstants.TYPE_GEOHASH) {
                                Assert.assertEquals(1, reader.u8());
                                Assert.assertEquals(0, reader.u8());
                                Assert.assertEquals(precision, reader.varint());
                                for (int i = 0; i < (precision + 7) / 8; i++) {
                                    Assert.assertEquals((int) ((expected >>> (i * 8)) & 0xff), reader.u8());
                                }
                            } else {
                                Assert.assertEquals(0, reader.u8());
                                Assert.assertEquals(0, reader.i32());
                                Assert.assertEquals(fields[4].length(), reader.i32());
                                Assert.assertEquals(fields[4], reader.ascii(fields[4].length()));
                            }
                            Assert.assertEquals(size, reader.position());
                        } catch (AssertionError e) {
                            throw new AssertionError("case_id=" + fields[0] + ", target="
                                    + ColumnType.nameOf(targetType) + ": " + e.getMessage(), e);
                        }
                    }
                    count++;
                }
            }
            Assert.assertEquals(15, count);
        });
    }

    @Test
    public void testNativeGeoHashEveryPrecisionUsesFixedWidthText() throws Exception {
        assertMemoryLeak(() -> {
            for (int precision = 1; precision <= 60; precision++) {
                try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    rows(buffer, column("value", ColumnType.STRING))
                            .geoHashColumn("value", -1, precision);
                    buffer.nextRow();
                    int size = encoder.encodeSchema(buffer);
                    QwpTestWireReader reader = tableReader(encoder, size, 1, QwpConstants.TYPE_VARCHAR);
                    Assert.assertEquals(0, reader.u8());
                    Assert.assertEquals(0, reader.i32());
                    Assert.assertEquals(precision, reader.i32());
                    for (int i = 0; i < precision; i++) {
                        Assert.assertEquals('1', reader.u8());
                    }
                    Assert.assertEquals(size, reader.position());
                }
            }
        });
    }

    @Test
    public void testNativeGeoHashOmissionRollbackInferenceAndGuards() throws Exception {
        assertMemoryLeak(() -> {
            int geo20 = ColumnType.getGeoHashTypeWithBits(20);
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer,
                        column("value", geo20), column("bad", ColumnType.UUID));
                rows.geoHashColumn("value", 0xabcde, 20)
                        .geoHashColumn("value", 0x12345, 20);
                buffer.nextRow();
                buffer.nextRow();
                rows.geoHashColumn("value", 0x11111, 20);
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.geoHashColumn("bad", 1, 20));
                rollbackCurrentRow(buffer);
                rows.geoHashColumn("value", 0x12345, 20);
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 3, QwpConstants.TYPE_GEOHASH);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(0x02, reader.u8());
                Assert.assertEquals(20, reader.varint());
                for (long value : new long[]{0xabcde, 0x12345}) {
                    for (int i = 0; i < 3; i++) {
                        Assert.assertEquals((int) ((value >>> (i * 8)) & 0xff), reader.u8());
                    }
                }
                Assert.assertEquals(size, reader.position());
            }

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(buffer, schemaResult(QwpSchemaProtocol.RESULT_MISSING));
                rows.geoHashColumn("value", 0xabcde, 20);
                buffer.nextRow();
                Assert.assertEquals(QwpConstants.TYPE_GEOHASH, buffer.getColumnDefs()[0].getTypeCode());
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, -1, -1, 1, QwpConstants.TYPE_GEOHASH);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(20, reader.varint());
                Assert.assertEquals(0xde, reader.u8());
                Assert.assertEquals(0xbc, reader.u8());
                Assert.assertEquals(0x0a, reader.u8());
                Assert.assertEquals(size, reader.position());
            }

            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer,
                        column("mismatch", ColumnType.getGeoHashTypeWithBits(15)),
                        column("unsupported", ColumnType.UUID),
                        column("parameterized", geo20, new byte[]{1}));
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> rows.geoHashColumn("mismatch", 1, 0));
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> rows.geoHashColumn("mismatch", 1, 61));
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.geoHashColumn("mismatch", 1, 20));
                rollbackCurrentRow(buffer);
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.geoHashColumn("unsupported", 1, 20));
                rollbackCurrentRow(buffer);
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.geoHashColumn("parameterized", 1, 20));
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(buffer,
                        known(0, column("ts", ColumnType.TIMESTAMP_MICRO)));
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.geoHashColumn("ts", 1, 20));
            }
        });
    }

    @Test
    public void testNativeGeoHashTextOverloadPreservesSourcePrecision() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer,
                        column("geo", ColumnType.getGeoHashTypeWithBits(20)),
                        column("text", ColumnType.STRING));
                rows.geoHashColumn("geo", "u33d")
                        .geoHashColumn("text", "U");
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                QwpTestWireReader reader = tableReader(encoder, size, 1,
                        QwpConstants.TYPE_GEOHASH, QwpConstants.TYPE_VARCHAR);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(20, reader.varint());
                long expected = Numbers.parseGeoHashBase32("u33d");
                for (int i = 0; i < 3; i++) {
                    Assert.assertEquals((int) ((expected >>> (i * 8)) & 0xff), reader.u8());
                }
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0, reader.i32());
                Assert.assertEquals(5, reader.i32());
                Assert.assertEquals("11010", reader.ascii(5));
                Assert.assertEquals(size, reader.position());
            }

            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = rows(buffer,
                        column("value", ColumnType.getGeoHashTypeWithBits(15)));
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.geoHashColumn("value", "u33d"));
                rollbackCurrentRow(buffer);
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> rows.geoHashColumn("value", (CharSequence) null));
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> rows.geoHashColumn("value", ""));
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> rows.geoHashColumn("value", "0123456789bcd"));
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> rows.geoHashColumn("value", "a"));
            }
        });
    }

    private static void assertCtorReason(int result, LineSenderSchemaException.Reason reason, String message) {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t");
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            try {
                new QwpSchemaBinding(buffer, schemaResult(result));
                Assert.fail("expected schema exception");
            } catch (LineSenderSchemaException e) {
                Assert.assertEquals(reason, e.getReason());
                Assert.assertFalse(e.isRetryable());
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(message));
            }
            QwpSchemaBinding binding = rows(buffer, column("l", ColumnType.LONG));
            binding.longColumn("l", 7);
            buffer.nextRow();
            assertLongFrame(encoder, encoder.encodeSchema(buffer), 7);
        }
    }

    private static void assertLongFrame(QwpWebSocketEncoder encoder, int size, long... values) {
        QwpTestWireReader reader = tableReader(encoder, size, values.length, QwpConstants.TYPE_LONG);
        Assert.assertEquals(0, reader.u8());
        for (long value : values) {
            Assert.assertEquals(value, reader.i64());
        }
        Assert.assertEquals(size, reader.position());
    }

    private static void appendSmallInteger(
            QwpSchemaBinding rows,
            String inputType,
            long value,
            String column
    ) {
        switch (inputType) {
            case "BYTE":
                rows.byteColumn(column, (byte) value);
                break;
            case "SHORT":
                rows.shortColumn(column, (short) value);
                break;
            case "INT":
                rows.intColumn(column, (int) value);
                break;
            default:
                throw new AssertionError(inputType);
        }
    }

    private static int decimalStorageIndex(String target) {
        switch (target) {
            case "DECIMAL8":
                return 0;
            case "DECIMAL16":
                return 1;
            case "DECIMAL32":
                return 2;
            case "DECIMAL64":
                return 3;
            case "DECIMAL128":
                return 4;
            default:
                Assert.assertEquals("DECIMAL256", target);
                return 5;
        }
    }

    private static String decimalStorageType(int precision) {
        if (precision <= 2) {
            return "DECIMAL8";
        }
        if (precision <= 4) {
            return "DECIMAL16";
        }
        if (precision <= 9) {
            return "DECIMAL32";
        }
        if (precision <= 18) {
            return "DECIMAL64";
        }
        if (precision <= 38) {
            return "DECIMAL128";
        }
        return "DECIMAL256";
    }

    private static int decimalWireLongCount(String target) {
        if ("DECIMAL128".equals(target)) {
            return 2;
        }
        if ("DECIMAL256".equals(target)) {
            return 4;
        }
        return 1;
    }

    private static byte decimalWireType(String target) {
        if ("DECIMAL128".equals(target)) {
            return QwpConstants.TYPE_DECIMAL128;
        }
        if ("DECIMAL256".equals(target)) {
            return QwpConstants.TYPE_DECIMAL256;
        }
        return QwpConstants.TYPE_DECIMAL64;
    }

    private static void appendIntegerTemporal(
            QwpSchemaBinding rows,
            String inputType,
            long value,
            String column
    ) {
        switch (inputType) {
            case "BYTE":
                rows.byteColumn(column, (byte) value);
                break;
            case "SHORT":
                rows.shortColumn(column, (short) value);
                break;
            case "INT":
                rows.intColumn(column, (int) value);
                break;
            case "LONG":
                rows.longColumn(column, value);
                break;
            default:
                throw new AssertionError(inputType);
        }
    }

    private static byte[] hexBytes(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    private static void assertNumericValue(QwpTestWireReader reader, String target, String expected) {
        switch (target) {
            case "BYTE":
                Assert.assertEquals(Integer.parseInt(expected) & 0xff, reader.u8());
                break;
            case "SHORT":
                Assert.assertEquals(Integer.parseInt(expected) & 0xffff, reader.u16());
                break;
            case "INT":
                Assert.assertEquals(Integer.parseInt(expected), reader.i32());
                break;
            case "LONG":
                Assert.assertEquals(Long.parseLong(expected), reader.i64());
                break;
            case "FLOAT":
                Assert.assertEquals((int) Long.parseUnsignedLong(expected.substring(2), 16), reader.i32());
                break;
            case "DOUBLE":
                Assert.assertEquals(Long.parseUnsignedLong(expected.substring(2), 16), reader.i64());
                break;
            default:
                Assert.fail(target);
        }
    }

    private static void assertNumericNull(QwpTestWireReader reader, String target) {
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(target, 1, reader.u8());
    }

    private static void rollbackCurrentRow(QwpTableBuffer buffer) {
        buffer.cancelCurrentRow();
        buffer.rollbackUncommittedColumns();
    }

    private static int numericColumnType(String target) {
        switch (target) {
            case "BYTE": return ColumnType.BYTE;
            case "SHORT": return ColumnType.SHORT;
            case "INT": return ColumnType.INT;
            case "LONG": return ColumnType.LONG;
            case "FLOAT": return ColumnType.FLOAT;
            case "DOUBLE": return ColumnType.DOUBLE;
            default: throw new AssertionError(target);
        }
    }

    private static long parseHexLong(String value) {
        Assert.assertNotEquals("-", value);
        return Long.parseUnsignedLong(value, 16);
    }

    private static int smallIntegerSourceIndex(String source) {
        switch (source) {
            case "BYTE":
                return 0;
            case "SHORT":
                return 1;
            default:
                Assert.assertEquals("INT", source);
                return 2;
        }
    }

    private static int timestampColumnType(String target) {
        switch (target) {
            case "TIMESTAMP": return ColumnType.TIMESTAMP_MICRO;
            case "TIMESTAMP_NS": return ColumnType.TIMESTAMP_NANO;
            default: throw new AssertionError(target);
        }
    }

    private static int temporalColumnType(String target) {
        switch (target) {
            case "DATE": return ColumnType.DATE;
            case "TIMESTAMP": return ColumnType.TIMESTAMP_MICRO;
            case "TIMESTAMP_NS": return ColumnType.TIMESTAMP_NANO;
            default: throw new AssertionError(target);
        }
    }

    private static int textColumnType(String target) {
        switch (target) {
            case "STRING": return ColumnType.STRING;
            case "VARCHAR": return ColumnType.VARCHAR;
            case "SYMBOL": return ColumnType.SYMBOL;
            default: throw new AssertionError(target);
        }
    }

    private static byte textWireType(String target) {
        return "SYMBOL".equals(target) ? QwpConstants.TYPE_SYMBOL : QwpConstants.TYPE_VARCHAR;
    }

    private static byte timestampWireType(String target) {
        switch (target) {
            case "TIMESTAMP": return QwpConstants.TYPE_TIMESTAMP;
            case "TIMESTAMP_NS": return QwpConstants.TYPE_TIMESTAMP_NANOS;
            default: throw new AssertionError(target);
        }
    }

    private static byte temporalWireType(String target) {
        switch (target) {
            case "DATE": return QwpConstants.TYPE_DATE;
            case "TIMESTAMP": return QwpConstants.TYPE_TIMESTAMP;
            case "TIMESTAMP_NS": return QwpConstants.TYPE_TIMESTAMP_NANOS;
            default: throw new AssertionError(target);
        }
    }

    private static byte numericWireType(String target) {
        switch (target) {
            case "BYTE": return QwpConstants.TYPE_BYTE;
            case "SHORT": return QwpConstants.TYPE_SHORT;
            case "INT": return QwpConstants.TYPE_INT;
            case "LONG": return QwpConstants.TYPE_LONG;
            case "FLOAT": return QwpConstants.TYPE_FLOAT;
            case "DOUBLE": return QwpConstants.TYPE_DOUBLE;
            default: throw new AssertionError(target);
        }
    }

    private static byte[] copyFrame(QwpWebSocketEncoder encoder, int size) {
        byte[] copy = new byte[size];
        long address = encoder.getBuffer().getBufferPtr();
        for (int i = 0; i < size; i++) {
            copy[i] = Unsafe.getUnsafe().getByte(address + i);
        }
        return copy;
    }

    private static void assertIllegalArgument(Runnable action) {
        try {
            action.run();
            Assert.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    private static void assertIllegalState(Runnable action) {
        try {
            action.run();
            Assert.fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    private static void assertEncoderUnchanged(QwpWebSocketEncoder encoder, Runnable action) {
        int position = encoder.getBuffer().getPosition();
        byte[] before = copyFrame(encoder, position);
        assertIllegalState(action);
        Assert.assertEquals(position, encoder.getBuffer().getPosition());
        Assert.assertArrayEquals(before, copyFrame(encoder, position));
    }

    private static void assertEncoderUnchangedIllegalArgument(QwpWebSocketEncoder encoder, Runnable action) {
        int position = encoder.getBuffer().getPosition();
        byte[] before = copyFrame(encoder, position);
        assertIllegalArgument(action);
        Assert.assertEquals(position, encoder.getBuffer().getPosition());
        Assert.assertArrayEquals(before, copyFrame(encoder, position));
    }

    private static QwpSchemaBinding rows(QwpTableBuffer buffer, byte[]... columns) {
        return new QwpSchemaBinding(buffer, known(columns));
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, int rows, byte... wireTypes) {
        return tableReader(encoder, size, 1, 1, rows, wireTypes);
    }

    private static QwpTestWireReader tableReader(
            QwpWebSocketEncoder encoder,
            int size,
            int tableId,
            long metadataVersion,
            int rows,
            byte... wireTypes
    ) {
        QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
        Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, reader.i32());
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_SCHEMA, reader.u8());
        Assert.assertEquals(1, reader.u16());
        Assert.assertEquals(size - QwpConstants.HEADER_SIZE, reader.i32());
        Assert.assertEquals("t", reader.string());
        if (tableId < 0) {
            Assert.assertEquals(0, reader.u8());
        } else {
            Assert.assertEquals(1, reader.u8());
            Assert.assertEquals(tableId, reader.i32());
            Assert.assertEquals(metadataVersion, reader.i64());
        }
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(wireTypes.length, reader.varint());
        for (byte wireType : wireTypes) {
            reader.string();
            Assert.assertEquals(wireType, reader.u8());
        }
        return reader;
    }
}
