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
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

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
                        Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1, wireType);
                        Assert.assertEquals("supplied timestamp must be bitmap-present", 0, reader.byteValue());
                        Assert.assertEquals("short timestamp columns use raw encoding", 0, reader.byteValue());
                        Assert.assertEquals(Long.parseLong(fields[5]), reader.longValue());
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
                Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1,
                        QwpConstants.TYPE_TIMESTAMP_NANOS);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(123, reader.longValue());
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
                Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), 2,
                        QwpConstants.TYPE_TIMESTAMP, QwpConstants.TYPE_TIMESTAMP_NANOS);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(1_000_000, reader.longValue());
                Assert.assertEquals(3_000_000, reader.longValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(4, reader.longValue());
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
                        Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1, wireType);
                        Assert.assertEquals("supplied timestamp must be bitmap-present", 0, reader.byteValue());
                        Assert.assertEquals("short timestamp columns use raw encoding", 0, reader.byteValue());
                        Assert.assertEquals(Long.parseLong(fields[4]), reader.longValue());
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
                Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), 2,
                        QwpConstants.TYPE_TIMESTAMP, QwpConstants.TYPE_TIMESTAMP_NANOS);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(7, reader.longValue());
                Assert.assertEquals(9, reader.longValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(10, reader.longValue());
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
                Reader nulls = tableReader(encoder, encoder.encodeSchema(buffer), 2,
                        QwpConstants.TYPE_TIMESTAMP, QwpConstants.TYPE_TIMESTAMP_NANOS);
                Assert.assertEquals(1, nulls.byteValue());
                Assert.assertEquals(3, nulls.byteValue());
                Assert.assertEquals(0, nulls.byteValue());
                Assert.assertEquals(1, nulls.byteValue());
                Assert.assertEquals(3, nulls.byteValue());
                Assert.assertEquals(0, nulls.byteValue());
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
            new QwpSchemaBinding(buffer, result(QwpSchemaProtocol.RESULT_MISSING)).longColumn("x", 1);
        }
        assertCtorReason(QwpSchemaProtocol.RESULT_TOO_LARGE, LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, "response limit");
    }

    @Test
    public void testInfersNativeTargetsForMissingSchemaAndColumns() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(buffer, result(QwpSchemaProtocol.RESULT_MISSING));
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
                QwpSchemaBinding rows = new QwpSchemaBinding(buffer, result(QwpSchemaProtocol.RESULT_MISSING));
                rows.uuidColumn("u", 0x0102030405060708L, 0x1112131415161718L)
                        .floatColumn("f", Float.intBitsToFloat(0x3fc00000))
                        .stringColumn("s", "quest");
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                Reader reader = tableReader(encoder, size, -1, -1, 1,
                        QwpConstants.TYPE_UUID, QwpConstants.TYPE_FLOAT, QwpConstants.TYPE_VARCHAR);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(0x0102030405060708L, reader.longValue());
                Assert.assertEquals(0x1112131415161718L, reader.longValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(0x3fc00000, reader.intValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(0, reader.intValue());
                Assert.assertEquals(5, reader.intValue());
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
                QwpSchemaBinding rows = new QwpSchemaBinding(buffer, result(QwpSchemaProtocol.RESULT_MISSING));
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
                Reader reader = tableReader(encoder, size, -1, -1, 2, QwpConstants.TYPE_LONG);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(1, reader.longValue());
                Assert.assertEquals(3, reader.longValue());
                Assert.assertEquals(size, reader.position());
            }

            try (QwpTableBuffer micros = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(micros, result(QwpSchemaProtocol.RESULT_MISSING));
                rows.designatedTimestamp(1, ChronoUnit.MICROS);
                Assert.assertEquals(QwpConstants.TYPE_TIMESTAMP, micros.getColumnDefs()[0].getTypeCode());
                micros.nextRow();
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> rows.designatedTimestamp(2, ChronoUnit.NANOS));
                micros.cancelCurrentRow();
                micros.rollbackUncommittedColumns();
            }
            try (QwpTableBuffer nanos = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(nanos, result(QwpSchemaProtocol.RESULT_MISSING));
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
                Reader reader = tableReader(encoder, size, 2, QwpConstants.TYPE_UUID, QwpConstants.TYPE_LONG);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(1, reader.longValue());
                Assert.assertEquals(2, reader.longValue());
                Assert.assertEquals(0x0123456789abcdefL, reader.longValue());
                Assert.assertEquals(0x0123456789abcdefL, reader.longValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(2, reader.byteValue());
                Assert.assertEquals(4, reader.longValue());
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
                            Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1,
                                    numericWireType(fields[3]));
                            if ("<NULL>".equals(fields[4])) {
                                assertNumericNull(reader, fields[3]);
                            } else {
                                Assert.assertEquals(0, reader.byteValue());
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
                Reader reader = tableReader(encoder, size, 2,
                        QwpConstants.TYPE_LONG, QwpConstants.TYPE_DOUBLE);
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(2, reader.byteValue());
                Assert.assertEquals(-1, reader.longValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(Double.doubleToRawLongBits(3), reader.longValue());
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
                    Reader reader = tableReader(encoder, size, 3, numericWireType(target));
                    Assert.assertEquals(target, 1, reader.byteValue());
                    Assert.assertEquals(target, 3, reader.byteValue());
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
    public void testSmallIntegerMissingSchemaUsesNativeWireTypes() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding rows = new QwpSchemaBinding(buffer, result(QwpSchemaProtocol.RESULT_MISSING));
                rows.byteColumn("b", (byte) -1).shortColumn("s", (short) 2).intColumn("i", 3);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                Reader reader = tableReader(encoder, size, -1, -1, 1,
                        QwpConstants.TYPE_BYTE, QwpConstants.TYPE_SHORT, QwpConstants.TYPE_INT);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(0xff, reader.byteValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(2, reader.shortValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(3, reader.intValue());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testSmallIntegerUnsupportedParameterizedDesignatedAndStaleTargets() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding rows = rows(buffer,
                    column("uuid", ColumnType.UUID),
                    column("future", ColumnType.INT, new byte[]{1}));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.byteColumn("uuid", (byte) 1));
            rollbackCurrentRow(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> rows.shortColumn("future", (short) 2));
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
                        Reader reader = tableReader(encoder, size, 1, wireType);
                        if ("<NULL>".equals(fields[3])) {
                            if (input == Long.MIN_VALUE) {
                                assertNumericNull(reader, fields[2]);
                            } else {
                                Assert.assertEquals("INT sentinel is carried as a non-null wire value", 0, reader.byteValue());
                                Assert.assertEquals(Integer.MIN_VALUE, reader.intValue());
                            }
                        } else {
                            Assert.assertEquals(0, reader.byteValue());
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
                Reader reader = tableReader(encoder, size, 2, QwpConstants.TYPE_BYTE, QwpConstants.TYPE_INT);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(7, reader.byteValue());
                Assert.assertEquals(3, reader.byteValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(1, reader.intValue());
                Assert.assertEquals(4, reader.intValue());
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
                Reader reader = tableReader(encoder, size, 1, QwpConstants.TYPE_UUID);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(1, reader.longValue());
                Assert.assertEquals(2, reader.longValue());
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
                Reader reader = tableReader(encoder, size, 1, QwpConstants.TYPE_BYTE, QwpConstants.TYPE_SHORT,
                        QwpConstants.TYPE_INT, QwpConstants.TYPE_LONG, QwpConstants.TYPE_FLOAT, QwpConstants.TYPE_DOUBLE);
                Assert.assertEquals(0, reader.byteValue()); Assert.assertEquals(7, reader.byteValue());
                Assert.assertEquals(0, reader.byteValue()); Assert.assertEquals(7, reader.shortValue());
                Assert.assertEquals(0, reader.byteValue()); Assert.assertEquals(7, reader.intValue());
                Assert.assertEquals(0, reader.byteValue()); Assert.assertEquals(7, reader.longValue());
                Assert.assertEquals(0, reader.byteValue()); Assert.assertEquals(Float.floatToRawIntBits(7), reader.intValue());
                Assert.assertEquals(0, reader.byteValue()); Assert.assertEquals(Double.doubleToRawLongBits(7), reader.longValue());
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
                Reader reader = tableReader(encoder, size, 2, QwpConstants.TYPE_LONG);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(1, reader.longValue());
                Assert.assertEquals(3, reader.longValue());
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
                Reader reader = tableReader(encoder, size, 1, QwpConstants.TYPE_BYTE, QwpConstants.TYPE_SHORT,
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
                Reader reader = tableReader(encoder, size, 1, QwpConstants.TYPE_UUID, QwpConstants.TYPE_LONG);
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
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
                Reader reader = tableReader(encoder, size, 2, QwpConstants.TYPE_LONG, QwpConstants.TYPE_UUID);
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(2, reader.byteValue());
                Assert.assertEquals(1, reader.longValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(0x0123456789abcdefL, reader.longValue());
                Assert.assertEquals(0x0123456789abcdefL, reader.longValue());
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
                    Reader reader = tableReader(encoder, size, 1, QwpConstants.TYPE_LONG);
                    Assert.assertEquals(0, reader.byteValue());
                    Assert.assertEquals(7, reader.longValue());
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
                Reader reader = tableReader(encoder, size, 2, QwpConstants.TYPE_UUID);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(lo, reader.longValue());
                Assert.assertEquals(hi, reader.longValue());
                Assert.assertEquals(lo, reader.longValue());
                Assert.assertEquals(hi, reader.longValue());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    private static void assertCtorReason(int result, LineSenderSchemaException.Reason reason, String message) {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t");
             QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
            try {
                new QwpSchemaBinding(buffer, result(result));
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
        Reader reader = tableReader(encoder, size, values.length, QwpConstants.TYPE_LONG);
        Assert.assertEquals(0, reader.byteValue());
        for (long value : values) {
            Assert.assertEquals(value, reader.longValue());
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

    private static void assertNumericValue(Reader reader, String target, String expected) {
        switch (target) {
            case "BYTE":
                Assert.assertEquals(Integer.parseInt(expected) & 0xff, reader.byteValue());
                break;
            case "SHORT":
                Assert.assertEquals(Integer.parseInt(expected) & 0xffff, reader.shortValue());
                break;
            case "INT":
                Assert.assertEquals(Integer.parseInt(expected), reader.intValue());
                break;
            case "LONG":
                Assert.assertEquals(Long.parseLong(expected), reader.longValue());
                break;
            case "FLOAT":
                Assert.assertEquals((int) Long.parseUnsignedLong(expected.substring(2), 16), reader.intValue());
                break;
            case "DOUBLE":
                Assert.assertEquals(Long.parseUnsignedLong(expected.substring(2), 16), reader.longValue());
                break;
            default:
                Assert.fail(target);
        }
    }

    private static void assertNumericNull(Reader reader, String target) {
        Assert.assertEquals(1, reader.byteValue());
        Assert.assertEquals(target, 1, reader.byteValue());
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

    private static int timestampColumnType(String target) {
        switch (target) {
            case "TIMESTAMP": return ColumnType.TIMESTAMP_MICRO;
            case "TIMESTAMP_NS": return ColumnType.TIMESTAMP_NANO;
            default: throw new AssertionError(target);
        }
    }

    private static byte timestampWireType(String target) {
        switch (target) {
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

    private static void assertReason(LineSenderSchemaException.Reason reason, Runnable action) {
        try {
            action.run();
            Assert.fail("expected schema exception");
        } catch (LineSenderSchemaException e) {
            Assert.assertEquals(reason, e.getReason());
            Assert.assertFalse(e.isRetryable());
        }
    }

    private static byte[] column(String name, int type) {
        return column(name, type, new byte[0]);
    }

    private static byte[] column(String name, int type, byte[] params) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + bytes.length + 4 + 2 + params.length).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) bytes.length).put(bytes).putInt(type).putShort((short) params.length).put(params).array();
    }

    private static QwpSchemaResponse known(byte[]... columns) {
        return known(-1, columns);
    }

    private static QwpSchemaResponse known(int designatedIndex, byte[]... columns) {
        int length = 1 + 8 + 1 + 4 + 8 + 2 + 2;
        for (byte[] column : columns) {
            length += column.length;
        }
        ByteBuffer payload = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN)
                .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(1).put((byte) QwpSchemaProtocol.RESULT_KNOWN)
                .putInt(1).putLong(1).putShort((short) designatedIndex).putShort((short) columns.length);
        for (byte[] column : columns) {
            payload.put(column);
        }
        return decode(frame(payload.array()));
    }

    private static QwpSchemaResponse result(int result) {
        return decode(frame(ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN)
                .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(1).put((byte) result).array()));
    }

    private static QwpSchemaBinding rows(QwpTableBuffer buffer, byte[]... columns) {
        return new QwpSchemaBinding(buffer, known(columns));
    }

    private static byte[] frame(byte[] payload) {
        return ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payload.length).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(QwpConstants.MAGIC_MESSAGE).put((byte) 1).put(QwpSchemaProtocol.FLAG_CONTROL)
                .putShort((short) 0).putInt(payload.length).put(payload).array();
    }

    private static QwpSchemaResponse decode(byte[] frame) {
        long address = Unsafe.malloc(frame.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < frame.length; i++) {
                Unsafe.getUnsafe().putByte(address + i, frame[i]);
            }
            return QwpSchemaProtocol.decodeResponse(address, frame.length);
        } finally {
            Unsafe.free(address, frame.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, int rows, byte... wireTypes) {
        return tableReader(encoder, size, 1, 1, rows, wireTypes);
    }

    private static Reader tableReader(
            QwpWebSocketEncoder encoder,
            int size,
            int tableId,
            long metadataVersion,
            int rows,
            byte... wireTypes
    ) {
        Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
        Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, reader.intValue());
        Assert.assertEquals(1, reader.byteValue());
        Assert.assertEquals(QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_SCHEMA, reader.byteValue());
        Assert.assertEquals(1, reader.shortValue());
        Assert.assertEquals(size - QwpConstants.HEADER_SIZE, reader.intValue());
        Assert.assertEquals("t", reader.stringValue());
        if (tableId < 0) {
            Assert.assertEquals(0, reader.byteValue());
        } else {
            Assert.assertEquals(1, reader.byteValue());
            Assert.assertEquals(tableId, reader.intValue());
            Assert.assertEquals(metadataVersion, reader.longValue());
        }
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(wireTypes.length, reader.varint());
        for (byte wireType : wireTypes) {
            reader.stringValue();
            Assert.assertEquals(wireType, reader.byteValue());
        }
        return reader;
    }

    private static final class Reader {
        private final long address;
        private final int limit;
        private int position;

        private Reader(long address, int limit) {
            this.address = address;
            this.limit = limit;
        }

        private int byteValue() {
            Assert.assertTrue(position < limit);
            return Unsafe.getUnsafe().getByte(address + position++) & 0xff;
        }

        private String ascii(int length) {
            StringBuilder sink = new StringBuilder(length);
            for (int i = 0; i < length; i++) {
                sink.append((char) byteValue());
            }
            return sink.toString();
        }

        private int intValue() {
            Assert.assertTrue(position + 4 <= limit);
            int value = Unsafe.getUnsafe().getInt(address + position);
            position += 4;
            return value;
        }

        private long longValue() {
            Assert.assertTrue(position + 8 <= limit);
            long value = Unsafe.getUnsafe().getLong(address + position);
            position += 8;
            return value;
        }

        private int position() {
            return position;
        }

        private int shortValue() {
            Assert.assertTrue(position + 2 <= limit);
            int value = Unsafe.getUnsafe().getShort(address + position) & 0xffff;
            position += 2;
            return value;
        }

        private String stringValue() {
            int length = varint();
            byte[] bytes = new byte[length];
            for (int i = 0; i < length; i++) {
                bytes[i] = (byte) byteValue();
            }
            return new String(bytes, StandardCharsets.UTF_8);
        }

        private int varint() {
            int result = 0;
            int shift = 0;
            int value;
            do {
                value = byteValue();
                result |= (value & 0x7f) << shift;
                shift += 7;
            } while ((value & 0x80) != 0);
            return result;
        }
    }
}
