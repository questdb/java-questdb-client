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
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
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
                        Reader reader = tableReader(
                                encoder, encoder.encodeSchema(buffer), 1, QwpConstants.TYPE_VARCHAR);
                        String expectedText = "<EMPTY>".equals(fields[6]) ? "" : fields[6];
                        byte[] expected = expectedText.getBytes(StandardCharsets.UTF_8);
                        Assert.assertEquals(0, reader.u8());
                        Assert.assertEquals(0, reader.i32());
                        Assert.assertEquals(expected.length, reader.i32());
                        Assert.assertArrayEquals(expected, reader.bytes(expected.length));
                        Assert.assertEquals(reader.limit, reader.position);
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
                Reader reader = tableHeader(encoder, size, 5, 2);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.u8());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.u8());
                assertVarchar(reader, 0x14, "", "1970-01-01T00:00:00.000Z", "1969-12-31T23:59:59.999Z");
                assertVarchar(reader, 0x0f, "1970-01-01T00:00:00.000Z");
                Assert.assertEquals(size, reader.position);

                buffer.reset();
                binding.timestampColumn("a", 0, ChronoUnit.SECONDS);
                buffer.nextRow();
                int resetSize = encoder.encodeSchema(buffer);
                Reader reset = tableHeader(encoder, resetSize, 1, 2);
                Assert.assertEquals("a", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reset.u8());
                Assert.assertEquals("c", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reset.u8());
                assertVarchar(reset, -1, "1970-01-01T00:00:00.000Z");
                assertVarchar(reset, 0x01);
                Assert.assertEquals(resetSize, reset.position);
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

    private static void assertVarchar(Reader reader, int bitmap, String... values) {
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

    private static QwpSchemaBinding binding(QwpTableBuffer buffer, int designated, byte[]... columns) {
        return new QwpSchemaBinding(buffer, known(designated, columns));
    }

    private static byte[] column(String name, int type) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + bytes.length + 6).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) bytes.length).put(bytes).putInt(type).putShort((short) 0).array();
    }

    private static QwpSchemaResponse known(int designated, byte[]... columns) {
        int length = 26;
        for (byte[] column : columns) {
            length += column.length;
        }
        ByteBuffer payload = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN)
                .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(1).put((byte) QwpSchemaProtocol.RESULT_KNOWN)
                .putInt(1).putLong(1).putShort((short) designated).putShort((short) columns.length);
        for (byte[] column : columns) {
            payload.put(column);
        }
        return decode(payload.array());
    }

    private static QwpSchemaResponse missing() {
        return decode(ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN)
                .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(1).put((byte) QwpSchemaProtocol.RESULT_MISSING).array());
    }

    private static QwpSchemaResponse decode(byte[] payload) {
        ByteBuffer frame = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payload.length).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(QwpConstants.MAGIC_MESSAGE).put((byte) 1).put(QwpSchemaProtocol.FLAG_CONTROL)
                .putShort((short) 0).putInt(payload.length).put(payload);
        long address = Unsafe.malloc(frame.capacity(), MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < frame.capacity(); i++) {
                Unsafe.getUnsafe().putByte(address + i, frame.array()[i]);
            }
            return QwpSchemaProtocol.decodeResponse(address, frame.capacity());
        } finally {
            Unsafe.free(address, frame.capacity(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, int rows, byte type) {
        Reader reader = tableHeader(encoder, size, rows, 1);
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(type, reader.u8());
        return reader;
    }

    private static Reader tableHeader(QwpWebSocketEncoder encoder, int size, int rows, int columns) {
        Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
        Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, reader.i32());
        Assert.assertEquals(QwpConstants.VERSION, reader.u8());
        Assert.assertEquals(QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_SCHEMA, reader.u8());
        Assert.assertEquals(1, reader.u16());
        Assert.assertEquals(size - QwpConstants.HEADER_SIZE, reader.i32());
        Assert.assertEquals("t", reader.string());
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(1, reader.i32());
        Assert.assertEquals(1, reader.i64());
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(columns, reader.varint());
        return reader;
    }

    private static void assertReason(LineSenderSchemaException.Reason reason, Runnable action) {
        Assert.assertEquals(reason, Assert.assertThrows(LineSenderSchemaException.class, action::run).getReason());
    }

    private static final class Reader {
        private final long address;
        private final int limit;
        private int position;

        private Reader(long address, int limit) {
            this.address = address;
            this.limit = limit;
        }

        private byte[] bytes(int length) {
            byte[] bytes = new byte[length];
            for (int i = 0; i < length; i++) {
                bytes[i] = (byte) u8();
            }
            return bytes;
        }

        private int i32() {
            Assert.assertTrue(position + 4 <= limit);
            int value = Unsafe.getUnsafe().getInt(address + position);
            position += 4;
            return value;
        }

        private long i64() {
            Assert.assertTrue(position + 8 <= limit);
            long value = Unsafe.getUnsafe().getLong(address + position);
            position += 8;
            return value;
        }

        private void skip(int length) {
            position += length;
            Assert.assertTrue(position <= limit);
        }

        private String string() {
            return new String(bytes(varint()), StandardCharsets.UTF_8);
        }

        private int u8() {
            Assert.assertTrue(position < limit);
            return Unsafe.getUnsafe().getByte(address + position++) & 0xff;
        }

        private int u16() {
            return u8() | (u8() << 8);
        }

        private int varint() {
            int result = 0;
            int shift = 0;
            int value;
            do {
                value = u8();
                result |= (value & 0x7f) << shift;
                shift += 7;
            } while ((value & 0x80) != 0);
            return result;
        }
    }
}
