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

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpSchemaBindingStringTimestampTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/string-to-timestamp.tsv";

    @Test
    public void testCorpusUsesExactTargetUnitWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingStringTimestampTest.class.getResourceAsStream(CORPUS);
            Assert.assertNotNull(CORPUS, stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                Assert.assertEquals("case_id\ttarget\tinput_kind\tinput\toutcome\texpected_raw\tfeature", lines.readLine());
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') {
                        continue;
                    }
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 7, fields.length);
                    Assert.assertTrue(line, "VALID".equals(fields[4]) || "NULL".equals(fields[4]) || "INVALID".equals(fields[4]));
                    int target = targetType(fields[1]);
                    byte wireType = wireType(fields[1]);
                    CharSequence value = input(fields[2], fields[3]);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer, -1, column("value", target));
                        if ("INVALID".equals(fields[4])) {
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                                    () -> binding.stringColumn("value", value));
                            Assert.assertEquals(0, buffer.getRowCount());
                            count++;
                            continue;
                        }
                        binding.stringColumn("value", value);
                        buffer.nextRow();
                        int size = encoder.encodeSchema(buffer);
                        Reader reader = tableReader(encoder, size, 1, "value", wireType);
                        if ("NULL".equals(fields[4])) {
                            Assert.assertEquals(1, reader.u8());
                            Assert.assertEquals(1, reader.u8());
                            Assert.assertEquals(0, reader.u8());
                        } else {
                            Assert.assertEquals(0, reader.u8());
                            Assert.assertEquals(0, reader.u8());
                            Assert.assertEquals(Long.parseLong(fields[5]), reader.i64());
                        }
                        Assert.assertEquals(size, reader.position());
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertEquals(110, count);
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
                    binding.stringColumn("value", null).binaryColumn("value", new byte[]{1});
                    buffer.nextRow();
                    binding.stringColumn("value", "1970-01-01T00:00:00.000001Z");
                    buffer.nextRow();
                    buffer.nextRow();
                    int size = encoder.encodeSchema(buffer);
                    Reader reader = tableReader(encoder, size, 3, "value", wireType);
                    Assert.assertEquals(1, reader.u8());
                    Assert.assertEquals(5, reader.u8());
                    Assert.assertEquals(0, reader.u8());
                    Assert.assertEquals(target == ColumnType.TIMESTAMP_MICRO ? 1 : 1000, reader.i64());
                    Assert.assertEquals(size, reader.position());

                    buffer.reset();
                    binding.stringColumn("value", "1969-12-31T23:59:59.999999Z");
                    buffer.nextRow();
                    int resetSize = encoder.encodeSchema(buffer);
                    Reader reset = tableReader(encoder, resetSize, 1, "value", wireType);
                    Assert.assertEquals(0, reset.u8());
                    Assert.assertEquals(0, reset.u8());
                    Assert.assertEquals(target == ColumnType.TIMESTAMP_MICRO ? -1 : -1000, reset.i64());
                    Assert.assertEquals(resetSize, reset.position());
                }
            }

            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("a", ColumnType.TIMESTAMP_MICRO),
                        column("bad", ColumnType.UUID),
                        column("c", ColumnType.TIMESTAMP_NANO));
                binding.stringColumn("a", "1969-12-31T23:59:59.999999Z");
                buffer.nextRow();
                binding.stringColumn("a", "1970-01-01T00:00:00.000002Z");
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.stringColumn("bad", "not-a-uuid"));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.stringColumn("c", "1970-01-01T00:00:00.000004Z");
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                Reader reader = tableHeader(encoder, size, 2, 2);
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
                Assert.assertEquals(4000, reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testNativeInferenceAndNamedDesignatedRejection() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
            binding.stringColumn("value", "1970-01-01T00:00:00Z");
            buffer.nextRow();
            int size = encoder.encodeSchema(buffer);
            Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
            reader.skip(QwpConstants.HEADER_SIZE);
            Assert.assertEquals("t", reader.string());
            Assert.assertEquals(0, reader.u8());
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals("value", reader.string());
            Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.u8());
            Assert.assertEquals(0, reader.u8());
            Assert.assertEquals(0, reader.i32());
            Assert.assertEquals(20, reader.i32());
            Assert.assertEquals("1970-01-01T00:00:00Z", reader.ascii(20));
            Assert.assertEquals(size, reader.position());
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, 0, column("ts", ColumnType.TIMESTAMP_MICRO));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.stringColumn("ts", "1970-01-01T00:00:00Z"));
        }
    }

    private static QwpSchemaBinding binding(QwpTableBuffer buffer, int designated, byte[]... columns) {
        return new QwpSchemaBinding(buffer, known(designated, columns));
    }

    private static byte[] column(String name, int type) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + bytes.length + 6).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) bytes.length).put(bytes).putInt(type).putShort((short) 0).array();
    }

    private static QwpSchemaResponse missing() {
        return decode(ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN)
                .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(1).put((byte) QwpSchemaProtocol.RESULT_MISSING).array());
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

    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, int rows, String name, byte type) {
        Reader reader = tableHeader(encoder, size, rows, 1);
        Assert.assertEquals(name, reader.string());
        Assert.assertEquals(type, reader.u8());
        return reader;
    }

    private static Reader tableHeader(QwpWebSocketEncoder encoder, int size, int rows, int columns) {
        Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        Assert.assertEquals("t", reader.string());
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(1, reader.i32());
        Assert.assertEquals(1, reader.i64());
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(columns, reader.varint());
        return reader;
    }

    private static int targetType(String value) {
        Assert.assertTrue(value, "TIMESTAMP".equals(value) || "TIMESTAMP_NANOS".equals(value));
        return "TIMESTAMP".equals(value) ? ColumnType.TIMESTAMP_MICRO : ColumnType.TIMESTAMP_NANO;
    }

    private static CharSequence input(String kind, String value) {
        Assert.assertTrue(kind, "TEXT".equals(kind) || "UTF16_HEX".equals(kind));
        if ("<NULL>".equals(value)) {
            return null;
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

    private static byte wireType(String value) {
        return "TIMESTAMP".equals(value) ? QwpConstants.TYPE_TIMESTAMP : QwpConstants.TYPE_TIMESTAMP_NANOS;
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

        private int i32() {
            Assert.assertTrue(position <= limit - Integer.BYTES);
            int value = Unsafe.getUnsafe().getInt(address + position);
            position += 4;
            return value;
        }

        private String ascii(int length) {
            byte[] value = new byte[length];
            for (int i = 0; i < length; i++) {
                value[i] = (byte) u8();
            }
            return new String(value, StandardCharsets.UTF_8);
        }

        private long i64() {
            Assert.assertTrue(position <= limit - Long.BYTES);
            long value = Unsafe.getUnsafe().getLong(address + position);
            position += 8;
            return value;
        }

        private int position() {
            return position;
        }

        private void skip(int length) {
            position += length;
            Assert.assertTrue(position <= limit);
        }

        private String string() {
            int length = varint();
            byte[] bytes = new byte[length];
            for (int i = 0; i < length; i++) {
                bytes[i] = (byte) u8();
            }
            return new String(bytes, StandardCharsets.UTF_8);
        }

        private int u8() {
            Assert.assertTrue(position < limit);
            return Unsafe.getUnsafe().getByte(address + position++) & 0xff;
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
