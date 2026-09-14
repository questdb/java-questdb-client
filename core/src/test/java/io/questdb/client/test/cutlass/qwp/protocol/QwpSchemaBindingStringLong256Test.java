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
                            Reader reader = tableReader(encoder, size, 1);
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
                            Reader reader = tableReader(encoder, size, 1);
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
                Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
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
                Reader reset = new Reader(encoder.getBuffer().getBufferPtr(), resetSize);
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
                    response(0, column("ts", ColumnType.TIMESTAMP_MICRO)));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.stringColumn("ts", "0x00"));
        }
    }

    private static void assertLong256Values(Reader reader, int bitmap, long value) {
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(bitmap, reader.u8());
        Assert.assertEquals(value, reader.i64());
        Assert.assertEquals(0, reader.i64());
        Assert.assertEquals(0, reader.i64());
        Assert.assertEquals(0, reader.i64());
    }

    private static QwpSchemaBinding binding(QwpTableBuffer buffer, byte[]... columns) {
        return new QwpSchemaBinding(buffer, response(columns));
    }

    private static byte[] column(String name, int type) {
        return column(name, type, new byte[0]);
    }

    private static byte[] parameterizedColumn(String name, int type) {
        return column(name, type, new byte[]{1});
    }

    private static byte[] column(String name, int type, byte[] parameters) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + bytes.length + 6 + parameters.length).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) bytes.length).put(bytes).putInt(type).putShort((short) parameters.length)
                .put(parameters).array();
    }

    private static QwpSchemaResponse missing() {
        ByteBuffer payload = ByteBuffer.allocate(10).order(ByteOrder.LITTLE_ENDIAN)
                .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(1).put((byte) QwpSchemaProtocol.RESULT_MISSING);
        return decode(payload.array());
    }

    private static QwpSchemaResponse response(byte[]... columns) {
        return response(-1, columns);
    }

    private static QwpSchemaResponse response(int designatedIndex, byte[]... columns) {
        int length = 26;
        for (byte[] column : columns) length += column.length;
        ByteBuffer payload = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN)
                .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(1).put((byte) QwpSchemaProtocol.RESULT_KNOWN)
                .putInt(1).putLong(1).putShort((short) designatedIndex).putShort((short) columns.length);
        for (byte[] column : columns) payload.put(column);
        return decode(payload.array());
    }

    private static QwpSchemaResponse decode(byte[] payload) {
        ByteBuffer frame = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payload.length).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(QwpConstants.MAGIC_MESSAGE).put((byte) 1).put(QwpSchemaProtocol.FLAG_CONTROL)
                .putShort((short) 0).putInt(payload.length).put(payload);
        long address = Unsafe.malloc(frame.capacity(), MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < frame.capacity(); i++) Unsafe.getUnsafe().putByte(address + i, frame.array()[i]);
            return QwpSchemaProtocol.decodeResponse(address, frame.capacity());
        } finally {
            Unsafe.free(address, frame.capacity(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, int rows) {
        return tableReader(encoder, size, rows, "value");
    }

    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, int rows, String column) {
        Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
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

    private static void assertReason(LineSenderSchemaException.Reason reason, Runnable action) {
        LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class, action::run);
        Assert.assertEquals(reason, error.getReason());
    }

    private static final class Reader {
        private final long address;
        private final int limit;
        private int position;

        private Reader(long address, int limit) {
            this.address = address;
            this.limit = limit;
        }

        private String ascii(int length) { return new String(bytes(length), StandardCharsets.UTF_8); }
        private byte[] bytes(int length) {
            byte[] value = new byte[length];
            for (int i = 0; i < length; i++) value[i] = (byte) u8();
            return value;
        }
        private int i32() { int value = Unsafe.getUnsafe().getInt(address + position); position += 4; return value; }
        private long i64() { long value = Unsafe.getUnsafe().getLong(address + position); position += 8; return value; }
        private int position() { return position; }
        private void skip(int length) { position += length; Assert.assertTrue(position <= limit); }
        private String string() { return ascii(varint()); }
        private int u8() { Assert.assertTrue(position < limit); return Unsafe.getUnsafe().getByte(address + position++) & 0xff; }
        private int varint() {
            int result = 0;
            int shift = 0;
            int value;
            do { value = u8(); result |= (value & 0x7f) << shift; shift += 7; } while ((value & 0x80) != 0);
            return result;
        }
    }
}
