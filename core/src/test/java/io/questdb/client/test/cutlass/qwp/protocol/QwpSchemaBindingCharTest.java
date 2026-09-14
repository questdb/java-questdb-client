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

public class QwpSchemaBindingCharTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/char-conversions.tsv";
    private static final String HEADER = "# case_id\tinput_kind\tutf16_hex\texpected_char\texpected_null";

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
                    Reader reader = tableReader(encoder, size, QwpConstants.TYPE_CHAR);
                    if ("<NULL>".equals(fields[2])) {
                        Assert.assertEquals(1, reader.u8());
                        Assert.assertEquals(1, reader.u8());
                    } else {
                        Assert.assertEquals(0, reader.u8());
                        Assert.assertEquals(Integer.parseInt(fields[3]), reader.u16());
                    }
                    Assert.assertEquals(size, reader.position);
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
            Reader reader = tableHeader(encoder, size, 3, 2);
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
            Assert.assertEquals(size, reader.position);

            buffer.reset();
            binding.stringColumn("a", "Q");
            buffer.nextRow();
            int resetSize = encoder.encodeSchema(buffer);
            Reader reset = tableHeader(encoder, resetSize, 1, 2);
            Assert.assertEquals("a", reset.string());
            Assert.assertEquals(QwpConstants.TYPE_CHAR, reset.u8());
            Assert.assertEquals("c", reset.string());
            Assert.assertEquals(QwpConstants.TYPE_CHAR, reset.u8());
            Assert.assertEquals(0, reset.u8());
            Assert.assertEquals('Q', reset.u16());
            Assert.assertEquals(1, reset.u8());
            Assert.assertEquals(1, reset.u8());
            Assert.assertEquals(resetSize, reset.position);
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
                .putInt(QwpConstants.MAGIC_MESSAGE).put((byte) QwpConstants.VERSION)
                .put(QwpSchemaProtocol.FLAG_CONTROL).putShort((short) 0).putInt(payload.length).put(payload);
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

    private static String fromUtf16Hex(String hex) {
        StringBuilder value = new StringBuilder(hex.length() / 4);
        for (int i = 0; i < hex.length(); i += 4) {
            value.append((char) Integer.parseInt(hex.substring(i, i + 4), 16));
        }
        return value.toString();
    }

    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, byte type) {
        Reader reader = tableHeader(encoder, size, 1, 1);
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
