/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.Sender;
import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaProtocol;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpSchemaBindingLongTextTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/long-to-text.tsv";

    @Test
    public void testCorpusUsesExactTextWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingLongTextTest.class.getResourceAsStream(CORPUS);
            Assert.assertNotNull(CORPUS, stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') continue;
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 6, fields.length);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        byte wireType = wireType(fields[3]);
                        QwpSchemaBinding binding = binding(buffer, column("value", targetType(fields[2])));
                        binding.longColumn("value", Long.parseLong(fields[1]));
                        buffer.nextRow();
                        int size = encoder.encodeSchema(buffer);
                        Reader reader = tableReader(encoder, size, wireType, 1);
                        if ("<NULL>".equals(fields[4])) {
                            Assert.assertEquals("<NULL>", fields[5]);
                            Assert.assertEquals(1, reader.byteValue());
                            Assert.assertEquals(1, reader.byteValue());
                            if (wireType == QwpConstants.TYPE_VARCHAR) Assert.assertEquals(0, reader.intValue());
                            else Assert.assertEquals(0, reader.varint());
                        } else {
                            byte[] expected = hex(fields[4]);
                            Assert.assertEquals(fields[5], new String(expected, StandardCharsets.UTF_8));
                            Assert.assertEquals(0, reader.byteValue());
                            if (wireType == QwpConstants.TYPE_VARCHAR) {
                                Assert.assertEquals(0, reader.intValue());
                                Assert.assertEquals(expected.length, reader.intValue());
                                Assert.assertArrayEquals(expected, reader.bytes(expected.length));
                            } else {
                                Assert.assertEquals(1, reader.varint());
                                Assert.assertArrayEquals(expected, reader.stringBytes());
                                Assert.assertEquals(0, reader.varint());
                            }
                        }
                        Assert.assertEquals(size, reader.position());
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertEquals(18, count);
        });
    }

    @Test
    public void testDuplicateOmissionRollbackAndReset() throws Exception {
        assertMemoryLeak(() -> {
            for (int targetType : new int[]{ColumnType.STRING, ColumnType.VARCHAR, ColumnType.SYMBOL}) {
                try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    byte wireType = targetType == ColumnType.SYMBOL
                            ? QwpConstants.TYPE_SYMBOL
                            : QwpConstants.TYPE_VARCHAR;
                    QwpSchemaBinding binding = binding(buffer, column("value", targetType));
                    binding.longColumn("value", Long.MIN_VALUE).binaryColumn("value", new byte[]{1});
                    buffer.nextRow();
                    binding.longColumn("value", 42);
                    buffer.nextRow();
                    buffer.nextRow();
                    int size = encoder.encodeSchema(buffer);
                    Reader reader = tableReader(encoder, size, wireType, 3);
                    Assert.assertEquals(1, reader.byteValue());
                    Assert.assertEquals(5, reader.byteValue());
                    if (wireType == QwpConstants.TYPE_VARCHAR) {
                        Assert.assertEquals(0, reader.intValue());
                        Assert.assertEquals(2, reader.intValue());
                        Assert.assertArrayEquals(new byte[]{'4', '2'}, reader.bytes(2));
                    } else {
                        Assert.assertEquals(1, reader.varint());
                        Assert.assertArrayEquals(new byte[]{'4', '2'}, reader.stringBytes());
                        Assert.assertEquals(0, reader.varint());
                    }
                    Assert.assertEquals(size, reader.position());
                }
            }
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer,
                        column("a", ColumnType.STRING), column("bad", ColumnType.UUID),
                        column("c", ColumnType.VARCHAR), column("sym", ColumnType.SYMBOL));
                binding.longColumn("a", 10).stringColumn("a", "ignored");
                buffer.nextRow();
                binding.longColumn("sym", 7).longColumn("sym", Long.MIN_VALUE);
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.longColumn("bad", 20));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.longColumn("c", 30);
                buffer.nextRow();
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                Reader reader = tableReader(encoder, size, QwpConstants.TYPE_VARCHAR, 3, 2);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.byteValue());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(6, reader.byteValue());
                Assert.assertEquals(0, reader.intValue());
                Assert.assertEquals(2, reader.intValue());
                Assert.assertArrayEquals(new byte[]{'1', '0'}, reader.bytes(2));
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(5, reader.byteValue());
                Assert.assertEquals(0, reader.intValue());
                Assert.assertEquals(2, reader.intValue());
                Assert.assertArrayEquals(new byte[]{'3', '0'}, reader.bytes(2));
                Assert.assertEquals(size, reader.position());
            }
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, column("value", ColumnType.SYMBOL));
                binding.longColumn("value", 8);
                buffer.nextRow();
                buffer.reset();
                binding.longColumn("value", 42);
                buffer.nextRow();
                binding.longColumn("value", 43);
                buffer.nextRow();
                binding.longColumn("value", 42);
                buffer.nextRow();
                int resetSize = encoder.encodeSchema(buffer);
                Reader reset = tableReader(encoder, resetSize, QwpConstants.TYPE_SYMBOL, 3);
                Assert.assertEquals(0, reset.byteValue());
                Assert.assertEquals(2, reset.varint());
                Assert.assertArrayEquals(new byte[]{'4', '2'}, reset.stringBytes());
                Assert.assertArrayEquals(new byte[]{'4', '3'}, reset.stringBytes());
                Assert.assertEquals(0, reset.varint());
                Assert.assertEquals(1, reset.varint());
                Assert.assertEquals(0, reset.varint());
                Assert.assertEquals(resetSize, reset.position());
            }
        });
    }

    @Test
    public void testMissingSchemaInfersNativeLong() {
        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
             QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
            binding.longColumn("value", 42);
            buffer.nextRow();
            Assert.assertEquals(1, buffer.getColumnCount());
            Assert.assertEquals(QwpConstants.TYPE_LONG, buffer.getColumnDefs()[0].getTypeCode());
            int size = encoder.encodeSchema(buffer);
            Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
            reader.skip(QwpConstants.HEADER_SIZE);
            Assert.assertEquals("t", reader.string());
            Assert.assertEquals(0, reader.byteValue());
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals("value", reader.string());
            Assert.assertEquals(QwpConstants.TYPE_LONG, reader.byteValue());
            Assert.assertEquals(0, reader.byteValue());
            Assert.assertEquals(42, reader.longValue());
            Assert.assertEquals(size, reader.position());
        }
    }

    @Test
    public void testOwnerBackedSymbolUsesGlobalDictionary() throws Exception {
        assertMemoryLeak(() -> {
            TestWebSocketServer.WebSocketServerHandler handler = new TestWebSocketServer.WebSocketServerHandler() { };
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                try (Sender senderApi = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                        + ";close_flush_timeout_millis=0;")) {
                    QwpWebSocketSender sender = (QwpWebSocketSender) senderApi;
                    Assert.assertEquals(0, sender.getOrAddGlobalSymbol("prefix"));
                    GlobalSymbolDictionary mirror = new GlobalSymbolDictionary();
                    Assert.assertEquals(0, mirror.getOrAddSymbol("prefix"));
                    try (QwpTableBuffer buffer = new QwpTableBuffer("t", sender);
                         QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                        QwpSchemaBinding binding = binding(buffer, column("value", ColumnType.SYMBOL));
                        binding.longColumn("value", 42);
                        buffer.nextRow();
                        binding.longColumn("value", 43);
                        buffer.nextRow();
                        binding.longColumn("value", 42);
                        buffer.nextRow();
                        Assert.assertEquals(1, sender.getOrAddGlobalSymbol("42"));
                        Assert.assertEquals(2, sender.getOrAddGlobalSymbol("43"));
                        Assert.assertEquals(0, buffer.getColumn(0).getSymbolDictionarySize());
                        mirror.getOrAddSymbol("42");
                        mirror.getOrAddSymbol("43");
                        encoder.beginSchemaMessage(1, mirror, -1, 2);
                        encoder.addSchemaTable(buffer, binding.getTableId(), binding.getMetadataVersion());
                        int size = encoder.finishMessage();
                        Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
                        reader.skip(QwpConstants.HEADER_SIZE);
                        Assert.assertEquals(0, reader.varint());
                        Assert.assertEquals(3, reader.varint());
                        Assert.assertArrayEquals("prefix".getBytes(StandardCharsets.UTF_8), reader.stringBytes());
                        Assert.assertArrayEquals("42".getBytes(StandardCharsets.UTF_8), reader.stringBytes());
                        Assert.assertArrayEquals("43".getBytes(StandardCharsets.UTF_8), reader.stringBytes());
                        skipTableHeader(reader, QwpConstants.TYPE_SYMBOL, 3);
                        Assert.assertEquals(0, reader.byteValue());
                        Assert.assertEquals(1, reader.varint());
                        Assert.assertEquals(2, reader.varint());
                        Assert.assertEquals(1, reader.varint());
                        Assert.assertEquals(size, reader.position());
                    }
                }
            }
        });
    }

    private static QwpSchemaBinding binding(QwpTableBuffer buffer, byte[]... columns) {
        return new QwpSchemaBinding(buffer, response((byte) QwpSchemaProtocol.RESULT_KNOWN, columns));
    }

    private static byte[] column(String name, int type) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + bytes.length + 6).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) bytes.length).put(bytes).putInt(type).putShort((short) 0).array();
    }

    private static byte[] frame(byte[] payload) {
        return ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payload.length).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(QwpConstants.MAGIC_MESSAGE).put((byte) 1).put(QwpSchemaProtocol.FLAG_CONTROL)
                .putShort((short) 0).putInt(payload.length).put(payload).array();
    }

    private static QwpSchemaResponse missing() {
        ByteBuffer payload = ByteBuffer.allocate(1 + 8 + 1).order(ByteOrder.LITTLE_ENDIAN)
                .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(1).put((byte) QwpSchemaProtocol.RESULT_MISSING);
        return decodeResponse(payload.array());
    }

    private static QwpSchemaResponse response(byte result, byte[]... columns) {
        int length = 1 + 8 + 1 + 4 + 8 + 2 + 2;
        for (byte[] column : columns) length += column.length;
        ByteBuffer payload = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN)
                .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(1).put(result)
                .putInt(result == QwpSchemaProtocol.RESULT_KNOWN ? 1 : -1)
                .putLong(result == QwpSchemaProtocol.RESULT_KNOWN ? 1 : -1)
                .putShort((short) -1).putShort((short) columns.length);
        for (byte[] column : columns) payload.put(column);
        return decodeResponse(payload.array());
    }

    private static QwpSchemaResponse decodeResponse(byte[] payload) {
        byte[] frame = frame(payload);
        long address = Unsafe.malloc(frame.length, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < frame.length; i++) Unsafe.getUnsafe().putByte(address + i, frame[i]);
            return QwpSchemaProtocol.decodeResponse(address, frame.length);
        } finally {
            Unsafe.free(address, frame.length, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, byte wireType, int rows) {
        return tableReader(encoder, size, wireType, rows, 1);
    }

    private static void skipTableHeader(Reader reader, byte wireType, int rows) {
        Assert.assertEquals("t", reader.string());
        Assert.assertEquals(1, reader.byteValue());
        Assert.assertEquals(1, reader.intValue());
        Assert.assertEquals(1, reader.longValue());
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(1, reader.varint());
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(wireType, reader.byteValue());
    }

    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, byte wireType, int rows, int columns) {
        Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        Assert.assertEquals("t", reader.string());
        Assert.assertEquals(1, reader.byteValue());
        Assert.assertEquals(1, reader.intValue());
        Assert.assertEquals(1, reader.longValue());
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(columns, reader.varint());
        if (columns == 1) {
            Assert.assertEquals("value", reader.string());
            Assert.assertEquals(wireType, reader.byteValue());
        }
        return reader;
    }

    private static int targetType(String value) {
        if ("STRING".equals(value)) return ColumnType.STRING;
        if ("VARCHAR".equals(value)) return ColumnType.VARCHAR;
        if ("SYMBOL".equals(value)) return ColumnType.SYMBOL;
        throw new AssertionError(value);
    }

    private static byte wireType(String value) {
        return "SYMBOL".equals(value) ? QwpConstants.TYPE_SYMBOL : QwpConstants.TYPE_VARCHAR;
    }

    private static byte[] hex(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) bytes[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        return bytes;
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

        private int byteValue() {
            Assert.assertTrue(position < limit);
            return Unsafe.getUnsafe().getByte(address + position++) & 0xff;
        }

        private byte[] bytes(int length) {
            byte[] value = new byte[length];
            for (int i = 0; i < length; i++) value[i] = (byte) byteValue();
            return value;
        }

        private int intValue() {
            int value = Unsafe.getUnsafe().getInt(address + position);
            position += 4;
            return value;
        }

        private long longValue() {
            long value = Unsafe.getUnsafe().getLong(address + position);
            position += 8;
            return value;
        }
        private int position() { return position; }
        private void skip(int length) {
            position += length;
            Assert.assertTrue(position <= limit);
        }
        private String string() { return new String(stringBytes(), StandardCharsets.UTF_8); }
        private byte[] stringBytes() { return bytes(varint()); }
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
