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
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.assertReason;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.binding;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.column;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.missing;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.skipTableHeader;
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
                        QwpTestWireReader reader = tableReader(encoder, size, wireType, 1);
                        if ("<NULL>".equals(fields[4])) {
                            Assert.assertEquals("<NULL>", fields[5]);
                            Assert.assertEquals(1, reader.u8());
                            Assert.assertEquals(1, reader.u8());
                            if (wireType == QwpConstants.TYPE_VARCHAR) Assert.assertEquals(0, reader.i32());
                            else Assert.assertEquals(0, reader.varint());
                        } else {
                            byte[] expected = hex(fields[4]);
                            Assert.assertEquals(fields[5], new String(expected, StandardCharsets.UTF_8));
                            Assert.assertEquals(0, reader.u8());
                            if (wireType == QwpConstants.TYPE_VARCHAR) {
                                Assert.assertEquals(0, reader.i32());
                                Assert.assertEquals(expected.length, reader.i32());
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
                    QwpTestWireReader reader = tableReader(encoder, size, wireType, 3);
                    Assert.assertEquals(1, reader.u8());
                    Assert.assertEquals(5, reader.u8());
                    if (wireType == QwpConstants.TYPE_VARCHAR) {
                        Assert.assertEquals(0, reader.i32());
                        Assert.assertEquals(2, reader.i32());
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
                QwpTestWireReader reader = tableReader(encoder, size, QwpConstants.TYPE_VARCHAR, 3, 2);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.u8());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(6, reader.u8());
                Assert.assertEquals(0, reader.i32());
                Assert.assertEquals(2, reader.i32());
                Assert.assertArrayEquals(new byte[]{'1', '0'}, reader.bytes(2));
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(5, reader.u8());
                Assert.assertEquals(0, reader.i32());
                Assert.assertEquals(2, reader.i32());
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
                QwpTestWireReader reset = tableReader(encoder, resetSize, QwpConstants.TYPE_SYMBOL, 3);
                Assert.assertEquals(0, reset.u8());
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
            QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
            reader.skip(QwpConstants.HEADER_SIZE);
            Assert.assertEquals("t", reader.string());
            Assert.assertEquals(0, reader.u8());
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals(1, reader.varint());
            Assert.assertEquals("value", reader.string());
            Assert.assertEquals(QwpConstants.TYPE_LONG, reader.u8());
            Assert.assertEquals(0, reader.u8());
            Assert.assertEquals(42, reader.i64());
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
                        QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
                        reader.skip(QwpConstants.HEADER_SIZE);
                        Assert.assertEquals(0, reader.varint());
                        Assert.assertEquals(3, reader.varint());
                        Assert.assertArrayEquals("prefix".getBytes(StandardCharsets.UTF_8), reader.stringBytes());
                        Assert.assertArrayEquals("42".getBytes(StandardCharsets.UTF_8), reader.stringBytes());
                        Assert.assertArrayEquals("43".getBytes(StandardCharsets.UTF_8), reader.stringBytes());
                        skipTableHeader(reader, QwpConstants.TYPE_SYMBOL, 3);
                        Assert.assertEquals(0, reader.u8());
                        Assert.assertEquals(1, reader.varint());
                        Assert.assertEquals(2, reader.varint());
                        Assert.assertEquals(1, reader.varint());
                        Assert.assertEquals(size, reader.position());
                    }
                }
            }
        });
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, byte wireType, int rows) {
        return tableReader(encoder, size, wireType, rows, 1);
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, byte wireType, int rows, int columns) {
        QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        Assert.assertEquals("t", reader.string());
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(1, reader.i32());
        Assert.assertEquals(1, reader.i64());
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(columns, reader.varint());
        if (columns == 1) {
            Assert.assertEquals("value", reader.string());
            Assert.assertEquals(wireType, reader.u8());
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
}
