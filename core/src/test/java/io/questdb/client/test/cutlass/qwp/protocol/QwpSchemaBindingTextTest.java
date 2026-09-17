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
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaResponse;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Unsafe;
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
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.known;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.skipTableHeader;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpSchemaBindingTextTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/text-to-text.tsv";

    @Test
    public void testConformanceCorpusUsesExactTargetWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingTextTest.class.getResourceAsStream(CORPUS);
            Assert.assertNotNull(CORPUS, stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') {
                        continue;
                    }
                    String[] fields = line.split("\\t", -1);
                    Assert.assertEquals(line, 7, fields.length);
                    try {
                        String value = "<NULL>".equals(fields[3]) ? null : utf16(fields[3]);
                        byte wireType = wireType(fields[4]);
                        try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                             QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                            QwpSchemaBinding binding = binding(buffer, column("value", targetType(fields[2])));
                            append(binding, fields[1], value);
                            buffer.nextRow();
                            QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), wireType);
                            if (value == null) {
                                Assert.assertEquals(1, reader.u8());
                                Assert.assertEquals(1, reader.u8());
                                if (wireType == QwpConstants.TYPE_VARCHAR) {
                                    Assert.assertEquals(0, reader.i32());
                                } else {
                                    Assert.assertEquals(0, reader.varint());
                                }
                            } else if (wireType == QwpConstants.TYPE_VARCHAR) {
                                byte[] expected = hex(fields[5]);
                                Assert.assertEquals(0, reader.u8());
                                Assert.assertEquals(0, reader.i32());
                                Assert.assertEquals(expected.length, reader.i32());
                                Assert.assertArrayEquals(expected, reader.bytes(expected.length));
                                Assert.assertEquals(utf16(fields[6]), new String(expected, StandardCharsets.UTF_8));
                            } else {
                                byte[] expected = hex(fields[5]);
                                Assert.assertEquals(0, reader.u8());
                                Assert.assertEquals(1, reader.varint());
                                Assert.assertArrayEquals(expected, reader.stringBytes());
                                Assert.assertEquals(0, reader.varint());
                                Assert.assertEquals(utf16(fields[6]), new String(expected, StandardCharsets.UTF_8));
                            }
                            Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                        }
                        count++;
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                    }
                }
            }
            Assert.assertEquals(90, count);
        });
    }

    @Test
    public void testFirstValueNullOmissionAndMutableInput() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer,
                        column("text", ColumnType.STRING), column("sym", ColumnType.SYMBOL));
                StringBuilder mutable = new StringBuilder("before");
                binding.symbol("text", mutable).stringColumn("text", "ignored");
                binding.stringColumn("sym", "first").symbol("sym", null);
                mutable.replace(0, mutable.length(), "after");
                buffer.nextRow();
                binding.stringColumn("text", null).symbol("sym", null);
                buffer.nextRow();
                buffer.nextRow();
                Assert.assertEquals("before".length(), buffer.getColumn(0).getStringDataSize());
                Assert.assertArrayEquals("before".getBytes(StandardCharsets.UTF_8),
                        bytes(buffer.getColumn(0).getStringDataAddress(), "before".length()));
                Assert.assertEquals("first", buffer.getColumn(1).getSymbolValue(0).toString());
                int size = encoder.encodeSchema(buffer);
                Assert.assertTrue(size > QwpConstants.HEADER_SIZE);
                Assert.assertEquals(3, buffer.getRowCount());
            }
        });
    }

    @Test
    public void testFailureRollbackAndLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaResponse schema = known(-1,
                        column("a", ColumnType.STRING), column("only_b", ColumnType.SYMBOL),
                        column("bad", ColumnType.UUID), column("c", ColumnType.VARCHAR));
                QwpSchemaBinding binding = new QwpSchemaBinding(buffer, schema);
                binding.stringColumn("a", "A");
                buffer.nextRow();
                binding.symbol("only_b", "B");
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.symbol("bad", "invalid"));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.symbol("c", "C");
                buffer.nextRow();
                Assert.assertEquals(2, buffer.getColumnCount());
                Assert.assertEquals("a", buffer.getColumnDefs()[0].getName());
                Assert.assertEquals("c", buffer.getColumnDefs()[1].getName());
                Assert.assertTrue(encoder.encodeSchema(buffer) > 0);

                buffer.reset();
                binding.symbol("a", "reset");
                buffer.nextRow();
                int resetSize = encoder.encodeSchema(buffer);
                QwpTestWireReader reset = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), resetSize);
                reset.skip(QwpConstants.HEADER_SIZE);
                Assert.assertEquals("t", new String(reset.stringBytes(), StandardCharsets.UTF_8));
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.i32());
                Assert.assertEquals(1, reset.i64());
                Assert.assertEquals(1, reset.varint());
                Assert.assertEquals(2, reset.varint());
                Assert.assertEquals("a", new String(reset.stringBytes(), StandardCharsets.UTF_8));
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reset.u8());
                Assert.assertEquals("c", new String(reset.stringBytes(), StandardCharsets.UTF_8));
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reset.u8());
                Assert.assertEquals(0, reset.u8());
                Assert.assertEquals(0, reset.i32());
                Assert.assertEquals(5, reset.i32());
                Assert.assertArrayEquals("reset".getBytes(StandardCharsets.UTF_8), reset.bytes(5));
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(0, reset.i32());
                Assert.assertEquals(resetSize, reset.position());
                buffer.clear();
                assertIllegalState(() -> binding.symbol("a", "stale"));
                assertIllegalState(() -> binding.stringColumn("a", "stale"));
            }
        });
    }

    @Test
    public void testUnsupportedParameterizedUnknownAndDesignatedTargets() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer,
                    column("long", ColumnType.LONG), column("future", ColumnType.SYMBOL, new byte[]{1}),
                    column("flagged", ColumnType.STRING | 0x10000));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.symbol("long", null));
            buffer.cancelCurrentRow();
            buffer.rollbackUncommittedColumns();
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.symbol("future", "x"));
            buffer.cancelCurrentRow();
            buffer.rollbackUncommittedColumns();
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.stringColumn("flagged", "x"));
            buffer.cancelCurrentRow();
            buffer.rollbackUncommittedColumns();
            binding.symbol("missing", "x");
            Assert.assertEquals(QwpConstants.TYPE_SYMBOL,
                    buffer.getColumnDefs()[buffer.getColumnCount() - 1].getTypeCode());
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = new QwpSchemaBinding(buffer, known(0, column("d", ColumnType.SYMBOL)));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.symbol("d", null));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.stringColumn("d", null));
        }
    }

    @Test
    public void testUnsupportedDuplicateIsIgnoredAcrossTextSetters() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer,
                        column("uuid", ColumnType.UUID), column("long", ColumnType.LONG));
                binding.stringColumn("uuid", "01234567-89ab-cdef-0123-456789abcdef")
                        .symbol("uuid", null);
                binding.longColumn("long", 7).symbol("long", null);
                buffer.nextRow();
                Assert.assertTrue(encoder.encodeSchema(buffer) > 0);
            }
        });
    }

    @Test
    public void testOwnerBackedGlobalSymbolIdsAndEncoding() throws Exception {
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
                        StringBuilder mutable = new StringBuilder("mutable");
                        binding.symbol("value", mutable);
                        mutable.replace(0, mutable.length(), "changed");
                        buffer.nextRow();
                        binding.stringColumn("value", "?");
                        buffer.nextRow();
                        binding.symbol("value", "\ud800");
                        buffer.nextRow();
                        binding.symbol("value", "mutable");
                        buffer.nextRow();

                        Assert.assertEquals(1, sender.getOrAddGlobalSymbol("mutable"));
                        Assert.assertEquals(2, sender.getOrAddGlobalSymbol("?"));
                        Assert.assertEquals(3, sender.getOrAddGlobalSymbol("\ud800"));
                        Assert.assertEquals(1, sender.getOrAddGlobalSymbol("mutable"));
                        mirror.getOrAddSymbol("mutable");
                        mirror.getOrAddSymbol("?");
                        mirror.getOrAddSymbol("\ud800");
                        Assert.assertEquals(0, buffer.getColumn(0).getSymbolDictionarySize());

                        encoder.beginSchemaMessage(1, mirror, -1, 3);
                        encoder.addSchemaTable(buffer, binding.getTableId(), binding.getMetadataVersion());
                        int size = encoder.finishMessage();
                        QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
                        reader.skip(QwpConstants.HEADER_SIZE);
                        Assert.assertEquals(0, reader.varint());
                        Assert.assertEquals(4, reader.varint());
                        Assert.assertArrayEquals("prefix".getBytes(StandardCharsets.UTF_8), reader.stringBytes());
                        Assert.assertArrayEquals("mutable".getBytes(StandardCharsets.UTF_8), reader.stringBytes());
                        Assert.assertArrayEquals(new byte[]{'?'}, reader.stringBytes());
                        Assert.assertArrayEquals(new byte[]{'?'}, reader.stringBytes());
                        skipTableHeader(reader, QwpConstants.TYPE_SYMBOL, 4);
                        Assert.assertEquals(0, reader.u8());
                        Assert.assertEquals(1, reader.varint());
                        Assert.assertEquals(2, reader.varint());
                        Assert.assertEquals(3, reader.varint());
                        Assert.assertEquals(1, reader.varint());
                        Assert.assertEquals(size, reader.position());
                    }
                }
            }
        });
    }

    private static void append(QwpSchemaBinding binding, String inputType, CharSequence value) {
        if ("STRING".equals(inputType)) {
            binding.stringColumn("value", value);
        } else if ("SYMBOL".equals(inputType)) {
            binding.symbol("value", value);
        } else {
            throw new AssertionError("unknown input type: " + inputType);
        }
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, byte wireType) {
        QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        skipTableHeader(reader, wireType, 1);
        return reader;
    }

    private static int targetType(String value) {
        switch (value) {
            case "STRING":
                return ColumnType.STRING;
            case "VARCHAR":
                return ColumnType.VARCHAR;
            case "SYMBOL":
                return ColumnType.SYMBOL;
            default:
                throw new AssertionError("unknown target type: " + value);
        }
    }

    private static byte wireType(String value) {
        switch (value) {
            case "VARCHAR":
                return QwpConstants.TYPE_VARCHAR;
            case "SYMBOL":
                return QwpConstants.TYPE_SYMBOL;
            default:
                throw new AssertionError("unknown wire type: " + value);
        }
    }

    private static byte[] hex(String value) {
        Assert.assertEquals(0, value.length() & 1);
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    private static byte[] bytes(long address, int length) {
        byte[] value = new byte[length];
        for (int i = 0; i < length; i++) {
            value[i] = Unsafe.getUnsafe().getByte(address + i);
        }
        return value;
    }

    private static String utf16(String value) {
        Assert.assertEquals(0, value.length() & 3);
        char[] chars = new char[value.length() / 4];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) Integer.parseInt(value.substring(i * 4, i * 4 + 4), 16);
        }
        return new String(chars);
    }

    private static void assertIllegalState(Runnable action) {
        Assert.assertThrows(IllegalStateException.class, action::run);
    }
}
