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
import java.time.temporal.ChronoUnit;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpSchemaBindingStringNumericTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/string-to-numeric.tsv";

    @Test
    public void testConformanceCorpusUsesExactTargetWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingStringNumericTest.class.getResourceAsStream(CORPUS);
            Assert.assertNotNull(CORPUS, stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') continue;
                    String[] f = line.split("\\t", -1);
                    Assert.assertEquals(line, 7, f.length);
                    Assert.assertTrue(line, "VALID".equals(f[6]) || "INVALID".equals(f[6]));
                    CharSequence value = "<NULL>".equals(f[1]) ? null : utf16(f[1]);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer, -1, column("value", targetType(f[2])));
                        if ("INVALID".equals(f[6])) {
                            Assert.assertEquals(f[0], "<INVALID>", f[4]);
                            Assert.assertEquals(f[0], "<INVALID>", f[5]);
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, () -> binding.stringColumn("value", value));
                        } else {
                            binding.stringColumn("value", value);
                            buffer.nextRow();
                            Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), wireType(f[3]), 1, 1);
                            if ("<NULL>".equals(f[4])) {
                                Assert.assertEquals(1, reader.byteValue());
                                Assert.assertEquals(1, reader.byteValue());
                            } else {
                                Assert.assertEquals(0, reader.byteValue());
                                byte[] expected = littleEndianHex(f[4]);
                                Assert.assertArrayEquals(f[0], expected, reader.bytes(expected.length));
                            }
                            Assert.assertEquals(f[0], encoder.getBuffer().getPosition(), reader.position());
                        }
                    } catch (AssertionError | RuntimeException e) {
                        throw new AssertionError("case_id=" + f[0] + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertEquals(429, count);
        });
    }

    @Test
    public void testDuplicatePrecedesInvalidParseAndUnsupportedConversion() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("n", ColumnType.INT), column("uuid", ColumnType.UUID));
                binding.stringColumn("n", "42").stringColumn("n", "invalid");
                binding.uuidColumn("uuid", 1, 2).stringColumn("uuid", "not-a-uuid");
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
                reader.skip(QwpConstants.HEADER_SIZE);
                skipTablePrefix(reader, 1, 2);
                Assert.assertEquals("n", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_INT, reader.byteValue());
                Assert.assertEquals("uuid", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_UUID, reader.byteValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(42, reader.intValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(1, reader.longValue());
                Assert.assertEquals(2, reader.longValue());
                Assert.assertEquals(size, reader.position());
            }
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.TIMESTAMP_MICRO));
                binding.timestampColumn("value", 123, ChronoUnit.MICROS)
                        .stringColumn("value", "not-a-timestamp-conversion");
                buffer.nextRow();
                Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), QwpConstants.TYPE_TIMESTAMP, 1, 1);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(123, reader.longValue());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
            }
        });
    }

    @Test
    public void testFailureRollbackRemovesFailedOnlyColumnAndPreservesRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("a", ColumnType.INT), column("only_b", ColumnType.LONG),
                        column("bad", ColumnType.SHORT), column("c", ColumnType.DOUBLE));
                binding.stringColumn("a", "1");
                buffer.nextRow();
                binding.stringColumn("only_b", "2");
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, () -> binding.stringColumn("bad", "+3"));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.stringColumn("c", "-0.0");
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
                reader.skip(QwpConstants.HEADER_SIZE);
                skipTablePrefix(reader, 2, 2);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_INT, reader.byteValue());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_DOUBLE, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(2, reader.byteValue());
                Assert.assertEquals(1, reader.intValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(0x8000000000000000L, reader.longValue());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testResetClearAndMutableInput() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.LONG));
                StringBuilder mutable = new StringBuilder("7");
                binding.stringColumn("value", mutable);
                mutable.replace(0, 1, "9");
                buffer.nextRow();
                Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), QwpConstants.TYPE_LONG, 1, 1);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(7, reader.longValue());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());

                buffer.reset();
                binding.stringColumn("value", "8");
                buffer.nextRow();
                reader = tableReader(encoder, encoder.encodeSchema(buffer), QwpConstants.TYPE_LONG, 1, 1);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(8, reader.longValue());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                buffer.clear();
                assertIllegalState(() -> binding.stringColumn("value", "1"));
            }
        });
    }

    @Test
    public void testNullAndParsedSentinelsHaveDistinctWirePresence() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.INT));
                binding.stringColumn("value", "-2147483648");
                buffer.nextRow();
                binding.stringColumn("value", null);
                buffer.nextRow();
                Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), QwpConstants.TYPE_INT, 2, 1);
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(2, reader.byteValue());
                Assert.assertEquals(Integer.MIN_VALUE, reader.intValue());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
            }
        });
    }

    @Test
    public void testUnsupportedParameterizedUnknownAndDesignatedTargets() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, -1,
                    column("bool", ColumnType.BOOLEAN), column("date", ColumnType.DATE),
                    column("future", ColumnType.INT, new byte[]{1}),
                    column("flagged", ColumnType.LONG | 0x10000));
            assertSchemaError(LineSenderSchemaException.Reason.INVALID_VALUE, "bool", "BOOLEAN", () -> binding.stringColumn("bool", "2"));
            rollback(buffer);
            assertSchemaError(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, "date", "DATE", () -> binding.stringColumn("date", "1"));
            rollback(buffer);
            assertSchemaError(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, "future", "INT", () -> binding.stringColumn("future", "1"));
            rollback(buffer);
            assertSchemaError(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, "flagged", "unknown", () -> binding.stringColumn("flagged", "1"));
            rollback(buffer);
            binding.stringColumn("missing", "1");
            Assert.assertEquals(QwpConstants.TYPE_VARCHAR,
                    buffer.getColumnDefs()[buffer.getColumnCount() - 1].getTypeCode());
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, 0, column("d", ColumnType.DOUBLE));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.stringColumn("d", null));
        }
    }

    private static QwpSchemaBinding binding(QwpTableBuffer buffer, int designated, byte[]... columns) {
        return new QwpSchemaBinding(buffer, known(designated, columns));
    }

    private static byte[] column(String name, int type) {
        return column(name, type, new byte[0]);
    }

    private static byte[] column(String name, int type, byte[] params) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + bytes.length + 4 + 2 + params.length).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) bytes.length).put(bytes).putInt(type).putShort((short) params.length).put(params).array();
    }
    private static QwpSchemaResponse known(int designated, byte[]... columns) {
        int length = 1 + 8 + 1 + 4 + 8 + 2 + 2;
        for (byte[] column : columns) {
            length += column.length;
        }
        ByteBuffer payload = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN).put(QwpSchemaProtocol.KIND_SCHEMA).putLong(1)
                .put((byte) QwpSchemaProtocol.RESULT_KNOWN).putInt(1).putLong(1).putShort((short) designated).putShort((short) columns.length);
        for (byte[] column : columns) {
            payload.put(column);
        }
        ByteBuffer frame = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + length).order(ByteOrder.LITTLE_ENDIAN).putInt(QwpConstants.MAGIC_MESSAGE)
                .put((byte) 1).put(QwpSchemaProtocol.FLAG_CONTROL).putShort((short) 0).putInt(length).put(payload.array());
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
    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, byte type, int rows, int columns) {
        Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        skipTablePrefix(reader, rows, columns);
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(type, reader.byteValue());
        return reader;
    }

    private static void skipTablePrefix(Reader reader, int rows, int columns) {
        Assert.assertEquals("t", reader.string());
        Assert.assertEquals(1, reader.byteValue());
        Assert.assertEquals(1, reader.intValue());
        Assert.assertEquals(1, reader.longValue());
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(columns, reader.varint());
    }

    private static int targetType(String value) {
        switch (value) {
            case "BYTE": return ColumnType.BYTE;
            case "SHORT": return ColumnType.SHORT;
            case "INT": return ColumnType.INT;
            case "LONG": return ColumnType.LONG;
            case "FLOAT": return ColumnType.FLOAT;
            case "DOUBLE": return ColumnType.DOUBLE;
            default: throw new AssertionError(value);
        }
    }

    private static byte wireType(String value) {
        switch (value) {
            case "BYTE": return QwpConstants.TYPE_BYTE;
            case "SHORT": return QwpConstants.TYPE_SHORT;
            case "INT": return QwpConstants.TYPE_INT;
            case "LONG": return QwpConstants.TYPE_LONG;
            case "FLOAT": return QwpConstants.TYPE_FLOAT;
            case "DOUBLE": return QwpConstants.TYPE_DOUBLE;
            default: throw new AssertionError(value);
        }
    }

    private static String utf16(String hex) {
        Assert.assertEquals(0, hex.length() & 3);
        char[] chars = new char[hex.length() / 4];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) Integer.parseInt(hex.substring(i * 4, i * 4 + 4), 16);
        }
        return new String(chars);
    }

    private static byte[] littleEndianHex(String hex) {
        Assert.assertEquals(0, hex.length() & 1);
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring((bytes.length - 1 - i) * 2, (bytes.length - i) * 2), 16);
        }
        return bytes;
    }

    private static void rollback(QwpTableBuffer buffer) {
        buffer.cancelCurrentRow();
        buffer.rollbackUncommittedColumns();
    }

    private static void assertIllegalState(Runnable runnable) {
        Assert.assertThrows(IllegalStateException.class, runnable::run);
    }

    private static void assertReason(LineSenderSchemaException.Reason reason, Runnable runnable) {
        LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class, runnable::run);
        Assert.assertEquals(reason, error.getReason());
        Assert.assertFalse(error.isRetryable());
    }

    private static void assertSchemaError(
            LineSenderSchemaException.Reason reason,
            String column,
            String target,
            Runnable runnable
    ) {
        LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class, runnable::run);
        Assert.assertEquals(error.getMessage(), reason, error.getReason());
        Assert.assertFalse(error.getMessage(), error.isRetryable());
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("table=t"));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("column=" + column));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("inputType=STRING"));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("targetType=" + target));
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
            return Unsafe.getUnsafe().getByte(address + position++) & 255;
        }

        private byte[] bytes(int count) {
            Assert.assertTrue(position + count <= limit);
            byte[] bytes = new byte[count];
            for (int i = 0; i < count; i++) {
                bytes[i] = (byte) byteValue();
            }
            return bytes;
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

        private void skip(int count) {
            Assert.assertTrue(position + count <= limit);
            position += count;
        }

        private String string() {
            return new String(bytes(varint()), StandardCharsets.UTF_8);
        }

        private int varint() {
            int result = 0;
            int shift = 0;
            int value;
            do {
                value = byteValue();
                result |= (value & 127) << shift;
                shift += 7;
            } while ((value & 128) != 0);
            return result;
        }
    }
}
