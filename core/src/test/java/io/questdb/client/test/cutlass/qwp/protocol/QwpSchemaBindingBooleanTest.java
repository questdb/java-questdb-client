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

public class QwpSchemaBindingBooleanTest {
    private static final String BOOLEAN_CORPUS = "/io/questdb/client/cutlass/qwp/boolean-to-target.tsv";
    private static final String STRING_CORPUS = "/io/questdb/client/cutlass/qwp/string-to-boolean.tsv";

    @Test
    public void testBooleanCorpusUsesExactTargetWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingBooleanTest.class.getResourceAsStream(BOOLEAN_CORPUS);
            Assert.assertNotNull(BOOLEAN_CORPUS, stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') {
                        continue;
                    }
                    String[] fields = line.split("\\t", -1);
                    Assert.assertEquals(line, 6, fields.length);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer, -1, column("value", targetType(fields[2])));
                        Assert.assertTrue(fields[1], "TRUE".equals(fields[1]) || "FALSE".equals(fields[1]));
                        binding.boolColumn("value", "TRUE".equals(fields[1]));
                        buffer.nextRow();
                        Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), wireType(fields[3]), 1);
                        Assert.assertEquals(0, reader.byteValue());
                        assertValue(reader, fields[3], fields[4]);
                        Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
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
    public void testStringCorpusParsesStrictAsciiAndUsesBooleanWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingBooleanTest.class.getResourceAsStream(STRING_CORPUS);
            Assert.assertNotNull(STRING_CORPUS, stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') {
                        continue;
                    }
                    String[] fields = line.split("\\t", -1);
                    Assert.assertEquals(line, 4, fields.length);
                    CharSequence value = "<NULL>".equals(fields[1]) ? null : utf16(fields[1]);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.BOOLEAN));
                        if ("<INVALID>".equals(fields[2])) {
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, () -> binding.stringColumn("value", value));
                        } else {
                            binding.stringColumn("value", value);
                            buffer.nextRow();
                            Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), QwpConstants.TYPE_BOOLEAN, 1);
                            if (value == null) {
                                Assert.assertEquals(1, reader.byteValue());
                                Assert.assertEquals(1, reader.byteValue());
                            } else {
                                Assert.assertEquals(0, reader.byteValue());
                                Assert.assertEquals(Integer.parseInt(fields[2]), reader.byteValue());
                            }
                            Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                        }
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertEquals(80, count);
        });
    }

    @Test
    public void testBooleanPackingPastEightAndSixtyFourWithOmissions() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.BOOLEAN));
                for (int row = 0; row < 137; row++) {
                    if ((row & 1) == 0) {
                        binding.boolColumn("value", (row & 3) == 0);
                    }
                    buffer.nextRow();
                }
                Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), QwpConstants.TYPE_BOOLEAN, 137);
                Assert.assertEquals(1, reader.byteValue());
                byte[] nulls = new byte[18];
                for (int i = 0; i < 17; i++) nulls[i] = (byte) 0xaa;
                Assert.assertArrayEquals(nulls, reader.bytes(nulls.length));
                byte[] expected = new byte[9];
                for (int i = 0; i < 8; i++) expected[i] = 0x55;
                expected[8] = 0x15;
                Assert.assertArrayEquals(expected, reader.bytes(expected.length));
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());

                buffer.reset();
                for (int row = 0; row < 70; row++) buffer.nextRow();
                Reader allNull = tableReader(encoder, encoder.encodeSchema(buffer), QwpConstants.TYPE_BOOLEAN, 70);
                Assert.assertEquals(1, allNull.byteValue());
                byte[] allNulls = new byte[9];
                for (int i = 0; i < 8; i++) allNulls[i] = (byte) 0xff;
                allNulls[8] = 0x3f;
                Assert.assertArrayEquals(allNulls, allNull.bytes(allNulls.length));
                Assert.assertEquals(encoder.getBuffer().getPosition(), allNull.position());
            }
        });
    }

    @Test
    public void testDuplicateRollbackFailedRowAndLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("a", ColumnType.BOOLEAN), column("bad", ColumnType.BOOLEAN), column("c", ColumnType.INT));
                binding.boolColumn("a", true);
                buffer.nextRow();
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.stringColumn("bad", "invalid"));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.boolColumn("c", true);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
                reader.skip(QwpConstants.HEADER_SIZE);
                skipTablePrefix(reader, 2, 2);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_BOOLEAN, reader.byteValue());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_INT, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(2, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(1, reader.intValue());
                Assert.assertEquals(size, reader.position());

                buffer.reset();
                StringBuilder mutable = new StringBuilder("TrUe");
                binding.stringColumn("a", mutable).boolColumn("a", false);
                mutable.replace(0, mutable.length(), "false");
                buffer.nextRow();
                int resetSize = encoder.encodeSchema(buffer);
                Reader reset = new Reader(encoder.getBuffer().getBufferPtr(), resetSize);
                reset.skip(QwpConstants.HEADER_SIZE);
                skipTablePrefix(reset, 1, 2);
                Assert.assertEquals("a", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_BOOLEAN, reset.byteValue());
                Assert.assertEquals("c", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_INT, reset.byteValue());
                Assert.assertEquals(0, reset.byteValue());
                Assert.assertEquals(1, reset.byteValue());
                Assert.assertEquals(1, reset.byteValue());
                Assert.assertEquals(1, reset.byteValue());
                Assert.assertEquals(resetSize, reset.position());
                buffer.clear();
                assertIllegalState(() -> binding.boolColumn("a", true));
                assertIllegalState(() -> binding.stringColumn("a", "true"));
            }
        });
    }

    @Test
    public void testDuplicatePrecedesUnsupportedCrossSetterAndInvalidParse() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("flag", ColumnType.BOOLEAN), column("sym", ColumnType.SYMBOL));
                binding.boolColumn("flag", true).stringColumn("flag", "invalid");
                binding.stringColumn("sym", "first").boolColumn("sym", false);
                buffer.nextRow();
                Assert.assertTrue(encoder.encodeSchema(buffer) > 0);
            }
        });
    }

    @Test
    public void testUnsupportedParameterizedUnknownAndDesignatedTargets() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, -1,
                    column("sym", ColumnType.SYMBOL), column("future", ColumnType.BOOLEAN, new byte[]{1}),
                    column("flagged", ColumnType.BOOLEAN | 0x10000), column("plain", ColumnType.BOOLEAN));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.boolColumn("sym", true));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.symbol("plain", null));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.longColumn("plain", Long.MIN_VALUE));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.boolColumn("future", true));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.stringColumn("flagged", "true"));
            rollback(buffer);
            binding.boolColumn("missing", true);
            Assert.assertEquals(QwpConstants.TYPE_BOOLEAN,
                    buffer.getColumnDefs()[buffer.getColumnCount() - 1].getTypeCode());
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, 0, column("d", ColumnType.BOOLEAN));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.boolColumn("d", true));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.stringColumn("d", null));
        }
    }

    private static void assertValue(Reader reader, String type, String expected) {
        switch (type) {
            case "BOOLEAN": reader.assertByte(Integer.parseInt(expected)); break;
            case "BYTE": reader.assertByte(Integer.parseInt(expected)); break;
            case "SHORT": Assert.assertEquals(Integer.parseInt(expected), reader.shortValue()); break;
            case "INT": Assert.assertEquals(Integer.parseInt(expected), reader.intValue()); break;
            case "LONG": Assert.assertEquals(Long.parseLong(expected), reader.longValue()); break;
            case "FLOAT": Assert.assertEquals(Float.floatToRawIntBits(Float.parseFloat(expected)), Float.floatToRawIntBits(reader.floatValue())); break;
            case "DOUBLE": Assert.assertEquals(Double.doubleToRawLongBits(Double.parseDouble(expected)), Double.doubleToRawLongBits(reader.doubleValue())); break;
            case "VARCHAR":
                byte[] bytes = hex(expected);
                Assert.assertEquals(0, reader.intValue());
                Assert.assertEquals(bytes.length, reader.intValue());
                Assert.assertArrayEquals(bytes, reader.bytes(bytes.length));
                break;
            default: throw new AssertionError("unknown wire type: " + type);
        }
    }

    private static QwpSchemaBinding binding(QwpTableBuffer buffer, int designated, byte[]... columns) {
        return new QwpSchemaBinding(buffer, known(designated, columns));
    }

    private static byte[] column(String name, int type) { return column(name, type, new byte[0]); }

    private static byte[] column(String name, int type, byte[] params) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + bytes.length + 4 + 2 + params.length).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) bytes.length).put(bytes).putInt(type).putShort((short) params.length).put(params).array();
    }

    private static QwpSchemaResponse known(int designated, byte[]... columns) {
        int length = 1 + 8 + 1 + 4 + 8 + 2 + 2;
        for (byte[] column : columns) length += column.length;
        ByteBuffer payload = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN)
                .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(1).put((byte) QwpSchemaProtocol.RESULT_KNOWN)
                .putInt(1).putLong(1).putShort((short) designated).putShort((short) columns.length);
        for (byte[] column : columns) payload.put(column);
        ByteBuffer frame = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + length).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(QwpConstants.MAGIC_MESSAGE).put((byte) 1).put(QwpSchemaProtocol.FLAG_CONTROL)
                .putShort((short) 0).putInt(length).put(payload.array());
        long address = Unsafe.malloc(frame.capacity(), MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < frame.capacity(); i++) Unsafe.getUnsafe().putByte(address + i, frame.array()[i]);
            return QwpSchemaProtocol.decodeResponse(address, frame.capacity());
        } finally {
            Unsafe.free(address, frame.capacity(), MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, byte type, int rows) {
        Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        skipTablePrefix(reader, rows, 1);
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
            case "BOOLEAN": return ColumnType.BOOLEAN;
            case "BYTE": return ColumnType.BYTE;
            case "SHORT": return ColumnType.SHORT;
            case "INT": return ColumnType.INT;
            case "LONG": return ColumnType.LONG;
            case "FLOAT": return ColumnType.FLOAT;
            case "DOUBLE": return ColumnType.DOUBLE;
            case "STRING": return ColumnType.STRING;
            case "VARCHAR": return ColumnType.VARCHAR;
            default: throw new AssertionError("unknown target type: " + value);
        }
    }

    private static byte wireType(String value) {
        switch (value) {
            case "BOOLEAN": return QwpConstants.TYPE_BOOLEAN;
            case "BYTE": return QwpConstants.TYPE_BYTE;
            case "SHORT": return QwpConstants.TYPE_SHORT;
            case "INT": return QwpConstants.TYPE_INT;
            case "LONG": return QwpConstants.TYPE_LONG;
            case "FLOAT": return QwpConstants.TYPE_FLOAT;
            case "DOUBLE": return QwpConstants.TYPE_DOUBLE;
            case "VARCHAR": return QwpConstants.TYPE_VARCHAR;
            default: throw new AssertionError("unknown wire type: " + value);
        }
    }

    private static byte[] hex(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) bytes[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        return bytes;
    }

    private static String utf16(String value) {
        Assert.assertEquals(0, value.length() & 3);
        char[] chars = new char[value.length() / 4];
        for (int i = 0; i < chars.length; i++) chars[i] = (char) Integer.parseInt(value.substring(i * 4, i * 4 + 4), 16);
        return new String(chars);
    }

    private static void rollback(QwpTableBuffer buffer) { buffer.cancelCurrentRow(); buffer.rollbackUncommittedColumns(); }
    private static void assertIllegalState(Runnable action) { Assert.assertThrows(IllegalStateException.class, action::run); }
    private static void assertReason(LineSenderSchemaException.Reason reason, Runnable action) {
        Assert.assertEquals(reason, Assert.assertThrows(LineSenderSchemaException.class, action::run).getReason());
    }

    private static final class Reader {
        private final long address; private final int limit; private int position;
        private Reader(long address, int limit) { this.address = address; this.limit = limit; }
        private void assertByte(int value) { Assert.assertEquals(value, byteValue()); }
        private int byteValue() { Assert.assertTrue(position < limit); return Unsafe.getUnsafe().getByte(address + position++) & 0xff; }
        private byte[] bytes(int length) { Assert.assertTrue(position + length <= limit); byte[] value = new byte[length]; for (int i = 0; i < length; i++) value[i] = (byte) byteValue(); return value; }
        private short shortValue() { Assert.assertTrue(position + 2 <= limit); short v = Unsafe.getUnsafe().getShort(address + position); position += 2; return v; }
        private int intValue() { Assert.assertTrue(position + 4 <= limit); int v = Unsafe.getUnsafe().getInt(address + position); position += 4; return v; }
        private long longValue() { Assert.assertTrue(position + 8 <= limit); long v = Unsafe.getUnsafe().getLong(address + position); position += 8; return v; }
        private float floatValue() { return Float.intBitsToFloat(intValue()); }
        private double doubleValue() { return Double.longBitsToDouble(longValue()); }
        private int position() { return position; }
        private void skip(int length) { Assert.assertTrue(position + length <= limit); position += length; }
        private String string() { return new String(bytes(varint()), StandardCharsets.UTF_8); }
        private int varint() { int r=0,s=0,v; do { v=byteValue(); r|=(v&0x7f)<<s; s+=7; } while ((v&0x80)!=0); return r; }
    }
}
