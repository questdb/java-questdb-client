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

public class QwpSchemaBindingFloatingTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/floating-to-numeric.tsv";

    @Test
    public void testConformanceCorpusUsesExactTargetWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingFloatingTest.class.getResourceAsStream(CORPUS);
            Assert.assertNotNull(CORPUS, stream);
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty() || line.charAt(0) == '#') continue;
                    String[] f = line.split("\\t", -1);
                    Assert.assertEquals(line, 7, f.length);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer, -1, column("value", targetType(f[3])));
                        Runnable append = () -> append(binding, f[1], f[2]);
                        if ("<INVALID>".equals(f[5])) {
                            Assert.assertEquals(f[0], "<INVALID>", f[6]);
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE, append);
                        } else {
                            append.run();
                            buffer.nextRow();
                            Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), wireType(f[4]), 1, 1);
                            if ("<NULL>".equals(f[5])) {
                                Assert.assertEquals(f[0],
                                        "BYTE".equals(f[3]) || "SHORT".equals(f[3]) ? "0" : "NULL", f[6]);
                                Assert.assertEquals(1, reader.byteValue());
                                Assert.assertEquals(1, reader.byteValue());
                            } else {
                                Assert.assertEquals(0, reader.byteValue());
                                Assert.assertArrayEquals(f[0], hex(f[5]), reader.bytes(hex(f[5]).length));
                            }
                            Assert.assertEquals(f[0], encoder.getBuffer().getPosition(), reader.position());
                        }
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + f[0] + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertEquals(350, count);
        });
    }

    @Test
    public void testNaNIsMissingWithOtherNullAndPresentValues() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.DOUBLE));
                binding.floatColumn("value", Float.intBitsToFloat(0x7fc12345));
                buffer.nextRow();
                buffer.nextRow();
                binding.doubleColumn("value", -0.0d);
                buffer.nextRow();
                Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), QwpConstants.TYPE_DOUBLE, 3, 1);
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(3, reader.byteValue());
                Assert.assertEquals(0x8000000000000000L, reader.longValue());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
            }
        });
    }

    @Test
    public void testDuplicatePrecedesInvalidAndUnsupportedConversion() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("n", ColumnType.LONG), column("uuid", ColumnType.UUID));
                binding.floatColumn("n", 1).doubleColumn("n", Double.POSITIVE_INFINITY);
                binding.uuidColumn("uuid", 1, 2).floatColumn("uuid", Float.NaN);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
                reader.skip(QwpConstants.HEADER_SIZE);
                skipTablePrefix(reader, 1, 2);
                Assert.assertEquals("n", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_LONG, reader.byteValue());
                Assert.assertEquals("uuid", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_UUID, reader.byteValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(1, reader.longValue());
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(1, reader.longValue());
                Assert.assertEquals(2, reader.longValue());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testFailureRollbackRemovesFailedOnlyColumnAndPreservesRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("a", ColumnType.FLOAT), column("only_b", ColumnType.DOUBLE),
                        column("bad", ColumnType.BYTE), column("c", ColumnType.LONG));
                binding.doubleColumn("a", 1.5);
                buffer.nextRow();
                binding.floatColumn("only_b", 2);
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.doubleColumn("bad", 128));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.floatColumn("c", 3);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
                reader.skip(QwpConstants.HEADER_SIZE);
                skipTablePrefix(reader, 2, 2);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_FLOAT, reader.byteValue());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_LONG, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(2, reader.byteValue());
                Assert.assertEquals(Float.floatToRawIntBits(1.5f), reader.intValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(1, reader.byteValue());
                Assert.assertEquals(3, reader.longValue());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testResetClearAndStaleBinding() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.FLOAT));
                binding.doubleColumn("value", 9.25);
                buffer.nextRow();
                buffer.reset();
                binding.floatColumn("value", -0.0f);
                buffer.nextRow();
                Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), QwpConstants.TYPE_FLOAT, 1, 1);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(0x80000000, reader.intValue());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                buffer.clear();
                assertIllegalState(() -> binding.floatColumn("value", 1));
                assertIllegalState(() -> binding.doubleColumn("value", 1));
            }
        });
    }

    @Test
    public void testUnsupportedParameterizedUnknownAndDesignatedTargets() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, -1,
                    column("uuid", ColumnType.UUID), column("future", ColumnType.FLOAT, new byte[]{1}),
                    column("flagged", ColumnType.DOUBLE | 0x10000), column("bool", ColumnType.BOOLEAN),
                    column("timestamp", ColumnType.TIMESTAMP), column("binary", ColumnType.BINARY),
                    column("byte", ColumnType.BYTE), column("short", ColumnType.SHORT));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.floatColumn("uuid", Float.NaN));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.doubleColumn("future", Double.NaN));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.floatColumn("flagged", 1));
            rollback(buffer);
            binding.doubleColumn("missing", 1);
            Assert.assertEquals(QwpConstants.TYPE_DOUBLE,
                    buffer.getColumnDefs()[buffer.getColumnCount() - 1].getTypeCode());
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.floatColumn("bool", Float.NaN));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.doubleColumn("timestamp", Double.NaN));
            rollback(buffer);
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.floatColumn("binary", Float.NaN));
            rollback(buffer);
            assertInvalidContext(binding, buffer, "byte", true);
            assertInvalidContext(binding, buffer, "short", false);
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, 0, column("d", ColumnType.DOUBLE));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.floatColumn("d", Float.NaN));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, () -> binding.doubleColumn("d", Double.NaN));
        }
    }

    private static void append(QwpSchemaBinding binding, String inputType, String bits) {
        if ("FLOAT".equals(inputType)) {
            binding.floatColumn("value", Float.intBitsToFloat((int) Long.parseLong(bits, 16)));
        } else if ("DOUBLE".equals(inputType)) {
            binding.doubleColumn("value", Double.longBitsToDouble(Long.parseUnsignedLong(bits, 16)));
        } else {
            throw new AssertionError("unknown input type: " + inputType);
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
        } finally { Unsafe.free(address, frame.capacity(), MemoryTag.NATIVE_DEFAULT); }
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
        Assert.assertEquals("t", reader.string()); Assert.assertEquals(1, reader.byteValue());
        Assert.assertEquals(1, reader.intValue()); Assert.assertEquals(1, reader.longValue());
        Assert.assertEquals(rows, reader.varint()); Assert.assertEquals(columns, reader.varint());
    }

    private static int targetType(String value) {
        switch (value) {
            case "BYTE": return ColumnType.BYTE; case "SHORT": return ColumnType.SHORT;
            case "INT": return ColumnType.INT; case "LONG": return ColumnType.LONG;
            case "FLOAT": return ColumnType.FLOAT; case "DOUBLE": return ColumnType.DOUBLE;
            default: throw new AssertionError("unknown target type: " + value);
        }
    }

    private static byte wireType(String value) {
        switch (value) {
            case "BYTE": return QwpConstants.TYPE_BYTE; case "SHORT": return QwpConstants.TYPE_SHORT;
            case "INT": return QwpConstants.TYPE_INT; case "LONG": return QwpConstants.TYPE_LONG;
            case "FLOAT": return QwpConstants.TYPE_FLOAT; case "DOUBLE": return QwpConstants.TYPE_DOUBLE;
            default: throw new AssertionError("unknown wire type: " + value);
        }
    }

    private static byte[] hex(String value) {
        Assert.assertEquals(value, 0, value.length() & 1); byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) bytes[i] = (byte) Integer.parseInt(value.substring((bytes.length - 1 - i) * 2, (bytes.length - i) * 2), 16);
        return bytes;
    }

    private static void rollback(QwpTableBuffer buffer) { buffer.cancelCurrentRow(); buffer.rollbackUncommittedColumns(); }
    private static void assertInvalidContext(QwpSchemaBinding binding, QwpTableBuffer buffer, String column, boolean floatInput) {
        LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class,
                () -> { if (floatInput) binding.floatColumn(column, 128); else binding.doubleColumn(column, Double.POSITIVE_INFINITY); });
        Assert.assertEquals(LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
        Assert.assertFalse(error.isRetryable());
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("column=" + column));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("inputType=" + (floatInput ? "FLOAT" : "DOUBLE")));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("targetType=" + column.toUpperCase()));
        rollback(buffer);
    }
    private static void assertIllegalState(Runnable action) { Assert.assertThrows(IllegalStateException.class, action::run); }
    private static void assertReason(LineSenderSchemaException.Reason reason, Runnable action) {
        Assert.assertEquals(reason, Assert.assertThrows(LineSenderSchemaException.class, action::run).getReason());
    }

    private static final class Reader {
        private final long address; private final int limit; private int position;
        private Reader(long address, int limit) { this.address=address; this.limit=limit; }
        private int byteValue() { Assert.assertTrue(position<limit); return Unsafe.getUnsafe().getByte(address+position++)&0xff; }
        private byte[] bytes(int n) { Assert.assertTrue(position+n<=limit); byte[] b=new byte[n]; for(int i=0;i<n;i++)b[i]=(byte)byteValue(); return b; }
        private int intValue() { Assert.assertTrue(position+4<=limit); int v=Unsafe.getUnsafe().getInt(address+position); position+=4; return v; }
        private long longValue() { Assert.assertTrue(position+8<=limit); long v=Unsafe.getUnsafe().getLong(address+position); position+=8; return v; }
        private int position() { return position; }
        private void skip(int n) { Assert.assertTrue(position+n<=limit); position+=n; }
        private String string() { return new String(bytes(varint()), StandardCharsets.UTF_8); }
        private int varint() { int r=0,s=0,v; do {v=byteValue();r|=(v&0x7f)<<s;s+=7;}while((v&0x80)!=0);return r; }
    }
}
