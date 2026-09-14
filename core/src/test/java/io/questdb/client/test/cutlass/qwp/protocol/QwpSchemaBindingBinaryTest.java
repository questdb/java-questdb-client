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
import io.questdb.client.std.bytes.DirectByteSlice;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpSchemaBindingBinaryTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/binary-input.tsv";

    @Test
    public void testConformanceCorpusUsesExactBinaryWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingBinaryTest.class.getResourceAsStream(CORPUS);
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
                    Assert.assertEquals(line, "BINARY", fields[3]);
                    Assert.assertEquals(line, "VALID", fields[6]);
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.BINARY));
                        if ("BINARY".equals(fields[1])) {
                            binding.binaryColumn("value", bytes(fields[2]));
                        } else if ("STRING".equals(fields[1])) {
                            binding.stringColumn("value", "<NULL>".equals(fields[2]) ? null : utf16(fields[2]));
                        } else {
                            throw new AssertionError(fields[1]);
                        }
                        buffer.nextRow();
                        Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1, 1);
                        if ("<NULL>".equals(fields[4])) {
                            Assert.assertEquals(fields[0], 1, reader.byteValue());
                            Assert.assertEquals(fields[0], 1, reader.byteValue());
                            Assert.assertEquals(fields[0], 0, reader.intValue());
                        } else {
                            byte[] expected = bytes(fields[4]);
                            if ("binary_all_256".equals(fields[0])) {
                                assertAllByteValues(bytes(fields[2]));
                                assertAllByteValues(expected);
                                assertAllByteValues(bytes(fields[5]));
                            }
                            Assert.assertEquals(fields[0], 0, reader.byteValue());
                            Assert.assertEquals(fields[0], 0, reader.intValue());
                            Assert.assertEquals(fields[0], expected.length, reader.intValue());
                            Assert.assertArrayEquals(fields[0], expected, reader.bytes(expected.length));
                        }
                        Assert.assertEquals(fields[0], encoder.getBuffer().getPosition(), reader.position());
                    }
                    count++;
                }
            }
            Assert.assertEquals(18, count);
        });
    }

    @Test
    public void testAllBinaryOverloadsCopyBeforeReturning() throws Exception {
        assertMemoryLeak(() -> {
            long address = Unsafe.malloc(6, MemoryTag.NATIVE_DEFAULT);
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.BINARY));
                byte[] array = {1, 2};
                binding.binaryColumn("value", array);
                array[0] = 9;
                buffer.nextRow();

                put(address, new byte[]{3, 4});
                DirectByteSlice slice = new DirectByteSlice().of(address, 2);
                binding.binaryColumn("value", slice);
                put(address, new byte[]{8, 8});
                slice.of(0, 0);
                buffer.nextRow();

                put(address, new byte[]{5, 6});
                binding.binaryColumn("value", address, 2);
                put(address, new byte[]{7, 7});
                buffer.nextRow();
                Unsafe.free(address, 6, MemoryTag.NATIVE_DEFAULT);
                address = 0;
                binding.binaryColumn("value", 0, 0);
                buffer.nextRow();

                Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), 4, 1);
                Assert.assertEquals(0, reader.byteValue());
                for (int offset : new int[]{0, 2, 4, 6, 6}) {
                    Assert.assertEquals(offset, reader.intValue());
                }
                Assert.assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6}, reader.bytes(6));
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
            } finally {
                if (address != 0) {
                    Unsafe.free(address, 6, MemoryTag.NATIVE_DEFAULT);
                }
            }
        });
    }

    @Test
    public void testDuplicatePrecedesBinaryArgumentValidation() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.BINARY));
                binding.binaryColumn("value", new byte[]{42})
                        .binaryColumn("value", (byte[]) null)
                        .binaryColumn("value", (DirectByteSlice) null)
                        .binaryColumn("value", 0, 1)
                        .binaryColumn("value", 1, (long) Integer.MAX_VALUE + 1)
                        .stringColumn("value", new ThrowingCharSequence(new TestCharSequenceException()));
                buffer.nextRow();
                Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1, 1);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(0, reader.intValue());
                Assert.assertEquals(1, reader.intValue());
                Assert.assertEquals(42, reader.byteValue());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
            }
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.BINARY));
                binding.stringColumn("value", "x").binaryColumn("value", (byte[]) null);
                buffer.nextRow();
                Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1, 1);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(0, reader.intValue());
                Assert.assertEquals(1, reader.intValue());
                Assert.assertEquals('x', reader.byteValue());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
            }
        });
    }

    @Test
    public void testInvalidBinaryArgumentsAreTypedAndCheckedBeforeMemoryAccess() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.BINARY));
            assertInvalid("binary value", () -> binding.binaryColumn("value", (byte[]) null));
            rollback(buffer);
            assertInvalid("binary slice", () -> binding.binaryColumn("value", (DirectByteSlice) null));
            rollback(buffer);
            assertInvalid("non-negative", () -> binding.binaryColumn("value", new DirectByteSlice().of(1, -1)));
            rollback(buffer);
            assertInvalid("pointer", () -> binding.binaryColumn("value", new DirectByteSlice().of(0, 1)));
            rollback(buffer);
            assertInvalid("non-negative", () -> binding.binaryColumn("value", 1, -1));
            rollback(buffer);
            assertInvalid("pointer", () -> binding.binaryColumn("value", 0, 1));
            rollback(buffer);
            assertInvalid("maximum", () -> binding.binaryColumn("value", 1, (long) Integer.MAX_VALUE + 1));
        }
    }

    @Test
    public void testBitmapCrossesSixtyFourRowsWithPresentAndOmittedValues() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.BINARY));
                for (int row = 0; row < 130; row++) {
                    if ((row & 1) == 0) {
                        binding.binaryColumn("value", new byte[]{(byte) row});
                    }
                    buffer.nextRow();
                }
                Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), 130, 1);
                Assert.assertEquals(1, reader.byteValue());
                for (int i = 0; i < 16; i++) {
                    Assert.assertEquals(0xaa, reader.byteValue());
                }
                Assert.assertEquals(0x02, reader.byteValue());
                for (int i = 0; i <= 65; i++) {
                    Assert.assertEquals(i, reader.intValue());
                }
                for (int row = 0; row < 130; row += 2) {
                    Assert.assertEquals(row & 0xff, reader.byteValue());
                }
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
            }
        });
    }

    @Test
    public void testFailureRollbackRemovesFailedOnlyColumnAndPreservesRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1,
                        column("a", ColumnType.BINARY), column("existing_b", ColumnType.BINARY),
                        column("only_b", ColumnType.BINARY), column("c", ColumnType.BINARY));
                binding.binaryColumn("a", new byte[]{1});
                binding.binaryColumn("existing_b", new byte[]{2});
                buffer.nextRow();
                TestCharSequenceException existingFailure = new TestCharSequenceException();
                Assert.assertSame(existingFailure, Assert.assertThrows(TestCharSequenceException.class,
                        () -> binding.stringColumn("existing_b", new ThrowingCharSequence(existingFailure))));
                rollback(buffer);
                TestCharSequenceException newColumnFailure = new TestCharSequenceException();
                Assert.assertSame(newColumnFailure, Assert.assertThrows(TestCharSequenceException.class,
                        () -> binding.stringColumn("only_b", new ThrowingCharSequence(newColumnFailure))));
                rollback(buffer);
                binding.binaryColumn("c", new byte[]{3});
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
                reader.skip(QwpConstants.HEADER_SIZE);
                skipTablePrefix(reader, 2, 3);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_BINARY, reader.byteValue());
                Assert.assertEquals("existing_b", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_BINARY, reader.byteValue());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_BINARY, reader.byteValue());
                assertSparseBinary(reader, 2, 1);
                assertSparseBinary(reader, 2, 2);
                assertSparseBinary(reader, 1, 3);
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testStringAppendIsExceptionAtomicForVarcharRuntimeAndError() throws Exception {
        assertMemoryLeak(() -> {
            for (Throwable failure : new Throwable[]{new TestCharSequenceException(), new TestCharSequenceError()}) {
                try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.VARCHAR));
                    binding.stringColumn("value", "a");
                    buffer.nextRow();
                    ThrowingCharSequence input = failure instanceof Error
                            ? new ThrowingCharSequence(failure, 4096, 300)
                            : new ThrowingCharSequence(failure);
                    Throwable actual = Assert.assertThrows(failure.getClass(),
                            () -> binding.stringColumn("value", input));
                    Assert.assertSame(failure, actual);
                    rollback(buffer);
                    binding.stringColumn("value", "c");
                    buffer.nextRow();
                    Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), 2, 1, QwpConstants.TYPE_VARCHAR);
                    Assert.assertEquals(0, reader.byteValue());
                    for (int offset : new int[]{0, 1, 2}) {
                        Assert.assertEquals(offset, reader.intValue());
                    }
                    Assert.assertArrayEquals(new byte[]{'a', 'c'}, reader.bytes(2));
                    Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                }
            }
        });
    }

    @Test
    public void testResetClearAndSchemaGuards() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.BINARY));
                binding.binaryColumn("value", new byte[]{7});
                buffer.nextRow();
                buffer.reset();
                binding.binaryColumn("value", new byte[]{8});
                buffer.nextRow();
                Reader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1, 1);
                Assert.assertEquals(0, reader.byteValue());
                Assert.assertEquals(0, reader.intValue());
                Assert.assertEquals(1, reader.intValue());
                Assert.assertEquals(8, reader.byteValue());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
                buffer.clear();
                Assert.assertThrows(IllegalStateException.class, () -> binding.binaryColumn("value", new byte[]{1}));
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                assertUnsupported(binding(buffer, -1, column("value", ColumnType.STRING)));
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                assertUnsupported(binding(buffer, -1, column("value", ColumnType.INT)));
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                assertUnsupported(binding(buffer, -1, column("value", ColumnType.BINARY, new byte[]{1})));
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                assertUnsupported(binding(buffer, 0, column("value", ColumnType.BINARY)));
            }
        });
    }

    private static void assertSparseBinary(Reader reader, int bitmap, int value) {
        Assert.assertEquals(1, reader.byteValue());
        Assert.assertEquals(bitmap, reader.byteValue());
        Assert.assertEquals(0, reader.intValue());
        Assert.assertEquals(1, reader.intValue());
        Assert.assertEquals(value, reader.byteValue());
    }

    private static void assertAllByteValues(byte[] bytes) {
        Assert.assertEquals(256, bytes.length);
        for (int i = 0; i < bytes.length; i++) {
            Assert.assertEquals(i, bytes[i] & 0xff);
        }
    }

    private static void assertUnsupported(QwpSchemaBinding binding) {
        LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class,
                () -> binding.binaryColumn("value", new byte[]{1}));
        Assert.assertEquals(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE, error.getReason());
    }

    private static void assertInvalid(String detail, Runnable runnable) {
        LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class, runnable::run);
        Assert.assertEquals(error.getMessage(), LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
        Assert.assertFalse(error.getMessage(), error.isRetryable());
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("table=t"));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("column="));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("inputType=BINARY"));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains("targetType=BINARY"));
        Assert.assertTrue(error.getMessage(), error.getMessage().contains(detail));
    }

    private static void rollback(QwpTableBuffer buffer) {
        buffer.cancelCurrentRow();
        buffer.rollbackUncommittedColumns();
    }

    private static void put(long address, byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            Unsafe.getUnsafe().putByte(address + i, bytes[i]);
        }
    }

    private static byte[] bytes(String hex) {
        Assert.assertEquals(hex, 0, hex.length() & 1);
        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return result;
    }

    private static String utf16(String hex) {
        Assert.assertEquals(hex, 0, hex.length() & 3);
        char[] chars = new char[hex.length() / 4];
        for (int i = 0; i < chars.length; i++) {
            chars[i] = (char) Integer.parseInt(hex.substring(i * 4, i * 4 + 4), 16);
        }
        return new String(chars);
    }

    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, int rows, int columns) {
        return tableReader(encoder, size, rows, columns, QwpConstants.TYPE_BINARY);
    }

    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, int rows, int columns, int wireType) {
        Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        skipTablePrefix(reader, rows, columns);
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(wireType, reader.byteValue());
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
            put(address, frame.array());
            return QwpSchemaProtocol.decodeResponse(address, frame.capacity());
        } finally {
            Unsafe.free(address, frame.capacity(), MemoryTag.NATIVE_DEFAULT);
        }
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

    private static final class ThrowingCharSequence implements CharSequence {
        private final Throwable failure;
        private final int failureIndex;
        private final int length;

        private ThrowingCharSequence(Throwable failure) {
            this(failure, 2, 1);
        }

        private ThrowingCharSequence(Throwable failure, int length, int failureIndex) {
            this.failure = failure;
            this.length = length;
            this.failureIndex = failureIndex;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public char charAt(int index) {
            if (index < failureIndex) {
                return 'x';
            }
            if (failure instanceof Error) {
                throw (Error) failure;
            }
            throw (RuntimeException) failure;
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class TestCharSequenceException extends RuntimeException {
    }

    private static final class TestCharSequenceError extends Error {
    }
}
