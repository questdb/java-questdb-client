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
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.bytes.DirectByteSlice;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.binding;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.column;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.skipTablePrefix;
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
                        QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1, 1);
                        if ("<NULL>".equals(fields[4])) {
                            Assert.assertEquals(fields[0], 1, reader.u8());
                            Assert.assertEquals(fields[0], 1, reader.u8());
                            Assert.assertEquals(fields[0], 0, reader.i32());
                        } else {
                            byte[] expected = bytes(fields[4]);
                            if ("binary_all_256".equals(fields[0])) {
                                assertAllByteValues(bytes(fields[2]));
                                assertAllByteValues(expected);
                                assertAllByteValues(bytes(fields[5]));
                            }
                            Assert.assertEquals(fields[0], 0, reader.u8());
                            Assert.assertEquals(fields[0], 0, reader.i32());
                            Assert.assertEquals(fields[0], expected.length, reader.i32());
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

                QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 4, 1);
                Assert.assertEquals(0, reader.u8());
                for (int offset : new int[]{0, 2, 4, 6, 6}) {
                    Assert.assertEquals(offset, reader.i32());
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
                QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1, 1);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0, reader.i32());
                Assert.assertEquals(1, reader.i32());
                Assert.assertEquals(42, reader.u8());
                Assert.assertEquals(encoder.getBuffer().getPosition(), reader.position());
            }
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder(); QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, -1, column("value", ColumnType.BINARY));
                binding.stringColumn("value", "x").binaryColumn("value", (byte[]) null);
                buffer.nextRow();
                QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1, 1);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0, reader.i32());
                Assert.assertEquals(1, reader.i32());
                Assert.assertEquals('x', reader.u8());
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
                QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 130, 1);
                Assert.assertEquals(1, reader.u8());
                for (int i = 0; i < 16; i++) {
                    Assert.assertEquals(0xaa, reader.u8());
                }
                Assert.assertEquals(0x02, reader.u8());
                for (int i = 0; i <= 65; i++) {
                    Assert.assertEquals(i, reader.i32());
                }
                for (int row = 0; row < 130; row += 2) {
                    Assert.assertEquals(row & 0xff, reader.u8());
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
                QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
                reader.skip(QwpConstants.HEADER_SIZE);
                skipTablePrefix(reader, 2, 3);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_BINARY, reader.u8());
                Assert.assertEquals("existing_b", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_BINARY, reader.u8());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_BINARY, reader.u8());
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
                    QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 2, 1, QwpConstants.TYPE_VARCHAR);
                    Assert.assertEquals(0, reader.u8());
                    for (int offset : new int[]{0, 1, 2}) {
                        Assert.assertEquals(offset, reader.i32());
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
                QwpTestWireReader reader = tableReader(encoder, encoder.encodeSchema(buffer), 1, 1);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(0, reader.i32());
                Assert.assertEquals(1, reader.i32());
                Assert.assertEquals(8, reader.u8());
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

    private static void assertSparseBinary(QwpTestWireReader reader, int bitmap, int value) {
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(bitmap, reader.u8());
        Assert.assertEquals(0, reader.i32());
        Assert.assertEquals(1, reader.i32());
        Assert.assertEquals(value, reader.u8());
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

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, int rows, int columns) {
        return tableReader(encoder, size, rows, columns, QwpConstants.TYPE_BINARY);
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, int rows, int columns, int wireType) {
        QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        skipTablePrefix(reader, rows, columns);
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(wireType, reader.u8());
        return reader;
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
