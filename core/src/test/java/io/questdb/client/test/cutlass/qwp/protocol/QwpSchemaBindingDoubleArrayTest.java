/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 ******************************************************************************/
package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.array.DoubleArray;
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwpSchemaBindingDoubleArrayTest {

    @Test
    public void testAllPublicRepresentationsUseExactTargetWire() throws Exception {
        assertMemoryLeak(() -> {
            int[] shape32 = new int[ColumnType.ARRAY_NDIMS_LIMIT];
            java.util.Arrays.fill(shape32, 1);
            double payloadNaN = Double.longBitsToDouble(0x7ff8000000000042L);
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t");
                 DoubleArray array = new DoubleArray(shape32)) {
                array.append(10.5);
                QwpSchemaBinding binding = binding(buffer,
                        column("a1", arrayType(1)),
                        column("a2", arrayType(2)),
                        column("a3", arrayType(3)),
                        column("a32", arrayType(ColumnType.ARRAY_NDIMS_LIMIT)));
                binding.doubleArray("a1", new double[]{payloadNaN, -0.0, Double.POSITIVE_INFINITY});
                binding.doubleArray("a2", new double[][]{{3.0, 4.0}, {5.0, 6.0}});
                binding.doubleArray("a3", new double[][][]{{{7.0}, {8.0}}});
                binding.doubleArray("a32", array);
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                Reader reader = tableHeader(encoder, size, 1, 4);
                columnDef(reader, "a1");
                columnDef(reader, "a2");
                columnDef(reader, "a3");
                columnDef(reader, "a32");
                array(reader, new int[]{3}, payloadNaN, -0.0, Double.POSITIVE_INFINITY);
                array(reader, new int[]{2, 2}, 3.0, 4.0, 5.0, 6.0);
                array(reader, new int[]{1, 2, 1}, 7.0, 8.0);
                array(reader, shape32, 10.5);
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testNullRepresentationsAreNoopsAndWrapperRankTracksReshape() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t");
                 DoubleArray array = new DoubleArray(1, 1)) {
                QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
                binding.doubleArray("a", (double[]) null)
                        .doubleArray("b", (double[][]) null)
                        .doubleArray("c", (double[][][]) null)
                        .doubleArray("d", (DoubleArray) null);
                Assert.assertEquals(0, buffer.getColumnDefs().length);

                Assert.assertEquals(2, array.getDimensionality());
                array.reshape(1);
                Assert.assertEquals(1, array.getDimensionality());
                array.append(42.0);
                binding.doubleArray("value", array);
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                Reader reader = tableHeader(encoder, size, 1, 1, -1, -1);
                columnDef(reader, "value");
                Assert.assertEquals(0, reader.u8());
                rawArray(reader, 1, new int[]{1}, new double[]{42.0});
                Assert.assertEquals(size, reader.position());
            }
        });

        try (DoubleArray array = new DoubleArray(1)) {
            array.close();
            Assert.assertThrows(LineSenderException.class, array::getDimensionality);
        }
    }

    @Test
    public void testKnownRankDuplicateRollbackAndShapeValidation() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t");
                 DoubleArray closedDuplicate = new DoubleArray(1)) {
                QwpSchemaBinding binding = binding(buffer,
                        column("v", arrayType(1)), column("m", arrayType(2)));

                binding.doubleArray("v", new double[]{1.0});
                binding.doubleArray("v", new double[][]{{99.0}}); // duplicate: first value wins
                closedDuplicate.close();
                binding.doubleArray("v", closedDuplicate); // duplicate: do not inspect an ignored value
                buffer.nextRow();

                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.doubleArray("v", new double[][]{{2.0}}),
                        "sourceDims=2", "targetDims=1");
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();

                Assert.assertThrows(LineSenderException.class,
                        () -> binding.doubleArray("m", new double[][]{{2.0}, {3.0, 4.0}}));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();

                binding.doubleArray("v", new double[0]);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                Reader reader = tableHeader(encoder, size, 2, 1);
                columnDef(reader, "v");
                Assert.assertEquals(0, reader.u8());
                rawArray(reader, 1, new int[]{1}, new double[]{1.0});
                rawArray(reader, 1, new int[]{0}, new double[0]);
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    @Test
    public void testMissingSchemaPinsFirstEffectiveRank() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
                binding.doubleArray("v", new double[][]{{1.0, 2.0}});
                buffer.nextRow();

                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.doubleArray("v", new double[]{3.0}),
                        "sourceDims=1", "inferredDims=2");
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();

                binding.doubleArray("v", new double[][]{{4.0}, {5.0}});
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                Reader reader = tableHeader(encoder, size, 2, 1, -1, -1);
                columnDef(reader, "v");
                Assert.assertEquals(0, reader.u8());
                rawArray(reader, 2, new int[]{1, 2}, new double[]{1.0, 2.0});
                rawArray(reader, 2, new int[]{2, 1}, new double[]{4.0, 5.0});
                Assert.assertEquals(size, reader.position());

                buffer.reset();
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.doubleArray("v", new double[]{6.0}),
                        "sourceDims=1", "inferredDims=2");
            }
        });
    }

    @Test
    public void testRejectsWrongElementTypeWeakRankParametersAndDesignatedColumn() {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, column("v", ColumnType.encodeArrayType(ColumnType.LONG, 1)));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.doubleArray("v", new double[]{1.0}), "targetType=LONG[]");
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            int weak = arrayType(1) | (1 << 19);
            QwpSchemaBinding binding = binding(buffer, column("v", weak));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.doubleArray("v", new double[]{1.0}), "conversion is not implemented");
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, -1, column("v", arrayType(1), new byte[]{1}));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.doubleArray("v", new double[]{1.0}), "parameterized target type");
        }
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer, 0, column("v", arrayType(1)));
            assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                    () -> binding.doubleArray("v", new double[]{1.0}), "designated timestamp writes");
        }
    }

    private static void array(Reader reader, int[] shape, double... values) {
        Assert.assertEquals(0, reader.u8());
        rawArray(reader, shape.length, shape, values);
    }

    private static int arrayType(int dimensions) {
        return ColumnType.encodeArrayType(ColumnType.DOUBLE, dimensions);
    }

    private static QwpSchemaBinding binding(QwpTableBuffer buffer, byte[]... columns) {
        return binding(buffer, -1, columns);
    }

    private static QwpSchemaBinding binding(QwpTableBuffer buffer, int designated, byte[]... columns) {
        return new QwpSchemaBinding(buffer, known(designated, columns));
    }

    private static byte[] column(String name, int type) {
        return column(name, type, new byte[0]);
    }

    private static byte[] column(String name, int type, byte[] params) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + bytes.length + 6 + params.length).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) bytes.length).put(bytes).putInt(type)
                .putShort((short) params.length).put(params).array();
    }

    private static void columnDef(Reader reader, String name) {
        Assert.assertEquals(name, reader.string());
        Assert.assertEquals(QwpConstants.TYPE_DOUBLE_ARRAY, reader.u8());
    }

    private static QwpSchemaResponse decode(byte[] payload) {
        ByteBuffer frame = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payload.length).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(QwpConstants.MAGIC_MESSAGE).put((byte) 1).put(QwpSchemaProtocol.FLAG_CONTROL)
                .putShort((short) 0).putInt(payload.length).put(payload);
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

    private static void rawArray(Reader reader, int expectedDims, int[] shape, double[] values) {
        Assert.assertEquals(expectedDims, reader.u8());
        for (int dimension : shape) {
            Assert.assertEquals(dimension, reader.i32());
        }
        for (double value : values) {
            Assert.assertEquals(Double.doubleToRawLongBits(value), reader.i64());
        }
    }

    private static void assertReason(
            LineSenderSchemaException.Reason reason,
            Runnable action,
            String... messageParts
    ) {
        LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class, action::run);
        Assert.assertEquals(reason, error.getReason());
        for (String part : messageParts) {
            Assert.assertTrue(error.getMessage(), error.getMessage().contains(part));
        }
    }

    private static Reader tableHeader(QwpWebSocketEncoder encoder, int size, int rows, int columns) {
        return tableHeader(encoder, size, rows, columns, 1, 1);
    }

    private static Reader tableHeader(
            QwpWebSocketEncoder encoder,
            int size,
            int rows,
            int columns,
            int tableId,
            long version
    ) {
        Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        Assert.assertEquals("t", reader.string());
        if (tableId < 0) {
            Assert.assertEquals(0, reader.u8());
        } else {
            Assert.assertEquals(1, reader.u8());
            Assert.assertEquals(tableId, reader.i32());
            Assert.assertEquals(version, reader.i64());
        }
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(columns, reader.varint());
        return reader;
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
            Assert.assertTrue(position <= limit - Integer.BYTES);
            int value = Unsafe.getUnsafe().getInt(address + position);
            position += Integer.BYTES;
            return value;
        }

        private long i64() {
            Assert.assertTrue(position <= limit - Long.BYTES);
            long value = Unsafe.getUnsafe().getLong(address + position);
            position += Long.BYTES;
            return value;
        }

        private int position() {
            return position;
        }

        private void skip(int length) {
            position += length;
            Assert.assertTrue(position <= limit);
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

        private int varint() {
            int result = 0;
            int shift = 0;
            for (; shift < 32; shift += 7) {
                int b = u8();
                result |= (b & 0x7f) << shift;
                if ((b & 0x80) == 0) {
                    return result;
                }
            }
            throw new AssertionError("unterminated varint");
        }
    }
}
