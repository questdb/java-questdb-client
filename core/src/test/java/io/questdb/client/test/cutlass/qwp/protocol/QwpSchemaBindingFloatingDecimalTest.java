/*+*****************************************************************************
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 * Licensed under the Apache License, Version 2.0
 *******************************************************************************/
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

public class QwpSchemaBindingFloatingDecimalTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/floating-to-decimal.tsv";
    private static final String HEADER = "# case_id\tsource_type\tinput_bits_hex\ttarget_type\t"
            + "target_precision\ttarget_scale\toutcome\texpected_ll_hex\texpected_lh_hex\t"
            + "expected_hl_hex\texpected_hh_hex\texpected_sql";

    @Test
    public void testCorpusUsesExactTargetDecimalWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingFloatingDecimalTest.class.getResourceAsStream(CORPUS);
            Assert.assertNotNull(CORPUS, stream);
            boolean[][] sourceTargets = new boolean[2][6];
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                Assert.assertEquals(HEADER, lines.readLine());
                String line;
                while ((line = lines.readLine()) != null) {
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 12, fields.length);
                    Vector vector = new Vector(fields);
                    sourceTargets[vector.sourceIndex()][targetIndex(vector.targetType)] = true;
                    assertVector(vector);
                    count++;
                }
            }
            Assert.assertEquals(60, count);
            for (int source = 0; source < sourceTargets.length; source++) {
                for (int target = 0; target < sourceTargets[source].length; target++) {
                    Assert.assertTrue("missing source/target pair " + source + '/' + target,
                            sourceTargets[source][target]);
                }
            }
        });
    }

    @Test
    public void testDuplicateNullRollbackAndOmissionLifecycle() throws Exception {
        assertMemoryLeak(() -> {
            int target = ColumnType.getDecimalType(3, 1);
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer,
                        column("value", target), column("only_b", target));

                // Duplicate suppression precedes both an invalid conversion and null normalization.
                binding.floatColumn("value", 1.0f)
                        .doubleColumn("value", 1.25)
                        .doubleColumn("value", Double.NEGATIVE_INFINITY);
                buffer.nextRow();

                // A null first value also wins over a later invalid duplicate.
                binding.doubleColumn("value", Double.longBitsToDouble(0x7ff8000000000042L))
                        .floatColumn("value", 1.25f);
                buffer.nextRow();

                // Row B must disappear without disturbing completed A or following C.
                binding.doubleColumn("only_b", 2.0);
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.doubleColumn("value", 1.25));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();

                binding.floatColumn("value", -2.5f);
                buffer.nextRow();
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                Reader reader = tableReader(encoder, size, QwpConstants.TYPE_DECIMAL64, 4);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(0x0a, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(10, reader.i64());
                Assert.assertEquals(-25, reader.i64());
                Assert.assertEquals(size, reader.position());

                buffer.reset();
                buffer.nextRow();
                int resetSize = encoder.encodeSchema(buffer);
                Reader reset = tableReader(encoder, resetSize, QwpConstants.TYPE_DECIMAL64, 1);
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(resetSize, reset.position());
            }
        });
    }

    private static void assertInvalid(Vector vector) throws Exception {
        try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
            QwpSchemaBinding binding = binding(buffer,
                    column("value", ColumnType.getDecimalType(vector.targetPrecision, vector.targetScale)));
            LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class,
                    () -> vector.append(binding, "value"));
            Assert.assertEquals(vector.caseId, LineSenderSchemaException.Reason.INVALID_VALUE, error.getReason());
            Assert.assertFalse(vector.caseId, error.isRetryable());
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("column=value"));
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("inputType=" + vector.sourceType));
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("targetType=DECIMAL"));
            buffer.cancelCurrentRow();
            buffer.rollbackUncommittedColumns();
            Assert.assertEquals(vector.caseId, 0, buffer.getColumnCount());
        }
    }

    private static void assertReason(LineSenderSchemaException.Reason reason, Runnable action) {
        LineSenderSchemaException error = Assert.assertThrows(LineSenderSchemaException.class, action::run);
        Assert.assertEquals(error.getMessage(), reason, error.getReason());
        Assert.assertFalse(error.getMessage(), error.isRetryable());
    }

    private static void assertVector(Vector vector) throws Exception {
        try {
            if (vector.invalid()) {
                assertInvalid(vector);
                return;
            }
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer,
                        column("value", ColumnType.getDecimalType(vector.targetPrecision, vector.targetScale)));
                vector.append(binding, "value");
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                Assert.assertEquals(QwpConstants.FLAG_SCHEMA,
                        Unsafe.getUnsafe().getByte(encoder.getBuffer().getBufferPtr()
                                + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
                Reader reader = tableReader(encoder, size, wireType(vector.targetType), 1);
                if (vector.isNull()) {
                    Assert.assertEquals(vector.caseId, 1, reader.u8());
                    Assert.assertEquals(vector.caseId, 1, reader.u8());
                    Assert.assertEquals(vector.caseId, vector.targetScale, reader.u8());
                } else {
                    Assert.assertEquals(vector.caseId, 0, reader.u8());
                    Assert.assertEquals(vector.caseId, vector.targetScale, reader.u8());
                    for (int i = 0, n = wireLongCount(vector.targetType); i < n; i++) {
                        Assert.assertEquals(vector.caseId, vector.expectedLimbs[i], reader.i64());
                    }
                }
                Assert.assertEquals(vector.caseId, size, reader.position());
            }
        } catch (AssertionError e) {
            throw new AssertionError("case_id=" + vector.caseId + ": " + e.getMessage(), e);
        }
    }

    private static QwpSchemaBinding binding(QwpTableBuffer buffer, byte[]... columns) {
        return new QwpSchemaBinding(buffer, known(columns));
    }

    private static byte[] column(String name, int type) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + bytes.length + 6).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) bytes.length).put(bytes).putInt(type).putShort((short) 0).array();
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

    private static QwpSchemaResponse known(byte[]... columns) {
        int length = 26;
        for (byte[] column : columns) {
            length += column.length;
        }
        ByteBuffer payload = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN)
                .put(QwpSchemaProtocol.KIND_SCHEMA).putLong(1).put((byte) QwpSchemaProtocol.RESULT_KNOWN)
                .putInt(1).putLong(1).putShort((short) -1).putShort((short) columns.length);
        for (byte[] column : columns) {
            payload.put(column);
        }
        return decode(payload.array());
    }

    private static long parseHex(String value) {
        Assert.assertNotEquals("-", value);
        return Long.parseUnsignedLong(value, 16);
    }

    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, byte wireType, int rows) {
        Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        Assert.assertEquals("t", reader.string());
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(1, reader.i32());
        Assert.assertEquals(1, reader.i64());
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(1, reader.varint());
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(wireType, reader.u8());
        return reader;
    }

    private static int targetIndex(String target) {
        switch (target) {
            case "DECIMAL8":
                return 0;
            case "DECIMAL16":
                return 1;
            case "DECIMAL32":
                return 2;
            case "DECIMAL64":
                return 3;
            case "DECIMAL128":
                return 4;
            default:
                return 5;
        }
    }

    private static byte wireType(String target) {
        if ("DECIMAL128".equals(target)) {
            return QwpConstants.TYPE_DECIMAL128;
        }
        if ("DECIMAL256".equals(target)) {
            return QwpConstants.TYPE_DECIMAL256;
        }
        return QwpConstants.TYPE_DECIMAL64;
    }

    private static int wireLongCount(String target) {
        if ("DECIMAL128".equals(target)) {
            return 2;
        }
        if ("DECIMAL256".equals(target)) {
            return 4;
        }
        return 1;
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
            int value;
            do {
                value = u8();
                result |= (value & 0x7f) << shift;
                shift += 7;
            } while ((value & 0x80) != 0);
            return result;
        }
    }

    private static final class Vector {
        private final String caseId;
        private final long[] expectedLimbs = new long[4];
        private final long inputBits;
        private final String outcome;
        private final String sourceType;
        private final int targetPrecision;
        private final int targetScale;
        private final String targetType;

        private Vector(String[] fields) {
            caseId = fields[0];
            sourceType = fields[1];
            Assert.assertTrue(sourceType.equals("FLOAT") || sourceType.equals("DOUBLE"));
            inputBits = Long.parseUnsignedLong(fields[2], 16);
            Assert.assertEquals(sourceType.equals("FLOAT") ? 8 : 16, fields[2].length());
            targetType = fields[3];
            targetPrecision = Integer.parseInt(fields[4]);
            targetScale = Integer.parseInt(fields[5]);
            outcome = fields[6];
            Assert.assertTrue(outcome.equals("VALUE") || outcome.equals("INVALID") || outcome.equals("NULL"));
            Assert.assertEquals(expectedTargetType(targetPrecision), targetType);
            int limbs = outcome.equals("VALUE") ? wireLongCount(targetType) : 0;
            for (int i = 0; i < limbs; i++) {
                expectedLimbs[i] = parseHex(fields[7 + i]);
            }
            for (int i = limbs; i < 4; i++) {
                Assert.assertEquals("-", fields[7 + i]);
            }
            if (outcome.equals("NULL")) {
                Assert.assertEquals("NULL", fields[11]);
            } else if (outcome.equals("INVALID")) {
                Assert.assertEquals("-", fields[11]);
            } else {
                Assert.assertFalse(fields[11].isEmpty());
                Assert.assertNotEquals("-", fields[11]);
            }
        }

        private void append(QwpSchemaBinding binding, String column) {
            if ("FLOAT".equals(sourceType)) {
                binding.floatColumn(column, Float.intBitsToFloat((int) inputBits));
            } else {
                binding.doubleColumn(column, Double.longBitsToDouble(inputBits));
            }
        }

        private boolean invalid() {
            return outcome.equals("INVALID");
        }

        private boolean isNull() {
            return outcome.equals("NULL");
        }

        private int sourceIndex() {
            return "FLOAT".equals(sourceType) ? 0 : 1;
        }

        private static String expectedTargetType(int precision) {
            if (precision <= 2) {
                return "DECIMAL8";
            }
            if (precision <= 4) {
                return "DECIMAL16";
            }
            if (precision <= 9) {
                return "DECIMAL32";
            }
            if (precision <= 18) {
                return "DECIMAL64";
            }
            if (precision <= 38) {
                return "DECIMAL128";
            }
            return "DECIMAL256";
        }
    }
}
