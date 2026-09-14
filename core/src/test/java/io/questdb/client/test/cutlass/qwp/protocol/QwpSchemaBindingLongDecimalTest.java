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

public class QwpSchemaBindingLongDecimalTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/long-to-decimal.tsv";
    private static final String HEADER = "# case_id\tinput\ttarget_type\ttarget_precision\ttarget_scale\toutcome\texpected_ll_hex\texpected_lh_hex\texpected_hl_hex\texpected_hh_hex\texpected_sql";

    @Test
    public void testCorpusUsesExactTargetDecimalWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = QwpSchemaBindingLongDecimalTest.class.getResourceAsStream(CORPUS);
            Assert.assertNotNull(CORPUS, stream);
            boolean[] targets = new boolean[6];
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                Assert.assertEquals(HEADER, lines.readLine());
                String line;
                while ((line = lines.readLine()) != null) {
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 11, fields.length);
                    Vector vector = new Vector(fields);
                    targets[targetIndex(vector.targetType)] = true;
                    try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                         QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                        QwpSchemaBinding binding = binding(buffer,
                                column("value", ColumnType.getDecimalType(vector.targetPrecision, vector.targetScale)));
                        if (vector.invalid()) {
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                                    () -> binding.longColumn("value", vector.input));
                            buffer.cancelCurrentRow();
                            buffer.rollbackUncommittedColumns();
                            Assert.assertEquals(vector.caseId, 0, buffer.getColumnCount());
                        } else {
                            binding.longColumn("value", vector.input);
                            buffer.nextRow();
                            int size = encoder.encodeSchema(buffer);
                            Assert.assertEquals(QwpConstants.FLAG_SCHEMA,
                                    Unsafe.getUnsafe().getByte(encoder.getBuffer().getBufferPtr()
                                            + QwpConstants.HEADER_OFFSET_FLAGS) & QwpConstants.FLAG_SCHEMA);
                            Reader reader = tableReader(encoder, size, wireType(vector.targetType));
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
                    count++;
                }
            }
            Assert.assertEquals(39, count);
            for (int i = 0; i < targets.length; i++) {
                Assert.assertTrue("missing target " + i, targets[i]);
            }
        });
    }

    @Test
    public void testDuplicateNullOmissionAndWholeRowRollback() throws Exception {
        assertMemoryLeak(() -> {
            int target = ColumnType.getDecimalType(2, 1);
            try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                 QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer,
                        column("value", target), column("only_b", target));

                binding.longColumn("value", 1).longColumn("value", 10);
                buffer.nextRow();

                binding.longColumn("only_b", 1);
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.longColumn("value", 10));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();

                binding.longColumn("value", -1);
                buffer.nextRow();
                binding.longColumn("value", Long.MIN_VALUE).longColumn("value", 1);
                buffer.nextRow();
                buffer.nextRow();

                int size = encoder.encodeSchema(buffer);
                Reader reader = tableHeader(encoder, size, 4, 1);
                decimalDefinition(reader, "value", QwpConstants.TYPE_DECIMAL64);
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(0x0c, reader.u8());
                Assert.assertEquals(1, reader.u8());
                Assert.assertEquals(10, reader.i64());
                Assert.assertEquals(-10, reader.i64());
                Assert.assertEquals(size, reader.position());

                buffer.reset();
                buffer.nextRow();
                int resetSize = encoder.encodeSchema(buffer);
                Reader reset = tableHeader(encoder, resetSize, 1, 1);
                decimalDefinition(reset, "value", QwpConstants.TYPE_DECIMAL64);
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(resetSize, reset.position());
            }
        });
    }

    private static void assertReason(LineSenderSchemaException.Reason reason, Runnable action) {
        Assert.assertEquals(reason, Assert.assertThrows(LineSenderSchemaException.class, action::run).getReason());
    }

    private static QwpSchemaBinding binding(QwpTableBuffer buffer, byte[]... columns) {
        return new QwpSchemaBinding(buffer, known(columns));
    }

    private static byte[] column(String name, int type) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + bytes.length + 6).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) bytes.length).put(bytes).putInt(type).putShort((short) 0).array();
    }

    private static void decimalDefinition(Reader reader, String name, byte type) {
        Assert.assertEquals(name, reader.string());
        Assert.assertEquals(type, reader.u8());
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

    private static Reader tableHeader(QwpWebSocketEncoder encoder, int size, int rows, int columns) {
        Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
        reader.skip(QwpConstants.HEADER_SIZE);
        Assert.assertEquals("t", reader.string());
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(1, reader.i32());
        Assert.assertEquals(1, reader.i64());
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(columns, reader.varint());
        return reader;
    }

    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, byte type) {
        Reader reader = tableHeader(encoder, size, 1, 1);
        decimalDefinition(reader, "value", type);
        return reader;
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
        private final long input;
        private final String outcome;
        private final int targetPrecision;
        private final int targetScale;
        private final String targetType;

        private Vector(String[] fields) {
            caseId = fields[0];
            input = Long.parseLong(fields[1]);
            targetType = fields[2];
            targetPrecision = Integer.parseInt(fields[3]);
            targetScale = Integer.parseInt(fields[4]);
            outcome = fields[5];
            Assert.assertTrue(outcome.equals("VALUE") || outcome.equals("INVALID") || outcome.equals("NULL"));
            Assert.assertEquals(expectedTargetType(targetPrecision), targetType);
            int limbs = outcome.equals("VALUE") ? wireLongCount(targetType) : 0;
            for (int i = 0; i < limbs; i++) {
                expectedLimbs[i] = parseHex(fields[6 + i]);
            }
            for (int i = limbs; i < 4; i++) {
                Assert.assertEquals("-", fields[6 + i]);
            }
        }

        private boolean invalid() {
            return outcome.equals("INVALID");
        }

        private boolean isNull() {
            return outcome.equals("NULL");
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
