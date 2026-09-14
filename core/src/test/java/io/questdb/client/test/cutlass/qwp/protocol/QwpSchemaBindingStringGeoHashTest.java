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

public class QwpSchemaBindingStringGeoHashTest {
    private static final String CORPUS = "/io/questdb/client/cutlass/qwp/string-geohash-conversions.tsv";
    private static final String HEADER = "# case_id\tinput\tbits\toutcome\tvalue_hex";

    @Test
    public void testCorpusUsesExactGeoHashWire() throws Exception {
        assertMemoryLeak(() -> {
            InputStream stream = getClass().getResourceAsStream(CORPUS);
            Assert.assertNotNull(CORPUS, stream);
            boolean headerSeen = false;
            int count = 0;
            try (BufferedReader lines = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String line;
                while ((line = lines.readLine()) != null) {
                    if (line.isEmpty()) {
                        continue;
                    }
                    if (line.charAt(0) == '#') {
                        Assert.assertFalse(headerSeen);
                        Assert.assertEquals(HEADER, line);
                        headerSeen = true;
                        continue;
                    }
                    String[] fields = line.split("\t", -1);
                    Assert.assertEquals(line, 5, fields.length);
                    int bits = Integer.parseInt(fields[2]);
                    try (QwpTableBuffer buffer = new QwpTableBuffer("t");
                         QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                        QwpSchemaBinding binding = new QwpSchemaBinding(buffer,
                                known(-1, column("value", ColumnType.getGeoHashTypeWithBits(bits))));
                        CharSequence value = "<NULL>".equals(fields[1]) ? null
                                : "<EMPTY>".equals(fields[1]) ? "" : fields[1];
                        if ("INVALID".equals(fields[3])) {
                            assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                                    () -> binding.stringColumn("value", value));
                        } else if ("VALUE".equals(fields[3]) || "NULL".equals(fields[3])) {
                            binding.stringColumn("value", value);
                            buffer.nextRow();
                            int size = encoder.encodeSchema(buffer);
                            Reader reader = tableReader(encoder, size, 1);
                            if ("NULL".equals(fields[3])) {
                                Assert.assertEquals(1, reader.u8());
                                Assert.assertEquals(1, reader.u8());
                                Assert.assertEquals(bits, reader.varint());
                            } else {
                                Assert.assertEquals(1, reader.u8());
                                Assert.assertEquals(0, reader.u8());
                                Assert.assertEquals(bits, reader.varint());
                                long expected = Long.parseUnsignedLong(fields[4], 16);
                                for (int i = 0; i < (bits + 7) / 8; i++) {
                                    Assert.assertEquals((int) ((expected >>> (8 * i)) & 0xff), reader.u8());
                                }
                            }
                            Assert.assertEquals(size, reader.position);
                        } else {
                            throw new AssertionError("unknown outcome: " + fields[3]);
                        }
                    } catch (AssertionError e) {
                        throw new AssertionError("case_id=" + fields[0] + ": " + e.getMessage(), e);
                    }
                    count++;
                }
            }
            Assert.assertTrue(headerSeen);
            Assert.assertEquals(18, count);
        });
    }

    @Test
    public void testDuplicateBitmapRollbackResetInferenceAndGuards() throws Exception {
        assertMemoryLeak(() -> {
            int geo8 = ColumnType.getGeoHashTypeWithBits(8);
            try (QwpTableBuffer buffer = new QwpTableBuffer("t"); QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                QwpSchemaBinding binding = new QwpSchemaBinding(buffer,
                        known(-1, column("a", geo8), column("bad", ColumnType.LONG), column("c", geo8)));
                binding.stringColumn("a", null).binaryColumn("a", new byte[]{1});
                buffer.nextRow();
                binding.stringColumn("a", "zz");
                buffer.nextRow();
                binding.stringColumn("a", "");
                buffer.nextRow();
                buffer.nextRow();
                binding.stringColumn("a", "zz");
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.stringColumn("bad", "z"));
                buffer.cancelCurrentRow();
                buffer.rollbackUncommittedColumns();
                binding.stringColumn("c", "04");
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer);
                Reader reader = tableHeader(encoder, size, 5, 2);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_GEOHASH, reader.u8());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_GEOHASH, reader.u8());
                assertGeo(reader, 29, 8, 0xff);
                assertGeo(reader, 15, 8, 1);
                Assert.assertEquals(size, reader.position);

            buffer.reset();
                binding.stringColumn("a", "04");
                buffer.nextRow();
                int resetSize = encoder.encodeSchema(buffer);
                Reader reset = tableHeader(encoder, resetSize, 1, 2);
                Assert.assertEquals("a", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_GEOHASH, reset.u8());
                Assert.assertEquals("c", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_GEOHASH, reset.u8());
                assertGeo(reset, 0, 8, 1);
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(8, reset.varint());
                Assert.assertEquals(resetSize, reset.position);
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
                binding.stringColumn("value", "z");
                buffer.nextRow();
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, buffer.getColumnDefs()[0].getTypeCode());
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(buffer, missing());
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.unsupportedColumn("value", "GEOHASH"));
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(buffer,
                        known(-1, parameterizedColumn("value", geo8)));
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.stringColumn("value", "z"));
            }
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = new QwpSchemaBinding(buffer,
                        known(0, column("ts", ColumnType.TIMESTAMP_MICRO)));
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.stringColumn("ts", "z"));
            }
        });
    }

    @Test
    public void testMalformedPackedGeoHashTargetsRemainTypedUnsupported() throws Exception {
        assertMemoryLeak(() -> {
            int geoFlag = 1 << 16;
            int[] malformed = {
                    ColumnType.GEOHASH,
                    geoFlag | ColumnType.GEOBYTE,
                    geoFlag | (61 << 8) | ColumnType.GEOLONG,
                    geoFlag | (8 << 8) | ColumnType.STRING,
                    ColumnType.getGeoHashTypeWithBits(8) | (1 << 24)
            };
            for (int targetType : malformed) {
                try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    QwpSchemaBinding binding = new QwpSchemaBinding(buffer,
                            known(-1, column("value", targetType)));
                    assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                            () -> binding.stringColumn("value", "zz"));
                }
            }
        });
    }

    @Test
    public void testAllSixtyBitValuesAndLegacyEncodingRemainUnambiguous() throws Exception {
        assertMemoryLeak(() -> {
            for (int bits = 1; bits <= 60; bits++) {
                int targetType = ColumnType.getGeoHashTypeWithBits(bits);
                StringBuilder input = new StringBuilder((bits + 4) / 5);
                for (int i = 0; i < (bits + 4) / 5; i++) {
                    input.append('z');
                }
                long max = (1L << bits) - 1;
                try (QwpTableBuffer buffer = new QwpTableBuffer("t");
                     QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                    QwpSchemaBinding binding = new QwpSchemaBinding(buffer,
                            known(-1, column("value", targetType)));
                    binding.stringColumn("value", input);
                    buffer.nextRow();
                    int size = encoder.encodeSchema(buffer);
                    Reader reader = tableReader(encoder, size, 1);
                    assertGeo(reader, 0, bits, max);
                    Assert.assertEquals(size, reader.position);

                    buffer.reset();
                    binding.stringColumn("value", input);
                    buffer.nextRow();
                    binding.stringColumn("value", null);
                    buffer.nextRow();
                    binding.stringColumn("value", "");
                    buffer.nextRow();
                    size = encoder.encodeSchema(buffer);
                    reader = tableReader(encoder, size, 3);
                    assertGeo(reader, 6, bits, max);
                    Assert.assertEquals(size, reader.position);
                }
            }

            try (QwpTableBuffer buffer = new QwpTableBuffer("t"); QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                buffer.getOrCreateColumn("value", QwpConstants.TYPE_GEOHASH, true)
                        .addGeoHash(0x0fffffffffffffffL, 60);
                buffer.nextRow();
                int size = encoder.encode(buffer);
                Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
                Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, reader.i32());
                Assert.assertEquals(QwpConstants.VERSION, reader.u8());
                Assert.assertEquals(QwpConstants.FLAG_GORILLA, reader.u8());
                Assert.assertEquals(1, reader.u16());
                Assert.assertEquals(size - QwpConstants.HEADER_SIZE, reader.i32());
                Assert.assertEquals("t", reader.string());
                Assert.assertEquals(1, reader.varint());
                Assert.assertEquals(1, reader.varint());
                Assert.assertEquals("value", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_GEOHASH, reader.u8());
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(60, reader.varint());
                Assert.assertEquals(0x0fffffffffffffffL, reader.i64());
                Assert.assertEquals(size, reader.position);
            }

            try (QwpTableBuffer buffer = new QwpTableBuffer("t"); QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                buffer.getOrCreateColumn("value", QwpConstants.TYPE_GEOHASH, false)
                        .addGeoHash(0x0fffffffffffffffL, 60);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer, 1, 1);
                Reader reader = tableReader(encoder, size, 1);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(60, reader.varint());
                Assert.assertEquals(0x0fffffffffffffffL, reader.i64());
                Assert.assertEquals(size, reader.position);
            }
        });
    }

    private static void assertGeo(Reader reader, int bitmap, int bits, long value) {
        Assert.assertEquals(bitmap < 0 ? 0 : 1, reader.u8());
        if (bitmap >= 0) {
            Assert.assertEquals(bitmap, reader.u8());
        }
        Assert.assertEquals(bits, reader.varint());
        for (int i = 0; i < (bits + 7) / 8; i++) {
            Assert.assertEquals((int) ((value >>> (8 * i)) & 0xff), reader.u8());
        }
    }

    private static byte[] column(String name, int type) {
        return column(name, type, new byte[0]);
    }

    private static byte[] parameterizedColumn(String name, int type) {
        return column(name, type, new byte[]{1});
    }

    private static byte[] column(String name, int type, byte[] parameters) {
        byte[] bytes = name.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(2 + bytes.length + 6 + parameters.length).order(ByteOrder.LITTLE_ENDIAN)
                .putShort((short) bytes.length).put(bytes).putInt(type).putShort((short) parameters.length)
                .put(parameters).array();
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

    private static QwpSchemaResponse decode(byte[] payload) {
        ByteBuffer frame = ByteBuffer.allocate(QwpConstants.HEADER_SIZE + payload.length).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(QwpConstants.MAGIC_MESSAGE).put((byte) QwpConstants.VERSION)
                .put(QwpSchemaProtocol.FLAG_CONTROL).putShort((short) 0).putInt(payload.length).put(payload);
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

    private static Reader tableReader(QwpWebSocketEncoder encoder, int size, int rows) {
        Reader reader = tableHeader(encoder, size, rows, 1);
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(QwpConstants.TYPE_GEOHASH, reader.u8());
        return reader;
    }

    private static Reader tableHeader(QwpWebSocketEncoder encoder, int size, int rows, int columns) {
        Reader reader = new Reader(encoder.getBuffer().getBufferPtr(), size);
        Assert.assertEquals(QwpConstants.MAGIC_MESSAGE, reader.i32());
        Assert.assertEquals(QwpConstants.VERSION, reader.u8());
        Assert.assertEquals(QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_SCHEMA, reader.u8());
        Assert.assertEquals(1, reader.u16());
        Assert.assertEquals(size - QwpConstants.HEADER_SIZE, reader.i32());
        Assert.assertEquals("t", reader.string());
        Assert.assertEquals(1, reader.u8());
        Assert.assertEquals(1, reader.i32());
        Assert.assertEquals(1, reader.i64());
        Assert.assertEquals(rows, reader.varint());
        Assert.assertEquals(columns, reader.varint());
        return reader;
    }

    private static void assertReason(LineSenderSchemaException.Reason reason, Runnable action) {
        Assert.assertEquals(reason, Assert.assertThrows(LineSenderSchemaException.class, action::run).getReason());
    }

    private static final class Reader {
        private final long address;
        private final int limit;
        private int position;

        private Reader(long address, int limit) { this.address = address; this.limit = limit; }
        private int i32() { check(4); int value = Unsafe.getUnsafe().getInt(address + position); position += 4; return value; }
        private long i64() { check(8); long value = Unsafe.getUnsafe().getLong(address + position); position += 8; return value; }
        private void skip(int length) { check(length); position += length; }
        private String string() { int length = varint(); byte[] bytes = new byte[length]; for (int i = 0; i < length; i++) bytes[i] = (byte) u8(); return new String(bytes, StandardCharsets.UTF_8); }
        private int u8() { check(1); return Unsafe.getUnsafe().getByte(address + position++) & 0xff; }
        private int u16() { check(2); int value = Unsafe.getUnsafe().getShort(address + position) & 0xffff; position += 2; return value; }
        private int varint() { int result = 0; int shift = 0; int b; do { b = u8(); result |= (b & 0x7f) << shift; shift += 7; } while ((b & 0x80) != 0); return result; }
        private void check(int size) { Assert.assertTrue(position + size <= limit); }
    }
}
