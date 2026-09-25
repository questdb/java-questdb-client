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
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.assertReason;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.column;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.known;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.missing;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.parameterizedColumn;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.tableHeader;
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
                            QwpTestWireReader reader = tableReader(encoder, size, 1);
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
                            Assert.assertEquals(size, reader.position());
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
                QwpTestWireReader reader = tableHeader(encoder, size, 5, 2);
                Assert.assertEquals("a", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_GEOHASH, reader.u8());
                Assert.assertEquals("c", reader.string());
                Assert.assertEquals(QwpConstants.TYPE_GEOHASH, reader.u8());
                assertGeo(reader, 29, 8, 0xff);
                assertGeo(reader, 15, 8, 1);
                Assert.assertEquals(size, reader.position());

            buffer.reset();
                binding.stringColumn("a", "04");
                buffer.nextRow();
                int resetSize = encoder.encodeSchema(buffer);
                QwpTestWireReader reset = tableHeader(encoder, resetSize, 1, 2);
                Assert.assertEquals("a", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_GEOHASH, reset.u8());
                Assert.assertEquals("c", reset.string());
                Assert.assertEquals(QwpConstants.TYPE_GEOHASH, reset.u8());
                assertGeo(reset, 0, 8, 1);
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(8, reset.varint());
                Assert.assertEquals(resetSize, reset.position());
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
                    QwpTestWireReader reader = tableReader(encoder, size, 1);
                    assertGeo(reader, 0, bits, max);
                    Assert.assertEquals(size, reader.position());

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
                    Assert.assertEquals(size, reader.position());
                }
            }

            try (QwpTableBuffer buffer = new QwpTableBuffer("t"); QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                buffer.getOrCreateColumn("value", QwpConstants.TYPE_GEOHASH, true)
                        .addGeoHash(0x0fffffffffffffffL, 60);
                buffer.nextRow();
                int size = encoder.encode(buffer);
                QwpTestWireReader reader = new QwpTestWireReader(encoder.getBuffer().getBufferPtr(), size);
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
                Assert.assertEquals(size, reader.position());
            }

            try (QwpTableBuffer buffer = new QwpTableBuffer("t"); QwpWebSocketEncoder encoder = new QwpWebSocketEncoder()) {
                buffer.getOrCreateColumn("value", QwpConstants.TYPE_GEOHASH, false)
                        .addGeoHash(0x0fffffffffffffffL, 60);
                buffer.nextRow();
                int size = encoder.encodeSchema(buffer, 1, 1);
                QwpTestWireReader reader = tableReader(encoder, size, 1);
                Assert.assertEquals(0, reader.u8());
                Assert.assertEquals(60, reader.varint());
                Assert.assertEquals(0x0fffffffffffffffL, reader.i64());
                Assert.assertEquals(size, reader.position());
            }
        });
    }

    private static void assertGeo(QwpTestWireReader reader, int bitmap, int bits, long value) {
        Assert.assertEquals(bitmap < 0 ? 0 : 1, reader.u8());
        if (bitmap >= 0) {
            Assert.assertEquals(bitmap, reader.u8());
        }
        Assert.assertEquals(bits, reader.varint());
        for (int i = 0; i < (bits + 7) / 8; i++) {
            Assert.assertEquals((int) ((value >>> (8 * i)) & 0xff), reader.u8());
        }
    }

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, int rows) {
        QwpTestWireReader reader = tableHeader(encoder, size, rows, 1);
        Assert.assertEquals("value", reader.string());
        Assert.assertEquals(QwpConstants.TYPE_GEOHASH, reader.u8());
        return reader;
    }
}
