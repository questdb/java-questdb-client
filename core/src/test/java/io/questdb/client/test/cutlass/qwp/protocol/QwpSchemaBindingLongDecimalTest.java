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
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.assertReason;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.binding;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.column;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.tableHeader;
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
                            QwpTestWireReader reader = tableReader(encoder, size, wireType(vector.targetType));
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
                QwpTestWireReader reader = tableHeader(encoder, size, 4, 1);
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
                QwpTestWireReader reset = tableHeader(encoder, resetSize, 1, 1);
                decimalDefinition(reset, "value", QwpConstants.TYPE_DECIMAL64);
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(1, reset.u8());
                Assert.assertEquals(resetSize, reset.position());
            }
        });
    }

    private static void decimalDefinition(QwpTestWireReader reader, String name, byte type) {
        Assert.assertEquals(name, reader.string());
        Assert.assertEquals(type, reader.u8());
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

    private static QwpTestWireReader tableReader(QwpWebSocketEncoder encoder, int size, byte type) {
        QwpTestWireReader reader = tableHeader(encoder, size, 1, 1);
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
