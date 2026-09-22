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
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.assertReason;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.binding;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.column;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.missing;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.tableHeader;
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
                QwpTestWireReader reader = tableHeader(encoder, size, 1, 4);
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
                QwpTestWireReader reader = tableHeader(encoder, size, 1, 1, -1, -1);
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
                QwpTestWireReader reader = tableHeader(encoder, size, 2, 1);
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
                QwpTestWireReader reader = tableHeader(encoder, size, 2, 1, -1, -1);
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

    @Test
    public void testRejectsStringValuesForAllArrayRanks() throws Exception {
        assertMemoryLeak(() -> {
            for (int dimensions = 1; dimensions <= ColumnType.ARRAY_NDIMS_LIMIT; dimensions++) {
                try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    QwpSchemaBinding binding = binding(buffer, column("a", arrayType(dimensions)));
                    assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                            () -> binding.stringColumn("a", "zz"));
                    buffer.cancelCurrentRow();
                    buffer.rollbackUncommittedColumns();
                    assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                            () -> binding.stringColumn("a", ""));
                }
            }
        });
    }

    @Test
    public void testRejectsGeoHashValuesForAllArrayRanks() throws Exception {
        assertMemoryLeak(() -> {
            for (int dimensions = 1; dimensions <= ColumnType.ARRAY_NDIMS_LIMIT; dimensions++) {
                try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    QwpSchemaBinding binding = binding(buffer, column("a", arrayType(dimensions)));
                    assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                            () -> binding.geoHashColumn("a", 1023L, 10));
                    buffer.cancelCurrentRow();
                    buffer.rollbackUncommittedColumns();
                    assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                            () -> binding.geoHashColumn("a", "zz"));
                }
            }
        });
    }

    @Test
    public void testStringNullUsesArrayNullWireForAllArrayRanks() throws Exception {
        assertMemoryLeak(() -> {
            for (int dimensions = 1; dimensions <= ColumnType.ARRAY_NDIMS_LIMIT; dimensions++) {
                try (QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
                     QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                    QwpSchemaBinding binding = binding(buffer, column("a", arrayType(dimensions)));
                    binding.stringColumn("a", null);
                    buffer.nextRow();
                    int size = encoder.encodeSchema(buffer);
                    QwpTestWireReader reader = tableHeader(encoder, size, 1, 1);
                    columnDef(reader, "a");
                    Assert.assertEquals(1, reader.u8());
                    Assert.assertEquals(1, reader.u8());
                    Assert.assertEquals(size, reader.position());
                }
            }
        });
    }

    private static void array(QwpTestWireReader reader, int[] shape, double... values) {
        Assert.assertEquals(0, reader.u8());
        rawArray(reader, shape.length, shape, values);
    }

    private static int arrayType(int dimensions) {
        return ColumnType.encodeArrayType(ColumnType.DOUBLE, dimensions);
    }

    private static void columnDef(QwpTestWireReader reader, String name) {
        Assert.assertEquals(name, reader.string());
        Assert.assertEquals(QwpConstants.TYPE_DOUBLE_ARRAY, reader.u8());
    }

    private static void rawArray(QwpTestWireReader reader, int expectedDims, int[] shape, double[] values) {
        Assert.assertEquals(expectedDims, reader.u8());
        for (int dimension : shape) {
            Assert.assertEquals(dimension, reader.i32());
        }
        for (double value : values) {
            Assert.assertEquals(Double.doubleToRawLongBits(value), reader.i64());
        }
    }
}
