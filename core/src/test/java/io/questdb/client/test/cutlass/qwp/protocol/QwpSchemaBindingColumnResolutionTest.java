/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (c) 2014-2019 Appsicle
 * Copyright (c) 2019-2026 QuestDB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.LineSenderSchemaException;
import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpSchemaBinding;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.assertReason;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.binding;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.column;
import static io.questdb.client.test.cutlass.qwp.protocol.QwpSchemaTestFixtures.parameterizedColumn;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * A binding resolves each column against the schema once, when it creates the
 * buffer column, and later writes reuse that resolution. These tests pin that
 * the reuse keeps every per-value rule of the first write.
 */
public class QwpSchemaBindingColumnResolutionTest {

    @Test
    public void testDuplicateWriteToResolvedColumnKeepsFirstValue() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, column("v", ColumnType.INT));
                binding.longColumn("v", 1);
                buffer.nextRow();
                binding.longColumn("v", 2);
                binding.longColumn("V", 3);
                buffer.nextRow();
                assertInts(buffer.getColumn(0), 1, 2);
            }
        });
    }

    @Test
    public void testInferredColumnTypeConflictIsDetectedOnLaterRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, column("v", ColumnType.INT));
                binding.longColumn("x", 1);
                buffer.nextRow();
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.doubleColumn("x", 2.0), "inferred column type conflict");
                buffer.cancelCurrentRow();
                binding.longColumn("X", 3);
                buffer.nextRow();
                Assert.assertEquals(1, buffer.getColumnCount());
                QwpTableBuffer.ColumnBuffer column = buffer.getColumn(0);
                Assert.assertEquals(QwpConstants.TYPE_LONG, column.getType());
                Assert.assertEquals(2, column.getSize());
                Assert.assertEquals(1, Unsafe.getUnsafe().getLong(column.getDataAddress()));
                Assert.assertEquals(3, Unsafe.getUnsafe().getLong(column.getDataAddress() + Long.BYTES));
            }
        });
    }

    @Test
    public void testInvalidNameIsRejectedAfterColumnsExist() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, column("v", ColumnType.INT));
                binding.longColumn("v", 1);
                buffer.nextRow();
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.longColumn("", 2), "invalid column name");
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.longColumn(null, 2), "invalid column name");
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.longColumn("v?", 2), "invalid column name");
                Assert.assertEquals(1, buffer.getColumnCount());
            }
        });
    }

    @Test
    public void testRejectedSchemaColumnsStayRejectedAcrossRows() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, 0,
                        column("ts", ColumnType.TIMESTAMP_MICRO),
                        parameterizedColumn("p", ColumnType.LONG),
                        column("v", ColumnType.INT));
                for (int attempt = 0; attempt < 2; attempt++) {
                    assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                            () -> binding.longColumn("ts", 1), "designated timestamp writes");
                    buffer.cancelCurrentRow();
                    assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                            () -> binding.longColumn("p", 1), "parameterized target type");
                    buffer.cancelCurrentRow();
                    binding.longColumn("v", attempt);
                    buffer.nextRow();
                }
                Assert.assertEquals("rejected targets must not create buffer columns", 1, buffer.getColumnCount());
                assertInts(buffer.getColumn(0), 0, 1);
            }
        });
    }

    @Test
    public void testResolvedColumnKeepsConversionAndRangeChecks() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpTableBuffer buffer = new QwpTableBuffer("t")) {
                QwpSchemaBinding binding = binding(buffer, column("v", ColumnType.INT));
                binding.longColumn("v", 7);
                buffer.nextRow();
                binding.longColumn("V", 8);
                buffer.nextRow();
                assertReason(LineSenderSchemaException.Reason.INVALID_VALUE,
                        () -> binding.longColumn("v", 1L << 40));
                buffer.cancelCurrentRow();
                assertReason(LineSenderSchemaException.Reason.UNSUPPORTED_FEATURE,
                        () -> binding.binaryColumn("v", new byte[]{1}), "conversion is not implemented");
                buffer.cancelCurrentRow();
                Assert.assertEquals(1, buffer.getColumnCount());
                Assert.assertEquals(QwpConstants.TYPE_INT, buffer.getColumn(0).getType());
                assertInts(buffer.getColumn(0), 7, 8);
            }
        });
    }

    private static void assertInts(QwpTableBuffer.ColumnBuffer column, int... expected) {
        Assert.assertEquals(expected.length, column.getSize());
        for (int i = 0; i < expected.length; i++) {
            Assert.assertEquals(expected[i], Unsafe.getUnsafe().getInt(column.getDataAddress() + (long) i * Integer.BYTES));
        }
    }
}
