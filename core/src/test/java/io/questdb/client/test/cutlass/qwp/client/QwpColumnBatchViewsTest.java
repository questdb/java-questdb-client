/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.cutlass.qwp.client.ColumnView;
import io.questdb.client.cutlass.qwp.client.QwpColumnBatch;
import io.questdb.client.cutlass.qwp.client.RowView;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Long256Impl;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.Uuid;
import io.questdb.client.std.bytes.DirectByteSequence;
import io.questdb.client.std.str.DirectUtf8Sequence;
import io.questdb.client.std.str.StringSink;
import io.questdb.client.std.str.Utf8s;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;

/**
 * White-box unit tests for {@link RowView} and {@link ColumnView}.
 * <p>
 * Each test allocates native memory directly, populates a
 * {@code QwpColumnLayout} via reflection (the layout fields are package-private
 * on the production type; the production module is declared {@code open}, so
 * reflective access works from the test module), registers it on a fresh
 * {@link QwpColumnBatch}, and asserts the view's behaviour. All allocations
 * are tracked in {@link #allocations} and freed in {@link #tearDown()}.
 * <p>
 * The full wire-format round-trip is covered by
 * {@code QwpEgressTypesExhaustiveTest} in the parent module; these tests pin
 * the view-layer behaviour in isolation so a regression there points
 * unambiguously at the views, not the decoder.
 */
public class QwpColumnBatchViewsTest {

    private static final String COLUMN_INFO = "io.questdb.client.cutlass.qwp.client.QwpEgressColumnInfo";
    private static final String COLUMN_LAYOUT = "io.questdb.client.cutlass.qwp.client.QwpColumnLayout";
    private final List<long[]> allocations = new ArrayList<>();

    @Before
    public void setUp() {
        allocations.clear();
    }

    @After
    public void tearDown() {
        // Safety net for exits that bypass the assertMemoryLeak wrapper;
        // normally a no-op because the wrapper's finally already freed them.
        freeAllocations();
    }

    /**
     * Wraps a test body in {@link TestUtils#assertMemoryLeak} and frees the
     * tracked allocations BEFORE the leak check fires -- LeakCheck closes at
     * the end of the wrapped lambda, so freeing only in @After would run too
     * late and fail every test now that the check asserts strict per-tag
     * equality.
     */
    private void assertMemoryLeak(TestUtils.LeakProneCode code) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try {
                code.run();
            } finally {
                freeAllocations();
            }
        });
    }

    private void freeAllocations() {
        for (long[] alloc : allocations) {
            Unsafe.free(alloc[0], alloc[1], MemoryTag.NATIVE_DEFAULT);
        }
        allocations.clear();
    }

    @Test
    public void testColumnViewArrayRowAddr() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            Object l = setupArrayColumnLayout(batch,
                    new boolean[]{false, true, false},
                    new double[][]{{1.0, 2.0}, null, {3.0, 4.0}});
            ColumnView col = batch.column(0);
            Assert.assertNotEquals(0L, col.arrayRowAddr(0));
            Assert.assertEquals(0L, col.arrayRowAddr(1));    // NULL row
            Assert.assertNotEquals(0L, col.arrayRowAddr(2));
            Assert.assertEquals(1, col.getArrayNDims(0));
            Assert.assertEquals(0, col.getArrayNDims(1));    // NULL row -> 0
            long[] perRow = (long[]) readField(l, "arrayRowAddr");
            Assert.assertEquals(perRow[2], col.arrayRowAddr(2));
        });
    }

    @Test
    public void testColumnViewBatchAccessorReturnsParent() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 1);
            setupLongColumnLayout(batch, 0, "x", new long[]{42L}, new boolean[]{false});
            Assert.assertSame(batch, batch.column(0).batch());
        });
    }

    @Test
    public void testColumnViewBinaryAccessors() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            setupBinaryColumnLayout(batch,
                    new byte[][]{{0x00, 0x7F, (byte) 0xFF}, null, {0x01}},
                    new boolean[]{false, true, false});
            ColumnView col = batch.column(0);
            // heap-allocating variant
            byte[] copy = col.getBinary(0);
            Assert.assertArrayEquals(new byte[]{0x00, 0x7F, (byte) 0xFF}, copy);
            Assert.assertNull(col.getBinary(1));
            // zero-alloc A view
            DirectByteSequence a = col.getBinaryA(0);
            Assert.assertNotNull(a);
            Assert.assertEquals(3, a.size());
            Assert.assertEquals((byte) 0xFF, a.byteAt(2));
            // B view reads independently of A (dual slots)
            DirectByteSequence b = col.getBinaryB(2);
            Assert.assertNotNull(b);
            Assert.assertEquals(1, b.size());
            Assert.assertEquals((byte) 0x01, b.byteAt(0));
            Assert.assertNull(col.getBinaryB(1));
        });
    }

    @Test
    public void testColumnViewBoolValue() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 5);
            setupBooleanColumnLayout(batch, 0,
                    new boolean[]{true, false, true, true, false},
                    new boolean[]{false, false, false, false, false});
            ColumnView col = batch.column(0);
            Assert.assertTrue(col.getBoolValue(0));
            Assert.assertFalse(col.getBoolValue(1));
            Assert.assertTrue(col.getBoolValue(2));
            Assert.assertTrue(col.getBoolValue(3));
            Assert.assertFalse(col.getBoolValue(4));
            Assert.assertEquals(0, col.bytesPerValue());
        });
    }

    @Test
    public void testColumnViewByteValue() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 4);
            setupByteColumnLayout(batch, 0,
                    new byte[]{Byte.MIN_VALUE, -1, 0, Byte.MAX_VALUE},
                    new boolean[]{false, false, false, false});
            ColumnView col = batch.column(0);
            Assert.assertEquals(Byte.MIN_VALUE, col.getByteValue(0));
            Assert.assertEquals(-1, col.getByteValue(1));
            Assert.assertEquals(0, col.getByteValue(2));
            Assert.assertEquals(Byte.MAX_VALUE, col.getByteValue(3));
        });
    }

    @Test
    public void testColumnViewBytesPerValuePerType() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(8, 1);
            setupLongColumnLayout(batch, 0, "l", new long[]{0}, new boolean[]{false});
            setupIntColumnLayout(batch, 1, new int[]{0}, new boolean[]{false});
            setupDoubleColumnLayout(batch, 2, new double[]{0.0}, new boolean[]{false});
            setupFloatColumnLayout(batch, 3, new float[]{0.0f}, new boolean[]{false});
            setupBooleanColumnLayout(batch, 4, new boolean[]{false}, new boolean[]{false});
            setupByteColumnLayout(batch, 5, new byte[]{0}, new boolean[]{false});
            setupShortColumnLayout(batch, 6, new short[]{0}, new boolean[]{false});
            setupVarcharColumnLayout(batch, 7, "v", new String[]{""}, new boolean[]{false});

            Assert.assertEquals(8, batch.column(0).bytesPerValue());
            Assert.assertEquals(4, batch.column(1).bytesPerValue());
            Assert.assertEquals(8, batch.column(2).bytesPerValue());
            Assert.assertEquals(4, batch.column(3).bytesPerValue());
            Assert.assertEquals(0, batch.column(4).bytesPerValue());
            Assert.assertEquals(1, batch.column(5).bytesPerValue());
            Assert.assertEquals(2, batch.column(6).bytesPerValue());
            Assert.assertEquals(-1, batch.column(7).bytesPerValue());
        });
    }

    @Test
    public void testColumnViewCachedPerColumnIndex() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(2, 1);
            setupLongColumnLayout(batch, 0, "a", new long[]{1L}, new boolean[]{false});
            setupLongColumnLayout(batch, 1, "b", new long[]{2L}, new boolean[]{false});

            ColumnView a1 = batch.column(0);
            ColumnView b1 = batch.column(1);
            ColumnView a2 = batch.column(0);
            ColumnView b2 = batch.column(1);

            Assert.assertNotSame(a1, b1);
            Assert.assertSame(a1, a2);
            Assert.assertSame(b1, b2);
            Assert.assertEquals(0, a1.getColumnIndex());
            Assert.assertEquals(1, b1.getColumnIndex());
            Assert.assertEquals(1L, a1.getLongValue(0));
            Assert.assertEquals(2L, b1.getLongValue(0));
        });
    }

    @Test
    public void testColumnViewCharValue() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            setupCharColumnLayout(batch, 0,
                    new char[]{'A', 'z', '0'},
                    new boolean[]{false, false, false});
            ColumnView col = batch.column(0);
            Assert.assertEquals('A', col.getCharValue(0));
            Assert.assertEquals('z', col.getCharValue(1));
            Assert.assertEquals('0', col.getCharValue(2));
        });
    }

    @Test
    public void testColumnViewDecimal128Accessors() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            long[] lo = {0xFFEE_DDCC_BBAA_9988L, 0L, 0x1L};
            long[] hi = {0x1122_3344_5566_7788L, 0L, 0x2L};
            setupDecimal128ColumnLayout(batch, lo, hi, new boolean[]{false, true, false}, 6);
            ColumnView col = batch.column(0);
            Assert.assertEquals(0xFFEE_DDCC_BBAA_9988L, col.getDecimal128Low(0));
            Assert.assertEquals(0x1122_3344_5566_7788L, col.getDecimal128High(0));
            Assert.assertEquals(0L, col.getDecimal128Low(1));     // NULL -> 0
            Assert.assertEquals(0L, col.getDecimal128High(1));
            Assert.assertEquals(0x1L, col.getDecimal128Low(2));
            Assert.assertEquals(0x2L, col.getDecimal128High(2));
        });
    }

    @Test
    public void testColumnViewDelegatesAgreeWithBatchPrimitives() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(5, 4);
            setupLongColumnLayout(batch, 0, "l", new long[]{1L, 2L, 0L, 4L}, new boolean[]{false, false, true, false});
            setupIntColumnLayout(batch, 1, new int[]{10, 20, 0, 40}, new boolean[]{false, false, true, false});
            setupDoubleColumnLayout(batch, 2, new double[]{1.5, 2.5, 0.0, 4.5}, new boolean[]{false, false, true, false});
            setupBooleanColumnLayout(batch, 3, new boolean[]{true, false, false, true}, new boolean[]{false, false, true, false});
            setupVarcharColumnLayout(batch, 4, "s", new String[]{"a", "bb", null, "dddd"}, new boolean[]{false, false, true, false});

            for (int c = 0; c < 5; c++) {
                ColumnView col = batch.column(c);
                for (int r = 0; r < 4; r++) {
                    Assert.assertEquals("isNull col=" + c + " row=" + r, batch.isNull(c, r), col.isNull(r));
                }
            }
            for (int r = 0; r < 4; r++) {
                Assert.assertEquals(batch.getLongValue(0, r), batch.column(0).getLongValue(r));
                Assert.assertEquals(batch.getIntValue(1, r), batch.column(1).getIntValue(r));
                Assert.assertEquals(batch.getDoubleValue(2, r), batch.column(2).getDoubleValue(r), 0.0);
                Assert.assertEquals(batch.getBoolValue(3, r), batch.column(3).getBoolValue(r));
                DirectUtf8Sequence batchStr = batch.getStrA(4, r);
                DirectUtf8Sequence colStr = batch.column(4).getStrA(r);
                if (batchStr == null) {
                    Assert.assertNull("row " + r + ": column-view returned non-null for NULL row", colStr);
                } else {
                    Assert.assertNotNull("row " + r + ": column-view returned null for non-NULL row", colStr);
                    Assert.assertEquals(Utf8s.stringFromUtf8Bytes(batchStr), Utf8s.stringFromUtf8Bytes(colStr));
                }
            }
        });
    }

    @Test
    public void testColumnViewDoubleArrayElements() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            setupArrayColumnLayout(batch,
                    new boolean[]{false, true, false},
                    new double[][]{{1.0, 2.0, 3.0}, null, {4.5, 5.5}});
            ColumnView col = batch.column(0);
            Assert.assertArrayEquals(new double[]{1.0, 2.0, 3.0}, col.getDoubleArrayElements(0), 0.0);
            Assert.assertNull(col.getDoubleArrayElements(1));
            Assert.assertArrayEquals(new double[]{4.5, 5.5}, col.getDoubleArrayElements(2), 0.0);
        });
    }

    @Test
    public void testColumnViewDoubleValue() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 4);
            setupDoubleColumnLayout(batch, 0,
                    new double[]{1.5, -1.5, 0.0, Double.MAX_VALUE},
                    new boolean[]{false, false, false, false});
            ColumnView col = batch.column(0);
            Assert.assertEquals(1.5, col.getDoubleValue(0), 0.0);
            Assert.assertEquals(-1.5, col.getDoubleValue(1), 0.0);
            Assert.assertEquals(0.0, col.getDoubleValue(2), 0.0);
            Assert.assertEquals(Double.MAX_VALUE, col.getDoubleValue(3), 0.0);
        });
    }

    @Test
    public void testColumnViewFloatValue() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            setupFloatColumnLayout(batch, 0,
                    new float[]{1.5f, -1.5f, 0.0f},
                    new boolean[]{false, false, false});
            ColumnView col = batch.column(0);
            Assert.assertEquals(1.5f, col.getFloatValue(0), 0.0f);
            Assert.assertEquals(-1.5f, col.getFloatValue(1), 0.0f);
            Assert.assertEquals(0.0f, col.getFloatValue(2), 0.0f);
        });
    }

    @Test
    public void testColumnViewGeohashValue() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(3, 2);
            setupGeohashColumnLayout(batch, 0, "g20", new long[]{0xABCDEL, 0L}, new boolean[]{false, true}, 20);
            setupGeohashColumnLayout(batch, 1, "g40", new long[]{0x12345_6789AL, 0L}, new boolean[]{false, true}, 40);
            setupGeohashColumnLayout(batch, 2, "g60", new long[]{0x0FFFF_FFFF_FFFF_FFFL, 0L}, new boolean[]{false, true}, 60);

            Assert.assertEquals(0xABCDEL, batch.column(0).getGeohashValue(0));
            Assert.assertEquals(0x12345_6789AL, batch.column(1).getGeohashValue(0));
            Assert.assertEquals(0x0FFFF_FFFF_FFFF_FFFL, batch.column(2).getGeohashValue(0));
            // NULL rows return 0.
            Assert.assertEquals(0L, batch.column(0).getGeohashValue(1));
            Assert.assertEquals(0L, batch.column(1).getGeohashValue(1));
            Assert.assertEquals(0L, batch.column(2).getGeohashValue(1));
            // Stride mirrors (precisionBits + 7) / 8.
            Assert.assertEquals(3, batch.column(0).bytesPerValue());   // 20 bits -> 3 bytes
            Assert.assertEquals(5, batch.column(1).bytesPerValue());   // 40 bits -> 5 bytes
            Assert.assertEquals(8, batch.column(2).bytesPerValue());   // 60 bits -> 8 bytes
        });
    }

    @Test
    public void testColumnViewGetColumnIndex() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(3, 1);
            setupLongColumnLayout(batch, 0, "a", new long[]{0}, new boolean[]{false});
            setupLongColumnLayout(batch, 1, "b", new long[]{0}, new boolean[]{false});
            setupLongColumnLayout(batch, 2, "c", new long[]{0}, new boolean[]{false});
            Assert.assertEquals(0, batch.column(0).getColumnIndex());
            Assert.assertEquals(1, batch.column(1).getColumnIndex());
            Assert.assertEquals(2, batch.column(2).getColumnIndex());
        });
    }

    @Test
    public void testColumnViewGetColumnWireType() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(2, 1);
            setupLongColumnLayout(batch, 0, "l", new long[]{0}, new boolean[]{false});
            setupVarcharColumnLayout(batch, 1, "s", new String[]{""}, new boolean[]{false});
            Assert.assertEquals(QwpConstants.TYPE_LONG, batch.column(0).getColumnWireType());
            Assert.assertEquals(QwpConstants.TYPE_VARCHAR, batch.column(1).getColumnWireType());
        });
    }

    @Test
    public void testColumnViewIntValue() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 4);
            setupIntColumnLayout(batch, 0,
                    new int[]{Integer.MIN_VALUE + 1, -1, 0, Integer.MAX_VALUE},
                    new boolean[]{false, false, false, false});
            ColumnView col = batch.column(0);
            Assert.assertEquals(Integer.MIN_VALUE + 1, col.getIntValue(0));
            Assert.assertEquals(-1, col.getIntValue(1));
            Assert.assertEquals(0, col.getIntValue(2));
            Assert.assertEquals(Integer.MAX_VALUE, col.getIntValue(3));
        });
    }

    @Test
    public void testColumnViewLong256AndLong256Word() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 2);
            long[][] words = {{0xAAAAL, 0xBBBBL, 0xCCCCL, 0xDDDDL}, {0L, 0L, 0L, 0L}};
            setupLong256ColumnLayout(batch, words, new boolean[]{false, true});
            ColumnView col = batch.column(0);

            // Sink-based bulk read
            Long256Impl sink = new Long256Impl();
            Assert.assertTrue(col.getLong256(0, sink));
            Assert.assertEquals(0xAAAAL, sink.getLong0());
            Assert.assertEquals(0xBBBBL, sink.getLong1());
            Assert.assertEquals(0xCCCCL, sink.getLong2());
            Assert.assertEquals(0xDDDDL, sink.getLong3());
            Assert.assertFalse("NULL returns false", col.getLong256(1, sink));

            // Per-word read
            Assert.assertEquals(0xAAAAL, col.getLong256Word(0, 0));
            Assert.assertEquals(0xBBBBL, col.getLong256Word(0, 1));
            Assert.assertEquals(0xCCCCL, col.getLong256Word(0, 2));
            Assert.assertEquals(0xDDDDL, col.getLong256Word(0, 3));
            Assert.assertEquals(0L, col.getLong256Word(1, 0));  // NULL -> 0
        });
    }

    @Test
    public void testColumnViewLongValue() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 4);
            setupLongColumnLayout(batch, 0, "l",
                    new long[]{Long.MIN_VALUE + 1, -1L, 0L, Long.MAX_VALUE},
                    new boolean[]{false, false, false, false});
            ColumnView col = batch.column(0);
            Assert.assertEquals(Long.MIN_VALUE + 1, col.getLongValue(0));
            Assert.assertEquals(-1L, col.getLongValue(1));
            Assert.assertEquals(0L, col.getLongValue(2));
            Assert.assertEquals(Long.MAX_VALUE, col.getLongValue(3));
        });
    }

    @Test
    public void testColumnViewNonNullCount() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 5);
            setupLongColumnLayout(batch, 0, "l",
                    new long[]{1L, 2L, 0L, 4L, 0L},
                    new boolean[]{false, false, true, false, true});
            Assert.assertEquals(3, batch.column(0).nonNullCount());
        });
    }

    @Test
    public void testColumnViewNonNullIndex() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 5);
            // Rows 1 and 3 are NULL; dense indices for non-null rows are 0, 1, 2.
            setupLongColumnLayout(batch, 0, "l",
                    new long[]{10L, 0L, 20L, 0L, 30L},
                    new boolean[]{false, true, false, true, false});
            ColumnView col = batch.column(0);
            Assert.assertEquals(0, col.nonNullIndex(0));
            Assert.assertEquals(1, col.nonNullIndex(2));
            Assert.assertEquals(2, col.nonNullIndex(4));
        });
    }

    @Test
    public void testColumnViewNonNullIndexNoNulls() throws Exception {
        // When there are no nulls, dense index equals row index (layout skips the
        // nonNullIdx fill; the method just returns the row back).
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            setupLongColumnLayout(batch, 0, "l",
                    new long[]{1L, 2L, 3L},
                    new boolean[]{false, false, false});
            ColumnView col = batch.column(0);
            Assert.assertEquals(0, col.nonNullIndex(0));
            Assert.assertEquals(1, col.nonNullIndex(1));
            Assert.assertEquals(2, col.nonNullIndex(2));
        });
    }

    @Test
    public void testColumnViewNullBitmapAddrNoNulls() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            setupLongColumnLayout(batch, 0, "l",
                    new long[]{1L, 2L, 3L},
                    new boolean[]{false, false, false});
            Assert.assertEquals(0L, batch.column(0).nullBitmapAddr());
        });
    }

    @Test
    public void testColumnViewNullBitmapAddrWithNulls() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 5);
            setupLongColumnLayout(batch, 0, "l",
                    new long[]{1L, 0L, 3L, 0L, 5L},
                    new boolean[]{false, true, false, true, false});
            ColumnView col = batch.column(0);
            long bm = col.nullBitmapAddr();
            Assert.assertNotEquals(0L, bm);
            byte b = Unsafe.getUnsafe().getByte(bm);
            Assert.assertEquals(0, b & 1);
            Assert.assertEquals(2, b & 2);
            Assert.assertEquals(0, b & 4);
            Assert.assertEquals(8, b & 8);
            Assert.assertEquals(0, b & 16);
        });
    }

    @Test
    public void testColumnViewNullValuesReturnTypeSentinels() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(6, 1);
            setupLongColumnLayout(batch, 0, "l", new long[]{0L}, new boolean[]{true});
            setupIntColumnLayout(batch, 1, new int[]{0}, new boolean[]{true});
            setupDoubleColumnLayout(batch, 2, new double[]{0.0}, new boolean[]{true});
            setupFloatColumnLayout(batch, 3, new float[]{0.0f}, new boolean[]{true});
            setupBooleanColumnLayout(batch, 4, new boolean[]{false}, new boolean[]{true});
            setupVarcharColumnLayout(batch, 5, "s", new String[]{null}, new boolean[]{true});

            Assert.assertEquals(0L, batch.column(0).getLongValue(0));
            Assert.assertEquals(0, batch.column(1).getIntValue(0));
            Assert.assertTrue(Double.isNaN(batch.column(2).getDoubleValue(0)));
            Assert.assertTrue(Float.isNaN(batch.column(3).getFloatValue(0)));
            Assert.assertFalse(batch.column(4).getBoolValue(0));
            Assert.assertNull(batch.column(5).getStrA(0));
            for (int c = 0; c < 6; c++) {
                Assert.assertTrue("col " + c + " row 0 must be null", batch.column(c).isNull(0));
            }
        });
    }

    @Test
    public void testColumnViewOfReturnsThis() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(2, 1);
            setupLongColumnLayout(batch, 0, "a", new long[]{1L}, new boolean[]{false});
            setupLongColumnLayout(batch, 1, "b", new long[]{2L}, new boolean[]{false});
            ColumnView v = batch.column(0);
            Assert.assertSame(v, v.of(1));
            Assert.assertEquals(1, v.getColumnIndex());
            Assert.assertEquals(2L, v.getLongValue(0));
        });
    }

    @Test
    public void testColumnViewRebindingPicksUpFreshLayout() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 2);
            setupLongColumnLayout(batch, 0, "l", new long[]{1L, 2L}, new boolean[]{false, false});
            ColumnView col = batch.column(0);
            Assert.assertEquals(1L, col.getLongValue(0));

            // Swap in a fresh layout, as the decoder would on the next batch.
            Object freshLayout = newLayoutInstance();
            Object info = newColumnInfo("l", QwpConstants.TYPE_LONG);
            writeField(freshLayout, "info", info);
            long addr = allocate(16);
            Unsafe.getUnsafe().putLong(addr, 99L);
            Unsafe.getUnsafe().putLong(addr + 8, 100L);
            writeField(freshLayout, "valuesAddr", addr);
            writeField(freshLayout, "nonNullCount", 2);
            writeField(freshLayout, "nullBitmapAddr", 0L);
            writeField(freshLayout, "nonNullIdx", null);
            setLayoutInBatch(batch, 0, freshLayout);

            ColumnView refreshed = batch.column(0);
            Assert.assertSame(col, refreshed);
            Assert.assertEquals(99L, refreshed.getLongValue(0));
            Assert.assertEquals(100L, refreshed.getLongValue(1));
        });
    }

    @Test
    public void testColumnViewShortValue() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 4);
            setupShortColumnLayout(batch, 0,
                    new short[]{Short.MIN_VALUE + 1, -1, 0, Short.MAX_VALUE},
                    new boolean[]{false, false, false, false});
            ColumnView col = batch.column(0);
            Assert.assertEquals(Short.MIN_VALUE + 1, col.getShortValue(0));
            Assert.assertEquals(-1, col.getShortValue(1));
            Assert.assertEquals(0, col.getShortValue(2));
            Assert.assertEquals(Short.MAX_VALUE, col.getShortValue(3));
        });
    }

    @Test
    public void testColumnViewStrBDualHold() throws Exception {
        // strA and strB are independent slots; a call to strB must not invalidate
        // an already-obtained strA view, and vice-versa.
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            setupVarcharColumnLayout(batch, 0, "s",
                    new String[]{"alpha", "beta", null},
                    new boolean[]{false, false, true});
            ColumnView col = batch.column(0);
            DirectUtf8Sequence a = col.getStrA(0);
            DirectUtf8Sequence b = col.getStrB(1);
            Assert.assertNotNull(a);
            Assert.assertNotNull(b);
            // Both must read back their original bindings.
            Assert.assertEquals("alpha", Utf8s.stringFromUtf8Bytes(a));
            Assert.assertEquals("beta", Utf8s.stringFromUtf8Bytes(b));
            Assert.assertNull(col.getStrB(2));
        });
    }

    @Test
    public void testColumnViewStringHeapAllocated() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            setupVarcharColumnLayout(batch, 0, "s",
                    new String[]{"alpha", null, "gamma"},
                    new boolean[]{false, true, false});
            ColumnView col = batch.column(0);
            Assert.assertEquals("alpha", col.getString(0));
            Assert.assertNull(col.getString(1));
            Assert.assertEquals("gamma", col.getString(2));
        });
    }

    @Test
    public void testColumnViewStringSink() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            setupVarcharColumnLayout(batch, 0, "s",
                    new String[]{"alpha", null, "gamma"},
                    new boolean[]{false, true, false});
            ColumnView col = batch.column(0);
            StringSink sink = new StringSink();

            Assert.assertTrue(col.getString(0, sink));
            Assert.assertEquals("alpha", sink.toString());
            sink.clear();

            Assert.assertFalse(col.getString(1, sink));
            Assert.assertEquals(0, sink.length());

            Assert.assertTrue(col.getString(2, sink));
            Assert.assertEquals("gamma", sink.toString());
        });
    }

    @Test
    public void testColumnViewSymbolAccessors() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 5);
            String[] dict = {"AAPL", "MSFT", "GOOG"};
            int[] rowIds = {0, 1, 0, 2, -1};
            setupSymbolColumnLayout(batch, dict, rowIds);

            ColumnView col = batch.column(0);
            Assert.assertEquals(3, col.symbolDictSize());
            Assert.assertNotEquals(0L, col.symbolDictHeapAddr());
            Assert.assertNotEquals(0L, col.symbolDictEntriesAddr());
            Assert.assertSame(rowIds, col.symbolRowIds());

            Assert.assertEquals("AAPL", col.getSymbol(0));
            Assert.assertEquals("MSFT", col.getSymbol(1));
            Assert.assertEquals("AAPL", col.getSymbol(2));
            Assert.assertEquals("GOOG", col.getSymbol(3));
            Assert.assertNull(col.getSymbol(4));

            Assert.assertEquals(0, col.getSymbolId(0));
            Assert.assertEquals(1, col.getSymbolId(1));
            Assert.assertEquals(0, col.getSymbolId(2));
            Assert.assertEquals(2, col.getSymbolId(3));
            Assert.assertEquals(-1, col.getSymbolId(4));

            Assert.assertSame(col.getSymbol(0), col.getSymbol(2));
        });
    }

    @Test
    public void testColumnViewUuidLoHi() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 2);
            long[] lo = {0xCAFE_BABEL, 0L};
            long[] hi = {0xDEAD_BEEFL, 0L};
            setupUuidColumnLayout(batch, lo, hi, new boolean[]{false, true});
            ColumnView col = batch.column(0);
            Assert.assertEquals(0xCAFE_BABEL, col.getUuidLo(0));
            Assert.assertEquals(0xDEAD_BEEFL, col.getUuidHi(0));
            Assert.assertEquals(0L, col.getUuidLo(1));    // NULL
            Assert.assertEquals(0L, col.getUuidHi(1));
        });
    }

    @Test
    public void testColumnViewUuidWithSink() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 2);
            long[] lo = {0x1111_1111_1111_1111L, 0L};
            long[] hi = {0x2222_2222_2222_2222L, 0L};
            setupUuidColumnLayout(batch, lo, hi, new boolean[]{false, true});
            ColumnView col = batch.column(0);
            Uuid sink = new Uuid();
            Assert.assertTrue(col.getUuid(0, sink));
            Assert.assertEquals(0x1111_1111_1111_1111L, sink.getLo());
            Assert.assertEquals(0x2222_2222_2222_2222L, sink.getHi());
            Assert.assertFalse(col.getUuid(1, sink));
        });
    }

    @Test
    public void testColumnViewValuesAddrMatchesLayout() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(2, 1);
            Object lLayout = setupLongColumnLayout(batch, 0, "l", new long[]{1L}, new boolean[]{false});
            Object dLayout = setupDoubleColumnLayout(batch, 1, new double[]{2.0}, new boolean[]{false});
            Assert.assertEquals((long) readField(lLayout, "valuesAddr"), batch.column(0).valuesAddr());
            Assert.assertEquals((long) readField(dLayout, "valuesAddr"), batch.column(1).valuesAddr());
        });
    }

    @Test
    public void testColumnViewVarcharAndStringBytesAddr() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            setupVarcharColumnLayout(batch, 0, "v",
                    new String[]{"hello", "world", null},
                    new boolean[]{false, false, true});
            ColumnView col = batch.column(0);
            Assert.assertNotEquals(0L, col.valuesAddr());
            Assert.assertNotEquals(0L, col.stringBytesAddr());
            // getStrA re-points the same view -- read it before reading row 1.
            Assert.assertEquals("hello", Utf8s.stringFromUtf8Bytes(col.getStrA(0)));
            Assert.assertEquals("world", Utf8s.stringFromUtf8Bytes(col.getStrA(1)));
            Assert.assertNull(col.getStrA(2));
            Assert.assertEquals(2, col.nonNullCount());
        });
    }

    @Test
    public void testForEachRowEmptyBatch() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 0);
            // Register a minimal layout so column() doesn't trip on null, though
            // forEachRow never reaches into it.
            Object l = newLayoutInstance();
            writeField(l, "info", newColumnInfo("l", QwpConstants.TYPE_LONG));
            writeField(l, "valuesAddr", 0L);
            writeField(l, "nullBitmapAddr", 0L);
            writeField(l, "nonNullCount", 0);
            setLayoutInBatch(batch, 0, l);

            int[] callbackCount = {0};
            batch.forEachRow(row -> callbackCount[0]++);
            Assert.assertEquals(0, callbackCount[0]);
        });
    }

    @Test
    public void testForEachRowExceptionPropagates() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 5);
            setupLongColumnLayout(batch, 0, "l",
                    new long[]{1L, 2L, 3L, 4L, 5L},
                    new boolean[]{false, false, false, false, false});
            int[] visited = {0};
            try {
                batch.forEachRow(row -> {
                    visited[0]++;
                    if (row.getRowIndex() == 2) throw new RuntimeException("boom");
                });
                Assert.fail("expected RuntimeException to propagate");
            } catch (RuntimeException expected) {
                Assert.assertEquals("boom", expected.getMessage());
            }
            Assert.assertEquals(3, visited[0]);
        });
    }

    @Test
    public void testForEachRowReusesSameInstance() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 4);
            setupLongColumnLayout(batch, 0, "l",
                    new long[]{1L, 2L, 3L, 4L},
                    new boolean[]{false, false, false, false});
            IdentityHashMap<RowView, Boolean> seen = new IdentityHashMap<>();
            batch.forEachRow(row -> seen.put(row, Boolean.TRUE));
            Assert.assertEquals(1, seen.size());
        });
    }

    @Test
    public void testForEachRowVisitsRowsInOrder() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 5);
            setupLongColumnLayout(batch, 0, "l",
                    new long[]{10L, 20L, 30L, 40L, 50L},
                    new boolean[]{false, false, false, false, false});
            List<Long> values = new ArrayList<>();
            int[] indices = new int[5];
            int[] cursor = {0};
            batch.forEachRow(row -> {
                indices[cursor[0]++] = row.getRowIndex();
                values.add(row.getLongValue(0));
            });
            Assert.assertEquals(5, values.size());
            Assert.assertArrayEquals(new int[]{0, 1, 2, 3, 4}, indices);
            Assert.assertEquals(Long.valueOf(10L), values.get(0));
            Assert.assertEquals(Long.valueOf(50L), values.get(4));
        });
    }

    @Test
    public void testRowViewArrayAccessors() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            setupArrayColumnLayout(batch,
                    new boolean[]{false, true, false},
                    new double[][]{{1.0, 2.0}, null, {3.0, 4.0, 5.0}});
            Assert.assertEquals(1, batch.row(0).getArrayNDims(0));
            Assert.assertEquals(0, batch.row(1).getArrayNDims(0));   // NULL
            Assert.assertArrayEquals(new double[]{1.0, 2.0}, batch.row(0).getDoubleArrayElements(0), 0.0);
            Assert.assertNull(batch.row(1).getDoubleArrayElements(0));
            Assert.assertArrayEquals(new double[]{3.0, 4.0, 5.0}, batch.row(2).getDoubleArrayElements(0), 0.0);
        });
    }

    @Test
    public void testRowViewBatchAccessor() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 1);
            setupLongColumnLayout(batch, 0, "x", new long[]{42L}, new boolean[]{false});
            Assert.assertSame(batch, batch.row(0).batch());
        });
    }

    @Test
    public void testRowViewBinaryAccessor() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 2);
            setupBinaryColumnLayout(batch,
                    new byte[][]{{0x00, 0x7F, (byte) 0xFF}, null},
                    new boolean[]{false, true});
            RowView row = batch.row(0);
            DirectByteSequence v0 = row.getBinaryA(0);
            Assert.assertNotNull(v0);
            Assert.assertEquals(3, v0.size());
            Assert.assertEquals((byte) 0x00, v0.byteAt(0));
            Assert.assertEquals((byte) 0x7F, v0.byteAt(1));
            Assert.assertEquals((byte) 0xFF, v0.byteAt(2));
            Assert.assertNull(batch.row(1).getBinaryA(0));
            byte[] copy = batch.row(0).getBinary(0);
            Assert.assertArrayEquals(new byte[]{0x00, 0x7F, (byte) 0xFF}, copy);
        });
    }

    @Test
    public void testRowViewBinaryBDualHold() throws Exception {
        // binaryA and binaryB are independent slots, parallel to strA/strB.
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 2);
            setupBinaryColumnLayout(batch,
                    new byte[][]{{0x01, 0x02}, {(byte) 0xFE, (byte) 0xFF}},
                    new boolean[]{false, false});
            DirectByteSequence a = batch.row(0).getBinaryA(0);
            DirectByteSequence b = batch.row(1).getBinaryB(0);
            Assert.assertNotNull(a);
            Assert.assertNotNull(b);
            // Both stay valid concurrently.
            Assert.assertEquals(2, a.size());
            Assert.assertEquals((byte) 0x01, a.byteAt(0));
            Assert.assertEquals(2, b.size());
            Assert.assertEquals((byte) 0xFF, b.byteAt(1));
        });
    }

    @Test
    public void testRowViewByteAndShortAndCharAndFloat() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(4, 2);
            setupByteColumnLayout(batch, 0, new byte[]{(byte) 127, 0}, new boolean[]{false, true});
            setupShortColumnLayout(batch, 1, new short[]{(short) -32000, 0}, new boolean[]{false, true});
            setupCharColumnLayout(batch, 2, new char[]{'Q', 0}, new boolean[]{false, true});
            setupFloatColumnLayout(batch, 3, new float[]{3.25f, 0f}, new boolean[]{false, true});

            RowView row0 = batch.row(0);
            Assert.assertEquals((byte) 127, row0.getByteValue(0));
            Assert.assertEquals((short) -32000, row0.getShortValue(1));
            Assert.assertEquals('Q', row0.getCharValue(2));
            Assert.assertEquals(3.25f, row0.getFloatValue(3), 0f);

            // NULL row: type-specific sentinels via the row facade.
            RowView row1 = batch.row(1);
            Assert.assertEquals(0, row1.getByteValue(0));
            Assert.assertEquals(0, row1.getShortValue(1));
            Assert.assertEquals(0, row1.getCharValue(2));
            Assert.assertTrue(Float.isNaN(row1.getFloatValue(3)));
        });
    }

    @Test
    public void testRowViewDecimal128() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 2);
            long[] lo = {0x1122_3344_5566_7788L, 0L};
            long[] hi = {0x99AA_BBCC_DDEE_FF00L, 0L};
            setupDecimal128ColumnLayout(batch, lo, hi, new boolean[]{false, true}, 4);
            Assert.assertEquals(0x1122_3344_5566_7788L, batch.row(0).getDecimal128Low(0));
            Assert.assertEquals(0x99AA_BBCC_DDEE_FF00L, batch.row(0).getDecimal128High(0));
            Assert.assertEquals(0L, batch.row(1).getDecimal128Low(0));
            Assert.assertEquals(0L, batch.row(1).getDecimal128High(0));
        });
    }

    @Test
    public void testRowViewDelegatesAgreeWithBatchPrimitives() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(5, 4);
            setupLongColumnLayout(batch, 0, "l", new long[]{1L, 2L, 0L, 4L}, new boolean[]{false, false, true, false});
            setupIntColumnLayout(batch, 1, new int[]{10, 20, 0, 40}, new boolean[]{false, false, true, false});
            setupDoubleColumnLayout(batch, 2, new double[]{1.5, 2.5, 0.0, 4.5}, new boolean[]{false, false, true, false});
            setupBooleanColumnLayout(batch, 3, new boolean[]{true, false, false, true}, new boolean[]{false, false, true, false});
            setupVarcharColumnLayout(batch, 4, "s", new String[]{"a", "bb", null, "dddd"}, new boolean[]{false, false, true, false});

            for (int r = 0; r < 4; r++) {
                RowView row = batch.row(r);
                for (int c = 0; c < 5; c++) {
                    Assert.assertEquals("isNull(c=" + c + ", r=" + r + ")", batch.isNull(c, r), row.isNull(c));
                }
                Assert.assertEquals(batch.getLongValue(0, r), row.getLongValue(0));
                Assert.assertEquals(batch.getIntValue(1, r), row.getIntValue(1));
                Assert.assertEquals(batch.getDoubleValue(2, r), row.getDoubleValue(2), 0.0);
                Assert.assertEquals(batch.getBoolValue(3, r), row.getBoolValue(3));
            }
        });
    }

    @Test
    public void testRowViewGeohashValue() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 2);
            setupGeohashColumnLayout(batch, 0, "g", new long[]{0xDEAD_BEEFL, 0L}, new boolean[]{false, true}, 32);
            Assert.assertEquals(0xDEAD_BEEFL, batch.row(0).getGeohashValue(0));
            Assert.assertEquals(0L, batch.row(1).getGeohashValue(0));
        });
    }

    @Test
    public void testRowViewGetRowIndex() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 5);
            setupLongColumnLayout(batch, 0, "l",
                    new long[]{0L, 0L, 0L, 0L, 0L},
                    new boolean[]{false, false, false, false, false});
            RowView row = batch.row(3);
            Assert.assertEquals(3, row.getRowIndex());
            row.of(0);
            Assert.assertEquals(0, row.getRowIndex());
            row.of(4);
            Assert.assertEquals(4, row.getRowIndex());
        });
    }

    @Test
    public void testRowViewLong256WithSink() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 2);
            long[][] words = {{0x1L, 0x2L, 0x3L, 0x4L}, {0L, 0L, 0L, 0L}};
            setupLong256ColumnLayout(batch, words, new boolean[]{false, true});
            Long256Impl sink = new Long256Impl();

            Assert.assertTrue(batch.row(0).getLong256(0, sink));
            Assert.assertEquals(0x1L, sink.getLong0());
            Assert.assertEquals(0x2L, sink.getLong1());
            Assert.assertEquals(0x3L, sink.getLong2());
            Assert.assertEquals(0x4L, sink.getLong3());

            Assert.assertFalse(batch.row(1).getLong256(0, sink));
        });
    }

    @Test
    public void testRowViewLong256Word() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 2);
            long[][] words = {{0x11L, 0x22L, 0x33L, 0x44L}, {0L, 0L, 0L, 0L}};
            setupLong256ColumnLayout(batch, words, new boolean[]{false, true});
            RowView row = batch.row(0);
            Assert.assertEquals(0x11L, row.getLong256Word(0, 0));
            Assert.assertEquals(0x22L, row.getLong256Word(0, 1));
            Assert.assertEquals(0x33L, row.getLong256Word(0, 2));
            Assert.assertEquals(0x44L, row.getLong256Word(0, 3));
            Assert.assertEquals(0L, batch.row(1).getLong256Word(0, 0));    // NULL -> 0
        });
    }

    @Test
    public void testRowViewOfReturnsThis() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 2);
            setupLongColumnLayout(batch, 0, "l", new long[]{7L, 8L}, new boolean[]{false, false});
            RowView v = batch.row(0);
            Assert.assertSame(v, v.of(1));
            Assert.assertEquals(8L, v.getLongValue(0));
        });
    }

    @Test
    public void testRowViewSingleSharedInstance() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            setupLongColumnLayout(batch, 0, "l", new long[]{1L, 2L, 3L}, new boolean[]{false, false, false});
            RowView a = batch.row(0);
            RowView b = batch.row(1);
            RowView c = batch.row(2);
            Assert.assertSame(a, b);
            Assert.assertSame(b, c);
            Assert.assertEquals(2, a.getRowIndex());
            Assert.assertEquals(3L, a.getLongValue(0));
        });
    }

    @Test
    public void testRowViewStrAStrBDualHold() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 2);
            setupVarcharColumnLayout(batch, 0, "s",
                    new String[]{"alpha", "beta"},
                    new boolean[]{false, false});
            RowView row = batch.row(0);
            DirectUtf8Sequence a = row.getStrA(0);
            DirectUtf8Sequence b = batch.row(1).getStrB(0);
            Assert.assertNotNull(a);
            Assert.assertNotNull(b);
            Assert.assertEquals("alpha", Utf8s.stringFromUtf8Bytes(a));
            Assert.assertEquals("beta", Utf8s.stringFromUtf8Bytes(b));
        });
    }

    @Test
    public void testRowViewStringAccessors() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 3);
            setupVarcharColumnLayout(batch, 0, "s",
                    new String[]{"alpha", null, "gamma"},
                    new boolean[]{false, true, false});
            // Heap-allocating variant
            Assert.assertEquals("alpha", batch.row(0).getString(0));
            Assert.assertNull(batch.row(1).getString(0));
            Assert.assertEquals("gamma", batch.row(2).getString(0));
            // Sink variant
            StringSink sink = new StringSink();
            Assert.assertTrue(batch.row(0).getString(0, sink));
            Assert.assertEquals("alpha", sink.toString());
            sink.clear();
            Assert.assertFalse(batch.row(1).getString(0, sink));
            Assert.assertEquals(0, sink.length());
            Assert.assertTrue(batch.row(2).getString(0, sink));
            Assert.assertEquals("gamma", sink.toString());
        });
    }

    @Test
    public void testRowViewSymbolAccessors() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 4);
            String[] dict = {"AAPL", "MSFT"};
            int[] rowIds = {0, 1, 0, -1};
            setupSymbolColumnLayout(batch, dict, rowIds);
            Assert.assertEquals("AAPL", batch.row(0).getSymbol(0));
            Assert.assertEquals("MSFT", batch.row(1).getSymbol(0));
            Assert.assertEquals("AAPL", batch.row(2).getSymbol(0));
            Assert.assertNull(batch.row(3).getSymbol(0));
            Assert.assertEquals(0, batch.row(0).getSymbolId(0));
            Assert.assertEquals(1, batch.row(1).getSymbolId(0));
            Assert.assertEquals(0, batch.row(2).getSymbolId(0));
            Assert.assertEquals(-1, batch.row(3).getSymbolId(0));
        });
    }

    // ---- Reflection + setup helpers ----

    @Test
    public void testRowViewUuidLoHi() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 2);
            long[] lo = {0xCAFE_BABEL, 0L};
            long[] hi = {0xDEAD_BEEFL, 0L};
            setupUuidColumnLayout(batch, lo, hi, new boolean[]{false, true});
            Assert.assertEquals(0xCAFE_BABEL, batch.row(0).getUuidLo(0));
            Assert.assertEquals(0xDEAD_BEEFL, batch.row(0).getUuidHi(0));
            Assert.assertEquals(0L, batch.row(1).getUuidLo(0));
            Assert.assertEquals(0L, batch.row(1).getUuidHi(0));
        });
    }

    @Test
    public void testRowViewUuidWithSink() throws Exception {
        assertMemoryLeak(() -> {
            QwpColumnBatch batch = newBatch(1, 2);
            long[] lo = {0xAAAAL, 0L};
            long[] hi = {0xBBBBL, 0L};
            setupUuidColumnLayout(batch, lo, hi, new boolean[]{false, true});
            Uuid sink = new Uuid();
            Assert.assertTrue(batch.row(0).getUuid(0, sink));
            Assert.assertEquals(0xAAAAL, sink.getLo());
            Assert.assertEquals(0xBBBBL, sink.getHi());
            Assert.assertFalse(batch.row(1).getUuid(0, sink));
        });
    }

    private static int countNonNull(boolean[] nulls) {
        int n = 0;
        for (boolean b : nulls) if (!b) n++;
        return n;
    }

    private static Field findField(Class<?> cls, String name) throws NoSuchFieldException {
        Class<?> c = cls;
        while (c != null) {
            try {
                return c.getDeclaredField(name);
            } catch (NoSuchFieldException ignore) {
                c = c.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }

    private static Object newColumnInfo(String name, byte wireType) throws Exception {
        Class<?> cls = Class.forName(COLUMN_INFO);
        java.lang.reflect.Constructor<?> ctor = cls.getDeclaredConstructor();
        ctor.setAccessible(true);
        Object info = ctor.newInstance();
        java.lang.reflect.Method of = cls.getDeclaredMethod("of", String.class, byte.class);
        of.setAccessible(true);
        of.invoke(info, name, wireType);
        return info;
    }

    private static Object newLayoutInstance() throws Exception {
        java.lang.reflect.Constructor<?> ctor = Class.forName(COLUMN_LAYOUT).getDeclaredConstructor();
        ctor.setAccessible(true);
        return ctor.newInstance();
    }

    private static Object readField(Object target, String name) throws Exception {
        Field f = findField(target.getClass(), name);
        f.setAccessible(true);
        return f.get(target);
    }

    private static void writeField(Object target, String name, Object value) throws Exception {
        Field f = findField(target.getClass(), name);
        f.setAccessible(true);
        f.set(target, value);
    }

    private static void writeField(Object target, String name, long value) throws Exception {
        Field f = findField(target.getClass(), name);
        f.setAccessible(true);
        f.setLong(target, value);
    }

    private static void writeField(Object target, String name, int value) throws Exception {
        Field f = findField(target.getClass(), name);
        f.setAccessible(true);
        f.setInt(target, value);
    }

    private long allocate(int bytes) {
        long addr = Unsafe.malloc(bytes, MemoryTag.NATIVE_DEFAULT);
        allocations.add(new long[]{addr, bytes});
        return addr;
    }

    private void applyNullBitmap(Object layout, int rowCount, boolean[] nulls) throws Exception {
        boolean hasNulls = false;
        for (boolean n : nulls)
            if (n) {
                hasNulls = true;
                break;
            }
        if (!hasNulls) {
            writeField(layout, "nullBitmapAddr", 0L);
            writeField(layout, "nonNullIdx", null);
            return;
        }
        int bitmapBytes = (rowCount + 7) >>> 3;
        long bitmap = allocate(bitmapBytes);
        for (int i = 0; i < bitmapBytes; i++) Unsafe.getUnsafe().putByte(bitmap + i, (byte) 0);
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r]) {
                long addr = bitmap + (r >>> 3);
                Unsafe.getUnsafe().putByte(addr, (byte) (Unsafe.getUnsafe().getByte(addr) | (1 << (r & 7))));
            }
        }
        int[] nonNullIdx = new int[rowCount];
        int dense = 0;
        for (int r = 0; r < rowCount; r++) {
            nonNullIdx[r] = nulls[r] ? -1 : dense++;
        }
        writeField(layout, "nullBitmapAddr", bitmap);
        writeField(layout, "nonNullIdx", nonNullIdx);
    }

    private QwpColumnBatch newBatch(int columnCount, int rowCount) throws Exception {
        QwpColumnBatch batch = new QwpColumnBatch();
        writeField(batch, "columnCount", columnCount);
        writeField(batch, "rowCount", rowCount);

        // Populate a matching ObjList<QwpEgressColumnInfo> so the batch's public
        // getters (getColumnName, getColumnWireType) don't NPE. Each slot is
        // filled by the per-column setup helper.
        Class<?> objListCls = Class.forName("io.questdb.client.std.ObjList");
        Object columnsList = objListCls.getDeclaredConstructor().newInstance();
        for (int i = 0; i < columnCount; i++) {
            objListCls.getMethod("add", Object.class).invoke(columnsList, (Object) null);
        }
        writeField(batch, "columns", columnsList);

        Object layouts = readField(batch, "columnLayouts");
        objListCls.getMethod("setPos", int.class).invoke(layouts, columnCount);
        return batch;
    }

    private void setColumnInfoInBatch(QwpColumnBatch batch, int colIdx, Object info) throws Exception {
        Object columns = readField(batch, "columns");
        Class<?> objListCls = Class.forName("io.questdb.client.std.ObjList");
        objListCls.getMethod("setQuick", int.class, Object.class).invoke(columns, colIdx, info);
    }

    private void setLayoutInBatch(QwpColumnBatch batch, int colIdx, Object layout) throws Exception {
        Object layouts = readField(batch, "columnLayouts");
        Class<?> objListCls = Class.forName("io.questdb.client.std.ObjList");
        objListCls.getMethod("setQuick", int.class, Object.class).invoke(layouts, colIdx, layout);
    }

    private Object setupArrayColumnLayout(QwpColumnBatch batch, boolean[] nulls, double[][] arrays) throws Exception {
        int rowCount = arrays.length;
        int colIdx = 0;
        Object l = newLayoutInstance();
        Object info = newColumnInfo("a", QwpConstants.TYPE_DOUBLE_ARRAY);
        writeField(l, "info", info);
        long[] perRowAddr = new long[rowCount];
        int[] perRowLen = new int[rowCount];
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r] || arrays[r] == null) {
                perRowAddr[r] = -1L;
                continue;
            }
            int len = 1 + 4 + 8 * arrays[r].length;
            long addr = allocate(len);
            // 1-D layout: 1 byte ndims (=1), 1 x int32 dim size, then flat doubles.
            Unsafe.getUnsafe().putByte(addr, (byte) 1);
            Unsafe.getUnsafe().putInt(addr + 1, arrays[r].length);
            for (int i = 0; i < arrays[r].length; i++) {
                Unsafe.getUnsafe().putDouble(addr + 5 + 8L * i, arrays[r][i]);
            }
            perRowAddr[r] = addr;
            perRowLen[r] = len;
        }
        writeField(l, "arrayRowAddr", perRowAddr);
        writeField(l, "arrayRowLen", perRowLen);
        writeField(l, "valuesAddr", 0L);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", countNonNull(nulls));
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
        return l;
    }

    private void setupBinaryColumnLayout(QwpColumnBatch batch, byte[][] values, boolean[] nulls) throws Exception {
        int rowCount = values.length;
        int nonNull = countNonNull(nulls);
        int colIdx = 0;
        Object l = newLayoutInstance();
        Object info = newColumnInfo("b", QwpConstants.TYPE_BINARY);
        writeField(l, "info", info);
        long offsetsAddr = allocate((nonNull + 1) * 4);
        int totalBytes = 0;
        for (int r = 0; r < rowCount; r++) {
            if (!nulls[r] && values[r] != null) totalBytes += values[r].length;
        }
        long bytesAddr = allocate(Math.max(1, totalBytes));
        int dense = 0, byteOff = 0;
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r] || values[r] == null) continue;
            Unsafe.getUnsafe().putInt(offsetsAddr + 4L * dense, byteOff);
            for (int i = 0; i < values[r].length; i++) {
                Unsafe.getUnsafe().putByte(bytesAddr + byteOff + i, values[r][i]);
            }
            byteOff += values[r].length;
            dense++;
        }
        Unsafe.getUnsafe().putInt(offsetsAddr + 4L * dense, byteOff);
        writeField(l, "valuesAddr", offsetsAddr);
        writeField(l, "stringBytesAddr", bytesAddr);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", nonNull);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
    }

    private void setupBooleanColumnLayout(QwpColumnBatch batch, int colIdx,
                                          boolean[] values, boolean[] nulls) throws Exception {
        int rowCount = values.length;
        int nonNull = countNonNull(nulls);
        Object l = newLayoutInstance();
        Object info = newColumnInfo("b", QwpConstants.TYPE_BOOLEAN);
        writeField(l, "info", info);
        int bytes = Math.max(1, (nonNull + 7) >>> 3);
        long addr = allocate(bytes);
        for (int i = 0; i < bytes; i++) Unsafe.getUnsafe().putByte(addr + i, (byte) 0);
        int dense = 0;
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r]) continue;
            if (values[r]) {
                long byteAddr = addr + (dense >>> 3);
                Unsafe.getUnsafe().putByte(byteAddr,
                        (byte) (Unsafe.getUnsafe().getByte(byteAddr) | (1 << (dense & 7))));
            }
            dense++;
        }
        writeField(l, "valuesAddr", addr);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", nonNull);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
    }

    private void setupByteColumnLayout(QwpColumnBatch batch, int colIdx,
                                       byte[] values, boolean[] nulls) throws Exception {
        int rowCount = values.length;
        int nonNull = countNonNull(nulls);
        Object l = newLayoutInstance();
        Object info = newColumnInfo("y", QwpConstants.TYPE_BYTE);
        writeField(l, "info", info);
        long addr = allocate(Math.max(1, nonNull));
        int dense = 0;
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r]) continue;
            Unsafe.getUnsafe().putByte(addr + dense, values[r]);
            dense++;
        }
        writeField(l, "valuesAddr", addr);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", nonNull);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
    }

    private void setupCharColumnLayout(QwpColumnBatch batch, int colIdx,
                                       char[] values, boolean[] nulls) throws Exception {
        int rowCount = values.length;
        int nonNull = countNonNull(nulls);
        Object l = newLayoutInstance();
        Object info = newColumnInfo("c", QwpConstants.TYPE_CHAR);
        writeField(l, "info", info);
        long addr = allocate(Math.max(2, nonNull * 2));
        int dense = 0;
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r]) continue;
            Unsafe.getUnsafe().putShort(addr + 2L * dense, (short) values[r]);
            dense++;
        }
        writeField(l, "valuesAddr", addr);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", nonNull);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
    }

    private void setupDecimal128ColumnLayout(QwpColumnBatch batch,
                                             long[] lo, long[] hi, boolean[] nulls, int scale) throws Exception {
        int rowCount = lo.length;
        int nonNull = countNonNull(nulls);
        int colIdx = 0;
        Object l = newLayoutInstance();
        Object info = newColumnInfo("d", QwpConstants.TYPE_DECIMAL128);
        writeField(info, "scale", scale);
        writeField(l, "info", info);
        long addr = allocate(Math.max(16, nonNull * 16));
        int dense = 0;
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r]) continue;
            // Matching the batch's layout: low 64 bits at offset 0, high 64 at offset +8.
            Unsafe.getUnsafe().putLong(addr + 16L * dense, lo[r]);
            Unsafe.getUnsafe().putLong(addr + 16L * dense + 8L, hi[r]);
            dense++;
        }
        writeField(l, "valuesAddr", addr);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", nonNull);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
    }

    private Object setupDoubleColumnLayout(QwpColumnBatch batch, int colIdx,
                                           double[] values, boolean[] nulls) throws Exception {
        int rowCount = values.length;
        int nonNull = countNonNull(nulls);
        Object l = newLayoutInstance();
        Object info = newColumnInfo("d", QwpConstants.TYPE_DOUBLE);
        writeField(l, "info", info);
        long addr = allocate(Math.max(8, nonNull * 8));
        int dense = 0;
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r]) continue;
            Unsafe.getUnsafe().putDouble(addr + 8L * dense, values[r]);
            dense++;
        }
        writeField(l, "valuesAddr", addr);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", nonNull);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
        return l;
    }

    private void setupFloatColumnLayout(QwpColumnBatch batch, int colIdx,
                                        float[] values, boolean[] nulls) throws Exception {
        int rowCount = values.length;
        int nonNull = countNonNull(nulls);
        Object l = newLayoutInstance();
        Object info = newColumnInfo("f", QwpConstants.TYPE_FLOAT);
        writeField(l, "info", info);
        long addr = allocate(Math.max(4, nonNull * 4));
        int dense = 0;
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r]) continue;
            Unsafe.getUnsafe().putFloat(addr + 4L * dense, values[r]);
            dense++;
        }
        writeField(l, "valuesAddr", addr);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", nonNull);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
    }

    private void setupGeohashColumnLayout(QwpColumnBatch batch, int colIdx, String name,
                                          long[] values, boolean[] nulls, int precisionBits) throws Exception {
        int rowCount = values.length;
        int nonNull = countNonNull(nulls);
        int bytesPerValue = (precisionBits + 7) >>> 3;
        Object l = newLayoutInstance();
        Object info = newColumnInfo(name, QwpConstants.TYPE_GEOHASH);
        writeField(info, "precisionBits", precisionBits);
        writeField(l, "info", info);
        long addr = allocate(Math.max(1, nonNull * bytesPerValue));
        int dense = 0;
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r]) continue;
            long v = values[r];
            // Little-endian LSB-first packing; matches QwpColumnBatch.getGeohashValue.
            for (int b = 0; b < bytesPerValue; b++) {
                Unsafe.getUnsafe().putByte(addr + (long) bytesPerValue * dense + b, (byte) ((v >>> (b * 8)) & 0xFF));
            }
            dense++;
        }
        writeField(l, "valuesAddr", addr);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", nonNull);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
    }

    private void setupIntColumnLayout(QwpColumnBatch batch, int colIdx,
                                      int[] values, boolean[] nulls) throws Exception {
        int rowCount = values.length;
        int nonNull = countNonNull(nulls);
        Object l = newLayoutInstance();
        Object info = newColumnInfo("i", QwpConstants.TYPE_INT);
        writeField(l, "info", info);
        long addr = allocate(Math.max(4, nonNull * 4));
        int dense = 0;
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r]) continue;
            Unsafe.getUnsafe().putInt(addr + 4L * dense, values[r]);
            dense++;
        }
        writeField(l, "valuesAddr", addr);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", nonNull);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
    }

    private void setupLong256ColumnLayout(QwpColumnBatch batch, long[][] words4, boolean[] nulls) throws Exception {
        int rowCount = words4.length;
        int nonNull = countNonNull(nulls);
        int colIdx = 0;
        Object l = newLayoutInstance();
        Object info = newColumnInfo("x", QwpConstants.TYPE_LONG256);
        writeField(l, "info", info);
        long addr = allocate(Math.max(32, nonNull * 32));
        int dense = 0;
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r]) continue;
            for (int w = 0; w < 4; w++) {
                Unsafe.getUnsafe().putLong(addr + 32L * dense + 8L * w, words4[r][w]);
            }
            dense++;
        }
        writeField(l, "valuesAddr", addr);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", nonNull);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
    }

    private Object setupLongColumnLayout(QwpColumnBatch batch, int colIdx, String name,
                                         long[] values, boolean[] nulls) throws Exception {
        int rowCount = values.length;
        int nonNull = countNonNull(nulls);
        Object l = newLayoutInstance();
        Object info = newColumnInfo(name, QwpConstants.TYPE_LONG);
        writeField(l, "info", info);
        long addr = allocate(Math.max(8, nonNull * 8));
        int dense = 0;
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r]) continue;
            Unsafe.getUnsafe().putLong(addr + 8L * dense, values[r]);
            dense++;
        }
        writeField(l, "valuesAddr", addr);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", nonNull);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
        return l;
    }

    private void setupShortColumnLayout(QwpColumnBatch batch, int colIdx,
                                        short[] values, boolean[] nulls) throws Exception {
        int rowCount = values.length;
        int nonNull = countNonNull(nulls);
        Object l = newLayoutInstance();
        Object info = newColumnInfo("s", QwpConstants.TYPE_SHORT);
        writeField(l, "info", info);
        long addr = allocate(Math.max(2, nonNull * 2));
        int dense = 0;
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r]) continue;
            Unsafe.getUnsafe().putShort(addr + 2L * dense, values[r]);
            dense++;
        }
        writeField(l, "valuesAddr", addr);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", nonNull);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
    }

    private void setupSymbolColumnLayout(QwpColumnBatch batch, String[] dict, int[] rowIds) throws Exception {
        int rowCount = rowIds.length;
        int colIdx = 0;
        Object l = newLayoutInstance();
        Object info = newColumnInfo("sym", QwpConstants.TYPE_SYMBOL);
        writeField(l, "info", info);
        int totalBytes = 0;
        byte[][] encoded = new byte[dict.length][];
        for (int i = 0; i < dict.length; i++) {
            encoded[i] = dict[i].getBytes(StandardCharsets.UTF_8);
            totalBytes += encoded[i].length;
        }
        long heapAddr = allocate(Math.max(1, totalBytes));
        long entriesAddr = allocate(dict.length * 8);
        int off = 0;
        for (int i = 0; i < dict.length; i++) {
            for (int b = 0; b < encoded[i].length; b++) {
                Unsafe.getUnsafe().putByte(heapAddr + off + b, encoded[i][b]);
            }
            long packed = ((long) off & 0xFFFF_FFFFL) | ((long) encoded[i].length << 32);
            Unsafe.getUnsafe().putLong(entriesAddr + 8L * i, packed);
            off += encoded[i].length;
        }
        writeField(l, "symbolDictHeapAddr", heapAddr);
        writeField(l, "symbolDictEntriesAddr", entriesAddr);
        writeField(l, "symbolDictSize", dict.length);
        writeField(l, "symbolRowIds", rowIds);
        boolean[] nulls = new boolean[rowCount];
        for (int r = 0; r < rowCount; r++) nulls[r] = rowIds[r] < 0;
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", countNonNull(nulls));
        writeField(l, "valuesAddr", 0L);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
    }

    private void setupUuidColumnLayout(QwpColumnBatch batch, long[] lo, long[] hi, boolean[] nulls) throws Exception {
        int rowCount = lo.length;
        int nonNull = countNonNull(nulls);
        int colIdx = 0;
        Object l = newLayoutInstance();
        Object info = newColumnInfo("u", QwpConstants.TYPE_UUID);
        writeField(l, "info", info);
        long addr = allocate(Math.max(16, nonNull * 16));
        int dense = 0;
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r]) continue;
            Unsafe.getUnsafe().putLong(addr + 16L * dense, lo[r]);
            Unsafe.getUnsafe().putLong(addr + 16L * dense + 8L, hi[r]);
            dense++;
        }
        writeField(l, "valuesAddr", addr);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", nonNull);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
    }

    private void setupVarcharColumnLayout(QwpColumnBatch batch, int colIdx, String name,
                                          String[] values, boolean[] nulls) throws Exception {
        int rowCount = values.length;
        int nonNull = countNonNull(nulls);
        Object l = newLayoutInstance();
        Object info = newColumnInfo(name, QwpConstants.TYPE_VARCHAR);
        writeField(l, "info", info);
        long offsetsAddr = allocate((nonNull + 1) * 4);
        int totalBytes = 0;
        byte[][] encoded = new byte[rowCount][];
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r] || values[r] == null) continue;
            encoded[r] = values[r].getBytes(StandardCharsets.UTF_8);
            totalBytes += encoded[r].length;
        }
        long bytesAddr = allocate(Math.max(1, totalBytes));
        int dense = 0, byteOff = 0;
        for (int r = 0; r < rowCount; r++) {
            if (nulls[r] || values[r] == null) continue;
            Unsafe.getUnsafe().putInt(offsetsAddr + 4L * dense, byteOff);
            for (int i = 0; i < encoded[r].length; i++) {
                Unsafe.getUnsafe().putByte(bytesAddr + byteOff + i, encoded[r][i]);
            }
            byteOff += encoded[r].length;
            dense++;
        }
        Unsafe.getUnsafe().putInt(offsetsAddr + 4L * dense, byteOff);
        writeField(l, "valuesAddr", offsetsAddr);
        writeField(l, "stringBytesAddr", bytesAddr);
        applyNullBitmap(l, rowCount, nulls);
        writeField(l, "nonNullCount", nonNull);
        setLayoutInBatch(batch, colIdx, l);
        setColumnInfoInBatch(batch, colIdx, info);
    }
}
