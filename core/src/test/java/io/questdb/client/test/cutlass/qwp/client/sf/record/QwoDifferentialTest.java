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


package io.questdb.client.test.cutlass.qwp.client.sf.record;

import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.NativeBufferWriter;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoFormat;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoMaterializer;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordReader;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordWriter;
import io.questdb.client.cutlass.qwp.protocol.QwpGorillaEncoder;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;
import java.util.function.Consumer;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Differential oracle for {@link QwoMaterializer}. The same rows go two ways:
 * <ul>
 *     <li>legacy: into one {@link QwpTableBuffer} per frame block holding all of
 *     its rows, encoded by the production {@link QwpWebSocketEncoder};</li>
 *     <li>record: one staged row at a time into a {@link QwoRecordWriter}, which
 *     is published, sealed and materialised.</li>
 * </ul>
 * Both use one {@link GlobalSymbolDictionary}, the same confirmed id (the
 * record's {@code dictStartId - 1}) and the same batch max id, and the two
 * frames must be byte-identical. Expected bytes are never hand-encoded.
 */
public class QwoDifferentialTest {
    private static final String[] ALPHABET = {"a", "b", "q", "Z", "0", " ", "é", "€", "😀"};
    private static final int DICT_RESERVE = 16 * 1024;
    private static final byte[] EVERY_TYPE = {
            TYPE_BOOLEAN, TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG, TYPE_FLOAT, TYPE_DOUBLE, TYPE_SYMBOL,
            TYPE_TIMESTAMP, TYPE_DATE, TYPE_UUID, TYPE_LONG256, TYPE_GEOHASH, TYPE_VARCHAR, TYPE_TIMESTAMP_NANOS,
            TYPE_DOUBLE_ARRAY, TYPE_LONG_ARRAY, TYPE_DECIMAL64, TYPE_DECIMAL128, TYPE_DECIMAL256, TYPE_CHAR,
            TYPE_BINARY, TYPE_IPv4
    };
    private static final int REGION = 1024 * 1024;
    private static final long TS_BASE = 1_700_000_000_000_000L;

    @Test
    public void testArrays() throws Exception {
        withHarness(h -> {
            h.begin(2);
            final Col[] cols = {
                    new Col("da", TYPE_DOUBLE_ARRAY, 0),
                    new Col("la", TYPE_LONG_ARRAY, 0),
                    Col.designated(TYPE_TIMESTAMP)
            };
            final Object[] doubles = {
                    new double[][]{{1.5, 2.5, 3.5}, {4.5, 5.5, 6.5}},
                    null,
                    new double[0],
                    new double[]{-0.0, Double.NaN, Double.MAX_VALUE},
                    new double[][][]{{{1}, {2}}, {{3}, {4}}},
                    new double[][]{{}, {}},
                    null,
                    new double[][]{{9, 8, 7}, {6, 5, 4}},
                    new double[]{42},
                    null,
                    new double[0]
            };
            final Object[] longs = {
                    new long[]{1, 2, 3, 4, 5},
                    new long[0],
                    null,
                    new long[][]{{1, 2}, {3, 4}, {5, 6}},
                    null,
                    new long[]{Long.MIN_VALUE, Long.MAX_VALUE, 0, -1, 7},
                    new long[][][]{{{1, 2, 3}}},
                    new long[0],
                    null,
                    new long[]{5, 4, 3, 2, 1},
                    new long[][]{{}}
            };
            QwpTableBuffer staged = h.table("arrays");
            QwpTableBuffer legacy = h.table("arrays");
            for (int r = 0; r < doubles.length; r++) {
                h.row(staged, legacy, rowOf(cols, new Object[]{doubles[r], longs[r], TS_BASE + r}, h.dict));
                h.publish();
            }
            h.assertFrame();
        });
    }

    @Test
    public void testBitmapsAcrossOddChunkBoundaries() throws Exception {
        withHarness(h -> {
            final Col[] cols = {new Col("d", TYPE_DOUBLE, 0)};
            final boolean[] isNullRow = nullRows(20, 0, 1, 3, 6, 14);
            // every row count from 1 to 20 ends the last chunk at a different bit
            for (int n = 1; n <= 20; n++) {
                h.begin(0);
                QwpTableBuffer staged = h.table("bits");
                QwpTableBuffer legacy = h.table("bits");
                for (int r = 0; r < n; r++) {
                    h.row(staged, legacy, rowOf(cols, new Object[]{isNullRow[r] ? null : r + 0.5}, h.dict));
                    h.publish();
                }
                h.assertFrame();
            }
        });
    }

    @Test
    public void testEmptyBlockIsSkippedInTableCount() throws Exception {
        withHarness(h -> {
            h.begin(1);
            final Col[] colsA = {new Col("v", TYPE_LONG, 0), Col.designated(TYPE_TIMESTAMP)};
            final Col[] colsB = {new Col("s", TYPE_SYMBOL, 0)};
            final Col[] colsC = {new Col("x", TYPE_VARCHAR, 0)};
            QwpTableBuffer stagedA = h.table("a");
            QwpTableBuffer legacyA = h.table("a");
            QwpTableBuffer stagedB = h.table("b");
            QwpTableBuffer legacyB = h.table("b");
            QwpTableBuffer stagedC = h.table("c");
            QwpTableBuffer legacyC = h.table("c");
            for (int r = 0; r < 3; r++) {
                h.row(stagedA, legacyA, rowOf(colsA, new Object[]{(long) r, TS_BASE + r}, h.dict));
                h.row(stagedB, legacyB, rowOf(colsB, new Object[]{"b" + r}, h.dict));
                h.row(stagedC, legacyC, rowOf(colsC, new Object[]{"c" + r}, h.dict));
                h.publish();
            }
            int used = h.seal();
            QwoRecordReader reader = h.reader;
            reader.of(h.mem, used);
            Assert.assertEquals(3, reader.blockCount());
            int blockB = reader.nextBlock(reader.firstBlock());
            Assert.assertEquals(3, reader.rowCount(blockB));
            // A cut publish can leave a reachable block whose row word still counts
            // zero rows. Model it by storing a zero row count into b's row word.
            long rowWordAddr = h.mem + blockB + QwoFormat.D_ROW_WORD;
            long word = Unsafe.getUnsafe().getLong(rowWordAddr);
            Unsafe.getUnsafe().putLong(rowWordAddr, QwoFormat.rowWord(0, QwoFormat.crcOf(word)));
            Assert.assertEquals(0, reader.rowCount(blockB));

            // b's symbols stay in the dictionary section; only its table block goes
            h.legacyBlocks.remove(legacyB);
            h.assertMaterialised();
            Assert.assertEquals(2, Unsafe.getUnsafe().getShort(h.out.getBufferPtr() + 6));
        });
    }

    @Test
    public void testEveryTypeWithNullsAtEveryChunkBoundary() throws Exception {
        withHarness(h -> {
            h.begin(1);
            final Col[] cols = new Col[EVERY_TYPE.length];
            for (int i = 0; i < cols.length; i++) {
                byte type = EVERY_TYPE[i];
                cols[i] = new Col("c" + i, type, fixedPrefix(type));
            }
            // chunks hold rows 0 | 1-2 | 3-6 | 7-14 | 15-30 | 31-62: nulls on both
            // sides of every boundary, and the last chunk is left partly empty
            final boolean[] isNullRow = nullRows(37, 0, 1, 2, 3, 4, 7, 8, 15, 16, 31, 32);
            Random rnd = new Random(37);
            QwpTableBuffer staged = h.table("every");
            QwpTableBuffer legacy = h.table("every");
            for (int r = 0; r < isNullRow.length; r++) {
                Object[] values = new Object[cols.length];
                for (int i = 0; i < cols.length; i++) {
                    // BOOLEAN, BYTE, SHORT and CHAR have no bitmap: a null is the type's default
                    values[i] = isNullRow[r] ? null : randomValue(rnd, cols[i], r, 24);
                }
                h.row(staged, legacy, rowOf(cols, values, h.dict));
                if (r % 3 == 2) {
                    h.publish();
                }
            }
            h.publish();
            h.assertFrame();
        });
    }

    @Test
    public void testFourFixedColumnsOneTable() throws Exception {
        withHarness(h -> {
            final Col[] cols = {
                    new Col("price", TYPE_DOUBLE, 0),
                    new Col("qty", TYPE_LONG, 0),
                    new Col("venue", TYPE_SYMBOL, 0),
                    Col.designated(TYPE_TIMESTAMP)
            };
            final int[] rowCounts = {1, 2, 3, 7, 1000};
            for (int n : rowCounts) {
                h.begin(n % 3);
                QwpTableBuffer staged = h.table("trades");
                QwpTableBuffer legacy = h.table("trades");
                for (int r = 0; r < n; r++) {
                    // every fourth row reuses a symbol an earlier frame confirmed: its id is below dictStartId
                    String venue = h.dictStartId > 0 && r % 4 == 1 ? "sent" + r % h.dictStartId : "venue" + r % 5;
                    h.row(staged, legacy, rowOf(cols, new Object[]{
                            r * 0.25, 3L * r, venue, TS_BASE + 1_000_000L * r
                    }, h.dict));
                    h.publish();
                }
                h.assertFrame();
            }
        });
    }

    @Test
    public void testGeohashAndDecimalPrefixes() throws Exception {
        withHarness(h -> {
            h.begin(0);
            final Col[] cols = {
                    new Col("g12", TYPE_GEOHASH, 12),
                    new Col("g60", TYPE_GEOHASH, 60),
                    new Col("d64", TYPE_DECIMAL64, 2),
                    new Col("d128", TYPE_DECIMAL128, 6),
                    new Col("d256", TYPE_DECIMAL256, 10),
                    // never set: no precision and no scale reach the record
                    new Col("gnull", TYPE_GEOHASH, 7),
                    new Col("dnull", TYPE_DECIMAL128, 3),
                    Col.designated(TYPE_TIMESTAMP)
            };
            Random rnd = new Random(12);
            QwpTableBuffer staged = h.table("prefixes");
            QwpTableBuffer legacy = h.table("prefixes");
            for (int r = 0; r < 13; r++) {
                Object[] values = new Object[cols.length];
                for (int i = 0; i < 5; i++) {
                    values[i] = (r + i) % 4 == 0 ? null : randomValue(rnd, cols[i], r, 0);
                }
                values[7] = TS_BASE + r;
                h.row(staged, legacy, rowOf(cols, values, h.dict));
                h.publish();
            }
            h.assertFrame();
        });
    }

    @Test
    public void testRandomisedSchemas() throws Exception {
        withHarness(h -> {
            final long base = new Random().nextLong();
            for (int i = 0; i < 200; i++) {
                final long seed = base + i;
                try {
                    randomRecord(h, new Random(seed));
                } catch (Throwable t) {
                    throw new AssertionError("randomised schema failed [seed=" + seed + ']', t);
                }
            }
        });
    }

    @Test
    public void testRelocatedVariableSizeChunkMaterialisesLikeTheOracle() throws Exception {
        withHarness(h -> {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 500; i++) {
                sb.append(ALPHABET[i % ALPHABET.length]);
            }
            final String longString = sb.toString();
            final byte[] longBytes = new byte[500];
            new Random(5).nextBytes(longBytes);
            final byte[] types = {TYPE_VARCHAR, TYPE_BINARY};
            QwoRecordReader open = new QwoRecordReader();
            for (byte type : types) {
                h.begin(1);
                final Col[] cols = {new Col("v", type, 0)};
                final Object[] values = type == TYPE_VARCHAR
                        ? new Object[]{"a", "bb", longString}
                        : new Object[]{new byte[]{1}, new byte[]{2, 3}, longBytes};
                QwpTableBuffer staged = h.table("reloc");
                QwpTableBuffer legacy = h.table("reloc");
                // row 0 fills the 1-row first chunk, row 1 opens a 2-row chunk sized for short values
                h.row(staged, legacy, rowOf(cols, new Object[]{values[0]}, h.dict));
                h.row(staged, legacy, rowOf(cols, new Object[]{values[1]}, h.dict));
                h.publish();
                open.of(h.mem, REGION);
                int def = open.columnDef(open.firstBlock(), 0);
                int c0 = open.firstChunk(def);
                int c0End = c0 + QwoFormat.C_DATA + QwoFormat.chunkDataBytes(type, 0, true, open.chunkRowCap(c0), open.chunkByteCap(c0));
                int c1 = open.nextChunk(c0);
                Assert.assertEquals("the chunks start out contiguous", c0End, c1);
                Assert.assertEquals(2, open.chunkRowCap(c1));
                Assert.assertTrue(open.chunkByteCap(c1) < 2 + 500);

                // row 2's value outgrows the bytes the 2-row chunk has left: the chunk is relocated (R5)
                h.row(staged, legacy, rowOf(cols, new Object[]{values[2]}, h.dict));
                h.publish();
                h.assertFrame();
                QwoRecordReader sealed = h.reader;
                def = sealed.columnDef(sealed.firstBlock(), 0);
                Assert.assertEquals(c0, sealed.firstChunk(def));
                int relocated = sealed.nextChunk(c0);
                Assert.assertTrue("the relocated chunk lies past the dead copy at " + c1 + ": " + relocated, relocated > c1);
                Assert.assertNotEquals("the column's chunks are no longer contiguous", c0End, relocated);
                Assert.assertEquals(2, sealed.chunkRowCap(relocated));
                Assert.assertTrue(sealed.chunkByteCap(relocated) >= 2 + 500);
                Assert.assertEquals(0, sealed.nextChunk(relocated));
            }
        });
    }

    @Test
    public void testThreeTablesAndASchemaChange() throws Exception {
        withHarness(h -> {
            h.begin(2);
            final Col x = new Col("x", TYPE_LONG, 0);
            final Col[] colsA1 = {x};
            final Col[] colsA2 = {x, new Col("y", TYPE_VARCHAR, 0)};
            final Col[] colsB = {new Col("s", TYPE_SYMBOL, 0), new Col("d", TYPE_DOUBLE, 0), Col.designated(TYPE_TIMESTAMP)};
            final Col[] colsC = {new Col("flag", TYPE_BOOLEAN, 0), new Col("v", TYPE_VARCHAR, 0)};
            QwpTableBuffer stagedA = h.table("a");
            // the record splits table a into two blocks; the oracle models each with its own buffer
            QwpTableBuffer legacyA1 = h.table("a");
            QwpTableBuffer legacyA2 = h.table("a");
            QwpTableBuffer stagedB = h.table("b");
            QwpTableBuffer legacyB = h.table("b");
            QwpTableBuffer stagedC = h.table("c");
            QwpTableBuffer legacyC = h.table("c");
            for (int r = 0; r < 10; r++) {
                if (r < 5) {
                    h.row(stagedA, legacyA1, rowOf(colsA1, new Object[]{(long) r}, h.dict));
                } else {
                    h.row(stagedA, legacyA2, rowOf(colsA2, new Object[]{(long) r, r % 3 == 0 ? null : "y" + r}, h.dict));
                }
                h.row(stagedB, legacyB, rowOf(colsB, new Object[]{"s" + r % 4, r * 1.5, TS_BASE + 1_000_000L * r}, h.dict));
                h.row(stagedC, legacyC, rowOf(colsC, new Object[]{r % 2 == 0, "c" + r}, h.dict));
                h.publish();
            }
            Assert.assertEquals(4, h.writer.blockCount());
            Assert.assertEquals(4, h.legacyBlocks.size());
            Assert.assertSame(legacyA2, h.legacyBlocks.get(3));
            h.assertFrame();
            Assert.assertEquals(4, Unsafe.getUnsafe().getShort(h.out.getBufferPtr() + 6));
        });
    }

    @Test
    public void testTimestampsGorillaAndRaw() throws Exception {
        withHarness(h -> {
            h.begin(0);
            final int rows = 16;
            final Col[] cols = {
                    new Col("regular", TYPE_TIMESTAMP, 0),
                    new Col("three", TYPE_TIMESTAMP, 0),
                    new Col("two", TYPE_TIMESTAMP, 0),
                    new Col("one", TYPE_TIMESTAMP_NANOS, 0),
                    new Col("jump", TYPE_TIMESTAMP_NANOS, 0),
                    Col.designated(TYPE_TIMESTAMP_NANOS)
            };
            final long[] regular = new long[rows];
            final long[] jump = new long[rows];
            for (int r = 0; r < rows; r++) {
                regular[r] = TS_BASE + 1_000_000L * r;
                // one delta-of-delta beyond int32 forces the raw encoding
                jump[r] = TS_BASE * 1000 + r + (r >= 9 ? 1L << 40 : 0);
            }
            Assert.assertTrue(gorillaSize(regular) >= 0);
            Assert.assertEquals(-1, gorillaSize(jump));
            QwpTableBuffer staged = h.table("ts");
            QwpTableBuffer legacy = h.table("ts");
            for (int r = 0; r < rows; r++) {
                h.row(staged, legacy, rowOf(cols, new Object[]{
                        regular[r],
                        r == 0 || r == 5 || r == 15 ? regular[r] : null,
                        r == 4 || r == 11 ? regular[r] : null,
                        r == 7 ? jump[r] : null,
                        jump[r],
                        TS_BASE * 1000 + 1000L * r
                }, h.dict));
                h.publish();
            }
            h.assertFrame();
        });
    }

    @Test
    public void testVarcharAndBinaryAcrossChunks() throws Exception {
        withHarness(h -> {
            h.begin(0);
            final Col[] cols = {
                    new Col("s", TYPE_VARCHAR, 0),
                    new Col("b", TYPE_BINARY, 0),
                    Col.designated(TYPE_TIMESTAMP)
            };
            Random rnd = new Random(300);
            QwpTableBuffer staged = h.table("var");
            QwpTableBuffer legacy = h.table("var");
            for (int r = 0; r < 100; r++) {
                Object[] values = new Object[cols.length];
                values[0] = rnd.nextInt(7) == 0 ? null : randomString(rnd, 300);
                values[1] = rnd.nextInt(7) == 0 ? null : randomBytes(rnd, 300);
                values[2] = TS_BASE + r;
                h.row(staged, legacy, rowOf(cols, values, h.dict));
                if (rnd.nextBoolean()) {
                    h.publish();
                }
            }
            h.publish();
            h.assertFrame();
        });
    }

    private static void assertSameBytes(long expectedAddr, int expectedLen, long actualAddr, int actualLen) {
        int n = Math.min(expectedLen, actualLen);
        int diff = -1;
        for (int i = 0; i < n; i++) {
            if (Unsafe.getUnsafe().getByte(expectedAddr + i) != Unsafe.getUnsafe().getByte(actualAddr + i)) {
                diff = i;
                break;
            }
        }
        if (diff < 0 && expectedLen == actualLen) {
            return;
        }
        if (diff < 0) {
            diff = n;
        }
        Assert.fail("frames differ at offset " + diff
                + " [legacyLen=" + expectedLen + ", recordLen=" + actualLen + ']'
                + "\nlegacy " + hexAround(expectedAddr, expectedLen, diff)
                + "\nrecord " + hexAround(actualAddr, actualLen, diff));
    }

    private static int fixedPrefix(byte type) {
        switch (type) {
            case TYPE_GEOHASH:
                return 20;
            case TYPE_DECIMAL64:
                return 3;
            case TYPE_DECIMAL128:
                return 7;
            case TYPE_DECIMAL256:
                return 11;
            default:
                return 0;
        }
    }

    private static int gorillaSize(long[] values) {
        long size = 8L * values.length;
        long p = Unsafe.malloc(size, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < values.length; i++) {
                Unsafe.getUnsafe().putLong(p + 8L * i, values[i]);
            }
            return QwpGorillaEncoder.calculateEncodedSizeIfSupported(p, values.length);
        } finally {
            Unsafe.free(p, size, MemoryTag.NATIVE_DEFAULT);
        }
    }

    private static String hexAround(long addr, int len, int at) {
        int from = Math.max(0, at - 16);
        int to = Math.min(len, at + 16);
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(from).append(',').append(to).append("):");
        for (int i = from; i < to; i++) {
            sb.append(i == at ? " >" : " ");
            int b = Unsafe.getUnsafe().getByte(addr + i) & 0xFF;
            sb.append(Character.forDigit(b >>> 4, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }

    private static boolean[] nullRows(int rows, int... nulls) {
        boolean[] isNullRow = new boolean[rows];
        for (int r : nulls) {
            isNullRow[r] = true;
        }
        return isNullRow;
    }

    private static void put(QwpTableBuffer t, Col c, Object v, GlobalSymbolDictionary dict) {
        QwpTableBuffer.ColumnBuffer col = c.isDesignated
                ? t.getOrCreateDesignatedTimestampColumn(c.type)
                : t.getOrCreateColumn(c.name, c.type, c.hasBitmap);
        if (v == null) {
            col.addNull();
            return;
        }
        switch (c.type) {
            case TYPE_BOOLEAN:
                col.addBoolean((Boolean) v);
                break;
            case TYPE_BYTE:
                col.addByte((Byte) v);
                break;
            case TYPE_SHORT:
                col.addShort((Short) v);
                break;
            case TYPE_CHAR:
                col.addShort((short) ((Character) v).charValue());
                break;
            case TYPE_INT:
                col.addInt((Integer) v);
                break;
            case TYPE_IPv4:
                col.addIPv4((Integer) v);
                break;
            case TYPE_LONG:
            case TYPE_DATE:
            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
                col.addLong((Long) v);
                break;
            case TYPE_FLOAT:
                col.addFloat((Float) v);
                break;
            case TYPE_DOUBLE:
                col.addDouble((Double) v);
                break;
            case TYPE_SYMBOL: {
                String symbol = (String) v;
                col.addSymbolWithGlobalId(symbol, dict.getOrAddSymbol(symbol));
                break;
            }
            case TYPE_UUID: {
                long[] uuid = (long[]) v;
                col.addUuid(uuid[0], uuid[1]);
                break;
            }
            case TYPE_LONG256: {
                long[] l = (long[]) v;
                col.addLong256(l[0], l[1], l[2], l[3]);
                break;
            }
            case TYPE_GEOHASH:
                col.addGeoHash((Long) v, c.prefix);
                break;
            case TYPE_VARCHAR:
                col.addString((String) v);
                break;
            case TYPE_BINARY:
                col.addBinary((byte[]) v);
                break;
            case TYPE_DOUBLE_ARRAY:
                if (v instanceof double[]) {
                    col.addDoubleArray((double[]) v);
                } else if (v instanceof double[][]) {
                    col.addDoubleArray((double[][]) v);
                } else {
                    col.addDoubleArray((double[][][]) v);
                }
                break;
            case TYPE_LONG_ARRAY:
                if (v instanceof long[]) {
                    col.addLongArray((long[]) v);
                } else if (v instanceof long[][]) {
                    col.addLongArray((long[][]) v);
                } else {
                    col.addLongArray((long[][][]) v);
                }
                break;
            case TYPE_DECIMAL64:
                col.addDecimal64((Decimal64) v);
                break;
            case TYPE_DECIMAL128:
                col.addDecimal128((Decimal128) v);
                break;
            case TYPE_DECIMAL256:
                col.addDecimal256((Decimal256) v);
                break;
            default:
                throw new AssertionError("unexpected type " + c.type);
        }
    }

    private static Object randomArray(Random rnd, boolean isDouble) {
        int nDims = 1 + rnd.nextInt(3);
        int d0 = rnd.nextInt(4);
        int d1 = rnd.nextInt(3);
        int d2 = rnd.nextInt(3);
        if (isDouble) {
            switch (nDims) {
                case 1: {
                    double[] a = new double[d0];
                    for (int i = 0; i < d0; i++) {
                        a[i] = rnd.nextDouble();
                    }
                    return a;
                }
                case 2: {
                    double[][] a = new double[d0][d1];
                    for (int i = 0; i < d0; i++) {
                        for (int j = 0; j < d1; j++) {
                            a[i][j] = rnd.nextDouble();
                        }
                    }
                    return a;
                }
                default: {
                    double[][][] a = new double[d0][d1][d2];
                    for (int i = 0; i < d0; i++) {
                        for (int j = 0; j < d1; j++) {
                            for (int k = 0; k < d2; k++) {
                                a[i][j][k] = rnd.nextDouble();
                            }
                        }
                    }
                    return a;
                }
            }
        }
        switch (nDims) {
            case 1: {
                long[] a = new long[d0];
                for (int i = 0; i < d0; i++) {
                    a[i] = rnd.nextLong();
                }
                return a;
            }
            case 2: {
                long[][] a = new long[d0][d1];
                for (int i = 0; i < d0; i++) {
                    for (int j = 0; j < d1; j++) {
                        a[i][j] = rnd.nextLong();
                    }
                }
                return a;
            }
            default: {
                long[][][] a = new long[d0][d1][d2];
                for (int i = 0; i < d0; i++) {
                    for (int j = 0; j < d1; j++) {
                        for (int k = 0; k < d2; k++) {
                            a[i][j][k] = rnd.nextLong();
                        }
                    }
                }
                return a;
            }
        }
    }

    private static byte[] randomBytes(Random rnd, int maxLen) {
        byte[] b = new byte[rnd.nextInt(maxLen + 1)];
        rnd.nextBytes(b);
        return b;
    }

    private static int randomPrefix(Random rnd, byte type) {
        switch (type) {
            case TYPE_GEOHASH:
                return 1 + rnd.nextInt(60);
            case TYPE_DECIMAL64:
                return rnd.nextInt(9);
            case TYPE_DECIMAL128:
                return rnd.nextInt(20);
            case TYPE_DECIMAL256:
                return rnd.nextInt(30);
            default:
                return 0;
        }
    }

    /**
     * One record with 1-4 tables of 1-8 random columns and 1-64 rows each, rows
     * interleaved across tables, published at random points, about 20% nulls.
     * A few rows appended after the last publish are rolled back by the seal and
     * never reach the oracle: their bytes stay in the chunks past the committed rows.
     */
    private static void randomRecord(Harness h, Random rnd) {
        h.begin(rnd.nextInt(4));
        int tableCount = 1 + rnd.nextInt(4);
        Col[][] schemas = new Col[tableCount][];
        QwpTableBuffer[] staged = new QwpTableBuffer[tableCount];
        QwpTableBuffer[] legacy = new QwpTableBuffer[tableCount];
        int[] rowsLeft = new int[tableCount];
        int[] rowsDone = new int[tableCount];
        int totalRows = 0;
        for (int t = 0; t < tableCount; t++) {
            int colCount = 1 + rnd.nextInt(8);
            Col[] cols = new Col[colCount];
            boolean hasDesignated = rnd.nextInt(3) == 0;
            for (int c = 0; c < colCount; c++) {
                if (hasDesignated && c == colCount - 1) {
                    cols[c] = Col.designated(rnd.nextBoolean() ? TYPE_TIMESTAMP : TYPE_TIMESTAMP_NANOS);
                } else {
                    byte type = EVERY_TYPE[rnd.nextInt(EVERY_TYPE.length)];
                    cols[c] = new Col("c" + c, type, randomPrefix(rnd, type));
                }
            }
            schemas[t] = cols;
            staged[t] = h.table("t" + t);
            legacy[t] = h.table("t" + t);
            rowsLeft[t] = 1 + rnd.nextInt(64);
            totalRows += rowsLeft[t];
        }
        for (int i = 0; i < totalRows; i++) {
            int t = rnd.nextInt(tableCount);
            while (rowsLeft[t] == 0) {
                t = (t + 1) % tableCount;
            }
            Object[] values = randomRow(rnd, schemas[t], rowsDone[t], false, h.dictStartId);
            h.row(staged[t], legacy[t], rowOf(schemas[t], values, h.dict));
            rowsLeft[t]--;
            rowsDone[t]++;
            if (rnd.nextInt(4) == 0) {
                h.publish();
            }
        }
        h.publish();
        for (int i = 0, n = rnd.nextInt(3); i < n; i++) {
            int t = rnd.nextInt(tableCount);
            h.uncommittedRow(staged[t], rowOf(schemas[t], randomRow(rnd, schemas[t], rowsDone[t], true, h.dictStartId), h.dict));
        }
        h.assertFrame();
    }

    /**
     * Values for one row: about 20% nulls, the designated timestamp never null.
     * An uncommitted row leaves GEOHASH and DECIMAL columns null so that a rolled
     * back value cannot hand a precision or scale to an otherwise all-null column
     * (ruling R6 accepts that difference; the oracle does not pin it). A SYMBOL
     * value is now and then one of the {@code confirmedSymbols} an earlier frame
     * confirmed ({@link Harness#begin}), whose id lies below the record's dictStartId.
     */
    private static Object[] randomRow(Random rnd, Col[] cols, int row, boolean isUncommitted, int confirmedSymbols) {
        Object[] values = new Object[cols.length];
        for (int c = 0; c < cols.length; c++) {
            Col col = cols[c];
            boolean hasPrefix = col.type == TYPE_GEOHASH || col.type == TYPE_DECIMAL64
                    || col.type == TYPE_DECIMAL128 || col.type == TYPE_DECIMAL256;
            if (!col.isDesignated && (rnd.nextInt(5) == 0 || isUncommitted && hasPrefix)) {
                values[c] = null;
            } else if (col.type == TYPE_SYMBOL && confirmedSymbols > 0 && rnd.nextInt(4) == 0) {
                values[c] = "sent" + rnd.nextInt(confirmedSymbols);
            } else {
                values[c] = randomValue(rnd, col, row, 40);
            }
        }
        return values;
    }

    private static String randomString(Random rnd, int maxLen) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0, n = rnd.nextInt(maxLen + 1); i < n; i++) {
            sb.append(ALPHABET[rnd.nextInt(ALPHABET.length)]);
        }
        return sb.toString();
    }

    private static Object randomValue(Random rnd, Col c, int row, int maxVarLen) {
        switch (c.type) {
            case TYPE_BOOLEAN:
                return rnd.nextBoolean();
            case TYPE_BYTE:
                return (byte) rnd.nextInt();
            case TYPE_SHORT:
                return (short) rnd.nextInt();
            case TYPE_CHAR:
                return (char) (32 + rnd.nextInt(0xD000));
            case TYPE_INT:
            case TYPE_IPv4:
                return rnd.nextInt();
            case TYPE_LONG:
            case TYPE_DATE:
                return rnd.nextLong();
            case TYPE_FLOAT:
                return rnd.nextFloat();
            case TYPE_DOUBLE:
                return rnd.nextDouble();
            case TYPE_TIMESTAMP:
            case TYPE_TIMESTAMP_NANOS:
                // mostly regular (Gorilla), now and then a jump no int32 delta-of-delta holds (raw)
                return rnd.nextInt(20) == 0 ? rnd.nextLong() >>> 2 : TS_BASE + 1_000_000L * row + rnd.nextInt(1000);
            case TYPE_SYMBOL:
                return "sym" + rnd.nextInt(12);
            case TYPE_UUID:
                return new long[]{rnd.nextLong(), rnd.nextLong()};
            case TYPE_LONG256:
                return new long[]{rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), rnd.nextLong()};
            case TYPE_GEOHASH:
                return rnd.nextLong() & ((1L << c.prefix) - 1);
            case TYPE_VARCHAR:
                return randomString(rnd, maxVarLen);
            case TYPE_BINARY:
                return randomBytes(rnd, maxVarLen);
            case TYPE_DOUBLE_ARRAY:
                return randomArray(rnd, true);
            case TYPE_LONG_ARRAY:
                return randomArray(rnd, false);
            case TYPE_DECIMAL64:
                return new Decimal64(rnd.nextLong() >> 8, c.prefix);
            case TYPE_DECIMAL128:
                return new Decimal128(rnd.nextInt(1 << 30), rnd.nextLong(), c.prefix);
            case TYPE_DECIMAL256:
                return new Decimal256(rnd.nextInt(1 << 20), rnd.nextLong(), rnd.nextLong(), rnd.nextLong(), c.prefix);
            default:
                throw new AssertionError("unexpected type " + c.type);
        }
    }

    private static Consumer<QwpTableBuffer> rowOf(Col[] cols, Object[] values, GlobalSymbolDictionary dict) {
        return t -> {
            for (int i = 0; i < cols.length; i++) {
                put(t, cols[i], values[i], dict);
            }
        };
    }

    private static void withHarness(HarnessCode code) throws Exception {
        assertMemoryLeak(() -> {
            try (Harness h = new Harness()) {
                code.run(h);
            }
        });
    }

    private static final class Col {
        final boolean hasBitmap;
        final boolean isDesignated;
        final String name;
        final int prefix;
        final byte type;

        Col(String name, byte type, int prefix) {
            this(name, type, prefix, false);
        }

        private Col(String name, byte type, int prefix, boolean isDesignated) {
            this.name = name;
            this.type = type;
            this.prefix = prefix;
            this.isDesignated = isDesignated;
            // the sender's choice: these four have no null bitmap, a null is the type's default
            this.hasBitmap = isDesignated || (type != TYPE_BOOLEAN && type != TYPE_BYTE && type != TYPE_SHORT && type != TYPE_CHAR);
        }

        static Col designated(byte type) {
            return new Col("", type, 0, true);
        }
    }

    /**
     * Drives both sides of the oracle over one record at a time.
     */
    private static final class Harness implements QuietCloseable {
        final GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
        final QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();
        final ArrayList<QwpTableBuffer> legacyBlocks = new ArrayList<>();
        final QwoMaterializer materializer = new QwoMaterializer();
        final long mem;
        final NativeBufferWriter out = new NativeBufferWriter();
        final QwoRecordReader reader = new QwoRecordReader();
        final ArrayList<QwpTableBuffer> tables = new ArrayList<>();
        final QwoRecordWriter writer = new QwoRecordWriter();
        int batchMax;
        int dictStartId;

        Harness() {
            mem = Unsafe.malloc(REGION, MemoryTag.NATIVE_DEFAULT);
        }

        /**
         * Seals the record, materialises it and compares it with the oracle.
         */
        int assertFrame() {
            reader.of(mem, seal());
            return assertMaterialised();
        }

        /**
         * Materialises the sealed record in {@link #reader} and compares it with
         * the legacy frame of {@link #legacyBlocks}; returns the frame size.
         */
        int assertMaterialised() {
            out.reset();
            int frameSize = materializer.write(reader, out);
            Assert.assertEquals(frameSize, out.getPosition());
            encoder.beginMessage(legacyBlocks.size(), dict, dictStartId - 1, batchMax);
            for (int i = 0, n = legacyBlocks.size(); i < n; i++) {
                encoder.addTable(legacyBlocks.get(i));
            }
            int legacySize = encoder.finishMessage();
            assertSameBytes(encoder.getBuffer().getBufferPtr(), legacySize, out.getBufferPtr(), frameSize);
            long bound = (long) reader.usedBytes() + QwoFormat.WIRE_MARGIN_FIXED
                    + (long) QwoFormat.WIRE_MARGIN_PER_BLOCK * reader.blockCount();
            Assert.assertTrue("frame exceeds the record's wire bound [frame=" + frameSize + ", bound=" + bound + ']',
                    frameSize <= bound);
            return frameSize;
        }

        /**
         * Starts a record whose dictionary continues after {@code dictStartId}
         * symbols the server already has.
         */
        void begin(int dictStartId) {
            closeTables();
            legacyBlocks.clear();
            dict.clear();
            for (int i = 0; i < dictStartId; i++) {
                dict.getOrAddSymbol("sent" + i);
            }
            this.dictStartId = dictStartId;
            batchMax = dictStartId - 1;
            // garbage, so nothing relies on zeroed memory
            Unsafe.getUnsafe().setMemory(mem, REGION, (byte) 0x5A);
            writer.open(mem, REGION, VERSION, dictStartId, DICT_RESERVE);
        }

        @Override
        public void close() {
            closeTables();
            encoder.close();
            materializer.close();
            out.close();
            writer.close();
            Unsafe.free(mem, REGION, MemoryTag.NATIVE_DEFAULT);
        }

        void publish() {
            writer.publish();
        }

        /**
         * Appends one row to the record through {@code staged} and the same row
         * to {@code legacy}, the buffer that models the row's frame block.
         */
        void row(QwpTableBuffer staged, QwpTableBuffer legacy, Consumer<QwpTableBuffer> values) {
            values.accept(staged);
            staged.nextRow();
            batchMax = Math.max(batchMax, dict.size() - 1);
            Assert.assertEquals(QwoRecordWriter.APPEND_OK, writer.appendStagedRow(staged, dict, batchMax));
            staged.clearStagedRow();
            values.accept(legacy);
            legacy.nextRow();
            for (int i = 0, n = legacyBlocks.size(); i < n; i++) {
                if (legacyBlocks.get(i) == legacy) {
                    return;
                }
            }
            legacyBlocks.add(legacy);
        }

        int seal() {
            return writer.seal(false);
        }

        QwpTableBuffer table(String name) {
            QwpTableBuffer t = new QwpTableBuffer(name);
            tables.add(t);
            return t;
        }

        /**
         * Appends a row to the record only; the caller seals without publishing it.
         */
        void uncommittedRow(QwpTableBuffer staged, Consumer<QwpTableBuffer> values) {
            values.accept(staged);
            staged.nextRow();
            Assert.assertEquals(QwoRecordWriter.APPEND_OK, writer.appendStagedRow(staged, dict, Math.max(batchMax, dict.size() - 1)));
            staged.clearStagedRow();
        }

        private void closeTables() {
            for (int i = 0, n = tables.size(); i < n; i++) {
                tables.get(i).close();
            }
            tables.clear();
        }
    }

    @FunctionalInterface
    private interface HarnessCode {
        void run(Harness h) throws Exception;
    }
}
