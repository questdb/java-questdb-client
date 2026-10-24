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
import io.questdb.client.cutlass.qwp.client.sf.record.QwoFormat;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordReader;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordWriter;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoValidator;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Crc32c;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Random;
import java.util.function.Consumer;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwoRecordWriterTest {

    private static final byte POISON = 0x5A;
    private static final int REGION = 256 * 1024;

    @Test
    public void testAppendFourFixedColumnsAcrossThreeReleasesPublishesCounts() throws Exception {
        withRecord((mem, writer) -> {
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            Assert.assertTrue(writer.isOpen());
            QwoRecordReader reader = new QwoRecordReader();
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                int lastUsed = writer.usedBytes();
                for (int r = 0; r < 3; r++) {
                    final int row = r;
                    append(writer, tb, dict, -1, t -> {
                        t.getOrCreateColumn("price", QwpConstants.TYPE_DOUBLE, true).addDouble(1.5 * (row + 1));
                        t.getOrCreateColumn("qty", QwpConstants.TYPE_LONG, true).addLong(100 + row);
                        t.getOrCreateColumn("venue", QwpConstants.TYPE_INT, true).addInt(7 * row);
                        t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(1_000_000L * (row + 1));
                    });
                    Assert.assertEquals(r + 1, writer.rowCount());
                    Assert.assertTrue(writer.hasUncommittedRows());
                    writer.publish();
                    Assert.assertFalse(writer.hasUncommittedRows());
                    Assert.assertEquals(r + 1, writer.committedRowCount());

                    reader.of(mem, REGION);
                    Assert.assertEquals(1, reader.blockCount());
                    int b = reader.firstBlock();
                    Assert.assertEquals(r + 1, reader.rowCount(b));
                    Assert.assertEquals(r + 1, QwoFormat.rowCountOf(Unsafe.getUnsafe().getLong(mem + b + QwoFormat.D_ROW_WORD)));
                    Assert.assertEquals(0, reader.nextBlock(b));
                    Assert.assertEquals(writer.usedBytes(), reader.usedBytes());
                    if (r < 2) {
                        // rows 0 and 1 allocate the descriptor, then the 1-row and the 2-row chunks
                        Assert.assertTrue(reader.usedBytes() > lastUsed);
                    } else {
                        // row 2 fills the free slots of the 2-row chunks
                        Assert.assertEquals(lastUsed, reader.usedBytes());
                    }
                    lastUsed = reader.usedBytes();
                }
                Assert.assertEquals(1, writer.blockCount());
                Assert.assertEquals(0, tb.qwoBlock());
                Assert.assertEquals(writer.epoch(), tb.qwoEpoch());

                int b = reader.firstBlock();
                Assert.assertEquals("trades", nameOf(reader.tableNameAddr(b), reader.tableNameLen(b)));
                Assert.assertEquals(4, reader.colCount(b));
                Assert.assertEquals(Unsafe.getUnsafe().getInt(mem + b + QwoFormat.D_CRC), QwoFormat.descriptorCrc(mem + b));
                for (int col = 0; col < 4; col++) {
                    int def = reader.columnDef(b, col);
                    int c0 = reader.firstChunk(def);
                    Assert.assertEquals(1, reader.chunkRowCap(c0));
                    int c1 = reader.nextChunk(c0);
                    Assert.assertEquals(2, reader.chunkRowCap(c1));
                    Assert.assertEquals(0, reader.nextChunk(c1));
                }
                int priceDef = reader.columnDef(b, 0);
                Assert.assertEquals(QwpConstants.TYPE_DOUBLE, reader.columnType(priceDef));
                Assert.assertTrue(reader.columnUsesBitmap(priceDef));
                int c0 = reader.firstChunk(priceDef);
                int c1 = reader.nextChunk(c0);
                Assert.assertEquals(1.5, Unsafe.getUnsafe().getDouble(reader.chunkValuesAddr(c0, QwpConstants.TYPE_DOUBLE, true)), 0.0);
                long v1 = reader.chunkValuesAddr(c1, QwpConstants.TYPE_DOUBLE, true);
                Assert.assertEquals(3.0, Unsafe.getUnsafe().getDouble(v1), 0.0);
                Assert.assertEquals(4.5, Unsafe.getUnsafe().getDouble(v1 + 8), 0.0);
                int tsDef = reader.columnDef(b, 3);
                Assert.assertEquals(0, reader.columnNameLen(tsDef));
                long ts1 = reader.chunkValuesAddr(reader.nextChunk(reader.firstChunk(tsDef)), QwpConstants.TYPE_TIMESTAMP, true);
                Assert.assertEquals(3_000_000L, Unsafe.getUnsafe().getLong(ts1 + 8));
            }
        });
    }

    @Test
    public void testBlockCountStopsAtTheU16Limit() throws Exception {
        assertMemoryLeak(() -> {
            // the header's blockCount is a u16: 65535 blocks of one LONG column take about 4 MiB
            final int capacity = 8 * 1024 * 1024;
            long mem = Unsafe.malloc(capacity, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter(); QwpTableBuffer tb = new QwpTableBuffer("t")) {
                writer.open(mem, capacity, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
                GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
                for (int i = 0; i < QwoFormat.MAX_BLOCKS; i++) {
                    // a fresh column buffer matches no block, so every row opens one
                    tb.truncateColumns(0);
                    appendLong(writer, tb, dict, i);
                }
                Assert.assertEquals(QwoFormat.MAX_BLOCKS, writer.blockCount());
                // the last block still takes rows
                appendLong(writer, tb, dict, -1);
                writer.publish();
                Assert.assertEquals(QwoFormat.MAX_BLOCKS + 1, writer.rowCount());

                int used = writer.usedBytes();
                tb.truncateColumns(0);
                stage(tb, t -> t.getOrCreateColumn("v", QwpConstants.TYPE_LONG, true).addLong(-2));
                Assert.assertEquals("a row that needs one more block is refused",
                        QwoRecordWriter.APPEND_NO_SPACE, writer.appendStagedRow(tb, dict, -1));
                tb.clearStagedRow();
                Assert.assertEquals(0, writer.lastRefusedDictBytes());
                Assert.assertEquals(QwoFormat.MAX_BLOCKS, writer.blockCount());
                Assert.assertEquals(used, writer.usedBytes());
                Assert.assertFalse(writer.hasUncommittedRows());

                int sealed = writer.seal(false);
                QwoRecordReader reader = new QwoRecordReader();
                reader.of(mem, sealed);
                Assert.assertEquals("the header field did not wrap", QwoFormat.MAX_BLOCKS, reader.blockCount());
                QwoValidator validator = new QwoValidator();
                Assert.assertTrue(validator.detail(), validator.validateSealed(mem, sealed));
                Assert.assertEquals(QwoFormat.MAX_BLOCKS + 1, validator.committedRows());
            } finally {
                Unsafe.free(mem, capacity, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testBlockRowLimitSeals() throws Exception {
        withRecord((mem, writer) -> {
            writer.setMaxRowsPerBlockForTest(3);
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer tb = new QwpTableBuffer("t"); QwpTableBuffer other = new QwpTableBuffer("u")) {
                for (int r = 0; r < 3; r++) {
                    final long v = r;
                    append(writer, tb, dict, -1, t -> t.getOrCreateColumn("v", QwpConstants.TYPE_LONG, true).addLong(v));
                }
                writer.publish();
                int used = writer.usedBytes();
                stage(tb, t -> t.getOrCreateColumn("v", QwpConstants.TYPE_LONG, true).addLong(3));
                Assert.assertEquals(QwoRecordWriter.APPEND_NO_SPACE, writer.appendStagedRow(tb, dict, -1));
                Assert.assertEquals(0, writer.lastRefusedDictBytes());
                Assert.assertEquals(3, writer.rowCount());
                Assert.assertEquals(used, writer.usedBytes());
                Assert.assertFalse(writer.hasUncommittedRows());
                tb.clearStagedRow();

                // another table still gets its own block
                append(writer, other, dict, -1, t -> t.getOrCreateColumn("v", QwpConstants.TYPE_LONG, true).addLong(9));
                Assert.assertEquals(2, writer.blockCount());
                Assert.assertEquals(4, writer.rowCount());
            }
        });
    }

    @Test
    public void testBlockWithMoreColumnsThanTheLimitIsRefused() throws Exception {
        Assert.assertEquals("the descriptor's colCount is a u16", 0xFFFF, QwoFormat.MAX_COLUMNS);
        withRecord((mem, writer) -> {
            // the staged buffer caps a table far below the u16 limit, so the test lowers it
            writer.setMaxColumnsPerBlockForTest(2);
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer tb = new QwpTableBuffer("wide")) {
                append(writer, tb, dict, -1, t -> {
                    t.getOrCreateColumn("a", QwpConstants.TYPE_LONG, true).addLong(1);
                    t.getOrCreateColumn("b", QwpConstants.TYPE_LONG, true).addLong(2);
                });
                writer.publish();
                int used = writer.usedBytes();
                stage(tb, t -> {
                    t.getOrCreateColumn("a", QwpConstants.TYPE_LONG, true).addLong(3);
                    t.getOrCreateColumn("b", QwpConstants.TYPE_LONG, true).addLong(4);
                    t.getOrCreateColumn("c", QwpConstants.TYPE_LONG, true).addLong(5);
                });
                try {
                    writer.appendStagedRow(tb, dict, -1);
                    Assert.fail("a block with more columns than the limit must be refused");
                } catch (IllegalArgumentException expected) {
                    TestUtils.assertContains(expected.getMessage(),
                            "table 'wide' has more columns than a record block holds [columns=3, max=2]");
                }
                tb.clearStagedRow();
                Assert.assertEquals(1, writer.blockCount());
                Assert.assertEquals(1, writer.rowCount());
                Assert.assertEquals(used, writer.usedBytes());
                Assert.assertFalse(writer.hasUncommittedRows());
                int sealed = writer.seal(false);
                QwoValidator validator = new QwoValidator();
                Assert.assertTrue(validator.detail(), validator.validateSealed(mem, sealed));
                Assert.assertEquals(1, validator.committedRows());
            }
        });
    }

    @Test
    public void testChunksShrinkWhenTheRecordRunsShort() throws Exception {
        withRecord((mem, writer) -> {
            final int capacity = 60 * 1024;
            writer.open(mem, capacity, QwpConstants.VERSION, 0, capacity / 32);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 4096; i++) {
                sb.append((char) ('a' + i % 26));
            }
            final String wide = sb.toString();
            final String narrow = "12345678";
            ArrayList<String> expected = new ArrayList<>();
            try (QwpTableBuffer tb = new QwpTableBuffer("shrink")) {
                for (int pass = 0; pass < 2; pass++) {
                    final String value = pass == 0 ? wide : narrow;
                    while (true) {
                        stage(tb, t -> t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(value));
                        int result = writer.appendStagedRow(tb, dict, -1);
                        tb.clearStagedRow();
                        if (result == QwoRecordWriter.APPEND_NO_SPACE) {
                            break;
                        }
                        expected.add(value);
                        writer.publish();
                        Assert.assertTrue(
                                writer.usedBytes() + QwoFormat.WIRE_MARGIN_FIXED + QwoFormat.WIRE_MARGIN_PER_BLOCK <= capacity
                        );
                    }
                }
                Assert.assertTrue(expected.size() > 15);
                Assert.assertTrue("no narrow row fitted", expected.contains(narrow));

                QwoRecordReader reader = new QwoRecordReader();
                reader.of(mem, capacity);
                int b = reader.firstBlock();
                Assert.assertEquals(expected.size(), reader.rowCount(b));
                int chunk = reader.firstChunk(reader.columnDef(b, 0));
                int row = 0;
                boolean isShrunk = false;
                int previousCap = 0;
                while (chunk != 0) {
                    int cap = reader.chunkRowCap(chunk);
                    if (previousCap > 0 && cap != Math.min(2 * previousCap, QwoFormat.MAX_CHUNK_ROWS) && cap != previousCap) {
                        isShrunk = true;
                    }
                    previousCap = cap;
                    long offsets = reader.chunkOffsetsAddr(chunk, true);
                    long bytes = reader.chunkBytesAddr(chunk, true, cap);
                    for (int i = 0; i < cap && row < expected.size(); i++, row++) {
                        int start = Unsafe.getUnsafe().getInt(offsets + 4L * i);
                        int end = Unsafe.getUnsafe().getInt(offsets + 4L * (i + 1));
                        Assert.assertEquals("row " + row, expected.get(row), nameOf(bytes + start, end - start));
                    }
                    chunk = reader.nextChunk(chunk);
                }
                Assert.assertEquals(expected.size(), row);
                Assert.assertTrue("no chunk was shrunk", isShrunk);
            }
        });
    }

    @Test
    public void testColumnDefsFollowStagedBufferOrder() throws Exception {
        withRecord((mem, writer) -> {
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer tb = new QwpTableBuffer("ordered")) {
                append(writer, tb, dict, -1, t -> {
                    t.getOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true).addString("bee");
                    t.getOrCreateColumn("a", QwpConstants.TYPE_LONG, true).addLong(1);
                    t.getOrCreateColumn("flag", QwpConstants.TYPE_BOOLEAN, false).addBoolean(true);
                    t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP_NANOS).addLong(5);
                });
                writer.publish();
                QwoRecordReader reader = new QwoRecordReader();
                reader.of(mem, REGION);
                int b = reader.firstBlock();
                Assert.assertEquals(4, reader.colCount(b));
                String[] names = {"b", "a", "flag", ""};
                byte[] types = {QwpConstants.TYPE_VARCHAR, QwpConstants.TYPE_LONG, QwpConstants.TYPE_BOOLEAN, QwpConstants.TYPE_TIMESTAMP_NANOS};
                boolean[] bitmaps = {true, true, false, true};
                for (int col = 0; col < 4; col++) {
                    int def = reader.columnDef(b, col);
                    Assert.assertEquals(names[col], nameOf(reader.columnNameAddr(def), reader.columnNameLen(def)));
                    Assert.assertEquals(types[col], reader.columnType(def));
                    Assert.assertEquals(bitmaps[col], reader.columnUsesBitmap(def));
                    Assert.assertEquals(QwoFormat.PREFIX_NONE, reader.prefixKind(def));
                    Assert.assertEquals(-1, reader.prefix(def));
                    if (col < 3) {
                        Assert.assertEquals(reader.columnDef(b, col + 1), reader.nextColumnDef(def));
                    }
                }
                // the descriptor CRC is stored at publish and ignores the row word and D_NEXT
                int stored = Unsafe.getUnsafe().getInt(mem + b + QwoFormat.D_CRC);
                Assert.assertEquals(stored, QwoFormat.descriptorCrc(mem + b));
                append(writer, tb, dict, -1, t -> {
                    t.getOrCreateColumn("b", QwpConstants.TYPE_VARCHAR, true).addString("second");
                    t.getOrCreateColumn("a", QwpConstants.TYPE_LONG, true).addLong(2);
                    t.getOrCreateColumn("flag", QwpConstants.TYPE_BOOLEAN, false).addBoolean(false);
                    t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP_NANOS).addLong(6);
                });
                writer.publish();
                Assert.assertEquals(2, reader.rowCount(b));
                Assert.assertEquals(stored, Unsafe.getUnsafe().getInt(mem + b + QwoFormat.D_CRC));
                Assert.assertEquals(stored, QwoFormat.descriptorCrc(mem + b));
                // BOOLEAN values are bit-packed: row 0 true, row 1 false, in two chunks
                int flagDef = reader.columnDef(b, 2);
                int f0 = reader.firstChunk(flagDef);
                Assert.assertEquals(1, Unsafe.getUnsafe().getByte(reader.chunkValuesAddr(f0, QwpConstants.TYPE_BOOLEAN, false)) & 1);
                int f1 = reader.nextChunk(f0);
                Assert.assertEquals(0, Unsafe.getUnsafe().getByte(reader.chunkValuesAddr(f1, QwpConstants.TYPE_BOOLEAN, false)) & 1);
            }
        });
    }

    @Test
    public void testCrcStreamMatchesReferenceForEveryValueWidth() throws Exception {
        // QwoCrc folds a 1-, 4- or 8-byte value from a register and any other width from
        // memory. Every width, both bitmap modes, VARCHAR values of 1, 4 and 8 bytes behind
        // their length, SYMBOL varints of 1, 3 and 4 bytes, and a second block, whose CRC
        // starts again from INIT, against streams built independently of the writer.
        withRecord((mem, writer) -> {
            // the smallest id whose varint takes 4 bytes; ids below dictStartId belong to
            // earlier records, so this record needs no dictionary entry for it
            final int largeId = 1 << 21;
            writer.open(mem, REGION, QwpConstants.VERSION, largeId + 1, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            final String[] strings = {"abcd", "abcdefgh", null, "x"};
            final int[] symbols = {largeId, 5, -1, 20_000};
            QwoRecordReader reader = new QwoRecordReader();
            try (
                    QwpTableBuffer tb = new QwpTableBuffer("widths");
                    QwpTableBuffer second = new QwpTableBuffer("second");
                    NativeBufferWriter ref = new NativeBufferWriter();
                    NativeBufferWriter secondRef = new NativeBufferWriter()
            ) {
                for (int r = 0; r < 4; r++) {
                    final int row = r;
                    // row 2 leaves every bitmap column null
                    final boolean present = r != 2;
                    final long v = 0x0102030405060708L * (r + 1) ^ 0x8070605040302010L;
                    stage(tb, t -> {
                        t.getOrCreateColumn("b", QwpConstants.TYPE_BYTE, false).addByte((byte) v);
                        t.getOrCreateColumn("sh", QwpConstants.TYPE_SHORT, false).addShort((short) v);
                        t.getOrCreateColumn("ch", QwpConstants.TYPE_CHAR, false).addShort((short) (v >>> 16));
                        if (present) {
                            t.getOrCreateColumn("i", QwpConstants.TYPE_INT, true).addInt((int) v);
                            t.getOrCreateColumn("fl", QwpConstants.TYPE_FLOAT, true).addFloat(1.0f + row / 8.0f);
                            t.getOrCreateColumn("ip", QwpConstants.TYPE_IPv4, true).addIPv4((int) (v >>> 24) | 1);
                            t.getOrCreateColumn("g5", QwpConstants.TYPE_GEOHASH, true).addGeoHash(v & 0x1F, 5);
                            t.getOrCreateColumn("g30", QwpConstants.TYPE_GEOHASH, true).addGeoHash(v & 0x3FFF_FFFFL, 30);
                            t.getOrCreateColumn("g40", QwpConstants.TYPE_GEOHASH, true).addGeoHash(v & 0xFF_FFFF_FFFFL, 40);
                            t.getOrCreateColumn("d64", QwpConstants.TYPE_DECIMAL64, true).addDecimal64(Decimal64.fromLong(v >> 8, 3));
                            t.getOrCreateColumn("dt", QwpConstants.TYPE_DATE, true).addLong(v >>> 1);
                            t.getOrCreateColumn("ns", QwpConstants.TYPE_TIMESTAMP_NANOS, true).addLong(v >>> 2);
                            t.getOrCreateColumn("u", QwpConstants.TYPE_UUID, true).addUuid(v, ~v);
                            t.getOrCreateColumn("l256", QwpConstants.TYPE_LONG256, true).addLong256(v, v + 1, v + 2, v + 3);
                            t.getOrCreateColumn("d256", QwpConstants.TYPE_DECIMAL256, true)
                                    .addDecimal256(new Decimal256(0, row, v >>> 4, v, 2));
                            t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(strings[row]);
                            t.getOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, true)
                                    .addSymbolWithGlobalId("v" + symbols[row], symbols[row]);
                        }
                    });
                    Assert.assertEquals(QwoRecordWriter.APPEND_OK, writer.appendStagedRow(tb, dict, -1));
                    tb.clearStagedRow();

                    // reference stream, in column order
                    ref.putByte((byte) v);
                    ref.putShort((short) v);
                    ref.putShort((short) (v >>> 16));
                    putFlagged(ref, present, Integer.BYTES, v);
                    putFlagged(ref, present, Integer.BYTES, Float.floatToRawIntBits(1.0f + row / 8.0f));
                    putFlagged(ref, present, Integer.BYTES, (v >>> 24) | 1);
                    putFlagged(ref, present, 1, v & 0x1F);
                    putFlagged(ref, present, 4, v & 0x3FFF_FFFFL);
                    putFlagged(ref, present, 5, v & 0xFF_FFFF_FFFFL);
                    putFlagged(ref, present, Long.BYTES, v >> 8);
                    putFlagged(ref, present, Long.BYTES, v >>> 1);
                    putFlagged(ref, present, Long.BYTES, v >>> 2);
                    // UUID: lo, then hi
                    putFlagged(ref, present, Long.BYTES, ~v, v);
                    putFlagged(ref, present, Long.BYTES, v, v + 1, v + 2, v + 3);
                    // DECIMAL256 on the wire: ll, lh, hl, hh
                    putFlagged(ref, present, Long.BYTES, v, v >>> 4, row, 0);
                    putFlaggedUtf8(ref, strings[row]);
                    if (putPresence(ref, present)) {
                        ref.putVarint(symbols[row]);
                    }

                    writer.publish();
                    reader.of(mem, REGION);
                    int b = reader.firstBlock();
                    Assert.assertEquals(r + 1, reader.rowCount(b));
                    Assert.assertEquals(
                            "row crc after " + (r + 1) + " rows",
                            Crc32c.updateUnsafe(Crc32c.INIT, ref.getBufferPtr(), ref.getPosition()),
                            QwoFormat.crcOf(Unsafe.getUnsafe().getLong(mem + b + QwoFormat.D_ROW_WORD))
                    );
                }
                Assert.assertEquals("one block: the column set never changed", 1, writer.blockCount());

                for (int r = 0; r < 2; r++) {
                    final int row = r;
                    stage(second, t -> {
                        t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(strings[row]);
                        t.getOrCreateColumn("i", QwpConstants.TYPE_INT, true).addInt(row + 7);
                    });
                    Assert.assertEquals(QwoRecordWriter.APPEND_OK, writer.appendStagedRow(second, dict, -1));
                    second.clearStagedRow();
                    putFlaggedUtf8(secondRef, strings[row]);
                    putFlagged(secondRef, true, Integer.BYTES, row + 7);
                }
                writer.publish();
                reader.of(mem, REGION);
                int b2 = reader.nextBlock(reader.firstBlock());
                Assert.assertEquals(2, reader.rowCount(b2));
                Assert.assertEquals(
                        "the second block's row crc starts from INIT",
                        Crc32c.updateUnsafe(Crc32c.INIT, secondRef.getBufferPtr(), secondRef.getPosition()),
                        QwoFormat.crcOf(Unsafe.getUnsafe().getLong(mem + b2 + QwoFormat.D_ROW_WORD))
                );
                QwoValidator validator = new QwoValidator();
                Assert.assertTrue(validator.detail(), validator.validateOpen(mem, REGION));
                Assert.assertEquals(6, validator.committedRows());
            }
        });
    }

    @Test
    public void testCrcStreamMatchesReferenceOverRows() throws Exception {
        withRecord((mem, writer) -> {
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            for (int i = 0; i <= 300; i++) {
                dict.getOrAddSymbol("v" + i);
            }
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            final Double[] doubles = {1.5, null, -2.25, 4.0, null};
            final boolean[] flags = {true, false, true, true, false};
            final String[] strings = {"x", "", null, "hello world", "ü€"};
            final int[] symbols = {0, 1, -1, 0, 300};
            final long[] geohashes = {0xABC, -1, 0x123, 0xFFF, 0x001};
            final long[][] decimals = {{1, 2}, null, {0, -5}, {-1, 77}, {3, 4}};
            final long[][] arrays = {{1, 2, 3}, null, {}, {7}, null};
            final byte[] bytes = {1, 2, 3, 4, 5};
            QwoRecordReader reader = new QwoRecordReader();
            try (
                    QwpTableBuffer tb = new QwpTableBuffer("mixed");
                    NativeBufferWriter ref = new NativeBufferWriter()
            ) {
                int batchMax = -1;
                for (int r = 0; r < 5; r++) {
                    final int row = r;
                    if (symbols[r] > batchMax) {
                        batchMax = symbols[r];
                    }
                    stage(tb, t -> {
                        if (doubles[row] != null) {
                            t.getOrCreateColumn("d", QwpConstants.TYPE_DOUBLE, true).addDouble(doubles[row]);
                        }
                        t.getOrCreateColumn("flag", QwpConstants.TYPE_BOOLEAN, false).addBoolean(flags[row]);
                        if (strings[row] != null) {
                            t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(strings[row]);
                        }
                        if (symbols[row] >= 0) {
                            t.getOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, true)
                                    .addSymbolWithGlobalId("v" + symbols[row], symbols[row]);
                        }
                        if (geohashes[row] >= 0) {
                            t.getOrCreateColumn("g", QwpConstants.TYPE_GEOHASH, true).addGeoHash(geohashes[row], 12);
                        }
                        if (decimals[row] != null) {
                            t.getOrCreateColumn("dec", QwpConstants.TYPE_DECIMAL128, true)
                                    .addDecimal128(new Decimal128(decimals[row][0], decimals[row][1], 2));
                        }
                        if (arrays[row] != null) {
                            t.getOrCreateColumn("arr", QwpConstants.TYPE_LONG_ARRAY, true).addLongArray(arrays[row]);
                        }
                        t.getOrCreateColumn("b", QwpConstants.TYPE_BYTE, false).addByte(bytes[row]);
                        t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(1000L * (row + 1));
                    });
                    Assert.assertEquals(QwoRecordWriter.APPEND_OK, writer.appendStagedRow(tb, dict, batchMax));
                    tb.clearStagedRow();

                    // reference stream, independent of the writer
                    if (doubles[r] != null) {
                        ref.putByte((byte) 1);
                        ref.putDouble(doubles[r]);
                    } else {
                        ref.putByte((byte) 0);
                    }
                    ref.putByte(flags[r] ? (byte) 1 : (byte) 0);
                    if (strings[r] != null) {
                        byte[] utf8 = strings[r].getBytes(StandardCharsets.UTF_8);
                        ref.putByte((byte) 1);
                        ref.putInt(utf8.length);
                        for (byte x : utf8) {
                            ref.putByte(x);
                        }
                    } else {
                        ref.putByte((byte) 0);
                    }
                    if (symbols[r] >= 0) {
                        ref.putByte((byte) 1);
                        ref.putVarint(symbols[r]);
                    } else {
                        ref.putByte((byte) 0);
                    }
                    if (geohashes[r] >= 0) {
                        ref.putByte((byte) 1);
                        ref.putByte((byte) geohashes[r]);
                        ref.putByte((byte) (geohashes[r] >>> 8));
                    } else {
                        ref.putByte((byte) 0);
                    }
                    if (decimals[r] != null) {
                        ref.putByte((byte) 1);
                        ref.putLong(decimals[r][1]);
                        ref.putLong(decimals[r][0]);
                    } else {
                        ref.putByte((byte) 0);
                    }
                    if (arrays[r] != null) {
                        ref.putByte((byte) 1);
                        ref.putInt(1 + 4 + 8 * arrays[r].length);
                        ref.putByte((byte) 1);
                        ref.putInt(arrays[r].length);
                        for (long x : arrays[r]) {
                            ref.putLong(x);
                        }
                    } else {
                        ref.putByte((byte) 0);
                    }
                    ref.putByte(bytes[r]);
                    ref.putByte((byte) 1);
                    ref.putLong(1000L * (r + 1));

                    if (r == 1 || r == 4) {
                        writer.publish();
                        reader.of(mem, REGION);
                        int b = reader.firstBlock();
                        Assert.assertEquals(r + 1, reader.rowCount(b));
                        long word = Unsafe.getUnsafe().getLong(mem + b + QwoFormat.D_ROW_WORD);
                        Assert.assertEquals(
                                "row crc after " + (r + 1) + " rows",
                                Crc32c.updateUnsafe(Crc32c.INIT, ref.getBufferPtr(), ref.getPosition()),
                                QwoFormat.crcOf(word)
                        );
                    }
                }
                Assert.assertEquals(1, writer.blockCount());
                int b = reader.firstBlock();
                int geoDef = reader.columnDef(b, 4);
                Assert.assertEquals(QwpConstants.TYPE_GEOHASH, reader.columnType(geoDef));
                Assert.assertEquals(QwoFormat.PREFIX_GEOHASH, reader.prefixKind(geoDef));
                Assert.assertEquals(12, reader.prefix(geoDef));
                int decDef = reader.columnDef(b, 5);
                Assert.assertEquals(QwoFormat.PREFIX_DECIMAL, reader.prefixKind(decDef));
                Assert.assertEquals(2, reader.prefix(decDef));
                Assert.assertEquals(301, reader.dictCount());
                Assert.assertEquals(300, writer.dictEndId());
            }
        });
    }

    @Test
    public void testDictionaryEntriesAppendedOnceInIdOrder() throws Exception {
        withRecord((mem, writer) -> {
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            Assert.assertEquals(0, writer.dictStartId());
            Assert.assertEquals(-1, writer.dictEndId());
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer tb = new QwpTableBuffer("syms")) {
                final int alpha = dict.getOrAddSymbol("alpha");
                final int beta = dict.getOrAddSymbol("beta");
                append(writer, tb, dict, beta, t -> {
                    t.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true).addSymbolWithGlobalId("alpha", alpha);
                    t.getOrCreateColumn("s2", QwpConstants.TYPE_SYMBOL, true).addSymbolWithGlobalId("beta", beta);
                });
                Assert.assertEquals(1, writer.dictEndId());
                Assert.assertEquals(-1, writer.committedDictEndId());
                writer.publish();
                Assert.assertEquals(1, writer.committedDictEndId());
                final int gamma = dict.getOrAddSymbol("gamma");
                append(writer, tb, dict, gamma, t -> {
                    t.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true).addSymbolWithGlobalId("alpha", alpha);
                    t.getOrCreateColumn("s2", QwpConstants.TYPE_SYMBOL, true).addSymbolWithGlobalId("gamma", gamma);
                });
                // a row that mints nothing registers nothing
                append(writer, tb, dict, gamma, t -> {
                    t.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true).addSymbolWithGlobalId("beta", beta);
                    t.getOrCreateColumn("s2", QwpConstants.TYPE_SYMBOL, true).addSymbolWithGlobalId("alpha", alpha);
                });
                writer.publish();
                Assert.assertEquals(2, writer.dictEndId());

                reader.of(mem, REGION);
                Assert.assertEquals(0, reader.dictStartId());
                Assert.assertEquals(3, reader.dictCount());
                Assert.assertEquals(QwoFormat.HEADER_SIZE, reader.dictEntriesAddr() - mem);
                long expected = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
                try {
                    int n = QwoFormat.putString(expected, "alpha");
                    n += QwoFormat.putString(expected + n, "beta");
                    n += QwoFormat.putString(expected + n, "gamma");
                    Assert.assertEquals(n, reader.dictEntriesLen());
                    for (int i = 0; i < n; i++) {
                        Assert.assertEquals(Unsafe.getUnsafe().getByte(expected + i), Unsafe.getUnsafe().getByte(reader.dictEntriesAddr() + i));
                    }
                    long dictWord = Unsafe.getUnsafe().getLong(mem + QwoFormat.OFF_DICT_WORD);
                    Assert.assertEquals(3, (int) (dictWord >>> 32));
                    Assert.assertEquals(Crc32c.updateUnsafe(Crc32c.INIT, expected, n), (int) dictWord);
                } finally {
                    Unsafe.free(expected, 64, MemoryTag.NATIVE_DEFAULT);
                }
                // symbol values are varint global ids
                int b = reader.firstBlock();
                int s2 = reader.columnDef(b, 1);
                int c0 = reader.firstChunk(s2);
                Assert.assertEquals(beta, Unsafe.getUnsafe().getByte(reader.chunkValuesAddr(c0, QwpConstants.TYPE_SYMBOL, true)));
                long v1 = reader.chunkValuesAddr(reader.nextChunk(c0), QwpConstants.TYPE_SYMBOL, true);
                Assert.assertEquals(gamma, Unsafe.getUnsafe().getByte(v1));
                Assert.assertEquals(alpha, Unsafe.getUnsafe().getByte(v1 + 1));
            }
        });
    }

    @Test
    public void testDictionaryRegionFullReportsBytesNeeded() throws Exception {
        withRecord((mem, writer) -> {
            writer.open(mem, REGION, QwpConstants.VERSION, 5, 32);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            for (int i = 0; i < 5; i++) {
                dict.getOrAddSymbol("old" + i);
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 100; i++) {
                sb.append((char) ('a' + i % 26));
            }
            final String big = sb.toString();
            final int id = dict.getOrAddSymbol(big);
            Assert.assertEquals(5, id);
            try (QwpTableBuffer tb = new QwpTableBuffer("t"); QwpTableBuffer other = new QwpTableBuffer("u")) {
                int used = writer.usedBytes();
                stage(tb, t -> t.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true).addSymbolWithGlobalId(big, id));
                Assert.assertEquals(QwoRecordWriter.APPEND_NO_SPACE, writer.appendStagedRow(tb, dict, id));
                Assert.assertEquals(QwoFormat.stringBytes(big), writer.lastRefusedDictBytes());
                Assert.assertEquals(101, writer.lastRefusedDictBytes());
                Assert.assertEquals(0, writer.rowCount());
                Assert.assertEquals(0, writer.blockCount());
                Assert.assertEquals(used, writer.usedBytes());
                Assert.assertEquals(4, writer.dictEndId());
                tb.clearStagedRow();

                append(writer, other, dict, 4, t -> t.getOrCreateColumn("v", QwpConstants.TYPE_LONG, true).addLong(1));
                Assert.assertEquals(0, writer.lastRefusedDictBytes());
                Assert.assertEquals(4, writer.dictEndId());
            }
        });
    }

    @Test
    public void testFirstChunkHoldsOneRowAndIsNeverRelocated() throws Exception {
        withRecord((mem, writer) -> {
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            // after the first value, each value outgrows the bytes its chunk has left
            final int[] lengths = {1, 300, 3000, 30, 9000};
            try (QwpTableBuffer tb = new QwpTableBuffer("first")) {
                int c0 = 0;
                int c1 = 0;
                boolean isRelocated = false;
                for (int r = 0; r < lengths.length; r++) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < lengths[r]; i++) {
                        sb.append((char) ('a' + r));
                    }
                    final String value = sb.toString();
                    append(writer, tb, dict, -1, t -> t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(value));
                    writer.publish();
                    reader.of(mem, REGION);
                    int def = reader.columnDef(reader.firstBlock(), 0);
                    if (r == 0) {
                        c0 = reader.firstChunk(def);
                    }
                    Assert.assertEquals("row " + r, c0, reader.firstChunk(def));
                    Assert.assertEquals(1, reader.chunkRowCap(c0));
                    Assert.assertEquals("a", nameOf(reader.chunkBytesAddr(c0, true, 1), 1));
                    if (r == 1) {
                        c1 = reader.nextChunk(c0);
                    } else if (r == 2) {
                        isRelocated = reader.nextChunk(c0) != c1;
                    }
                }
                Assert.assertTrue("the second chunk was relocated, the first never", isRelocated);
            }
        });
    }

    @Test
    public void testFitShortcutRefusesARowWithoutColumnsLikeTheFullFit() throws Exception {
        // A row without columns plans no chunk, so the shortcut alone decides whether the
        // descriptor of its table's block fits: one-row tables without columns fill a small
        // record until the budget runs out, and both writers refuse the same table.
        assertMemoryLeak(() -> {
            final int capacity = 2048;
            // slack past the capacity, so a writer that ignored the budget writes in bounds
            final int region = 2 * capacity;
            long memA = Unsafe.calloc(region, MemoryTag.NATIVE_DEFAULT);
            long memB = Unsafe.calloc(region, MemoryTag.NATIVE_DEFAULT);
            ArrayList<QwpTableBuffer> tables = new ArrayList<>();
            try (QwoRecordWriter shortcut = new QwoRecordWriter(); QwoRecordWriter fullFit = new QwoRecordWriter()) {
                fullFit.setFitShortcutForTest(false);
                GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
                shortcut.open(memA, capacity, QwpConstants.VERSION, 0, 0);
                fullFit.open(memB, capacity, QwpConstants.VERSION, 0, 0);
                int appended = 0;
                while (true) {
                    String name = "z" + appended;
                    QwpTableBuffer tbA = new QwpTableBuffer(name);
                    tables.add(tbA);
                    QwpTableBuffer tbB = new QwpTableBuffer(name);
                    tables.add(tbB);
                    tbA.nextRow();
                    tbB.nextRow();
                    long budget = (long) capacity - shortcut.usedBytes() - QwoFormat.WIRE_MARGIN_FIXED
                            - (long) QwoFormat.WIRE_MARGIN_PER_BLOCK * (shortcut.blockCount() + 1)
                            - (QwoFormat.D_NAME + QwoFormat.stringBytes(name));
                    int code = shortcut.appendStagedRow(tbA, dict, -1);
                    Assert.assertEquals("table " + appended, fullFit.appendStagedRow(tbB, dict, -1), code);
                    Assert.assertEquals("table " + appended + ", budget " + budget,
                            budget >= 0 ? QwoRecordWriter.APPEND_OK : QwoRecordWriter.APPEND_NO_SPACE, code);
                    assertSameRecord(shortcut, fullFit, memA, memB, "table " + appended);
                    if (code == QwoRecordWriter.APPEND_NO_SPACE) {
                        break;
                    }
                    appended++;
                }
                Assert.assertTrue("tables before the refusal: " + appended, appended > 20);
                Assert.assertTrue(shortcut.usedBytes() <= capacity);
                // a second row of a table with a block plans nothing and still fits
                QwpTableBuffer firstA = tables.get(0);
                QwpTableBuffer firstB = tables.get(1);
                firstA.clearStagedRow();
                firstB.clearStagedRow();
                firstA.nextRow();
                firstB.nextRow();
                Assert.assertEquals(QwoRecordWriter.APPEND_OK, shortcut.appendStagedRow(firstA, dict, -1));
                Assert.assertEquals(QwoRecordWriter.APPEND_OK, fullFit.appendStagedRow(firstB, dict, -1));
                assertSameRecord(shortcut, fullFit, memA, memB, "second row of z0");
                Assert.assertEquals(appended + 1, shortcut.rowCount());
            } finally {
                for (int i = 0, n = tables.size(); i < n; i++) {
                    tables.get(i).close();
                }
                Unsafe.free(memA, region, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(memB, region, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testFitShortcutWritesTheSameRecordsAsTheFullFit() throws Exception {
        // A row whose columns all fit their current chunks skips fitChunks. A second writer
        // with the shortcut off is the reference: over rows that fit, rows that start chunks,
        // a VARCHAR value that relocates its chunk while the other columns fit, rollbacks
        // and the refusals of a full record, both writers write the same bytes.
        assertMemoryLeak(() -> {
            final int capacity = 24 * 1024;
            final int rows = 4000;
            final String[] strings = new String[rows];
            final Double[] doubles = new Double[rows];
            final int[] symbols = new int[rows];
            // rows 0-4 are fixed: row 2's 30 bytes overflow the 32-byte two-row chunk that
            // already holds row 1's 10, so its VARCHAR chunk relocates while every other
            // column has room; row 3 starts the four-row chunks and row 4 fits them
            final String[] head = {"a", "abcdefghij", "abcdefghijabcdefghijabcdefghij", "x", "y"};
            Random rnd = new Random(42);
            for (int r = 0; r < rows; r++) {
                if (r < head.length) {
                    strings[r] = head[r];
                    doubles[r] = r * 0.5;
                    symbols[r] = r;
                    continue;
                }
                int dice = rnd.nextInt(100);
                if (dice >= 15) {
                    int len = dice < 20 ? 200 + rnd.nextInt(100) : rnd.nextInt(60);
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < len; i++) {
                        sb.append((char) ('a' + rnd.nextInt(26)));
                    }
                    strings[r] = sb.toString();
                }
                doubles[r] = rnd.nextInt(5) == 0 ? null : rnd.nextDouble();
                symbols[r] = rnd.nextInt(10) == 0 ? -1 : rnd.nextInt(10);
            }
            long memA = Unsafe.calloc(capacity, MemoryTag.NATIVE_DEFAULT);
            long memB = Unsafe.calloc(capacity, MemoryTag.NATIVE_DEFAULT);
            try (
                    QwoRecordWriter shortcut = new QwoRecordWriter();
                    QwoRecordWriter fullFit = new QwoRecordWriter();
                    QwpTableBuffer tbA = new QwpTableBuffer("t");
                    QwpTableBuffer tbB = new QwpTableBuffer("t")
            ) {
                fullFit.setFitShortcutForTest(false);
                GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
                for (int i = 0; i < 10; i++) {
                    dict.getOrAddSymbol("s" + i);
                }
                shortcut.open(memA, capacity, QwpConstants.VERSION, 0, 256);
                fullFit.open(memB, capacity, QwpConstants.VERSION, 0, 256);
                QwoRecordReader reader = new QwoRecordReader();
                int longChunk1 = 0;
                int stringChunk1 = 0;
                int shortcutRows = 0;
                int chunkRows = 0;
                int seals = 0;
                for (int r = 0; r < rows; r++) {
                    stageMixedRow(tbA, r, strings, doubles, symbols);
                    stageMixedRow(tbB, r, strings, doubles, symbols);
                    int usedBefore = shortcut.usedBytes();
                    int code = shortcut.appendStagedRow(tbA, dict, 9);
                    Assert.assertEquals("row " + r, fullFit.appendStagedRow(tbB, dict, 9), code);
                    assertSameRecord(shortcut, fullFit, memA, memB, "row " + r);
                    if (code == QwoRecordWriter.APPEND_NO_SPACE) {
                        shortcut.publish();
                        fullFit.publish();
                        Assert.assertEquals(fullFit.seal(false), shortcut.seal(false));
                        assertSameBytes(memA, memB, capacity, "sealed record " + seals);
                        seals++;
                        // the ring hands out zeroed space
                        Unsafe.getUnsafe().setMemory(memA, capacity, (byte) 0);
                        Unsafe.getUnsafe().setMemory(memB, capacity, (byte) 0);
                        shortcut.open(memA, capacity, QwpConstants.VERSION, 0, 256);
                        fullFit.open(memB, capacity, QwpConstants.VERSION, 0, 256);
                        Assert.assertEquals(QwoRecordWriter.APPEND_OK, shortcut.appendStagedRow(tbA, dict, 9));
                        Assert.assertEquals(QwoRecordWriter.APPEND_OK, fullFit.appendStagedRow(tbB, dict, 9));
                        assertSameRecord(shortcut, fullFit, memA, memB, "row " + r + " in a fresh record");
                    } else if (shortcut.usedBytes() == usedBefore) {
                        shortcutRows++;
                    } else {
                        chunkRows++;
                    }
                    tbA.clearStagedRow();
                    tbB.clearStagedRow();
                    if (r == 4) {
                        Assert.assertEquals("row 4 fits the chunks row 3 started", usedBefore, shortcut.usedBytes());
                    }
                    if (r % 50 == 49) {
                        shortcut.rollback();
                        fullFit.rollback();
                        assertSameRecord(shortcut, fullFit, memA, memB, "rollback after row " + r);
                    } else if (r <= 2 || r % 3 == 2) {
                        shortcut.publish();
                        fullFit.publish();
                        assertSameRecord(shortcut, fullFit, memA, memB, "publish after row " + r);
                    }
                    if (r == 1 || r == 2) {
                        reader.of(memA, capacity);
                        int b = reader.firstBlock();
                        int longC1 = reader.nextChunk(reader.firstChunk(reader.columnDef(b, 0)));
                        int stringC1 = reader.nextChunk(reader.firstChunk(reader.columnDef(b, 1)));
                        if (r == 1) {
                            longChunk1 = longC1;
                            stringChunk1 = stringC1;
                        } else {
                            Assert.assertEquals("the LONG column keeps its chunk", longChunk1, longC1);
                            Assert.assertNotEquals("the VARCHAR chunk relocated", stringChunk1, stringC1);
                            Assert.assertTrue(usedBefore < shortcut.usedBytes());
                        }
                    }
                }
                Assert.assertTrue("rows that fit their chunks: " + shortcutRows, shortcutRows > 1000);
                Assert.assertTrue("rows that took chunks: " + chunkRows, chunkRows > 50);
                Assert.assertTrue("records sealed full: " + seals, seals > 2);
            } finally {
                Unsafe.free(memA, capacity, MemoryTag.NATIVE_DEFAULT);
                Unsafe.free(memB, capacity, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @Test
    public void testGeohashAndDecimalColumnsWithoutABitmapAreRefused() throws Exception {
        withRecord((mem, writer) -> {
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            final int used = writer.usedBytes();
            final byte[] types = {
                    QwpConstants.TYPE_GEOHASH, QwpConstants.TYPE_DECIMAL64, QwpConstants.TYPE_DECIMAL128, QwpConstants.TYPE_DECIMAL256
            };
            for (byte type : types) {
                for (int pass = 0; pass < 2; pass++) {
                    // a column without a bitmap stores a null as a sentinel value, which would carry no precision or scale
                    final boolean hasBitmap = pass == 1;
                    try (QwpTableBuffer tb = new QwpTableBuffer("prefixed")) {
                        stage(tb, t -> {
                            QwpTableBuffer.ColumnBuffer col = t.getOrCreateColumn("x", type, hasBitmap);
                            switch (type) {
                                case QwpConstants.TYPE_GEOHASH:
                                    col.addGeoHash(5, 20);
                                    break;
                                case QwpConstants.TYPE_DECIMAL64:
                                    col.addDecimal64(Decimal64.fromLong(1, 2));
                                    break;
                                case QwpConstants.TYPE_DECIMAL128:
                                    col.addDecimal128(Decimal128.fromLong(1, 2));
                                    break;
                                default:
                                    col.addDecimal256(Decimal256.fromLong(1, 2));
                                    break;
                            }
                        });
                        if (hasBitmap) {
                            Assert.assertEquals(QwoRecordWriter.APPEND_OK, writer.appendStagedRow(tb, dict, -1));
                            writer.rollback();
                        } else {
                            try {
                                writer.appendStagedRow(tb, dict, -1);
                                Assert.fail("type " + type + " without a null bitmap must be refused");
                            } catch (IllegalArgumentException expected) {
                                TestUtils.assertContains(expected.getMessage(), "column 'x'");
                                TestUtils.assertContains(expected.getMessage(), "null bitmap");
                            }
                        }
                        tb.clearStagedRow();
                    }
                    Assert.assertEquals(0, writer.blockCount());
                    Assert.assertEquals(0, writer.rowCount());
                    Assert.assertEquals(used, writer.usedBytes());
                }
            }
        });
    }

    @Test
    public void testNewColumnAllocatesSecondBlockForSameTable() throws Exception {
        withRecord((mem, writer) -> {
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer tb = new QwpTableBuffer("evolving")) {
                for (int r = 0; r < 3; r++) {
                    final long v = r;
                    append(writer, tb, dict, -1, t -> {
                        t.getOrCreateColumn("a", QwpConstants.TYPE_LONG, true).addLong(v);
                        t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(v);
                    });
                }
                writer.publish();
                append(writer, tb, dict, -1, t -> {
                    t.getOrCreateColumn("a", QwpConstants.TYPE_LONG, true).addLong(3);
                    t.getOrCreateColumn("b", QwpConstants.TYPE_DOUBLE, true).addDouble(0.5);
                    t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(3);
                });
                Assert.assertEquals(2, writer.blockCount());
                Assert.assertEquals(1, tb.qwoBlock());
                writer.publish();

                QwoRecordReader reader = new QwoRecordReader();
                reader.of(mem, REGION);
                Assert.assertEquals(2, reader.blockCount());
                int b0 = reader.firstBlock();
                int b1 = reader.nextBlock(b0);
                Assert.assertEquals(3, reader.rowCount(b0));
                Assert.assertEquals(2, reader.colCount(b0));
                Assert.assertEquals(1, reader.rowCount(b1));
                Assert.assertEquals(3, reader.colCount(b1));
                Assert.assertEquals(0, reader.nextBlock(b1));
                Assert.assertEquals("evolving", nameOf(reader.tableNameAddr(b1), reader.tableNameLen(b1)));
                Assert.assertEquals(Unsafe.getUnsafe().getInt(mem + b1 + QwoFormat.D_CRC), QwoFormat.descriptorCrc(mem + b1));
                int tsDef = reader.columnDef(b1, 1);
                Assert.assertEquals("", nameOf(reader.columnNameAddr(tsDef), reader.columnNameLen(tsDef)));
                int bDef = reader.columnDef(b1, 2);
                Assert.assertEquals("b", nameOf(reader.columnNameAddr(bDef), reader.columnNameLen(bDef)));
                Assert.assertEquals(0.5, Unsafe.getUnsafe().getDouble(reader.chunkValuesAddr(reader.firstChunk(bDef), QwpConstants.TYPE_DOUBLE, true)), 0.0);

                // the next row with the same columns lands in the second block
                append(writer, tb, dict, -1, t -> {
                    t.getOrCreateColumn("a", QwpConstants.TYPE_LONG, true).addLong(4);
                    t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(4);
                });
                writer.publish();
                Assert.assertEquals(2, writer.blockCount());
                Assert.assertEquals(2, reader.rowCount(b1));
            }
        });
    }

    @Test
    public void testNoSpaceLeavesRecordUnchanged() throws Exception {
        withRecord((mem, writer) -> {
            final int capacity = 64 * 1024;
            writer.open(mem, capacity, QwpConstants.VERSION, 0, capacity / 32);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 4096; i++) {
                sb.append((char) ('a' + i % 26));
            }
            final String wide = sb.toString();
            byte[] before = new byte[capacity];
            try (QwpTableBuffer tb = new QwpTableBuffer("wide")) {
                int appended = 0;
                while (true) {
                    final long ts = appended;
                    stage(tb, t -> {
                        t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(wide);
                        t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(ts);
                    });
                    int used = writer.usedBytes();
                    int rows = writer.rowCount();
                    for (int i = 0; i < capacity; i++) {
                        before[i] = Unsafe.getUnsafe().getByte(mem + i);
                    }
                    int result = writer.appendStagedRow(tb, dict, -1);
                    tb.clearStagedRow();
                    if (result == QwoRecordWriter.APPEND_NO_SPACE) {
                        Assert.assertEquals(used, writer.usedBytes());
                        Assert.assertEquals(rows, writer.rowCount());
                        Assert.assertEquals(0, writer.lastRefusedDictBytes());
                        for (int i = 0; i < capacity; i++) {
                            if (before[i] != Unsafe.getUnsafe().getByte(mem + i)) {
                                Assert.fail("refused append changed byte " + i);
                            }
                        }
                        break;
                    }
                    Assert.assertEquals(QwoRecordWriter.APPEND_OK, result);
                    appended++;
                    writer.publish();
                    Assert.assertTrue(
                            writer.usedBytes() + QwoFormat.WIRE_MARGIN_FIXED + QwoFormat.WIRE_MARGIN_PER_BLOCK * writer.blockCount() <= capacity
                    );
                }
                Assert.assertTrue("appended " + appended, appended >= 10);
                Assert.assertEquals(appended, writer.committedRowCount());
                QwoRecordReader reader = new QwoRecordReader();
                reader.of(mem, capacity);
                Assert.assertEquals(appended, reader.rowCount(reader.firstBlock()));
            }
        });
    }

    @Test
    public void testNullsSetBitmapBitsAndSkipValues() throws Exception {
        withRecord((mem, writer) -> {
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            final boolean[] isNull = {true, false, false, true, false, true};
            try (QwpTableBuffer tb = new QwpTableBuffer("sparse")) {
                for (int r = 0; r < isNull.length; r++) {
                    final int row = r;
                    append(writer, tb, dict, -1, t -> {
                        if (!isNull[row]) {
                            t.getOrCreateColumn("d", QwpConstants.TYPE_DOUBLE, true).addDouble(row * 10.0);
                        } else if (row == 0) {
                            // an explicit null; later null rows are padded by nextRow()
                            t.getOrCreateColumn("d", QwpConstants.TYPE_DOUBLE, true).addNull();
                        }
                        t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(row);
                    });
                    if (r == 2) {
                        writer.publish();
                    }
                }
                writer.publish();
                QwoRecordReader reader = new QwoRecordReader();
                reader.of(mem, REGION);
                int b = reader.firstBlock();
                Assert.assertEquals(6, reader.rowCount(b));
                Assert.assertEquals(1, reader.blockCount());
                int def = reader.columnDef(b, 0);
                Assert.assertEquals(QwpConstants.TYPE_DOUBLE, reader.columnType(def));
                Assert.assertTrue(reader.columnUsesBitmap(def));
                int chunk = reader.firstChunk(def);
                int row = 0;
                int values = 0;
                while (chunk != 0) {
                    int cap = reader.chunkRowCap(chunk);
                    long bitmap = reader.chunkBitmapAddr(chunk, true);
                    long vals = reader.chunkValuesAddr(chunk, QwpConstants.TYPE_DOUBLE, true);
                    int slot = 0;
                    for (int i = 0; i < cap && row < 6; i++, row++) {
                        boolean bit = (Unsafe.getUnsafe().getByte(bitmap + (i >>> 3)) & (1 << (i & 7))) != 0;
                        Assert.assertEquals("row " + row, isNull[row], bit);
                        if (!bit) {
                            Assert.assertEquals(row * 10.0, Unsafe.getUnsafe().getDouble(vals + 8L * slot), 0.0);
                            slot++;
                            values++;
                        }
                    }
                    chunk = reader.nextChunk(chunk);
                }
                Assert.assertEquals(6, row);
                Assert.assertEquals(3, values);
            }
        });
    }

    @Test
    public void testPrefixesUpToTheirLargestValuesTakeOneByte() throws Exception {
        withRecord((mem, writer) -> {
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer tb = new QwpTableBuffer("prefixes")) {
                append(writer, tb, dict, -1, t -> {
                    t.getOrCreateColumn("g", QwpConstants.TYPE_GEOHASH, true).addGeoHash(1, 60);
                    t.getOrCreateColumn("d64", QwpConstants.TYPE_DECIMAL64, true).addDecimal64(Decimal64.fromLong(1, Decimal64.MAX_SCALE));
                    t.getOrCreateColumn("d128", QwpConstants.TYPE_DECIMAL128, true).addDecimal128(Decimal128.fromLong(1, Decimal128.MAX_SCALE));
                    t.getOrCreateColumn("d256", QwpConstants.TYPE_DECIMAL256, true).addDecimal256(Decimal256.fromLong(1, Decimal256.MAX_SCALE));
                });
                writer.publish();
                QwoRecordReader reader = new QwoRecordReader();
                reader.of(mem, REGION);
                int b = reader.firstBlock();
                final int[] expected = {60, Decimal64.MAX_SCALE, Decimal128.MAX_SCALE, Decimal256.MAX_SCALE};
                for (int col = 0; col < expected.length; col++) {
                    int def = reader.columnDef(b, col);
                    Assert.assertEquals(expected[col], reader.prefix(def));
                    // the largest precision and scale fit the one-byte varint form the writer stores
                    long tail = reader.columnNameAddr(def) + reader.columnNameLen(def);
                    Assert.assertEquals(expected[col], Unsafe.getUnsafe().getByte(tail + QwoFormat.COL_PREFIX));
                    Assert.assertEquals(0, Unsafe.getUnsafe().getByte(tail + QwoFormat.COL_PREFIX + 1));
                }
            }
        });
    }

    @Test
    public void testRelocatedChunkIsRelinkedAtPublish() throws Exception {
        withRecord((mem, writer) -> {
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 100; i++) {
                sb.append((char) ('A' + i % 26));
            }
            final String[] values = {"a", "bb", sb.toString()};
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer tb = new QwpTableBuffer("reloc")) {
                append(writer, tb, dict, -1, t -> t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(values[0]));
                append(writer, tb, dict, -1, t -> t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(values[1]));
                writer.publish();
                reader.of(mem, REGION);
                int b = reader.firstBlock();
                int def = reader.columnDef(b, 0);
                int c0 = reader.firstChunk(def);
                int oldC1 = reader.nextChunk(c0);
                Assert.assertEquals(2, reader.chunkRowCap(oldC1));
                int oldByteCap = reader.chunkByteCap(oldC1);
                Assert.assertTrue(oldByteCap < 2 + 100);

                append(writer, tb, dict, -1, t -> t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(values[2]));
                // not published yet: the published chain still ends in the old chunk
                Assert.assertEquals(oldC1, reader.nextChunk(c0));
                Assert.assertEquals(2, reader.rowCount(b));
                writer.publish();

                Assert.assertEquals(3, reader.rowCount(b));
                Assert.assertEquals(c0, reader.firstChunk(def));
                int newC1 = reader.nextChunk(c0);
                Assert.assertNotEquals(oldC1, newC1);
                Assert.assertTrue(newC1 > oldC1);
                Assert.assertEquals(2, reader.chunkRowCap(newC1));
                Assert.assertEquals(0, reader.nextChunk(newC1));
                Assert.assertTrue(reader.chunkByteCap(newC1) >= 2 + 100);

                long offsets = reader.chunkOffsetsAddr(newC1, true);
                long bytesAddr = reader.chunkBytesAddr(newC1, true, reader.chunkRowCap(newC1));
                Assert.assertEquals(0, Unsafe.getUnsafe().getInt(offsets));
                Assert.assertEquals(2, Unsafe.getUnsafe().getInt(offsets + 4));
                Assert.assertEquals(102, Unsafe.getUnsafe().getInt(offsets + 8));
                Assert.assertEquals("bb", nameOf(bytesAddr, 2));
                Assert.assertEquals(values[2], nameOf(bytesAddr + 2, 100));
                Assert.assertEquals("a", nameOf(reader.chunkBytesAddr(c0, true, 1), 1));
            }
        });
    }

    @Test
    public void testRollbackDropsUncommittedRowsAndChunks() throws Exception {
        withRecord((mem, writer) -> {
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer tb = new QwpTableBuffer("rb"); QwpTableBuffer late = new QwpTableBuffer("late")) {
                appendLong(writer, tb, dict, 0);
                appendLong(writer, tb, dict, 1);
                writer.publish();
                int published = writer.usedBytes();
                QwoRecordReader reader = new QwoRecordReader();
                reader.of(mem, REGION);
                Assert.assertEquals(published, reader.usedBytes());

                appendLong(writer, tb, dict, 2);
                appendLong(writer, tb, dict, 3);
                appendLong(writer, tb, dict, 4);
                // a table first seen after the publish gets a block that rollback forgets
                appendLongTo(writer, late, dict, 99);
                Assert.assertEquals(6, writer.rowCount());
                Assert.assertEquals(2, writer.blockCount());
                Assert.assertTrue(writer.usedBytes() > published);
                Assert.assertTrue(writer.hasUncommittedRows());

                writer.rollback();
                Assert.assertEquals(2, writer.rowCount());
                Assert.assertEquals(2, writer.committedRowCount());
                Assert.assertEquals(1, writer.blockCount());
                Assert.assertEquals(published, writer.usedBytes());
                Assert.assertFalse(writer.hasUncommittedRows());
                Assert.assertTrue(writer.hasCommittedRows());
                Assert.assertEquals(-1, late.qwoBlock());
                Assert.assertEquals(0, tb.qwoBlock());

                // row 2 reuses the free slot of the 2-row chunk, row 3 allocates where the rolled-back chunk was
                appendLong(writer, tb, dict, 100);
                Assert.assertEquals(published, writer.usedBytes());
                appendLong(writer, tb, dict, 101);
                writer.publish();

                reader.of(mem, REGION);
                Assert.assertEquals(1, reader.blockCount());
                int b = reader.firstBlock();
                Assert.assertEquals(4, reader.rowCount(b));
                int def = reader.columnDef(b, 0);
                int c0 = reader.firstChunk(def);
                int c1 = reader.nextChunk(c0);
                int c2 = reader.nextChunk(c1);
                Assert.assertEquals(published, c2);
                Assert.assertEquals(4, reader.chunkRowCap(c2));
                Assert.assertEquals(0, Unsafe.getUnsafe().getLong(reader.chunkValuesAddr(c0, QwpConstants.TYPE_LONG, true)));
                long v1 = reader.chunkValuesAddr(c1, QwpConstants.TYPE_LONG, true);
                Assert.assertEquals(1, Unsafe.getUnsafe().getLong(v1));
                Assert.assertEquals(100, Unsafe.getUnsafe().getLong(v1 + 8));
                Assert.assertEquals(101, Unsafe.getUnsafe().getLong(reader.chunkValuesAddr(c2, QwpConstants.TYPE_LONG, true)));

                // the forgotten table gets a fresh block again
                appendLongTo(writer, late, dict, 7);
                Assert.assertEquals(1, late.qwoBlock());
                Assert.assertEquals(2, writer.blockCount());
            }
        });
    }

    @Test
    public void testSealLeavesTheBytesAboveTheHighWaterMarkUntouched() throws Exception {
        withRecord((mem, writer) -> {
            // withRecord poisons the whole region; in a ring reservation those bytes are zero
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer tb = new QwpTableBuffer("s")) {
                appendLong(writer, tb, dict, 1);
                appendLong(writer, tb, dict, 2);
                writer.publish();
                int committed = writer.usedBytes();
                // row 3 fills the 2-row chunk, row 4 allocates a 4-row chunk past the published
                // extent; the seal rolls both back
                appendLong(writer, tb, dict, 3);
                appendLong(writer, tb, dict, 4);
                int mark = writer.maxUsedBytes();
                Assert.assertEquals(writer.usedBytes(), mark);
                Assert.assertTrue(mark > committed);

                int used = writer.seal(false);
                Assert.assertEquals(committed, used);
                assertFilled(mem, used, mark, (byte) 0);
                assertFilled(mem, mark, REGION, POISON);
            }
        });
    }

    @Test
    public void testSealWritesFlagsUsedBytesAndZeroesTail() throws Exception {
        withRecord((mem, writer) -> {
            // a ring reservation is zero; the seal zeroes only what the appends dirtied
            Unsafe.getUnsafe().setMemory(mem, REGION, (byte) 0);
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            QwoRecordReader reader = new QwoRecordReader();
            reader.of(mem, REGION);
            Assert.assertEquals(QwoFormat.MAGIC_RECORD, Unsafe.getUnsafe().getInt(mem));
            Assert.assertEquals(QwpConstants.VERSION, reader.version());
            Assert.assertEquals(QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_DELTA_SYMBOL_DICT, reader.flags());
            Assert.assertEquals(REGION, reader.capacity());
            int headerCrc = Unsafe.getUnsafe().getInt(mem + QwoFormat.OFF_HEADER_CRC);
            Assert.assertEquals(QwoFormat.headerCrc(mem), headerCrc);

            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer tb = new QwpTableBuffer("s")) {
                appendLong(writer, tb, dict, 1);
                appendLong(writer, tb, dict, 2);
                writer.publish();
                int committed = writer.usedBytes();
                // unpublished rows are not part of the sealed record; row 3 allocates a 4-row chunk
                appendLong(writer, tb, dict, 3);
                appendLong(writer, tb, dict, 4);
                Assert.assertTrue(writer.usedBytes() > committed);

                int used = writer.seal(true);
                Assert.assertFalse(writer.isOpen());
                Assert.assertEquals(committed, used);
                Assert.assertEquals(2, writer.rowCount());
                reader.of(mem, used);
                Assert.assertEquals(
                        QwpConstants.FLAG_DEFER_COMMIT | QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_DELTA_SYMBOL_DICT,
                        reader.flags()
                );
                Assert.assertEquals(used, reader.usedBytes());
                Assert.assertEquals(1, reader.blockCount());
                Assert.assertEquals(2, reader.rowCount(reader.firstBlock()));
                for (int i = used; i < REGION; i++) {
                    if (Unsafe.getUnsafe().getByte(mem + i) != 0) {
                        Assert.fail("non-zero byte at " + i);
                    }
                }
                // the header CRC covers only fields fixed at open, so neither publish nor seal rewrote it
                Assert.assertEquals(headerCrc, Unsafe.getUnsafe().getInt(mem + QwoFormat.OFF_HEADER_CRC));
                Assert.assertEquals(QwoFormat.headerCrc(mem), headerCrc);

                // a second record over the same memory, sealed without the flag
                long epoch = writer.epoch();
                writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
                Assert.assertEquals(epoch + 1, writer.epoch());
                Assert.assertEquals(0, writer.rowCount());
                Assert.assertEquals("open resets the mark", QwoFormat.HEADER_SIZE + QwoFormat.DEFAULT_DICT_RESERVE,
                        writer.maxUsedBytes());
                appendLong(writer, tb, dict, 5);
                Assert.assertEquals(0, tb.qwoBlock());
                writer.publish();
                writer.seal(false);
                reader.of(mem, REGION);
                Assert.assertEquals(QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_DELTA_SYMBOL_DICT, reader.flags());
                Assert.assertEquals(1, reader.rowCount(reader.firstBlock()));
            }
        });
    }

    @Test
    public void testSealZeroesWhatARolledBackRelocationDirtied() throws Exception {
        withRecord((mem, writer) -> {
            // a ring reservation is zero past what the writer writes
            Unsafe.getUnsafe().setMemory(mem, REGION, (byte) 0);
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 100; i++) {
                sb.append((char) ('A' + i % 26));
            }
            final String big = sb.toString();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer tb = new QwpTableBuffer("reloc")) {
                append(writer, tb, dict, -1, t -> t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString("a"));
                append(writer, tb, dict, -1, t -> t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString("bb"));
                writer.publish();
                int committed = writer.usedBytes();
                reader.of(mem, REGION);
                int def = reader.columnDef(reader.firstBlock(), 0);
                int c0 = reader.firstChunk(def);
                int c1 = reader.nextChunk(c0);

                // the third value overflows the published two-row chunk: its copy goes past the
                // published extent, and the link to it waits for a publish that never comes
                append(writer, tb, dict, -1, t -> t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(big));
                int mark = writer.usedBytes();
                Assert.assertTrue("the relocated chunk lies past the published extent", mark > committed);
                Assert.assertEquals(mark, writer.maxUsedBytes());
                writer.rollback();
                Assert.assertEquals(committed, writer.usedBytes());
                Assert.assertTrue("the rolled-back relocation left bytes past usedBytes", hasNonZero(mem, committed, mark));

                int used = writer.seal(false);
                Assert.assertEquals(committed, used);
                Assert.assertEquals(mark, writer.maxUsedBytes());
                assertFilled(mem, used, REGION, (byte) 0);
                // the published chain is the one before the relocation
                Assert.assertEquals(c1, reader.nextChunk(c0));
                Assert.assertEquals(0, reader.nextChunk(c1));
                long offsets = reader.chunkOffsetsAddr(c1, true);
                Assert.assertEquals(2, Unsafe.getUnsafe().getInt(offsets + 4));
                Assert.assertEquals("bb", nameOf(reader.chunkBytesAddr(c1, true, reader.chunkRowCap(c1)), 2));
                QwoValidator validator = new QwoValidator();
                Assert.assertTrue(validator.detail(), validator.validateSealed(mem, used));
                Assert.assertEquals(2, validator.committedRows());
            }
        });
    }

    @Test
    public void testSealZeroesWhatRolledBackAppendsDirtied() throws Exception {
        withRecord((mem, writer) -> {
            // a ring reservation is zero past what the writer writes
            Unsafe.getUnsafe().setMemory(mem, REGION, (byte) 0);
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            Assert.assertEquals("the header and the dictionary reserve lie below the mark",
                    QwoFormat.HEADER_SIZE + QwoFormat.DEFAULT_DICT_RESERVE, writer.maxUsedBytes());
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer tb = new QwpTableBuffer("rb"); QwpTableBuffer late = new QwpTableBuffer("late")) {
                appendLong(writer, tb, dict, 0);
                appendLong(writer, tb, dict, 1);
                writer.publish();
                int committed = writer.usedBytes();
                Assert.assertEquals(committed, writer.maxUsedBytes());

                // row 3 allocates a chunk and the new table a block past the published extent
                appendLong(writer, tb, dict, 2);
                appendLong(writer, tb, dict, 3);
                appendLongTo(writer, late, dict, 99);
                int mark = writer.usedBytes();
                Assert.assertEquals(mark, writer.maxUsedBytes());
                writer.rollback();
                Assert.assertEquals(committed, writer.usedBytes());
                Assert.assertEquals("a rollback does not lower the mark", mark, writer.maxUsedBytes());
                Assert.assertTrue("the rolled-back appends left bytes past usedBytes", hasNonZero(mem, committed, mark));

                // appended again they dirty less than the first time, and the seal rolls them back too
                appendLong(writer, tb, dict, 4);
                appendLong(writer, tb, dict, 5);
                Assert.assertTrue(writer.usedBytes() > committed);
                Assert.assertTrue(writer.usedBytes() < mark);
                Assert.assertEquals(mark, writer.maxUsedBytes());

                int used = writer.seal(false);
                Assert.assertEquals(committed, used);
                Assert.assertEquals(mark, writer.maxUsedBytes());
                assertFilled(mem, used, mark, (byte) 0);
                // nothing above the mark was written, so the whole tail is zero, as the ring needs
                assertFilled(mem, mark, REGION, (byte) 0);
                QwoValidator validator = new QwoValidator();
                Assert.assertTrue(validator.detail(), validator.validateSealed(mem, used));
                Assert.assertEquals(2, validator.committedRows());
            }
        });
    }

    @Test
    public void testVariableSizeChunkRebaseState() throws Exception {
        withRecord((mem, writer) -> {
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            final int rows = 40;
            final String[] values = new String[rows];
            long total = 0;
            for (int r = 0; r < rows; r++) {
                if (r % 7 == 3) {
                    continue;
                }
                int len = (r * 37) % 101;
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < len; i++) {
                    sb.append((char) ('a' + (r + i) % 26));
                }
                values[r] = sb.toString();
                total += len;
            }
            try (QwpTableBuffer tb = new QwpTableBuffer("vs")) {
                for (int r = 0; r < rows; r++) {
                    final int row = r;
                    append(writer, tb, dict, -1, t -> {
                        if (values[row] != null) {
                            t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(values[row]);
                        }
                        t.getOrCreateColumn("n", QwpConstants.TYPE_LONG, true).addLong(row);
                    });
                    if (r % 5 == 4) {
                        writer.publish();
                    }
                }
                QwoRecordReader reader = new QwoRecordReader();
                reader.of(mem, REGION);
                int b = reader.firstBlock();
                Assert.assertEquals(rows, reader.rowCount(b));
                int def = reader.columnDef(b, 0);
                Assert.assertEquals(QwpConstants.TYPE_VARCHAR, reader.columnType(def));
                int chunk = reader.firstChunk(def);
                int row = 0;
                long sum = 0;
                int chunks = 0;
                while (chunk != 0) {
                    int cap = reader.chunkRowCap(chunk);
                    long bitmap = reader.chunkBitmapAddr(chunk, true);
                    long offsets = reader.chunkOffsetsAddr(chunk, true);
                    long bytes = reader.chunkBytesAddr(chunk, true, cap);
                    Assert.assertEquals(0, Unsafe.getUnsafe().getInt(offsets));
                    int slot = 0;
                    for (int i = 0; i < cap && row < rows; i++, row++) {
                        boolean isNull = (Unsafe.getUnsafe().getByte(bitmap + (i >>> 3)) & (1 << (i & 7))) != 0;
                        Assert.assertEquals("row " + row, values[row] == null, isNull);
                        if (!isNull) {
                            int start = Unsafe.getUnsafe().getInt(offsets + 4L * slot);
                            int end = Unsafe.getUnsafe().getInt(offsets + 4L * (slot + 1));
                            Assert.assertTrue(end <= reader.chunkByteCap(chunk));
                            Assert.assertEquals("row " + row, values[row], nameOf(bytes + start, end - start));
                            slot++;
                        }
                    }
                    sum += Unsafe.getUnsafe().getInt(offsets + 4L * slot);
                    chunks++;
                    chunk = reader.nextChunk(chunk);
                }
                Assert.assertEquals(rows, row);
                Assert.assertEquals(total, sum);
                Assert.assertTrue(chunks > 3);
            }
        });
    }

    private static void append(
            QwoRecordWriter writer,
            QwpTableBuffer tb,
            GlobalSymbolDictionary dict,
            int batchMaxSymbolId,
            Consumer<QwpTableBuffer> row
    ) {
        stage(tb, row);
        Assert.assertEquals(QwoRecordWriter.APPEND_OK, writer.appendStagedRow(tb, dict, batchMaxSymbolId));
        tb.clearStagedRow();
        Assert.assertEquals(0, tb.getRowCount());
    }

    private static void appendLong(QwoRecordWriter writer, QwpTableBuffer tb, GlobalSymbolDictionary dict, long value) {
        append(writer, tb, dict, -1, t -> t.getOrCreateColumn("v", QwpConstants.TYPE_LONG, true).addLong(value));
    }

    private static void appendLongTo(QwoRecordWriter writer, QwpTableBuffer tb, GlobalSymbolDictionary dict, long value) {
        append(writer, tb, dict, -1, t -> t.getOrCreateColumn("w", QwpConstants.TYPE_LONG, true).addLong(value));
    }

    private static void assertFilled(long mem, int from, int to, byte value) {
        for (int i = from; i < to; i++) {
            byte b = Unsafe.getUnsafe().getByte(mem + i);
            if (b != value) {
                Assert.fail("byte " + b + " at " + i + ", expected " + value + " in [" + from + ", " + to + ')');
            }
        }
    }

    private static void assertSameBytes(long memA, long memB, int len, String label) {
        int i = 0;
        while (i + Long.BYTES <= len && Unsafe.getUnsafe().getLong(memA + i) == Unsafe.getUnsafe().getLong(memB + i)) {
            i += Long.BYTES;
        }
        for (; i < len; i++) {
            byte a = Unsafe.getUnsafe().getByte(memA + i);
            byte b = Unsafe.getUnsafe().getByte(memB + i);
            if (a != b) {
                Assert.fail(label + ": byte " + a + " != " + b + " at " + i);
            }
        }
    }

    private static void assertSameRecord(QwoRecordWriter a, QwoRecordWriter b, long memA, long memB, String label) {
        Assert.assertEquals(label, b.usedBytes(), a.usedBytes());
        Assert.assertEquals(label, b.maxUsedBytes(), a.maxUsedBytes());
        Assert.assertEquals(label, b.rowCount(), a.rowCount());
        Assert.assertEquals(label, b.blockCount(), a.blockCount());
        Assert.assertEquals(label, b.dictEntriesLen(), a.dictEntriesLen());
        assertSameBytes(memA, memB, a.maxUsedBytes(), label);
    }

    private static boolean hasNonZero(long mem, int from, int to) {
        for (int i = from; i < to; i++) {
            if (Unsafe.getUnsafe().getByte(mem + i) != 0) {
                return true;
            }
        }
        return false;
    }

    private static String nameOf(long addr, int len) {
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = Unsafe.getUnsafe().getByte(addr + i);
        }
        return new String(b, StandardCharsets.UTF_8);
    }

    /**
     * Appends a column's reference CRC contribution: its presence byte and, when
     * present, the low {@code width} bytes of each word in little-endian order.
     */
    private static void putFlagged(NativeBufferWriter ref, boolean present, int width, long... words) {
        if (putPresence(ref, present)) {
            for (long word : words) {
                for (int i = 0; i < width; i++) {
                    ref.putByte((byte) (word >>> (8 * i)));
                }
            }
        }
    }

    /**
     * Appends a VARCHAR column's reference CRC contribution: its presence byte
     * and, for a non-null value, the {@code u32} UTF-8 length and the bytes.
     */
    private static void putFlaggedUtf8(NativeBufferWriter ref, String value) {
        if (putPresence(ref, value != null)) {
            byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
            ref.putInt(utf8.length);
            for (byte x : utf8) {
                ref.putByte(x);
            }
        }
    }

    private static boolean putPresence(NativeBufferWriter ref, boolean present) {
        ref.putByte(present ? (byte) 1 : (byte) 0);
        return present;
    }

    private static void stage(QwpTableBuffer tb, Consumer<QwpTableBuffer> row) {
        row.accept(tb);
        tb.nextRow();
        Assert.assertEquals(1, tb.getRowCount());
    }

    /**
     * Stages row {@code r} of the shortcut test: LONG, VARCHAR, DOUBLE, SYMBOL,
     * BOOLEAN (no bitmap) and INT, the nullable ones left out when null.
     */
    private static void stageMixedRow(QwpTableBuffer t, int r, String[] strings, Double[] doubles, int[] symbols) {
        t.getOrCreateColumn("l", QwpConstants.TYPE_LONG, true).addLong(r);
        if (strings[r] != null) {
            t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(strings[r]);
        }
        if (doubles[r] != null) {
            t.getOrCreateColumn("d", QwpConstants.TYPE_DOUBLE, true).addDouble(doubles[r]);
        }
        if (symbols[r] >= 0) {
            t.getOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, true).addSymbolWithGlobalId("s" + symbols[r], symbols[r]);
        }
        t.getOrCreateColumn("f", QwpConstants.TYPE_BOOLEAN, false).addBoolean(r % 3 == 0);
        t.getOrCreateColumn("i", QwpConstants.TYPE_INT, true).addInt(r * 31);
        t.nextRow();
    }

    private static void withRecord(RecordCode code) throws Exception {
        assertMemoryLeak(() -> {
            long mem = Unsafe.malloc(REGION, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter()) {
                // garbage, so nothing relies on zeroed memory
                Unsafe.getUnsafe().setMemory(mem, REGION, POISON);
                code.run(mem, writer);
            } finally {
                Unsafe.free(mem, REGION, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @FunctionalInterface
    private interface RecordCode {
        void run(long mem, QwoRecordWriter writer) throws Exception;
    }
}
