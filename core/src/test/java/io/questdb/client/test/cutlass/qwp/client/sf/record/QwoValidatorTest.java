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
import io.questdb.client.cutlass.qwp.client.sf.record.QwoFormat;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordReader;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordWriter;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoValidator;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.function.Consumer;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.*;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwoValidatorTest {
    private static final byte[] EVERY_TYPE = {
            TYPE_BOOLEAN, TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG, TYPE_FLOAT, TYPE_DOUBLE, TYPE_SYMBOL,
            TYPE_TIMESTAMP, TYPE_DATE, TYPE_UUID, TYPE_LONG256, TYPE_GEOHASH, TYPE_VARCHAR, TYPE_TIMESTAMP_NANOS,
            TYPE_DOUBLE_ARRAY, TYPE_LONG_ARRAY, TYPE_DECIMAL64, TYPE_DECIMAL128, TYPE_DECIMAL256, TYPE_CHAR,
            TYPE_BINARY, TYPE_IPv4
    };
    private static final byte FLAGS_OPEN = (byte) (FLAG_GORILLA | FLAG_DELTA_SYMBOL_DICT);
    private static final int REGION = 256 * 1024;
    private static final long TS_BASE = 1_700_000_000_000_000L;

    @Test
    public void testBadFlagsAreInvalid() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                for (int r = 0; r < 3; r++) {
                    appendTrade(writer, tb, dict, r);
                }
                writer.publish();
                int used = writer.seal(false);
                final byte[] bad = {
                        0,
                        FLAG_GORILLA,
                        (byte) (FLAG_DELTA_SYMBOL_DICT | FLAG_DEFER_COMMIT),
                        (byte) (FLAGS_OPEN | FLAG_ZSTD),
                        (byte) (FLAGS_OPEN | 0x02),
                        (byte) (FLAGS_OPEN | 0x80)
                };
                for (byte flags : bad) {
                    Unsafe.getUnsafe().putByte(mem + QwoFormat.OFF_FLAGS, flags);
                    assertInvalid(validator.validateSealed(mem, used), validator, "flags");
                    assertInvalid(validator.validateOpen(mem, REGION), validator, "flags");
                }
                final byte[] good = {FLAGS_OPEN, (byte) (FLAGS_OPEN | FLAG_DEFER_COMMIT)};
                for (byte flags : good) {
                    Unsafe.getUnsafe().putByte(mem + QwoFormat.OFF_FLAGS, flags);
                    assertValid(validator.validateSealed(mem, used), validator);
                    assertValid(validator.validateOpen(mem, REGION), validator);
                    Assert.assertEquals(3, validator.committedRows());
                }
            }
        });
    }

    @Test
    public void testBlockChainCycleIsInvalid() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer a = new QwpTableBuffer("a"); QwpTableBuffer b = new QwpTableBuffer("b")) {
                appendTrade(writer, a, dict, 0);
                appendTrade(writer, b, dict, 1);
                writer.publish();
                reader.of(mem, REGION);
                int b0 = reader.firstBlock();
                int b1 = reader.nextBlock(b0);
                Assert.assertNotEquals(0, b1);
                Assert.assertEquals(0, reader.nextBlock(b1));
                assertValid(validator.validateOpen(mem, REGION), validator);

                Unsafe.getUnsafe().putInt(mem + b1 + QwoFormat.D_NEXT, b0);
                assertInvalid(validator.validateOpen(mem, REGION), validator, "cycles");
                Unsafe.getUnsafe().putInt(mem + b1 + QwoFormat.D_NEXT, b1);
                assertInvalid(validator.validateOpen(mem, REGION), validator, "cycles");
                Unsafe.getUnsafe().putInt(mem + b1 + QwoFormat.D_NEXT, 0);
                Unsafe.getUnsafe().putInt(mem + b0 + QwoFormat.D_NEXT, b0);
                assertInvalid(validator.validateOpen(mem, REGION), validator, "cycles");
                Unsafe.getUnsafe().putInt(mem + b0 + QwoFormat.D_NEXT, b1);
                assertValid(validator.validateOpen(mem, REGION), validator);
                Assert.assertEquals(2, validator.committedRows());
            }
        });
    }

    @Test
    public void testChunkChainOutOfBoundsOrCyclicIsInvalid() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer tb = new QwpTableBuffer("chain")) {
                for (int r = 0; r < 4; r++) {
                    final long v = r;
                    append(writer, tb, dict, t -> t.getOrCreateColumn("v", TYPE_LONG, true).addLong(v));
                }
                writer.publish();
                reader.of(mem, REGION);
                int def = reader.columnDef(reader.firstBlock(), 0);
                // rows 0 | 1-2 | 3
                int c0 = reader.firstChunk(def);
                int c1 = reader.nextChunk(c0);
                int c2 = reader.nextChunk(c1);
                Assert.assertEquals(1, reader.chunkRowCap(c0));
                Assert.assertEquals(2, reader.chunkRowCap(c1));
                Assert.assertEquals(4, reader.chunkRowCap(c2));
                assertValid(validator.validateOpen(mem, REGION), validator);

                final int[] cyclic = {c0, c1};
                for (int target : cyclic) {
                    Unsafe.getUnsafe().putInt(mem + c1 + QwoFormat.C_NEXT, target);
                    assertInvalid(validator.validateOpen(mem, REGION), validator, "cycles");
                }
                Unsafe.getUnsafe().putInt(mem + c1 + QwoFormat.C_NEXT, c2);

                Unsafe.getUnsafe().putInt(mem + c0 + QwoFormat.C_NEXT, 0);
                assertInvalid(validator.validateOpen(mem, REGION), validator, "chunk chain ends");
                final int[] outside = {REGION - 4, REGION, QwoFormat.HEADER_SIZE - 1, -8};
                for (int target : outside) {
                    Unsafe.getUnsafe().putInt(mem + c0 + QwoFormat.C_NEXT, target);
                    assertInvalid(validator.validateOpen(mem, REGION), validator, "outside");
                }
                Unsafe.getUnsafe().putInt(mem + c0 + QwoFormat.C_NEXT, c1);

                final int[] rowCaps = {0, -1, QwoFormat.MAX_CHUNK_ROWS + 1};
                for (int rowCap : rowCaps) {
                    Unsafe.getUnsafe().putInt(mem + c1 + QwoFormat.C_ROW_CAP, rowCap);
                    assertInvalid(validator.validateOpen(mem, REGION), validator, "rowCap");
                }
                Unsafe.getUnsafe().putInt(mem + c1 + QwoFormat.C_ROW_CAP, 2);
                assertValid(validator.validateOpen(mem, REGION), validator);

                // a sealed record bounds every chunk by its usedBytes
                int used = writer.seal(false);
                assertValid(validator.validateSealed(mem, used), validator);
                Unsafe.getUnsafe().putInt(mem + c2 + QwoFormat.C_ROW_CAP, QwoFormat.MAX_CHUNK_ROWS);
                assertInvalid(validator.validateSealed(mem, used), validator, "runs past");
            }
        });
    }

    @Test
    public void testColumnDefOutOfRangeIsInvalid() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer tb = new QwpTableBuffer("defs")) {
                for (int r = 0; r < 2; r++) {
                    final int v = r;
                    append(writer, tb, dict, t -> {
                        t.getOrCreateColumn("v", TYPE_LONG, true).addLong(v);
                        t.getOrCreateColumn("w", TYPE_INT, true).addInt(v);
                    });
                }
                writer.publish();
                reader.of(mem, REGION);
                int b = reader.firstBlock();
                long tail = tailOf(reader, reader.columnDef(b, 1));
                byte type = Unsafe.getUnsafe().getByte(tail + QwoFormat.COL_TYPE);
                final byte[] unknown = {0, 0x08, 0x19, 0x7F, (byte) 0xFF};
                for (byte bad : unknown) {
                    Unsafe.getUnsafe().putByte(tail + QwoFormat.COL_TYPE, bad);
                    Unsafe.getUnsafe().putInt(mem + b + QwoFormat.D_CRC, QwoFormat.descriptorCrc(mem + b));
                    assertInvalid(validator.validateOpen(mem, REGION), validator, "unknown type");
                }
                Unsafe.getUnsafe().putByte(tail + QwoFormat.COL_TYPE, type);
                Unsafe.getUnsafe().putInt(mem + b + QwoFormat.D_CRC, QwoFormat.descriptorCrc(mem + b));
                assertValid(validator.validateOpen(mem, REGION), validator);

                Unsafe.getUnsafe().putByte(tail + QwoFormat.COL_BITMAP, (byte) 2);
                Unsafe.getUnsafe().putInt(mem + b + QwoFormat.D_CRC, QwoFormat.descriptorCrc(mem + b));
                assertInvalid(validator.validateOpen(mem, REGION), validator, "bitmap");

                // the bitmap byte is under the descriptor CRC
                Unsafe.getUnsafe().putByte(tail + QwoFormat.COL_BITMAP, (byte) 1);
                assertInvalid(validator.validateOpen(mem, REGION), validator, "block 0 descriptor crc mismatch");
                Unsafe.getUnsafe().putInt(mem + b + QwoFormat.D_CRC, QwoFormat.descriptorCrc(mem + b));
                assertValid(validator.validateOpen(mem, REGION), validator);
            }
        });
    }

    @Test
    public void testCorruptedDictionaryEntryFailsDictCrc() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                for (int r = 0; r < 3; r++) {
                    appendTrade(writer, tb, dict, r);
                }
                writer.publish();
                reader.of(mem, REGION);
                Assert.assertEquals(3, reader.dictCount());
                // inside the first entry's utf8 bytes, past its length varint
                long p = reader.dictEntriesAddr() + 2;
                byte original = Unsafe.getUnsafe().getByte(p);
                Unsafe.getUnsafe().putByte(p, (byte) (original ^ 0x20));
                assertInvalid(validator.validateOpen(mem, REGION), validator, "dictionary crc mismatch");
                Unsafe.getUnsafe().putByte(p, original);
                assertValid(validator.validateOpen(mem, REGION), validator);

                int used = writer.seal(false);
                assertValid(validator.validateSealed(mem, used), validator);
                Unsafe.getUnsafe().putByte(p, (byte) (original ^ 0x01));
                assertInvalid(validator.validateSealed(mem, used), validator, "dictionary crc mismatch");
            }
        });
    }

    @Test
    public void testCorruptedValueByteFailsRowCrc() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                for (int r = 0; r < 3; r++) {
                    appendTrade(writer, tb, dict, r);
                }
                writer.publish();
                reader.of(mem, REGION);
                int def = reader.columnDef(reader.firstBlock(), 0);
                int c1 = reader.nextChunk(reader.firstChunk(def));
                // row 1's price
                long value = reader.chunkValuesAddr(c1, TYPE_DOUBLE, true);
                Assert.assertEquals(1.5, Unsafe.getUnsafe().getDouble(value), 0.0);
                byte original = Unsafe.getUnsafe().getByte(value + 3);
                Unsafe.getUnsafe().putByte(value + 3, (byte) (original ^ 0x10));
                assertInvalid(validator.validateOpen(mem, REGION), validator, "block 0 (table 'trades') row crc mismatch");
                Unsafe.getUnsafe().putByte(value + 3, original);
                assertValid(validator.validateOpen(mem, REGION), validator);

                // a committed row turned null
                long bitmap = reader.chunkBitmapAddr(c1, true);
                byte bits = Unsafe.getUnsafe().getByte(bitmap);
                Unsafe.getUnsafe().putByte(bitmap, (byte) (bits | 1));
                assertInvalid(validator.validateOpen(mem, REGION), validator, "block 0 (table 'trades') row crc mismatch");
                Unsafe.getUnsafe().putByte(bitmap, bits);

                int used = writer.seal(false);
                assertValid(validator.validateSealed(mem, used), validator);
                Unsafe.getUnsafe().putByte(value, (byte) (Unsafe.getUnsafe().getByte(value) ^ 0x01));
                assertInvalid(validator.validateSealed(mem, used), validator, "block 0 (table 'trades') row crc mismatch");
            }
        });
    }

    @Test
    public void testCutPublishAfterUsedBytesIsRecoveredToPreviousState() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer tb = new QwpTableBuffer("cut"); QwpTableBuffer late = new QwpTableBuffer("late")) {
                appendNote(writer, tb, dict, "a", "x");
                appendNote(writer, tb, dict, "b", "y");
                writer.publish();
                reader.of(mem, REGION);
                int b = reader.firstBlock();
                int def = reader.columnDef(b, 0);
                int c0 = reader.firstChunk(def);
                int oldC1 = reader.nextChunk(c0);
                final long[] dictWords = {Unsafe.getUnsafe().getLong(mem + QwoFormat.OFF_DICT_WORD), 0};
                final long[] rowWords = {Unsafe.getUnsafe().getLong(mem + b + QwoFormat.D_ROW_WORD), 0};

                // the third row's value outgrows the 2-row chunk, so the publish relinks the
                // chain to a relocated copy; it also mints a symbol, and a new table adds a block
                appendNote(writer, tb, dict, text(100), "z");
                append(writer, late, dict, t -> t.getOrCreateColumn("v", TYPE_LONG, true).addLong(7));
                writer.publish();
                int newC1 = reader.nextChunk(c0);
                Assert.assertNotEquals(oldC1, newC1);
                int lateBlock = reader.nextBlock(b);
                Assert.assertNotEquals(0, lateBlock);
                Assert.assertEquals(2, reader.blockCount());
                dictWords[1] = Unsafe.getUnsafe().getLong(mem + QwoFormat.OFF_DICT_WORD);
                rowWords[1] = Unsafe.getUnsafe().getLong(mem + b + QwoFormat.D_ROW_WORD);
                long lateRowWord = Unsafe.getUnsafe().getLong(mem + lateBlock + QwoFormat.D_ROW_WORD);
                Assert.assertNotEquals(dictWords[0], dictWords[1]);

                // publish stores usedBytes, the structure, the dictionary word, then the row
                // words of "cut" and "late"; every prefix of that sequence must recover
                final int[] expectedRows = {2, 2, 3, 4};
                for (int stored = 0; stored < expectedRows.length; stored++) {
                    Unsafe.getUnsafe().putLong(mem + QwoFormat.OFF_DICT_WORD, dictWords[stored > 0 ? 1 : 0]);
                    Unsafe.getUnsafe().putLong(mem + b + QwoFormat.D_ROW_WORD, rowWords[stored > 1 ? 1 : 0]);
                    Unsafe.getUnsafe().putLong(mem + lateBlock + QwoFormat.D_ROW_WORD, stored > 2 ? lateRowWord : 0L);
                    assertValid(validator.validateOpen(mem, REGION), validator);
                    Assert.assertEquals("stores after the structure: " + stored, expectedRows[stored], validator.committedRows());
                    Assert.assertEquals(writer.usedBytes(), validator.usedBytes());
                }

                // the cut right after the structure: the previous state comes back and seals
                Unsafe.getUnsafe().putLong(mem + QwoFormat.OFF_DICT_WORD, dictWords[0]);
                Unsafe.getUnsafe().putLong(mem + b + QwoFormat.D_ROW_WORD, rowWords[0]);
                Unsafe.getUnsafe().putLong(mem + lateBlock + QwoFormat.D_ROW_WORD, 0L);
                assertValid(validator.validateOpen(mem, REGION), validator);
                Assert.assertEquals(2, validator.committedRows());
                int used = validator.usedBytes();
                validator.finalizeForRecovery(mem);
                assertValid(validator.validateSealed(mem, used), validator);
                Assert.assertEquals(2, validator.committedRows());
                Assert.assertEquals(used, validator.usedBytes());
            }
        });
    }

    @Test
    public void testDescriptorNextPointingOutsideUsedBytesIsInvalid() throws Exception {
        withRecord((mem, writer, validator) -> {
            // a ring reservation is zero, and the seal keeps the tail past the record so
            Unsafe.getUnsafe().setMemory(mem, REGION, (byte) 0);
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer a = new QwpTableBuffer("a"); QwpTableBuffer b = new QwpTableBuffer("b")) {
                for (int r = 0; r < 3; r++) {
                    appendTrade(writer, a, dict, r);
                    appendTrade(writer, b, dict, r);
                }
                writer.publish();
                int used = writer.seal(false);
                reader.of(mem, used);
                int b0 = reader.firstBlock();
                int b1 = reader.nextBlock(b0);
                int descSize = reader.columnDef(b1, reader.colCount(b1)) - b1;
                assertValid(validator.validateSealed(mem, used), validator);

                // a valid copy of the second descriptor past usedBytes, inside the capacity
                int moved = used + 64;
                Unsafe.getUnsafe().copyMemory(mem + b1, mem + moved, descSize);
                Unsafe.getUnsafe().putInt(mem + b0 + QwoFormat.D_NEXT, moved);
                assertInvalid(validator.validateSealed(mem, used), validator, "outside");
                // an open record bounds its structure by the capacity instead
                assertValid(validator.validateOpen(mem, REGION), validator);
                Assert.assertEquals(6, validator.committedRows());
                Assert.assertEquals(moved + descSize, validator.usedBytes());
                assertInvalid(validator.validateOpen(mem, moved + descSize - 1), validator, "runs past");
                // so does a header capacity smaller than the reservation
                Unsafe.getUnsafe().putInt(mem + QwoFormat.OFF_CAPACITY, moved + descSize - 1);
                Unsafe.getUnsafe().putInt(mem + QwoFormat.OFF_HEADER_CRC, QwoFormat.headerCrc(mem));
                assertInvalid(validator.validateOpen(mem, REGION), validator, "runs past");
                Unsafe.getUnsafe().putInt(mem + QwoFormat.OFF_CAPACITY, REGION);
                Unsafe.getUnsafe().putInt(mem + QwoFormat.OFF_HEADER_CRC, QwoFormat.headerCrc(mem));
                assertValid(validator.validateOpen(mem, REGION), validator);

                Unsafe.getUnsafe().putInt(mem + b0 + QwoFormat.D_NEXT, REGION);
                assertInvalid(validator.validateOpen(mem, REGION), validator, "outside");
                // the zeroed tail holds no valid descriptor
                Unsafe.getUnsafe().putInt(mem + b0 + QwoFormat.D_NEXT, used + 8);
                assertInvalid(validator.validateOpen(mem, REGION), validator, "block 1 descriptor crc mismatch");
                Unsafe.getUnsafe().putInt(mem + b0 + QwoFormat.D_NEXT, b1);
                assertValid(validator.validateSealed(mem, used), validator);
            }
        });
    }

    @Test
    public void testDictionaryEntryLengthRunningPastTheRegionIsInvalid() throws Exception {
        withRecord((mem, writer, validator) -> {
            // an 8-byte dictionary region: [48, 56)
            writer.open(mem, REGION, VERSION, 0, 8);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer tb = new QwpTableBuffer("syms")) {
                append(writer, tb, dict, t -> {
                    int id = dict.getOrAddSymbol("abc");
                    t.getOrCreateColumn("s", TYPE_SYMBOL, true).addSymbolWithGlobalId("abc", id);
                });
                writer.publish();
                reader.of(mem, REGION);
                Assert.assertEquals(4, reader.dictEntriesLen());
                assertValid(validator.validateOpen(mem, REGION), validator);

                // "abc" [48, 52), "xy" [52, 55), then a length varint that starts on the
                // region's last byte and continues past its end
                long region = mem + QwoFormat.HEADER_SIZE;
                Unsafe.getUnsafe().putByte(region + 4, (byte) 2);
                Unsafe.getUnsafe().putByte(region + 5, (byte) 'x');
                Unsafe.getUnsafe().putByte(region + 6, (byte) 'y');
                Unsafe.getUnsafe().putByte(region + 7, (byte) 0x80);
                Unsafe.getUnsafe().putLong(mem + QwoFormat.OFF_DICT_WORD, 3L << 32);
                Assert.assertEquals(-1, reader.dictEntriesLen());
                assertInvalid(validator.validateOpen(mem, REGION), validator, "dictionary entries");

                // a length varint whose fifth byte still has the continuation bit
                for (int i = 0; i < 5; i++) {
                    Unsafe.getUnsafe().putByte(region + i, (byte) 0x80);
                }
                Unsafe.getUnsafe().putLong(mem + QwoFormat.OFF_DICT_WORD, 1L << 32);
                Assert.assertEquals(-1, reader.dictEntriesLen());
                assertInvalid(validator.validateOpen(mem, REGION), validator, "dictionary entries");

                Unsafe.getUnsafe().putLong(mem + QwoFormat.OFF_DICT_WORD, -1L << 32);
                Assert.assertEquals(-1, reader.dictEntriesLen());
                assertInvalid(validator.validateOpen(mem, REGION), validator, "dictionary entries");
            }
        });
    }

    @Test
    public void testFinalizeForRecoveryClearsDeferFlagAndFixesCrc() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer a = new QwpTableBuffer("a"); QwpTableBuffer b = new QwpTableBuffer("b")) {
                for (int r = 0; r < 2; r++) {
                    appendTrade(writer, a, dict, r);
                    appendTrade(writer, b, dict, r);
                }
                writer.publish();
                int used = writer.seal(true);
                int headerCrc = Unsafe.getUnsafe().getInt(mem + QwoFormat.OFF_HEADER_CRC);
                reader.of(mem, REGION);
                Assert.assertEquals(FLAGS_OPEN | FLAG_DEFER_COMMIT, reader.flags());

                // a seal cut after its flags store, with blockCount and usedBytes left stale
                Unsafe.getUnsafe().putShort(mem + QwoFormat.OFF_BLOCK_COUNT, (short) 1);
                Unsafe.getUnsafe().putInt(mem + QwoFormat.OFF_USED_BYTES, QwoFormat.HEADER_SIZE + QwoFormat.DEFAULT_DICT_RESERVE);
                assertValid(validator.validateOpen(mem, REGION), validator);
                Assert.assertEquals(4, validator.committedRows());
                Assert.assertEquals(used, validator.usedBytes());

                validator.finalizeForRecovery(mem);
                Assert.assertEquals(FLAGS_OPEN, reader.flags());
                Assert.assertEquals(2, reader.blockCount());
                Assert.assertEquals(used, reader.usedBytes());
                // nothing finalizeForRecovery stores is under the header CRC
                Assert.assertEquals(headerCrc, Unsafe.getUnsafe().getInt(mem + QwoFormat.OFF_HEADER_CRC));
                Assert.assertEquals(QwoFormat.headerCrc(mem), headerCrc);
                assertValid(validator.validateSealed(mem, used), validator);
                Assert.assertEquals(4, validator.committedRows());
            }
        });
    }

    @Test
    public void testFinalizeForRecoveryNeedsAValidatedOpenRecord() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                for (int r = 0; r < 3; r++) {
                    appendTrade(writer, tb, dict, r);
                }
                writer.publish();
                assertIllegalState(() -> validator.finalizeForRecovery(mem));
                assertValid(validator.validateSealed(mem, writer.usedBytes()), validator);
                assertIllegalState(() -> validator.finalizeForRecovery(mem));
                assertValid(validator.validateOpen(mem, REGION), validator);
                assertIllegalState(() -> validator.finalizeForRecovery(mem + 8));
                validator.finalizeForRecovery(mem);

                Unsafe.getUnsafe().putInt(mem + QwoFormat.OFF_DICT_START_ID, 1);
                assertInvalid(validator.validateOpen(mem, REGION), validator, "header crc mismatch");
                assertIllegalState(() -> validator.finalizeForRecovery(mem));
            }
        });
    }

    @Test
    public void testHeaderCrcMismatchIsInvalid() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 2, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            dict.getOrAddSymbol("sent0");
            dict.getOrAddSymbol("sent1");
            try (QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                for (int r = 0; r < 3; r++) {
                    appendTrade(writer, tb, dict, r);
                }
                writer.publish();
                assertValid(validator.validateOpen(mem, REGION), validator);
                // capacity, dictionary region, its capacity, dictStartId, the reserved word, the CRC itself
                final int[] covered = {
                        QwoFormat.OFF_CAPACITY, QwoFormat.OFF_DICT_REGION + 1, QwoFormat.OFF_DICT_CAPACITY,
                        QwoFormat.OFF_DICT_START_ID, QwoFormat.OFF_RESERVED + 3, QwoFormat.OFF_HEADER_CRC + 2
                };
                for (int offset : covered) {
                    byte original = Unsafe.getUnsafe().getByte(mem + offset);
                    Unsafe.getUnsafe().putByte(mem + offset, (byte) (original ^ 0x01));
                    assertInvalid(validator.validateOpen(mem, REGION), validator, "header crc mismatch");
                    Unsafe.getUnsafe().putByte(mem + offset, original);
                }
                assertValid(validator.validateOpen(mem, REGION), validator);

                Unsafe.getUnsafe().putByte(mem + 3, (byte) '2');
                assertInvalid(validator.validateOpen(mem, REGION), validator, "magic");
                Unsafe.getUnsafe().putByte(mem + 3, (byte) '1');
                Unsafe.getUnsafe().putByte(mem + QwoFormat.OFF_VERSION, (byte) 2);
                Unsafe.getUnsafe().putInt(mem + QwoFormat.OFF_HEADER_CRC, QwoFormat.headerCrc(mem));
                assertInvalid(validator.validateOpen(mem, REGION), validator, "version");
            }
        });
    }

    @Test
    public void testOpenRecordWithUncommittedTailValidatesCommittedOnly() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer tb = new QwpTableBuffer("trades"); QwpTableBuffer other = new QwpTableBuffer("other")) {
                for (int r = 0; r < 3; r++) {
                    appendTrade(writer, tb, dict, r);
                }
                writer.publish();
                int published = writer.usedBytes();
                // row 3 allocates 4-row chunks; the other table a block; neither is published
                appendTrade(writer, tb, dict, 3);
                appendTrade(writer, tb, dict, 4);
                append(writer, other, dict, t -> {
                    int id = dict.getOrAddSymbol("unpublished");
                    t.getOrCreateColumn("s", TYPE_SYMBOL, true).addSymbolWithGlobalId("unpublished", id);
                });
                Assert.assertEquals(6, writer.rowCount());
                Assert.assertTrue(writer.usedBytes() > published);

                assertValid(validator.validateOpen(mem, REGION), validator);
                Assert.assertEquals(3, validator.committedRows());
                Assert.assertEquals(published, validator.usedBytes());
            }
        });
    }

    @Test
    public void testPrefixOutOfRangeIsInvalid() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer tb = new QwpTableBuffer("prefixes")) {
                for (int r = 0; r < 3; r++) {
                    final int row = r;
                    append(writer, tb, dict, t -> {
                        t.getOrCreateColumn("g", TYPE_GEOHASH, true).addGeoHash(row, 20);
                        t.getOrCreateColumn("d", TYPE_DECIMAL64, true).addDecimal64(new Decimal64(row, 2));
                        t.getOrCreateColumn("i", TYPE_INT, true).addInt(row);
                        t.getOrCreateColumn("gn", TYPE_GEOHASH, true).addNull();
                        t.getOrCreateColumn("dn", TYPE_DECIMAL128, true).addNull();
                    });
                }
                writer.publish();
                reader.of(mem, REGION);
                int b = reader.firstBlock();
                long g = tailOf(reader, reader.columnDef(b, 0));
                long d = tailOf(reader, reader.columnDef(b, 1));
                long i = tailOf(reader, reader.columnDef(b, 2));
                long gn = tailOf(reader, reader.columnDef(b, 3));
                long dn = tailOf(reader, reader.columnDef(b, 4));
                Assert.assertEquals(20, reader.prefix(reader.columnDef(b, 0)));
                Assert.assertEquals(-1, reader.prefix(reader.columnDef(b, 3)));
                assertValid(validator.validateOpen(mem, REGION), validator);

                assertPrefix(validator, mem, g, QwoFormat.PREFIX_DECIMAL, 20, "out-of-range prefix");
                assertPrefix(validator, mem, g, QwoFormat.PREFIX_GEOHASH, 0, "out-of-range prefix");
                assertPrefix(validator, mem, g, QwoFormat.PREFIX_GEOHASH, 61, "out-of-range prefix");
                assertPrefix(validator, mem, g, (byte) 3, 20, "out-of-range prefix");
                assertPrefix(validator, mem, d, QwoFormat.PREFIX_GEOHASH, 2, "out-of-range prefix");
                assertPrefix(validator, mem, i, QwoFormat.PREFIX_GEOHASH, 5, "out-of-range prefix");
                assertPrefix(validator, mem, i, QwoFormat.PREFIX_DECIMAL, 0, "out-of-range prefix");
                // committed non-null values the materialiser could not encode without the prefix
                assertPrefix(validator, mem, g, QwoFormat.PREFIX_NONE, 0, "no precision");
                assertPrefix(validator, mem, d, QwoFormat.PREFIX_NONE, 0, "no scale");
                // all-null columns may carry a prefix or not
                assertPrefix(validator, mem, gn, QwoFormat.PREFIX_GEOHASH, 7, null);
                assertPrefix(validator, mem, dn, QwoFormat.PREFIX_DECIMAL, 3, null);
                assertValid(validator.validateOpen(mem, REGION), validator);
            }
        });
    }

    @Test
    public void testRecordVersionOutsideTheAcceptedRangeIsInvalid() throws Exception {
        withRecord((mem, writer, validator) -> {
            // the record format owns its accepted versions; a wire protocol bump must not orphan records on disk
            Assert.assertEquals(1, QwoFormat.RECORD_VERSION_MIN);
            Assert.assertEquals(1, QwoFormat.RECORD_VERSION_MAX);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            for (int version = QwoFormat.RECORD_VERSION_MIN - 1; version <= QwoFormat.RECORD_VERSION_MAX + 1; version++) {
                Unsafe.getUnsafe().setMemory(mem, REGION, (byte) 0x5A);
                writer.open(mem, REGION, (byte) version, 0, QwoFormat.DEFAULT_DICT_RESERVE);
                try (QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                    appendTrade(writer, tb, dict, 0);
                    writer.publish();
                    if (version >= QwoFormat.RECORD_VERSION_MIN && version <= QwoFormat.RECORD_VERSION_MAX) {
                        assertValid(validator.validateOpen(mem, REGION), validator);
                        assertValid(validator.validateSealed(mem, writer.seal(false)), validator);
                    } else {
                        assertInvalid(validator.validateOpen(mem, REGION), validator, "unsupported version " + version);
                        assertInvalid(validator.validateSealed(mem, writer.seal(false)), validator, "unsupported version " + version);
                    }
                }
            }
        });
    }

    @Test
    public void testRecordWithNoCommittedRowsReportsZero() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            int empty = QwoFormat.HEADER_SIZE + QwoFormat.DEFAULT_DICT_RESERVE;
            assertValid(validator.validateOpen(mem, REGION), validator);
            Assert.assertEquals(0, validator.committedRows());
            Assert.assertEquals(empty, validator.usedBytes());
            assertValid(validator.validateSealed(mem, empty), validator);
            Assert.assertEquals(0, validator.committedRows());

            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                appendTrade(writer, tb, dict, 0);
                assertValid(validator.validateOpen(mem, REGION), validator);
                Assert.assertEquals(0, validator.committedRows());
                Assert.assertEquals(empty, validator.usedBytes());

                // a reachable block whose row word is still 0 contributes nothing
                writer.publish();
                reader.of(mem, REGION);
                Unsafe.getUnsafe().putLong(mem + reader.firstBlock() + QwoFormat.D_ROW_WORD, 0L);
                assertValid(validator.validateOpen(mem, REGION), validator);
                Assert.assertEquals(0, validator.committedRows());
                Assert.assertEquals(writer.usedBytes(), validator.usedBytes());
            }
        });
    }

    @Test
    public void testSealedRecordFromWriterValidates() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer every = new QwpTableBuffer("every"); QwpTableBuffer trades = new QwpTableBuffer("trades")) {
                // chunks hold rows 0 | 1-2 | 3-6 | 7-14 | 15-30 | 31-62: nulls on both sides of every boundary
                final int rows = 37;
                final boolean[] isNullRow = new boolean[rows];
                final int[] nulls = {0, 1, 2, 3, 4, 7, 8, 15, 16, 31, 32};
                for (int r : nulls) {
                    isNullRow[r] = true;
                }
                for (int r = 0; r < rows; r++) {
                    final int row = r;
                    append(writer, every, dict, t -> putEveryType(t, row, isNullRow[row], dict));
                    if (r % 2 == 0) {
                        appendTrade(writer, trades, dict, r);
                    }
                    if (r % 3 == 2) {
                        writer.publish();
                    }
                }
                writer.publish();
                reader.of(mem, REGION);
                int b = reader.firstBlock();
                Assert.assertEquals(EVERY_TYPE.length, reader.colCount(b));
                for (int i = 0; i < EVERY_TYPE.length; i++) {
                    Assert.assertEquals(EVERY_TYPE[i], reader.columnType(reader.columnDef(b, i)));
                }
                int committed = writer.committedRowCount();
                Assert.assertEquals(rows + (rows + 1) / 2, committed);

                assertValid(validator.validateOpen(mem, REGION), validator);
                Assert.assertEquals(committed, validator.committedRows());
                Assert.assertEquals(writer.usedBytes(), validator.usedBytes());

                int used = writer.seal(false);
                assertValid(validator.validateSealed(mem, used), validator);
                Assert.assertEquals(committed, validator.committedRows());
                Assert.assertEquals(used, validator.usedBytes());
                // a payload longer than usedBytes is fine
                assertValid(validator.validateSealed(mem, REGION), validator);
                Assert.assertEquals(used, validator.usedBytes());
            }
        });
    }

    @Test
    public void testStaleBlockCountIsToleratedOnOpenRecord() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer a = new QwpTableBuffer("a"); QwpTableBuffer b = new QwpTableBuffer("b")) {
                for (int r = 0; r < 2; r++) {
                    appendTrade(writer, a, dict, r);
                    appendTrade(writer, b, dict, r);
                }
                writer.publish();
                int used = writer.seal(false);
                assertValid(validator.validateSealed(mem, used), validator);
                final short[] stale = {1, 0, 3};
                for (short blockCount : stale) {
                    Unsafe.getUnsafe().putShort(mem + QwoFormat.OFF_BLOCK_COUNT, blockCount);
                    assertValid(validator.validateOpen(mem, REGION), validator);
                    Assert.assertEquals(4, validator.committedRows());
                    assertInvalid(validator.validateSealed(mem, used), validator, "blockCount");
                }
            }
        });
    }

    @Test
    public void testUncommittedGarbageBeyondCountsIsIgnored() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer tb = new QwpTableBuffer("garbage")) {
                for (int r = 0; r < 4; r++) {
                    final int row = r;
                    append(writer, tb, dict, t -> {
                        t.getOrCreateColumn("price", TYPE_DOUBLE, true).addDouble(row);
                        t.getOrCreateColumn("flag", TYPE_BOOLEAN, false).addBoolean(row % 2 == 0);
                        String venue = "v" + row;
                        t.getOrCreateColumn("venue", TYPE_SYMBOL, true).addSymbolWithGlobalId(venue, dict.getOrAddSymbol(venue));
                        t.getOrCreateColumn("note", TYPE_VARCHAR, true).addString(text(row + 1));
                    });
                }
                writer.publish();
                int used = writer.usedBytes();
                reader.of(mem, REGION);
                int b = reader.firstBlock();
                Random rnd = new Random(42);
                // rows 0 | 1-2 | 3-6: the third chunk of every column holds one committed row
                for (int col = 0; col < 4; col++) {
                    int def = reader.columnDef(b, col);
                    int c = reader.nextChunk(reader.nextChunk(reader.firstChunk(def)));
                    Assert.assertEquals(4, reader.chunkRowCap(c));
                    // links past the committed rows are never followed
                    Unsafe.getUnsafe().putInt(mem + c + QwoFormat.C_NEXT, rnd.nextInt());
                    boolean hasBitmap = reader.columnUsesBitmap(def);
                    if (hasBitmap) {
                        long bitmap = reader.chunkBitmapAddr(c, true);
                        byte bits = Unsafe.getUnsafe().getByte(bitmap);
                        Unsafe.getUnsafe().putByte(bitmap, (byte) ((bits & 1) | (rnd.nextInt() & 0xFE)));
                    }
                    byte type = reader.columnType(def);
                    long values = reader.chunkValuesAddr(c, type, hasBitmap);
                    switch (type) {
                        case TYPE_DOUBLE:
                            scribble(rnd, values + 8, 3 * 8);
                            break;
                        case TYPE_BOOLEAN: {
                            byte bits = Unsafe.getUnsafe().getByte(values);
                            Unsafe.getUnsafe().putByte(values, (byte) ((bits & 1) | (rnd.nextInt() & 0xFE)));
                            break;
                        }
                        case TYPE_SYMBOL:
                            // one 1-byte varint is committed
                            scribble(rnd, values + 1, 4 * 5 - 1);
                            break;
                        default: {
                            long offsets = reader.chunkOffsetsAddr(c, true);
                            scribble(rnd, offsets + 2 * 4, 3 * 4);
                            int end = Unsafe.getUnsafe().getInt(offsets + 4);
                            long bytes = reader.chunkBytesAddr(c, true, 4);
                            scribble(rnd, bytes + end, reader.chunkByteCap(c) - end);
                            break;
                        }
                    }
                }
                // dictionary bytes past the committed entries, and everything past usedBytes
                int dictLen = reader.dictEntriesLen();
                scribble(rnd, reader.dictEntriesAddr() + dictLen, QwoFormat.DEFAULT_DICT_RESERVE - dictLen);
                scribble(rnd, mem + used, REGION - used);

                assertValid(validator.validateOpen(mem, REGION), validator);
                Assert.assertEquals(4, validator.committedRows());
                Assert.assertEquals(used, validator.usedBytes());
            }
        });
    }

    @Test
    public void testUsedBytesOutsideTheBoundIsInvalid() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            try (QwpTableBuffer tb = new QwpTableBuffer("trades")) {
                for (int r = 0; r < 3; r++) {
                    appendTrade(writer, tb, dict, r);
                }
                writer.publish();
                int used = writer.seal(false);
                assertValid(validator.validateSealed(mem, used), validator);
                assertInvalid(validator.validateSealed(mem, used - 1), validator, "usedBytes");
                assertInvalid(validator.validateSealed(mem, QwoFormat.HEADER_SIZE - 1), validator, "shorter than the record header");
                assertInvalid(validator.validateOpen(mem, QwoFormat.HEADER_SIZE - 1), validator, "shorter than the record header");
                // a reservation smaller than the stored usedBytes
                assertInvalid(validator.validateOpen(mem, used - 1), validator, "usedBytes");
                assertValid(validator.validateOpen(mem, used), validator);

                final int[] storedUsedBytes = {REGION + 1, QwoFormat.HEADER_SIZE - 1, -1};
                for (int stored : storedUsedBytes) {
                    Unsafe.getUnsafe().putInt(mem + QwoFormat.OFF_USED_BYTES, stored);
                    assertInvalid(validator.validateOpen(mem, REGION), validator, "usedBytes");
                    assertInvalid(validator.validateSealed(mem, REGION), validator, "usedBytes");
                }
                Unsafe.getUnsafe().putInt(mem + QwoFormat.OFF_USED_BYTES, used);

                // the header's capacity bounds the record too
                Unsafe.getUnsafe().putInt(mem + QwoFormat.OFF_CAPACITY, used);
                Unsafe.getUnsafe().putInt(mem + QwoFormat.OFF_HEADER_CRC, QwoFormat.headerCrc(mem));
                assertValid(validator.validateOpen(mem, REGION), validator);
                assertValid(validator.validateSealed(mem, used), validator);
                Unsafe.getUnsafe().putInt(mem + QwoFormat.OFF_CAPACITY, used - 1);
                Unsafe.getUnsafe().putInt(mem + QwoFormat.OFF_HEADER_CRC, QwoFormat.headerCrc(mem));
                assertInvalid(validator.validateOpen(mem, REGION), validator, "usedBytes");
                assertInvalid(validator.validateSealed(mem, used), validator, "usedBytes");
            }
        });
    }

    @Test
    public void testVariableSizeOffsetsOutOfRangeAreInvalid() throws Exception {
        withRecord((mem, writer, validator) -> {
            writer.open(mem, REGION, VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            QwoRecordReader reader = new QwoRecordReader();
            try (QwpTableBuffer tb = new QwpTableBuffer("offsets")) {
                final String[] values = {"ab", "cde", "f"};
                for (String value : values) {
                    append(writer, tb, dict, t -> t.getOrCreateColumn("s", TYPE_VARCHAR, true).addString(value));
                }
                writer.publish();
                reader.of(mem, REGION);
                int def = reader.columnDef(reader.firstBlock(), 0);
                int c1 = reader.nextChunk(reader.firstChunk(def));
                long offsets = reader.chunkOffsetsAddr(c1, true);
                int byteCap = reader.chunkByteCap(c1);
                Assert.assertEquals(0, Unsafe.getUnsafe().getInt(offsets));
                Assert.assertEquals(3, Unsafe.getUnsafe().getInt(offsets + 4));
                Assert.assertEquals(4, Unsafe.getUnsafe().getInt(offsets + 8));
                assertValid(validator.validateOpen(mem, REGION), validator);

                final int[][] bad = {{1, byteCap + 1}, {2, byteCap + 1}, {0, 1}, {2, 2}, {1, -1}, {2, Integer.MIN_VALUE}};
                for (int[] patch : bad) {
                    long p = offsets + 4L * patch[0];
                    int original = Unsafe.getUnsafe().getInt(p);
                    Unsafe.getUnsafe().putInt(p, patch[1]);
                    assertInvalid(validator.validateOpen(mem, REGION), validator, "offsets");
                    Unsafe.getUnsafe().putInt(p, original);
                }
                // in range but not what the writer stored: the row CRC tells
                Unsafe.getUnsafe().putInt(offsets + 8, byteCap);
                assertInvalid(validator.validateOpen(mem, REGION), validator, "row crc mismatch");
                Unsafe.getUnsafe().putInt(offsets + 8, 4);
                assertValid(validator.validateOpen(mem, REGION), validator);
            }
        });
    }

    private static void append(QwoRecordWriter writer, QwpTableBuffer tb, GlobalSymbolDictionary dict, Consumer<QwpTableBuffer> row) {
        stage(tb, row);
        Assert.assertEquals(QwoRecordWriter.APPEND_OK, writer.appendStagedRow(tb, dict, dict.size() - 1));
        tb.clearStagedRow();
    }

    private static void appendNote(QwoRecordWriter writer, QwpTableBuffer tb, GlobalSymbolDictionary dict, String note, String venue) {
        append(writer, tb, dict, t -> {
            t.getOrCreateColumn("note", TYPE_VARCHAR, true).addString(note);
            t.getOrCreateColumn("venue", TYPE_SYMBOL, true).addSymbolWithGlobalId(venue, dict.getOrAddSymbol(venue));
        });
    }

    private static void appendTrade(QwoRecordWriter writer, QwpTableBuffer tb, GlobalSymbolDictionary dict, int row) {
        append(writer, tb, dict, t -> {
            t.getOrCreateColumn("price", TYPE_DOUBLE, true).addDouble(1.5 * row);
            String venue = "venue" + row % 3;
            t.getOrCreateColumn("venue", TYPE_SYMBOL, true).addSymbolWithGlobalId(venue, dict.getOrAddSymbol(venue));
            t.getOrCreateColumn("note", TYPE_VARCHAR, true).addString(text(row * 3));
            t.getOrCreateDesignatedTimestampColumn(TYPE_TIMESTAMP).addLong(TS_BASE + row);
        });
    }

    private static void assertIllegalState(Runnable code) {
        try {
            code.run();
            Assert.fail("expected IllegalStateException");
        } catch (IllegalStateException expected) {
            Assert.assertNotNull(expected.getMessage());
        }
    }

    private static void assertInvalid(boolean isValid, QwoValidator validator, String phrase) {
        Assert.assertFalse("expected a failure naming '" + phrase + '\'', isValid);
        String detail = validator.detail();
        Assert.assertNotNull(detail);
        Assert.assertTrue("detail '" + detail + "' does not name '" + phrase + '\'', detail.contains(phrase));
        Assert.assertEquals(0, validator.committedRows());
        Assert.assertEquals(0, validator.usedBytes());
    }

    /**
     * Stores {@code [kind][prefix][0]} in the def whose type byte is at
     * {@code tail}, asserts the open record fails naming {@code phrase} (or
     * validates when it is null), then restores the def.
     */
    private static void assertPrefix(QwoValidator validator, long mem, long tail, byte kind, int prefix, String phrase) {
        int original = Unsafe.getUnsafe().getInt(tail + QwoFormat.COL_PREFIX_KIND);
        Unsafe.getUnsafe().putByte(tail + QwoFormat.COL_PREFIX_KIND, kind);
        Unsafe.getUnsafe().putByte(tail + QwoFormat.COL_PREFIX, (byte) prefix);
        Unsafe.getUnsafe().putByte(tail + QwoFormat.COL_PREFIX + 1, (byte) 0);
        if (phrase == null) {
            assertValid(validator.validateOpen(mem, REGION), validator);
        } else {
            assertInvalid(validator.validateOpen(mem, REGION), validator, phrase);
        }
        Unsafe.getUnsafe().putInt(tail + QwoFormat.COL_PREFIX_KIND, original);
    }

    private static void assertValid(boolean isValid, QwoValidator validator) {
        Assert.assertTrue(validator.detail(), isValid);
        Assert.assertNull(validator.detail());
    }

    private static void putEveryType(QwpTableBuffer t, int row, boolean isNull, GlobalSymbolDictionary dict) {
        for (int i = 0; i < EVERY_TYPE.length; i++) {
            byte type = EVERY_TYPE[i];
            // the sender's choice: these four have no null bitmap, a null is the type's default
            boolean hasBitmap = type != TYPE_BOOLEAN && type != TYPE_BYTE && type != TYPE_SHORT && type != TYPE_CHAR;
            QwpTableBuffer.ColumnBuffer col = t.getOrCreateColumn("c" + i, type, hasBitmap);
            if (isNull) {
                col.addNull();
                continue;
            }
            switch (type) {
                case TYPE_BOOLEAN:
                    col.addBoolean(row % 3 == 0);
                    break;
                case TYPE_BYTE:
                    col.addByte((byte) row);
                    break;
                case TYPE_SHORT:
                    col.addShort((short) (row * 7));
                    break;
                case TYPE_CHAR:
                    col.addShort((short) ('A' + row % 26));
                    break;
                case TYPE_INT:
                    col.addInt(row * 31);
                    break;
                case TYPE_IPv4:
                    col.addIPv4(0x0A000001 + row);
                    break;
                case TYPE_LONG:
                case TYPE_DATE:
                    col.addLong(row * 1_000_003L);
                    break;
                case TYPE_TIMESTAMP:
                case TYPE_TIMESTAMP_NANOS:
                    col.addLong(TS_BASE + row);
                    break;
                case TYPE_FLOAT:
                    col.addFloat(row * 0.5f);
                    break;
                case TYPE_DOUBLE:
                    col.addDouble(row * 0.25);
                    break;
                case TYPE_SYMBOL: {
                    String symbol = "sym" + row % 5;
                    col.addSymbolWithGlobalId(symbol, dict.getOrAddSymbol(symbol));
                    break;
                }
                case TYPE_UUID:
                    col.addUuid(row, ~row);
                    break;
                case TYPE_LONG256:
                    col.addLong256(row, row + 1, row + 2, row + 3);
                    break;
                case TYPE_GEOHASH:
                    col.addGeoHash(row & 0xFFFFF, 20);
                    break;
                case TYPE_VARCHAR:
                    // lengths that outgrow the chunks' byte capacity, so chunks get relocated
                    col.addString(text(row * 7 % 61));
                    break;
                case TYPE_BINARY: {
                    byte[] bytes = new byte[row % 17];
                    for (int k = 0; k < bytes.length; k++) {
                        bytes[k] = (byte) (row + k);
                    }
                    col.addBinary(bytes);
                    break;
                }
                case TYPE_DOUBLE_ARRAY:
                    col.addDoubleArray(new double[]{row, row + 0.5});
                    break;
                case TYPE_LONG_ARRAY:
                    col.addLongArray(new long[][]{{row, -row}, {row + 1, 7}});
                    break;
                case TYPE_DECIMAL64:
                    col.addDecimal64(new Decimal64(row * 100L + 1, 2));
                    break;
                case TYPE_DECIMAL128:
                    col.addDecimal128(new Decimal128(row, row * 1000L + 7, 6));
                    break;
                case TYPE_DECIMAL256:
                    col.addDecimal256(new Decimal256(0, row, 0, row * 3L + 1, 10));
                    break;
                default:
                    throw new AssertionError("unexpected type " + type);
            }
        }
    }

    private static void scribble(Random rnd, long addr, long len) {
        for (long i = 0; i < len; i++) {
            Unsafe.getUnsafe().putByte(addr + i, (byte) rnd.nextInt());
        }
    }

    private static void stage(QwpTableBuffer tb, Consumer<QwpTableBuffer> row) {
        row.accept(tb);
        tb.nextRow();
        Assert.assertEquals(1, tb.getRowCount());
    }

    /**
     * Address of the type byte of the column def at offset {@code def}.
     */
    private static long tailOf(QwoRecordReader reader, int def) {
        return reader.columnNameAddr(def) + reader.columnNameLen(def);
    }

    private static String text(int len) {
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            sb.append((char) ('a' + i % 26));
        }
        return sb.toString();
    }

    private static void withRecord(RecordCode code) throws Exception {
        assertMemoryLeak(() -> {
            long mem = Unsafe.malloc(REGION, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter()) {
                // garbage, so nothing relies on zeroed memory
                Unsafe.getUnsafe().setMemory(mem, REGION, (byte) 0x5A);
                code.run(mem, writer, new QwoValidator());
            } finally {
                Unsafe.free(mem, REGION, MemoryTag.NATIVE_DEFAULT);
            }
        });
    }

    @FunctionalInterface
    private interface RecordCode {
        void run(long mem, QwoRecordWriter writer, QwoValidator validator) throws Exception;
    }
}
