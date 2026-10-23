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
import io.questdb.client.cutlass.qwp.client.sf.record.QwoMaterializer;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordReader;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoRecordWriter;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

public class QwoMaterializerTest {
    private static final int REGION = 64 * 1024;

    @Test
    public void testHeaderFieldsAndPayloadLength() throws Exception {
        withRecord((mem, writer) -> {
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            for (int i = 0; i < 3; i++) {
                dict.getOrAddSymbol("sent" + i);
            }
            writer.open(mem, REGION, QwpConstants.VERSION, 3, QwoFormat.DEFAULT_DICT_RESERVE);
            final int x = dict.getOrAddSymbol("x");
            final int yy = dict.getOrAddSymbol("yy");
            try (
                    QwpTableBuffer alpha = new QwpTableBuffer("alpha");
                    QwpTableBuffer beta = new QwpTableBuffer("beta");
                    QwoMaterializer materializer = new QwoMaterializer();
                    NativeBufferWriter out = new NativeBufferWriter()
            ) {
                append(writer, alpha, dict, x, t -> t.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true).addSymbolWithGlobalId("x", x));
                append(writer, beta, dict, yy, t -> t.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true).addSymbolWithGlobalId("yy", yy));
                writer.publish();
                int used = writer.seal(true);
                QwoRecordReader reader = new QwoRecordReader();
                reader.of(mem, used);

                int size = materializer.write(reader, out);
                Assert.assertEquals(size, out.getPosition());
                long p = out.getBufferPtr();
                Assert.assertEquals('Q', Unsafe.getUnsafe().getByte(p));
                Assert.assertEquals('W', Unsafe.getUnsafe().getByte(p + 1));
                Assert.assertEquals('P', Unsafe.getUnsafe().getByte(p + 2));
                Assert.assertEquals('1', Unsafe.getUnsafe().getByte(p + 3));
                Assert.assertEquals(QwpConstants.VERSION, Unsafe.getUnsafe().getByte(p + 4));
                byte flags = (byte) (QwpConstants.FLAG_GORILLA | QwpConstants.FLAG_DELTA_SYMBOL_DICT | QwpConstants.FLAG_DEFER_COMMIT);
                Assert.assertEquals(flags, reader.flags());
                Assert.assertEquals(flags, Unsafe.getUnsafe().getByte(p + QwpConstants.HEADER_OFFSET_FLAGS));
                Assert.assertEquals(2, Unsafe.getUnsafe().getShort(p + 6));
                Assert.assertEquals(size - QwpConstants.HEADER_SIZE, Unsafe.getUnsafe().getInt(p + 8));

                // dictionary section: start id 3, two entries, then alpha's block
                byte[] expected = {
                        3, 2, 1, 'x', 2, 'y', 'y',
                        5, 'a', 'l', 'p', 'h', 'a', 1, 1, 1, 's', QwpConstants.TYPE_SYMBOL, 0, 3
                };
                for (int i = 0; i < expected.length; i++) {
                    Assert.assertEquals("byte " + (QwpConstants.HEADER_SIZE + i), expected[i], Unsafe.getUnsafe().getByte(p + QwpConstants.HEADER_SIZE + i));
                }
            }
        });
    }

    @Test
    public void testWriteAppendsAtCurrentPosition() throws Exception {
        withRecord((mem, writer) -> {
            GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
            writer.open(mem, REGION, QwpConstants.VERSION, 0, QwoFormat.DEFAULT_DICT_RESERVE);
            try (
                    QwpTableBuffer tb = new QwpTableBuffer("t");
                    QwoMaterializer materializer = new QwoMaterializer();
                    NativeBufferWriter expected = new NativeBufferWriter();
                    // small, so the frame grows the sink while it is written
                    NativeBufferWriter out = new NativeBufferWriter(16)
            ) {
                for (int r = 0; r < 40; r++) {
                    final int row = r;
                    append(writer, tb, dict, -1, t -> {
                        t.getOrCreateColumn("v", QwpConstants.TYPE_LONG, true).addLong(row * 7L);
                        t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(row % 4 == 0 ? null : "value-" + row);
                        t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(1_000_000L * row);
                    });
                    writer.publish();
                }
                int used = writer.seal(false);
                QwoRecordReader reader = new QwoRecordReader();
                reader.of(mem, used);
                int expectedSize = materializer.write(reader, expected);

                for (int i = 0; i < 7; i++) {
                    out.putByte((byte) (0x70 + i));
                }
                int size = materializer.write(reader, out);
                Assert.assertEquals(expectedSize, size);
                Assert.assertEquals(7 + size, out.getPosition());
                long p = out.getBufferPtr();
                for (int i = 0; i < 7; i++) {
                    Assert.assertEquals(0x70 + i, Unsafe.getUnsafe().getByte(p + i));
                }
                for (int i = 0; i < size; i++) {
                    Assert.assertEquals("byte " + i, Unsafe.getUnsafe().getByte(expected.getBufferPtr() + i), Unsafe.getUnsafe().getByte(p + 7 + i));
                }
                Assert.assertEquals(size - QwpConstants.HEADER_SIZE, Unsafe.getUnsafe().getInt(p + 7 + 8));
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
        row.accept(tb);
        tb.nextRow();
        Assert.assertEquals(QwoRecordWriter.APPEND_OK, writer.appendStagedRow(tb, dict, batchMaxSymbolId));
        tb.clearStagedRow();
    }

    private static void withRecord(RecordCode code) throws Exception {
        assertMemoryLeak(() -> {
            long mem = Unsafe.malloc(REGION, MemoryTag.NATIVE_DEFAULT);
            try (QwoRecordWriter writer = new QwoRecordWriter()) {
                Unsafe.getUnsafe().setMemory(mem, REGION, (byte) 0x5A);
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
