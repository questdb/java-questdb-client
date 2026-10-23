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

import io.questdb.client.cutlass.qwp.client.NativeBufferWriter;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoFormat;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.std.Crc32c;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

public class QwoFormatTest {

    @Test
    public void testChunkDataBytesPerType() {
        Assert.assertEquals(8 * 16, QwoFormat.chunkDataBytes(QwpConstants.TYPE_DOUBLE, 0, false, 16, 0));
        Assert.assertEquals(2 + 8 * 16, QwoFormat.chunkDataBytes(QwpConstants.TYPE_DOUBLE, 0, true, 16, 0));
        Assert.assertEquals(2, QwoFormat.chunkDataBytes(QwpConstants.TYPE_BOOLEAN, 0, false, 16, 0));
        Assert.assertEquals(2 + 17 * 4 + 300, QwoFormat.chunkDataBytes(QwpConstants.TYPE_VARCHAR, 0, true, 16, 300));
        Assert.assertEquals(5 * 3, QwoFormat.chunkDataBytes(QwpConstants.TYPE_SYMBOL, 0, false, 3, 0));
        Assert.assertEquals(2 * 16, QwoFormat.chunkDataBytes(QwpConstants.TYPE_GEOHASH, 12, false, 16, 0));
        Assert.assertEquals(8 * 16, QwoFormat.chunkDataBytes(QwpConstants.TYPE_GEOHASH, 0, false, 16, 0));
    }

    @Test
    public void testDescriptorCrcSkipsMutableFields() {
        long d = Unsafe.malloc(64, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().setMemory(d, 64, (byte) 0x33);
            Unsafe.getUnsafe().putShort(d + QwoFormat.D_COL_COUNT, (short) 2);
            Unsafe.getUnsafe().putShort(d + QwoFormat.D_FLAGS, (short) 0);
            long p = d + QwoFormat.D_NAME;
            p += QwoFormat.putString(p, "tbl");
            long def0 = p;
            p += QwoFormat.putString(p, "a");
            Unsafe.getUnsafe().putByte(p++, QwpConstants.TYPE_LONG);
            long prefix0 = p;
            p += 3;
            Unsafe.getUnsafe().putByte(p++, (byte) 1);
            Unsafe.getUnsafe().putInt(p, 1234);
            p += 4;
            p += QwoFormat.putString(p, "");
            Unsafe.getUnsafe().putByte(p++, QwpConstants.TYPE_TIMESTAMP);
            long prefix1 = p;
            p += 3;
            Unsafe.getUnsafe().putByte(p++, (byte) 1);
            Unsafe.getUnsafe().putInt(p, 5678);
            long end = p + 4;

            int expected = Crc32c.updateUnsafe(Crc32c.INIT, d + QwoFormat.D_COL_COUNT, 4);
            expected = Crc32c.updateUnsafe(expected, d + QwoFormat.D_NAME, prefix0 - (d + QwoFormat.D_NAME));
            expected = Crc32c.updateUnsafe(expected, prefix0 + 3, prefix1 - (prefix0 + 3));
            expected = Crc32c.updateUnsafe(expected, prefix1 + 3, end - (prefix1 + 3));
            int crc = QwoFormat.descriptorCrc(d);
            Assert.assertEquals(expected, crc);

            long[] unchanged = {
                    d + QwoFormat.D_CRC, d + QwoFormat.D_NEXT + 2, d + QwoFormat.D_ROW_WORD + 5,
                    prefix0, prefix0 + 1, prefix0 + 2, prefix1, prefix1 + 2
            };
            for (long a : unchanged) {
                flip(a);
                Assert.assertEquals(crc, QwoFormat.descriptorCrc(d));
                flip(a);
            }
            long[] changed = {
                    d + QwoFormat.D_FLAGS, d + QwoFormat.D_NAME + 1, def0 + 1, prefix0 - 1, prefix0 + 3,
                    prefix0 + 5, prefix1 - 1, end - 1
            };
            for (long a : changed) {
                flip(a);
                Assert.assertNotEquals(crc, QwoFormat.descriptorCrc(d));
                flip(a);
            }
        } finally {
            Unsafe.free(d, 64, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testHeaderCrcSkipsItsOwnField() {
        long p = Unsafe.malloc(QwoFormat.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            for (int i = 0; i < QwoFormat.HEADER_SIZE; i++) {
                Unsafe.getUnsafe().putByte(p + i, (byte) (i * 7 + 1));
            }
            int crc = QwoFormat.headerCrc(p);
            int expected = Crc32c.updateUnsafe(Crc32c.INIT, p, 5);
            expected = Crc32c.updateUnsafe(expected, p + 8, 4);
            expected = Crc32c.updateUnsafe(expected, p + 20, 12);
            expected = Crc32c.updateUnsafe(expected, p + 44, 4);
            Assert.assertEquals(expected, crc);
            // flags, blockCount, the CRC itself, firstBlock, dictWord and usedBytes change after open()
            int[] unchanged = {5, 6, 13, 16, 32, 40};
            for (int off : unchanged) {
                flip(p + off);
                Assert.assertEquals("offset " + off, crc, QwoFormat.headerCrc(p));
                flip(p + off);
            }
            int[] changed = {2, 9, 20, 28, 44};
            for (int off : changed) {
                flip(p + off);
                Assert.assertNotEquals("offset " + off, crc, QwoFormat.headerCrc(p));
                flip(p + off);
            }
        } finally {
            Unsafe.free(p, QwoFormat.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testIsRecordPayload() {
        long p = Unsafe.malloc(QwoFormat.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putInt(p, QwoFormat.MAGIC_RECORD);
            Assert.assertTrue(QwoFormat.isRecordPayload(p, QwoFormat.HEADER_SIZE));
            Assert.assertFalse(QwoFormat.isRecordPayload(p, QwoFormat.HEADER_SIZE - 1));
            Unsafe.getUnsafe().putInt(p, QwpConstants.MAGIC_MESSAGE);
            Assert.assertFalse(QwoFormat.isRecordPayload(p, QwoFormat.HEADER_SIZE));
        } finally {
            Unsafe.free(p, QwoFormat.HEADER_SIZE, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testMagicIsQwo1LittleEndian() {
        long p = Unsafe.malloc(8, MemoryTag.NATIVE_DEFAULT);
        try {
            Unsafe.getUnsafe().putInt(p, QwoFormat.MAGIC_RECORD);
            Assert.assertEquals('Q', Unsafe.getUnsafe().getByte(p));
            Assert.assertEquals('W', Unsafe.getUnsafe().getByte(p + 1));
            Assert.assertEquals('O', Unsafe.getUnsafe().getByte(p + 2));
            Assert.assertEquals('1', Unsafe.getUnsafe().getByte(p + 3));
            Assert.assertNotEquals(QwpConstants.MAGIC_MESSAGE, QwoFormat.MAGIC_RECORD);
        } finally {
            Unsafe.free(p, 8, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testPutStringMatchesNativeBufferWriter() {
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < 200; i++) {
            big.append('x');
        }
        String[] samples = {"", "a", big.toString(), "héllo € 😀"};
        long p = Unsafe.malloc(1024, MemoryTag.NATIVE_DEFAULT);
        try (NativeBufferWriter w = new NativeBufferWriter()) {
            for (String s : samples) {
                w.reset();
                w.putString(s);
                int expected = w.getPosition();
                int n = QwoFormat.putString(p, s);
                Assert.assertEquals(expected, n);
                Assert.assertEquals(expected, QwoFormat.stringBytes(s));
                for (int i = 0; i < expected; i++) {
                    Assert.assertEquals(
                            Unsafe.getUnsafe().getByte(w.getBufferPtr() + i),
                            Unsafe.getUnsafe().getByte(p + i)
                    );
                }
            }
        } finally {
            Unsafe.free(p, 1024, MemoryTag.NATIVE_DEFAULT);
        }
    }

    @Test
    public void testRowWordPacksCountAndCrc() {
        long w = QwoFormat.rowWord(123456, 0xDEADBEEF);
        Assert.assertEquals(123456, QwoFormat.rowCountOf(w));
        Assert.assertEquals(0xDEADBEEF, QwoFormat.crcOf(w));
    }

    @Test
    public void testValueWidth() {
        Assert.assertEquals(1, QwoFormat.valueWidth(QwpConstants.TYPE_BYTE, 0));
        Assert.assertEquals(2, QwoFormat.valueWidth(QwpConstants.TYPE_SHORT, 0));
        Assert.assertEquals(2, QwoFormat.valueWidth(QwpConstants.TYPE_CHAR, 0));
        Assert.assertEquals(4, QwoFormat.valueWidth(QwpConstants.TYPE_IPv4, 0));
        Assert.assertEquals(8, QwoFormat.valueWidth(QwpConstants.TYPE_DECIMAL64, 0));
        Assert.assertEquals(16, QwoFormat.valueWidth(QwpConstants.TYPE_UUID, 0));
        Assert.assertEquals(32, QwoFormat.valueWidth(QwpConstants.TYPE_DECIMAL256, 0));
        Assert.assertEquals(1, QwoFormat.valueWidth(QwpConstants.TYPE_GEOHASH, 8));
        Assert.assertEquals(2, QwoFormat.valueWidth(QwpConstants.TYPE_GEOHASH, 9));
        Assert.assertEquals(8, QwoFormat.valueWidth(QwpConstants.TYPE_GEOHASH, 60));
        Assert.assertTrue(QwoFormat.isVariableSize(QwpConstants.TYPE_VARCHAR));
        Assert.assertFalse(QwoFormat.isVariableSize(QwpConstants.TYPE_LONG));
    }

    private static void flip(long addr) {
        Unsafe.getUnsafe().putByte(addr, (byte) ~Unsafe.getUnsafe().getByte(addr));
    }
}
