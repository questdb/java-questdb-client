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

import io.questdb.client.cutlass.qwp.client.NativeBufferWriter;
import io.questdb.client.std.Unsafe;
import io.questdb.client.std.str.Utf8s;
import org.junit.Assert;
import org.junit.Test;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.*;

public class NativeBufferWriterTest {

    @Test
    public void testEnsureCapacityGrowsBuffer() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter(16)) {
                assertEquals(16, writer.getCapacity());
                writer.ensureCapacity(32);
                assertTrue(writer.getCapacity() >= 32);
            }
        });
    }

    @Test
    public void testGetWriteAddressAndWritableBytes() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter(64)) {
                // Initially at position 0
                assertEquals(writer.getBufferPtr(), writer.getWriteAddress());
                assertEquals(64, writer.getWritableBytes());

                // Write some data
                writer.putInt(42);
                writer.putLong(123L);
                assertEquals(12, writer.getPosition());
                assertEquals(writer.getBufferPtr() + 12, writer.getWriteAddress());
                assertEquals(writer.getCapacity() - 12, writer.getWritableBytes());

                // After ensureCapacity (may realloc), contract still holds
                writer.ensureCapacity(100);
                assertEquals(writer.getBufferPtr() + 12, writer.getWriteAddress());
                assertEquals(writer.getCapacity() - 12, writer.getWritableBytes());

                // Write directly at getWriteAddress(), then skip
                Unsafe.getUnsafe().putLong(writer.getWriteAddress(), 999L);
                writer.skip(8);
                assertEquals(20, writer.getPosition());
                assertEquals(writer.getBufferPtr() + 20, writer.getWriteAddress());
            }
        });
    }

    @Test
    public void testGrowBuffer() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter(16)) {
                // Write more than initial capacity
                for (int i = 0; i < 100; i++) {
                    writer.putLong(i);
                }
                Assert.assertEquals(800, writer.getPosition());
                // Verify data
                for (int i = 0; i < 100; i++) {
                    Assert.assertEquals(i, Unsafe.getUnsafe().getLong(writer.getBufferPtr() + i * 8));
                }
            }
        });
    }

    @Test
    public void testMultipleWrites() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                writer.putByte((byte) 'Q');
                writer.putByte((byte) 'W');
                writer.putByte((byte) 'P');
                writer.putByte((byte) '1');
                writer.putByte((byte) 1);  // Version
                writer.putByte((byte) 0);  // Flags
                writer.putShort((short) 1);  // Table count
                writer.putInt(0);  // Payload length placeholder

                Assert.assertEquals(12, writer.getPosition());

                // Verify QWP1 header
                Assert.assertEquals((byte) 'Q', Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
                Assert.assertEquals((byte) 'W', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
                Assert.assertEquals((byte) 'P', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 2));
                Assert.assertEquals((byte) '1', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 3));
            }
        });
    }

    @Test
    public void testNativeBufferWriterUtf8LengthInvalidSurrogatePair() throws Exception {
        assertMemoryLeak(() -> {
            // High surrogate followed by non-low-surrogate: '?' (1) + 'X' (1) = 2
            assertEquals(2, NativeBufferWriter.utf8Length("\uD800X"));
            // Lone high surrogate at end: '?' (1)
            assertEquals(1, NativeBufferWriter.utf8Length("\uD800"));
            // Lone low surrogate: '?' (1)
            assertEquals(1, NativeBufferWriter.utf8Length("\uDC00"));
            // Valid pair still works: 4 bytes
            assertEquals(4, NativeBufferWriter.utf8Length("\uD83D\uDE00"));
        });
    }

    @Test
    public void testPatchInt() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                writer.putInt(0);  // Placeholder at offset 0
                writer.putInt(100);  // At offset 4
                writer.patchInt(0, 42);  // Patch first int
                Assert.assertEquals(42, Unsafe.getUnsafe().getInt(writer.getBufferPtr()));
                Assert.assertEquals(100, Unsafe.getUnsafe().getInt(writer.getBufferPtr() + 4));
            }
        });
    }

    @Test
    public void testPatchIntAtLastValidOffset() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter(16)) {
                writer.putLong(0L); // 8 bytes, position = 8
                // Patch at offset 4 covers bytes [4..7], exactly at the boundary
                writer.patchInt(4, 0x1234);
                assertEquals(0x1234, Unsafe.getUnsafe().getInt(writer.getBufferPtr() + 4));
            }
        });
    }

    @Test
    public void testPatchIntAtValidOffset() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter(16)) {
                writer.putInt(0); // placeholder at offset 0
                writer.putInt(0xBEEF); // data at offset 4
                // Patch the placeholder
                writer.patchInt(0, 0xCAFE);
                assertEquals(0xCAFE, Unsafe.getUnsafe().getInt(writer.getBufferPtr()));
                assertEquals(0xBEEF, Unsafe.getUnsafe().getInt(writer.getBufferPtr() + 4));
            }
        });
    }

    @Test
    public void testPutBlockOfBytes() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter();
                 NativeBufferWriter source = new NativeBufferWriter()) {
                // Prepare source data
                source.putByte((byte) 1);
                source.putByte((byte) 2);
                source.putByte((byte) 3);
                source.putByte((byte) 4);

                // Copy to writer
                writer.putBlockOfBytes(source.getBufferPtr(), 4);
                Assert.assertEquals(4, writer.getPosition());
                Assert.assertEquals((byte) 1, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
                Assert.assertEquals((byte) 2, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
                Assert.assertEquals((byte) 3, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 2));
                Assert.assertEquals((byte) 4, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 3));
            }
        });
    }

    @Test
    public void testPutBlockOfBytesRejectsLenExceedingIntMax() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter(16)) {
                try {
                    writer.putBlockOfBytes(0, (long) Integer.MAX_VALUE + 1);
                    fail("expected IllegalArgumentException");
                } catch (IllegalArgumentException e) {
                    Assert.assertTrue(e.getMessage().contains("len"));
                }
            }
        });
    }

    @Test
    public void testPutUtf8InvalidSurrogatePair() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter(64)) {
                // High surrogate \uD800 followed by non-low-surrogate 'X'.
                // Should produce '?' for the lone high surrogate, then 'X'.
                writer.putUtf8("\uD800X");
                assertEquals(2, writer.getPosition());
                assertEquals((byte) '?', Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
                assertEquals((byte) 'X', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
            }
        });
    }

    @Test
    public void testPutUtf8LoneHighSurrogateAtEnd() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter(64)) {
                writer.putUtf8("\uD800");
                assertEquals(1, writer.getPosition());
                assertEquals((byte) '?', Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            }
        });
    }

    @Test
    public void testPutUtf8LoneLowSurrogate() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter(64)) {
                writer.putUtf8("\uDC00");
                assertEquals(1, writer.getPosition());
                assertEquals((byte) '?', Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            }
        });
    }

    @Test
    public void testPutUtf8LoneSurrogateMatchesUtf8Length() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter(64)) {
                // Verify putUtf8 and utf8Length agree for all lone surrogate cases
                String[] cases = {"\uD800", "\uDBFF", "\uDC00", "\uDFFF", "\uD800X", "A\uDC00B"};
                for (String s : cases) {
                    writer.reset();
                    writer.putUtf8(s);
                    assertEquals("length mismatch for: " + s.codePoints()
                                    .mapToObj(cp -> String.format("U+%04X", cp))
                                    .reduce((a, b) -> a + " " + b).orElse(""),
                            NativeBufferWriter.utf8Length(s), writer.getPosition());
                }
            }
        });
    }

    @Test
    public void testQwpBufferWriterUtf8LengthInvalidSurrogatePair() throws Exception {
        assertMemoryLeak(() -> {
            // High surrogate followed by non-low-surrogate: '?' (1) + 'X' (1) = 2
            assertEquals(2, NativeBufferWriter.utf8Length("\uD800X"));
            // Lone high surrogate at end: '?' (1)
            assertEquals(1, NativeBufferWriter.utf8Length("\uD800"));
            // Lone low surrogate: '?' (1)
            assertEquals(1, NativeBufferWriter.utf8Length("\uDC00"));
            // Valid pair still works: 4 bytes
            assertEquals(4, NativeBufferWriter.utf8Length("\uD83D\uDE00"));
        });
    }

    @Test
    public void testReset() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                writer.putInt(12345);
                Assert.assertEquals(4, writer.getPosition());
                writer.reset();
                Assert.assertEquals(0, writer.getPosition());
                // Can write again
                writer.putByte((byte) 0xFF);
                Assert.assertEquals(1, writer.getPosition());
            }
        });
    }

    @Test
    public void testSkipAdvancesPosition() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter(16)) {
                writer.skip(4);
                assertEquals(4, writer.getPosition());
                writer.skip(8);
                assertEquals(12, writer.getPosition());
            }
        });
    }

    @Test
    public void testSkipBeyondCapacityGrowsBuffer() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter(16)) {
                // skip past the 16-byte buffer — must grow, not corrupt memory
                writer.skip(32);
                assertEquals(32, writer.getPosition());
                assertTrue(writer.getCapacity() >= 32);
                // writing after the skip must also succeed
                writer.putInt(0xCAFE);
                assertEquals(36, writer.getPosition());
                assertEquals(0xCAFE, Unsafe.getUnsafe().getInt(writer.getBufferPtr() + 32));
            }
        });
    }

    @Test
    public void testSkipThenPatchInt() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter(8)) {
                int patchOffset = writer.getPosition();
                writer.skip(4); // reserve space for a length field
                writer.putInt(0xDEAD);
                // Patch the reserved space
                writer.patchInt(patchOffset, 4);
                assertEquals(0x4, Unsafe.getUnsafe().getInt(writer.getBufferPtr() + patchOffset));
                assertEquals(0xDEAD, Unsafe.getUnsafe().getInt(writer.getBufferPtr() + 4));
            }
        });
    }

    @Test
    public void testUtf8Length() throws Exception {
        assertMemoryLeak(() -> {
            Assert.assertEquals(0, NativeBufferWriter.utf8Length(null));
            Assert.assertEquals(0, NativeBufferWriter.utf8Length(""));
            Assert.assertEquals(5, NativeBufferWriter.utf8Length("hello"));
            Assert.assertEquals(2, NativeBufferWriter.utf8Length("ñ"));
            Assert.assertEquals(3, NativeBufferWriter.utf8Length("€"));
        });
    }

    @Test
    public void testVarintSizeMatchesEncodedLength() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                long[] values = {0, 1, 127, 128, 16383, 16384, 2_097_151, 2_097_152, Long.MAX_VALUE};
                for (long value : values) {
                    writer.reset();
                    writer.putVarint(value);
                    Assert.assertEquals("value=" + value, NativeBufferWriter.varintSize(value), writer.getPosition());
                }
            }
        });
    }

    @Test
    public void testWriteByte() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                writer.putByte((byte) 0x42);
                Assert.assertEquals(1, writer.getPosition());
                Assert.assertEquals((byte) 0x42, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            }
        });
    }

    @Test
    public void testWriteDouble() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                writer.putDouble(3.14159265359);
                Assert.assertEquals(8, writer.getPosition());
                Assert.assertEquals(3.14159265359, Unsafe.getUnsafe().getDouble(writer.getBufferPtr()), 0.0000000001);
            }
        });
    }

    @Test
    public void testWriteEmptyString() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                writer.putString("");
                Assert.assertEquals(1, writer.getPosition());
                Assert.assertEquals((byte) 0, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            }
        });
    }

    @Test
    public void testWriteFloat() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                writer.putFloat(3.14f);
                Assert.assertEquals(4, writer.getPosition());
                Assert.assertEquals(3.14f, Unsafe.getUnsafe().getFloat(writer.getBufferPtr()), 0.0001f);
            }
        });
    }

    @Test
    public void testWriteInt() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                writer.putInt(0x12345678);
                Assert.assertEquals(4, writer.getPosition());
                // Little-endian
                Assert.assertEquals(0x12345678, Unsafe.getUnsafe().getInt(writer.getBufferPtr()));
            }
        });
    }

    @Test
    public void testWriteLong() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                writer.putLong(0x123456789ABCDEF0L);
                Assert.assertEquals(8, writer.getPosition());
                Assert.assertEquals(0x123456789ABCDEF0L, Unsafe.getUnsafe().getLong(writer.getBufferPtr()));
            }
        });
    }

    @Test
    public void testWriteNullString() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                writer.putString(null);
                Assert.assertEquals(1, writer.getPosition());
                Assert.assertEquals((byte) 0, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            }
        });
    }

    @Test
    public void testWriteShort() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                writer.putShort((short) 0x1234);
                Assert.assertEquals(2, writer.getPosition());
                // Little-endian
                Assert.assertEquals((byte) 0x34, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
                Assert.assertEquals((byte) 0x12, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
            }
        });
    }

    @Test
    public void testWriteString() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                writer.putString("hello");
                // Length (1 byte varint) + 5 bytes
                Assert.assertEquals(6, writer.getPosition());
                // Check length
                Assert.assertEquals((byte) 5, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
                // Check content
                Assert.assertEquals((byte) 'h', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
                Assert.assertEquals((byte) 'e', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 2));
                Assert.assertEquals((byte) 'l', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 3));
                Assert.assertEquals((byte) 'l', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 4));
                Assert.assertEquals((byte) 'o', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 5));
            }
        });
    }

    @Test
    public void testWriteStringMixedAsciiAndNonAscii() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                // "hi€" = 'h'(1) + 'i'(1) + '€'(3) = 5 UTF-8 bytes
                // Exercises the ASCII fast path breaking out mid-string
                writer.putString("hi€");
                int utf8Len = 5;
                int varintLen = 1;
                Assert.assertEquals(varintLen + utf8Len, writer.getPosition());
                Assert.assertEquals((byte) utf8Len, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
                Assert.assertEquals((byte) 'h', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
                Assert.assertEquals((byte) 'i', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 2));
                Assert.assertEquals((byte) 0xE2, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 3));
                Assert.assertEquals((byte) 0x82, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 4));
                Assert.assertEquals((byte) 0xAC, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 5));
            }
        });
    }

    @Test
    public void testWriteStringNonAscii() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                // "€ñ" = '€'(3) + 'ñ'(2) = 5 UTF-8 bytes
                // Non-ASCII at position 0, triggers fallback immediately
                writer.putString("€ñ");
                int utf8Len = 5;
                Assert.assertEquals(1 + utf8Len, writer.getPosition());
                Assert.assertEquals((byte) utf8Len, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            }
        });
    }

    @Test
    public void testWriteUtf8MixedAsciiAndNonAscii() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                // "abc€def" = 3 + 3 + 3 = 9 UTF-8 bytes
                // ASCII fast path writes 'a','b','c', then hits '€' and falls back
                writer.putUtf8("abc€def");
                Assert.assertEquals(9, writer.getPosition());
                Assert.assertEquals((byte) 'a', Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
                Assert.assertEquals((byte) 'b', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
                Assert.assertEquals((byte) 'c', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 2));
                // € at bytes 3-5
                Assert.assertEquals((byte) 0xE2, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 3));
                Assert.assertEquals((byte) 0x82, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 4));
                Assert.assertEquals((byte) 0xAC, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 5));
                // 'def' at bytes 6-8
                Assert.assertEquals((byte) 'd', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 6));
                Assert.assertEquals((byte) 'e', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 7));
                Assert.assertEquals((byte) 'f', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 8));
            }
        });
    }

    @Test
    public void testWriteUtf8MixedAsciiAndNonAsciiAfterGrow() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter(8)) {
                String value = "abcdefghijklmnop世界世界世界世界世界世界世界世界世界世界";

                writer.putUtf8(value);

                int utf8Len = Utf8s.utf8Bytes(value);
                Assert.assertEquals(utf8Len, writer.getPosition());
                Assert.assertEquals(value, Utf8s.stringFromUtf8Bytes(writer.getBufferPtr(), writer.getBufferPtr() + utf8Len));
            }
        });
    }

    @Test
    public void testWriteUtf8Ascii() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                writer.putUtf8("ABC");
                Assert.assertEquals(3, writer.getPosition());
                Assert.assertEquals((byte) 'A', Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
                Assert.assertEquals((byte) 'B', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
                Assert.assertEquals((byte) 'C', Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 2));
            }
        });
    }

    @Test
    public void testWriteUtf8ThreeByte() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                // € is 3 bytes in UTF-8
                writer.putUtf8("€");
                Assert.assertEquals(3, writer.getPosition());
                Assert.assertEquals((byte) 0xE2, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
                Assert.assertEquals((byte) 0x82, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
                Assert.assertEquals((byte) 0xAC, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 2));
            }
        });
    }

    @Test
    public void testWriteUtf8TwoByte() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                // ñ is 2 bytes in UTF-8
                writer.putUtf8("ñ");
                Assert.assertEquals(2, writer.getPosition());
                Assert.assertEquals((byte) 0xC3, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
                Assert.assertEquals((byte) 0xB1, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
            }
        });
    }

    @Test
    public void testWriteVarintLarge() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                // Test larger value
                writer.putVarint(16384);
                Assert.assertEquals(3, writer.getPosition());
                // LEB128: 16384 = 0x80 0x80 0x01
                Assert.assertEquals((byte) 0x80, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
                Assert.assertEquals((byte) 0x80, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
                Assert.assertEquals((byte) 0x01, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 2));
            }
        });
    }

    @Test
    public void testWriteVarintMedium() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                // Two bytes for 128
                writer.putVarint(128);
                Assert.assertEquals(2, writer.getPosition());
                // LEB128: 128 = 0x80 0x01
                Assert.assertEquals((byte) 0x80, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
                Assert.assertEquals((byte) 0x01, Unsafe.getUnsafe().getByte(writer.getBufferPtr() + 1));
            }
        });
    }

    @Test
    public void testWriteVarintSmall() throws Exception {
        assertMemoryLeak(() -> {
            try (NativeBufferWriter writer = new NativeBufferWriter()) {
                // Single byte for values < 128
                writer.putVarint(127);
                Assert.assertEquals(1, writer.getPosition());
                Assert.assertEquals((byte) 127, Unsafe.getUnsafe().getByte(writer.getBufferPtr()));
            }
        });
    }
}
