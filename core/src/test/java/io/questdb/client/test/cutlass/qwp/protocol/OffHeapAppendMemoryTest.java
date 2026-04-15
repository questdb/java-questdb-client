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

package io.questdb.client.test.cutlass.qwp.protocol;

import io.questdb.client.cutlass.qwp.protocol.OffHeapAppendMemory;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Test;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OffHeapAppendMemoryTest {

    @Test
    public void testCloseFreesMemory() throws Exception {
        assertMemoryLeak(() -> {
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ILP_RSS);
            OffHeapAppendMemory mem = new OffHeapAppendMemory(1024);
            long during = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ILP_RSS);
            assertTrue(during > before);

            mem.close();
            long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ILP_RSS);
            assertEquals(before, after);
        });
    }

    @Test
    public void testDoubleCloseIsSafe() throws Exception {
        assertMemoryLeak(() -> {
            OffHeapAppendMemory mem = new OffHeapAppendMemory();
            mem.putInt(42);
            mem.close();
            mem.close(); // should not throw
        });
    }

    @Test
    public void testGrowth() throws Exception {
        assertMemoryLeak(() -> {
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ILP_RSS);
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory(8)) {
                // Write more data than initial capacity to force growth
                for (int i = 0; i < 100; i++) {
                    mem.putLong(i);
                }

                assertEquals(800, mem.getAppendOffset());
                for (int i = 0; i < 100; i++) {
                    assertEquals(i, Unsafe.getUnsafe().getLong(mem.addressOf((long) i * 8)));
                }
            }
            long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ILP_RSS);
            assertEquals(before, after);
        });
    }

    @Test
    public void testJumpTo() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                mem.putLong(100);
                mem.putLong(200);
                mem.putLong(300);
                assertEquals(24, mem.getAppendOffset());

                // Jump back to offset 8 (after first long)
                mem.jumpTo(8);
                assertEquals(8, mem.getAppendOffset());

                // Write new value at offset 8
                mem.putLong(999);
                assertEquals(16, mem.getAppendOffset());
                assertEquals(100, Unsafe.getUnsafe().getLong(mem.addressOf(0)));
                assertEquals(999, Unsafe.getUnsafe().getLong(mem.addressOf(8)));
            }
        });
    }

    @Test
    public void testLargeGrowth() throws Exception {
        assertMemoryLeak(() -> {
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ILP_RSS);
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory(8)) {
                // Write 10000 doubles to stress growth
                for (int i = 0; i < 10_000; i++) {
                    mem.putDouble(i * 1.1);
                }
                assertEquals(80_000, mem.getAppendOffset());

                // Verify first and last values
                assertEquals(0.0, Unsafe.getUnsafe().getDouble(mem.addressOf(0)), 0.0);
                assertEquals(9999 * 1.1, Unsafe.getUnsafe().getDouble(mem.addressOf(79_992)), 0.001);
            }
            long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ILP_RSS);
            assertEquals(before, after);
        });
    }

    @Test
    public void testMixedTypes() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                mem.putByte((byte) 1);
                mem.putShort((short) 2);
                mem.putInt(3);
                mem.putLong(4L);
                mem.putFloat(5.0f);
                mem.putDouble(6.0);

                long addr = mem.pageAddress();
                assertEquals(1, Unsafe.getUnsafe().getByte(addr));
                assertEquals(2, Unsafe.getUnsafe().getShort(addr + 1));
                assertEquals(3, Unsafe.getUnsafe().getInt(addr + 3));
                assertEquals(4L, Unsafe.getUnsafe().getLong(addr + 7));
                assertEquals(5.0f, Unsafe.getUnsafe().getFloat(addr + 15), 0.0f);
                assertEquals(6.0, Unsafe.getUnsafe().getDouble(addr + 19), 0.0);
                assertEquals(27, mem.getAppendOffset());
            }
        });
    }

    @Test
    public void testPageAddress() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                assertTrue(mem.pageAddress() != 0);
                assertEquals(mem.pageAddress(), mem.addressOf(0));
                mem.putLong(42);
                assertEquals(mem.pageAddress() + 8, mem.addressOf(8));
            }
        });
    }

    @Test
    public void testPutAndReadByte() throws Exception {
        assertMemoryLeak(() -> {
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ILP_RSS);
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                mem.putByte((byte) 42);
                mem.putByte((byte) -1);
                mem.putByte((byte) 0);

                assertEquals(3, mem.getAppendOffset());
                assertEquals(42, Unsafe.getUnsafe().getByte(mem.addressOf(0)));
                assertEquals(-1, Unsafe.getUnsafe().getByte(mem.addressOf(1)));
                assertEquals(0, Unsafe.getUnsafe().getByte(mem.addressOf(2)));
            }
            long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ILP_RSS);
            assertEquals(before, after);
        });
    }

    @Test
    public void testPutAndReadDouble() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                mem.putDouble(2.718281828);
                mem.putDouble(Double.NaN);

                assertEquals(16, mem.getAppendOffset());
                assertEquals(2.718281828, Unsafe.getUnsafe().getDouble(mem.addressOf(0)), 0.0);
                assertTrue(Double.isNaN(Unsafe.getUnsafe().getDouble(mem.addressOf(8))));
            }
        });
    }

    @Test
    public void testPutAndReadFloat() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                mem.putFloat(3.14f);
                mem.putFloat(Float.NaN);

                assertEquals(8, mem.getAppendOffset());
                assertEquals(3.14f, Unsafe.getUnsafe().getFloat(mem.addressOf(0)), 0.0f);
                assertTrue(Float.isNaN(Unsafe.getUnsafe().getFloat(mem.addressOf(4))));
            }
        });
    }

    @Test
    public void testPutAndReadInt() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                mem.putInt(100_000);
                mem.putInt(Integer.MIN_VALUE);

                assertEquals(8, mem.getAppendOffset());
                assertEquals(100_000, Unsafe.getUnsafe().getInt(mem.addressOf(0)));
                assertEquals(Integer.MIN_VALUE, Unsafe.getUnsafe().getInt(mem.addressOf(4)));
            }
        });
    }

    @Test
    public void testPutAndReadLong() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                mem.putLong(1_000_000_000_000L);
                mem.putLong(Long.MIN_VALUE);

                assertEquals(16, mem.getAppendOffset());
                assertEquals(1_000_000_000_000L, Unsafe.getUnsafe().getLong(mem.addressOf(0)));
                assertEquals(Long.MIN_VALUE, Unsafe.getUnsafe().getLong(mem.addressOf(8)));
            }
        });
    }

    @Test
    public void testPutAndReadShort() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                mem.putShort((short) 12_345);
                mem.putShort(Short.MIN_VALUE);
                mem.putShort(Short.MAX_VALUE);

                assertEquals(6, mem.getAppendOffset());
                assertEquals(12_345, Unsafe.getUnsafe().getShort(mem.addressOf(0)));
                assertEquals(Short.MIN_VALUE, Unsafe.getUnsafe().getShort(mem.addressOf(2)));
                assertEquals(Short.MAX_VALUE, Unsafe.getUnsafe().getShort(mem.addressOf(4)));
            }
        });
    }

    @Test
    public void testPutBoolean() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                mem.putBoolean(true);
                mem.putBoolean(false);
                mem.putBoolean(true);

                assertEquals(3, mem.getAppendOffset());
                assertEquals(1, Unsafe.getUnsafe().getByte(mem.addressOf(0)));
                assertEquals(0, Unsafe.getUnsafe().getByte(mem.addressOf(1)));
                assertEquals(1, Unsafe.getUnsafe().getByte(mem.addressOf(2)));
            }
        });
    }

    @Test
    public void testPutUtf8Ascii() throws Exception {
        assertMemoryLeak(() -> {
            long before = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ILP_RSS);
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                mem.putUtf8("hello");
                assertEquals(5, mem.getAppendOffset());
                assertEquals('h', Unsafe.getUnsafe().getByte(mem.addressOf(0)));
                assertEquals('e', Unsafe.getUnsafe().getByte(mem.addressOf(1)));
                assertEquals('l', Unsafe.getUnsafe().getByte(mem.addressOf(2)));
                assertEquals('l', Unsafe.getUnsafe().getByte(mem.addressOf(3)));
                assertEquals('o', Unsafe.getUnsafe().getByte(mem.addressOf(4)));
            }
            long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_ILP_RSS);
            assertEquals(before, after);
        });
    }

    @Test
    public void testPutUtf8Empty() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                mem.putUtf8("");
                assertEquals(0, mem.getAppendOffset());
            }
        });
    }

    @Test
    public void testPutUtf8InvalidSurrogatePair() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                // High surrogate \uD800 followed by non-low-surrogate 'X'.
                // Should produce '?' for the lone high surrogate, then 'X'.
                mem.putUtf8("\uD800X");
                assertEquals(2, mem.getAppendOffset());
                assertEquals((byte) '?', Unsafe.getUnsafe().getByte(mem.addressOf(0)));
                assertEquals((byte) 'X', Unsafe.getUnsafe().getByte(mem.addressOf(1)));
            }
        });
    }

    @Test
    public void testPutUtf8Mixed() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                // Mix: ASCII "A" (1 byte) + e-acute (2 bytes) + CJK (3 bytes) + emoji (4 bytes) = 10 bytes
                mem.putUtf8("Aé世\uD83D\uDE00");
                assertEquals(10, mem.getAppendOffset());
            }
        });
    }

    @Test
    public void testPutUtf8MultiByte() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                // 2-byte: U+00E9 (e-acute) = C3 A9
                mem.putUtf8("é");
                assertEquals(2, mem.getAppendOffset());
                assertEquals((byte) 0xC3, Unsafe.getUnsafe().getByte(mem.addressOf(0)));
                assertEquals((byte) 0xA9, Unsafe.getUnsafe().getByte(mem.addressOf(1)));
            }
        });
    }

    @Test
    public void testPutUtf8Null() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                mem.putUtf8(null);
                assertEquals(0, mem.getAppendOffset());
            }
        });
    }

    @Test
    public void testPutUtf8SurrogatePairs() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                // U+1F600 (grinning face) = F0 9F 98 80
                mem.putUtf8("\uD83D\uDE00");
                assertEquals(4, mem.getAppendOffset());
                assertEquals((byte) 0xF0, Unsafe.getUnsafe().getByte(mem.addressOf(0)));
                assertEquals((byte) 0x9F, Unsafe.getUnsafe().getByte(mem.addressOf(1)));
                assertEquals((byte) 0x98, Unsafe.getUnsafe().getByte(mem.addressOf(2)));
                assertEquals((byte) 0x80, Unsafe.getUnsafe().getByte(mem.addressOf(3)));
            }
        });
    }

    @Test
    public void testPutUtf8ThreeByte() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                // 3-byte: U+4E16 (CJK character) = E4 B8 96
                mem.putUtf8("世");
                assertEquals(3, mem.getAppendOffset());
                assertEquals((byte) 0xE4, Unsafe.getUnsafe().getByte(mem.addressOf(0)));
                assertEquals((byte) 0xB8, Unsafe.getUnsafe().getByte(mem.addressOf(1)));
                assertEquals((byte) 0x96, Unsafe.getUnsafe().getByte(mem.addressOf(2)));
            }
        });
    }

    @Test
    public void testSkip() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                mem.putInt(1);
                mem.skip(8);
                mem.putInt(2);

                assertEquals(16, mem.getAppendOffset());
                assertEquals(1, Unsafe.getUnsafe().getInt(mem.addressOf(0)));
                assertEquals(2, Unsafe.getUnsafe().getInt(mem.addressOf(12)));
            }
        });
    }

    @Test
    public void testTruncate() throws Exception {
        assertMemoryLeak(() -> {
            try (OffHeapAppendMemory mem = new OffHeapAppendMemory()) {
                mem.putInt(1);
                mem.putInt(2);
                mem.putInt(3);
                assertEquals(12, mem.getAppendOffset());

                mem.truncate();
                assertEquals(0, mem.getAppendOffset());

                // Can write again after truncate
                mem.putInt(42);
                assertEquals(4, mem.getAppendOffset());
                assertEquals(42, Unsafe.getUnsafe().getInt(mem.addressOf(0)));
            }
        });
    }
}
