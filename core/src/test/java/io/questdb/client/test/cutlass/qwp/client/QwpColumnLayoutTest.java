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

import io.questdb.client.cutlass.qwp.client.QwpColumnLayout;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Coverage for {@link QwpColumnLayout}: pooled per-column scratch state used
 * by the result-batch decoder. The layout's fields are package-private and
 * stamped by the decoder on the hot path; we use reflection to write the
 * fields the decoder would set, then verify that {@code clear()} /
 * {@code close()} clean up correctly and that the lazy-allocating
 * {@code ensureOwnedEntriesAddr} / {@code ensureTimestampDecodeAddr} grow
 * monotonically. All native allocations are accounted for via
 * {@code TestUtils.assertMemoryLeak}.
 */
public class QwpColumnLayoutTest {

    @Test
    public void testClearResetsScalarsButPreservesSymbolCacheReference() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpColumnLayout layout = new QwpColumnLayout()) {
                writeLong(layout, "valuesAddr", 0xDEADBEEFL);
                writeLong(layout, "nullBitmapAddr", 0xCAFEBABEL);
                writeLong(layout, "stringBytesAddr", 0x1000L);
                writeLong(layout, "symbolDictHeapAddr", 0x2000L);
                writeLong(layout, "symbolDictEntriesAddr", 0x3000L);
                writeLong(layout, "nextAddr", 0x4000L);
                writeInt(layout, "nonNullCount", 99);
                writeInt(layout, "symbolDictSize", 7);

                Object cacheBefore = readField(layout, "symbolStringCache");
                Assert.assertNotNull("symbolStringCache must be eagerly initialised", cacheBefore);

                layout.clear();

                Assert.assertEquals(0L, readLong(layout, "valuesAddr"));
                Assert.assertEquals(0L, readLong(layout, "nullBitmapAddr"));
                Assert.assertEquals(0L, readLong(layout, "stringBytesAddr"));
                Assert.assertEquals(0L, readLong(layout, "symbolDictHeapAddr"));
                Assert.assertEquals(0L, readLong(layout, "symbolDictEntriesAddr"));
                Assert.assertEquals(0L, readLong(layout, "nextAddr"));
                Assert.assertEquals(0, readInt(layout, "nonNullCount"));
                Assert.assertEquals(0, readInt(layout, "symbolDictSize"));
                Assert.assertNull("clear() must drop the schema info reference", readField(layout, "info"));

                // The symbol-string cache is intentionally retained across batches;
                // clear() must not wipe or replace it. Lazy invalidation keys off
                // symbolDictVersion vs. symbolCacheVersion at lookup time.
                Assert.assertSame("clear() must NOT replace the symbolStringCache instance",
                        cacheBefore, readField(layout, "symbolStringCache"));
            }
        });
    }

    @Test
    public void testEnsureOwnedEntriesAddrLazyAllocAndGrowth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpColumnLayout layout = new QwpColumnLayout()) {
                Method ensure = QwpColumnLayout.class.getDeclaredMethod("ensureOwnedEntriesAddr", int.class);
                ensure.setAccessible(true);

                // First call -- buffer was 0, so newCap = max(0, max(64, 16)) = 64.
                long addr1 = (long) ensure.invoke(layout, 16);
                Assert.assertNotEquals("alloc must produce a non-zero address", 0L, addr1);
                Assert.assertEquals(64, readInt(layout, "ownedEntriesCapacity"));

                // Re-call within capacity -- address must be returned without realloc.
                long addr2 = (long) ensure.invoke(layout, 32);
                Assert.assertEquals("within-capacity request must reuse the existing buffer",
                        addr1, addr2);
                Assert.assertEquals(64, readInt(layout, "ownedEntriesCapacity"));

                // Grow request -- newCap must be at least max(prev*2, requested).
                long addr3 = (long) ensure.invoke(layout, 200);
                Assert.assertNotEquals("grow must produce a non-zero address", 0L, addr3);
                Assert.assertTrue("capacity must double-and-floor at requested",
                        readInt(layout, "ownedEntriesCapacity") >= 200);

                // Even larger request -- doubling beats the requested value.
                long prevCap = readInt(layout, "ownedEntriesCapacity");
                ensure.invoke(layout, (int) prevCap + 1);
                Assert.assertTrue("capacity must grow past the previous capacity",
                        readInt(layout, "ownedEntriesCapacity") > prevCap);
            }
        });
    }

    @Test
    public void testEnsureTimestampDecodeAddrLazyAllocAndGrowth() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpColumnLayout layout = new QwpColumnLayout()) {
                Method ensure = QwpColumnLayout.class.getDeclaredMethod("ensureTimestampDecodeAddr", int.class);
                ensure.setAccessible(true);

                long addr1 = (long) ensure.invoke(layout, 8);
                Assert.assertNotEquals(0L, addr1);
                Assert.assertEquals("min capacity floors at 64 even when requested is smaller",
                        64, readInt(layout, "timestampDecodeCapacity"));

                long addr2 = (long) ensure.invoke(layout, 64);
                Assert.assertEquals("at-cap request must reuse buffer", addr1, addr2);

                long addr3 = (long) ensure.invoke(layout, 65);
                Assert.assertNotEquals(0L, addr3);
                Assert.assertTrue(readInt(layout, "timestampDecodeCapacity") >= 65);
            }
        });
    }

    @Test
    public void testCloseFreesBothBuffers() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpColumnLayout layout = new QwpColumnLayout();
            Method ensureEntries = QwpColumnLayout.class.getDeclaredMethod("ensureOwnedEntriesAddr", int.class);
            Method ensureTs = QwpColumnLayout.class.getDeclaredMethod("ensureTimestampDecodeAddr", int.class);
            ensureEntries.setAccessible(true);
            ensureTs.setAccessible(true);

            ensureEntries.invoke(layout, 128);
            ensureTs.invoke(layout, 128);
            Assert.assertNotEquals(0L, readLong(layout, "ownedEntriesAddr"));
            Assert.assertNotEquals(0L, readLong(layout, "timestampDecodeAddr"));

            layout.close();

            Assert.assertEquals(0L, readLong(layout, "ownedEntriesAddr"));
            Assert.assertEquals(0L, readLong(layout, "timestampDecodeAddr"));
            Assert.assertEquals(0, readInt(layout, "ownedEntriesCapacity"));
            Assert.assertEquals(0, readInt(layout, "timestampDecodeCapacity"));
        });
    }

    @Test
    public void testCloseIsIdempotent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpColumnLayout layout = new QwpColumnLayout();
            Method ensureEntries = QwpColumnLayout.class.getDeclaredMethod("ensureOwnedEntriesAddr", int.class);
            ensureEntries.setAccessible(true);
            ensureEntries.invoke(layout, 64);
            layout.close();
            // Second close must be a no-op (and must not double-free).
            layout.close();
            layout.close();
            Assert.assertEquals(0L, readLong(layout, "ownedEntriesAddr"));
        });
    }

    @Test
    public void testCloseOnFreshLayoutIsNoOp() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpColumnLayout layout = new QwpColumnLayout();
            // Never allocated; close() must handle the all-zero case.
            layout.close();
            Assert.assertEquals(0L, readLong(layout, "ownedEntriesAddr"));
            Assert.assertEquals(0L, readLong(layout, "timestampDecodeAddr"));
        });
    }

    @Test
    public void testDenseIndexFastPathWhenNoNulls() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpColumnLayout layout = new QwpColumnLayout()) {
                // nullBitmapAddr == 0 means "column has no nulls in this batch";
                // dense index equals row index. The decoder skips populating
                // nonNullIdx in that case, so we must NOT touch it here.
                writeLong(layout, "nullBitmapAddr", 0L);
                Assert.assertEquals(0, layout.denseIndex(0));
                Assert.assertEquals(7, layout.denseIndex(7));
                Assert.assertEquals(99, layout.denseIndex(99));
            }
        });
    }

    @Test
    public void testDenseIndexSlowPathReadsNonNullIdx() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpColumnLayout layout = new QwpColumnLayout()) {
                // Non-zero nullBitmapAddr -- decoder has populated nonNullIdx.
                // Row 1 is null (-1), rows 0/2/3 map to dense indices 0/1/2.
                writeLong(layout, "nullBitmapAddr", 0xABCDL);
                writeField(layout, "nonNullIdx", new int[]{0, -1, 1, 2});

                Assert.assertEquals(0, layout.denseIndex(0));
                Assert.assertEquals(-1, layout.denseIndex(1));
                Assert.assertEquals(1, layout.denseIndex(2));
                Assert.assertEquals(2, layout.denseIndex(3));
            }
        });
    }

    private static long readLong(Object target, String name) throws Exception {
        Field f = QwpColumnLayout.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getLong(target);
    }

    private static int readInt(Object target, String name) throws Exception {
        Field f = QwpColumnLayout.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getInt(target);
    }

    private static Object readField(Object target, String name) throws Exception {
        Field f = QwpColumnLayout.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }

    private static void writeLong(Object target, String name, long value) throws Exception {
        Field f = QwpColumnLayout.class.getDeclaredField(name);
        f.setAccessible(true);
        f.setLong(target, value);
    }

    private static void writeInt(Object target, String name, int value) throws Exception {
        Field f = QwpColumnLayout.class.getDeclaredField(name);
        f.setAccessible(true);
        f.setInt(target, value);
    }

    private static void writeField(Object target, String name, Object value) throws Exception {
        Field f = QwpColumnLayout.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }
}
