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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.client.test.std;

import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class UnsafeMemoryAccountingTest {
    private static final int ITERATIONS = 100_000;

    @Test
    public void testAllocationAccounting() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertAllocationAccounting(MemoryTag.NATIVE_DEFAULT, true);
            assertAllocationAccounting(MemoryTag.NATIVE_PATH, false);
        });
    }

    @Test
    public void testConcurrentCounters() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long mallocs = Unsafe.getMallocCount();
            long reallocs = Unsafe.getReallocCount();
            long frees = Unsafe.getFreeCount();
            int threadCount = 4;
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService executor = Executors.newFixedThreadPool(threadCount, runnable -> {
                Thread thread = new Thread(runnable, "native-counter-test");
                thread.setDaemon(true);
                return thread;
            });
            try {
                Future<?>[] tasks = new Future<?>[threadCount];
                for (int i = 0; i < threadCount; i++) {
                    tasks[i] = executor.submit(() -> {
                        start.await();
                        for (int j = 0; j < ITERATIONS && !Thread.currentThread().isInterrupted(); j++) {
                            allocationCycle((j & 1) == 0 ? MemoryTag.NATIVE_DEFAULT : MemoryTag.NATIVE_PATH);
                        }
                        return null;
                    });
                }
                start.countDown();
                for (Future<?> task : tasks) {
                    task.get(30, TimeUnit.SECONDS);
                }
            } finally {
                start.countDown();
                executor.shutdownNow();
                Assert.assertTrue("counter workers did not stop", executor.awaitTermination(30, TimeUnit.SECONDS));
            }
            assertCounts(mallocs, reallocs, frees, (long) threadCount * ITERATIONS);
        });
    }

    @Test
    public void testLargeAdjustments() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            assertAdjustment(MemoryTag.NATIVE_DEFAULT, true);
            assertAdjustment(MemoryTag.MMAP_DEFAULT, false);
        });
    }

    @Test
    public void testRepeatedCounters() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            long mallocs = Unsafe.getMallocCount();
            long reallocs = Unsafe.getReallocCount();
            long frees = Unsafe.getFreeCount();
            for (int i = 0; i < ITERATIONS; i++) {
                allocationCycle((i & 1) == 0 ? MemoryTag.NATIVE_DEFAULT : MemoryTag.NATIVE_PATH);
            }
            assertCounts(mallocs, reallocs, frees, ITERATIONS);
        });
    }

    private static void allocationCycle(int memoryTag) {
        long size = 8;
        long ptr = Unsafe.malloc(size, memoryTag);
        try {
            ptr = Unsafe.realloc(ptr, size, 32, memoryTag);
            size = 32;
        } finally {
            Unsafe.free(ptr, size, memoryTag);
        }
    }

    private static void assertAdjustment(int memoryTag, boolean isRss) {
        long bytes = 1L << 33;
        long total = Unsafe.getMemUsed();
        long rss = Unsafe.getRssMemUsed();
        long tagged = Unsafe.getMemUsedByTag(memoryTag);
        Unsafe.recordMemAlloc(bytes, memoryTag);
        try {
            Assert.assertEquals(total + bytes, Unsafe.getMemUsed());
            Assert.assertEquals(rss + (isRss ? bytes : 0), Unsafe.getRssMemUsed());
            Assert.assertEquals(tagged + bytes, Unsafe.getMemUsedByTag(memoryTag));
            Unsafe.recordMemAlloc(0, memoryTag);
            Assert.assertEquals(total + bytes, Unsafe.getMemUsed());
        } finally {
            Unsafe.recordMemAlloc(-bytes, memoryTag);
        }
        Assert.assertEquals(total, Unsafe.getMemUsed());
        Assert.assertEquals(rss, Unsafe.getRssMemUsed());
        Assert.assertEquals(tagged, Unsafe.getMemUsedByTag(memoryTag));
    }

    private static void assertAllocationAccounting(int memoryTag, boolean isRss) {
        long mallocs = Unsafe.getMallocCount();
        long reallocs = Unsafe.getReallocCount();
        long frees = Unsafe.getFreeCount();
        long total = Unsafe.getMemUsed();
        long rss = Unsafe.getRssMemUsed();
        long tagged = Unsafe.getMemUsedByTag(memoryTag);
        long size = 8;
        long ptr = Unsafe.malloc(size, memoryTag);
        try {
            assertMemory(total, rss, tagged, memoryTag, isRss, size);
            ptr = Unsafe.realloc(ptr, size, 32, memoryTag);
            size = 32;
            assertMemory(total, rss, tagged, memoryTag, isRss, size);
            ptr = Unsafe.realloc(ptr, size, size, memoryTag);
            assertMemory(total, rss, tagged, memoryTag, isRss, size);
            ptr = Unsafe.realloc(ptr, size, 4, memoryTag);
            size = 4;
            assertMemory(total, rss, tagged, memoryTag, isRss, size);
        } finally {
            Unsafe.free(ptr, size, memoryTag);
        }
        assertMemory(total, rss, tagged, memoryTag, isRss, 0);
        Assert.assertEquals(mallocs + 1, Unsafe.getMallocCount());
        Assert.assertEquals(reallocs + 3, Unsafe.getReallocCount());
        Assert.assertEquals(frees + 1, Unsafe.getFreeCount());
    }

    private static void assertCounts(long mallocs, long reallocs, long frees, long count) {
        Assert.assertEquals(mallocs + count, Unsafe.getMallocCount());
        Assert.assertEquals(reallocs + count, Unsafe.getReallocCount());
        Assert.assertEquals(frees + count, Unsafe.getFreeCount());
    }

    private static void assertMemory(long total, long rss, long tagged, int memoryTag, boolean isRss, long size) {
        Assert.assertEquals(total + size, Unsafe.getMemUsed());
        Assert.assertEquals(rss + (isRss ? size : 0), Unsafe.getRssMemUsed());
        Assert.assertEquals(tagged + size, Unsafe.getMemUsedByTag(memoryTag));
    }
}
