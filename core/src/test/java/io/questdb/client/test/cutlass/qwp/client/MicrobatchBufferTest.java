/*******************************************************************************
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

import io.questdb.client.cutlass.qwp.client.MicrobatchBuffer;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MicrobatchBufferTest {

    @Test
    public void testConcurrentBatchIdUniqueness() throws Exception {
        int threadCount = 8;
        int buffersPerThread = 500;
        int totalBuffers = threadCount * buffersPerThread;
        Set<Long> batchIds = ConcurrentHashMap.newKeySet();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
            threads[t] = new Thread(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                try {
                    for (int i = 0; i < buffersPerThread; i++) {
                        MicrobatchBuffer buf = new MicrobatchBuffer(64);
                        batchIds.add(buf.getBatchId());
                        buf.close();
                    }
                } finally {
                    doneLatch.countDown();
                }
            });
            threads[t].start();
        }

        startLatch.countDown();
        assertTrue("Threads did not finish in time", doneLatch.await(30, TimeUnit.SECONDS));

        assertEquals(
                "Duplicate batch IDs detected: expected " + totalBuffers + " unique IDs but got " + batchIds.size(),
                totalBuffers,
                batchIds.size()
        );
    }

    @Test
    public void testConcurrentResetBatchIdUniqueness() throws Exception {
        int threadCount = 8;
        int resetsPerThread = 500;
        int totalIds = threadCount * resetsPerThread;
        Set<Long> batchIds = ConcurrentHashMap.newKeySet();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        Thread[] threads = new Thread[threadCount];
        for (int t = 0; t < threadCount; t++) {
            threads[t] = new Thread(() -> {
                try {
                    startLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                try {
                    MicrobatchBuffer buf = new MicrobatchBuffer(64);
                    for (int i = 0; i < resetsPerThread; i++) {
                        buf.seal();
                        buf.markSending();
                        buf.markRecycled();
                        buf.reset();
                        batchIds.add(buf.getBatchId());
                    }
                    buf.close();
                } finally {
                    doneLatch.countDown();
                }
            });
            threads[t].start();
        }

        startLatch.countDown();
        assertTrue("Threads did not finish in time", doneLatch.await(30, TimeUnit.SECONDS));

        assertEquals(
                "Duplicate batch IDs from reset(): expected " + totalIds + " unique IDs but got " + batchIds.size(),
                totalIds,
                batchIds.size()
        );
    }
}
