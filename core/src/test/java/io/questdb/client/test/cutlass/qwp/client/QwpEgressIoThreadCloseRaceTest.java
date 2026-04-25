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

import io.questdb.client.cutlass.qwp.client.QwpBatchBuffer;
import io.questdb.client.cutlass.qwp.client.QwpEgressIoThread;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Races {@link QwpEgressIoThread#releaseBuffer} against
 * {@link QwpEgressIoThread#closePool} to confirm the TOCTOU close-race guard
 * holds: prior to the fix, a user thread could read {@code closed == false},
 * pause, let {@code closePool} drain and clear {@code freeBuffers}, then
 * offer its buffer into the emptied-but-still-instantiated queue -- stranding
 * the buffer's native scratch with no consumer to close it.
 * <p>
 * {@link TestUtils#assertMemoryLeak} makes the leak assertion automatic: any
 * {@link QwpBatchBuffer} not closed by either {@code releaseBuffer}'s
 * in-place fallback or {@code closePool}'s drain shows up as a native-memory
 * leak delta at the end of the test.
 */
public class QwpEgressIoThreadCloseRaceTest {

    @Test
    public void testReleaseBufferRacesClosePoolSafely() throws Exception {
        Method closePoolMethod = QwpEgressIoThread.class.getDeclaredMethod("closePool");
        closePoolMethod.setAccessible(true);
        TestUtils.assertMemoryLeak(() -> {
            int iterations = 200;
            for (int iter = 0; iter < iterations; iter++) {
                // A fresh IoThread per iteration -- each runs its own race
                // and must leave freeBuffers empty + all buffers closed by
                // the time both threads exit.
                QwpEgressIoThread io = new QwpEgressIoThread(null, /*bufferPoolSize=*/ 2,
                        (status, message) -> { /* unused */ });
                QwpBatchBuffer b0 = new QwpBatchBuffer(64);
                QwpBatchBuffer b1 = new QwpBatchBuffer(64);

                CountDownLatch start = new CountDownLatch(1);
                AtomicInteger failures = new AtomicInteger();

                Thread releaseThread = new Thread(() -> {
                    try {
                        start.await();
                        io.releaseBuffer(b0);
                        io.releaseBuffer(b1);
                    } catch (Throwable t) {
                        failures.incrementAndGet();
                    }
                }, "release-" + iter);
                Thread closeThread = new Thread(() -> {
                    try {
                        start.await();
                        closePoolMethod.invoke(io);
                    } catch (Throwable t) {
                        failures.incrementAndGet();
                    }
                }, "close-" + iter);

                releaseThread.start();
                closeThread.start();
                start.countDown();
                releaseThread.join(TimeUnit.SECONDS.toMillis(5));
                closeThread.join(TimeUnit.SECONDS.toMillis(5));

                if (failures.get() != 0) {
                    throw new AssertionError("releaseBuffer/closePool race threw on iteration " + iter);
                }
            }
        });
    }
}
