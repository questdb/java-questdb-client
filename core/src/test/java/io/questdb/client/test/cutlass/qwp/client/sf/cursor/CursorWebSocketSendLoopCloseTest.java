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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;

public class CursorWebSocketSendLoopCloseTest {

    /**
     * Regression: {@code close()} must not hang if {@code start()} threw
     * <em>after</em> assigning {@code ioThread} but before {@code ioThread.start()}
     * succeeded — e.g. native stack allocation OOM at the JVM level.
     * <p>
     * In that window, {@code ioThread != null} but the {@code ioLoop()} body
     * never ran, so the {@code shutdownLatch} is stuck at count 1 forever.
     * Pre-fix {@code close()} blocks indefinitely on {@code shutdownLatch.await()}.
     */
    @Test
    public void testCloseDoesNotHangIfStartFailedAfterIoThreadAssigned() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Bypass the constructor entirely. We're not exercising the loop's
            // wire path — only the close() teardown contract for a corrupted
            // post-start state.
            CursorWebSocketSendLoop loop =
                    (CursorWebSocketSendLoop) Unsafe.getUnsafe().allocateInstance(CursorWebSocketSendLoop.class);

            // Reproduce the bad state: ioThread non-null (so close() awaits the
            // latch), latch count = 1 (no ioLoop ever ran, so it's never counted
            // down), running irrelevant.
            setField(loop, "shutdownLatch", new CountDownLatch(1));
            Thread orphan = new Thread(() -> { /* never started */ }, "orphan-io-thread");
            setField(loop, "ioThread", orphan);

            // Run close() on a worker so a hang doesn't deadlock the test JVM.
            Thread closer = new Thread(loop::close, "close-runner");
            closer.setDaemon(true);
            closer.start();
            closer.join(2_000L);

            Assert.assertFalse(
                    "close() hung waiting on shutdownLatch — start() partial-failure "
                            + "leaves ioThread assigned but the latch is never counted down",
                    closer.isAlive());
        });
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }
}
