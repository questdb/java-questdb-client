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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Regression coverage (M3): {@code catch (Throwable)} in the reconnect
 * machinery used to swallow {@link java.lang.Error} (OOM, LinkageError,
 * StackOverflowError) into an indefinite "transport outage" retry with only
 * a throttled, possibly-null-message WARN as a trace. A JVM/programming
 * failure is not a transport outage -- retrying cannot clear it -- so the
 * blocking connection helper must rethrow {@code Error} immediately and the
 * background loop must latch it for the producer before stopping.
 */
public class CursorWebSocketSendLoopJvmErrorTest {

    @Test(timeout = 30_000)
    public void testIoLoopLatchesReconnectJvmErrorAndShutsDown() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        LinkageError injected = new LinkageError("simulated JVM failure");
        try (CursorSendEngine engine = new CursorSendEngine(null, 64 * 1024)) {
            CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                    null,
                    engine,
                    0L,
                    CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                    () -> {
                        attempts.incrementAndGet();
                        throw injected;
                    },
                    1L,
                    4L,
                    false
            );
            try {
                loop.start();
                long deadlineNanos = System.nanoTime() + 5_000_000_000L;
                while ((loop.getTerminalError() == null || loop.isRunning())
                        && System.nanoTime() < deadlineNanos) {
                    Thread.yield();
                }

                Throwable terminal = loop.getTerminalError();
                Assert.assertNotNull("I/O loop did not latch the JVM Error", terminal);
                Assert.assertEquals("a JVM Error must not be retried", 1, attempts.get());
                Assert.assertFalse("a terminal JVM Error must stop the loop", loop.isRunning());
                try {
                    loop.checkError();
                    Assert.fail("producer-facing checkError() must surface the terminal failure");
                } catch (LineSenderException thrown) {
                    Assert.assertSame("checkError() must rethrow the latched wrapper", terminal, thrown);
                    Assert.assertSame("the wrapper must retain the original JVM Error", injected, thrown.getCause());
                }
            } finally {
                // Also proves that the real I/O-thread shutdown latch is released.
                loop.close();
            }
        }
    }

    @Test
    public void testConnectWithRetryPropagatesJvmError() {
        // The budgeted blocking initial-connect helper must not burn the
        // connect budget retrying a JVM Error; it propagates to the caller
        // (the producer thread in buildAndConnect) on the first attempt.
        AtomicInteger attempts = new AtomicInteger();
        try {
            CursorWebSocketSendLoop.connectWithRetry(
                    () -> {
                        attempts.incrementAndGet();
                        throw new LinkageError("simulated JVM failure");
                    },
                    /* maxDurationMillis */ 60_000L,
                    /* initialBackoffMillis */ 1L,
                    /* maxBackoffMillis */ 4L,
                    "test initial connect");
            Assert.fail("a JVM Error must propagate, not consume the connect budget");
        } catch (LinkageError expected) {
            Assert.assertEquals("simulated JVM failure", expected.getMessage());
        }
        Assert.assertEquals("no retry on a JVM Error", 1, attempts.get());
    }

}
