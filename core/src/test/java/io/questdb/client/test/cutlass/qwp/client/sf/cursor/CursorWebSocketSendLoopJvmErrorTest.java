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
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Regression coverage (M3): {@code catch (Throwable)} in the reconnect
 * machinery used to swallow {@link java.lang.Error} (OOM, LinkageError,
 * StackOverflowError) into an indefinite "transport outage" retry with only
 * a throttled, possibly-null-message WARN as a trace. A JVM/programming
 * failure is not a transport outage -- retrying cannot clear it -- so every
 * retry loop must rethrow {@code Error}, after latching it as terminal where
 * a producer could otherwise hang in {@code checkError()}.
 * <p>
 * Uses the same {@code Unsafe.allocateInstance} bare-loop pattern as
 * {@link CursorWebSocketSendLoopErrorLatchTest}: the retry loops only touch
 * the fields wired below, so no live wire client or engine is needed.
 */
public class CursorWebSocketSendLoopJvmErrorTest {

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

    @Test
    public void testConnectLoopPropagatesJvmErrorAndLatchesTerminal() throws Exception {
        // The background per-outage reconnect loop must (1) latch the Error
        // as terminal FIRST -- a producer parked in checkError() would
        // otherwise never observe the failure -- and (2) rethrow so the I/O
        // thread dies loudly instead of reconnect-looping forever.
        CursorWebSocketSendLoop loop = newBareLoop();
        AtomicInteger attempts = new AtomicInteger();
        wireReconnectPlumbing(loop, attempts);

        Method connectLoop = CursorWebSocketSendLoop.class.getDeclaredMethod(
                "connectLoop", Throwable.class, String.class, long.class);
        connectLoop.setAccessible(true);
        try {
            connectLoop.invoke(loop, new LineSenderException("initial wire failure"), "reconnect", 0L);
            Assert.fail("a JVM Error must escape connectLoop, not be retried");
        } catch (InvocationTargetException ite) {
            Assert.assertTrue("expected LinkageError, got " + ite.getCause(),
                    ite.getCause() instanceof LinkageError);
        }
        Assert.assertEquals("no retry on a JVM Error", 1, attempts.get());
        assertErrorLatchedAndStopped(loop);
    }

    @Test
    public void testIoLoopDoesNotFunnelJvmErrorIntoReconnect() throws Exception {
        // ioLoop's catch (Throwable) used to funnel EVERYTHING into
        // fail(t) -> connectLoop(t, "reconnect"). An Error must instead be
        // latched as terminal and rethrown; the finally still counts down
        // the shutdown latch so close() cannot hang on the dead thread.
        CursorWebSocketSendLoop loop = newBareLoop();
        AtomicInteger attempts = new AtomicInteger();
        wireReconnectPlumbing(loop, attempts);
        CountDownLatch shutdownLatch = new CountDownLatch(1);
        setField(loop, "shutdownLatch", shutdownLatch);
        // client == null + running routes ioLoop into attemptInitialConnect
        // -> connectLoop -> the throwing factory, exercising the full funnel.

        Method ioLoop = CursorWebSocketSendLoop.class.getDeclaredMethod("ioLoop");
        ioLoop.setAccessible(true);
        try {
            ioLoop.invoke(loop);
            Assert.fail("a JVM Error must escape ioLoop, not re-enter the reconnect loop");
        } catch (InvocationTargetException ite) {
            Assert.assertTrue("expected LinkageError, got " + ite.getCause(),
                    ite.getCause() instanceof LinkageError);
        }
        Assert.assertEquals("no retry on a JVM Error", 1, attempts.get());
        Assert.assertEquals("shutdown latch must count down so close() cannot hang",
                0L, shutdownLatch.getCount());
        assertErrorLatchedAndStopped(loop);
    }

    private static void assertErrorLatchedAndStopped(CursorWebSocketSendLoop loop)
            throws Exception {
        Throwable terminal = loop.getTerminalError();
        Assert.assertNotNull("Error must be latched as terminal for checkError()", terminal);
        Assert.assertTrue("latch wraps the raw Error once",
                terminal instanceof LineSenderException);
        Assert.assertTrue("latched cause must be the original Error",
                terminal.getCause() instanceof LinkageError);
        Assert.assertFalse("recordFatal must stop the loop",
                (Boolean) getField(loop, "running"));
        try {
            loop.checkError();
            Assert.fail("producer-facing checkError must surface the latched terminal");
        } catch (LineSenderException thrown) {
            Assert.assertSame(terminal, thrown);
        }
    }

    /**
     * Wires the minimal state the reconnect paths dereference: a factory
     * throwing {@link LinkageError} on every attempt, live {@code running},
     * and the attempt counters (field initializers do not run under
     * {@code Unsafe.allocateInstance}).
     */
    private static void wireReconnectPlumbing(CursorWebSocketSendLoop loop,
                                              AtomicInteger attempts) throws Exception {
        CursorWebSocketSendLoop.ReconnectFactory factory = () -> {
            attempts.incrementAndGet();
            throw new LinkageError("simulated JVM failure");
        };
        setField(loop, "reconnectFactory", factory);
        setField(loop, "running", true);
        setField(loop, "totalReconnectAttempts", new AtomicLong());
        setField(loop, "totalReconnects", new AtomicLong());
    }

    private static CursorWebSocketSendLoop newBareLoop() throws Exception {
        // Bypass the real constructor -- no wire client or engine needed.
        return (CursorWebSocketSendLoop) Unsafe.getUnsafe()
                .allocateInstance(CursorWebSocketSendLoop.class);
    }

    private static Object getField(Object target, String name) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(target);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }
}
