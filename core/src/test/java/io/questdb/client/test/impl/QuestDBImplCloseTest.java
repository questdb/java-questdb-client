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

package io.questdb.client.test.impl;

import io.questdb.client.Sender;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.impl.QuestDBImpl;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntFunction;

/**
 * P2 regression: {@link QuestDBImpl#close()} must not return before shutdown
 * has completed, even when two threads call it concurrently. {@code closed} is
 * volatile and set BEFORE the pool teardown chain runs, so a naive guard lets a
 * second concurrent caller observe {@code closed == true} and RETURN while the
 * first caller is still inside {@code closeQuietly(senderPool)} tearing down the
 * flock/mmap/I/O-thread resources. After ANY {@code close()} returns -- the
 * losing concurrent caller included -- callers must be able to assume shutdown
 * has completed.
 * <p>
 * The window is opened deterministically by injecting (via the {@code @TestOnly}
 * senderFactory seam) a fake delegate whose {@code close()} parks on a latch.
 * The last teardown step, {@code senderPool.close()}, closes the prewarmed idle
 * delegate on the closing thread OUTSIDE the pool lock, so thread A parks there
 * with {@code closed} already raised. Thread B then calls {@code close()}:
 * <ul>
 * <li>pre-fix -- B reads {@code closed == true} and returns immediately while A
 * is still tearing down (premature return);</li>
 * <li>fixed -- B blocks until A finishes the teardown, then returns.</li>
 * </ul>
 * Latch-coordinated (no {@code Thread.sleep} for correctness) with a JUnit
 * timeout on the two-thread interleaving.
 */
public class QuestDBImplCloseTest {

    // Non-SF http config: the injected senderFactory replaces the native build,
    // but the constructor's eager config probe must still parse it.
    private static final String QUERY_CFG = "ws::addr=127.0.0.1:9000;";
    private static final String SENDER_CFG = "http::addr=127.0.0.1:1;protocol_version=2;auto_flush=off;";

    // RED (closed set before teardown, no serialization): while thread A is
    // parked inside the delegate teardown of the final senderPool.close() step,
    // thread B's close() sees closed==true and RETURNS -- closeReturnedEarly is
    // true and the first assertion fails. GREEN (close() serialized through
    // completion): B blocks on A until the teardown finishes, so its close()
    // does not return until delegateCloses == 1.
    @Test(timeout = 30_000)
    public void concurrentCloseSecondCallerBlocksUntilShutdownCompletes() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicInteger delegateCloses = new AtomicInteger();
            CountDownLatch inDelegateClose = new CountDownLatch(1);
            CountDownLatch releaseDelegateClose = new CountDownLatch(1);
            IntFunction<Sender> senderFactory = slotIndex ->
                    parkingCloseSender(delegateCloses, inDelegateClose, releaseDelegateClose);
            // queryMin = 0 -> QueryClientPool prewarms nothing, so the connect
            // hook is never reached and its teardown is a no-op; the parking
            // delegate is the only blocking teardown step.
            Consumer<QwpQueryClient> connectHook = client -> {
            };

            QuestDBImpl questDB = newQuestDB(senderFactory, connectHook);

            // Thread A: enter close() and park inside the final teardown step
            // (senderPool.close() -> idle delegate close()), with closed
            // already raised.
            Thread closerA = new Thread(questDB::close, "questdb-closer-A");
            closerA.start();
            Assert.assertTrue("closer A never reached the delegate teardown",
                    inDelegateClose.await(10, TimeUnit.SECONDS));

            // Thread B: a concurrent close(). It must NOT return while A is
            // still tearing down.
            Thread closerB = new Thread(questDB::close, "questdb-closer-B");
            closerB.start();
            closerB.join(300);
            boolean closeReturnedEarly = !closerB.isAlive();
            int closesWhenBReturned = delegateCloses.get();

            // Always unpark the teardown so the test fails on the assertion, not
            // its own timeout.
            releaseDelegateClose.countDown();

            Assert.assertFalse(
                    "concurrent close() returned while the first caller was still tearing down "
                            + "(delegateCloses=" + closesWhenBReturned + " when B returned): "
                            + "close() must serialize through shutdown completion",
                    closeReturnedEarly);

            // Once the teardown completes, B must return promptly and the
            // delegate must have been torn down exactly once.
            closerB.join(TimeUnit.SECONDS.toMillis(10));
            Assert.assertFalse("concurrent close() did not return after the teardown completed",
                    closerB.isAlive());
            closerA.join(TimeUnit.SECONDS.toMillis(10));
            Assert.assertFalse("first close() did not return after the teardown completed",
                    closerA.isAlive());
            Assert.assertEquals("the prewarmed delegate must be torn down exactly once",
                    1, delegateCloses.get());
        });
    }

    private static QuestDBImpl newQuestDB(
            IntFunction<Sender> senderFactory, Consumer<QwpQueryClient> connectHook
    ) {
        return new QuestDBImpl(
                SENDER_CFG, QUERY_CFG,
                /*senderMin*/ 1, /*senderMax*/ 1,
                /*queryMin*/ 0, /*queryMax*/ 1,
                /*acquireTimeoutMillis*/ 250L,
                /*idleTimeoutMillis*/ Long.MAX_VALUE,
                /*maxLifetimeMillis*/ Long.MAX_VALUE,
                /*housekeeperIntervalMillis*/ Long.MAX_VALUE,
                senderFactory, connectHook);
    }

    /**
     * Proxy-backed fake Sender whose {@code close()} signals {@code inClose},
     * parks on {@code releaseClose}, then bumps {@code closes} -- a delegate
     * teardown frozen mid-close so the test can probe what a concurrent
     * close() does while it runs.
     */
    private static Sender parkingCloseSender(
            AtomicInteger closes,
            CountDownLatch inClose,
            CountDownLatch releaseClose
    ) {
        return (Sender) Proxy.newProxyInstance(
                Sender.class.getClassLoader(),
                new Class[]{Sender.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "close":
                            inClose.countDown();
                            if (!releaseClose.await(10, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("test never released the parked close");
                            }
                            closes.incrementAndGet();
                            return null;
                        case "toString":
                            return "ParkingCloseFakeSender";
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            Class<?> rt = method.getReturnType();
                            if (rt == boolean.class) return false;
                            if (rt == byte.class) return (byte) 0;
                            if (rt == short.class) return (short) 0;
                            if (rt == int.class) return 0;
                            if (rt == long.class) return 0L;
                            if (rt == float.class) return 0f;
                            if (rt == double.class) return 0d;
                            if (rt == char.class) return (char) 0;
                            if (rt == void.class) return null;
                            if (rt.isInstance(proxy)) return proxy;
                            return null;
                    }
                });
    }
}
