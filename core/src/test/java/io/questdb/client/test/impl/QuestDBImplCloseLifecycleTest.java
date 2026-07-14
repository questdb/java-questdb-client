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

import io.questdb.client.QueryException;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.impl.QueryClientPool;
import io.questdb.client.impl.QuestDBImpl;
import io.questdb.client.impl.SenderPool;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.IntFunction;

public class QuestDBImplCloseLifecycleTest {

    private static final String QUERY_CFG = "ws::addr=127.0.0.1:9000;";
    private static final String SENDER_CFG = "http::addr=127.0.0.1:1;protocol_version=2;auto_flush=off;";

    @Test(timeout = 30_000)
    public void facadeCloseWaitsForQueryCreationAndTeardown() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountDownLatch inCreation = new CountDownLatch(1);
            CountDownLatch releaseCreation = new CountDownLatch(1);
            CountDownLatch inTeardown = new CountDownLatch(1);
            CountDownLatch releaseTeardown = new CountDownLatch(1);
            AtomicInteger teardownCount = new AtomicInteger();
            Consumer<QwpQueryClient> connectHook = client -> {
                client.setBeforeCloseHookForTest(() -> {
                    inTeardown.countDown();
                    awaitOrFail(releaseTeardown, "test never released query teardown");
                    teardownCount.incrementAndGet();
                });
                inCreation.countDown();
                awaitOrFail(releaseCreation, "test never released query creation");
            };
            QuestDBImpl db = newQuestDB(0, 0, slotIndex -> fakeSender(null, null, null), connectHook);
            AtomicReference<Throwable> borrowOutcome = new AtomicReference<>();
            AtomicBoolean closeReturnedInterrupted = new AtomicBoolean();
            Thread borrower = new Thread(() -> {
                try {
                    db.borrowQuery();
                } catch (Throwable t) {
                    borrowOutcome.set(t);
                }
            }, "facade-query-borrower");
            Thread closer = new Thread(() -> {
                db.close();
                closeReturnedInterrupted.set(Thread.currentThread().isInterrupted());
            }, "facade-query-closer");
            try {
                borrower.start();
                Assert.assertTrue("query borrow never reached construction",
                        inCreation.await(10, TimeUnit.SECONDS));

                closer.start();
                QueryClientPool pool = (QueryClientPool) getField(db, "queryPool");
                awaitBooleanField(pool, "closed");
                awaitCreationWaiter(pool,
                        "facade close did not wait while query construction was internally owned");
                closer.interrupt();
                awaitCreationWaiter(pool,
                        "interrupt allowed facade close to abandon query creation ownership");

                releaseCreation.countDown();
                Assert.assertTrue("query borrow never reached closed-mid-creation teardown",
                        inTeardown.await(10, TimeUnit.SECONDS));
                awaitCreationWaiter(pool,
                        "facade close abandoned query ownership during internal teardown");

                releaseTeardown.countDown();
                borrower.join(TimeUnit.SECONDS.toMillis(10));
                closer.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("query borrower did not finish", borrower.isAlive());
                Assert.assertFalse("facade close did not finish", closer.isAlive());
                Assert.assertEquals("internally owned query client must be torn down exactly once",
                        1, teardownCount.get());
                Assert.assertTrue("facade close must preserve interruption after internal teardown",
                        closeReturnedInterrupted.get());
                Assert.assertTrue("borrowQuery() must report facade closure, got: " + borrowOutcome.get(),
                        borrowOutcome.get() instanceof QueryException
                                && String.valueOf(borrowOutcome.get().getMessage()).contains("closed"));
            } finally {
                releaseCreation.countDown();
                releaseTeardown.countDown();
                db.close();
                borrower.join(TimeUnit.SECONDS.toMillis(10));
                closer.join(TimeUnit.SECONDS.toMillis(10));
            }
        });
    }

    @Test(timeout = 30_000)
    public void facadeCloseWaitsForSenderCreationAndTeardown() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountDownLatch inCreation = new CountDownLatch(1);
            CountDownLatch releaseCreation = new CountDownLatch(1);
            CountDownLatch inTeardown = new CountDownLatch(1);
            CountDownLatch releaseTeardown = new CountDownLatch(1);
            AtomicInteger teardownCount = new AtomicInteger();
            IntFunction<Sender> senderFactory = slotIndex -> {
                inCreation.countDown();
                awaitOrFail(releaseCreation, "test never released sender creation");
                return fakeSender(teardownCount, inTeardown, releaseTeardown);
            };
            QuestDBImpl db = newQuestDB(0, 0, senderFactory, client -> {
            });
            AtomicReference<Throwable> borrowOutcome = new AtomicReference<>();
            Thread borrower = new Thread(() -> {
                try {
                    db.borrowSender();
                } catch (Throwable t) {
                    borrowOutcome.set(t);
                }
            }, "facade-sender-borrower");
            Thread closer = new Thread(db::close, "facade-sender-closer");
            try {
                borrower.start();
                Assert.assertTrue("sender borrow never reached construction",
                        inCreation.await(10, TimeUnit.SECONDS));

                closer.start();
                SenderPool pool = (SenderPool) getField(db, "senderPool");
                awaitBooleanField(pool, "closeStarted");
                awaitCreationWaiter(pool,
                        "facade close did not wait while sender construction was internally owned");

                releaseCreation.countDown();
                Assert.assertTrue("sender borrow never reached closed-mid-creation teardown",
                        inTeardown.await(10, TimeUnit.SECONDS));
                awaitCreationWaiter(pool,
                        "facade close abandoned sender ownership during internal teardown");

                releaseTeardown.countDown();
                borrower.join(TimeUnit.SECONDS.toMillis(10));
                closer.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("sender borrower did not finish", borrower.isAlive());
                Assert.assertFalse("facade close did not finish", closer.isAlive());
                Assert.assertEquals("internally owned sender must be torn down exactly once",
                        1, teardownCount.get());
                Assert.assertTrue("borrowSender() must report facade closure, got: " + borrowOutcome.get(),
                        borrowOutcome.get() instanceof LineSenderException
                                && String.valueOf(borrowOutcome.get().getMessage()).contains("closed"));
            } finally {
                releaseCreation.countDown();
                releaseTeardown.countDown();
                db.close();
                borrower.join(TimeUnit.SECONDS.toMillis(10));
                closer.join(TimeUnit.SECONDS.toMillis(10));
            }
        });
    }

    private static void awaitBooleanField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (field.getBoolean(target)) {
                return;
            }
            Thread.yield();
        }
        Assert.fail("field did not become true: " + fieldName);
    }

    private static void awaitCreationWaiter(Object pool, String message) throws Exception {
        ReentrantLock lock = (ReentrantLock) getField(pool, "lock");
        Condition creationFinished = (Condition) getField(pool, "creationFinished");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            lock.lock();
            try {
                if (lock.hasWaiters(creationFinished)) {
                    return;
                }
            } finally {
                lock.unlock();
            }
            Thread.yield();
        }
        Assert.fail(message);
    }

    private static void awaitOrFail(CountDownLatch latch, String message) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException(message);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(message, e);
        }
    }

    private static Sender fakeSender(
            AtomicInteger teardownCount,
            CountDownLatch inTeardown,
            CountDownLatch releaseTeardown
    ) {
        return (Sender) Proxy.newProxyInstance(
                Sender.class.getClassLoader(),
                new Class[]{Sender.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "close":
                            if (inTeardown != null) {
                                inTeardown.countDown();
                                awaitOrFail(releaseTeardown, "test never released sender teardown");
                            }
                            if (teardownCount != null) {
                                teardownCount.incrementAndGet();
                            }
                            return null;
                        case "toString":
                            return "FacadeCloseFakeSender";
                        case "hashCode":
                            return System.identityHashCode(proxy);
                        case "equals":
                            return proxy == args[0];
                        default:
                            Class<?> returnType = method.getReturnType();
                            if (returnType == boolean.class) return false;
                            if (returnType == byte.class) return (byte) 0;
                            if (returnType == short.class) return (short) 0;
                            if (returnType == int.class) return 0;
                            if (returnType == long.class) return 0L;
                            if (returnType == float.class) return 0f;
                            if (returnType == double.class) return 0d;
                            if (returnType == char.class) return (char) 0;
                            if (returnType == void.class) return null;
                            if (returnType.isInstance(proxy)) return proxy;
                            return null;
                    }
                });
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static QuestDBImpl newQuestDB(
            int senderMin,
            int queryMin,
            IntFunction<Sender> senderFactory,
            Consumer<QwpQueryClient> connectHook
    ) {
        return new QuestDBImpl(
                SENDER_CFG, QUERY_CFG,
                senderMin, 1,
                queryMin, 1,
                10_000L,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                senderFactory, connectHook);
    }
}
