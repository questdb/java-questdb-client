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
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.IntFunction;

public class QuestDBImplCloseLifecycleTest {

    private static final String QUERY_CFG = "ws::addr=127.0.0.1:9000;";
    private static final String SENDER_CFG = "http::addr=127.0.0.1:1;protocol_version=2;auto_flush=off;";

    @Test(timeout = 30_000)
    public void facadeCloseIsBoundedByZeroAcquireTimeoutDuringQueryCreation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountDownLatch inCreation = new CountDownLatch(1);
            CountDownLatch releaseCreation = new CountDownLatch(1);
            AtomicInteger teardownCount = new AtomicInteger();
            Consumer<QwpQueryClient> connectHook = client -> {
                client.setBeforeCloseHookForTest(teardownCount::incrementAndGet);
                inCreation.countDown();
                awaitOrFail(releaseCreation, "test never released query creation");
            };
            QuestDBImpl db = newQuestDB(
                    SENDER_CFG, 0, 0, 0, slotIndex -> fakeSender(null, null, null), connectHook);
            QueryClientPool pool = db.getQueryPoolForTesting();
            AtomicReference<Throwable> borrowOutcome = new AtomicReference<>();
            Thread borrower = new Thread(() -> {
                try {
                    db.borrowQuery();
                } catch (Throwable t) {
                    borrowOutcome.set(t);
                }
            }, "bounded-query-borrower");
            Thread closer = new Thread(db::close, "bounded-query-closer");
            long nativeBaseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
            try {
                borrower.start();
                Assert.assertTrue("query borrow never reached construction",
                        inCreation.await(10, TimeUnit.SECONDS));
                Assert.assertEquals(1, pool.inFlightCreations());

                closer.start();
                closer.join(TimeUnit.SECONDS.toMillis(5));
                Assert.assertFalse(
                        "facade close exceeded its zero creation-wait budget",
                        closer.isAlive());
                Assert.assertEquals(
                        "close must retain late-completion cleanup ownership",
                        1, pool.inFlightCreations());
                Assert.assertEquals("the still-constructing query client must remain live", 0, teardownCount.get());

                releaseCreation.countDown();
                borrower.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("query borrower did not finish", borrower.isAlive());
                Assert.assertEquals("late query creation must be torn down exactly once", 1, teardownCount.get());
                Assert.assertEquals("late query creation reservation must be released", 0, pool.inFlightCreations());
                Assert.assertEquals("late query cleanup must release native scratch",
                        nativeBaseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT));
                Assert.assertTrue("borrowQuery() must report facade closure, got: " + borrowOutcome.get(),
                        borrowOutcome.get() instanceof QueryException
                                && String.valueOf(borrowOutcome.get().getMessage()).contains("closed"));
            } finally {
                releaseCreation.countDown();
                db.close();
                borrower.join(TimeUnit.SECONDS.toMillis(10));
                closer.join(TimeUnit.SECONDS.toMillis(10));
            }
        });
    }

    @Test(timeout = 30_000)
    public void facadeCloseIsBoundedByZeroAcquireTimeoutDuringSenderCreation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountDownLatch inCreation = new CountDownLatch(1);
            CountDownLatch releaseCreation = new CountDownLatch(1);
            AtomicInteger teardownCount = new AtomicInteger();
            IntFunction<Sender> senderFactory = slotIndex -> {
                inCreation.countDown();
                awaitOrFail(releaseCreation, "test never released sender creation");
                return fakeSender(teardownCount, null, null);
            };
            String senderConfig = "ws::addr=localhost:1;sf_dir="
                    + System.getProperty("java.io.tmpdir") + "/qdb-bounded-pool-" + System.nanoTime() + ";";
            QuestDBImpl db = newQuestDB(senderConfig, 0, 0, 0, senderFactory, client -> {
            });
            SenderPool pool = db.getSenderPoolForTesting();
            AtomicReference<Throwable> borrowOutcome = new AtomicReference<>();
            Thread borrower = new Thread(() -> {
                try {
                    db.borrowSender();
                } catch (Throwable t) {
                    borrowOutcome.set(t);
                }
            }, "bounded-sender-borrower");
            Thread closer = new Thread(db::close, "bounded-sender-closer");
            try {
                borrower.start();
                Assert.assertTrue("sender borrow never reached construction",
                        inCreation.await(10, TimeUnit.SECONDS));
                Assert.assertEquals(1, pool.getInFlightCreationsForTesting());
                Assert.assertTrue("SF slot must stay reserved during creation",
                        pool.isSlotInUseForTesting(0));

                closer.start();
                closer.join(TimeUnit.SECONDS.toMillis(5));
                Assert.assertFalse(
                        "facade close exceeded its zero creation-wait budget",
                        closer.isAlive());
                Assert.assertEquals(
                        "close must retain late-completion cleanup ownership",
                        1, pool.getInFlightCreationsForTesting());
                Assert.assertTrue("close must not abandon the reserved SF slot",
                        pool.isSlotInUseForTesting(0));

                releaseCreation.countDown();
                borrower.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("sender borrower did not finish", borrower.isAlive());
                Assert.assertEquals("late sender creation must be torn down exactly once", 1, teardownCount.get());
                Assert.assertEquals("late sender creation reservation must be released",
                        0, pool.getInFlightCreationsForTesting());
                Assert.assertFalse("late sender cleanup must release the SF slot",
                        pool.isSlotInUseForTesting(0));
                Assert.assertTrue("borrowSender() must report facade closure, got: " + borrowOutcome.get(),
                        borrowOutcome.get() instanceof LineSenderException
                                && String.valueOf(borrowOutcome.get().getMessage()).contains("closed"));
            } finally {
                releaseCreation.countDown();
                db.close();
                borrower.join(TimeUnit.SECONDS.toMillis(10));
                closer.join(TimeUnit.SECONDS.toMillis(10));
            }
        });
    }

    @Test(timeout = 30_000)
    public void facadeCloseIsBoundedUnderRepeatedInterruptsDuringQueryCreation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountDownLatch inCreation = new CountDownLatch(1);
            CountDownLatch releaseCreation = new CountDownLatch(1);
            AtomicInteger teardownCount = new AtomicInteger();
            Consumer<QwpQueryClient> connectHook = client -> {
                client.setBeforeCloseHookForTest(teardownCount::incrementAndGet);
                inCreation.countDown();
                awaitOrFail(releaseCreation, "test never released query creation");
            };
            // A 1s creation-wait budget, not 100ms: the interrupt storm below must land at least
            // twice inside this window for the deadline-restart property to be exercised at all,
            // and a freshly started, yielding interrupter thread is not guaranteed two scheduler
            // quanta within 100ms on a saturated CI agent (observed on hosted 3-core mac agents,
            // where the post-join count assert failed with the product deadline honored exactly).
            QuestDBImpl db = newQuestDB(
                    SENDER_CFG, 0, 0, 1000, slotIndex -> fakeSender(null, null, null), connectHook);
            QueryClientPool pool = db.getQueryPoolForTesting();
            AtomicReference<Throwable> borrowOutcome = new AtomicReference<>();
            AtomicBoolean closeReturnedInterrupted = new AtomicBoolean();
            AtomicBoolean keepInterrupting = new AtomicBoolean(true);
            AtomicInteger interruptCount = new AtomicInteger();
            Thread borrower = new Thread(() -> {
                try {
                    db.borrowQuery();
                } catch (Throwable t) {
                    borrowOutcome.set(t);
                }
            }, "interrupted-query-borrower");
            Thread closer = new Thread(() -> {
                db.close();
                closeReturnedInterrupted.set(Thread.currentThread().isInterrupted());
            }, "interrupted-query-closer");
            Thread interrupter = new Thread(() -> {
                while (keepInterrupting.get()) {
                    interruptCount.incrementAndGet();
                    closer.interrupt();
                    Thread.yield();
                }
            }, "query-close-interrupter");
            long nativeBaseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
            try {
                borrower.start();
                Assert.assertTrue("query borrow never reached construction",
                        inCreation.await(10, TimeUnit.SECONDS));
                Assert.assertEquals(1, pool.inFlightCreations());

                closer.start();
                awaitCreationWaiter(pool,
                        "facade close did not wait while query construction was internally owned");
                interrupter.start();
                awaitRepeatedInterrupts(interruptCount, pool::hasCreationWaiterForTesting,
                        "query close left its creation wait before the interrupt storm landed twice");
                closer.join(TimeUnit.SECONDS.toMillis(5));
                Assert.assertFalse(
                        "repeated interrupts restarted the query creation-wait deadline",
                        closer.isAlive());
                Assert.assertTrue("test did not repeatedly interrupt query close", interruptCount.get() > 1);
                Assert.assertTrue("facade close must restore query closer interruption",
                        closeReturnedInterrupted.get());
                Assert.assertEquals(
                        "close must retain late-completion cleanup ownership",
                        1, pool.inFlightCreations());
                Assert.assertEquals("the still-constructing query client must remain live", 0, teardownCount.get());

                releaseCreation.countDown();
                borrower.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("query borrower did not finish", borrower.isAlive());
                Assert.assertEquals("late query creation must be torn down exactly once", 1, teardownCount.get());
                Assert.assertEquals("late query creation reservation must be released", 0, pool.inFlightCreations());
                Assert.assertEquals("late query cleanup must release native scratch",
                        nativeBaseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT));
                Assert.assertTrue("borrowQuery() must report facade closure, got: " + borrowOutcome.get(),
                        borrowOutcome.get() instanceof QueryException
                                && String.valueOf(borrowOutcome.get().getMessage()).contains("closed"));
            } finally {
                keepInterrupting.set(false);
                releaseCreation.countDown();
                interrupter.join(TimeUnit.SECONDS.toMillis(10));
                db.close();
                borrower.join(TimeUnit.SECONDS.toMillis(10));
                closer.join(TimeUnit.SECONDS.toMillis(10));
            }
        });
    }

    @Test(timeout = 30_000)
    public void facadeCloseIsBoundedUnderRepeatedInterruptsDuringSenderCreation() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            CountDownLatch inCreation = new CountDownLatch(1);
            CountDownLatch releaseCreation = new CountDownLatch(1);
            AtomicInteger teardownCount = new AtomicInteger();
            IntFunction<Sender> senderFactory = slotIndex -> {
                inCreation.countDown();
                awaitOrFail(releaseCreation, "test never released sender creation");
                return fakeSender(teardownCount, null, null);
            };
            String senderConfig = "ws::addr=localhost:1;sf_dir="
                    + System.getProperty("java.io.tmpdir") + "/qdb-interrupted-pool-" + System.nanoTime() + ";";
            // 1s creation-wait budget for the same reason as the query-interrupt test above: the
            // interrupt storm must land at least twice inside the window even on a saturated agent.
            QuestDBImpl db = newQuestDB(senderConfig, 0, 0, 1000, senderFactory, client -> {
            });
            SenderPool pool = db.getSenderPoolForTesting();
            AtomicReference<Throwable> borrowOutcome = new AtomicReference<>();
            AtomicBoolean closeReturnedInterrupted = new AtomicBoolean();
            AtomicBoolean keepInterrupting = new AtomicBoolean(true);
            AtomicInteger interruptCount = new AtomicInteger();
            Thread borrower = new Thread(() -> {
                try {
                    db.borrowSender();
                } catch (Throwable t) {
                    borrowOutcome.set(t);
                }
            }, "interrupted-sender-borrower");
            Thread closer = new Thread(() -> {
                db.close();
                closeReturnedInterrupted.set(Thread.currentThread().isInterrupted());
            }, "interrupted-sender-closer");
            Thread interrupter = new Thread(() -> {
                while (keepInterrupting.get()) {
                    interruptCount.incrementAndGet();
                    closer.interrupt();
                    Thread.yield();
                }
            }, "sender-close-interrupter");
            try {
                borrower.start();
                Assert.assertTrue("sender borrow never reached construction",
                        inCreation.await(10, TimeUnit.SECONDS));
                Assert.assertEquals(1, pool.getInFlightCreationsForTesting());
                Assert.assertTrue("SF slot must stay reserved during creation",
                        pool.isSlotInUseForTesting(0));

                closer.start();
                awaitCreationWaiter(pool,
                        "facade close did not wait while sender construction was internally owned");
                interrupter.start();
                awaitRepeatedInterrupts(interruptCount, pool::hasCreationWaiterForTesting,
                        "sender close left its creation wait before the interrupt storm landed twice");
                closer.join(TimeUnit.SECONDS.toMillis(5));
                Assert.assertFalse(
                        "repeated interrupts restarted the sender creation-wait deadline",
                        closer.isAlive());
                Assert.assertTrue("test did not repeatedly interrupt sender close", interruptCount.get() > 1);
                Assert.assertTrue("facade close must restore sender closer interruption",
                        closeReturnedInterrupted.get());
                Assert.assertEquals(
                        "close must retain late-completion cleanup ownership",
                        1, pool.getInFlightCreationsForTesting());
                Assert.assertTrue("close must not abandon the reserved SF slot",
                        pool.isSlotInUseForTesting(0));

                releaseCreation.countDown();
                borrower.join(TimeUnit.SECONDS.toMillis(10));
                Assert.assertFalse("sender borrower did not finish", borrower.isAlive());
                Assert.assertEquals("late sender creation must be torn down exactly once", 1, teardownCount.get());
                Assert.assertEquals("late sender creation reservation must be released",
                        0, pool.getInFlightCreationsForTesting());
                Assert.assertFalse("late sender cleanup must release the SF slot",
                        pool.isSlotInUseForTesting(0));
                Assert.assertTrue("borrowSender() must report facade closure, got: " + borrowOutcome.get(),
                        borrowOutcome.get() instanceof LineSenderException
                                && String.valueOf(borrowOutcome.get().getMessage()).contains("closed"));
            } finally {
                keepInterrupting.set(false);
                releaseCreation.countDown();
                interrupter.join(TimeUnit.SECONDS.toMillis(10));
                db.close();
                borrower.join(TimeUnit.SECONDS.toMillis(10));
                closer.join(TimeUnit.SECONDS.toMillis(10));
            }
        });
    }

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
                QueryClientPool pool = db.getQueryPoolForTesting();
                awaitClosed(pool);
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
                SenderPool pool = db.getSenderPoolForTesting();
                awaitCloseStarted(pool);
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

    private static void awaitCloseStarted(SenderPool pool) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (pool.isCloseStartedForTesting()) {
                return;
            }
            Thread.yield();
        }
        Assert.fail("sender pool close did not start");
    }

    private static void awaitClosed(QueryClientPool pool) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (pool.isClosedForTesting()) {
                return;
            }
            Thread.yield();
        }
        Assert.fail("query pool did not close");
    }

    private static void awaitCreationWaiter(QueryClientPool pool, String message) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (pool.hasCreationWaiterForTesting()) {
                return;
            }
            Thread.yield();
        }
        Assert.fail(message);
    }

    private static void awaitCreationWaiter(SenderPool pool, String message) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (pool.hasCreationWaiterForTesting()) {
                return;
            }
            Thread.yield();
        }
        Assert.fail(message);
    }

    /**
     * Holds the test until the interrupt storm has landed at least twice while the facade close is
     * still inside its bounded creation wait. The deadline-restart property is only exercised by
     * interrupts that arrive during that wait, and the scheduler owes the interrupter thread
     * nothing: with a post-join count assert alone, the run races the close budget against thread
     * scheduling and can fail with the product invariant intact. Failing here instead separates
     * "interrupter starved before the budget expired" from a genuine deadline bug.
     */
    private static void awaitRepeatedInterrupts(
            AtomicInteger interruptCount,
            BooleanSupplier closerStillWaiting,
            String message
    ) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            // Count first: two interrupts observed while polling means the storm landed no matter
            // how quickly the wait ends afterwards, so a budget expiry seen next is not a failure.
            if (interruptCount.get() > 1) {
                return;
            }
            if (!closerStillWaiting.getAsBoolean()) {
                Assert.fail(message + "; interrupts landed: " + interruptCount.get());
            }
            Thread.yield();
        }
        Assert.fail(message + "; interrupts landed: " + interruptCount.get());
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

    private static QuestDBImpl newQuestDB(
            int senderMin,
            int queryMin,
            IntFunction<Sender> senderFactory,
            Consumer<QwpQueryClient> connectHook
    ) {
        return newQuestDB(SENDER_CFG, senderMin, queryMin, 10_000L, senderFactory, connectHook);
    }

    private static QuestDBImpl newQuestDB(
            String senderConfig,
            int senderMin,
            int queryMin,
            long acquireTimeoutMillis,
            IntFunction<Sender> senderFactory,
            Consumer<QwpQueryClient> connectHook
    ) {
        return new QuestDBImpl(
                senderConfig, QUERY_CFG,
                senderMin, 1,
                queryMin, 1,
                acquireTimeoutMillis,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                senderFactory, connectHook);
    }
}
