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

import io.questdb.client.Completion;
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.impl.QueryClientPool;
import io.questdb.client.impl.QueryWorker;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Unit tests for {@link QueryWorker}.
 * <p>
 * Coverage boundary: the lost-dispatch fix for the single-flight-reuse race
 * (clearing {@code current} under {@code signalLock} at the moment of
 * consumption rather than in a post-{@code runOn()} finally) has no
 * deterministic unit reproduction here. Reproducing the clobber needs the
 * worker to be mid-{@code runOn(client)} when the user thread re-dispatches on
 * the same lease, which requires a live query client to drive
 * {@code client.execute(...)} to its terminal callback. That regression is
 * guarded end-to-end by {@code QuestDBFacadeE2ETest.testSustainedMixedConcurrency}
 * in the parent questdb repo (more threads than pool slots, repeated
 * submit/await per lease). {@link #testShutdownRacingDispatchMustNotStrandCaller()}
 * below covers the adjacent but distinct shutdown-vs-dispatch branch only --
 * reverting the lost-dispatch hunk would not fail it.
 */
public class QueryWorkerTest {

    /**
     * Exercises {@link QueryWorker#client()} -- a pure getter exposed for
     * introspection. The worker is constructed but never started, so no
     * connect is needed; {@code newPlainText} only allocates the client.
     */
    @Test
    public void testClientGetterReturnsConstructorInstance() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000)) {
                QueryWorker worker = new QueryWorker(client, null, 0);
                Assert.assertSame("client() must return the instance passed to the constructor",
                        client, worker.client());
                // Idempotent across calls -- the field is final.
                Assert.assertSame(worker.client(), worker.client());
            }
        });
    }

    /**
     * Regression test for the shutdown-vs-dispatch race in
     * {@code QueryWorker.runLoop()}. If {@code shuttingDown} flips to true
     * after {@code dispatch()} has set {@code current = q} but before the
     * worker thread observes the wakeup, the run loop returns at the
     * {@code if (shuttingDown) return;} branch without ever invoking
     * {@code runOn(client)} or {@code signalUnexpected(...)}. The caller's
     * {@link Completion#await()} would then block forever because
     * {@code signalDone} is never called.
     * <p>
     * Rather than try to win a timing race, this test reproduces the buggy
     * state directly: it parks the worker on its condition, then takes the
     * worker's own {@code signalLock} and atomically sets both
     * {@code current} and {@code shuttingDown} before signalling. After the
     * worker thread exits, the test asserts the {@code QueryImpl} was signalled
     * to done. Without the fix the assertion fails because the run loop's early
     * return strands the {@code QueryImpl} with {@code done==false}, so any
     * caller blocked in {@code Completion.await()} would hang forever.
     */
    @Test(timeout = 30_000)
    public void testShutdownRacingDispatchMustNotStrandCaller() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Class<?> queryImplClass = Class.forName("io.questdb.client.impl.QueryImpl");

            Field lockF = QueryWorker.class.getDeclaredField("signalLock");
            Field condF = QueryWorker.class.getDeclaredField("signalCondition");
            Field currentF = QueryWorker.class.getDeclaredField("current");
            Field shuttingF = QueryWorker.class.getDeclaredField("shuttingDown");
            Field threadF = QueryWorker.class.getDeclaredField("thread");
            for (Field f : new Field[]{lockF, condF, currentF, shuttingF, threadF}) {
                f.setAccessible(true);
            }

            Field doneF = queryImplClass.getDeclaredField("done");
            Field unexpectedF = queryImplClass.getDeclaredField("unexpectedError");
            doneF.setAccessible(true);
            unexpectedF.setAccessible(true);

            // No QwpQueryClient is constructed here: runLoop exits at the
            // shuttingDown check before reaching the first reference to
            // {@code client} or {@code pool}, so passing null for both is fine
            // and keeps the test cleanly isolated from any network or socket state.
            QueryWorker worker = new QueryWorker(null, null, 0);
            Thread t = (Thread) threadF.get(worker);
            t.start();

            ReentrantLock lock = (ReentrantLock) lockF.get(worker);
            Condition cond = (Condition) condF.get(worker);

            // Wait until the worker thread is parked on its signalCondition.
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (true) {
                boolean parked;
                lock.lock();
                try {
                    parked = lock.hasWaiters(cond);
                } finally {
                    lock.unlock();
                }
                if (parked) {
                    break;
                }
                if (System.nanoTime() > deadlineNanos) {
                    Assert.fail("worker thread never parked on its signalCondition");
                }
                Thread.sleep(1);
            }

            // Construct a QueryImpl with done=false, mimicking the state set up
            // by QueryImpl.submit() just before it calls worker.dispatch().
            Constructor<?> ctor = queryImplClass.getDeclaredConstructor(QueryWorker.class);
            ctor.setAccessible(true);
            Object queryImpl = ctor.newInstance(new Object[]{null});
            doneF.setBoolean(queryImpl, false);

            // Atomically force the racy state under the worker's own lock:
            // current set AND shuttingDown set before the worker wakes.
            lock.lock();
            try {
                currentF.set(worker, queryImpl);
                shuttingF.setBoolean(worker, true);
                cond.signalAll();
            } finally {
                lock.unlock();
            }

            // The worker thread must exit (it has observed shuttingDown).
            t.join(5_000);
            Assert.assertFalse("worker thread did not exit after shuttingDown=true",
                    t.isAlive());

            // The QueryImpl must have been signalled to done. Without the fix,
            // done stays false because signalDone is never called, so a caller in
            // Completion.await() would hang forever. The worker reaches the
            // shutdown-race branch and calls signalUnexpected("QuestDB handle is
            // closed"), which sets done=true and records the unexpected error.
            Assert.assertTrue("BUG: QueryWorker.runLoop returned with shuttingDown=true "
                    + "while current!=null, never invoking runOn or signalUnexpected. "
                    + "The caller's Completion.await() hangs forever.", doneF.getBoolean(queryImpl));
            Assert.assertNotNull("signalUnexpected must record the closed-handle error",
                    unexpectedF.get(queryImpl));
        });
    }

    /**
     * Busy-worker variant of the shutdown-drop race fixed in df6f7ca
     * ({@code while (!shuttingDown)} -> {@code while (true)} in
     * {@link QueryWorker}'s run loop). Unlike
     * {@link #testShutdownRacingDispatchMustNotStrandCaller()} -- which only
     * drives the PARKED-worker branch (worker blocked in
     * {@code awaitUninterruptibly} before {@code shuttingDown} flips) and stays
     * green even with the fix reverted -- this test forces the worker THROUGH a
     * job's {@code runOn()} and then, on the worker thread at the exact instant
     * that job returns, reproduces a reused lease re-dispatching
     * ({@code current = q2}) racing a shutdown ({@code shuttingDown = true}),
     * both set before the loop re-enters the strand check.
     * <p>
     * With the fix the loop re-enters the {@code signalLock} block, observes
     * {@code shuttingDown}, and strands q2 (signalling its caller). With the bug
     * the loop exits at the top without re-reading {@code current}, so q2 is
     * dropped -- never run, never signalled -- and its caller's
     * {@code Completion.await()} would hang forever. The assertion on
     * {@code q2.done} fails if the fix is reverted.
     * <p>
     * The interleaving is made deterministic with a test-only worker-thread
     * barrier ({@code QueryWorker.busyWorkerTestHook}) instead of a sleep:
     * {@link QueryWorker} and {@code QueryImpl} are final and
     * {@code QwpQueryClient} has no test seam, so pausing between
     * {@code runOn()} and the loop check is the only race-free reproduction.
     * {@code client}/{@code pool} are null -- {@code q1.runOn(null)} throws an
     * NPE that {@code runLoop} catches and turns into q1's terminal signal, a
     * fast stand-in for a real job returning from {@code runOn()}.
     */
    @Test(timeout = 30_000)
    public void testBusyWorkerShutdownStrandsReDispatchedCurrent() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Class<?> queryImplClass = Class.forName("io.questdb.client.impl.QueryImpl");

            Field lockF = QueryWorker.class.getDeclaredField("signalLock");
            Field currentF = QueryWorker.class.getDeclaredField("current");
            Field shuttingF = QueryWorker.class.getDeclaredField("shuttingDown");
            Field threadF = QueryWorker.class.getDeclaredField("thread");
            Field hookF = QueryWorker.class.getDeclaredField("busyWorkerTestHook");
            for (Field f : new Field[]{lockF, currentF, shuttingF, threadF, hookF}) {
                f.setAccessible(true);
            }

            Field doneF = queryImplClass.getDeclaredField("done");
            Field unexpectedF = queryImplClass.getDeclaredField("unexpectedError");
            doneF.setAccessible(true);
            unexpectedF.setAccessible(true);

            // client == null: q1.runOn(null) throws NPE, which runLoop catches and
            // turns into q1's terminal signal -- a fast, deterministic stand-in for
            // a real job returning from runOn(). pool == null is never touched here.
            QueryWorker worker = new QueryWorker(null, null, 0);

            Constructor<?> ctor = queryImplClass.getDeclaredConstructor(QueryWorker.class);
            ctor.setAccessible(true);
            Object q1 = ctor.newInstance(new Object[]{worker});
            Object q2 = ctor.newInstance(new Object[]{worker});
            doneF.setBoolean(q1, false);
            doneF.setBoolean(q2, false);

            ReentrantLock lock = (ReentrantLock) lockF.get(worker);
            AtomicBoolean fired = new AtomicBoolean(false);

            // The busy-worker barrier: the FIRST time the worker returns from a
            // job's runOn(), simulate submit() -> dispatch() re-arming current with
            // q2 while shutdown() flips shuttingDown -- both set, under signalLock,
            // before the loop re-checks. Runs on the worker thread.
            Runnable hook = () -> {
                if (fired.compareAndSet(false, true)) {
                    lock.lock();
                    try {
                        currentF.set(worker, q2);
                        shuttingF.setBoolean(worker, true);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    } finally {
                        lock.unlock();
                    }
                }
            };
            hookF.set(worker, hook);

            // Pre-arm current with q1 so the worker consumes it immediately on
            // start (no need to wait for the await park); start() establishes the
            // happens-before that publishes current and the hook to the worker.
            currentF.set(worker, q1);

            Thread t = (Thread) threadF.get(worker);
            t.start();

            t.join(5_000);
            Assert.assertFalse("worker thread must exit after shuttingDown=true", t.isAlive());

            Assert.assertTrue(
                    "BUG (df6f7ca regressed): the busy worker returned from runOn() with a "
                            + "re-dispatched current!=null and shuttingDown=true, then exited the loop "
                            + "without stranding it. q2 was never signalled; its caller's await() hangs "
                            + "forever.",
                    doneF.getBoolean(q2));
            Assert.assertNotNull("the stranded busy-path job must record the closed-handle error",
                    unexpectedF.get(q2));
        });
    }

    /**
     * Result handlers (onBatch/onEnd/onError) run inline on the worker's
     * dispatch thread. The blocking lease ops -- {@code close()} and the two
     * {@code await()} variants -- would there wait on a terminal event that
     * only this same thread can deliver, a permanent self-deadlock. The
     * reentrancy guard must turn that into an immediate IllegalStateException.
     * <p>
     * The guard compares {@code Thread.currentThread()} to the worker's
     * dispatch thread, so this test points that field at the test thread (the
     * worker is never started) to stand in for a reentrant in-handler call.
     * Without the guard, {@code close()}/{@code await()} would park forever and
     * the method-level timeout would fail the test.
     */
    @Test(timeout = 30_000)
    public void testCloseAndAwaitFromWorkerThreadThrowInsteadOfDeadlocking() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            Class<?> queryImplClass = Class.forName("io.questdb.client.impl.QueryImpl");
            Field queryF = QueryWorker.class.getDeclaredField("query");
            queryF.setAccessible(true);
            Field threadF = QueryWorker.class.getDeclaredField("thread");
            threadF.setAccessible(true);
            Field doneF = queryImplClass.getDeclaredField("done");
            doneF.setAccessible(true);
            Method bump = QueryWorker.class.getDeclaredMethod("bumpGeneration");
            bump.setAccessible(true);
            Method isWorker = QueryWorker.class.getDeclaredMethod("isCurrentThreadWorker");
            isWorker.setAccessible(true);
            Method close = queryImplClass.getDeclaredMethod("close", long.class);
            close.setAccessible(true);
            Method awaitNoTimeout = queryImplClass.getDeclaredMethod("await", long.class);
            awaitNoTimeout.setAccessible(true);
            Method awaitTimed = queryImplClass.getDeclaredMethod("await", long.class, long.class, TimeUnit.class);
            awaitTimed.setAccessible(true);

            QueryClientPool pool = new QueryClientPool(
                    "ws::addr=localhost:9000;",
                    /*minSize*/ 0, /*maxSize*/ 2,
                    /*acquireTimeoutMillis*/ 1_000L,
                    /*idleTimeoutMillis*/ Long.MAX_VALUE,
                    /*maxLifetimeMillis*/ Long.MAX_VALUE);
            QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000);
            try {
                QueryWorker w = new QueryWorker(client, pool, 0);
                bump.invoke(w); // generation -> 1: a live lease
                Object impl = queryF.get(w);
                doneF.setBoolean(impl, false); // a submit is in flight, as during a handler

                // Off the worker thread the guard must NOT fire.
                Assert.assertFalse("guard must not fire on a normal caller thread",
                        (Boolean) isWorker.invoke(w));

                // Stand in for a reentrant call from inside a result handler: the
                // guard compares Thread.currentThread() to the worker's dispatch
                // thread, so point that field at this thread.
                threadF.set(w, Thread.currentThread());
                Assert.assertTrue((Boolean) isWorker.invoke(w));

                assertThrowsHandlerReentry("close", () -> close.invoke(impl, 1L));
                assertThrowsHandlerReentry("await", () -> awaitNoTimeout.invoke(impl, 1L));
                assertThrowsHandlerReentry("await(timeout)",
                        () -> awaitTimed.invoke(impl, 1L, 5L, TimeUnit.SECONDS));
            } finally {
                client.close();
                pool.close();
            }
        });
    }

    private static void assertThrowsHandlerReentry(String op, ReflectiveCall call) throws Exception {
        try {
            call.run();
            Assert.fail(op + "() from the worker thread must throw, not block/deadlock");
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            Assert.assertTrue(op + "(): expected IllegalStateException, was " + cause,
                    cause instanceof IllegalStateException);
            Assert.assertTrue(op + "(): message must point at cancel(), was: " + cause.getMessage(),
                    cause.getMessage().contains("cancel()"));
        }
    }

    @FunctionalInterface
    private interface ReflectiveCall {
        void run() throws Exception;
    }
}
