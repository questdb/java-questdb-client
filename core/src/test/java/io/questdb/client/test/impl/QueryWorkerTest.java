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
import io.questdb.client.impl.QueryWorker;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class QueryWorkerTest {

    /**
     * Exercises {@link QueryWorker#client()} -- a pure getter exposed for
     * introspection. The worker is constructed but never started, so no
     * connect is needed; {@code newPlainText} only allocates the client.
     */
    @Test
    public void testClientGetterReturnsConstructorInstance() {
        try (QwpQueryClient client = QwpQueryClient.newPlainText("localhost", 9000)) {
            QueryWorker worker = new QueryWorker(client, null, 0);
            Assert.assertSame("client() must return the instance passed to the constructor",
                    client, worker.client());
            // Idempotent across calls -- the field is final.
            Assert.assertSame(worker.client(), worker.client());
        }
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
     * worker thread exits, the test asserts the {@link Completion} has been
     * signalled. Today the assertion fails because the run loop's early
     * return strands the {@code QueryImpl}.
     */
    @Test(timeout = 30_000)
    public void testShutdownRacingDispatchMustNotStrandCaller() throws Exception {
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
        Field completionF = queryImplClass.getDeclaredField("completion");
        doneF.setAccessible(true);
        completionF.setAccessible(true);

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
        Completion completion = (Completion) completionF.get(queryImpl);

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

        // The Completion must have been signalled. Without the fix, await(2s)
        // returns false because signalDone is never called.
        boolean completed;
        try {
            completed = completion.await(2, TimeUnit.SECONDS);
        } catch (RuntimeException expectedAfterFix) {
            // Once fixed, the worker is expected to call signalUnexpected
            // with a QueryException("QuestDB handle is closed") which
            // await() rethrows. Either form of "completed" is acceptable;
            // the bug is the silent hang.
            completed = true;
        }
        Assert.assertTrue("BUG: QueryWorker.runLoop returned with shuttingDown=true "
                + "while current!=null, never invoking runOn or signalUnexpected. "
                + "The caller's Completion.await() hangs forever.", completed);
    }
}
