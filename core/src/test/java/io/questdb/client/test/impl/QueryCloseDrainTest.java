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

import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.impl.QueryClientPool;
import io.questdb.client.impl.QueryWorker;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.function.Consumer;

/**
 * Regression tests for the bounded, interruptible {@code Query.close()} drain.
 * When a submit is still in flight at close() time, the old drain blocked the
 * caller unbounded and uninterruptibly on the terminal event (and could hang
 * forever if a racing {@code QuestDB.close()} stranded it). The drain now waits
 * at most {@code closeQueryTimeoutMillis}, an interrupt aborts it, and a worker
 * that fails to drain in time is discarded -- its connection may still carry
 * late frames for the abandoned query -- rather than returned to the pool.
 * <p>
 * White-box style: a no-op connect hook builds workers without a network, and
 * the in-flight state is simulated by setting {@code QueryImpl.done=false}
 * reflectively, so no server or real {@code execute()} is needed to exercise
 * the close() drain logic.
 */
public class QueryCloseDrainTest {

    private static final String CFG = "ws::addr=127.0.0.1:1;";
    private static final Consumer<QwpQueryClient> NO_CONNECT = c -> {
    };

    @Test(timeout = 30_000)
    public void testCloseDiscardsWorkerWhenDrainTimesOut() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QueryClientPool pool = new QueryClientPool(
                    CFG, 0, 2, 1_000L, Long.MAX_VALUE, Long.MAX_VALUE, NO_CONNECT)) {
                setCloseQueryTimeout(pool, 150L);
                QueryWorker w = pool.acquire();
                long gen = generation(w);
                setDone(w, false); // pretend a submit is in flight; nothing will ever signal done

                long startNanos = System.nanoTime();
                closeQuery(w, gen);
                long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

                Assert.assertTrue("close() must wait about the close budget, elapsed=" + elapsedMs,
                        elapsedMs >= 120);
                Assert.assertTrue("close() must be bounded, not block unbounded, elapsed=" + elapsedMs,
                        elapsedMs < 5_000);
                Assert.assertFalse("a worker that did not drain must be discarded, not returned to the pool",
                        allWorkers(pool).contains(w));
                Assert.assertEquals("the discarded worker must leave the pool so it can grow a fresh one",
                        0, allWorkers(pool).size());
                Assert.assertFalse("the discarded worker's dispatch thread must have exited",
                        dispatchThread(w).isAlive());
            }
        });
    }

    @Test(timeout = 30_000)
    public void testCloseIsInterruptible() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QueryClientPool pool = new QueryClientPool(
                    CFG, 0, 2, 1_000L, Long.MAX_VALUE, Long.MAX_VALUE, NO_CONNECT)) {
                // A long budget: the only way close() can return promptly is by
                // honoring the caller's interrupt.
                setCloseQueryTimeout(pool, 60_000L);
                QueryWorker w = pool.acquire();
                long gen = generation(w);
                setDone(w, false);

                Thread.currentThread().interrupt();
                long startNanos = System.nanoTime();
                closeQuery(w, gen);
                long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000;

                Assert.assertTrue("close() must preserve the caller's interrupt flag", Thread.interrupted());
                Assert.assertTrue("interrupt must abort the drain promptly, elapsed=" + elapsedMs,
                        elapsedMs < 5_000);
                Assert.assertFalse("an interrupted close() must discard the worker",
                        allWorkers(pool).contains(w));
            }
        });
    }

    @Test(timeout = 30_000)
    public void testCloseReturnsWorkerWhenAlreadyDrained() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QueryClientPool pool = new QueryClientPool(
                    CFG, 0, 2, 1_000L, Long.MAX_VALUE, Long.MAX_VALUE, NO_CONNECT)) {
                setCloseQueryTimeout(pool, 150L);
                QueryWorker w = pool.acquire();
                long gen = generation(w);
                // done stays true (no in-flight submit): close() must take the fast
                // path and return the worker to the pool for reuse, not discard it.
                closeQuery(w, gen);
                Assert.assertTrue("an already-drained worker must be returned to the pool, not discarded",
                        allWorkers(pool).contains(w));
            }
        });
    }

    @SuppressWarnings("unchecked")
    private static ArrayList<QueryWorker> allWorkers(QueryClientPool pool) throws Exception {
        Field f = QueryClientPool.class.getDeclaredField("all");
        f.setAccessible(true);
        return (ArrayList<QueryWorker>) f.get(pool);
    }

    private static void closeQuery(QueryWorker w, long gen) throws Exception {
        Object impl = queryImpl(w);
        Method close = impl.getClass().getDeclaredMethod("close", long.class);
        close.setAccessible(true);
        close.invoke(impl, gen);
    }

    private static Thread dispatchThread(QueryWorker w) throws Exception {
        Field f = QueryWorker.class.getDeclaredField("thread");
        f.setAccessible(true);
        return (Thread) f.get(w);
    }

    private static long generation(QueryWorker w) throws Exception {
        Method m = QueryWorker.class.getDeclaredMethod("generation");
        m.setAccessible(true);
        return (long) m.invoke(w);
    }

    private static Object queryImpl(QueryWorker w) throws Exception {
        Field queryF = QueryWorker.class.getDeclaredField("query");
        queryF.setAccessible(true);
        return queryF.get(w);
    }

    private static void setCloseQueryTimeout(QueryClientPool pool, long millis) throws Exception {
        Field f = QueryClientPool.class.getDeclaredField("closeQueryTimeoutMillis");
        f.setAccessible(true);
        f.setLong(pool, millis);
    }

    private static void setDone(QueryWorker w, boolean done) throws Exception {
        Object impl = queryImpl(w);
        Field doneF = impl.getClass().getDeclaredField("done");
        doneF.setAccessible(true);
        doneF.setBoolean(impl, done);
    }
}
