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

import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainerPool;
import io.questdb.client.std.ObjList;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Concurrent regression for the {@code submit() / close()} race in
 * {@link BackgroundDrainerPool}.
 * <p>
 * The race window: T1's {@code submit()} reads {@code closed=false},
 * T2 then calls {@code close()} which sets {@code closed=true} and shuts
 * the executor down, then T1 resumes — adds the drainer to {@code active}
 * and calls {@code executor.submit(...)} which throws
 * {@link RejectedExecutionException}. The wrapping lambda's
 * {@code finally{active.remove(drainer)}} never runs, so the drainer is
 * orphaned in {@code active} forever and the caller sees the wrong
 * exception type.
 * <p>
 * Stresses the race with many submitters per close so the JVM scheduler
 * has to land at least one submission inside the unsafe window.
 */
public class BackgroundDrainerPoolRaceTest {

    private static final int ITERATIONS = 200;
    private static final int SUBMITTERS_PER_ITER = 8;

    @Test
    public void testSubmitDoesNotLeakOrThrowRejectedDuringClose() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            int leakedTotal = 0;
            int rejectedTotal = 0;
            int illegalStateTotal = 0;

            for (int iter = 0; iter < ITERATIONS; iter++) {
                BackgroundDrainerPool pool = new BackgroundDrainerPool(2);
                // One drainer per submitter so each thread has its own identity
                // and we can count leaks deterministically.
                BackgroundDrainer[] drainers = new BackgroundDrainer[SUBMITTERS_PER_ITER];
                for (int i = 0; i < SUBMITTERS_PER_ITER; i++) {
                    drainers[i] = (BackgroundDrainer) Unsafe.getUnsafe()
                            .allocateInstance(BackgroundDrainer.class);
                }

                CountDownLatch ready = new CountDownLatch(SUBMITTERS_PER_ITER + 1);
                CountDownLatch go = new CountDownLatch(1);
                AtomicInteger rejected = new AtomicInteger();
                AtomicInteger illegalState = new AtomicInteger();

                Thread[] submitters = new Thread[SUBMITTERS_PER_ITER];
                for (int i = 0; i < SUBMITTERS_PER_ITER; i++) {
                    final BackgroundDrainer d = drainers[i];
                    submitters[i] = new Thread(() -> {
                        ready.countDown();
                        try {
                            go.await();
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        try {
                            pool.submit(d);
                        } catch (RejectedExecutionException e) {
                            rejected.incrementAndGet();
                        } catch (IllegalStateException e) {
                            illegalState.incrementAndGet();
                        } catch (Throwable ignored) {
                        }
                    }, "submitter-" + iter + "-" + i);
                }
                Thread closer = new Thread(() -> {
                    ready.countDown();
                    try {
                        go.await();
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    pool.close();
                }, "closer-" + iter);

                for (Thread s : submitters) s.start();
                closer.start();
                ready.await();
                go.countDown();

                for (Thread s : submitters) s.join(5_000L);
                closer.join(10_000L);

                // After close returns, in-flight executor tasks have either run
                // their finally{active.remove} or been rejected (the bug). Count
                // any drainer still in active as a leak.
                ObjList<BackgroundDrainer> snap = pool.snapshot();
                for (BackgroundDrainer d : drainers) {
                    for (int i = 0, n = snap.size(); i < n; i++) {
                        if (snap.getQuick(i) == d) {
                            leakedTotal++;
                            break;
                        }
                    }
                }
                rejectedTotal += rejected.get();
                illegalStateTotal += illegalState.get();
            }

            // Expected post-fix: zero leaks, zero RejectedExecutionException
            // surfaced to the caller. IllegalStateException is acceptable —
            // submit() seeing closed=true after the user already called close()
            // is a legitimate caller error.
            List<String> failures = new ArrayList<>();
            if (leakedTotal > 0) {
                failures.add("drainers leaked in active[] after race: " + leakedTotal
                        + " (out of " + (ITERATIONS * SUBMITTERS_PER_ITER) + " submissions)");
            }
            if (rejectedTotal > 0) {
                failures.add("submit() threw RejectedExecutionException to the caller: "
                        + rejectedTotal + " — race exposed wrong exception type "
                        + "(should be IllegalStateException or success)");
            }
            if (!failures.isEmpty()) {
                failures.add("(IllegalStateException count for context: " + illegalStateTotal + ")");
                Assert.fail(String.join("; ", failures));
            }
        });
    }
}
