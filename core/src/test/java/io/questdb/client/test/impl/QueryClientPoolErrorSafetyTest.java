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
import io.questdb.client.cutlass.qwp.client.QwpQueryClient;
import io.questdb.client.impl.QueryClientPool;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

// Error-safety of the three QueryClientPool creation paths the teardown-hardening
// fix widened from catch (RuntimeException) to catch (Throwable). The native
// build/connect path runs under -ea and can throw an Error (AssertionError,
// OutOfMemoryError); the old catches let that Error skip cleanup.
//
// QwpQueryClient is a concrete class with no fake seam, so these tests inject an
// Error at the real connect step via the package-private connectHook constructor
// (reached by reflection -- the main module is declared `open`). fromConfig()
// still runs for real, committing the NATIVE_DEFAULT scratch the cleanup must
// reclaim, so the memory assertions are meaningful.
public class QueryClientPoolErrorSafetyTest {

    // ws config that fromConfig() parses without opening a socket; the injected
    // connectHook replaces connect(), so the port is never dialled.
    private static final String CFG = "ws::addr=127.0.0.1:9000;";

    // Site: acquire() inner catch around client.connect(). createUnlocked() must
    // close the half-built client when connect throws an Error, otherwise the
    // field-initialised QwpBindValues scratch (NATIVE_DEFAULT) leaks.
    // RED: catch (RuntimeException) -> Error skips client.close() -> leak.
    // GREEN: catch (Throwable) -> client.close() runs -> no leak.
    @Test(timeout = 30_000)
    public void acquireDoesNotLeakNativeScratchOnErrorFromConnect() throws Exception {
        QueryClientPool pool = newPool(CFG, 0, 1, 250, alwaysThrow());
        try {
            long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
            try {
                pool.acquire();
                Assert.fail("expected acquire() to propagate the injected Error");
            } catch (Throwable expected) {
                // wrapped or raw -- the leak check is the discriminator
            }
            long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
            Assert.assertEquals(
                    "acquire() leaked NATIVE_DEFAULT scratch on an Error from connect()",
                    baseline, after);
        } finally {
            pool.close();
        }
    }

    // Site: acquire() outer catch around createUnlocked()/start(). An Error must
    // still run inFlightCreations--, otherwise the reserved slot is leaked and
    // (maxSize == 1) the pool is wedged forever.
    // RED: catch (RuntimeException) -> inFlightCreations stuck at 1.
    // GREEN: catch (Throwable) -> inFlightCreations restored to 0.
    @Test(timeout = 30_000)
    public void acquireRestoresInFlightCreationsOnErrorFromConnect() throws Exception {
        QueryClientPool pool = newPool(CFG, 0, 1, 250, alwaysThrow());
        try {
            try {
                pool.acquire();
                Assert.fail("expected acquire() to propagate the injected Error");
            } catch (Throwable expected) {
                // expected
            }

            Assert.assertEquals(
                    "acquire() leaked an in-flight creation slot on an Error from connect()",
                    0, inFlightCreations(pool));

            // Corollary: capacity is usable again -- the next acquire() must
            // reach the creation path (and fail there) rather than time out.
            try {
                pool.acquire();
                Assert.fail("expected second acquire() to re-attempt creation");
            } catch (QueryException e) {
                Assert.assertFalse(
                        "pool wedged: second acquire() timed out -> capacity permanently lost ("
                                + e.getMessage() + ")",
                        e.getMessage() != null && e.getMessage().contains("timed out"));
            } catch (Throwable injectedAgain) {
                // also fine: the Error surfaced again from the re-attempt
            }
        } finally {
            pool.close();
        }
    }

    // Site: constructor prewarm outer catch. An Error mid-prewarm must run the
    // cleanup loop that shuts down already-built workers, otherwise the first
    // worker's client (NATIVE_DEFAULT) and I/O thread leak.
    // RED: catch (RuntimeException) -> first worker's client never closed.
    // GREEN: catch (Throwable) -> cleanup loop closes it -> no leak.
    @Test(timeout = 30_000)
    public void preWarmDoesNotLeakNativeScratchOnErrorFromConnect() throws Exception {
        long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
        // First connect() succeeds (no-op, leaves the client unconnected but
        // built); the second throws an Error mid-prewarm.
        AtomicInteger calls = new AtomicInteger();
        Consumer<QwpQueryClient> hook = client -> {
            if (calls.incrementAndGet() >= 2) {
                throw new AssertionError("injected native connect failure");
            }
        };
        try {
            newPool(CFG, 2, 2, 250, hook);
            Assert.fail("expected prewarm to propagate the injected Error");
        } catch (Throwable expected) {
            // expected -- construction aborts
        }
        long after = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_DEFAULT);
        Assert.assertEquals(
                "prewarm leaked NATIVE_DEFAULT scratch of an already-built worker on an Error",
                baseline, after);
    }

    private static Consumer<QwpQueryClient> alwaysThrow() {
        return client -> {
            throw new AssertionError("injected native connect failure");
        };
    }

    private static int inFlightCreations(QueryClientPool pool) throws Exception {
        Method m = QueryClientPool.class.getDeclaredMethod("inFlightCreations");
        m.setAccessible(true);
        return (int) m.invoke(pool);
    }

    private static QueryClientPool newPool(
            String cfg, int min, int max, long acquireMs, Consumer<QwpQueryClient> connectHook
    ) throws Exception {
        Constructor<QueryClientPool> c = QueryClientPool.class.getDeclaredConstructor(
                String.class, int.class, int.class, long.class, long.class, long.class, Consumer.class);
        c.setAccessible(true);
        return c.newInstance(cfg, min, max, acquireMs, Long.MAX_VALUE, Long.MAX_VALUE, connectHook);
    }
}
