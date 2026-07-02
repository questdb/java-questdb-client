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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.IntFunction;

// Error-safety of the QuestDBImpl constructor that ties SenderPool and
// QueryClientPool together. The constructor builds the SenderPool first, then
// the QueryClientPool; both run heavy native build/connect paths that, under -ea,
// can throw an Error (AssertionError, OutOfMemoryError). If the orchestrator's
// cleanup catch is narrower than the pools' own (catch (RuntimeException) vs
// catch (Throwable)), an Error from QueryClientPool construction propagates
// without closing the already-built SenderPool, stranding its prewarmed
// delegates (flock + mmap'd ring + I/O thread).
//
// Sender is an interface, faked with a Proxy whose close() flips a flag, injected
// via the SenderPool senderFactory seam. The connect Error is injected via the
// QueryClientPool connectHook seam. Both are passed through the @TestOnly public
// QuestDBImpl seam constructor; production uses the public overload that passes
// null for both.
public class QuestDBImplErrorSafetyTest {

    // Non-SF http config: the SenderPool factory replaces the build, but the
    // constructor's eager config probe must still parse it.
    private static final String SENDER_CFG = "http::addr=127.0.0.1:1;protocol_version=2;auto_flush=off;";
    // ws config that fromConfig() parses without opening a socket; the injected
    // connectHook replaces connect(), so the port is never dialled.
    private static final String QUERY_CFG = "ws::addr=127.0.0.1:9000;";

    // RED: catch (RuntimeException) -> the Error from QueryClientPool construction
    //      skips the cleanup block -> the prewarmed SenderPool is never closed ->
    //      its fake delegate's close() is never called.
    // GREEN: catch (Throwable) -> cleanup closes the SenderPool -> the fake
    //      delegate's close() runs.
    @Test(timeout = 30_000)
    public void ctorClosesBuiltSenderPoolWhenQueryPoolConstructionThrowsError() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            AtomicBoolean senderClosed = new AtomicBoolean(false);
            // senderMin = 1 -> SenderPool prewarms one observable delegate.
            IntFunction<Sender> senderFactory = slotIndex -> fakeSender(senderClosed);
            // queryMin = 1 -> QueryClientPool prewarm reaches connect(), which throws.
            Consumer<QwpQueryClient> connectHook = client -> {
                throw new AssertionError("injected native connect failure");
            };

            try {
                newQuestDB(senderFactory, connectHook);
                Assert.fail("expected QuestDBImpl construction to propagate the injected Error");
            } catch (Throwable expected) {
                // expected -- construction aborts
            }

            Assert.assertTrue(
                    "QuestDBImpl ctor leaked the already-built SenderPool on an Error from "
                            + "QueryClientPool construction: the prewarmed delegate's close() was never called",
                    senderClosed.get());
        });
    }

    private static Sender fakeSender(AtomicBoolean closedFlag) {
        return (Sender) Proxy.newProxyInstance(
                Sender.class.getClassLoader(),
                new Class[]{Sender.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "close":
                            closedFlag.set(true);
                            return null;
                        case "toString":
                            return "FakeSender";
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

    private static QuestDBImpl newQuestDB(
            IntFunction<Sender> senderFactory, Consumer<QwpQueryClient> connectHook
    ) {
        return new QuestDBImpl(
                SENDER_CFG, QUERY_CFG,
                /*senderMin*/ 1, /*senderMax*/ 1,
                /*queryMin*/ 1, /*queryMax*/ 1,
                /*acquireTimeoutMillis*/ 250L,
                /*idleTimeoutMillis*/ Long.MAX_VALUE,
                /*maxLifetimeMillis*/ Long.MAX_VALUE,
                /*housekeeperIntervalMillis*/ Long.MAX_VALUE,
                senderFactory, connectHook);
    }
}
