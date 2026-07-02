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

package io.questdb.client.test.cutlass.qwp.client;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Coverage of the connect-walk lock policy (M11): {@code buildAndConnect}
 * holds a dedicated lock across its endpoint walk, foreground walks block
 * on {@code lock()}, background (drainer) walks use {@code tryLock()} and
 * must YIELD on contention instead of queuing behind the foreground's
 * network I/O.
 * <p>
 * The contract under test has two halves:
 * <ol>
 *   <li>a background walk attempted while a foreground walk holds the lock
 *       throws a plain {@link LineSenderException} (transport-shaped, never
 *       a typed terminal) BEFORE any network attempt — the drainer's retry
 *       loops route it to indefinite capped-backoff retry (Invariant B), so
 *       lock contention can never quarantine a slot;</li>
 *   <li>once the lock is free, the same background factory proceeds into a
 *       real walk (the busy throw is contention-scoped, not sticky).</li>
 * </ol>
 */
public class QwpConnectWalkLockPriorityTest {

    /** Tracks every stub for defensive close (close() is idempotent). */
    private static final List<StubClient> LIVE_STUBS =
            Collections.synchronizedList(new ArrayList<>());

    @Test
    public void testBackgroundWalkYieldsWhileForegroundHoldsLockThenProceedsWhenFree() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 19999)) {
                final CountDownLatch foregroundInConnect = new CountDownLatch(1);
                final CountDownLatch releaseForeground = new CountDownLatch(1);
                final AtomicInteger factoryCalls = new AtomicInteger();
                sender.setClientFactoryOverride(() -> {
                    int call = factoryCalls.incrementAndGet();
                    StubClient stub = new StubClient(
                            call == 1 ? foregroundInConnect : null,
                            call == 1 ? releaseForeground : null);
                    LIVE_STUBS.add(stub);
                    return stub;
                });

                // Foreground walk on a helper thread: its stub connect()
                // parks on releaseForeground, so the walk holds the
                // connect-walk lock for as long as this test wants.
                final CursorWebSocketSendLoop.ReconnectFactory foreground =
                        sender.newReconnectFactory();
                final AtomicReference<Throwable> foregroundError = new AtomicReference<>();
                Thread fg = new Thread(() -> {
                    try {
                        foreground.reconnect();
                    } catch (Throwable e) {
                        foregroundError.set(e);
                    }
                }, "test-foreground-walk");
                fg.setDaemon(true);
                fg.start();
                try {
                    assertTrue("foreground walk must reach its (blocking) connect attempt",
                            foregroundInConnect.await(5, TimeUnit.SECONDS));

                    // HALF 1: background walk while the lock is held. Must
                    // throw the transport-shaped busy exception without
                    // touching the client factory (tryLock fires before any
                    // per-attempt work).
                    final CursorWebSocketSendLoop.ReconnectFactory background =
                            sender.newBackgroundReconnectFactory(() -> false);
                    try {
                        background.reconnect();
                        fail("background walk must yield (tryLock) while the foreground "
                                + "holds the connect-walk lock");
                    } catch (Exception e) {
                        assertSame("lock contention must surface as a PLAIN LineSenderException: "
                                        + "the drainer retry loops route exactly this shape to "
                                        + "indefinite backoff-retry (Invariant B); any typed "
                                        + "terminal here could quarantine a slot on contention",
                                LineSenderException.class, e.getClass());
                        assertTrue("busy message must name the lock (got: " + e.getMessage() + ")",
                                e.getMessage() != null
                                        && e.getMessage().contains("connect walk lock is busy"));
                    }
                    assertEquals("background contention must not reach the client factory "
                                    + "(the yield happens before any network attempt)",
                            1, factoryCalls.get());
                } finally {
                    releaseForeground.countDown();
                }
                fg.join(5_000);
                assertFalse("foreground walk thread must exit once released", fg.isAlive());
                Throwable fgErr = foregroundError.get();
                assertNotNull("foreground walk fails its (single-endpoint) round once the "
                        + "stub connect throws", fgErr);
                assertTrue("foreground failure is the ordinary end-of-round connect error",
                        fgErr instanceof LineSenderException
                                && String.valueOf(fgErr.getMessage()).contains("Failed to connect"));

                // HALF 2: lock is free now — the SAME background factory must
                // get past tryLock and run a real walk: the factory is
                // consulted (call 2) and the failure is the ordinary
                // end-of-round error, NOT the busy throw.
                final CursorWebSocketSendLoop.ReconnectFactory background2 =
                        sender.newBackgroundReconnectFactory(() -> false);
                try {
                    background2.reconnect();
                    fail("stub connect always throws; the walk must fail its round");
                } catch (Exception e) {
                    assertFalse("with the lock free the background walk must proceed past "
                                    + "tryLock (got busy throw: " + e.getMessage() + ")",
                            String.valueOf(e.getMessage()).contains("connect walk lock is busy"));
                }
                assertEquals("the free-lock background walk must reach the client factory",
                        2, factoryCalls.get());
            } finally {
                closeAllStubs();
            }
        });
    }

    private static void closeAllStubs() {
        synchronized (LIVE_STUBS) {
            for (StubClient c : LIVE_STUBS) {
                try {
                    c.close();
                } catch (Throwable ignored) {
                    // best-effort; close() is idempotent
                }
            }
            LIVE_STUBS.clear();
        }
    }

    /**
     * Real-constructor stub (native buffers allocated and freed by the base
     * class; the walk closes failed-attempt clients itself). {@code connect}
     * optionally parks on a latch to pin the walk — and thus the
     * connect-walk lock — then always throws, so no walk ever "succeeds"
     * and reaches upgrade or lifecycle commits.
     */
    private static final class StubClient extends WebSocketClient {
        private final CountDownLatch entered;
        private final CountDownLatch release;

        StubClient(CountDownLatch entered, CountDownLatch release) {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
            this.entered = entered;
            this.release = release;
        }

        @Override
        public void connect(CharSequence host, int port) {
            if (entered != null) {
                entered.countDown();
            }
            if (release != null) {
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new RuntimeException("stub connect never released");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("stub connect interrupted", e);
                }
            }
            throw new RuntimeException("stub: connection refused");
        }

        @Override
        protected void ioWait(int timeout, int op) {
            throw new UnsupportedOperationException("stub: no socket");
        }

        @Override
        protected void setupIoWait() {
            // no-op
        }
    }
}
