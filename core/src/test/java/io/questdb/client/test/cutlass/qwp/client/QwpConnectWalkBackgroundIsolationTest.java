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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Coverage of the connect-walk concurrency policy (M11): no network I/O
 * runs under a sender-wide lock for background work. A FOREGROUND walk
 * holds the connect-walk lock across its sweep (it owns the shared round
 * state and the lifecycle commits); BACKGROUND (drainer) walks take no
 * lock at all — each sweeps a private {@code QwpHostHealthTracker
 * .RoundCursor} and records health-only results — so a drainer sweep
 * proceeds CONCURRENTLY with a foreground walk that is parked inside a
 * blocking connect, and the foreground's reconnect and {@code close()}
 * paths can never queue behind (or be queued behind by) a drainer's
 * endpoint walk.
 * <p>
 * The proof shape: pin a foreground walk inside {@code connect()} (lock
 * held, I/O in flight), then run TWO full background sweeps to completion
 * while the foreground is still parked. Under the old walk-wide lock
 * (monitor or tryLock-yield) both background calls would have blocked or
 * yielded; lock-free they must reach the client factory and fail with the
 * ordinary end-of-round error.
 */
public class QwpConnectWalkBackgroundIsolationTest {

    /** Tracks every stub for defensive close (close() is idempotent). */
    private static final List<StubClient> LIVE_STUBS =
            Collections.synchronizedList(new ArrayList<>());

    @Test
    public void testBackgroundSweepRunsConcurrentlyWithParkedForegroundWalk() throws Exception {
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
                // connect-walk lock with I/O "in flight" for as long as
                // this test wants.
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

                    // TWO background sweeps run to completion while the
                    // foreground is parked mid-connect. Each must reach the
                    // client factory (lock-free walk, no yield, no blocking)
                    // and fail with the ordinary end-of-round error. Two
                    // sweeps prove per-walk cursor independence: the second
                    // sweep gets its own full walk, not the first's
                    // exhausted cursor.
                    for (int sweep = 1; sweep <= 2; sweep++) {
                        final CursorWebSocketSendLoop.ReconnectFactory background =
                                sender.newBackgroundReconnectFactory(() -> false);
                        try {
                            background.reconnect();
                            fail("stub connect always throws; background sweep " + sweep
                                    + " must fail its round");
                        } catch (Exception e) {
                            assertTrue("background sweep " + sweep + " must fail with the "
                                            + "ordinary end-of-round error, not a lock artifact "
                                            + "(got: " + e.getMessage() + ")",
                                    e instanceof LineSenderException
                                            && String.valueOf(e.getMessage())
                                            .contains("Failed to connect"));
                        }
                        assertEquals("background sweep " + sweep + " must have reached the "
                                        + "client factory while the foreground is parked",
                                1 + sweep, factoryCalls.get());
                        assertTrue("foreground must still be parked in connect (background "
                                        + "sweeps must not disturb it)",
                                fg.isAlive());
                        assertNull("foreground walk must not have failed while background "
                                        + "sweeps ran",
                                foregroundError.get());
                    }
                } finally {
                    releaseForeground.countDown();
                }
                fg.join(5_000);
                assertFalse("foreground walk thread must exit once released", fg.isAlive());

                // The foreground's own outcome is unaffected by the two
                // background sweeps that ran under it: the ordinary
                // end-of-round failure for its single-endpoint round.
                Throwable fgErr = foregroundError.get();
                assertNotNull("foreground walk fails its (single-endpoint) round once the "
                        + "stub connect throws", fgErr);
                assertTrue("foreground failure is the ordinary end-of-round connect error "
                                + "(got: " + fgErr.getMessage() + ")",
                        fgErr instanceof LineSenderException
                                && String.valueOf(fgErr.getMessage()).contains("Failed to connect"));
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
     * optionally parks on a latch to pin the walk — and, on the foreground
     * path, the connect-walk lock — then always throws, so no walk ever
     * "succeeds" and reaches upgrade or lifecycle commits.
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
