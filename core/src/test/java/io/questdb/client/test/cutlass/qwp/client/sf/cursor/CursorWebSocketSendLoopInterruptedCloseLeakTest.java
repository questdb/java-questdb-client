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

package io.questdb.client.test.cutlass.qwp.client.sf.cursor;

import io.questdb.client.DefaultHttpClientConfiguration;
import io.questdb.client.cutlass.http.client.WebSocketClient;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.Compat;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Red test for finding C5 — interrupted drainer teardown abandons a client
 * installed by an in-flight reconnect.
 * <p>
 * Production sequence being modeled: during a server outage an orphan
 * drainer's {@link CursorWebSocketSendLoop} I/O thread sits inside a
 * blocking native connect ({@code connect_timeout} defaults to 0 = OS
 * timeout, tens of seconds; neither {@code unpark} nor interrupt cancels
 * {@code connect(2)}). Under Invariant B the drainer no longer exits on a
 * wall-clock budget, so {@code BackgroundDrainerPool.close()} routinely
 * escalates: 2500&nbsp;ms graceful drain &rarr; {@code requestStop()} &rarr;
 * 500&nbsp;ms grace &rarr; {@code shutdownNow()}. The {@code shutdownNow()}
 * interrupt lands in {@code loop.close()}'s {@code shutdownLatch.await()};
 * pre-fix, {@code close()} swallows the {@link InterruptedException},
 * re-interrupts, and returns while the I/O thread is still alive. When the
 * in-flight {@code reconnect()} subsequently succeeds, {@code swapClient}
 * installs the live client into the abandoned loop — and no code path ever
 * closes it: {@code loop.close()} already ran (its {@code client} read saw
 * null), and {@code ioLoop}'s exit path only counts down the latch. The
 * client's native socket, fds and buffers leak for the life of the process.
 * <p>
 * The test pins the fix-agnostic ownership contract, not a fix strategy:
 * <b>every {@code WebSocketClient} the loop obtains — via constructor or
 * factory — must be closed by the time the loop is quiescent (I/O thread
 * exited, {@code close()} completed or failed loudly).</b> Any of the
 * candidate fixes satisfies it: (a) re-awaiting the shutdown latch in a
 * loop (close() then picks up the swapped client), (b) closing the current
 * client in {@code ioLoop}'s exit path, or (c) {@code connectLoop}
 * discarding-and-closing a factory client obtained after {@code running}
 * went false. A guard-only fix that merely skips engine teardown (the
 * SEGV half of C5) correctly leaves this test red — the leak is a distinct
 * defect.
 * <p>
 * Determinism notes: no sleeps or timing races. The interrupt is injected
 * by pre-setting the closer thread's interrupt flag —
 * {@code CountDownLatch.await()} checks {@code Thread.interrupted()} before
 * parking, so the swallow path is entered on the first call. The "stuck
 * native connect" is a factory blocked on a test latch, which is faithful:
 * a latch await re-parks after {@code close()}'s spurious {@code unpark},
 * and {@code close()} never interrupts the I/O thread.
 */
public class CursorWebSocketSendLoopInterruptedCloseLeakTest {

    @Test
    public void testC5_interruptedCloseMustNotLeakClientInstalledByInFlightReconnect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final CountDownLatch enteredReconnect = new CountDownLatch(1);
            final CountDownLatch releaseConnect = new CountDownLatch(1);
            final AtomicReference<Thread> ioThreadRef = new AtomicReference<>();
            final TrackingStubWebSocketClient liveClient = new TrackingStubWebSocketClient();

            // Stand-in for a blocking native connect(2): entered by the loop's
            // I/O thread, immune to unpark, never interrupted by loop.close().
            // Returns a live client once released — the "reconnect succeeds
            // mid-teardown" arm of C5.
            final CursorWebSocketSendLoop.ReconnectFactory stuckConnect = () -> {
                ioThreadRef.set(Thread.currentThread());
                enteredReconnect.countDown();
                releaseConnect.await();
                return liveClient;
            };

            final CursorSendEngine engine = new CursorSendEngine(null, 64 * 1024);
            try {
                CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                        null /* async-initial-connect: the I/O thread drives the connect */,
                        engine, 0L, 1_000L,
                        stuckConnect,
                        5_000L, 100L, 5_000L, false);
                loop.start();
                Assert.assertTrue("I/O thread never reached the reconnect factory",
                        enteredReconnect.await(5, TimeUnit.SECONDS));

                // Drainer-thread stand-in: BackgroundDrainer.run()'s finally calls
                // loop.close() and shutdownNow()'s interrupt lands in the latch
                // await. Pre-setting the flag makes that deterministic.
                final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
                Thread closer = new Thread(() -> {
                    Thread.currentThread().interrupt();
                    try {
                        loop.close();
                    } catch (Throwable t) {
                        // A close() that THROWS to signal the failed stop is a
                        // valid fix shape (QwpWebSocketSender.close()'s
                        // ioThreadStopped guard consumes exactly that signal).
                        // The ownership assertion below is what must hold.
                        closeFailure.set(t);
                    }
                }, "drainer-close-stand-in");
                closer.setDaemon(true);
                closer.start();

                // close()'s first action is running=false. Once observable, the
                // teardown is underway and the "connect" may complete. Under a
                // re-await fix the closer is still blocked inside close() here,
                // so the gate must open before joining it (fix-agnostic order).
                long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (loop.isRunning()) {
                    Assert.assertTrue("close() never started", System.nanoTime() < deadlineNanos);
                    Compat.onSpinWait();
                }
                releaseConnect.countDown();

                closer.join(5_000L);
                Assert.assertFalse("closer thread did not finish", closer.isAlive());
                Thread ioThread = ioThreadRef.get();
                Assert.assertNotNull(ioThread);
                ioThread.join(5_000L);
                Assert.assertFalse("I/O thread did not exit after the connect returned",
                        ioThread.isAlive());

                // Loop is quiescent. Capture the verdict BEFORE any cleanup so
                // the test's own close calls cannot mask the leak.
                boolean closedByLoop = liveClient.closeCount() > 0;

                Assert.assertTrue(
                        "C5: the WebSocketClient handed to the loop by an in-flight "
                                + "reconnect() was never closed. loop.close() swallowed the "
                                + "InterruptedException from shutdownLatch.await() and returned "
                                + "while the I/O thread was still inside the blocking connect; "
                                + "swapClient then installed the live client into the abandoned "
                                + "loop where nothing closes it — its native socket and fds leak "
                                + "past drainer teardown. Every client the loop obtains "
                                + "(constructor or factory) must be closed by the time the loop "
                                + "is quiescent.",
                        closedByLoop);
            } finally {
                liveClient.close();
                engine.close();
            }
        });
    }

    /**
     * Minimal concrete {@link WebSocketClient} — never performs I/O; counts
     * {@code close()} calls so the test can assert ownership at quiescence.
     * Close remains idempotent via the superclass, matching the production
     * contract owners rely on.
     */
    private static final class TrackingStubWebSocketClient extends WebSocketClient {
        private final AtomicInteger closeCount = new AtomicInteger();

        TrackingStubWebSocketClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
            super.close();
        }

        int closeCount() {
            return closeCount.get();
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
