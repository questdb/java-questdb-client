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
import io.questdb.client.cutlass.http.client.WebSocketFrameHandler;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * TRUE-CANCELLATION proof (P1): {@code close()} must cancel a connect attempt
 * that is blocked inside {@code reconnectFactory.reconnect(...)} (a black-holed
 * native connect that neither unpark nor interrupt can cancel). The in-flight
 * {@link WebSocketClient} is walk-local -- it is NOT the loop's {@code client}
 * field (that field is {@code null} on the async-initial connect and points at
 * the stale pre-drop client on a mid-flight reconnect) -- so before the fix
 * {@code close()}'s field-client {@code closeTraffic()} could not reach it and
 * {@code close()} blocked on the untimed {@code shutdownLatch.await()} for the
 * whole OS SYN-retry window (~60-130s per endpoint).
 * <p>
 * The fix publishes the in-flight client to a race-safe
 * {@link CursorWebSocketSendLoop.ConnectCancellation} handle before the
 * blocking connect, and {@code close()} breaks that client's traffic. These
 * tests assert TRUE cancellation, not just return: the fake in-flight client's
 * {@code closeTraffic()} is the ONLY thing that unblocks the parked
 * {@code reconnect()}, and the tests witness that {@code closeTraffic()} was
 * invoked (exactly once, by the closer thread) AND that {@code close()}
 * returned. Deterministic latches only; {@code @Test(timeout=...)} fails a
 * regression fast.
 */
public class CursorWebSocketSendLoopConnectPhaseCloseTest {

    /**
     * Generous budget: with the fix, close() breaks the in-flight connect and
     * returns well within this. Without the fix, close() blocks on the untimed
     * shutdown latch for the entire (simulated) connect and this budget lapses.
     */
    private static final long CLOSE_BUDGET_MILLIS = 5_000L;

    /**
     * Small, injectable bounded-await backstop for the TOCTOU test. Shrunk from
     * the production {@code DEFAULT_CLOSE_SHUTDOWN_AWAIT_MILLIS} (30 s) via
     * {@link CursorWebSocketSendLoop#setShutdownAwaitTimeoutMillis(long)} so the
     * timeout branch fires fast and deterministically -- no multi-second real
     * wait -- while still leaving CLOSE_BUDGET_MILLIS of slack for the closer
     * thread to return.
     */
    private static final long BACKSTOP_MILLIS = 500L;

    /**
     * Async-initial-connect path: the loop is built with a {@code null} client
     * and a reconnect factory, so the I/O thread drives the very first connect
     * through connectLoop while the {@code client} field stays {@code null}.
     */
    @Test(timeout = 30_000L)
    public void testCloseCancelsAsyncInitialConnectViaInFlightClient() throws Exception {
        runConnectCancelledByCloseTraffic(false);
    }

    /**
     * Mid-flight-reconnect path: an initial client is installed, its first
     * receive fails, so connectLoop reconnects and blocks. The {@code client}
     * field then points at the stale pre-drop client, never the in-flight one.
     */
    @Test(timeout = 30_000L)
    public void testCloseCancelsMidFlightReconnectViaInFlightClient() throws Exception {
        runConnectCancelledByCloseTraffic(true);
    }

    /**
     * BACKSTOP (bounded-await) proof: models the pathological, uninterruptible
     * TOCTOU case where {@code cancel()}'s {@code closeTraffic()} CANNOT break
     * the in-flight connect (as if cancellation landed after the pre-connect
     * guard but before native fd creation, making closeTraffic() a no-op). The
     * connect stays parked, so round-2 cancellation does NOT release the latch.
     * Asserts {@code close()} STILL returns within the bounded backstop (does
     * not hang) and surfaces the failed-stop contract: a loud
     * {@link LineSenderException} whose message names the stalled I/O thread and
     * the timeout, with client/engine teardown delegated to the I/O thread's
     * own exit path (no destructive close under the still-live worker).
     */
    @Test(timeout = 30_000L)
    public void testCloseReturnsBoundedWhenCancellationCannotBreakConnect() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final AtomicReference<UninterruptibleInFlightClient> published = new AtomicReference<>();
            final CountDownLatch connectEntered = new CountDownLatch(1);
            // The ONLY thing that releases the parked connect. closeTraffic()
            // deliberately does NOT touch it, modelling a cancel that cannot
            // break the connect; the test itself counts it down, AFTER the
            // backstop assertions, so the worker can unwind for the leak check.
            final CountDownLatch release = new CountDownLatch(1);

            final CursorWebSocketSendLoop.ReconnectFactory factory = new CursorWebSocketSendLoop.ReconnectFactory() {
                @Override
                public WebSocketClient reconnect() throws Exception {
                    return reconnect(null);
                }

                @Override
                public WebSocketClient reconnect(CursorWebSocketSendLoop.ConnectCancellation cancellation) {
                    UninterruptibleInFlightClient c = new UninterruptibleInFlightClient(release);
                    published.set(c);
                    if (cancellation != null) {
                        cancellation.publish(c);
                    }
                    connectEntered.countDown();
                    // Uninterruptible AND uncancellable: closeTraffic() does not
                    // release this park; only the test's release latch does.
                    c.awaitRelease();
                    c.close();
                    throw new LineSenderException("connect ended after release");
                }
            };

            final CursorSendEngine engine = new CursorSendEngine(null, 64 * 1024);
            final CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                    null,
                    engine,
                    0L,
                    CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                    factory,
                    /* reconnectInitialBackoffMillis */ 1_000L,
                    /* reconnectMaxBackoffMillis */ 5_000L,
                    false
            );
            // Shrink the bounded-await backstop so the timeout branch fires fast
            // (no multi-second real wait); production uses the 30s default.
            loop.setShutdownAwaitTimeoutMillis(BACKSTOP_MILLIS);

            final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            Thread closer = null;
            try {
                loop.start();
                Assert.assertTrue("I/O worker never entered the blocking connect",
                        connectEntered.await(5, TimeUnit.SECONDS));

                closer = new Thread(() -> {
                    try {
                        loop.close();
                    } catch (Throwable t) {
                        closeFailure.set(t);
                    }
                }, "backstop-closer");
                closer.start();

                closer.join(CLOSE_BUDGET_MILLIS);
                final boolean closedInTime = !closer.isAlive();

                final UninterruptibleInFlightClient inFlight = published.get();
                Assert.assertNotNull("no in-flight client was ever published", inFlight);
                // cancel() DID attempt to break the connect -- but closeTraffic()
                // is a no-op here, so the connect is still parked.
                Assert.assertTrue("close() must have attempted to cancel the in-flight connect",
                        inFlight.trafficCloseCount.get() >= 1);
                Assert.assertEquals("the parked connect must NOT have been released by closeTraffic()",
                        1L, release.getCount());

                // Decisive: close() returns within the bounded backstop even
                // though cancellation could not break the connect.
                Assert.assertTrue(
                        "close() must return within the bounded backstop (" + CLOSE_BUDGET_MILLIS
                                + "ms) even when cancellation cannot break the connect; instead it hung",
                        closedInTime);

                // Failed-stop contract: loud LineSenderException naming the
                // stalled thread and the timeout.
                final Throwable failure = closeFailure.get();
                Assert.assertNotNull("close() must loud-fail when the backstop times out", failure);
                Assert.assertTrue("failed stop must be a LineSenderException, was " + failure,
                        failure instanceof LineSenderException);
                Assert.assertTrue("message must name the stalled I/O thread: " + failure.getMessage(),
                        failure.getMessage().contains("cursor I/O thread did not stop"));
                Assert.assertTrue("message must attribute the failed stop to the backstop timeout: "
                                + failure.getMessage(),
                        failure.getMessage().contains("timed out"));
                // Teardown is delegated to the I/O thread's exit path, not done
                // destructively under the live worker: no terminal manufactured.
                Assert.assertNull("backstop timeout must not manufacture a terminal error",
                        loop.getTerminalError());
            } finally {
                // Now let the parked connect unwind so the worker exits and the
                // leak check sees a clean teardown. Restore a generous backstop
                // so the reconciling close() below awaits the worker's exit.
                loop.setShutdownAwaitTimeoutMillis(CLOSE_BUDGET_MILLIS);
                release.countDown();
                if (closer != null) {
                    closer.join(TimeUnit.SECONDS.toMillis(5));
                }
                loop.close();
                engine.close();
            }
        });
    }

    private void runConnectCancelledByCloseTraffic(boolean withInitialClient) throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Allocate the initial client INSIDE the leak-checked block so its
            // native buffers are inside the baseline, not counted as over-free.
            final WebSocketClient initialClient = withInitialClient ? new FailingInitialClient() : null;
            final AtomicReference<InFlightConnectClient> published = new AtomicReference<>();
            final CountDownLatch connectEntered = new CountDownLatch(1);

            // A reconnect factory that models the real connect walk: it creates
            // the client it is about to block on, PUBLISHES it to the loop's
            // cancellation handle before the blocking connect, then parks in a
            // way that ONLY closeTraffic() (i.e. cancellation) can release --
            // exactly a black-holed native connect that unpark/interrupt cannot
            // cancel.
            final CursorWebSocketSendLoop.ReconnectFactory factory = new CursorWebSocketSendLoop.ReconnectFactory() {
                @Override
                public WebSocketClient reconnect() throws Exception {
                    return reconnect(null);
                }

                @Override
                public WebSocketClient reconnect(CursorWebSocketSendLoop.ConnectCancellation cancellation) {
                    InFlightConnectClient c = new InFlightConnectClient();
                    published.set(c);
                    if (cancellation != null) {
                        cancellation.publish(c);
                    }
                    connectEntered.countDown();
                    // Black-holed connect: returns only once closeTraffic() breaks it.
                    c.awaitTrafficBreak();
                    // Cancelled: dispose the half-built client (frees native
                    // buffers) and surface a transport error, exactly as the
                    // real connect walk's catch does on a broken connect.
                    c.close();
                    throw new LineSenderException("connect cancelled by closeTraffic()");
                }
            };

            final CursorSendEngine engine = new CursorSendEngine(null, 64 * 1024);
            final CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                    initialClient,
                    engine,
                    0L,
                    CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                    factory,
                    /* reconnectInitialBackoffMillis */ 1_000L,
                    /* reconnectMaxBackoffMillis */ 5_000L,
                    false
            );

            final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
            Thread closer = null;
            try {
                loop.start();
                Assert.assertTrue("I/O worker never entered the blocking connect",
                        connectEntered.await(5, TimeUnit.SECONDS));

                closer = new Thread(() -> {
                    try {
                        loop.close();
                    } catch (Throwable t) {
                        closeFailure.set(t);
                    }
                }, "connect-phase-closer");
                closer.start();

                closer.join(CLOSE_BUDGET_MILLIS);
                final boolean closedInTime = !closer.isAlive();

                final InFlightConnectClient inFlight = published.get();
                Assert.assertNotNull("no in-flight client was ever published to the cancellation handle",
                        inFlight);

                // Let the worker finish unwinding regardless of outcome so the
                // memory-leak check sees a clean teardown.
                inFlight.trafficBroken.countDown();
                closer.join(TimeUnit.SECONDS.toMillis(10));

                Assert.assertNull("close() must not fail", closeFailure.get());
                Assert.assertTrue(
                        "close() must cancel the connect blocked inside reconnect() and return "
                                + "within " + CLOSE_BUDGET_MILLIS + "ms; instead it blocked on the "
                                + "untimed shutdown latch for the whole connect",
                        closedInTime);
                // Decisive: closeTraffic() on the IN-FLIGHT client is what
                // unblocked the parked connect -- and it was the closer thread,
                // not the I/O worker, that broke it.
                Assert.assertEquals(
                        "close() must break the in-flight connect's traffic exactly once",
                        1, inFlight.trafficCloseCount.get());
                Assert.assertEquals(
                        "the closer thread (not the I/O worker) must cancel the in-flight connect",
                        closer, inFlight.trafficCloseThread.get());
                Assert.assertNull("ordinary connect-phase close must not manufacture a terminal error",
                        loop.getTerminalError());
            } finally {
                InFlightConnectClient inFlight = published.get();
                if (inFlight != null) {
                    inFlight.trafficBroken.countDown();
                }
                if (closer != null) {
                    closer.join(TimeUnit.SECONDS.toMillis(5));
                }
                loop.close();
                engine.close();
                if (initialClient != null) {
                    initialClient.close();
                }
            }
        });
    }

    /**
     * The fake in-flight client. It never really connects: the factory parks in
     * {@link #awaitTrafficBreak()} until {@link #closeTraffic()} releases it,
     * which is the ONLY path out -- neither unpark nor interrupt can cancel it.
     */
    private static final class InFlightConnectClient extends WebSocketClient {
        final CountDownLatch trafficBroken = new CountDownLatch(1);
        final AtomicInteger trafficCloseCount = new AtomicInteger();
        final AtomicReference<Thread> trafficCloseThread = new AtomicReference<>();

        private InFlightConnectClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void closeTraffic() {
            trafficCloseThread.compareAndSet(null, Thread.currentThread());
            trafficCloseCount.incrementAndGet();
            trafficBroken.countDown();
        }

        void awaitTrafficBreak() {
            // Uninterruptible: only closeTraffic()'s countDown may release us,
            // faithfully modelling a native connect that unpark/interrupt cannot
            // cancel.
            boolean interrupted = false;
            while (trafficBroken.getCount() != 0L) {
                try {
                    trafficBroken.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
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

    /**
     * The fake in-flight client for the BACKSTOP test. Its {@link #closeTraffic()}
     * is a no-op w.r.t. releasing the park -- it only records that cancellation
     * was attempted -- so a {@code close()}->{@code cancel()} CANNOT unblock the
     * parked connect. This models the pathological uninterruptible/TOCTOU case;
     * the ONLY release is the test-owned {@code release} latch, counted down
     * AFTER the backstop assertions.
     */
    private static final class UninterruptibleInFlightClient extends WebSocketClient {
        final AtomicInteger trafficCloseCount = new AtomicInteger();
        private final CountDownLatch release;

        private UninterruptibleInFlightClient(CountDownLatch release) {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
            this.release = release;
        }

        @Override
        public void closeTraffic() {
            // Records the cancel attempt but does NOT release the parked
            // connect: exactly a closeTraffic() that lands before the native fd
            // exists (a no-op), so the connect blocks on regardless.
            trafficCloseCount.incrementAndGet();
        }

        void awaitRelease() {
            // Uninterruptible: neither unpark, interrupt, nor closeTraffic()
            // releases us -- only the test's release latch.
            boolean interrupted = false;
            while (release.getCount() != 0L) {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
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

    /**
     * A pre-installed client whose first receive fails, driving the I/O loop
     * into a reconnect so the in-flight client is published while the
     * {@code client} field still points here (the mid-flight-reconnect path).
     */
    private static final class FailingInitialClient extends WebSocketClient {

        private FailingInitialClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            throw new LineSenderException("initial wire dropped");
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
