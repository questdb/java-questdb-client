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
 * Pins the order of {@link CursorWebSocketSendLoop#close()}: an I/O thread that
 * is not stuck must stop on its own and close its client -- the WebSocket CLOSE
 * frame and TLS close_notify go out on that path -- before close() would break
 * the connection's traffic. Breaking traffic first made the server see every
 * sender close as a dropped connection. A connect blocked on cancellable work
 * must still be cancelled at once, not after the graceful-stop window.
 */
public class CursorWebSocketSendLoopGracefulCloseTest {

    @Test(timeout = 30_000L)
    public void testCloseCancelsInFlightConnectWithoutWaitingOutGracefulStop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final AtomicReference<InFlightConnectClient> published = new AtomicReference<>();
            final CountDownLatch connectEntered = new CountDownLatch(1);
            final CursorWebSocketSendLoop.ReconnectFactory factory = new CursorWebSocketSendLoop.ReconnectFactory() {
                @Override
                public WebSocketClient reconnect() {
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
                    c.awaitTrafficBreak();
                    c.close();
                    throw new LineSenderException("connect cancelled by closeTraffic()");
                }
            };
            final CursorSendEngine engine = new CursorSendEngine(null, 64 * 1024);
            final CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                    null,
                    engine,
                    0L,
                    CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                    factory,
                    1_000L,
                    5_000L,
                    false
            );
            // Far beyond the test timeout: waiting it out here would fail the test.
            loop.setGracefulStopMillis(TimeUnit.MINUTES.toMillis(5));
            try {
                loop.start();
                Assert.assertTrue("I/O worker never entered the blocking connect",
                        connectEntered.await(5, TimeUnit.SECONDS));

                long startNanos = System.nanoTime();
                loop.close();
                long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

                Assert.assertEquals("close() must cancel the in-flight connect exactly once",
                        1, published.get().trafficCloseCount.get());
                Assert.assertTrue("close() waited out the graceful-stop window for a connect it had to cancel, took "
                        + elapsedMillis + "ms", elapsedMillis < TimeUnit.SECONDS.toMillis(10));
                Assert.assertNull("ordinary connect-phase close must not manufacture a terminal error",
                        loop.getTerminalError());
            } finally {
                InFlightConnectClient inFlight = published.get();
                if (inFlight != null) {
                    inFlight.trafficBroken.countDown();
                }
                loop.close();
                engine.close();
            }
        });
    }

    @Test(timeout = 30_000L)
    public void testCloseLetsIdleWorkerCloseClientBeforeBreakingTraffic() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final IdleClient client = new IdleClient();
            final CursorSendEngine engine = new CursorSendEngine(null, 64 * 1024);
            final CursorWebSocketSendLoop loop = new CursorWebSocketSendLoop(
                    client,
                    engine,
                    0L,
                    CursorWebSocketSendLoop.DEFAULT_PARK_NANOS,
                    null,
                    1_000L,
                    5_000L,
                    false
            );
            // Far above any scheduling delay, so a loaded machine cannot flip the outcome.
            loop.setGracefulStopMillis(TimeUnit.SECONDS.toMillis(20));
            try {
                loop.start();
                Assert.assertTrue("I/O worker never polled for ACKs", client.polled.await(5, TimeUnit.SECONDS));

                loop.close();

                Assert.assertEquals("close() broke the traffic of an idle worker instead of letting it close cleanly",
                        0, client.trafficCloseCount.get());
                Assert.assertNotNull("the client was never closed", client.firstCloseThread.get());
                Assert.assertEquals("the I/O worker must close its own client on the way out",
                        client.pollThread.get(), client.firstCloseThread.get());
                Assert.assertNull("ordinary close must not manufacture a terminal error", loop.getTerminalError());
            } finally {
                loop.close();
                engine.close();
                client.close();
            }
        });
    }

    /**
     * Connected client with nothing to receive. Records which thread closes it
     * first and whether anyone broke its traffic.
     */
    private static final class IdleClient extends WebSocketClient {
        final AtomicReference<Thread> firstCloseThread = new AtomicReference<>();
        final AtomicReference<Thread> pollThread = new AtomicReference<>();
        final CountDownLatch polled = new CountDownLatch(1);
        final AtomicInteger trafficCloseCount = new AtomicInteger();

        private IdleClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void close() {
            firstCloseThread.compareAndSet(null, Thread.currentThread());
            super.close();
        }

        @Override
        public void closeTraffic() {
            trafficCloseCount.incrementAndGet();
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            pollThread.compareAndSet(null, Thread.currentThread());
            polled.countDown();
            return false;
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
     * Fake in-flight connect: parks until {@link #closeTraffic()} releases it,
     * like a native connect that neither unpark nor interrupt can cancel.
     */
    private static final class InFlightConnectClient extends WebSocketClient {
        final CountDownLatch trafficBroken = new CountDownLatch(1);
        final AtomicInteger trafficCloseCount = new AtomicInteger();

        private InFlightConnectClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void closeTraffic() {
            trafficCloseCount.incrementAndGet();
            trafficBroken.countDown();
        }

        @Override
        protected void ioWait(int timeout, int op) {
            throw new UnsupportedOperationException("stub: no socket");
        }

        @Override
        protected void setupIoWait() {
            // no-op
        }

        void awaitTrafficBreak() {
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
    }
}
