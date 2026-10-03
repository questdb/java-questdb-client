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
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendCounters;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * {@link CursorWebSocketSendLoop#closeIfLinkUp()}, the symbol-dictionary
 * recycle's loop stop, and the connect gate it shares with the I/O thread's
 * connect walk. The recycle must never stop a loop whose I/O thread has begun
 * a reconnect: the walk can sit in a credential pull or a hostname resolve
 * that {@code close()} cannot cancel, and the recycling producer would wait
 * out the whole shutdown budget. A {@code isLinkUp()} check alone cannot
 * promise that -- a drop can start the walk right after it -- so the stop and
 * the walk's entry decide through one CAS and exactly one of them wins. These
 * tests pin both outcomes deterministically.
 */
public class CursorWebSocketSendLoopConnectGateTest {

    /**
     * The owner won: once a stop is claimed, an I/O thread that observes a
     * drop must not start a connect walk at all -- it parks until the claimed
     * stop's {@code close()} clears {@code running}, then returns without
     * ever reaching the connect factory. Without the gate the walk would run
     * straight into the factory (where a real credential pull would block).
     */
    @Test(timeout = 30_000L)
    public void testClaimedStopKeepsIoThreadOutOfConnectWalk() throws Exception {
        CursorWebSocketSendLoop loop = (CursorWebSocketSendLoop) Unsafe.getUnsafe()
                .allocateInstance(CursorWebSocketSendLoop.class);
        AtomicInteger factoryCalls = new AtomicInteger();
        CursorWebSocketSendLoop.ReconnectFactory factory = () -> {
            factoryCalls.incrementAndGet();
            throw new LineSenderException("a claimed stop must keep the walk out of the factory");
        };
        // Field initializers do not run under allocateInstance: wire what the
        // walk would touch if it wrongly started.
        setField(loop, "reconnectFactory", factory);
        setField(loop, "counters", new CursorSendCounters());
        setField(loop, "parkNanos", TimeUnit.MILLISECONDS.toNanos(1));
        setField(loop, "running", true);
        setField(loop, "connectGate", getStaticInt("CONNECT_GATE_STOP_CLAIMED"));

        AtomicReference<Throwable> walkFailure = new AtomicReference<>();
        Thread io = new Thread(() -> {
            try {
                loop.connectLoopForTest(new LineSenderException("peer disconnect"), "reconnect", 0L);
            } catch (Throwable t) {
                walkFailure.set(t);
            }
        }, "claimed-stop-io");
        io.start();
        io.join(200);
        Assert.assertTrue("the walk must park behind the claimed stop, not skip past it or run",
                io.isAlive());
        Assert.assertEquals("the walk must not reach the connect factory", 0, factoryCalls.get());

        // What the claimed stop's close() does first.
        setField(loop, "running", false);
        LockSupport.unpark(io);
        io.join(5_000);
        Assert.assertFalse("the parked walk must return once running clears", io.isAlive());
        Assert.assertNull("the parked walk must return quietly", walkFailure.get());
        Assert.assertEquals("the walk must never reach the connect factory", 0, factoryCalls.get());
    }

    /**
     * The I/O thread won: while its connect walk is blocked in a pull that
     * ignores interrupts, {@code closeIfLinkUp()} must refuse without touching
     * the loop -- no stop, no cancel, no interrupt -- and return at once. Once
     * the walk installs a live client, the same call must stop the loop
     * promptly: a live link only needs its socket cancelled.
     */
    @Test(timeout = 30_000L)
    public void testCloseIfLinkUpRefusesDuringConnectWalkThenStopsLiveLoop() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final IdleLinkClient initialClient = new IdleLinkClient();
            final AtomicReference<IdleLinkClient> reconnectedClient = new AtomicReference<>();
            final CountDownLatch walkEntered = new CountDownLatch(1);
            final CountDownLatch releaseWalk = new CountDownLatch(1);
            final AtomicInteger interruptsSeenByWalk = new AtomicInteger();
            final CursorWebSocketSendLoop.ReconnectFactory factory = () -> {
                walkEntered.countDown();
                // Stand-in for a credential pull that ignores interrupts (a
                // blocking java.net read, a native HTTP round trip): only the
                // test's latch ends it.
                while (releaseWalk.getCount() != 0L) {
                    try {
                        releaseWalk.await();
                    } catch (InterruptedException e) {
                        interruptsSeenByWalk.incrementAndGet();
                    }
                }
                IdleLinkClient live = new IdleLinkClient();
                reconnectedClient.set(live);
                return live;
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
            try {
                loop.start();
                Assert.assertTrue("precondition: the loop holds a live link", loop.isLinkUp());

                initialClient.drop();
                Assert.assertTrue("the I/O thread never entered its connect walk",
                        walkEntered.await(5, TimeUnit.SECONDS));

                long t0 = System.nanoTime();
                Assert.assertFalse("must refuse while the I/O thread is inside a connect walk",
                        loop.closeIfLinkUp());
                long refuseMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                Assert.assertTrue("a refusal must not wait on the walk [millis=" + refuseMillis + ']',
                        refuseMillis < 1_000L);
                Assert.assertTrue("a refused stop must leave the loop running", loop.isRunning());
                Assert.assertEquals("a refused stop must not cancel or interrupt the walk",
                        0, interruptsSeenByWalk.get());

                releaseWalk.countDown();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (!loop.isLinkUp() && System.nanoTime() < deadline) {
                    Thread.sleep(1);
                }
                Assert.assertTrue("the walk must reinstall a live link", loop.isLinkUp());

                t0 = System.nanoTime();
                Assert.assertTrue("a live link must be stoppable", loop.closeIfLinkUp());
                long stopMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
                Assert.assertFalse("the claimed stop must stop the loop", loop.isRunning());
                Assert.assertTrue("stopping a live link must be prompt [millis=" + stopMillis + ']',
                        stopMillis < 5_000L);
                Assert.assertFalse("a stopped loop is not up", loop.isLinkUp());
            } finally {
                releaseWalk.countDown();
                loop.close();
                engine.close();
                // Both are closed by the loop already (swap and exit path);
                // close() is idempotent, this only covers a failed assertion.
                initialClient.close();
                IdleLinkClient live = reconnectedClient.get();
                if (live != null) {
                    live.close();
                }
            }
        });
    }

    /**
     * The window {@code isLinkUp()} cannot close on its own: the link check
     * reads the gate open, and the I/O thread starts its connect walk before
     * the stop's claim. {@code closeIfLinkUp()} must lose the gate to that
     * walk and refuse; stopping instead would wait out the shutdown budget on
     * a pull that ignores interrupts.
     */
    @Test(timeout = 30_000L)
    public void testCloseIfLinkUpRefusesWhenTheWalkStartsBehindItsLinkCheck() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final IdleLinkClient initialClient = new IdleLinkClient();
            final AtomicReference<IdleLinkClient> reconnectedClient = new AtomicReference<>();
            final CountDownLatch walkEntered = new CountDownLatch(1);
            final CountDownLatch releaseWalk = new CountDownLatch(1);
            final AtomicInteger interruptsSeenByWalk = new AtomicInteger();
            final CursorWebSocketSendLoop.ReconnectFactory factory = () -> {
                walkEntered.countDown();
                while (releaseWalk.getCount() != 0L) {
                    try {
                        releaseWalk.await();
                    } catch (InterruptedException e) {
                        interruptsSeenByWalk.incrementAndGet();
                    }
                }
                IdleLinkClient live = new IdleLinkClient();
                reconnectedClient.set(live);
                return live;
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
            // A stop that wrongly goes ahead waits on the blocked walk; keep that wait short.
            loop.setShutdownAwaitTimeoutMillis(1_000L);
            try {
                loop.start();
                Assert.assertTrue("precondition: the loop holds a live link", loop.isLinkUp());

                final AtomicBoolean walkStartedInsideLinkCheck = new AtomicBoolean();
                initialClient.onNextLinkCheckBy(Thread.currentThread(), () -> {
                    initialClient.drop();
                    try {
                        walkStartedInsideLinkCheck.set(walkEntered.await(5, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });

                boolean stopped = loop.closeIfLinkUp();
                Assert.assertTrue("the walk must have started inside the stop's own link check",
                        walkStartedInsideLinkCheck.get());
                Assert.assertFalse("the stop must lose the gate to a walk that started behind its link check",
                        stopped);
                Assert.assertTrue("a refused stop must leave the loop running", loop.isRunning());
                Assert.assertEquals("a refused stop must not cancel or interrupt the walk",
                        0, interruptsSeenByWalk.get());
            } finally {
                releaseWalk.countDown();
                loop.close();
                engine.close();
                initialClient.close();
                IdleLinkClient live = reconnectedClient.get();
                if (live != null) {
                    live.close();
                }
            }
        });
    }

    private static int getStaticInt(String name) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getInt(null);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = CursorWebSocketSendLoop.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }

    /**
     * An idle live link: reports connected and never delivers a frame until
     * {@link #drop()} (or a {@code closeTraffic()} from the loop's close),
     * after which its next receive fails the way a peer disconnect does.
     */
    private static final class IdleLinkClient extends WebSocketClient {
        private volatile boolean closed;
        private volatile boolean dropped;
        private volatile Runnable linkCheckHook;
        private volatile Thread linkCheckHookThread;

        private IdleLinkClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void close() {
            closed = true;
            super.close();
        }

        @Override
        public void closeTraffic() {
            dropped = true;
        }

        @Override
        public boolean isConnected() {
            Runnable hook = linkCheckHook;
            if (hook != null && Thread.currentThread() == linkCheckHookThread) {
                linkCheckHook = null;
                hook.run();
                // What a link check reads when the drop lands just behind it.
                return true;
            }
            return !closed;
        }

        @Override
        public boolean tryReceiveFrame(WebSocketFrameHandler handler) {
            if (dropped) {
                throw new LineSenderException("peer disconnect");
            }
            return false;
        }

        void drop() {
            dropped = true;
        }

        /** Runs {@code hook} inside the next {@code isConnected()} call {@code caller} makes; that call answers connected. */
        void onNextLinkCheckBy(Thread caller, Runnable hook) {
            linkCheckHookThread = caller;
            linkCheckHook = hook;
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
