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
import io.questdb.client.cutlass.qwp.client.sf.cursor.BackgroundDrainer;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorWebSocketSendLoop;
import io.questdb.client.network.PlainSocketFactory;
import io.questdb.client.std.Compat;
import io.questdb.client.std.Files;
import io.questdb.client.std.MemoryTag;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.tools.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Red test for the SEGV half of finding C5 — interrupted drainer teardown
 * must not unmap the engine under a live I/O thread.
 * <p>
 * Production sequence: during an outage the drainer's loop I/O thread sits
 * inside a blocking native connect ({@code connect_timeout} defaults to 0 =
 * OS timeout; neither unpark nor interrupt cancels {@code connect(2)}).
 * {@code BackgroundDrainerPool.close()} escalates — graceful drain,
 * {@code requestStop()}, then {@code shutdownNow()} — and the interrupt
 * lands in {@code loop.close()}'s {@code shutdownLatch.await()}. Pre-fix,
 * {@code close()} swallows it and returns while the I/O thread is alive;
 * {@code BackgroundDrainer.run()}'s finally then closes the engine —
 * {@code munmap}/{@code Unsafe.free} on segment memory a thread that is
 * still alive may touch with raw {@code Unsafe} reads.
 * <p>
 * The invariant pinned here: <b>at the moment {@code run()} returns, NOT
 * (loop I/O thread alive AND slot lock released)</b>. The slot lock is an
 * on-disk protocol shared with other processes and scanners, and
 * {@code engine.close()} releases it strictly after unmapping — so
 * "lock released" is the public, behavioral witness of "engine torn down".
 * Either valid fix shape satisfies the invariant: block until the thread
 * exits (re-await), or keep the lock/engine alive past {@code run()} by
 * delegating engine teardown to the I/O thread's exit path. The tail of the
 * test additionally requires that the slot lock is EVENTUALLY released once
 * the stuck connect resolves — a fix may defer teardown, not abandon it.
 * <p>
 * NOTE: this test is a proxy for the memory-safety property ("no engine
 * access after unmap"), which cannot be asserted in-process — a SEGV kills
 * the JVM, and {@code Unsafe.free}'d memory is not guaranteed to fault. The
 * invariant is a sufficient teardown discipline, deliberately stricter than
 * the minimal property; see the C5 review discussion.
 * <p>
 * Determinism: no sleeps. The interrupt is delivered after
 * {@code requestStop()} while the runner is either in an interrupt-immune
 * park ({@code LockSupport.parkNanos} preserves the flag) or already in the
 * latch await — both routes arrive at the await with the flag set, which
 * throws before parking. The "stuck connect" is a latch-gated factory
 * (unpark-immune; {@code close()} never interrupts the I/O thread). The
 * test only runs safely on pre-fix code because the already-landed
 * discard-when-stopped fix keeps the post-teardown I/O thread away from
 * engine memory — the hazard this test guards is real on any HEAD without
 * that commit.
 */
public class BackgroundDrainerInterruptedTeardownTest {

    private static final long SEGMENT_BYTES = 64 * 1024;
    private String tmpDir;

    @Before
    public void setUp() {
        tmpDir = Paths.get(System.getProperty("java.io.tmpdir"),
                "qdb-c5-teardown-" + System.nanoTime()).toString();
        Assert.assertEquals(0, Files.mkdir(tmpDir, Files.DIR_MODE_DEFAULT));
    }

    @After
    public void tearDown() {
        if (tmpDir == null) return;
        long find = Files.findFirst(tmpDir);
        if (find > 0) {
            try {
                int rc = 1;
                while (rc > 0) {
                    String name = Files.utf8ToString(Files.findName(find));
                    if (name != null && !".".equals(name) && !"..".equals(name)) {
                        Files.remove(tmpDir + "/" + name);
                    }
                    rc = Files.findNext(find);
                }
            } finally {
                Files.findClose(find);
            }
        }
        Files.remove(tmpDir);
    }

    @Test
    public void testC5_interruptedTeardownMustNotReleaseSlotUnderLiveIoThread() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // 1. Slot with one published, unacked frame so the drainer opens
            //    a real engine and spins up a send loop.
            long buf = Unsafe.malloc(16, MemoryTag.NATIVE_DEFAULT);
            try {
                CursorSendEngine prep = new CursorSendEngine(tmpDir, SEGMENT_BYTES);
                try {
                    for (int i = 0; i < 16; i++) {
                        Unsafe.getUnsafe().putByte(buf + i, (byte) i);
                    }
                    Assert.assertEquals(0L, prep.appendBlocking(buf, 16));
                } finally {
                    // Unacked data on disk -> close() keeps the .sfa files.
                    prep.close();
                }
            } finally {
                Unsafe.free(buf, 16, MemoryTag.NATIVE_DEFAULT);
            }

            final CountDownLatch enteredReconnect = new CountDownLatch(1);
            final CountDownLatch releaseConnect = new CountDownLatch(1);
            final AtomicInteger connects = new AtomicInteger();
            final AtomicReference<Thread> ioThreadRef = new AtomicReference<>();
            // Client #1: initial connect succeeds (on the runner thread), but
            // every send throws -- driving the I/O thread into its reconnect
            // loop. Client #2: handed back by the gated "connect" after the
            // teardown has already run.
            final StubWebSocketClient wireDownClient = new StubWebSocketClient();
            final StubWebSocketClient postTeardownClient = new StubWebSocketClient();

            final CursorWebSocketSendLoop.ReconnectFactory factory = () -> {
                if (connects.incrementAndGet() == 1) {
                    // Initial connect: runs on the drainer's runner thread.
                    return wireDownClient;
                }
                // Wire-failure reconnect: runs on the loop's I/O thread.
                // Stand-in for a blocking native connect(2): unpark-immune
                // (a latch await re-parks after a spurious wake) and never
                // interrupted (loop.close() only unparks).
                ioThreadRef.set(Thread.currentThread());
                enteredReconnect.countDown();
                releaseConnect.await();
                return postTeardownClient;
            };

            final BackgroundDrainer drainer = new BackgroundDrainer(
                    tmpDir, SEGMENT_BYTES, Long.MAX_VALUE, factory,
                    5_000L, 10L, 50L, false, 0L);

            Thread runner = new Thread(drainer::run, "drainer-runner");
            runner.setDaemon(true);
            runner.start();
            try {
                Assert.assertTrue("I/O thread never reached the reconnect factory",
                        enteredReconnect.await(10, TimeUnit.SECONDS));

                // Pool-shutdown stand-in: requestStop, then the shutdownNow
                // interrupt. Wherever the runner is at this instant -- the
                // poll park (flag-preserving) or already in the latch await --
                // the flag arrives at the await and throws before parking.
                drainer.requestStop();
                runner.interrupt();
                runner.join(10_000L);
                Assert.assertFalse("drainer did not return after stop + interrupt",
                        runner.isAlive());
                Assert.assertEquals(BackgroundDrainer.DrainOutcome.STOPPED,
                        drainer.outcome());

                Thread ioThread = ioThreadRef.get();
                Assert.assertNotNull(ioThread);
                boolean ioThreadAliveAtReturn = ioThread.isAlive();
                boolean slotLockFreeAtReturn = isSlotLockFree();
                Assert.assertFalse(
                        "C5 (SEGV): BackgroundDrainer.run() returned with the slot lock "
                                + "released (engine closed -- segments munmap'd/freed) while "
                                + "the loop's I/O thread was still alive inside a blocking "
                                + "connect. loop.close() swallowed the InterruptedException "
                                + "from shutdownLatch.await() and returned; the finally then "
                                + "unmapped memory a live thread may touch with raw Unsafe "
                                + "reads. Teardown must either wait for the thread or be "
                                + "delegated to its exit path.",
                        ioThreadAliveAtReturn && slotLockFreeAtReturn);
            } finally {
                // Unblock the "connect" and quiesce regardless of verdict so
                // the memory-leak wrapper sees a fully wound-down world.
                releaseConnect.countDown();
                Thread ioThread = ioThreadRef.get();
                if (ioThread != null) {
                    ioThread.join(10_000L);
                    Assert.assertFalse("I/O thread did not exit after the connect returned",
                            ioThread.isAlive());
                }
                wireDownClient.close();
                postTeardownClient.close();
            }

            // Deferred is fine; abandoned is not: once the stuck connect
            // resolved and the I/O thread exited, the slot lock must be
            // released (engine closed by whoever ended up owning teardown),
            // or no scanner can ever adopt the slot's remaining data.
            long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!isSlotLockFree()) {
                Assert.assertTrue(
                        "slot lock never released after the I/O thread exited -- "
                                + "engine teardown was abandoned, not deferred",
                        System.nanoTime() < deadlineNanos);
                Compat.onSpinWait();
            }
        });
    }

    /**
     * Public, behavioral probe of the slot lock: opening an engine on the
     * slot succeeds iff no other engine holds the on-disk lock. The probe
     * engine is closed immediately; the slot's unacked data keeps its files
     * on disk, so probing is observation-only.
     */
    private boolean isSlotLockFree() {
        try {
            new CursorSendEngine(tmpDir, SEGMENT_BYTES).close();
            return true;
        } catch (IllegalStateException e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("already in use")) {
                return false;
            }
            throw e;
        }
    }

    /**
     * Minimal concrete {@link WebSocketClient}: connect-level collaborator
     * only. Every send throws, so handing it to a live loop deterministically
     * drives the I/O thread into its reconnect path without native I/O.
     */
    private static final class StubWebSocketClient extends WebSocketClient {
        StubWebSocketClient() {
            super(DefaultHttpClientConfiguration.INSTANCE, PlainSocketFactory.INSTANCE);
        }

        @Override
        public void sendBinary(long dataPtr, int length) {
            throw new IllegalStateException("stub: wire down");
        }

        @Override
        public void sendBinary(long dataPtr, int length, int timeout) {
            throw new IllegalStateException("stub: wire down");
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
