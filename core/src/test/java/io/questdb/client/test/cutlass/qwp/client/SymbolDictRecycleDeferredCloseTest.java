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

import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentManager;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * The recycle's deferred-close discipline ({@code recycleForDictReset} step 3,
 * {@code awaitDeferredEngineClose}): when the SF worker is wedged in a syscall
 * past {@code SegmentManager}'s bounded join, the outgoing engine's close
 * returns with the slot flock retained and its release deferred to the
 * worker's exit path. The recycle must await that release before rebuilding on
 * the slot -- rebuilding against the retained flock throws
 * {@code SlotLockContentionException} and would latch the sender permanently
 * terminal for what is usually a transient disk stall. Only exhausting the
 * await budget (a genuinely dead worker) may latch terminal.
 * <p>
 * The wedge: {@code SegmentManager}'s trim-sync hook runs unconditionally once
 * per service pass, so parking the worker there leaves it un-joinable exactly
 * the way a stalled disk/NFS syscall does, and a shrunken
 * {@code workerJoinTimeoutMillis} lets the close's bounded join give up
 * promptly (the same recipe as {@code SlotLockReleasedContractTest}).
 */
public class SymbolDictRecycleDeferredCloseTest {

    @Rule
    public final TemporaryFolder temporaryFolder = TemporaryFolder.builder().assureDeletion().build();

    /**
     * A transient wedge: the worker un-wedges while the recycle is parked in
     * its deferred-close await. The recycle must ride the stall out and
     * complete -- fresh epoch, rebuilt engine, sender fully usable -- instead
     * of latching terminal on the retained flock.
     */
    @Test(timeout = 60_000L)
    public void testRecycleSurvivesDeferredEngineClose() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("recycle-deferred-survive").toString();
            try (TestWebSocketServer server = ackingServer()) {
                String cfg = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir + ";";
                CountDownLatch workerBlocked = new CountDownLatch(1);
                CountDownLatch releaseWorker = new CountDownLatch(1);
                AtomicBoolean wedgeFired = new AtomicBoolean();
                AtomicReference<Throwable> auxErr = new AtomicReference<>();
                Thread releaser = null;
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: batch must be acked before the recycle",
                            sender.awaitAckedFsn(fsn1, 5_000));

                    CursorSendEngine outgoing = ws.getCursorEngineForTesting();
                    SegmentManager manager = outgoing.getManagerForTesting();
                    try {
                        manager.setBeforeTrimSyncHook(() -> {
                            if (!wedgeFired.compareAndSet(false, true)) {
                                return;
                            }
                            workerBlocked.countDown();
                            try {
                                if (!releaseWorker.await(30, TimeUnit.SECONDS)) {
                                    auxErr.compareAndSet(null, new AssertionError(
                                            "timed out waiting for the test to release the worker"));
                                }
                            } catch (Throwable t) {
                                auxErr.compareAndSet(null, t);
                            }
                        });
                        manager.wakeWorker();
                        Assert.assertTrue("worker never reached the wedge hook",
                                workerBlocked.await(5, TimeUnit.SECONDS));
                        manager.setWorkerJoinTimeoutMillis(50L);

                        sender.resetSymbolDictionary();
                        Assert.assertTrue(ws.isResetArmed());

                        // Un-wedges the worker while the recycle is parked in its
                        // deferred-close await. The pre-release assert can never
                        // race: the flock release needs the worker's exit, which
                        // needs this very countDown.
                        releaser = new Thread(() -> {
                            try {
                                Thread.sleep(1_500L);
                                Assert.assertFalse(
                                        "deferred close cannot complete while the worker is wedged",
                                        outgoing.isCloseCompleted());
                            } catch (Throwable t) {
                                auxErr.compareAndSet(null, t);
                            } finally {
                                releaseWorker.countDown();
                            }
                        }, "deferred-close-releaser");
                        releaser.start();

                        // Triggers the recycle. Step 3's close cannot reap the
                        // wedged worker, so it returns with the slot flock
                        // retained; the bounded await must ride the wedge out
                        // instead of letting step 6 throw
                        // SlotLockContentionException and latch terminal.
                        sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();

                        Assert.assertEquals("the recycle must have committed",
                                1, ws.getSymbolDictEpoch());
                        Assert.assertFalse("recycle must disarm", ws.isResetArmed());
                        Assert.assertTrue("the outgoing engine's deferred close must have "
                                        + "completed before the rebuild",
                                outgoing.isCloseCompleted());
                        Assert.assertNotSame("the recycle must run on a rebuilt engine",
                                outgoing, ws.getCursorEngineForTesting());

                        sender.table("t").symbol("s", "c").longColumn("v", 3L).atNow();
                        long fsn2 = sender.flushAndGetSequence();
                        Assert.assertTrue("post-recycle batch must still get acked",
                                sender.awaitAckedFsn(fsn2, 5_000));
                        Assert.assertTrue("post-recycle FSN must exceed pre-recycle FSN "
                                + "[fsn1=" + fsn1 + ", fsn2=" + fsn2 + ']', fsn2 > fsn1);
                    } finally {
                        manager.setBeforeTrimSyncHook(null);
                        releaseWorker.countDown();
                    }
                } finally {
                    releaseWorker.countDown();
                    if (releaser != null) {
                        releaser.join(10_000L);
                    }
                }
                if (auxErr.get() != null) {
                    throw new AssertionError("auxiliary thread failed", auxErr.get());
                }
            }
        });
    }

    /**
     * A permanent wedge: the await budget (shrunk via the test seam) runs out
     * with the flock still held -- a genuinely dead worker. The recycle must
     * latch terminal BEFORE committing any of the swap (epoch stays 0), and
     * the still-locked engine must stay reachable through
     * {@code isSlotLockReleased()}'s re-probe so the slot's capacity is
     * recoverable if the worker ever exits.
     */
    @Test(timeout = 60_000L)
    public void testRecycleLatchesTerminalWhenDeferredCloseNeverReleases() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("recycle-deferred-timeout").toString();
            try (TestWebSocketServer server = ackingServer()) {
                String cfg = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir + ";";
                CountDownLatch workerBlocked = new CountDownLatch(1);
                CountDownLatch releaseWorker = new CountDownLatch(1);
                AtomicBoolean wedgeFired = new AtomicBoolean();
                AtomicReference<Throwable> auxErr = new AtomicReference<>();
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: batch must be acked before the recycle",
                            sender.awaitAckedFsn(fsn1, 5_000));

                    CursorSendEngine outgoing = ws.getCursorEngineForTesting();
                    SegmentManager manager = outgoing.getManagerForTesting();
                    try {
                        manager.setBeforeTrimSyncHook(() -> {
                            if (!wedgeFired.compareAndSet(false, true)) {
                                return;
                            }
                            workerBlocked.countDown();
                            try {
                                if (!releaseWorker.await(30, TimeUnit.SECONDS)) {
                                    auxErr.compareAndSet(null, new AssertionError(
                                            "timed out waiting for the test to release the worker"));
                                }
                            } catch (Throwable t) {
                                auxErr.compareAndSet(null, t);
                            }
                        });
                        manager.wakeWorker();
                        Assert.assertTrue("worker never reached the wedge hook",
                                workerBlocked.await(5, TimeUnit.SECONDS));
                        manager.setWorkerJoinTimeoutMillis(50L);
                        ws.setRecycleDeferredCloseMaxWaitMillisForTesting(150L);

                        sender.resetSymbolDictionary();
                        Assert.assertTrue(ws.isResetArmed());

                        try {
                            sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                            Assert.fail("expected the recycle to latch terminal once the "
                                    + "deferred-close await budget ran out");
                        } catch (LineSenderException e) {
                            TestUtils.assertContains(e.getMessage(),
                                    "deferred close did not release the slot lock");
                        }

                        // The await runs at step 3, before the step-5 swap: no
                        // epoch may have committed, and every later entry point
                        // must rethrow the latched failure.
                        Assert.assertEquals("the swap must not have committed",
                                0, ws.getSymbolDictEpoch());
                        try {
                            sender.table("t");
                            Assert.fail("expected a latched terminal sender to rethrow");
                        } catch (LineSenderException expected) {
                        }
                        Assert.assertFalse("the slot flock is still held by the wedged engine",
                                ws.isSlotLockReleased());

                        // The worker finally exits: the deferred cleanup must
                        // complete and the sender must expose the late release
                        // (the retained-engine re-probe), so a pool can recover
                        // the slot's capacity.
                        releaseWorker.countDown();
                        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        while (!outgoing.isCloseCompleted() && System.nanoTime() < deadlineNanos) {
                            Thread.sleep(10L);
                        }
                        Assert.assertTrue("deferred cleanup did not complete after the release",
                                outgoing.isCloseCompleted());
                        Assert.assertTrue("sender must expose the late flock release",
                                ws.isSlotLockReleased());
                    } finally {
                        manager.setBeforeTrimSyncHook(null);
                        releaseWorker.countDown();
                    }
                }
                if (auxErr.get() != null) {
                    throw new AssertionError("auxiliary thread failed", auxErr.get());
                }
            }
        });
    }

    private static TestWebSocketServer ackingServer() throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(new AckAllHandler());
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    /** ACKs every frame it receives; does not otherwise inspect the wire. */
    private static class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
