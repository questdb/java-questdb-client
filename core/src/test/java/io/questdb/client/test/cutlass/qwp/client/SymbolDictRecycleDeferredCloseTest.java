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
 * {@code SlotLockContentionException} and would fail the recycle for what is
 * usually a transient disk stall. Exhausting the await budget does not latch
 * the sender terminal either: it throws to the triggering
 * caller and leaves the recycle pending in its {@code RecycleResume.REBUILD}
 * state, so each later send retries the await, and one of them finishes the
 * swap once the worker finally exits. Nothing on this path is terminal.
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

                        // Positive witness that the await really parked.
                        // A sleep could not tell "the await is parked" from
                        // "the close completed inline and the test skipped the
                        // code under test"; this fires from inside the await's
                        // own deferred-close branch.
                        CountDownLatch parked = new CountDownLatch(1);
                        ws.setDeferredCloseParkWitnessForTesting(parked::countDown);

                        // Un-wedges the worker while the recycle is parked in its
                        // deferred-close await. The pre-release assert can never
                        // race: the flock release needs the worker's exit, which
                        // needs this very countDown.
                        releaser = new Thread(() -> {
                            try {
                                Assert.assertTrue("the await must actually park",
                                        parked.await(10, TimeUnit.SECONDS));
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
     * A wedge that outlives the await budget (shrunk via the test seam): every
     * send while the worker is stuck runs the await afresh and throws again,
     * and none of them may commit any part of the swap (epoch stays 0). The
     * repeated throw is NOT a latch, which the tail proves: once the worker
     * exits, the still-locked engine's late release becomes visible through
     * {@code isSlotLockReleased()}'s retained-engine re-probe -- so the slot's
     * capacity is recoverable -- and the very next send resumes the pending
     * recycle and completes it. A latched sender could do neither.
     */
    @Test(timeout = 60_000L)
    public void testExhaustedDeferredCloseAwaitKeepsThrowingWhileWedged() throws Exception {
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
                            Assert.fail("expected the exhausted deferred-close await to throw "
                                    + "while the worker stays wedged");
                        } catch (LineSenderException e) {
                            TestUtils.assertContains(e.getMessage(),
                                    "deferred close did not release the slot lock");
                        }

                        // The await runs at step 3, before the step-5 swap: no
                        // epoch may have committed, and every later entry point
                        // re-runs the await and throws the same transient
                        // verdict for as long as the worker stays wedged.
                        Assert.assertEquals("the swap must not have committed",
                                0, ws.getSymbolDictEpoch());
                        try {
                            sender.table("t");
                            Assert.fail("expected the retried await to throw again");
                        } catch (LineSenderException e) {
                            TestUtils.assertContains(e.getMessage(),
                                    "deferred close did not release the slot lock");
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

                        // The proof that none of the throws above latched: the
                        // next send resumes the pending recycle and commits it.
                        sender.table("t").symbol("s", "c").longColumn("v", 3L).atNow();
                        Assert.assertEquals("the pending recycle must complete once the wedge "
                                + "clears", 1, ws.getSymbolDictEpoch());
                        long fsn2 = sender.flushAndGetSequence();
                        Assert.assertTrue("post-resume batch must still get acked",
                                sender.awaitAckedFsn(fsn2, 5_000));
                        Assert.assertTrue("post-recycle FSN must exceed pre-recycle FSN "
                                + "[fsn1=" + fsn1 + ", fsn2=" + fsn2 + ']', fsn2 > fsn1);
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

    /**
     * The outgoing engine's slot-lock listener is the sender's own
     * onSlotLockReleased. Left attached, a release that completes after step 6
     * (a preempted retry thread between closeCompleted = true and listener.run())
     * would mark the REBUILT engine's flock released. Step 3 detaches it.
     */
    @Test(timeout = 60_000L)
    public void testRecycleDetachesTheOutgoingEngineListener() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("recycle-detach-listener").toString();
            try (TestWebSocketServer server = ackingServer()) {
                String cfg = "ws::addr=localhost:" + server.getPort() + ";sf_dir=" + sfDir + ";";
                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    Assert.assertTrue(sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));

                    CursorSendEngine outgoing = ws.getCursorEngineForTesting();
                    Assert.assertNotNull("the live engine carries the sender's listener",
                            outgoing.getSlotLockReleaseListenerForTesting());

                    sender.resetSymbolDictionary();
                    sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                    Assert.assertEquals(1, ws.getSymbolDictEpoch());

                    Assert.assertNull("step 3 must detach the outgoing engine's listener",
                            outgoing.getSlotLockReleaseListenerForTesting());
                    Assert.assertNotNull("the rebuilt engine carries the listener instead",
                            ws.getCursorEngineForTesting().getSlotLockReleaseListenerForTesting());
                    Assert.assertFalse("the rebuilt engine holds the flock", ws.isSlotLockReleased());
                }
            }
        });
    }

    /**
     * The await budget runs out while the worker is still wedged, but the
     * wedge is transient after all. Exhausting the budget must NOT latch the
     * sender terminal: the recycle stays pending in its
     * REBUILD resume state, and once the worker exits and the deferred close
     * releases the flock, the next send finishes the await, rebuilds and
     * commits the swap.
     */
    @Test(timeout = 60_000L)
    public void testExhaustedDeferredCloseAwaitResumesOnNextSend() throws Exception {
        assertMemoryLeak(() -> {
            String sfDir = temporaryFolder.getRoot().toPath().resolve("recycle-deferred-resume").toString();
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
                        ws.setRecycleDeferredCloseMaxWaitMillisForTesting(100L);

                        sender.resetSymbolDictionary();
                        Assert.assertTrue(ws.isResetArmed());

                        // With the worker wedged past the tiny await budget, the
                        // triggering call must abandon -- not latch.
                        try {
                            sender.table("t").symbol("s", "b").longColumn("v", 2L).atNow();
                            Assert.fail("expected the exhausted deferred-close await to surface");
                        } catch (LineSenderException expected) {
                        }
                        Assert.assertEquals("swap must not have committed", 0, ws.getSymbolDictEpoch());
                        releaseWorker.countDown();
                        // Wait for the worker to exit and release the flock, then the
                        // next call resumes and completes the recycle.
                        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                        while (!outgoing.isCloseCompleted() && System.nanoTime() < deadlineNanos) {
                            Thread.sleep(10L);
                        }
                        Assert.assertTrue("deferred cleanup did not complete after the release",
                                outgoing.isCloseCompleted());
                        sender.table("t").symbol("s", "c").longColumn("v", 3L).atNow();
                        Assert.assertEquals(1, ws.getSymbolDictEpoch());

                        long fsn2 = sender.flushAndGetSequence();
                        Assert.assertTrue("post-resume batch must still get acked",
                                sender.awaitAckedFsn(fsn2, 5_000));
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
