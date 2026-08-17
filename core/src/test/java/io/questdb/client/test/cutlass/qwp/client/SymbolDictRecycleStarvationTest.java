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
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.cutlass.qwp.protocol.QwpConstants.FLAG_DEFER_COMMIT;
import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * The bounded blocking wait {@code maybeBlockForStarvedReset()} runs from
 * {@code maybeRecycleForDictReset()} when a symbol-dictionary recycle is
 * armed but the ring is not yet drained: it opportunistically waits (parked,
 * {@code awaitAckedFsn}-shaped) for the outstanding acks to arrive, up to
 * {@code symbol_dict_reset_max_wait_millis}, before giving up for this armed
 * window. {@code resetMaxWaitMillis <= 0} disables the wait entirely (never
 * block); a deferred-commit group open at the time of the check must never
 * be waited on, since the server withholds its acks by design until the
 * closing commit lands (the one data-loss-adjacent path in this feature --
 * forcing a wait there would just stall until the deadline, since only THIS
 * thread can ever send the commit that unblocks it).
 */
public class SymbolDictRecycleStarvationTest {

    /**
     * {@code symbol_dict_reset_max_wait_millis=0} must disable the wait
     * unconditionally, regardless of how long the recycle has been armed or
     * how large the backlog is. Every {@code table()} call must return in
     * (near) constant time, the sender must never recycle, and it must stay
     * armed forever -- nothing ever consumes the arming.
     */
    @Test
    public void testMaxWaitZeroNeverBlocks() throws Exception {
        assertMemoryLeak(() -> {
            GatedAckHandler handler = new GatedAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                String cfg = "ws::addr=localhost:" + port
                        + ";symbol_dict_reset_threshold=2"
                        + ";symbol_dict_reset_max_wait_millis=0;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    sender.flush(); // unacked forever -- handler never releases
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());
                    Assert.assertEquals(0L, ws.getSymbolDictResetStarvationTimeoutsForTest());

                    // Repeated table() calls, spread over time, must every one of
                    // them return fast: resetMaxWaitMillis<=0 is checked BEFORE the
                    // armed-window elapsed check, so it never even looks at how long
                    // this arm has been outstanding.
                    for (int i = 0; i < 5; i++) {
                        long t0 = System.nanoTime();
                        sender.table("t");
                        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
                        Assert.assertTrue("table() call #" + i + " took " + elapsedMs
                                        + "ms -- resetMaxWaitMillis=0 must never block",
                                elapsedMs < 100);
                        Thread.sleep(20);
                    }

                    Assert.assertTrue("armed forever -- nothing ever consumes it with no factory "
                                    + "action taken",
                            ws.isResetArmed());
                    Assert.assertEquals("must never recycle -- the ring never drained and "
                                    + "blocking is disabled",
                            0L, ws.getSymbolDictEpochForTest());
                    Assert.assertEquals(0L, ws.getSymbolDictResetStarvationTimeoutsForTest());

                    // Release before the try-with-resources closes the sender below,
                    // or close()'s own drain would hang on this still-unacked batch --
                    // irrelevant to what this test is proving.
                    handler.releaseAcks();
                }
            }
        });
    }

    /**
     * With a non-zero max wait, a {@code table()} call made after the armed
     * window has elapsed must block (parked) until the backlog drains, then
     * recycle synchronously within that same call -- proving the wait
     * actually parks rather than spinning or returning early, and that a
     * drain arriving mid-wait is observed without needing a fresh
     * {@code table()} call.
     */
    @Test
    public void testBlocksThenRecyclesWhenAcksArrive() throws Exception {
        assertMemoryLeak(() -> {
            GatedAckHandler handler = new GatedAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                // Generous relative to releaseDelayMs below: the recycle itself
                // (I/O loop join, engine close, rebuild, fresh handshake) needs
                // headroom on top of the release delay, or the elapsedMs<maxWaitMillis
                // assertion below is flake-prone on a loaded machine.
                long maxWaitMillis = 700;
                String cfg = "ws::addr=localhost:" + port
                        + ";symbol_dict_reset_threshold=2"
                        + ";symbol_dict_reset_max_wait_millis=" + maxWaitMillis + ";";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    sender.flush();
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());

                    // Let the armed-window guard elapse (armedSinceNanos check uses
                    // the SAME resetMaxWaitMillis) so the triggering table() call
                    // actually enters the blocking wait -- with a FRESH maxWaitMillis
                    // deadline of its own -- instead of returning immediately.
                    Thread.sleep(maxWaitMillis + 50);

                    // Release the acks from another thread, well before the fresh
                    // maxWaitMillis deadline elapses, while the main thread is
                    // parked in the wait loop.
                    long releaseDelayMs = 150;
                    Thread releaser = new Thread(() -> {
                        try {
                            Thread.sleep(releaseDelayMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        handler.releaseAcks();
                    });
                    releaser.start();

                    long elapsedMs;
                    try {
                        long t0 = System.nanoTime();
                        sender.table("t"); // blocks, then recycles once the ack lands
                        elapsedMs = (System.nanoTime() - t0) / 1_000_000;
                    } finally {
                        // Join even if table() throws unexpectedly, so an assertion
                        // failure below never leaks a non-daemon thread.
                        releaser.join();
                    }

                    Assert.assertTrue("must have actually blocked for roughly the release delay "
                                    + "(" + releaseDelayMs + "ms), got " + elapsedMs + "ms",
                            elapsedMs >= releaseDelayMs - 50);
                    Assert.assertTrue("must not have run into the full max-wait deadline "
                                    + "(" + maxWaitMillis + "ms), got " + elapsedMs + "ms",
                            elapsedMs < maxWaitMillis);
                    Assert.assertFalse("recycle must disarm", ws.isResetArmed());
                    Assert.assertEquals("recycle must have run exactly once",
                            1L, ws.getSymbolDictEpochForTest());
                    Assert.assertEquals("a successful drain-and-recycle is not a timeout",
                            0L, ws.getSymbolDictResetStarvationTimeoutsForTest());
                }
            }
        });
    }

    /**
     * Acks never arrive at all: the blocked {@code table()} call must give up
     * at (roughly) {@code maxWaitMillis}, not hang indefinitely, and the
     * sender must keep working afterwards -- ingest is not stuck. The
     * starvation counter records the timeout, the recycle stays armed
     * (nothing consumed it), and at most one blocking wait happens per armed
     * window: an immediately-following table() call must NOT re-block. Once
     * the backlog does drain later (acks release), the still-armed recycle
     * fires on the next table() call -- opportunistically, without another
     * blocking wait.
     */
    @Test
    public void testTimeoutLogsAndReArms() throws Exception {
        assertMemoryLeak(() -> {
            GatedAckHandler handler = new GatedAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                long maxWaitMillis = 300;
                String cfg = "ws::addr=localhost:" + port
                        + ";symbol_dict_reset_threshold=2"
                        + ";symbol_dict_reset_max_wait_millis=" + maxWaitMillis + ";";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());

                    // Let the armed-window guard elapse so the call below actually
                    // enters the wait instead of short-circuiting on arm recency.
                    Thread.sleep(maxWaitMillis + 50);

                    long t0 = System.nanoTime();
                    sender.table("t"); // acks never come -- must give up at ~maxWaitMillis
                    long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

                    Assert.assertTrue("must have waited out roughly the full max-wait deadline "
                                    + "(" + maxWaitMillis + "ms), got " + elapsedMs + "ms",
                            elapsedMs >= maxWaitMillis - 50);
                    Assert.assertTrue("must not hang far past its own deadline, got " + elapsedMs + "ms",
                            elapsedMs < maxWaitMillis + 5_000);
                    Assert.assertEquals("a timed-out wait must record exactly one starvation timeout",
                            1L, ws.getSymbolDictResetStarvationTimeoutsForTest());
                    Assert.assertTrue("timing out must not consume the arming -- the recycle is "
                                    + "still owed once the backlog eventually drains",
                            ws.isResetArmed());
                    Assert.assertEquals("no recycle happened -- only the wait gave up",
                            0L, ws.getSymbolDictEpochForTest());

                    // At most one blocking wait per armed window: pendingRowCount is
                    // still 0 here (nothing added since the flush above) and the ring
                    // is still undrained, so this table() call reaches
                    // maybeBlockForStarvedReset() again -- but starvationWaitDoneThisArm
                    // is already set from the call above, so it must NOT re-block. (A
                    // probe placed after a pending row would short-circuit on the
                    // pendingRowCount!=0 guard in maybeRecycleForDictReset() before ever
                    // reaching the wait, making the "must not re-block" assertion true
                    // for the wrong reason.)
                    long t1 = System.nanoTime();
                    sender.table("t");
                    long secondElapsedMs = (System.nanoTime() - t1) / 1_000_000;
                    Assert.assertTrue("a second table() call in the same armed window must not "
                                    + "re-block, took " + secondElapsedMs + "ms",
                            secondElapsedMs < 100);
                    Assert.assertEquals("still just the one timeout from before",
                            1L, ws.getSymbolDictResetStarvationTimeoutsForTest());

                    // Ingest continues: more rows can still be appended and flushed
                    // without the sender getting stuck.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();

                    // Now let the backlog actually drain: the still-armed recycle
                    // must fire opportunistically on the next table() call, with no
                    // further blocking wait needed (isRingDrained() short-circuits
                    // straight to recycleForDictReset()). drain() uses watermark
                    // semantics -- it waits for publishedFsn(), covering BOTH the
                    // original arm batch and the ingest-continues row above it, not
                    // just fsn1's frame.
                    handler.releaseAcks();
                    Assert.assertTrue("setup: every pending frame must get acked once released",
                            sender.drain(5_000));
                    Assert.assertTrue("setup: the original arm batch must be covered too",
                            sender.awaitAckedFsn(fsn1, 0));

                    sender.table("t");
                    Assert.assertFalse("the still-armed recycle must fire now that the backlog "
                                    + "has drained",
                            ws.isResetArmed());
                    Assert.assertEquals(1L, ws.getSymbolDictEpochForTest());
                    Assert.assertEquals("draining later must not add another timeout",
                            1L, ws.getSymbolDictResetStarvationTimeoutsForTest());
                }
            }
        });
    }

    /**
     * A terminal error latched WHILE the wait is parked must interrupt it
     * immediately -- the wait polls {@code cursorSendLoop.checkError()} /
     * {@code checkConnectionError()} every park interval, exactly like
     * {@code awaitAckedFsn}. Forces the poison detector to escalate to a
     * terminal on the very first NACK ({@code max_frame_rejections=1},
     * {@code poison_min_escalation_window_millis=0}) so the timing is
     * deterministic instead of depending on the (much slower) defaults.
     */
    @Test
    public void testLatchedErrorDuringWaitThrows() throws Exception {
        assertMemoryLeak(() -> {
            GatedThenPoisonHandler handler = new GatedThenPoisonHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                long maxWaitMillis = 400;
                String cfg = "ws::addr=localhost:" + port
                        + ";symbol_dict_reset_threshold=2"
                        + ";symbol_dict_reset_max_wait_millis=" + maxWaitMillis
                        + ";max_frame_rejections=1;poison_min_escalation_window_millis=0;";

                Sender sender = Sender.fromConfig(cfg);
                try {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    sender.flush();
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());

                    // Let the armed-window guard elapse (armedSinceNanos check uses
                    // the SAME resetMaxWaitMillis) so the triggering table() call
                    // below actually enters the blocking wait with a FRESH deadline.
                    Thread.sleep(maxWaitMillis + 50);

                    long poisonDelayMs = 150;
                    Thread poisoner = new Thread(() -> {
                        try {
                            Thread.sleep(poisonDelayMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        handler.poison();
                    });
                    poisoner.start();

                    LineSenderException thrown = null;
                    long elapsedMs;
                    try {
                        long t0 = System.nanoTime();
                        try {
                            sender.table("t");
                        } catch (LineSenderException e) {
                            thrown = e;
                        }
                        elapsedMs = (System.nanoTime() - t0) / 1_000_000;
                    } finally {
                        // Join even if something unexpected escapes above, so an
                        // assertion failure never leaks a non-daemon thread.
                        poisoner.join();
                    }

                    Assert.assertNotNull("a terminal error latched during the wait must propagate "
                                    + "out of table(), not be swallowed into an indefinite hang",
                            thrown);
                    Assert.assertTrue("must have thrown promptly once poisoned, not waited out the "
                                    + "full max-wait deadline (" + maxWaitMillis + "ms), got "
                                    + elapsedMs + "ms",
                            elapsedMs < maxWaitMillis);
                    Assert.assertTrue("the recycle must never have run -- the swap requires a "
                                    + "healthy connection",
                            ws.isResetArmed());
                    Assert.assertEquals("a thrown wait is not a timeout",
                            0L, ws.getSymbolDictResetStarvationTimeoutsForTest());
                } finally {
                    try {
                        sender.close();
                    } catch (LineSenderException ignored) {
                        // close() may also observe the latched terminal -- irrelevant here
                    }
                }
            }
        });
    }

    /**
     * A deferred-commit group open past the armed window is the one
     * data-safety-critical case: the server withholds acks for
     * {@code FLAG_DEFER_COMMIT} frames by design, and this producer thread is
     * the only one that can ever send the closing commit. Blocking here would
     * therefore ALWAYS run out the clock -- worse, it would do so while
     * holding up the very thread the caller needs free to actually close the
     * group. {@code table()} must return immediately (the futility guard),
     * and once the group is closed and its commit acked, the still-armed
     * recycle must fire at the very next drained {@code table()} call.
     */
    @Test
    public void testDeferredCommitGroupSkipsWait() throws Exception {
        assertMemoryLeak(() -> {
            DeferAwareAckHandler handler = new DeferAwareAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                long maxWaitMillis = 300;
                String cfg = "ws::addr=localhost:" + port
                        + ";symbol_dict_reset_threshold=2"
                        + ";symbol_dict_reset_max_wait_millis=" + maxWaitMillis + ";";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    ws.setDeferCommit(true);

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    sender.flush(); // deferred frame -- server withholds its ack
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());

                    // Let the armed window elapse -- without the futility guard this
                    // is exactly when a naive implementation would start blocking.
                    Thread.sleep(maxWaitMillis + 50);

                    long t0 = System.nanoTime();
                    sender.table("t"); // futility guard: must return immediately
                    long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

                    Assert.assertTrue("an open deferred-commit group must never be waited on "
                                    + "(the server withholds its ack by design), took " + elapsedMs + "ms",
                            elapsedMs < 100);
                    Assert.assertTrue("must still be armed -- neither the wait nor the recycle ran",
                            ws.isResetArmed());
                    Assert.assertEquals("the futility guard is not a timeout",
                            0L, ws.getSymbolDictResetStarvationTimeoutsForTest());
                    Assert.assertEquals(0L, ws.getSymbolDictEpochForTest());

                    // Close the deferred group: commit, and wait for its ack.
                    ws.setDeferCommit(false);
                    long commitFsn = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: the commit must get acked",
                            sender.awaitAckedFsn(commitFsn, 5_000));

                    // The ring is now drained -- the still-armed recycle must fire at
                    // the very next drained row start, with no wait needed at all.
                    sender.table("t");
                    Assert.assertFalse("the still-armed recycle must fire once the group is "
                                    + "committed and acked",
                            ws.isResetArmed());
                    Assert.assertEquals(1L, ws.getSymbolDictEpochForTest());
                    Assert.assertEquals("no wait ever ran in this test",
                            0L, ws.getSymbolDictResetStarvationTimeoutsForTest());
                }
            }
        });
    }

    /**
     * Receives frames but withholds every ack until {@link #releaseAcks()} is
     * called, so a starvation wait provably has an unacknowledged target to
     * wait on. Mirrors {@code CloseDrainTest.GatedAckHandler} /
     * {@code SymbolDictRecycleFsnContinuityTest.GatedAckHandler}.
     */
    private static class GatedAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);
        private final CountDownLatch released = new CountDownLatch(1);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                if (!released.await(20, TimeUnit.SECONDS)) {
                    throw new AssertionError("starvation-wait witness never released the ack gate");
                }
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        void releaseAcks() {
            released.countDown();
        }
    }

    /**
     * Withholds its response until {@link #poison()} is called, then replies
     * with a terminal-worthy NACK (STATUS_PARSE_ERROR) instead of an ack --
     * models a connection killed while a starvation wait is parked.
     */
    private static class GatedThenPoisonHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);
        private final CountDownLatch poisoned = new CountDownLatch(1);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                if (!poisoned.await(20, TimeUnit.SECONDS)) {
                    throw new AssertionError("starvation-wait witness never poisoned the connection");
                }
                client.sendBinary(QwpWireTestUtils.buildNack(
                        nextSeq.getAndIncrement(), WebSocketResponse.STATUS_PARSE_ERROR));
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        void poison() {
            poisoned.countDown();
        }
    }

    /**
     * Acks every non-deferred frame immediately (each connection's wire
     * sequence restarts at 0), but withholds acks for any frame carrying
     * {@code FLAG_DEFER_COMMIT} -- exactly the real server's ack contract for
     * an open deferred-commit group (see
     * {@code CloseDrainTest.AckFirstFrameOnlyHandler}, which models the same
     * contract for a fixed one-committed-then-deferred shape). Acking the
     * closing commit frame's wire sequence retroactively covers the whole
     * group, since {@code ackedFsn} is a cumulative watermark.
     */
    private static class DeferAwareAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            long seq = nextSeq.getAndIncrement();
            boolean deferred = data.length > 5 && (data[5] & FLAG_DEFER_COMMIT) != 0;
            if (deferred) {
                return; // withhold the ack -- the group is still open
            }
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(seq));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
