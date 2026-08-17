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
import io.questdb.client.SenderError;
import io.questdb.client.SenderProgressHandler;
import io.questdb.client.LineSenderServerException;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * FSN epoch-base continuity across a symbol-dict recycle. Task 5's engine
 * rebuild restarts the internal cursor engine's raw FSNs at 0; every
 * user-visible FSN surface must stay strictly monotone across that boundary
 * by translating {@code external = fsnEpochBase + raw} (negative sentinels
 * pass untranslated). The recycle machinery itself lands in a later task --
 * these tests exercise the translation seam ({@code rollFsnEpochBaseForTest})
 * directly.
 * <p>
 * Every test that rolls the base does so on a sender BEFORE its first connect
 * (via {@link #createRolledSender}), never on an already-connected one:
 * {@code rollFsnEpochBase}'s precondition forbids rolling while a live
 * {@code CursorWebSocketSendLoop} is attached (its {@code externalFsnBase} is a
 * construction-time snapshot, never updated on a live loop -- see that method's
 * javadoc). Tests that need a realistic pre-roll FSN to roll by first drive a
 * SEPARATE, ordinarily-connected sender against the same server to publish and
 * ack a batch, close it, then hand that FSN to {@code createRolledSender} for a
 * second, fresh sender/engine -- modelling the post-recycle engine that
 * restarts its raw FSNs at 0.
 */
public class SymbolDictRecycleFsnContinuityTest {

    /**
     * Rolls a FRESH sender/engine (never published-to raw watermark starts at -1), not
     * the already-connected one that produced {@code fsn1}: {@code rollFsnEpochBase}'s
     * precondition forbids rolling while a live loop is attached (see its javadoc), and
     * -- independent of that -- an already-connected sender's engine keeps its raw
     * watermark across the roll, which would make this test pass even with the
     * translation deleted (raw {@code ackedFsn() == fsn1 >= fsn1} regardless of any
     * epoch math). Only a genuinely fresh engine (raw {@code ackedFsn() == -1}) makes
     * the pre-roll short-circuit the ONLY way {@code awaitAckedFsn(fsn1, 0)} can return
     * true here.
     */
    @Test
    public void testPreRollTargetAnswersTrueAfterRoll() throws Exception {
        try (TestWebSocketServer server = ackingServer()) {
            long fsn1;
            try (QwpWebSocketSender sender1 = (QwpWebSocketSender) Sender.fromConfig(cfg(server))) {
                sender1.table("t").longColumn("v", 1L).atNow();
                fsn1 = sender1.flushAndGetSequence();
                Assert.assertTrue("setup: the batch must actually be acked before the roll",
                        sender1.drain(5_000));
            }

            QwpWebSocketSender sender2 = createRolledSender(server, fsn1);
            try {
                long t0 = System.nanoTime();
                Assert.assertTrue("a target FSN from a pre-recycle epoch must be reported acked "
                                + "immediately -- it was proven acked before the swap",
                        sender2.awaitAckedFsn(fsn1, 0));
                long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
                Assert.assertTrue("must short-circuit, not poll: took " + elapsedMs + "ms",
                        elapsedMs < 200);
            } finally {
                sender2.close();
            }
        }
    }

    @Test
    public void testPostRollSequencesExceedAllPreRoll() throws Exception {
        try (TestWebSocketServer server = ackingServer()) {
            long fsn1;
            try (QwpWebSocketSender sender1 = (QwpWebSocketSender) Sender.fromConfig(cfg(server))) {
                sender1.table("t").longColumn("v", 1L).atNow();
                fsn1 = sender1.flushAndGetSequence();
                Assert.assertTrue(sender1.drain(5_000));
            }

            QwpWebSocketSender sender2 = createRolledSender(server, fsn1);
            try {
                long newBase = sender2.getFsnEpochBaseForTest();
                Assert.assertEquals(fsn1 + 1, newBase);

                sender2.table("t").longColumn("v", 2L).atNow();
                long fsn2 = sender2.flushAndGetSequence();
                Assert.assertTrue(sender2.drain(5_000));

                Assert.assertTrue("post-roll FSN must exceed every pre-roll FSN: fsn2=" + fsn2
                                + " fsn1=" + fsn1,
                        fsn2 > fsn1);
                // sender2's engine is genuinely fresh (raw publishedFsn() starts at -1), so
                // its first-ever flush publishes raw 0. The exact-equality check is strictly
                // stronger than ">" alone: it also catches an off-by-one in the roll formula
                // (e.g. fsnEpochBase += lastPublishedFsn instead of + 1L), which the ">"
                // check above would not.
                Assert.assertEquals(newBase, fsn2);
            } finally {
                sender2.close();
            }
        }
    }

    @Test
    public void testGetAckedFsnMonotoneAcrossRoll() throws Exception {
        try (TestWebSocketServer server = ackingServer()) {
            long w;
            long lastPublishedFsn;
            try (QwpWebSocketSender sender1 = (QwpWebSocketSender) Sender.fromConfig(cfg(server))) {
                sender1.table("t").longColumn("v", 1L).atNow();
                long fsn1 = sender1.flushAndGetSequence();
                Assert.assertTrue(sender1.drain(5_000));
                w = sender1.getAckedFsn();
                lastPublishedFsn = fsn1;
                Assert.assertEquals("sanity: single-batch acked watermark must match its own FSN",
                        fsn1, w);
            }

            // A fresh sender/engine models the post-recycle engine that restarts its
            // internal FSNs at 0; rolling its epoch base by the outgoing epoch's last
            // published FSN is exactly what the recycle swap does in production.
            QwpWebSocketSender sender2 = createRolledSender(server, lastPublishedFsn);
            try {
                long newBase = sender2.getFsnEpochBaseForTest();
                Assert.assertEquals(lastPublishedFsn + 1, newBase);

                Assert.assertEquals("before any new ack, getAckedFsn must read the synthetic "
                                + "watermark: one past the last external FSN the outgoing epoch "
                                + "ever reported",
                        newBase - 1, sender2.getAckedFsn());
                Assert.assertTrue(sender2.getAckedFsn() >= w);

                sender2.table("t").longColumn("v", 2L).atNow();
                sender2.flush();
                Assert.assertTrue(sender2.drain(5_000));
                Assert.assertTrue("a new ack must advance the watermark past the synthetic "
                                + "post-roll value",
                        sender2.getAckedFsn() > newBase - 1);
            } finally {
                sender2.close();
            }
        }
    }

    /**
     * The raw-feed bug test: without the {@code drain()} fix, a rolled epoch base makes
     * the raw {@code cursorEngine.publishedFsn()} target look like it belongs to a
     * pre-recycle epoch (its raw value is smaller than the rolled base), so the fixed
     * {@code awaitAckedFsn} would short-circuit {@code true} on an un-rebased target --
     * even though the frame was never actually acked. Must fail (spurious true) before
     * {@code drain()} translates its target by {@code fsnEpochBase}.
     */
    @Test
    public void testDrainAfterRollWaitsForNewFrames() throws Exception {
        GatedAckHandler handler = new GatedAckHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            // Roll well past the raw FSNs this fresh engine will ever publish, so a
            // missing translation in drain() would make its raw target look pre-roll.
            // Must roll before the sender's first connect (see rollFsnEpochBase's
            // precondition: cursorSendLoop must be null).
            QwpWebSocketSender sender = createRolledSender(server, 999L);
            try {
                sender.table("foo").longColumn("v", 1L).atNow();
                boolean drainedEarly = sender.drain(200);
                Assert.assertFalse("drain() must not spuriously report the new frame acked just "
                                + "because its raw FSN is smaller than the rolled epoch base",
                        drainedEarly);

                handler.releaseAcks();
                Assert.assertTrue("drain() must return true once the real ack arrives",
                        sender.drain(5_000));
            } finally {
                handler.releaseAcks();
                sender.close();
            }
        }
    }

    /**
     * {@link SenderError#getFromFsn()} / {@link SenderError#getToFsn()} surface synchronously
     * via {@link LineSenderServerException#getServerError()}, unreachable by any
     * dispatcher-side rebase -- the loop must rebase the span itself. Rolls the epoch base
     * BEFORE the sender's first connect (the loop's {@code externalFsnBase} is frozen at
     * construction) so the terminal NACK's span is built under a nonzero base.
     */
    @Test
    public void testSenderErrorSpansCarryExternalFsns() throws Exception {
        TerminalNackHandler handler = new TerminalNackHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            AtomicReference<SenderError> asyncError = new AtomicReference<>();
            QwpWebSocketSender sender = createRolledSender(server, 41L);
            long base = sender.getFsnEpochBaseForTest();
            sender.setErrorHandler(e -> asyncError.compareAndSet(null, e));
            try {
                sender.table("foo").longColumn("v", 1L).atNow();
                sender.flush();

                waitFor(() -> handler.totalBinaryReceived.get() >= 1, 5_000);
                waitFor(() -> sender.getLastTerminalError() != null, 5_000);

                LineSenderServerException thrown = null;
                try {
                    sender.table("foo").longColumn("v", 2L).atNow();
                    sender.flush();
                } catch (LineSenderServerException e) {
                    thrown = e;
                }
                Assert.assertNotNull("expected the latched terminal to surface synchronously",
                        thrown);
                SenderError err = thrown.getServerError();
                Assert.assertEquals("fromFsn must be the external (epoch-rebased) FSN, not raw",
                        base, err.getFromFsn());
                Assert.assertEquals("toFsn must be the external (epoch-rebased) FSN, not raw",
                        base, err.getToFsn());

                waitFor(() -> asyncError.get() != null, 5_000);
                SenderError asyncErr = asyncError.get();
                Assert.assertEquals("async error handler must observe the same fromFsn as the "
                                + "synchronous throw",
                        err.getFromFsn(), asyncErr.getFromFsn());
                Assert.assertEquals("async error handler must observe the same toFsn as the "
                                + "synchronous throw",
                        err.getToFsn(), asyncErr.getToFsn());
            } finally {
                try {
                    sender.close();
                } catch (LineSenderException ignored) {
                }
            }
        }
    }

    @Test
    public void testProgressStreamMonotoneAcrossRoll() throws Exception {
        try (TestWebSocketServer server = ackingServer()) {
            List<Long> observed = Collections.synchronizedList(new ArrayList<Long>());
            SenderProgressHandler collector = observed::add;

            long lastPublishedFsn;
            try (QwpWebSocketSender sender1 = (QwpWebSocketSender) Sender.fromConfig(cfg(server))) {
                sender1.setProgressHandler(collector);
                sender1.table("t").longColumn("v", 1L).atNow();
                lastPublishedFsn = sender1.flushAndGetSequence();
                Assert.assertTrue(sender1.drain(5_000));
            }
            waitFor(() -> !observed.isEmpty(), 5_000);
            int preRollCount = observed.size();

            // Roll BEFORE this sender's first connect so its loop's frozen
            // externalFsnBase actually carries the roll (see class javadoc).
            QwpWebSocketSender sender2 = createRolledSender(server, lastPublishedFsn);
            sender2.setProgressHandler(collector);
            try {
                sender2.table("t").longColumn("v", 2L).atNow();
                sender2.flush();
                Assert.assertTrue(sender2.drain(5_000));
                waitFor(() -> observed.size() > preRollCount, 5_000);

                List<Long> snapshot = new ArrayList<>(observed);
                for (int i = 1; i < snapshot.size(); i++) {
                    Assert.assertTrue("progress stream must be non-decreasing across the roll, "
                                    + "got " + snapshot,
                            snapshot.get(i) >= snapshot.get(i - 1));
                }
                Assert.assertTrue("post-roll progress must exceed the pre-roll watermark",
                        snapshot.get(snapshot.size() - 1) > lastPublishedFsn);
            } finally {
                sender2.close();
            }
        }
    }

    @Test
    public void testLatchedErrorStillThrowsForOldEpochTarget() throws Exception {
        // Acks the first frame it ever receives (from sender1, producing fsn1), then
        // terminal-NACKs everything after (sender2's frame, on its own fresh connection).
        AckFirstThenTerminalNackHandler handler = new AckFirstThenTerminalNackHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

            long fsn1;
            try (QwpWebSocketSender sender1 = (QwpWebSocketSender) Sender.fromConfig(cfg(server))) {
                sender1.table("foo").longColumn("v", 1L).atNow();
                fsn1 = sender1.flushAndGetSequence();
                Assert.assertTrue(sender1.drain(5_000));
            }

            // Roll before sender2's first connect (rollFsnEpochBase's precondition), then
            // force sender2's own loop to latch a terminal via the handler's NACK.
            QwpWebSocketSender sender2 = createRolledSender(server, fsn1);
            try {
                sender2.table("foo").longColumn("v", 2L).atNow();
                sender2.flush();
                waitFor(() -> sender2.getLastTerminalError() != null, 5_000);

                LineSenderException thrown = null;
                try {
                    boolean acked = sender2.awaitAckedFsn(fsn1, 0);
                    Assert.fail("awaitAckedFsn must throw on a latched terminal error, but "
                            + "returned " + acked);
                } catch (LineSenderException e) {
                    thrown = e;
                }
                Assert.assertNotNull("a latched terminal error must surface even for a "
                        + "pre-recycle-epoch target -- the error check must run before the "
                        + "pre-roll short-circuit", thrown);
            } finally {
                try {
                    sender2.close();
                } catch (LineSenderException ignored) {
                }
            }
        }
    }

    private static TestWebSocketServer ackingServer() throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(new AckAllHandler());
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    private static String cfg(TestWebSocketServer server) {
        return "ws::addr=localhost:" + server.getPort() + ";";
    }

    /**
     * An unconnected memory-mode sender with a freshly-attached (never published-to)
     * {@link CursorSendEngine}, its {@link QwpWebSocketSender#getFsnEpochBaseForTest()}
     * rolled by {@code rollAmount} before the first connect. Models the sender a symbol-dict
     * recycle swap hands off to: a fresh raw engine paired with an already-advanced epoch
     * base, so the loop this sender builds on first use gets that base baked into its
     * {@code externalFsnBase} from construction.
     */
    private static QwpWebSocketSender createRolledSender(TestWebSocketServer server, long rollAmount) {
        QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", server.getPort());
        CursorSendEngine engine = new CursorSendEngine(
                null, 4L * 1024 * 1024, 128L * 1024 * 1024,
                CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS);
        sender.setCursorEngine(engine, true);
        sender.rollFsnEpochBaseForTest(rollAmount);
        return sender;
    }

    private static void waitFor(BoolCondition cond, long timeoutMillis) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (cond.test()) {
                return;
            }
            Thread.sleep(20);
        }
        Assert.fail("waitFor timed out after " + timeoutMillis + "ms");
    }

    @FunctionalInterface
    private interface BoolCondition {
        boolean test();
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

    /** ACKs the first frame it receives, then terminal-NACKs every frame after it. */
    private static class AckFirstThenTerminalNackHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);
        private final AtomicLong receivedCount = new AtomicLong(0);

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                if (receivedCount.getAndIncrement() == 0) {
                    client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
                } else {
                    client.sendBinary(QwpWireTestUtils.buildNack(
                            nextSeq.getAndIncrement(), WebSocketResponse.STATUS_PARSE_ERROR));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Receives frames but withholds every ack until {@link #releaseAcks()} is called, so a
     * drain provably has an unacknowledged target to wait on. Mirrors
     * {@code CloseDrainTest.GatedAckHandler}.
     */
    private static class GatedAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final AtomicLong nextSeq = new AtomicLong(0);
        private final CountDownLatch released = new CountDownLatch(1);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                if (!released.await(20, TimeUnit.SECONDS)) {
                    throw new AssertionError("close-drain witness never released the ack gate");
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

    /** Terminal-NACKs (STATUS_PARSE_ERROR) every frame it receives. */
    private static class TerminalNackHandler implements TestWebSocketServer.WebSocketServerHandler {
        final AtomicLong totalBinaryReceived = new AtomicLong();
        private final AtomicLong nextSeq = new AtomicLong();

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            totalBinaryReceived.incrementAndGet();
            try {
                client.sendBinary(QwpWireTestUtils.buildNack(
                        nextSeq.getAndIncrement(), WebSocketResponse.STATUS_PARSE_ERROR));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
