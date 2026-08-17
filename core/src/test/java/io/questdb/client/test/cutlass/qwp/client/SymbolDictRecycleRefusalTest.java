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
 * The negative space of the symbol-dictionary recycle: every guard in
 * {@code QwpWebSocketSender.maybeRecycleForDictReset()} that must refuse the
 * swap even though {@code resetArmed} is true, plus the deferral guard in
 * {@code table(CharSequence)} that keeps a pre-connect manual request from
 * ever touching the (not yet existing) cursor engine or I/O loop.
 * <p>
 * {@link SymbolDictRecycleTest} and {@link SymbolDictRecycleMemoryModeTest}
 * pin the swap itself; {@link SymbolDictRecycleArmingTest} pins how
 * {@code resetArmed} flips true; {@link SymbolDictRecycleStarvationTest} pins
 * the bounded blocking wait for an unacked backlog. This suite pins the
 * OPPOSITE: every condition under which an armed sender must keep working
 * normally and NOT recycle, until that condition clears -- at which point
 * the still-armed request fires as a positive control in the same test.
 * Every test asserts both halves: no recycle (connection count and
 * {@code getSymbolDictEpochForTest()} unchanged) AND that ingestion keeps
 * working (a row lands and gets acked) both before and after the eventual
 * recycle.
 */
public class SymbolDictRecycleRefusalTest {

    /**
     * The most basic refusal: a batch that itself crossed the arming
     * threshold is still unacked when the very next {@code table()} call
     * checks the barrier. {@code symbol_dict_reset_max_wait_millis=0}
     * disables the (separately-pinned, {@link SymbolDictRecycleStarvationTest})
     * blocking wait, so every refusal here is instant and this test stays
     * purely about the ring-drained guard. Repeated {@code table()} calls
     * spread over a real span of wall-clock time (not one instantaneous
     * check) prove the recycle does not fire late, either -- the whole
     * mechanism is synchronous and producer-thread-driven, but a bounded
     * settle window is the only way a test can actually witness that rather
     * than assume it.
     */
    @Test
    public void testUnackedBacklogRefusesUntilAcked() throws Exception {
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
                    long fsn1 = sender.flushAndGetSequence(); // ack withheld by the handler
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());
                    Assert.assertEquals(1, server.handshakeCount());
                    Assert.assertEquals(0, ws.getSymbolDictEpochForTest());

                    for (int i = 0; i < 5; i++) {
                        sender.table("t");
                        Assert.assertTrue("recycle must not fire while the arming batch is unacked",
                                ws.isResetArmed());
                        Assert.assertEquals(0, ws.getSymbolDictEpochForTest());
                        Assert.assertEquals(1, server.handshakeCount());
                        Thread.sleep(30);
                    }

                    // Positive control: release the acks and prove the still-armed
                    // recycle fires on the very next drained table() call.
                    handler.releaseAcks();
                    Assert.assertTrue("setup: the arming batch must get acked once released",
                            sender.awaitAckedFsn(fsn1, 5_000));

                    sender.table("t");
                    Assert.assertFalse("recycle must fire once the backlog drains", ws.isResetArmed());
                    Assert.assertEquals(1, ws.getSymbolDictEpochForTest());
                    Assert.assertEquals("recycle must open a fresh connection",
                            2, server.handshakeCount());

                    // Ingestion continues on the fresh epoch.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue("post-recycle batch must still get acked",
                            sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertTrue(fsn2 > fsn1);
                }
            }
        });
    }

    /**
     * Isolates the {@code pendingRowCount != 0} guard from the ring-drained
     * guard {@link #testUnackedBacklogRefusesUntilAcked} pins: the arming
     * batch's ack is released and awaited WHILE a third row sits buffered
     * (committed via {@code atNow()}, but never flushed -- {@code auto_flush_rows}
     * is set well above 1 so it does not auto-flush). By the time the
     * settle-window loop runs, the ring itself is fully drained, so any
     * refusal it observes can only be this guard, not the earlier one.
     */
    @Test
    public void testPendingRowCountRefuses() throws Exception {
        assertMemoryLeak(() -> {
            GatedAckHandler handler = new GatedAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                String cfg = "ws::addr=localhost:" + port
                        + ";symbol_dict_reset_threshold=2"
                        + ";symbol_dict_reset_max_wait_millis=0"
                        + ";auto_flush_rows=10;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence(); // ack withheld by the handler
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());

                    // Ring not drained yet: refused by the OTHER guard, which just
                    // lets execution fall through so a new row can be buffered.
                    sender.table("t");
                    Assert.assertTrue(ws.isResetArmed());
                    Assert.assertEquals(0, ws.getSymbolDictEpochForTest());

                    // A third row, committed but never flushed: pendingRowCount=1,
                    // far under auto_flush_rows=10, so it stays buffered.
                    sender.symbol("s", "c").longColumn("v", 2L).atNow();

                    // Drain the arming batch -- from here on the ring itself is
                    // fully drained, isolating the pendingRowCount guard.
                    handler.releaseAcks();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn1, 5_000));

                    for (int i = 0; i < 5; i++) {
                        sender.table("t");
                        Assert.assertTrue("recycle must not fire while a row is buffered "
                                        + "unflushed, even with the ring otherwise drained",
                                ws.isResetArmed());
                        Assert.assertEquals(0, ws.getSymbolDictEpochForTest());
                        Assert.assertEquals(1, server.handshakeCount());
                        Thread.sleep(30);
                    }

                    // Positive control: flush the buffered row, then the
                    // still-armed recycle fires on the next table() call.
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn2, 5_000));

                    sender.table("t");
                    Assert.assertFalse("recycle must fire once the buffered batch is flushed "
                                    + "and acked",
                            ws.isResetArmed());
                    Assert.assertEquals(1, ws.getSymbolDictEpochForTest());
                    Assert.assertEquals(2, server.handshakeCount());

                    sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                    long fsn3 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn3, 5_000));
                    Assert.assertTrue(fsn3 > fsn2);
                }
            }
        });
    }

    /**
     * A row under construction (columns set, {@code atNow()} not yet called)
     * refuses the barrier two different ways depending on the next
     * {@code table()} call's table name. The same-name case is the sharper
     * proof: {@code table()}'s resetArmed check runs BEFORE the
     * same-table-name fast path that would otherwise skip straight past
     * everything, so this is the only way to prove the hook actually sits
     * ahead of that shortcut. The different-name case falls through to the
     * pre-existing "cannot switch tables while row is in progress" guard
     * instead -- a thrown exception, not a recycle, and not a new failure
     * mode this feature introduced.
     */
    @Test
    public void testInProgressRowRefuses() throws Exception {
        assertMemoryLeak(() -> {
            AckAllHandler handler = new AckAllHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                String cfg = "ws::addr=localhost:" + port + ";";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    // Start a row but do not commit it: symbol() registers "a"
                    // into the dictionary immediately, yet the row itself stays
                    // in progress until atNow() runs.
                    sender.table("t").symbol("s", "a");
                    Assert.assertEquals(0, ws.getSymbolDictEpochForTest());

                    // Arm WHILE the row is in progress: pendingRowCount is still
                    // 0 (an in-progress row is not counted as pending), so the
                    // manual request arms immediately even though a row is
                    // genuinely mid-flight.
                    sender.resetSymbolDictionary();
                    Assert.assertTrue(ws.isResetArmed());

                    for (int i = 0; i < 3; i++) {
                        sender.table("t"); // same name -- fast path would skip past everything
                        Assert.assertTrue("recycle must not fire while a row is in progress",
                                ws.isResetArmed());
                        Assert.assertEquals(0, ws.getSymbolDictEpochForTest());
                        Assert.assertEquals(1, server.handshakeCount());
                        Thread.sleep(20);
                    }

                    LineSenderException thrown = null;
                    try {
                        sender.table("other");
                        Assert.fail("expected 'cannot switch tables' while a row is in progress");
                    } catch (LineSenderException e) {
                        thrown = e;
                    }
                    Assert.assertNotNull(thrown);
                    Assert.assertTrue("unexpected message: " + thrown.getMessage(),
                            thrown.getMessage().contains("cannot switch tables while row is in progress"));
                    Assert.assertTrue("the failed table-switch attempt must not have consumed "
                                    + "the arming",
                            ws.isResetArmed());
                    Assert.assertEquals(0, ws.getSymbolDictEpochForTest());
                    Assert.assertEquals(1, server.handshakeCount());

                    // Complete the row: ingestion still works after both refusals.
                    sender.longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue("nothing yet consumed the arming", ws.isResetArmed());
                    Assert.assertEquals(0, ws.getSymbolDictEpochForTest());

                    // Positive control: with the row complete and the batch
                    // acked, the still-armed recycle fires on the next call.
                    sender.table("t");
                    Assert.assertFalse("recycle must fire once the row completes and the ring "
                                    + "drains",
                            ws.isResetArmed());
                    Assert.assertEquals(1, ws.getSymbolDictEpochForTest());
                    Assert.assertEquals(2, server.handshakeCount());

                    sender.table("t").symbol("s", "d").longColumn("v", 2L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertTrue(fsn2 > fsn1);
                }
            }
        });
    }

    /**
     * The one data-safety-critical refusal, mirroring
     * {@code SymbolDictRecycleStarvationTest#testDeferredCommitGroupSkipsWait}
     * but for the barrier itself rather than the blocking-wait futility
     * guard: the server withholds acks for {@code FLAG_DEFER_COMMIT} frames
     * by design until the closing commit lands, so {@code isRingDrained()}
     * stays false for as long as the group is open, however long that is.
     * {@code symbol_dict_reset_max_wait_millis=0} keeps this test orthogonal
     * to the (separately-pinned) starvation-wait timing.
     */
    @Test
    public void testDeferredCommitGroupRefusesUntilCommitAcked() throws Exception {
        assertMemoryLeak(() -> {
            DeferAwareAckHandler handler = new DeferAwareAckHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                String cfg = "ws::addr=localhost:" + port
                        + ";symbol_dict_reset_threshold=2"
                        + ";symbol_dict_reset_max_wait_millis=0;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                    ws.setDeferCommit(true);

                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    sender.flush(); // deferred frame -- server withholds its ack by design
                    Assert.assertTrue("must be armed after crossing threshold=2", ws.isResetArmed());

                    for (int i = 0; i < 5; i++) {
                        sender.table("t");
                        Assert.assertTrue("an open deferred-commit group must never let the "
                                        + "recycle fire -- the server withholds its ack until "
                                        + "the closing commit",
                                ws.isResetArmed());
                        Assert.assertEquals(0, ws.getSymbolDictEpochForTest());
                        Assert.assertEquals(1, server.handshakeCount());
                        Thread.sleep(30);
                    }

                    // Positive control: close the group, wait for its
                    // (retroactive) ack, and prove the still-armed recycle
                    // fires next.
                    ws.setDeferCommit(false);
                    long commitFsn = sender.flushAndGetSequence();
                    Assert.assertTrue("setup: the commit must get acked",
                            sender.awaitAckedFsn(commitFsn, 5_000));

                    sender.table("t");
                    Assert.assertFalse("recycle must fire once the group is committed and acked",
                            ws.isResetArmed());
                    Assert.assertEquals(1, ws.getSymbolDictEpochForTest());
                    Assert.assertEquals(2, server.handshakeCount());

                    sender.table("t").symbol("s", "c").longColumn("v", 2L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertTrue(fsn2 > commitFsn);
                }
            }
        });
    }

    /**
     * A manual {@code resetSymbolDictionary()} call arms {@code resetArmed}
     * regardless of connection state ({@code armIfEligible()} touches only
     * producer-side fields), so it can go through before the sender has ever
     * connected -- modelled the same way
     * {@code SymbolDictRecycleFsnContinuityTest} builds unconnected senders:
     * {@link QwpWebSocketSender#createForTesting} plus a manually attached
     * engine, with {@link QwpWebSocketSender#setEngineRebuildFactory} filled
     * in (unlike {@code createForTesting}'s production counterparts, a
     * connect()-built sender normally has none -- see
     * {@code SymbolDictRecycleTest#testConnectBuiltSenderNeverRecyclesWithoutFactory})
     * so the deferred request can actually execute once connected. The very
     * next {@code table()} call -- still pre-connect -- must defer rather
     * than NPE: {@code !connected} refuses the barrier before it ever
     * touches the cursor engine or I/O loop.
     */
    @Test
    public void testManualResetBeforeFirstConnectDeferred() throws Exception {
        assertMemoryLeak(() -> {
            AckAllHandler handler = new AckAllHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();

                QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", port);
                try {
                    CursorSendEngine engine = new CursorSendEngine(
                            null, 4L * 1024 * 1024, 128L * 1024 * 1024,
                            CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS);
                    sender.setCursorEngine(engine, true);
                    sender.setEngineRebuildFactory(() -> new CursorSendEngine(
                            null, 4L * 1024 * 1024, 128L * 1024 * 1024,
                            CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS));

                    // Manual request before the sender has ever connected.
                    sender.resetSymbolDictionary();
                    Assert.assertTrue("a manual request arms immediately, independent of "
                                    + "connection state",
                            sender.isResetArmed());
                    Assert.assertEquals(0, sender.getSymbolDictEpochForTest());
                    Assert.assertEquals(0, server.handshakeCount());

                    // table()'s barrier check runs here while still pre-connect
                    // (ensureConnected() only runs later, inside atNow()'s
                    // sendRow()) -- must defer quietly, not NPE.
                    sender.table("t").longColumn("v", 1L).atNow();
                    Assert.assertTrue("still armed -- deferred, not consumed",
                            sender.isResetArmed());
                    Assert.assertEquals(0, sender.getSymbolDictEpochForTest());
                    Assert.assertEquals(1, server.handshakeCount());

                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertTrue("flush alone does not consume the arming -- only table() "
                                    + "does",
                            sender.isResetArmed());
                    Assert.assertEquals(0, sender.getSymbolDictEpochForTest());

                    // Positive control: now connected and drained, the
                    // deferred request executes on the next table() call.
                    sender.table("t");
                    Assert.assertFalse("the deferred request must execute once connected and "
                                    + "drained",
                            sender.isResetArmed());
                    Assert.assertEquals(1, sender.getSymbolDictEpochForTest());
                    Assert.assertEquals(2, server.handshakeCount());

                    sender.table("t").longColumn("v", 2L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertTrue(fsn2 > fsn1);
                } finally {
                    sender.close();
                }
            }
        });
    }

    /**
     * {@code reset()} discards a buffered-but-never-shipped row -- including
     * reclaiming any symbol id it registered but never sent, via the same
     * {@code truncateTo} mechanism the {@code BatchTooLargeForCapException}
     * remedy documents. This proves that discard is compatible with an
     * already-armed swap: after {@code reset()} clears the in-progress row
     * that was the ONLY thing refusing the barrier, every guard is
     * satisfied (connected, no pending row, no in-progress row, ring
     * drained from the earlier shipped batch), so the next {@code table()}
     * call recycles -- observed here to fire deterministically, not
     * probabilistically, once those guards clear. It is compatible with
     * {@code reset()}'s own reclaim: the swap replaces the whole dictionary
     * object outright (step 5 of {@code recycleForDictReset()}), so
     * whatever {@code truncateTo} did to the outgoing instance is moot --
     * the swap subsumes it.
     */
    @Test
    public void testResetDiscardsBufferedRowThenArmedSwapFires() throws Exception {
        assertMemoryLeak(() -> {
            AckAllHandler handler = new AckAllHandler();
            try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                int port = server.getPort();
                String cfg = "ws::addr=localhost:" + port + ";symbol_dict_reset_threshold=3;";

                try (Sender sender = Sender.fromConfig(cfg)) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) sender;

                    // A real shipped batch: two distinct symbols, below the
                    // threshold of 3, so nothing arms yet.
                    sender.table("t").symbol("s", "a").longColumn("v", 1L).atNow();
                    sender.table("t").symbol("s", "b").longColumn("v", 1L).atNow();
                    long fsn1 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn1, 5_000));
                    Assert.assertFalse("dictionary has only 2 entries, below the threshold of 3",
                            ws.isResetArmed());

                    // Start (but never commit) a third row -- registers "c",
                    // crossing the threshold, but arming is only ever
                    // evaluated at a flush's tail or by resetSymbolDictionary(),
                    // neither of which has run yet.
                    sender.table("t").symbol("s", "c").longColumn("v", 2L);
                    Assert.assertFalse(ws.isResetArmed());

                    // Arm explicitly while the row is still in progress --
                    // the in-progress-row guard refuses table(), exactly as
                    // testInProgressRowRefuses proves.
                    sender.resetSymbolDictionary();
                    Assert.assertTrue(ws.isResetArmed());
                    sender.table("t"); // refused: row "c" is in progress
                    Assert.assertTrue(ws.isResetArmed());
                    Assert.assertEquals(0, ws.getSymbolDictEpochForTest());
                    Assert.assertEquals(1, server.handshakeCount());

                    // Discard the buffered row -- reset() drops the
                    // in-progress row AND reclaims "c"'s never-shipped id.
                    sender.reset();

                    // Every barrier guard is now satisfied: connected, no
                    // pending row (reset cleared it), no in-progress row
                    // (reset cleared it), ring drained (a, b were acked
                    // before any of this). The armed swap fires here.
                    sender.table("t");
                    Assert.assertFalse("the armed swap fires once reset() clears the blocking "
                                    + "in-progress row",
                            ws.isResetArmed());
                    Assert.assertEquals(1, ws.getSymbolDictEpochForTest());
                    Assert.assertEquals(2, server.handshakeCount());

                    // Ingestion continues correctly post-swap: a fresh row
                    // lands and gets acked with no exception.
                    sender.table("t").symbol("s", "d").longColumn("v", 3L).atNow();
                    long fsn2 = sender.flushAndGetSequence();
                    Assert.assertTrue(sender.awaitAckedFsn(fsn2, 5_000));
                    Assert.assertTrue(fsn2 > fsn1);
                }
            }
        });
    }

    /**
     * ACKs every frame it receives; does not otherwise inspect the wire.
     * Resets its wire sequence per new connection, mirroring
     * {@code SymbolDictRecycleTest.RecycleHandler}, so post-recycle
     * ingestion on the fresh connection acks correctly too.
     */
    private static class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private TestWebSocketServer.ClientHandler currentClient;
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                nextSeq.set(0);
            }
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Acks every non-deferred frame immediately, but withholds acks for any
     * frame carrying {@code FLAG_DEFER_COMMIT} -- the real server's ack
     * contract for an open deferred-commit group. Mirrors
     * {@code SymbolDictRecycleStarvationTest.DeferAwareAckHandler}, plus a
     * per-connection wire-sequence reset so ingestion on the post-recycle
     * connection acks correctly too.
     */
    private static class DeferAwareAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private TestWebSocketServer.ClientHandler currentClient;
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                nextSeq.set(0);
            }
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

    /**
     * Receives frames but withholds every ack until {@link #releaseAcks()}
     * is called, so a refusal-guard test provably has an unacknowledged
     * target to refuse on. Mirrors
     * {@code SymbolDictRecycleFsnContinuityTest.GatedAckHandler} /
     * {@code SymbolDictRecycleStarvationTest.GatedAckHandler}, plus a
     * per-connection wire-sequence reset so ingestion on the post-recycle
     * connection acks correctly too.
     */
    private static class GatedAckHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final CountDownLatch released = new CountDownLatch(1);
        private TestWebSocketServer.ClientHandler currentClient;
        private final AtomicLong nextSeq = new AtomicLong(0);

        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            try {
                if (!released.await(20, TimeUnit.SECONDS)) {
                    throw new AssertionError("refusal-guard witness never released the ack gate");
                }
                synchronized (this) {
                    if (currentClient != client) {
                        currentClient = client;
                        nextSeq.set(0);
                    }
                    client.sendBinary(QwpWireTestUtils.buildAck(nextSeq.getAndIncrement()));
                }
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        void releaseAcks() {
            released.countDown();
        }
    }
}
