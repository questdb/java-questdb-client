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

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.BatchTooLargeForCapException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.TransactionTooLargeException;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Batches of any size over QWP/WebSocket: the byte trigger that keeps every batch
 * within one frame, transactional and strict transactional mode, and rollback.
 * <p>
 * {@link TxnServer} answers frames the way the QuestDB server does: a
 * FLAG_DEFER_COMMIT frame gets no ack until a commit frame closes its group, a
 * commit frame gets a cumulative OK ack, and the rollback control frame gets a
 * cumulative OK ack once the server negotiated rollback.
 */
public class QwpLargeTransactionTest {
    private static final int CAP = 32 * 1024;
    // ~100 encoded bytes per row: 1000 rows (the default row trigger) are ~100 KB,
    // three times the 32 KiB cap.
    private static final String PAD = TestUtils.repeat("x", 90);

    @Test
    public void testAutoFlushOffIsStrictTransactionOfAnySize() throws Exception {
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (Sender s = Sender.fromConfig(conf(server) + "auto_flush=off;")) {
                    Assert.assertTrue(((QwpWebSocketSender) s).isStrictTransactional());
                    writeRows(s, "t", 0, 2_000);
                    // ~200 KB went out as deferred frames before flush(); none commits.
                    Assert.assertTrue(h.awaitFrames(5));
                    Assert.assertEquals(0, h.commitFrames());
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                }
                assertFramesWithinCap(h, CAP);
                assertOneTransaction(h);
            }
        });
    }

    @Test
    public void testAutoFlushOffRejectsTransactionOff() throws Exception {
        assertMemoryLeak(() -> {
            try {
                Sender.fromConfig("ws::addr=localhost:1;auto_flush=off;transaction=off;").close();
                Assert.fail("auto_flush=off must not combine with transaction=off");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(),
                        "auto_flush=off selects strict transactional mode on WebSocket");
            }
        });
    }

    @Test
    public void testDefaultSenderSplitsWideRowsIntoFrames() throws Exception {
        // Before: no byte trigger by default, so the 1000-row batch outgrew the cap
        // and flush threw BatchTooLargeForCapException.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (Sender s = Sender.fromConfig(conf(server))) {
                    writeRows(s, "t", 0, 2_000);
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                    QwpWebSocketSender ws = (QwpWebSocketSender) s;
                    Assert.assertEquals((long) CAP * 9 / 10, ws.getEffectiveAutoFlushBytes());
                }
                assertFramesWithinCap(h, CAP);
                Assert.assertTrue("rows must span frames: " + h.frames, h.frames.size() >= 7);
                // Not transactional: every frame commits on its own.
                Assert.assertEquals(h.frames.size(), h.commitFrames());
            }
        });
    }

    @Test
    public void testExplicitAutoFlushBytesOffKeepsNoByteTrigger() throws Exception {
        // auto_flush_bytes=off still opts out of the byte trigger, as documented.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (Sender s = Sender.fromConfig(conf(server) + "auto_flush_bytes=off;")) {
                    Assert.assertEquals(0, ((QwpWebSocketSender) s).getEffectiveAutoFlushBytes());
                    try {
                        writeRows(s, "t", 0, 1_000);
                        Assert.fail("a 1000-row batch over the cap must be rejected");
                    } catch (BatchTooLargeForCapException expected) {
                        TestUtils.assertContains(expected.getMessage(), "serverMaxBatchSize=" + CAP);
                    }
                    s.reset();
                }
                Assert.assertEquals(0, h.frames.size());
            }
        });
    }

    @Test
    public void testByteTriggerFollowsSegmentSize() throws Exception {
        // The server accepts 48 KiB frames, but a 16 KiB segment cannot hold them:
        // batches must be sized to the segment, not fail at the append.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, 48 * 1024)) {
                try (Sender s = Sender.fromConfig(conf(server) + "sf_max_segment_bytes=16k;")) {
                    writeRows(s, "t", 0, 600);
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                    int segmentPayload = 16 * 1024 - 24 - 8;
                    Assert.assertEquals(segmentPayload * 9 / 10, ((QwpWebSocketSender) s).getEffectiveAutoFlushBytes());
                }
                assertFramesWithinCap(h, 16 * 1024 - 24 - 8);
                Assert.assertTrue(h.frames.size() >= 4);
            }
        });
    }

    @Test
    public void testManualFlushKeepsOneCommitPerFlush() throws Exception {
        // A non-transactional sender with every auto-flush trigger off keeps its
        // flush as one commit: no byte trigger splits the batch early.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (Sender s = Sender.fromConfig(conf(server) + "auto_flush_rows=off;auto_flush_interval=off;")) {
                    Assert.assertEquals(0, ((QwpWebSocketSender) s).getEffectiveAutoFlushBytes());
                    writeRows(s, "a", 0, 250);
                    writeRows(s, "b", 0, 250);
                    Assert.assertEquals(0, h.frames.size());
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                }
                // The 50 KB batch splits per table at flush: one deferred frame, one commit.
                Assert.assertEquals(2, h.frames.size());
                Assert.assertTrue(h.frames.get(0).isDeferred());
                Assert.assertTrue(h.frames.get(1).isCommit());
            }
        });
    }

    @Test
    public void testNewSymbolsCountTowardByteTrigger() throws Exception {
        // Symbol columns buffer 4-byte ids; the 300-byte strings ride in the frame's
        // dictionary section. Counting them keeps the frame within the cap.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (Sender s = Sender.fromConfig(conf(server))) {
                    String prefix = TestUtils.repeat("s", 290);
                    for (int i = 0; i < 1_000; i++) {
                        s.table("t").symbol("sym", prefix + i).longColumn("v", i).atNow();
                    }
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                }
                assertFramesWithinCap(h, CAP);
                Assert.assertTrue(h.frames.size() >= 10);
            }
        });
    }

    @Test
    public void testPooledStrictLeaseCloseDuringOutageKeepsTheConnection() throws Exception {
        // Returning a strict lease discards its open transaction. During an outage that
        // must neither wait for the server nor cost the pool its connection: the next
        // borrower's transaction goes out behind the rollback frame once the server is back.
        assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            TxnServer before = new TxnServer();
            TxnServer after = new TxnServer();
            TestWebSocketServer revived = null;
            try (TestWebSocketServer first = start(before, CAP, port, true)) {
                try (QuestDB db = QuestDB.connect(outageConf(port) + "transaction=strict;query_pool_min=0;")) {
                    Sender lease = db.borrowSender();
                    writeRows(lease, "t", 0, 1_000);
                    Assert.assertTrue(before.awaitFrames(2));
                    first.close();
                    lease.close();
                    Sender next = db.borrowSender();
                    writeRows(next, "t", 0, 10);
                    next.flush();
                    revived = start(after, CAP, port, true);
                    Assert.assertTrue(next.drain(10_000));
                    next.close();
                }
            } finally {
                if (revived != null) {
                    revived.close();
                }
            }
            assertDiscardedThenCommitted(before, after);
        });
    }

    @Test
    public void testPooledStrictLeaseCloseRetiresConnectionWithoutServerRollback() throws Exception {
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (QuestDB db = QuestDB.connect(conf(server) + "transaction=strict;query_pool_min=0;")) {
                    Sender lease = db.borrowSender();
                    writeRows(lease, "t", 0, 1_000);
                    Assert.assertTrue(h.awaitFrames(2));
                    // The frames already sent cannot be rolled back: the pool retires
                    // the connection, which makes the server discard them.
                    lease.close();
                    Sender next = db.borrowSender();
                    writeRows(next, "t", 0, 10);
                    next.flush();
                    Assert.assertTrue(next.drain(5_000));
                    next.close();
                }
                Assert.assertEquals(1, h.commitFrames());
                Assert.assertEquals(0, h.rollbackFrames());
            }
        });
    }

    @Test
    public void testPooledStrictLeaseCloseRollsBackUncommitted() throws Exception {
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                server.setRollbackSupported(true);
                try (QuestDB db = QuestDB.connect(conf(server) + "transaction=strict;query_pool_min=0;")) {
                    Sender lease = db.borrowSender();
                    writeRows(lease, "t", 0, 1_000);
                    Assert.assertTrue(h.awaitFrames(2));
                    // Returning a strict lease without flush() discards the transaction,
                    // without waiting for the server.
                    lease.close();
                    Assert.assertTrue(h.awaitRollbackFrames(1));
                    Assert.assertTrue(h.frames.get(h.frames.size() - 1).isRollback());
                    Sender next = db.borrowSender();
                    writeRows(next, "t", 0, 10);
                    next.flush();
                    Assert.assertTrue(next.drain(5_000));
                    next.close();
                }
                Assert.assertEquals(1, h.commitFrames());
                Assert.assertEquals(1, h.rollbackFrames());
                assertRolledBackThenCommitted(h);
            }
        });
    }

    @Test
    public void testPooledStrictLeaseCloseWithOnlyPendingRowsKeepsConnection() throws Exception {
        // Nothing reached the server yet: returning the lease drops the pending
        // rows without a rollback frame and keeps the connection pooled.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (QuestDB db = QuestDB.connect(conf(server) + "transaction=strict;query_pool_min=0;")) {
                    Sender lease = db.borrowSender();
                    writeRows(lease, "t", 0, 10);
                    lease.close();
                    Sender next = db.borrowSender();
                    writeRows(next, "t", 0, 5);
                    next.flush();
                    Assert.assertTrue(next.drain(5_000));
                    next.close();
                }
                Assert.assertEquals(1, h.frames.size());
                Assert.assertTrue(h.frames.get(0).isCommit());
                Assert.assertEquals(1, server.upgradeRequestCount());
            }
        });
    }

    @Test
    public void testStrictMultiTableFlushSplitsWithinTheCap() throws Exception {
        // Ten tables in one batch carry ten schemas, so the batch the byte trigger
        // cuts outgrows the cap and splits per table. Every split frame of the
        // open transaction stays deferred.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, 4_096)) {
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=strict;")) {
                    for (int i = 0; i < 400; i++) {
                        s.table("table_with_a_long_name_" + (i % 10))
                                .longColumn("first_long_column_name", i)
                                .longColumn("second_long_column_name", i)
                                .stringColumn("s", "abc")
                                .atNow();
                    }
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                }
                assertFramesWithinCap(h, 4_096);
                assertOneTransaction(h);
                Assert.assertTrue(h.frames.size() > 10);
            }
        });
    }

    @Test
    public void testResetAbortsStrictTransactionWithoutServerRollback() throws Exception {
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=strict;")) {
                    writeRows(s, "t", 0, 1_000);
                    Assert.assertTrue(h.awaitFrames(2));
                    s.reset();
                    assertAborted(s::flush);
                    assertAborted(() -> s.table("t").longColumn("v", 1).atNow());
                }
                Assert.assertEquals("the reset transaction must never commit", 0, h.commitFrames());
            }
        });
    }

    @Test
    public void testResetDuringOutageDoesNotStallTheSender() throws Exception {
        // Store-and-forward keeps every producer call working through an outage. reset()
        // used to wait 30 s for the rollback ack and then abort the transaction, so every
        // later row and flush threw, even after the server came back.
        assertDiscardDuringOutage("transaction=on;", Sender::reset);
    }

    @Test
    public void testResetKeepsLegacyBehaviourWithoutServerRollback() throws Exception {
        // transaction=on against a server without rollback: reset() cannot reach the
        // frames already sent, and the next flush() commits them, as before.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=on;")) {
                    writeRows(s, "t", 0, 1_000);
                    Assert.assertTrue(h.awaitFrames(2));
                    s.reset();
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                }
                // The frames sent before reset() are deferred, and the flush after it
                // committed them with an empty commit frame.
                assertOneTransaction(h);
            }
        });
    }

    @Test
    public void testResetRollsBackSentFramesWhenServerSupportsIt() throws Exception {
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                server.setRollbackSupported(true);
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=on;")) {
                    writeRows(s, "t", 0, 1_000);
                    Assert.assertTrue(h.awaitFrames(2));
                    s.reset();
                    // reset() does not wait for the server; drain() returns once the server
                    // acked the rollback frame, which follows every frame of the transaction.
                    Assert.assertTrue(s.drain(5_000));
                    int rollbackIndex = h.frames.size() - 1;
                    Assert.assertTrue(h.frames.get(rollbackIndex).isRollback());
                    for (int i = 0; i < rollbackIndex; i++) {
                        Assert.assertTrue(h.frames.get(i).isDeferred());
                    }
                    Assert.assertEquals(1, h.rolledBackGroups);
                    // The sender stays usable for the next transaction.
                    writeRows(s, "t", 0, 10);
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                    Assert.assertTrue(h.frames.get(h.frames.size() - 1).isCommit());
                }
                assertRolledBackThenCommitted(h);
            }
        });
    }

    @Test
    public void testRollbackFrameNeverReachesServerWithoutRollbackSupport() throws Exception {
        // The transaction is discarded while the server that confirmed rollback is away,
        // and the endpoint comes back as a server without rollback support, as after a
        // failover to an older node. That server would read the rollback frame as an
        // empty deferred frame and commit the discarded rows with the next transaction.
        // The client closes the connection that holds them instead, and sends the next
        // transaction on a new one.
        assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            TxnServer before = new TxnServer();
            TxnServer after = new TxnServer();
            TestWebSocketServer revived = null;
            try (TestWebSocketServer first = start(before, CAP, port, true)) {
                try (Sender s = Sender.fromConfig(outageConf(port) + "transaction=on;")) {
                    writeRows(s, "t", 0, 1_000);
                    Assert.assertTrue(before.awaitFrames(2));
                    first.close();
                    s.rollback();
                    writeRows(s, "t", 0, 10);
                    s.flush();
                    revived = start(after, CAP, port, false);
                    Assert.assertTrue(s.drain(10_000));
                    Assert.assertFalse(((QwpWebSocketSender) s).isServerRollbackEnabled());
                }
            } finally {
                if (revived != null) {
                    revived.close();
                }
            }
            Assert.assertEquals(0, before.committedFrames().size());
            Assert.assertEquals("the rollback frame must not reach a server without rollback support: "
                    + after.frames, 0, after.rollbackFrames());
            List<List<TxnFrame>> connections = after.framesByConnection();
            Assert.assertEquals("one connection for the discarded transaction, one for the next: "
                    + after.frames, 2, connections.size());
            // The reader threads of the two connections may log their first frames in
            // either order: tell them apart by what they carry.
            List<TxnFrame> discarded = connections.get(0);
            List<TxnFrame> next = connections.get(1);
            if (next.get(next.size() - 1).isDeferred()) {
                discarded = connections.get(1);
                next = connections.get(0);
            }
            Assert.assertTrue("the discarded transaction is replayed: " + after.frames, discarded.size() >= 2);
            for (TxnFrame f : discarded) {
                Assert.assertTrue("frame must be deferred: " + after.frames, f.isDeferred());
            }
            Assert.assertTrue(next.get(next.size() - 1).isCommit());
            Assert.assertEquals("only the next transaction commits", next, after.committedFrames());
        });
    }

    @Test
    public void testRollbackDuringOutageDoesNotStallTheSender() throws Exception {
        assertDiscardDuringOutage("transaction=strict;", Sender::rollback);
    }

    @Test
    public void testRollbackDiscardsSentFrames() throws Exception {
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                server.setRollbackSupported(true);
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=strict;")) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) s;
                    Assert.assertTrue(ws.isServerRollbackEnabled());
                    writeRows(s, "t", 0, 1_000);
                    Assert.assertTrue(h.awaitFrames(2));
                    Assert.assertEquals(0, h.commitFrames());
                    s.rollback();
                    // rollback() does not wait for the server; drain() returns once the
                    // server acked the rollback frame, which follows every frame of the
                    // transaction.
                    Assert.assertTrue(s.drain(5_000));
                    int sent = h.frames.size() - 1;
                    TxnFrame rollback = h.frames.get(sent);
                    Assert.assertTrue(rollback.isRollback());
                    for (int i = 0; i < sent; i++) {
                        Assert.assertTrue(h.frames.get(i).isDeferred());
                    }
                    Assert.assertEquals(QwpConstants.HEADER_SIZE, rollback.size);
                    Assert.assertEquals(1, h.rolledBackGroups);
                    Assert.assertFalse(ws.hasDeferredMessagesForTesting());
                    // Nothing pending is left behind: an empty flush sends nothing.
                    s.flush();
                    Assert.assertEquals(sent + 1, h.frames.size());
                    // The next transaction starts clean and commits.
                    writeRows(s, "t", 0, 10);
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                    Assert.assertTrue(h.frames.get(h.frames.size() - 1).isCommit());
                }
                assertRolledBackThenCommitted(h);
            }
        });
    }

    @Test
    public void testRollbackOnNonWebSocketSenderThrows() throws Exception {
        assertMemoryLeak(() -> {
            try (Sender s = Sender.fromConfig("http::addr=localhost:1;protocol_version=2;")) {
                s.rollback();
                Assert.fail("rollback must be WebSocket-only");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "rollback is only supported for WebSocket transport");
            }
        });
    }

    @Test
    public void testRollbackUnsupportedAbortsTransaction() throws Exception {
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=on;")) {
                    Assert.assertFalse(((QwpWebSocketSender) s).isServerRollbackEnabled());
                    writeRows(s, "t", 0, 1_000);
                    Assert.assertTrue(h.awaitFrames(2));
                    assertAborted(s::rollback);
                    assertAborted(s::flush);
                    assertAborted(() -> s.table("t").longColumn("v", 1).atNow());
                    assertAborted(s::rollback);
                }
                // close() of an aborted transaction must not commit it either.
                Assert.assertEquals(0, h.commitFrames());
                Assert.assertEquals(0, h.rollbackFrames());
            }
        });
    }

    @Test
    public void testRollbackWithoutSentFramesSendsNothing() throws Exception {
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                server.setRollbackSupported(true);
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=strict;")) {
                    writeRows(s, "t", 0, 10);
                    s.rollback();
                    s.flush();
                    writeRows(s, "t", 0, 5);
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                }
                // Only the second transaction reached the server, as one commit frame.
                Assert.assertEquals(1, h.frames.size());
                Assert.assertTrue(h.frames.get(0).isCommit());
            }
        });
    }

    @Test
    public void testServerLimitsReachTheSender() throws Exception {
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                server.setAdvertisedMaxUncommittedRows(12_345);
                server.setRollbackSupported(true);
                try (Sender s = Sender.fromConfig(conf(server))) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) s;
                    Assert.assertEquals(12_345, ws.getServerMaxUncommittedRows());
                    Assert.assertTrue(ws.isServerRollbackEnabled());
                }
                Assert.assertEquals("true", server.pollRollbackRequest(5, TimeUnit.SECONDS));

                // A server that does not confirm rollback, nor advertise its row limit.
                server.setAdvertisedMaxUncommittedRows(0);
                server.setRollbackSupported(false);
                try (Sender s = Sender.fromConfig(conf(server))) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) s;
                    Assert.assertEquals(0, ws.getServerMaxUncommittedRows());
                    Assert.assertFalse(ws.isServerRollbackEnabled());
                }
            }
        });
    }

    @Test
    public void testStrictCloseWithoutFlushDiscardsTransaction() throws Exception {
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=strict;close_flush_timeout_millis=1000;")) {
                    writeRows(s, "t", 0, 1_000);
                    Assert.assertTrue(h.awaitFrames(2));
                }
                Assert.assertEquals("close() must not commit a strict transaction", 0, h.commitFrames());
                Assert.assertEquals(0, h.rollbackFrames());
            }
        });
    }

    @Test
    public void testStrictCloseWithoutFlushRollsBackWhenServerSupportsIt() throws Exception {
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                server.setRollbackSupported(true);
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=strict;")) {
                    writeRows(s, "t", 0, 1_000);
                    Assert.assertTrue(h.awaitFrames(2));
                }
                // close() drained until the rollback frame's ack.
                Assert.assertTrue(h.frames.get(h.frames.size() - 1).isRollback());
                Assert.assertEquals(0, h.commitFrames());
                Assert.assertEquals(1, h.rolledBackGroups);
            }
        });
    }

    @Test
    public void testStrictRowLimitFromServer() throws Exception {
        // The server advertises 100: a strict transaction holds at most 99 rows
        // of a table, since the server would commit the 100th on its own.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, 4_096)) {
                server.setAdvertisedMaxUncommittedRows(100);
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=strict;")) {
                    // 4 KiB frames: the 99 rows of "a" go out over several deferred frames.
                    writeRows(s, "a", 0, 99);
                    Assert.assertTrue(h.awaitFrames(2));
                    try {
                        writeRows(s, "a", 99, 1);
                        Assert.fail("the 100th row of a table must be refused");
                    } catch (TransactionTooLargeException e) {
                        TestUtils.assertContains(e.getMessage(),
                                "transaction too large for the server [table=a, rows=100, maxUncommittedRows=100]");
                    }
                    // Other tables keep their own count.
                    writeRows(s, "b", 0, 99);
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                    // A new transaction starts from zero again.
                    writeRows(s, "a", 0, 99);
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                }
                Assert.assertEquals(0, h.rollbackFrames());
                Assert.assertEquals(2, h.commitFrames());
            }
        });
    }

    @Test
    public void testStrictRowLimitDefaultsWhenNotAdvertised() throws Exception {
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=strict;")) {
                    QwpWebSocketSender ws = (QwpWebSocketSender) s;
                    Assert.assertEquals(0, ws.getServerMaxUncommittedRows());
                    // Pretend the server default is small to keep the test fast: the
                    // assumed default applies only while nothing is advertised.
                    ws.applyServerTransactionLimits(10, false);
                    writeRows(s, "a", 0, 9);
                    try {
                        writeRows(s, "a", 9, 1);
                        Assert.fail("the 10th row must be refused");
                    } catch (TransactionTooLargeException e) {
                        TestUtils.assertContains(e.getMessage(), "maxUncommittedRows=10");
                    }
                    ws.applyServerTransactionLimits(0, false);
                    // 0 means "not advertised": the server default applies.
                    writeRows(s, "a", 9, 1);
                    s.flush();
                }
            }
        });
    }

    @Test
    public void testStrictTransactionTooLargeForClientBufferFailsFast() throws Exception {
        // 64 KiB segments, 256 KiB budget: four segments. A strict transaction keeps
        // room for its commit, so it must refuse the flush that would take that room
        // instead of blocking for sf_append_deadline_millis.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=strict;"
                        + "sf_max_segment_bytes=64k;sf_max_total_bytes=256k;sf_append_deadline_millis=30000;")) {
                    long start = System.nanoTime();
                    int row = 0;
                    try {
                        for (; row < 100_000; row++) {
                            s.table("t").longColumn("v", row).stringColumn("s", PAD).atNow();
                        }
                        Assert.fail("a transaction larger than the client buffer must be refused");
                    } catch (TransactionTooLargeException e) {
                        TestUtils.assertContains(e.getMessage(), "transaction too large for the client buffer");
                    }
                    Assert.assertTrue("must fail fast, not wait for the append deadline",
                            System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10));
                    // The row being added stays in the transaction; further rows are refused.
                    try {
                        s.table("t").longColumn("v", -1).stringColumn("s", PAD).atNow();
                        Assert.fail("a full transaction must refuse rows");
                    } catch (TransactionTooLargeException e) {
                        TestUtils.assertContains(e.getMessage(), "the row was not added");
                    }
                    // The reserved room lets the transaction commit.
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                    Assert.assertTrue(h.frames.get(h.frames.size() - 1).isCommit());
                    Assert.assertEquals(1, h.commitFrames());
                    // And the next transaction starts with an empty buffer.
                    writeRows(s, "t", 0, 100);
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                }
            }
        });
    }

    @Test
    public void testUnacknowledgedRollbackDoesNotStallTheSender() throws Exception {
        // The server never acks the rollback frame on its own. rollback() used to wait
        // for that ack and abort the transaction when it did not come. Nothing waits for
        // it now: the next transaction goes out behind it, and the commit's cumulative
        // ack covers it.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            h.isAckingRollback = false;
            try (TestWebSocketServer server = start(h, CAP)) {
                server.setRollbackSupported(true);
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=strict;")) {
                    writeRows(s, "t", 0, 1_000);
                    Assert.assertTrue(h.awaitFrames(2));
                    s.rollback();
                    writeRows(s, "t", 0, 10);
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                }
                assertRolledBackThenCommitted(h);
            }
        });
    }

    @Test
    public void testTransactionalDefaultTriggersSplitWideRows() throws Exception {
        // Before: transaction=on with the default triggers threw
        // BatchTooLargeForCapException at the 1000th wide row.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=on;")) {
                    writeRows(s, "t", 0, 2_000);
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                }
                assertFramesWithinCap(h, CAP);
                assertOneTransaction(h);
            }
        });
    }

    private static void assertAborted(ThrowingRunnable r) {
        try {
            r.run();
            Assert.fail("expected the aborted-transaction error");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "transaction aborted");
        }
    }

    private static void assertFramesWithinCap(TxnServer h, int cap) {
        for (TxnFrame f : h.frames) {
            Assert.assertTrue("frame of " + f.size + " bytes exceeds " + cap, f.size <= cap);
        }
    }

    private static void assertOneTransaction(TxnServer h) {
        Assert.assertTrue(h.frames.size() > 1);
        for (int i = 0, n = h.frames.size() - 1; i < n; i++) {
            Assert.assertTrue("frame " + i + " must be deferred", h.frames.get(i).isDeferred());
        }
        Assert.assertTrue(h.frames.get(h.frames.size() - 1).isCommit());
    }

    // Asserts what the server that came back after an outage received and committed: the
    // discarded transaction's frames, replayed and then rolled back, and behind the rollback
    // frame the next transaction, which alone commits.
    private static void assertDiscardedThenCommitted(TxnServer before, TxnServer after) {
        Assert.assertEquals("nothing commits before the outage", 0, before.committedFrames().size());
        assertRolledBackThenCommitted(after);
    }

    // Asserts what one connection received: a transaction's frames, the rollback frame
    // that discarded them, and behind it the next transaction, which alone commits.
    private static void assertRolledBackThenCommitted(TxnServer h) {
        Assert.assertEquals("one connection: " + h.frames, 1, h.connectionCount());
        int rollbackAt = -1;
        for (int i = 0, n = h.frames.size(); i < n; i++) {
            if (h.frames.get(i).isRollback()) {
                Assert.assertEquals("one rollback frame: " + h.frames, -1, rollbackAt);
                rollbackAt = i;
            }
        }
        Assert.assertTrue("the discarded transaction goes out ahead of the rollback frame: " + h.frames,
                rollbackAt >= 2);
        for (int i = 0; i < rollbackAt; i++) {
            Assert.assertTrue("frame " + i + " must be deferred: " + h.frames, h.frames.get(i).isDeferred());
        }
        Assert.assertEquals(1, h.rolledBackGroups);
        Assert.assertTrue(h.frames.get(h.frames.size() - 1).isCommit());
        Assert.assertEquals("only the next transaction commits",
                h.frames.subList(rollbackAt + 1, h.frames.size()), h.committedFrames());
    }

    // The server confirmed rollback, received the first frames of a transaction and went
    // away. The transaction is discarded during the outage, the next one is committed, and
    // the server comes back on the same endpoint.
    private static void assertDiscardDuringOutage(String mode, Consumer<Sender> discard) throws Exception {
        assertMemoryLeak(() -> {
            int port = TestPorts.findUnusedPort();
            TxnServer before = new TxnServer();
            TxnServer after = new TxnServer();
            TestWebSocketServer revived = null;
            try (TestWebSocketServer first = start(before, CAP, port, true)) {
                try (Sender s = Sender.fromConfig(outageConf(port) + mode)) {
                    writeRows(s, "t", 0, 1_000);
                    Assert.assertTrue(before.awaitFrames(2));
                    first.close();
                    // Must neither wait for the server nor fail: the server is away.
                    discard.accept(s);
                    writeRows(s, "t", 0, 10);
                    s.flush();
                    revived = start(after, CAP, port, true);
                    Assert.assertTrue(s.drain(10_000));
                }
            } finally {
                if (revived != null) {
                    revived.close();
                }
            }
            assertDiscardedThenCommitted(before, after);
        });
    }

    private static String conf(TestWebSocketServer server) {
        return "ws::addr=localhost:" + server.getPort() + ";";
    }

    // Reconnects within milliseconds once the server is back. Each test drains before it
    // closes, so the short close drain only bounds how long a failing test takes.
    private static String outageConf(int port) {
        return "ws::addr=localhost:" + port
                + ";reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;close_flush_timeout_millis=1000;";
    }

    private static TestWebSocketServer start(TxnServer h, int cap) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(h);
        server.setAdvertisedMaxBatchSize(cap);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    // A server on a fixed port, so a test can take it down and bring it back on the same
    // endpoint, as one that supports rollback or as one that predates it.
    private static TestWebSocketServer start(TxnServer h, int cap, int port, boolean isRollbackSupported) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(h, false, null, port);
        server.setAdvertisedMaxBatchSize(cap);
        server.setRollbackSupported(isRollbackSupported);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    private static void writeRows(Sender s, String table, int from, int count) {
        for (int i = from, n = from + count; i < n; i++) {
            s.table(table).longColumn("v", i).stringColumn("s", PAD).atNow();
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }

    private static final class TxnFrame {
        // Index of the connection the frame arrived on, in connection order.
        final int connection;
        final byte flags;
        final int size;

        TxnFrame(byte flags, int size, int connection) {
            this.flags = flags;
            this.size = size;
            this.connection = connection;
        }

        boolean isCommit() {
            return (flags & QwpConstants.FLAG_DEFER_COMMIT) == 0;
        }

        boolean isDeferred() {
            return !isRollback() && (flags & QwpConstants.FLAG_DEFER_COMMIT) != 0;
        }

        boolean isRollback() {
            return size == QwpConstants.HEADER_SIZE && flags == QwpConstants.ROLLBACK_FRAME_FLAGS;
        }

        @Override
        public String toString() {
            return connection + ":" + (isRollback() ? "R" : isDeferred() ? "D" : "C") + size;
        }
    }

    /**
     * Acks frames the way the QuestDB server does, per connection: wire sequences
     * restart at 0 on every connection, deferred frames wait for the commit frame
     * that closes their group, which acks them cumulatively, and the rollback frame
     * discards the group and acks it too.
     * <p>
     * It also models what the server commits: a commit frame commits the group open
     * on its connection, while a rollback frame or a closed connection leaves that
     * group uncommitted. On a connection that did not confirm rollback the rollback
     * frame is what a server without rollback support makes of it, an empty deferred
     * frame: the group stays open, and the next commit on the connection commits it.
     */
    private static final class TxnServer implements TestWebSocketServer.WebSocketServerHandler {
        // Every frame received, over all connections, in arrival order.
        final List<TxnFrame> frames = new CopyOnWriteArrayList<>();
        volatile boolean isAckingRollback = true;
        volatile int rolledBackGroups;
        // Frames a commit frame committed, in commit order.
        private final List<TxnFrame> committed = new ArrayList<>();
        private final Map<TestWebSocketServer.ClientHandler, Connection> connections = new IdentityHashMap<>();

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            Connection connection = connections.get(client);
            if (connection == null) {
                connection = new Connection(connections.size());
                connections.put(client, connection);
            }
            long seq = connection.nextSeq++;
            TxnFrame frame = new TxnFrame(data[QwpConstants.HEADER_OFFSET_FLAGS], data.length, connection.index);
            frames.add(frame);
            notifyAll();
            try {
                if (frame.isCommit()) {
                    committed.addAll(connection.group);
                    committed.add(frame);
                    connection.group.clear();
                    client.sendBinary(ack(seq));
                } else if (frame.isRollback() && client.isRollbackConfirmed()) {
                    rolledBackGroups++;
                    connection.group.clear();
                    if (isAckingRollback) {
                        client.sendBinary(ack(seq));
                    }
                } else {
                    connection.group.add(frame);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        synchronized boolean awaitRollbackFrames(int count) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (rollbackFrames() < count) {
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(this, left);
            }
            return true;
        }

        synchronized List<TxnFrame> committedFrames() {
            return new ArrayList<>(committed);
        }

        synchronized int connectionCount() {
            return connections.size();
        }

        // The frames of each connection, in arrival order, by connection index.
        synchronized List<List<TxnFrame>> framesByConnection() {
            List<List<TxnFrame>> byConnection = new ArrayList<>();
            for (int i = 0, n = connections.size(); i < n; i++) {
                byConnection.add(new ArrayList<>());
            }
            for (TxnFrame f : frames) {
                byConnection.get(f.connection).add(f);
            }
            return byConnection;
        }

        synchronized boolean awaitFrames(int count) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (frames.size() < count) {
                long left = deadline - System.nanoTime();
                if (left <= 0) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(this, left);
            }
            return true;
        }

        synchronized int commitFrames() {
            int n = 0;
            for (TxnFrame f : frames) {
                if (f.isCommit()) {
                    n++;
                }
            }
            return n;
        }

        synchronized int rollbackFrames() {
            int n = 0;
            for (TxnFrame f : frames) {
                if (f.isRollback()) {
                    n++;
                }
            }
            return n;
        }

        private static byte[] ack(long seq) {
            byte[] buf = new byte[1 + 8 + 2];
            ByteBuffer bb = ByteBuffer.wrap(buf).order(ByteOrder.LITTLE_ENDIAN);
            bb.put((byte) 0x00);
            bb.putLong(seq);
            bb.putShort((short) 0);
            return buf;
        }

        private static final class Connection {
            // Deferred frames the connection holds uncommitted.
            final List<TxnFrame> group = new ArrayList<>();
            final int index;
            long nextSeq;

            Connection(int index) {
                this.index = index;
            }
        }
    }
}
