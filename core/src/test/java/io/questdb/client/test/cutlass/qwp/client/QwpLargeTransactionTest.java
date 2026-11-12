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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
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
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

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
    // The settings a refusal at the client buffer names.
    private static final String CLIENT_BUFFER_REMEDY = "Raise sf_max_total_bytes, or set sf_dir to keep the "
            + "transaction on disk";
    // What closing a sender or returning a pooled lease logs when it discards an open
    // strict transaction of 1000 rows, behind the action that discarded it.
    private static final String DISCARD_WARNING = " discarded an uncommitted transaction of 1000 rows: a strict "
            + "transactional sender (transaction=strict, or one with no auto-flush trigger) commits only on "
            + "flush(). Call flush() before close() to keep the rows, or ignore this if the discard is intentional";
    private static final String NO_TRIGGER_TRANSACTION_OFF_ERROR = "auto-flush is off (auto_flush=off, or "
            + "auto_flush_rows=off and auto_flush_interval=off), which on WebSocket selects strict transactional "
            + "mode and cannot be combined with transaction=off; set transaction=on or turn on an auto-flush trigger";
    // ~100 encoded bytes per row: 1000 rows (the default row trigger) are ~100 KB,
    // three times the 32 KiB cap.
    private static final String PAD = TestUtils.repeat("x", 90);

    @Test
    public void testAutoFlushBytesAloneCommitsEachBatch() throws Exception {
        // A positive auto_flush_bytes is an auto-flush trigger: with the row and
        // interval triggers off, every batch it cuts commits on its own, as before.
        // transaction=off may say so explicitly.
        assertMemoryLeak(() -> {
            String[] configs = {
                    "auto_flush_rows=off;auto_flush_interval=off;auto_flush_bytes=8192;",
                    "auto_flush_rows=off;auto_flush_interval=off;auto_flush_bytes=8192;transaction=off;"
            };
            for (String config : configs) {
                TxnServer h = new TxnServer();
                try (TestWebSocketServer server = start(h, CAP)) {
                    try (Sender s = Sender.fromConfig(conf(server) + config)) {
                        QwpWebSocketSender ws = (QwpWebSocketSender) s;
                        Assert.assertFalse(config, ws.isTransactional());
                        Assert.assertEquals(config, 8_192, ws.getEffectiveAutoFlushBytes());
                        // ~20 KB: the byte trigger sends at least two batches, and each
                        // commits before any flush().
                        writeRows(s, "t", 0, 200);
                        Assert.assertTrue(config, h.awaitFrames(2));
                        for (TxnFrame f : h.frames) {
                            Assert.assertTrue(config + ": " + h.frames, f.isCommit());
                        }
                    }
                    // close() commits the rest.
                    Assert.assertEquals(config, h.frames, h.committedFrames());
                }
            }
        });
    }

    @Test
    public void testAutoFlushOffRejectsPositiveAutoFlushBytes() throws Exception {
        // auto_flush=off turns every auto-flush trigger off, so a byte trigger
        // contradicts it, as a row or interval trigger already does. The ws:: parser
        // applies the trigger keys before auto_flush, whatever their order.
        // auto_flush_bytes=off is no trigger; see
        // testNoAutoFlushTriggerWithAutoFlushBytesOffSendsOneFramePerFlush.
        assertMemoryLeak(() -> {
            assertConfigRejected("auto_flush=off;auto_flush_bytes=65536;",
                    "auto flush bytes was already configured [autoFlushBytes=65536]");
            assertConfigRejected("auto_flush_bytes=65536;auto_flush=off;",
                    "auto flush bytes was already configured [autoFlushBytes=65536]");
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
    public void testNoAutoFlushTriggerIsStrictTransactionOfAnySize() throws Exception {
        // Every way to turn off all auto-flush triggers builds the same sender: a
        // strict transaction of any size, which only flush() commits.
        // auto_flush_rows=off with auto_flush_interval=off used to send each flush as
        // one batch, which failed past the frame cap, and to commit on close().
        assertMemoryLeak(() -> {
            for (NoTriggerSpelling spelling : noTriggerSpellings()) {
                TxnServer h = new TxnServer();
                List<TxnFrame> committed = null;
                try (TestWebSocketServer server = start(h, CAP)) {
                    try (Sender s = spelling.build(conf(server))) {
                        QwpWebSocketSender ws = (QwpWebSocketSender) s;
                        Assert.assertTrue(spelling.name, ws.isStrictTransactional());
                        Assert.assertEquals(spelling.name, CAP * 9 / 10, ws.getEffectiveAutoFlushBytes());
                        writeRows(s, "t", 0, 2_000);
                        // ~200 KB went out as deferred frames before flush(); none commits.
                        Assert.assertTrue(spelling.name, h.awaitFrames(5));
                        Assert.assertEquals(spelling.name, 0, h.commitFrames());
                        s.flush();
                        Assert.assertTrue(spelling.name, s.drain(5_000));
                        assertFramesWithinCap(h, CAP);
                        assertOneTransaction(h);
                        committed = h.committedFrames();
                        Assert.assertEquals(spelling.name, h.frames, committed);
                        // close() without flush() discards the next transaction, its
                        // frames already sent included.
                        writeRows(s, "t", 2_000, 1_000);
                        Assert.assertTrue(spelling.name, h.awaitFrames(committed.size() + 2));
                    }
                    Assert.assertEquals(spelling.name, committed, h.committedFrames());
                }
            }
        });
    }

    @Test
    public void testNoAutoFlushTriggerRejectsTransactionOff() throws Exception {
        // With no auto-flush trigger only flush() commits, and a non-transactional
        // flush has to fit one frame, so every spelling refuses transaction=off.
        assertMemoryLeak(() -> {
            assertConfigRejected("auto_flush=off;transaction=off;", NO_TRIGGER_TRANSACTION_OFF_ERROR);
            assertConfigRejected("auto_flush_rows=off;auto_flush_interval=off;transaction=off;",
                    NO_TRIGGER_TRANSACTION_OFF_ERROR);
            assertConfigRejected("auto_flush_rows=off;auto_flush_interval=off;auto_flush_bytes=off;transaction=off;",
                    NO_TRIGGER_TRANSACTION_OFF_ERROR);
        });
    }

    @Test
    public void testNoAutoFlushTriggerWithAutoFlushBytesOffSendsOneFramePerFlush() throws Exception {
        // auto_flush_bytes=off opts out of sizing batches to the frame: the
        // transaction still commits only on flush(), but the application keeps it
        // within one frame per table, and a flush past the cap is refused and keeps
        // the rows, as auto_flush_bytes=off documents.
        assertMemoryLeak(() -> {
            String[] configs = {
                    "auto_flush=off;auto_flush_bytes=off;",
                    "auto_flush_rows=off;auto_flush_interval=off;auto_flush_bytes=off;"
            };
            for (String config : configs) {
                TxnServer h = new TxnServer();
                try (TestWebSocketServer server = start(h, CAP)) {
                    try (Sender s = Sender.fromConfig(conf(server) + config)) {
                        QwpWebSocketSender ws = (QwpWebSocketSender) s;
                        Assert.assertTrue(config, ws.isStrictTransactional());
                        Assert.assertEquals(config, 0, ws.getEffectiveAutoFlushBytes());
                        // ~20 KB fits the 32 KiB cap: one frame, which commits.
                        writeRows(s, "t", 0, 200);
                        s.flush();
                        Assert.assertTrue(config, s.drain(5_000));
                        Assert.assertEquals(config, 1, h.frames.size());
                        Assert.assertTrue(config, h.frames.get(0).isCommit());
                        // ~100 KB does not.
                        writeRows(s, "t", 200, 1_000);
                        try {
                            s.flush();
                            Assert.fail(config + ": a flush past the cap must be refused");
                        } catch (BatchTooLargeForCapException e) {
                            TestUtils.assertContains(e.getMessage(), "serverMaxBatchSize=" + CAP);
                        }
                        s.reset();
                    }
                    Assert.assertEquals(config, 1, h.frames.size());
                }
            }
        });
    }

    @Test
    public void testNoAutoFlushTriggerWithTransactionOnCommitsOnClose() throws Exception {
        // An explicit transaction=on wins over the strict mode a sender with no
        // auto-flush trigger otherwise gets: close() commits, as transaction=on does.
        assertMemoryLeak(() -> {
            for (NoTriggerSpelling spelling : noTriggerSpellings()) {
                TxnServer h = new TxnServer();
                try (TestWebSocketServer server = start(h, CAP)) {
                    try (Sender s = spelling.build(conf(server) + "transaction=on;")) {
                        QwpWebSocketSender ws = (QwpWebSocketSender) s;
                        Assert.assertTrue(spelling.name, ws.isTransactional());
                        Assert.assertFalse(spelling.name, ws.isStrictTransactional());
                        writeRows(s, "t", 0, 2_000);
                        Assert.assertTrue(spelling.name, h.awaitFrames(5));
                        Assert.assertEquals(spelling.name, 0, h.commitFrames());
                    }
                    assertFramesWithinCap(h, CAP);
                    assertOneTransaction(h);
                    Assert.assertEquals(spelling.name, h.frames, h.committedFrames());
                }
            }
        });
    }

    @Test
    public void testOneAutoFlushTriggerLeftKeepsEveryBatchCommitting() throws Exception {
        // Turning off the row or the interval trigger alone leaves the other one, so
        // the sender is not transactional: every batch it sends commits on its own.
        assertMemoryLeak(() -> {
            String[] configs = {"auto_flush_rows=off;", "auto_flush_interval=off;"};
            for (String config : configs) {
                TxnServer h = new TxnServer();
                try (TestWebSocketServer server = start(h, CAP)) {
                    try (Sender s = Sender.fromConfig(conf(server) + config)) {
                        QwpWebSocketSender ws = (QwpWebSocketSender) s;
                        Assert.assertFalse(config, ws.isTransactional());
                        Assert.assertEquals(config, CAP * 9 / 10, ws.getEffectiveAutoFlushBytes());
                        // ~100 KB: the byte trigger sends at least three batches before
                        // any flush(), and each commits.
                        writeRows(s, "t", 0, 1_000);
                        Assert.assertTrue(config, h.awaitFrames(3));
                        for (TxnFrame f : h.frames) {
                            Assert.assertTrue(config + ": " + h.frames, f.isCommit());
                        }
                    }
                    Assert.assertEquals(config, h.frames, h.committedFrames());
                }
            }
        });
    }

    @Test
    public void testPooledLeaseWithAbortedTransactionRetiresQuietly() throws Exception {
        // reset() could not undo the frames already sent, so the transaction is
        // aborted and every flush already said so. Returning the lease retires the
        // connection, which discards the frames, without warning about them again.
        assertMemoryLeak(() -> {
            ListAppender<ILoggingEvent> log = captureSenderLog();
            try {
                TxnServer h = new TxnServer();
                try (TestWebSocketServer server = start(h, CAP)) {
                    try (QuestDB db = QuestDB.connect(conf(server)
                            + "auto_flush_rows=off;auto_flush_interval=off;query_pool_min=0;")) {
                        Sender lease = db.borrowSender();
                        writeRows(lease, "t", 0, 1_000);
                        lease.reset();
                        assertAborted(lease::flush);
                        lease.close();
                        Sender next = db.borrowSender();
                        writeRows(next, "t", 0, 10);
                        next.flush();
                        Assert.assertTrue(next.drain(5_000));
                        next.close();
                    }
                    List<TxnFrame> committed = h.committedFrames();
                    Assert.assertEquals("only the next lease commits", 1, committed.size());
                    Assert.assertTrue(committed.get(0).isCommit());
                    Assert.assertEquals("the aborted lease's connection was retired", 2, server.upgradeRequestCount());
                }
                Assert.assertEquals(Collections.emptyList(), discardWarnings(log));
            } finally {
                releaseSenderLog(log);
            }
        });
    }

    @Test
    public void testPooledLeaseWithoutAutoFlushTriggerDiscardsOnReturn() throws Exception {
        // Returning a lease of a pool with no auto-flush trigger used to commit its rows.
        // The lease now holds a strict transaction, which only flush() commits, so
        // returning it discards the rows, and says so once: whether the server rolls
        // back the frames already sent, or the pool retires the connection to discard them.
        assertMemoryLeak(() -> {
            ListAppender<ILoggingEvent> log = captureSenderLog();
            try {
                String[] spellings = {"auto_flush=off;", "auto_flush_rows=off;auto_flush_interval=off;"};
                for (String spelling : spellings) {
                    for (boolean isRollbackSupported : new boolean[]{false, true}) {
                        String name = spelling + " rollback=" + isRollbackSupported;
                        clearLog(log);
                        TxnServer h = new TxnServer();
                        try (TestWebSocketServer server = start(h, CAP)) {
                            server.setRollbackSupported(isRollbackSupported);
                            try (QuestDB db = QuestDB.connect(conf(server) + spelling + "query_pool_min=0;")) {
                                Sender lease = db.borrowSender();
                                writeRows(lease, "t", 0, 1_000);
                                Assert.assertTrue(name, h.awaitFrames(2));
                                lease.close();
                                Sender next = db.borrowSender();
                                writeRows(next, "t", 0, 10);
                                next.flush();
                                Assert.assertTrue(name, next.drain(5_000));
                                next.close();
                            }
                            List<TxnFrame> committed = h.committedFrames();
                            Assert.assertEquals(name + ": only the next lease commits", 1, committed.size());
                            Assert.assertTrue(name, committed.get(0).isCommit());
                            Assert.assertEquals(name, isRollbackSupported ? 1 : 0, h.rollbackFrames());
                        }
                        Assert.assertEquals(name,
                                Collections.singletonList("returning the sender to the pool" + DISCARD_WARNING),
                                discardWarnings(log));
                    }
                }
            } finally {
                releaseSenderLog(log);
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
    public void testStrictCloseWithoutFlushWarnsOfDiscardedRows() throws Exception {
        // A sender with no auto-flush trigger used to commit on close(). Now close()
        // discards its open transaction, and says so once, counting the rows already
        // sent with those still buffered. A committed transaction leaves nothing to
        // discard, and an aborted one already failed every flush.
        assertMemoryLeak(() -> {
            ListAppender<ILoggingEvent> log = captureSenderLog();
            try {
                TxnServer h = new TxnServer();
                try (TestWebSocketServer server = start(h, CAP)) {
                    try (Sender s = Sender.fromConfig(conf(server) + "auto_flush_rows=off;auto_flush_interval=off;")) {
                        writeRows(s, "t", 0, 1_000);
                        Assert.assertTrue(h.awaitFrames(2));
                    }
                    Assert.assertEquals(Collections.singletonList("close()" + DISCARD_WARNING), discardWarnings(log));
                    clearLog(log);

                    try (Sender s = Sender.fromConfig(conf(server) + "auto_flush_rows=off;auto_flush_interval=off;")) {
                        writeRows(s, "t", 0, 1_000);
                        s.flush();
                        Assert.assertTrue(s.drain(5_000));
                    }
                    try (Sender s = Sender.fromConfig(conf(server) + "transaction=strict;")) {
                        writeRows(s, "t", 0, 1_000);
                        // Frames went out and the server cannot roll them back.
                        s.reset();
                        assertAborted(s::flush);
                    }
                    Assert.assertEquals(Collections.emptyList(), discardWarnings(log));
                    Assert.assertEquals(1, h.commitFrames());
                }
            } finally {
                releaseSenderLog(log);
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
                        Assert.assertEquals(serverRowLimitError("a", 100), e.getMessage());
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
                        Assert.assertEquals(serverRowLimitError("a", 10), e.getMessage());
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
    public void testStrictCommitTooLargeForClientBufferIsNotSent() throws Exception {
        // With auto_flush_bytes=off on top of auto_flush=off, the whole transaction
        // waits for flush(), which sends one frame per table. Ten tables of ~28 KB need five 64 KiB
        // segments, and sf_max_total_bytes holds four: flush() sends nothing.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (Sender s = Sender.fromConfig(conf(server) + "auto_flush=off;auto_flush_bytes=off;"
                        + "sf_max_segment_bytes=64k;sf_max_total_bytes=256k;")) {
                    writeTables(s, 0, 10);
                    long refusedBytes = -1;
                    try {
                        s.flush();
                        Assert.fail("the client buffer cannot hold the commit");
                    } catch (TransactionTooLargeException e) {
                        refusedBytes = assertClientBufferRefusal(e, 262_144, "so the flush was not sent");
                    }
                    Assert.assertEquals("nothing was sent", 0, h.frames.size());
                    s.rollback();
                    // The same rows, one table per transaction: the refusal reported
                    // what these frames add up to.
                    for (int t = 0; t < 10; t++) {
                        writeTables(s, t, 1);
                        s.flush();
                    }
                    Assert.assertTrue(s.drain(5_000));
                    Assert.assertEquals(10, h.commitFrames());
                    Assert.assertEquals(refusedBytes, frameBytes(h));
                }
            }
        });
    }

    @Test
    public void testStrictDeferredFlushTooLargeForClientBufferIsNotSent() throws Exception {
        // setDeferCommit(true) makes flush() send rows without committing them. A
        // strict transaction keeps room for its commit next to such a frame. With
        // frames as large as a segment that takes four segments, and
        // sf_max_total_bytes holds three: flush() sends nothing, and the transaction
        // takes no further rows. The commit itself fits.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, 64 * 1024)) {
                long refusedBytes = -1;
                try (QwpWebSocketSender s = (QwpWebSocketSender) Sender.fromConfig(conf(server)
                        + "transaction=strict;sf_max_segment_bytes=64k;sf_max_total_bytes=192k;")) {
                    s.setDeferCommit(true);
                    writeRows(s, "t", 0, 1);
                    try {
                        s.flush();
                        Assert.fail("the client buffer cannot hold a deferred frame and the commit");
                    } catch (TransactionTooLargeException e) {
                        refusedBytes = assertClientBufferRefusal(e, 196_608, "so the flush was not sent");
                    }
                    Assert.assertEquals("nothing was sent", 0, h.frames.size());
                    try {
                        writeRows(s, "t", 1, 1);
                        Assert.fail("the transaction must take no further rows");
                    } catch (TransactionTooLargeException e) {
                        Assert.assertEquals(refusedBytes,
                                assertClientBufferRefusal(e, 196_608, "so the row was not added"));
                    }
                    s.setDeferCommit(false);
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                }
                // The commit carries the row the refused flush held, and only that row.
                Assert.assertEquals(1, h.frames.size());
                Assert.assertTrue(h.frames.get(0).isCommit());
                Assert.assertEquals(refusedBytes, h.frames.get(0).size);
            }
        });
    }

    @Test
    public void testStrictTransactionTooLargeForClientBufferFailsFast() throws Exception {
        // 64 KiB segments, 256 KiB budget: four segments. A strict transaction keeps
        // room for its commit, so once its frames fill the rest it must refuse rows
        // instead of blocking for sf_append_deadline_millis.
        assertMemoryLeak(() -> {
            TxnServer h = new TxnServer();
            try (TestWebSocketServer server = start(h, CAP)) {
                try (Sender s = Sender.fromConfig(conf(server) + "transaction=strict;"
                        + "sf_max_segment_bytes=64k;sf_max_total_bytes=256k;sf_append_deadline_millis=30000;")) {
                    long start = System.nanoTime();
                    long refusedBytes = -1;
                    try {
                        for (int row = 0; row < 100_000; row++) {
                            s.table("t").longColumn("v", row).stringColumn("s", PAD).atNow();
                        }
                        Assert.fail("a transaction larger than the client buffer must be refused");
                    } catch (TransactionTooLargeException e) {
                        // As at the server's row limit, the row that hit the limit is not
                        // in the transaction.
                        refusedBytes = assertClientBufferRefusal(e, 262_144, "so the row was not added");
                    }
                    Assert.assertTrue("must fail fast, not wait for the append deadline",
                            System.nanoTime() - start < TimeUnit.SECONDS.toNanos(10));
                    // Further rows are refused the same way.
                    try {
                        s.table("t").longColumn("v", -1).stringColumn("s", PAD).atNow();
                        Assert.fail("a full transaction must refuse rows");
                    } catch (TransactionTooLargeException e) {
                        Assert.assertEquals(refusedBytes,
                                assertClientBufferRefusal(e, 262_144, "so the row was not added"));
                    }
                    // The reserved room lets the transaction commit.
                    s.flush();
                    Assert.assertTrue(s.drain(5_000));
                    Assert.assertTrue(h.frames.get(h.frames.size() - 1).isCommit());
                    Assert.assertEquals(1, h.commitFrames());
                    // The refusal reported the size of the transaction that committed:
                    // the rows accepted before it, the last of which filled the buffer.
                    Assert.assertEquals(refusedBytes, frameBytes(h));
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

    // Captures what QwpWebSocketSender logs, so a test can assert its warnings.
    private static ListAppender<ILoggingEvent> captureSenderLog() {
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        senderLogger().addAppender(appender);
        return appender;
    }

    // The appender appends under its own lock, and I/O threads log into it too.
    private static void clearLog(ListAppender<ILoggingEvent> log) {
        synchronized (log) {
            log.list.clear();
        }
    }

    // The discarded-transaction warnings logged since the last clearLog().
    private static List<String> discardWarnings(ListAppender<ILoggingEvent> log) {
        List<String> warnings = new ArrayList<>();
        synchronized (log) {
            for (ILoggingEvent e : log.list) {
                String message = e.getFormattedMessage();
                if (e.getLevel() == Level.WARN && message.contains("discarded an uncommitted transaction")) {
                    warnings.add(message);
                }
            }
        }
        return warnings;
    }

    // Every way to configure a WebSocket sender with no auto-flush trigger, applied
    // on top of a config string.
    private static List<NoTriggerSpelling> noTriggerSpellings() {
        List<NoTriggerSpelling> spellings = new ArrayList<>();
        spellings.add(new NoTriggerSpelling("auto_flush=off", c -> Sender.builder(c + "auto_flush=off;")));
        spellings.add(new NoTriggerSpelling("auto_flush_rows=off;auto_flush_interval=off",
                c -> Sender.builder(c + "auto_flush_rows=off;auto_flush_interval=off;")));
        spellings.add(new NoTriggerSpelling("auto_flush_interval=off;auto_flush_rows=off",
                c -> Sender.builder(c + "auto_flush_interval=off;auto_flush_rows=off;")));
        spellings.add(new NoTriggerSpelling("disableAutoFlush()",
                c -> Sender.builder(c).disableAutoFlush()));
        spellings.add(new NoTriggerSpelling("autoFlushRows(0).autoFlushIntervalMillis(Integer.MAX_VALUE)",
                c -> Sender.builder(c).autoFlushRows(0).autoFlushIntervalMillis(Integer.MAX_VALUE)));
        return spellings;
    }

    private static void releaseSenderLog(ListAppender<ILoggingEvent> log) {
        senderLogger().detachAppender(log);
        log.stop();
    }

    private static Logger senderLogger() {
        return (Logger) LoggerFactory.getLogger(QwpWebSocketSender.class);
    }

    private static void assertAborted(ThrowingRunnable r) {
        try {
            r.run();
            Assert.fail("expected the aborted-transaction error");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), "transaction aborted");
        }
    }

    // Asserts the error of a strict transaction the client buffer cannot hold, with
    // 64 KiB segments, and returns the transaction size it reports.
    private static long assertClientBufferRefusal(TransactionTooLargeException e, long ringCapacityBytes, String outcome) {
        String message = e.getMessage();
        String prefix = "transaction too large for the client buffer [transactionBytes=";
        Assert.assertTrue(message, message.startsWith(prefix));
        long transactionBytes = Long.parseLong(message.substring(prefix.length(), message.indexOf(',')));
        Assert.assertTrue(message, transactionBytes > 0);
        Assert.assertEquals(prefix + transactionBytes + ", ringCapacityBytes=" + ringCapacityBytes
                + ", segmentBytes=65536]; the client holds a strict transaction until the server commits it, "
                + "and sf_max_total_bytes cannot hold this one, " + outcome + ". " + CLIENT_BUFFER_REMEDY, message);
        return transactionBytes;
    }

    // Build-time validation runs before the sender connects, so no server is needed.
    private static void assertConfigRejected(String config, String expectedMessage) {
        try {
            Sender.fromConfig("ws::addr=localhost:1;" + config).close();
            Assert.fail(config + " must be rejected");
        } catch (LineSenderException e) {
            TestUtils.assertContains(e.getMessage(), expectedMessage);
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

    private static long frameBytes(TxnServer h) {
        long bytes = 0;
        for (TxnFrame f : h.frames) {
            bytes += f.size;
        }
        return bytes;
    }

    private static String serverRowLimitError(String table, long maxUncommittedRows) {
        return "transaction too large for the server [table=" + table + ", rows=" + maxUncommittedRows
                + ", maxUncommittedRows=" + maxUncommittedRows + "]; the server commits a table's rows on its "
                + "own once qwp.max.uncommitted.rows of them are uncommitted, which would split the transaction, "
                + "so the row was not added. Raise qwp.max.uncommitted.rows on the server";
    }

    private static void writeRows(Sender s, String table, int from, int count) {
        for (int i = from, n = from + count; i < n; i++) {
            s.table(table).longColumn("v", i).stringColumn("s", PAD).atNow();
        }
    }

    // ~28 KB in each of count tables, from table t<first>: each fits a 32 KiB frame.
    private static void writeTables(Sender s, int first, int count) {
        for (int t = first, n = first + count; t < n; t++) {
            writeRows(s, "t" + t, 0, 280);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run();
    }

    private static final class NoTriggerSpelling {
        final String name;
        private final Function<String, Sender.LineSenderBuilder> builder;

        NoTriggerSpelling(String name, Function<String, Sender.LineSenderBuilder> builder) {
            this.name = name;
            this.builder = builder;
        }

        Sender build(String config) {
            return builder.apply(config).build();
        }
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
