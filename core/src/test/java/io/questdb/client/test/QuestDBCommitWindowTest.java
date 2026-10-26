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

package io.questdb.client.test;

import io.questdb.client.LineSenderServerException;
import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.impl.PooledSender;
import io.questdb.client.test.QuestDBFlushOnCloseTest.FrameLog;
import io.questdb.client.test.cutlass.qwp.client.QwpWireTestUtils;
import io.questdb.client.test.cutlass.qwp.websocket.CommitModelHandler;
import io.questdb.client.test.cutlass.qwp.websocket.QwpServerModel;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The pool's commit window as the server sees it. Each pooled release ships a
 * committing frame into the ring; the I/O thread sends it with the commit
 * deferred and commits once per window: with the frame that reaches
 * {@code commit_rows}, with a commit-only frame once the window is
 * {@code commit_interval_ms} old, or at once when a flush, a drain or a close
 * asks for it. The tests also cover the client's reaction to a rejected frame
 * inside a window.
 */
public class QuestDBCommitWindowTest {

    @Test
    public void testDefaultConfigDefersReleasesOnTheWire() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // no window keys: the window follows auto_flush_rows and auto_flush_interval (1000 rows, 100 ms)
            try (QuestDB db = QuestDB.connect(config(server.getPort(), ""))) {
                try (Sender s = db.borrowSender()) {
                    s.table("t").longColumn("v", 1L).atNow();
                }
                log.awaitDataFrames(1, 5_000);
                Assert.assertTrue("the release goes out with the commit deferred", log.deferFlag(0));
                log.awaitCommitFrames(1, 3_000);            // the 100 ms age bound commits the quiet window
                assertNoWindowMarker(log.frames());
            }
        }
    }

    @Test
    public void testDrainAndCloseInsideAnOpenWindowReturnPromptly() throws Exception {
        QwpServerModel model = new QwpServerModel();
        try (TestWebSocketServer server = new TestWebSocketServer(new CommitModelHandler(model))) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            QuestDB db = QuestDB.connect(config(server.getPort(),
                    "commit_rows=1000;commit_interval_ms=60000;close_flush_timeout_millis=5000;"));
            try {
                try (Sender s = db.borrowSender()) {
                    s.table("t").longColumn("v", 1L).atNow();
                }
                try (Sender s = db.borrowSender()) {
                    long start = System.nanoTime();
                    // the server withholds the ack of a deferred frame: the drain must commit the window
                    Assert.assertTrue(s.drain(5_000));
                    long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                    Assert.assertTrue("drain took " + tookMillis + " ms", tookMillis < 2_000);
                }
                try (Sender s = db.borrowSender()) {
                    s.table("t").longColumn("v", 2L).atNow();
                }
            } catch (Throwable t) {
                db.close();
                throw t;
            }
            long start = System.nanoTime();
            db.close();
            long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            Assert.assertTrue("close took " + tookMillis + " ms", tookMillis < 2_000);
            Assert.assertEquals("the drain and the close each committed one row", 2, model.committedRows());
        }
    }

    @Test
    public void testExplicitTransactionOnPoolCommitsEachRelease() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "transaction=on;commit_rows=1000;commit_interval_ms=60000;"))) {
                QwpWebSocketSender delegate = null;
                for (int i = 0; i < 3; i++) {
                    try (Sender s = db.borrowSender()) {
                        delegate = (QwpWebSocketSender) ((PooledSender) s).getDelegateForTesting();
                        s.table("t").longColumn("v", i).atNow();
                    }
                }
                log.awaitDataFrames(3, 5_000);
                int committingDataFrames = 0;
                for (int i = 0; i < 3; i++) {
                    if (!log.deferFlag(i)) {
                        committingDataFrames++;
                    }
                }
                Assert.assertEquals("a transaction=on sender commits each release on its own",
                        3, committingDataFrames + log.commitFrames());
                Assert.assertEquals("the window leaves a transaction=on sender alone", 0, delegate.getTotalFramesDeferred());
                assertNoWindowMarker(log.frames());
            }
        }
    }

    @Test
    public void testFlushInsideALeaseCommits() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "commit_rows=1000;commit_interval_ms=60000;"))) {
                try (Sender s = db.borrowSender()) {
                    s.table("t").longColumn("v", 1L).atNow();
                    s.flush();
                }
                log.awaitDataFrames(1, 5_000);
                Assert.assertFalse("a user's flush() goes out committing", log.deferFlag(0));
                assertNoWindowMarker(log.frames());
            }
        }
    }

    @Test
    public void testInLeaseAutoFlushCommitsAsOnMaster() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "auto_flush_rows=2;commit_rows=1000;commit_interval_ms=60000;"))) {
                try (Sender s = db.borrowSender()) {
                    for (int i = 0; i < 3; i++) {
                        s.table("t").longColumn("v", i).atNow();   // auto-flush at 2 rows
                    }
                }                                                   // the release ships row 3
                log.awaitDataFrames(2, 5_000);
                Assert.assertFalse("the in-lease auto-flush commits", log.deferFlag(0));
                Assert.assertTrue("the release frame is a window member", log.deferFlag(1));
                assertNoWindowMarker(log.frames());
            }
        }
    }

    @Test
    public void testRetriableCommitFailureReplaysGroupWithoutApplicationError() throws Exception {
        RejectSecondFrameHandler handler = new RejectSecondFrameHandler(WebSocketResponse.STATUS_INTERNAL_ERROR);
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "commit_rows=2;commit_interval_ms=60000;housekeeper_interval_ms=60000;auto_flush_interval=60000;")
                    + "reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;")) {
                try (Sender s = db.borrowSender()) {
                    s.table("t").longColumn("v", 1L).atNow();
                }
                try (Sender s = db.borrowSender()) {
                    s.table("t").longColumn("v", 2L).atNow();   // closes the window: the committing frame is rejected
                }
                handler.awaitSecondConnectionFrames(2, 5_000);
                Assert.assertEquals("both frames replay on the new connection", 2, handler.secondConnectionDataFrames());
                try (Sender s = db.borrowSender()) {
                    s.table("t").longColumn("v", 3L).atNow();   // no exception reaches the application
                }
            }
        }
    }

    @Test
    public void testRowlessFlushClosesTheOpenGroup() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "commit_interval_ms=60000;"))) {
                try (Sender s = db.borrowSender()) {
                    s.table("t").longColumn("v", 1L).atNow();
                }
                // the member must be on the wire, deferred, before the flush asks for the commit
                log.awaitDataFrames(1, 5_000);
                Assert.assertTrue(log.deferFlag(0));
                try (Sender s = db.borrowSender()) {
                    s.flush();
                }
                log.awaitCommitFrames(1, 2_000);
                assertNoWindowMarker(log.frames());
            }
        }
    }

    @Test
    public void testRowsWindowSendsDeferredFramesThenOneCommittingFrame() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "commit_rows=3;commit_interval_ms=60000;"))) {
                QuestDBFlushOnCloseTest.appendThreeRowsInThreeLeases(db);
                log.awaitDataFrames(3, 5_000);
                Assert.assertTrue(log.deferFlag(0));
                Assert.assertTrue(log.deferFlag(1));
                Assert.assertFalse("the third release fills the window: it goes out committing", log.deferFlag(2));
                Assert.assertEquals("no separate commit message when the closing frame carries rows", 0, log.commitFrames());
                assertNoWindowMarker(log.frames());
            }
        }
    }

    @Test
    public void testTerminalRejectionInsideAWindowReplaysOnceThenLatchesTheBadLeaseAlone() throws Exception {
        QwpServerModel model = new QwpServerModel();
        model.setRejector((connection, seq, frame) -> (connection <= 1 && seq == 1) ? WebSocketResponse.STATUS_SCHEMA_MISMATCH : 0);
        try (TestWebSocketServer server = new TestWebSocketServer(new CommitModelHandler(model))) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            QuestDB db = QuestDB.connect(config(server.getPort(), "commit_rows=1000;commit_interval_ms=60000;close_flush_timeout_millis=5000;")
                    + "reconnect_max_duration_millis=500;reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;");
            try {
                Sender retiredDelegate;
                try (Sender s = db.borrowSender()) {
                    retiredDelegate = ((PooledSender) s).getDelegateForTesting();
                    s.table("t").longColumn("v", 1L).atNow();
                }
                try (Sender s = db.borrowSender()) {
                    s.table("t").longColumn("v", 2L).atNow();   // the frame the server rejects
                }
                LineSenderServerException seen = null;
                long deadline = System.currentTimeMillis() + 5_000;
                while (seen == null && System.currentTimeMillis() < deadline) {
                    // try-with-resources: the typed error stays primary, close()'s wrapper is suppressed
                    try (Sender s = db.borrowSender()) {
                        Assert.assertSame("the slot stays in service until the latch", retiredDelegate, ((PooledSender) s).getDelegateForTesting());
                    } catch (LineSenderServerException e) {
                        seen = e;
                    } catch (LineSenderException e) {
                        if (!(e.getCause() instanceof LineSenderServerException)) {
                            throw e;
                        }
                        seen = (LineSenderServerException) e.getCause();
                    }
                    Thread.sleep(20);
                }
                Assert.assertNotNull("the latched terminal must surface on a later call on that slot", seen);
                Assert.assertEquals("it names the rejected frame", 1L, seen.getServerError().getFromFsn());
                Assert.assertEquals("it names the rejected frame", 1L, seen.getServerError().getToFsn());
                Assert.assertEquals("the replay committed the good lease", 1L, model.committedRows());
                try (Sender s = db.borrowSender()) {
                    Assert.assertNotSame("the pool replaces the retired slot", retiredDelegate, ((PooledSender) s).getDelegateForTesting());
                    s.table("t").longColumn("v", 3L).atNow();
                }
            } finally {
                db.close();
            }
            Assert.assertEquals("the new delegate's lease commits as well", 2L, model.committedRows());
        }
    }

    @Test
    public void testWindowReachesSlotsCreatedAfterStartup() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "sender_pool_min=1;sender_pool_max=2;commit_rows=1000;commit_interval_ms=60000;"))) {
                try (Sender first = db.borrowSender()) {
                    try (Sender second = db.borrowSender()) {
                        Assert.assertFalse("the second lease needs a new slot",
                                ((PooledSender) first).hasSameSlotForTesting((PooledSender) second));
                        second.table("t").longColumn("v", 1L).atNow();
                    }
                    log.awaitDataFrames(1, 5_000);
                    Assert.assertTrue("the new slot defers its release frame", log.deferFlag(0));
                }
                assertNoWindowMarker(log.frames());
            }
        }
    }

    @Test
    public void testZeroBoundsShipEveryReleaseCommitting() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "commit_rows=0;commit_interval_ms=0;"))) {
                QuestDBFlushOnCloseTest.appendThreeRowsInThreeLeases(db);
                log.awaitDataFrames(3, 5_000);
                for (int i = 0; i < 3; i++) {
                    Assert.assertFalse("no window: every release commits", log.deferFlag(i));
                }
                Assert.assertEquals(0, log.commitFrames());
                assertNoWindowMarker(log.frames());
            }
        }
    }

    private static void assertNoWindowMarker(List<byte[]> frames) {
        for (int i = 0, n = frames.size(); i < n; i++) {
            Assert.assertEquals("frame " + i + " carries the window marker on the wire", 0, QwpServerModel.flags(frames.get(i)) & 0x20);
        }
    }

    private static String config(int port, String extra) {
        return QuestDBFlushOnCloseTest.config(port, "flush_on_close=on;" + extra);
    }

    /**
     * First connection: withholds the ack of frame 0 (as the real server does
     * for a flagged frame inside an open group) and NACKs frame 1 with the
     * given status. Later connections: acks everything. Counts the frames the
     * second connection receives, so a replay from the acked watermark, which
     * is nothing, shows both frames again.
     */
    private static final class RejectSecondFrameHandler implements TestWebSocketServer.WebSocketServerHandler {
        private final byte rejectStatus;
        private int connections;
        private TestWebSocketServer.ClientHandler currentClient;
        private TestWebSocketServer.ClientHandler firstClient;
        private long nextSeq;
        private int secondConnectionDataFrames;

        RejectSecondFrameHandler(byte rejectStatus) {
            this.rejectStatus = rejectStatus;
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                connections++;
                nextSeq = 0;
                if (firstClient == null) {
                    firstClient = client;
                }
            }
            long seq = nextSeq++;
            if (connections >= 2 && data.length >= 8 && ((data[6] & 0xFF) | ((data[7] & 0xFF) << 8)) > 0) {
                secondConnectionDataFrames++;
            }
            try {
                if (client == firstClient) {
                    if (seq == 1) {
                        client.sendBinary(QwpWireTestUtils.buildNack(seq, rejectStatus));
                    }
                    // seq 0: ack withheld, the frame stays replayable client-side
                } else {
                    client.sendBinary(QwpWireTestUtils.buildAck(seq));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        void awaitSecondConnectionFrames(int atLeast, long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            while (secondConnectionDataFrames() < atLeast && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
        }

        synchronized int secondConnectionDataFrames() {
            return secondConnectionDataFrames;
        }
    }
}