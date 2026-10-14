package io.questdb.client.test;

import io.questdb.client.LineSenderServerException;
import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.impl.PooledSender;
import io.questdb.client.test.QuestDBFlushOnCloseTest.FrameLog;
import io.questdb.client.test.cutlass.qwp.client.QwpWireTestUtils;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * The commit window as the server sees it: flagged frames per close, one
 * unflagged frame per window, a rowless commit message for an idle tail, and
 * the client's reaction to a rejected frame inside a group.
 */
public class QuestDBCommitWindowTest {

    @Test
    public void testAgeWindowCommitsAtNextRelease() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "commit_rows=1000;commit_interval_ms=100;housekeeper_interval_ms=60000;auto_flush_interval=60000;"))) {
                try (Sender s = db.borrowSender()) {
                    s.table("t").longColumn("v", 1L).atNow();
                }
                // Hold the only slot across the window: the settle skips a leased slot (it
                // would otherwise commit the idle group on its 100 ms cadence), so the
                // release below is the first thing to find the group older than the window.
                try (Sender s = db.borrowSender()) {
                    Thread.sleep(150);
                    s.table("t").longColumn("v", 2L).atNow();
                }
                log.awaitDataFrames(2, 5_000);
                Assert.assertTrue(log.deferFlag(0));
                Assert.assertFalse(log.deferFlag(1));
            }
        }
    }

    @Test
    public void testLongLeaseAutoFlushShipsFlaggedAndFlushCommits() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "auto_flush_rows=2;commit_rows=1000;commit_interval_ms=60000;housekeeper_interval_ms=60000;"))) {
                try (Sender s = db.borrowSender()) {
                    for (int i = 0; i < 3; i++) {
                        s.table("t").longColumn("v", i).atNow();   // auto-flush at 2 rows: flagged
                    }
                    s.flush();                                      // row 3 rides in the committing frame
                }
                log.awaitDataFrames(2, 5_000);
                Assert.assertTrue(log.deferFlag(0));
                Assert.assertFalse(log.deferFlag(1));
                Assert.assertEquals(0, log.commitFrames());
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
    public void testRowsWindowShipsFlaggedFramesThenOneCommittingFrame() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "commit_rows=3;commit_interval_ms=60000;housekeeper_interval_ms=60000;"))) {
                QuestDBFlushOnCloseTest.appendThreeRowsInThreeLeases(db);
                log.awaitDataFrames(3, 5_000);
                Assert.assertTrue(log.deferFlag(0));
                Assert.assertTrue(log.deferFlag(1));
                Assert.assertFalse("third close fills the window: unflagged, commits the group", log.deferFlag(2));
                Assert.assertEquals("no separate commit message when the closing frame carries rows", 0, log.commitFrames());
            }
        }
    }

    @Test
    public void testSettleCommitsIdleGroupWithCommitMessage() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // settle cadence = min(auto_flush_interval, commit_interval_ms) = 100 ms
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "commit_rows=1000;commit_interval_ms=100;housekeeper_interval_ms=60000;"))) {
                try (Sender s = db.borrowSender()) {
                    s.table("t").longColumn("v", 1L).atNow();
                }
                log.awaitDataFrames(1, 5_000);
                Assert.assertTrue(log.deferFlag(0));
                log.awaitCommitFrames(1, 3_000);
            }
        }
    }

    @Test
    public void testTerminalOnSecondFrameSurfacesOnNextCallWithItsSequence() throws Exception {
        RejectSecondFrameHandler handler = new RejectSecondFrameHandler(WebSocketResponse.STATUS_SCHEMA_MISMATCH);
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "commit_rows=1000;commit_interval_ms=60000;housekeeper_interval_ms=60000;auto_flush_interval=60000;")
                    + "reconnect_max_duration_millis=500;reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;")) {
                Sender retiredDelegate = null;
                LineSenderServerException seen = null;
                long deadline = System.currentTimeMillis() + 5_000;
                for (long v = 1; seen == null && (v <= 2 || System.currentTimeMillis() < deadline); v++) {
                    // try-with-resources: the typed error stays primary, close()'s wrapper is suppressed
                    try (Sender s = db.borrowSender()) {
                        retiredDelegate = ((PooledSender) s).getDelegateForTesting();
                        s.table("t").longColumn("v", v).atNow();
                    } catch (LineSenderServerException e) {
                        seen = e;
                    } catch (LineSenderException e) {
                        // the terminal latched after the last row call: close() wraps it
                        if (!(e.getCause() instanceof LineSenderServerException)) {
                            throw e;
                        }
                        seen = (LineSenderServerException) e.getCause();
                    }
                    Thread.sleep(20);
                }
                Assert.assertNotNull("the latched terminal must surface on a later call on that slot", seen);
                Assert.assertEquals("it names the rejected frame", 1L, seen.getServerError().getMessageSequence());
                try (Sender s = db.borrowSender()) {
                    Assert.assertNotSame("the pool replaces the retired slot", retiredDelegate, ((PooledSender) s).getDelegateForTesting());
                    s.table("t").longColumn("v", 4L).atNow();
                }
            }
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