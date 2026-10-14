package io.questdb.client.test;

import io.questdb.client.QuestDB;
import io.questdb.client.Sender;
import io.questdb.client.test.cutlass.qwp.client.QwpWireTestUtils;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Close releases, flush ships, drain waits: observed on the wire through the
 * in-repo test server. Each test counts the data frames (table count above
 * zero) the server receives.
 */
public class QuestDBFlushOnCloseTest {

    @Test
    public void testCloseShipsNothingUntilSettleOrFlush() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // settle cadence 60 s: nothing ships inside the test window
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "auto_flush_interval=60000;housekeeper_interval_ms=60000;"))) {
                appendThreeRowsInThreeLeases(db);
                Thread.sleep(300);
                Assert.assertEquals("three closes must not ship", 0, log.dataFrames());
                try (Sender s = db.borrowSender()) {
                    s.flush();
                }
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals("explicit flush ships one frame holding all three rows", 1, log.dataFrames());
            }
        }
    }

    @Test
    public void testFlushOnCloseShipsOneFramePerClose() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "flush_on_close=on;housekeeper_interval_ms=60000;"))) {
                appendThreeRowsInThreeLeases(db);
                log.awaitDataFrames(3, 5_000);
                Assert.assertEquals(3, log.dataFrames());
                for (int i = 0; i < 3; i++) {
                    Assert.assertFalse("no commit window: frames are not flagged", log.deferFlag(i));
                }
            }
        }
    }

    @Test
    public void testSettleShipsOnDefaultCadenceWhenIntervalUnset() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // no auto_flush_interval: the settle cadence defaults to 100 ms, independent of the 60 s reap cadence
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "housekeeper_interval_ms=60000;"))) {
                appendThreeRowsInThreeLeases(db);
                log.awaitDataFrames(1, 2_000);
                Thread.sleep(300);
                Assert.assertEquals("the settle ships one frame and nothing else follows", 1, log.dataFrames());
            }
        }
    }

    @Test
    public void testSettleShipsWithinAutoFlushInterval() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "auto_flush_interval=100;housekeeper_interval_ms=60000;"))) {
                appendThreeRowsInThreeLeases(db);
                log.awaitDataFrames(1, 2_000);
                Thread.sleep(300);
                Assert.assertEquals("the settle ships one frame and nothing else follows", 1, log.dataFrames());
            }
        }
    }

    static void appendThreeRowsInThreeLeases(QuestDB db) {
        for (int i = 0; i < 3; i++) {
            try (Sender s = db.borrowSender()) {
                s.table("t").longColumn("v", i).atNow();
            }
        }
    }

    static String config(int port, String extra) {
        return "ws::addr=localhost:" + port
                + ";sender_pool_min=1;sender_pool_max=1"
                + ";query_pool_min=0;query_pool_max=1"
                + ";initial_connect_retry=async"
                + ";close_flush_timeout_millis=0;"
                + extra;
    }

    /**
     * Parses every QWP frame in each WebSocket message (a read can carry more
     * than one on replay), records its flags byte and table count, and acks
     * cumulatively so the sender never backs up. Public: the sender-level
     * probe test in a sub-package and the benchmark reuse it.
     */
    public static final class FrameLog implements TestWebSocketServer.WebSocketServerHandler {
        private static final int HEADER_SIZE = 12;
        private static final int OFFSET_FLAGS = 5;
        private static final int OFFSET_PAYLOAD_LENGTH = 8;
        private static final int OFFSET_TABLE_COUNT = 6;
        private final List<Integer> flags = new ArrayList<>();
        private final List<Integer> tableCounts = new ArrayList<>();
        private TestWebSocketServer.ClientHandler currentClient;
        private long nextSeq;

        public void awaitCommitFrames(int atLeast, long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            while (commitFrames() < atLeast && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            Assert.assertTrue("expected at least " + atLeast + " commit frame(s) within " + timeoutMillis + " ms, got " + commitFrames(),
                    commitFrames() >= atLeast);
        }

        public void awaitDataFrames(int atLeast, long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            while (dataFrames() < atLeast && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            Assert.assertTrue("expected at least " + atLeast + " data frame(s) within " + timeoutMillis + " ms, got " + dataFrames(),
                    dataFrames() >= atLeast);
        }

        /**
         * Count of rowless frames with the defer flag clear: the commit messages.
         */
        public synchronized int commitFrames() {
            int n = 0;
            for (int i = 0, size = tableCounts.size(); i < size; i++) {
                if (tableCounts.get(i) == 0 && (flags.get(i) & 0x01) == 0) {
                    n++;
                }
            }
            return n;
        }

        public synchronized int dataFrames() {
            int n = 0;
            for (int i = 0, size = tableCounts.size(); i < size; i++) {
                if (tableCounts.get(i) > 0) {
                    n++;
                }
            }
            return n;
        }

        /**
         * Defer flag of the i-th data frame (frames with a table count above zero).
         */
        public synchronized boolean deferFlag(int dataFrameIndex) {
            int seen = -1;
            for (int i = 0, size = tableCounts.size(); i < size; i++) {
                if (tableCounts.get(i) > 0 && ++seen == dataFrameIndex) {
                    return (flags.get(i) & 0x01) != 0;
                }
            }
            throw new AssertionError("no data frame at index " + dataFrameIndex);
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                nextSeq = 0;
            }
            int pos = 0;
            int framesInMessage = 0;
            while (pos + HEADER_SIZE <= data.length) {
                int tableCount = (data[pos + OFFSET_TABLE_COUNT] & 0xFF) | ((data[pos + OFFSET_TABLE_COUNT + 1] & 0xFF) << 8);
                long payloadLength = (data[pos + OFFSET_PAYLOAD_LENGTH] & 0xFFL)
                        | ((data[pos + OFFSET_PAYLOAD_LENGTH + 1] & 0xFFL) << 8)
                        | ((data[pos + OFFSET_PAYLOAD_LENGTH + 2] & 0xFFL) << 16)
                        | ((data[pos + OFFSET_PAYLOAD_LENGTH + 3] & 0xFFL) << 24);
                flags.add(data[pos + OFFSET_FLAGS] & 0xFF);
                tableCounts.add(tableCount);
                framesInMessage++;
                pos += HEADER_SIZE + (int) payloadLength;
            }
            nextSeq += Math.max(1, framesInMessage);
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq - 1));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
