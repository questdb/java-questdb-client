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

import io.questdb.client.QuestDB;
import io.questdb.client.LineSenderServerException;
import io.questdb.client.Sender;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.qwp.client.BatchTooLargeForCapException;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.impl.PooledSender;
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
    public void testCrossingRowOverTheMarginDropsOnlyThatLeasesRowsAndShipsTheRest() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // auto_flush_bytes unset: the byte trigger sits at 90 percent of the 4 KiB cap (3686 bytes)
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "auto_flush_interval=60000;housekeeper_interval_ms=60000;"))) {
                // warm-up lease: proves the connection and the advertised cap are in place
                // (initial_connect_retry=async learns the cap on the I/O thread)
                try (Sender s = db.borrowSender()) {
                    s.table("w").longColumn("v", 1L).atNow();
                    s.flush();
                }
                log.awaitDataFrames(1, 5_000);
                String wide = wideValue(1000);
                for (int i = 0; i < 3; i++) {
                    try (Sender s = db.borrowSender()) {
                        s.table("t").stringColumn("s", wide).atNow();    // 3 KB in all, below the trigger
                    }
                }
                Sender delegate = null;
                boolean hasCloseThrown = false;
                try (Sender s = db.borrowSender()) {
                    delegate = ((PooledSender) s).getDelegateForTesting();
                    try {
                        s.table("t").stringColumn("s", wideValue(1500)).atNow();
                        Assert.fail("the crossing row makes the auto-flushed batch 4.5 KB, over the 4 KiB cap");
                    } catch (BatchTooLargeForCapException expected) {
                        // the row stays committed and the rejection is retained
                    }
                } catch (LineSenderException e) {
                    hasCloseThrown = true;
                    Assert.assertTrue("close() wraps the rejection", e.getCause() instanceof BatchTooLargeForCapException);
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("dropped 1 row(s) of this lease"));
                }
                Assert.assertTrue("close() must report the dropped row", hasCloseThrown);
                log.awaitDataFrames(2, 5_000);
                Assert.assertEquals("the three earlier rows ship as one frame with table t", 1, log.tableCount(1));
                try (Sender s = db.borrowSender()) {
                    Assert.assertSame("the slot stayed in service", delegate, ((PooledSender) s).getDelegateForTesting());
                    Assert.assertEquals("only the crossing row was dropped, the rest shipped", 0, s.pendingRowCount());
                }
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
    public void testHandleCloseShipsRowsLeftInSlot() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            QuestDB db = QuestDB.connect(config(server.getPort(),
                    "housekeeper_interval_ms=60000;auto_flush_interval=60000;close_flush_timeout_millis=5000;"));
            appendThreeRowsInThreeLeases(db);
            db.close();
            log.awaitDataFrames(1, 5_000);
        }
    }

    @Test
    public void testLaterLeaseResetKeepsEarlierLeasesRows() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "auto_flush_interval=60000;housekeeper_interval_ms=60000;"))) {
                try (Sender a = db.borrowSender()) {
                    a.table("t1").longColumn("v", 1L).atNow();
                }
                try (Sender b = db.borrowSender()) {
                    b.table("t2").longColumn("v", 2L).atNow();
                    b.reset();                                       // B drops its own bad batch
                }
                try (Sender c = db.borrowSender()) {
                    Assert.assertEquals("A's row is still buffered", 1, c.pendingRowCount());
                    c.flush();
                }
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals("one frame, one table: t1 only", 1, log.tableCount(0));
                Assert.assertEquals(1, log.dataFrames());
            }
        }
    }

    @Test
    public void testOverCapBatchRetiresTheSlotInsteadOfPoisoningIt() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // auto_flush_bytes=off on purpose: defeat the cap-bound byte trigger so the batch outgrows the cap
            try (QuestDB db = QuestDB.connect(config(server.getPort(),
                    "auto_flush_bytes=off;auto_flush_interval=60000;housekeeper_interval_ms=60000;"))) {
                // warm-up lease: proves the connection and the advertised cap are in place
                // (initial_connect_retry=async learns the cap on the I/O thread)
                try (Sender s = db.borrowSender()) {
                    s.table("w").longColumn("v", 1L).atNow();
                    s.flush();
                }
                log.awaitDataFrames(1, 5_000);
                String wide = wideValue(1000);
                for (int i = 0; i < 6; i++) {
                    try (Sender s = db.borrowSender()) {
                        s.table("t").stringColumn("s", wide).atNow();
                    }
                }
                Sender poisoned = null;
                boolean hasCloseThrown = false;
                try (Sender s = db.borrowSender()) {
                    poisoned = ((PooledSender) s).getDelegateForTesting();
                    try {
                        s.flush();
                        Assert.fail("six 1 KB rows do not fit a 4 KiB cap");
                    } catch (BatchTooLargeForCapException expected) {
                        // the retained batch belongs to nobody: close() must retire the slot
                    }
                } catch (LineSenderException e) {
                    hasCloseThrown = true;
                    Assert.assertTrue("close() wraps the retained rejection", e.getCause() instanceof BatchTooLargeForCapException);
                    Assert.assertTrue(e.getMessage(), e.getMessage().contains("6 unshipped row(s)"));
                }
                Assert.assertTrue("close() must report the retirement", hasCloseThrown);
                try (Sender s = db.borrowSender()) {
                    Assert.assertNotSame("the retired slot is replaced", poisoned, ((PooledSender) s).getDelegateForTesting());
                    s.table("t").stringColumn("s", wide).atNow();
                    s.flush();
                }
                log.awaitDataFrames(2, 5_000);
                Assert.assertEquals("the over-cap batch never ships", 2, log.dataFrames());
            }
        }
    }

    @Test
    public void testRowOverTheCapFailsOnlyThatRowAndKeepsTheSlot() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "auto_flush_interval=60000;housekeeper_interval_ms=60000;"))) {
                // warm-up lease: proves the connection and the advertised cap are in place
                // (initial_connect_retry=async learns the cap on the I/O thread)
                try (Sender s = db.borrowSender()) {
                    s.table("w").longColumn("v", 1L).atNow();
                    s.flush();
                }
                log.awaitDataFrames(1, 5_000);
                Sender delegate;
                try (Sender s = db.borrowSender()) {
                    delegate = ((PooledSender) s).getDelegateForTesting();
                    try {
                        s.table("t").stringColumn("s", wideValue(5000)).atNow();
                        Assert.fail("a 5 KB row cannot fit a 4 KiB cap");
                    } catch (LineSenderException expected) {
                        Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("row too large"));
                    }
                    s.table("t").longColumn("v", 1L).atNow();           // the lease carries on
                }
                try (Sender s = db.borrowSender()) {
                    Assert.assertSame("the slot stayed in service", delegate, ((PooledSender) s).getDelegateForTesting());
                    Assert.assertEquals(1, s.pendingRowCount());
                    s.flush();
                }
                log.awaitDataFrames(2, 5_000);
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
                Assert.assertTrue("a settle tick landing mid-loop may split the rows, got " + log.dataFrames(),
                        log.dataFrames() >= 1 && log.dataFrames() <= 3);
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
                Assert.assertTrue("a settle tick landing mid-loop may split the rows, got " + log.dataFrames(),
                        log.dataFrames() >= 1 && log.dataFrames() <= 3);
            }
        }
    }

    @Test
    public void testTerminalRejectionDropsEveryLeaseRowInTheFrame() throws Exception {
        RejectFirstFrameHandler handler = new RejectFirstFrameHandler();
        try (TestWebSocketServer server = new TestWebSocketServer(handler)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "auto_flush_interval=60000;housekeeper_interval_ms=60000;")
                    + "reconnect_max_duration_millis=500;reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;")) {
                try (Sender a = db.borrowSender()) {
                    a.table("t1").longColumn("v", 1L).atNow();       // a clean row
                }
                try (Sender b = db.borrowSender()) {
                    b.table("t2").longColumn("v", 2L).atNow();       // the row the server will reject
                }
                LineSenderServerException seen = null;
                long deadline = System.currentTimeMillis() + 5_000;
                for (int i = 0; seen == null && System.currentTimeMillis() < deadline; i++) {
                    try (Sender c = db.borrowSender()) {
                        if (i == 0) {
                            c.flush();                                // ships A's and B's rows in ONE frame
                        } else {
                            c.table("t1").longColumn("v", 3L).atNow();
                        }
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
                Assert.assertNotNull("the terminal surfaces on a later lease, not on A or B", seen);
                Assert.assertEquals("both leases' rows shared the rejected frame", 1, handler.firstConnectionDataFrames());
                Assert.assertEquals(2, handler.firstConnectionTableCount());
                try (Sender d = db.borrowSender()) {
                    d.table("t1").longColumn("v", 4L).atNow();
                    d.flush();
                }
                handler.awaitLaterConnectionFrames(1, 5_000);
                Assert.assertEquals("nothing is replayed after a terminal: only D's frame arrives on the fresh slot",
                        1, handler.laterConnectionDataFrames());
            }
        }
    }

    @Test
    public void testThresholdOnLaterLeaseShipsEarlierRows() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (QuestDB db = QuestDB.connect(config(server.getPort(),
                    "auto_flush_rows=3;auto_flush_interval=60000;housekeeper_interval_ms=60000;"))) {
                appendThreeRowsInThreeLeases(db);
                log.awaitDataFrames(1, 5_000);
            }
        }
    }

    @Test
    public void testWideRowsAcrossLeasesShipAtTheServerCapNotAtTheRowThreshold() throws Exception {
        FrameLog log = new FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(16 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // settle 60 s, row threshold 1000: only the cap-bound byte trigger can ship inside the test
            try (QuestDB db = QuestDB.connect(config(server.getPort(), "auto_flush_interval=60000;housekeeper_interval_ms=60000;"))) {
                String wide = wideValue(1000);
                // initial_connect_retry=async: the cap is learned on the I/O thread's
                // handshake, and rows appended before it carry no byte trigger. Keep
                // appending (far below the 1000-row threshold) until a frame ships.
                for (int i = 0; i < 200 && log.dataFrames() < 1; i++) {
                    try (Sender s = db.borrowSender()) {
                        s.table("t").stringColumn("s", wide).atNow();
                    }
                    Thread.sleep(10);
                }
                log.awaitDataFrames(1, 5_000);
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

    public static String wideValue(int length) {
        char[] chars = new char[length];
        java.util.Arrays.fill(chars, 'x');
        return new String(chars);
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
                if (payloadLength < 0 || pos + HEADER_SIZE + payloadLength > data.length) {
                    break;
                }
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

        /**
         * Table count of the i-th data frame (frames with a table count above zero).
         */
        public synchronized int tableCount(int dataFrameIndex) {
            int seen = -1;
            for (int i = 0, size = tableCounts.size(); i < size; i++) {
                if (tableCounts.get(i) > 0 && ++seen == dataFrameIndex) {
                    return tableCounts.get(i);
                }
            }
            throw new AssertionError("no data frame at index " + dataFrameIndex);
        }
    }

    /**
     * First connection: NACKs its first data frame with SCHEMA_MISMATCH (a
     * terminal) and acks nothing else. Later connections: ack everything and
     * count their data frames, so a replay would show up there.
     */
    private static final class RejectFirstFrameHandler implements TestWebSocketServer.WebSocketServerHandler {
        private static final int OFFSET_TABLE_COUNT = 6;
        private int connections;
        private TestWebSocketServer.ClientHandler currentClient;
        private int firstConnectionDataFrames;
        private int firstConnectionTableCount;
        private int laterConnectionDataFrames;
        private long nextSeq;

        void awaitLaterConnectionFrames(int atLeast, long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            while (laterConnectionDataFrames() < atLeast && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
        }

        synchronized int firstConnectionDataFrames() {
            return firstConnectionDataFrames;
        }

        synchronized int firstConnectionTableCount() {
            return firstConnectionTableCount;
        }

        synchronized int laterConnectionDataFrames() {
            return laterConnectionDataFrames;
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                connections++;
                nextSeq = 0;
            }
            long seq = nextSeq++;
            int tableCount = data.length >= 8 ? ((data[OFFSET_TABLE_COUNT] & 0xFF) | ((data[OFFSET_TABLE_COUNT + 1] & 0xFF) << 8)) : 0;
            try {
                if (connections == 1) {
                    if (tableCount > 0) {
                        firstConnectionDataFrames++;
                        firstConnectionTableCount = tableCount;
                        client.sendBinary(QwpWireTestUtils.buildNack(seq, WebSocketResponse.STATUS_SCHEMA_MISMATCH));
                    }
                    // rowless frames on the first connection: no ack, no nack
                } else {
                    if (tableCount > 0) {
                        laterConnectionDataFrames++;
                    }
                    client.sendBinary(QwpWireTestUtils.buildAck(seq));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
