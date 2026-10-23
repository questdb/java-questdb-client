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
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.QuestDBFlushOnCloseTest;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * The WebSocket sender's open-record path in memory mode, on the wire: rows go
 * into an open record in the store-and-forward ring, {@code publishPending()}
 * makes them recoverable without shipping them, and a seal ({@code flush()}, an
 * auto-flush threshold, close) turns the record into one QWP frame. Every frame
 * is compared byte for byte with the frame the legacy encoder builds for the same
 * rows, so the record path changes nothing a server can see.
 */
public class QwpOpenRecordSenderTest {
    private static final long TS_BASE = 1_700_000_000_000_000L;

    @Test
    public void testAutoFlushIntervalSealsOnNextAppend() throws Exception {
        assertMemoryLeak(() -> {
            QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                         + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=50;close_flush_timeout_millis=0;")) {
                QwpTableBuffer t = oracle.block("t");
                longRow(sender, t, "v", 1);
                Thread.sleep(100);
                Assert.assertEquals("nothing checks the age between rows", 0, log.frames().size());
                longRow(sender, t, "v", 2);
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals("the second row's append found the record too old and sealed it", 0, sender.pendingRowCount());
                Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
            }
        });
    }

    @Test
    public void testAutoFlushRowsSealsAFrame() throws Exception {
        assertMemoryLeak(() -> {
            QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                         + ";auto_flush_bytes=off;auto_flush_rows=2;auto_flush_interval=60000;close_flush_timeout_millis=0;")) {
                QwpTableBuffer t = oracle.block("t");
                longRow(sender, t, "v", 1);
                Assert.assertEquals(1, sender.pendingRowCount());
                longRow(sender, t, "v", 2);
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals(0, sender.pendingRowCount());
                Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
                sender.table("t").longColumn("v", 3).at(TS_BASE + 3, ChronoUnit.MICROS);
                Assert.assertEquals("the third row opens the next record", 1, sender.pendingRowCount());
            }
        });
    }

    @Test
    public void testCancelRowKeepsEarlierRows() throws Exception {
        assertMemoryLeak(() -> {
            QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                QwpTableBuffer t = oracle.block("t");
                longRow(sender, t, "v", 1);
                sender.table("t").longColumn("v", 2).symbol("s", "gone");
                Assert.assertTrue(sender.hasRowInProgress());
                sender.cancelRow();
                Assert.assertFalse(sender.hasRowInProgress());
                Assert.assertEquals("the cancelled row never reached the record", 1, sender.pendingRowCount());
                longRow(sender, t, "v", 3);
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                // the oracle has neither the cancelled row's column nor its symbol
                Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
            }
        });
    }

    @Test
    public void testCloseSealsAndDrains() throws Exception {
        assertMemoryLeak(() -> {
            QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle()) {
                QwpTableBuffer t = oracle.block("t");
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;close_flush_timeout_millis=5000;")) {
                    longRow(sender, t, "v", 1);
                    longRow(sender, t, "v", 2);
                    sender.publishPending();
                    Assert.assertEquals(0, log.frames().size());
                }
                // close sealed the record and waited for its ack, which the server
                // sends after it logs the frame
                Assert.assertEquals(1, log.dataFrames());
                Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
            }
        });
    }

    @Test
    public void testFlushDeferredCarriesTheFlagAndFlushCommits() throws Exception {
        assertMemoryLeak(() -> {
            QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                QwpTableBuffer t = oracle.block("t");
                longRow(sender, t, "v", 1);
                Assert.assertTrue("the seal assigns an FSN", sender.flushDeferred() >= 0L);
                Assert.assertTrue(sender.hasDeferredFrames());
                Assert.assertEquals(0, sender.pendingRowCount());
                log.awaitDataFrames(1, 5_000);
                Assert.assertTrue(log.deferFlag(0));
                Assert.assertArrayEquals(oracle.frame(-1, true), dataFrame(log, 0));

                sender.flush();
                Assert.assertFalse(sender.hasDeferredFrames());
                log.awaitCommitFrames(1, 5_000);
                Assert.assertEquals("the rowless flush sent the commit message", 1, log.dataFrames());
            }
        });
    }

    @Test
    public void testPendingRowCountCountsRecordRows() throws Exception {
        assertMemoryLeak(() -> {
            QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                QwpTableBuffer t = oracle.block("t");
                Assert.assertEquals(0, sender.pendingRowCount());
                longRow(sender, t, "v", 1);
                Assert.assertEquals(1, sender.pendingRowCount());
                longRow(sender, t, "v", 2);
                Assert.assertEquals(2, sender.pendingRowCount());
                sender.publishPending();
                Assert.assertEquals("published rows are still pending until a seal ships them", 2, sender.pendingRowCount());
                sender.table("t").longColumn("v", 3).at(TS_BASE + 3, ChronoUnit.MICROS);
                Assert.assertEquals(3, sender.pendingRowCount());
                sender.reset();
                Assert.assertEquals("reset drops only the unpublished row", 2, sender.pendingRowCount());
                sender.flush();
                Assert.assertEquals(0, sender.pendingRowCount());
                Assert.assertEquals(3L, sender.rowsAppended());
                log.awaitDataFrames(1, 5_000);
                Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
            }
        });
    }

    @Test
    public void testPublishPendingShipsNothingAndFlushShipsAll() throws Exception {
        assertMemoryLeak(() -> {
            QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                Assert.assertTrue("memory mode selects delta mode", ((QwpWebSocketSender) sender).isDeltaDictEnabledForTest());
                QwpTableBuffer t = oracle.block("t");
                longRow(sender, t, "v", 1);
                longRow(sender, t, "v", 2);
                sender.publishPending();
                Thread.sleep(200);
                Assert.assertEquals("a publish ships nothing", 0, log.frames().size());
                Assert.assertFalse(sender.hasDeferredFrames());
                longRow(sender, t, "v", 3);
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals(1, log.frames().size());
                Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
            }
        });
    }

    @Test
    public void testResetDropsOnlyUnpublishedRows() throws Exception {
        assertMemoryLeak(() -> {
            QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                QwpTableBuffer t = oracle.block("t");
                longRow(sender, t, "v", 1);
                sender.publishPending();
                sender.table("t").longColumn("v", 2).at(TS_BASE + 2, ChronoUnit.MICROS);
                sender.reset();
                Assert.assertEquals(1, sender.pendingRowCount());
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
            }
        });
    }

    @Test
    public void testResetDropsTheColumnsOfUnpublishedRows() throws Exception {
        assertMemoryLeak(() -> {
            QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                QwpTableBuffer t = oracle.block("t");
                longRow(sender, t, "v", 1);
                sender.publishPending();
                sender.table("t").longColumn("v", 2).stringColumn("extra", "x").at(TS_BASE + 2, ChronoUnit.MICROS);
                sender.table("u").longColumn("w", 1).at(TS_BASE, ChronoUnit.MICROS);
                sender.reset();
                // the next row has no "extra": a column only a dropped row used must not ship null-padded
                longRow(sender, t, "v", 3);
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
            }
        });
    }

    @Test
    public void testRowWithColumnsInDifferentOrderLandsByName() throws Exception {
        assertMemoryLeak(() -> {
            QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                QwpTableBuffer t = oracle.block("t");
                sender.table("t").doubleColumn("a", 1).longColumn("b", 2).atNow();
                t.getOrCreateColumn("a", QwpConstants.TYPE_DOUBLE, true).addDouble(1);
                t.getOrCreateColumn("b", QwpConstants.TYPE_LONG, true).addLong(2);
                t.nextRow();
                sender.table("t").longColumn("b", 4).doubleColumn("a", 3).atNow();
                t.getOrCreateColumn("b", QwpConstants.TYPE_LONG, true).addLong(4);
                t.getOrCreateColumn("a", QwpConstants.TYPE_DOUBLE, true).addDouble(3);
                t.nextRow();
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals(1, log.tableCount(0));
                Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
            }
        });
    }

    @Test
    public void testRowsAppendedThenFlushArriveAsOneFrame() throws Exception {
        assertMemoryLeak(() -> {
            QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                QwpTableBuffer t = oracle.block("t");
                for (int i = 0; i < 3; i++) {
                    longRow(sender, t, "v", i);
                }
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals(1, log.dataFrames());
                Assert.assertEquals(1, log.tableCount(0));
                Assert.assertFalse(log.deferFlag(0));
                Assert.assertArrayEquals("the record materialises to the legacy encoder's frame",
                        oracle.frame(-1, false), dataFrame(log, 0));
            }
        });
    }

    @Test
    public void testSymbolsRegisteredOnceAcrossReleases() throws Exception {
        assertMemoryLeak(() -> {
            QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                QwpTableBuffer t = oracle.block("t");
                String[] symbols = {"a", "a", "a", "b"};
                for (int i = 0; i < symbols.length; i++) {
                    sender.table("t").symbol("s", symbols[i]).longColumn("v", i).at(TS_BASE + i, ChronoUnit.MICROS);
                    t.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true)
                            .addSymbolWithGlobalId(symbols[i], oracle.dict.getOrAddSymbol(symbols[i]));
                    t.getOrCreateColumn("v", QwpConstants.TYPE_LONG, true).addLong(i);
                    t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + i);
                    t.nextRow();
                    sender.publishPending();
                }
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                byte[] frame = dataFrame(log, 0);
                int[] position = {QwpConstants.HEADER_SIZE};
                Assert.assertEquals("delta start", 0, QwpWireTestUtils.readVarint(frame, position));
                Assert.assertEquals("one entry per distinct symbol", 2, QwpWireTestUtils.readVarint(frame, position));
                List<String> dictionary = new ArrayList<>();
                QwpWireTestUtils.accumulateDeltaDictionary(frame, dictionary);
                Assert.assertEquals(Arrays.asList("a", "b"), dictionary);
                Assert.assertArrayEquals(oracle.frame(-1, false), frame);
            }
        });
    }

    @Test
    public void testTwoTablesAndASchemaChangeProduceThreeBlocks() throws Exception {
        assertMemoryLeak(() -> {
            QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                QwpTableBuffer t1 = oracle.block("t1");
                QwpTableBuffer t2 = oracle.block("t2");
                // the record splits t1 into two blocks; the oracle models each with its own buffer
                QwpTableBuffer t1WithX = oracle.block("t1");
                longRow(sender, t1, "v", 1);
                sender.table("t2").doubleColumn("w", 2.5).at(TS_BASE + 2, ChronoUnit.MICROS);
                t2.getOrCreateColumn("w", QwpConstants.TYPE_DOUBLE, true).addDouble(2.5);
                t2.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + 2);
                t2.nextRow();
                sender.table("t1").longColumn("v", 3).stringColumn("x", "new").at(TS_BASE + 3, ChronoUnit.MICROS);
                t1WithX.getOrCreateColumn("v", QwpConstants.TYPE_LONG, true).addLong(3);
                t1WithX.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + 3);
                t1WithX.getOrCreateColumn("x", QwpConstants.TYPE_VARCHAR, true).addString("new");
                t1WithX.nextRow();
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals(3, log.tableCount(0));
                Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
            }
        });
    }

    private static String config(int port) {
        return "ws::addr=localhost:" + port + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;close_flush_timeout_millis=0;";
    }

    private static byte[] dataFrame(QuestDBFlushOnCloseTest.FrameLog log, int index) {
        int seen = -1;
        for (byte[] frame : log.frames()) {
            if (QwpWireTestUtils.tableCount(frame) > 0 && ++seen == index) {
                return frame;
            }
        }
        throw new AssertionError("no data frame at index " + index);
    }

    /**
     * Appends {@code table(name).longColumn(column, value).at(TS_BASE + value)} to
     * the sender and the same row to the oracle's block {@code t}.
     */
    private static void longRow(Sender sender, QwpTableBuffer t, String column, long value) {
        sender.table(t.getTableName()).longColumn(column, value).at(TS_BASE + value, ChronoUnit.MICROS);
        t.getOrCreateColumn(column, QwpConstants.TYPE_LONG, true).addLong(value);
        t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + value);
        t.nextRow();
    }

    private static TestWebSocketServer startServer(TestWebSocketServer.WebSocketServerHandler handler) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(handler);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    /**
     * The expected frame: one legacy table buffer per frame block, encoded by the
     * production {@link QwpWebSocketEncoder} against a dictionary that mints the
     * same ids as the sender's.
     */
    private static final class Oracle implements QuietCloseable {
        final ArrayList<QwpTableBuffer> blocks = new ArrayList<>();
        final GlobalSymbolDictionary dict = new GlobalSymbolDictionary();
        final QwpWebSocketEncoder encoder = new QwpWebSocketEncoder();

        QwpTableBuffer block(String table) {
            QwpTableBuffer t = new QwpTableBuffer(table);
            blocks.add(t);
            return t;
        }

        @Override
        public void close() {
            for (int i = 0, n = blocks.size(); i < n; i++) {
                blocks.get(i).close();
            }
            encoder.close();
        }

        /**
         * Encodes the non-empty blocks, in order, with a delta section after
         * {@code confirmedMaxId} up to the last id the dictionary minted.
         */
        byte[] frame(int confirmedMaxId, boolean deferCommit) {
            int tableCount = 0;
            for (int i = 0, n = blocks.size(); i < n; i++) {
                if (blocks.get(i).getRowCount() > 0) {
                    tableCount++;
                }
            }
            encoder.setDeferCommit(deferCommit);
            encoder.beginMessage(tableCount, dict, confirmedMaxId, dict.size() - 1);
            for (int i = 0, n = blocks.size(); i < n; i++) {
                if (blocks.get(i).getRowCount() > 0) {
                    encoder.addTable(blocks.get(i));
                }
            }
            int size = encoder.finishMessage();
            long ptr = encoder.getBuffer().getBufferPtr();
            byte[] bytes = new byte[size];
            for (int i = 0; i < size; i++) {
                bytes[i] = Unsafe.getUnsafe().getByte(ptr + i);
            }
            return bytes;
        }
    }
}
