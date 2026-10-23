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
import io.questdb.client.test.QuestDBFlushOnCloseTest;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The sender methods the pool relies on, exercised on a real WebSocket sender
 * against the in-repo test server, which acks every frame: the probes it runs
 * at release ({@code checkError()}, {@code hasRowInProgress()} and
 * {@code pendingRowCount()}), the savepoint pair ({@code markSavepoint()} and
 * {@code resetToSavepoint()}) behind a lease's {@code reset()}, the deferred
 * flush of the commit window and the cap-bound byte trigger. In memory mode the
 * sender appends rows into an open record sized to the server's batch cap: a row
 * the record cannot take seals the rows before it into a frame, and a row no
 * record can take fails on its own {@code at()}, so no batch over the cap is ever
 * retained.
 */
public class SenderProbeMethodsTest {

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testAutoFlushIfDueShipsOnlyRowsOlderThanTheInterval() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                    + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=2000;close_flush_timeout_millis=0;")) {
                sender.table("t").longColumn("v", 1L).atNow();
                sender.autoFlushIfDue();
                Assert.assertEquals("younger than the interval: kept", 1, sender.pendingRowCount());
                Thread.sleep(2200);
                sender.autoFlushIfDue();
                Assert.assertEquals("older than the interval: shipped", 0, sender.pendingRowCount());
                log.awaitDataFrames(1, 5_000);
            }
        }
    }

    @Test
    public void testCheckErrorThrowsLatchedTerminal() throws Exception {
        try (TestWebSocketServer server = new TestWebSocketServer(new AckAllHandler())) {
            server.setRejectWithStatus(401, "Unauthorized");
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // async connect: build() returns, the I/O thread hits the 401 wall and latches a terminal
            try (Sender sender = Sender.fromConfig(config(server.getPort())
                    + "initial_connect_retry=async;reconnect_max_duration_millis=300;"
                    + "reconnect_initial_backoff_millis=10;reconnect_max_backoff_millis=50;")) {
                long deadline = System.currentTimeMillis() + 5_000;
                LineSenderException seen = null;
                while (System.currentTimeMillis() < deadline) {
                    try {
                        sender.checkError();
                    } catch (LineSenderException e) {
                        seen = e;
                        break;
                    }
                    Thread.sleep(20);
                }
                Assert.assertNotNull("checkError() must throw the latched auth terminal", seen);
            }
        }
    }

    @Test
    public void testCrossingRowInATransactionShipsTheRowsBeforeItDeferred() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // transaction=on, which a commit window injects: the rows the full record
            // holds ship inside the open transaction, and only flush() commits it
            try (Sender sender = Sender.fromConfig(configWithoutAutoFlushBytes(server.getPort()) + "transaction=on;")) {
                String wide = QuestDBFlushOnCloseTest.wideValue(1000);
                for (int i = 0; i < 3; i++) {
                    sender.table("t").stringColumn("s", wide).atNow();
                }
                sender.table("t").stringColumn("s", QuestDBFlushOnCloseTest.wideValue(1500)).atNow();
                Assert.assertEquals("only the crossing row stays buffered", 1, sender.pendingRowCount());
                log.awaitDataFrames(1, 5_000);
                Assert.assertTrue("the rows before the crossing row ship without committing", log.deferFlag(0));
                Assert.assertTrue(sender.hasDeferredFrames());
                sender.flush();
                log.awaitDataFrames(2, 5_000);
                Assert.assertFalse("flush() commits the transaction with the crossing row", log.deferFlag(1));
                Assert.assertEquals(0, log.commitFrames());
                sender.checkError();
            }
        }
    }

    @Test
    public void testCrossingRowShipsTheRowsBeforeIt() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // no auto_flush_bytes key: the byte trigger sits at 90% of the 4 KiB cap (3686 bytes)
            try (Sender sender = Sender.fromConfig(configWithoutAutoFlushBytes(server.getPort()))) {
                String wide = QuestDBFlushOnCloseTest.wideValue(1000);
                for (int i = 0; i < 3; i++) {
                    sender.table("t").stringColumn("s", wide).atNow();
                }
                // the 4 KiB record has no room for this 1.5 KB row: the sender seals the
                // three rows before it into a frame and the row opens the next record
                sender.table("t").stringColumn("s", QuestDBFlushOnCloseTest.wideValue(1500)).atNow();
                Assert.assertEquals("only the crossing row stays buffered", 1, sender.pendingRowCount());
                log.awaitDataFrames(1, 5_000);
                sender.flush();
                log.awaitDataFrames(2, 5_000);
            }
        }
    }

    @Test
    public void testDefaultsOnNonWebSocketTransport() {
        try (Sender sender = Sender.fromConfig("http::addr=127.0.0.1:1;protocol_version=2;auto_flush=off;")) {
            sender.autoFlushIfDue();                        // default: no-op
            sender.checkError();                            // default: no-op
            Assert.assertFalse(sender.hasRowInProgress());  // default: false
            Assert.assertEquals(0, sender.pendingRowCount()); // default: 0
            Assert.assertEquals(0L, sender.rowsAppended());
            Assert.assertFalse(sender.hasDeferredFrames());
            try {
                sender.flushDeferred();
                Assert.fail("expected UnsupportedOperationException");
            } catch (UnsupportedOperationException expected) {
            }
        }
    }

    @Test
    public void testDeferredFlushShipsFlaggedFrameAndFlushCommits() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(server.getPort()))) {
                Assert.assertEquals(0L, sender.rowsAppended());
                Assert.assertFalse(sender.hasDeferredFrames());
                Assert.assertEquals(-1L, sender.flushDeferred());          // nothing pending

                sender.table("t").longColumn("v", 1L).atNow();
                Assert.assertEquals(1L, sender.rowsAppended());
                Assert.assertTrue(sender.flushDeferred() >= 0L);
                Assert.assertTrue("a deferred frame is open", sender.hasDeferredFrames());
                Assert.assertEquals(0, sender.pendingRowCount());

                sender.table("t").longColumn("v", 2L).atNow();
                sender.flush();                                            // closes the group
                Assert.assertFalse(sender.hasDeferredFrames());
                Assert.assertEquals(2L, sender.rowsAppended());

                log.awaitDataFrames(2, 5_000);
                Assert.assertEquals("no separate commit message: the committing frame carries the rows", 0, log.commitFrames());
                Assert.assertTrue("first frame carries FLAG_DEFER_COMMIT", log.deferFlag(0));
                Assert.assertFalse("committing frame is unflagged", log.deferFlag(1));
            }
        }
    }

    @Test
    public void testFlushAfterAnEmptySavepointResetShipsTheNextRow() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(server.getPort()))) {
                sender.markSavepoint();
                sender.table("t").longColumn("v", 1L).atNow();
                sender.flush();                                        // empties the savepoint
                sender.table("t").longColumn("v", 2L).atNow();
                sender.resetToSavepoint();                             // drops row 2
                sender.table("t").longColumn("v", 3L).atNow();
                sender.flush();
                log.awaitDataFrames(2, 5_000);
                sender.checkError();
            }
        }
    }

    @Test
    public void testFlushEmptiesTheSavepoint() throws Exception {
        try (TestWebSocketServer server = new TestWebSocketServer(new AckAllHandler())) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(server.getPort()))) {
                sender.table("t").longColumn("v", 1L).atNow();
                sender.markSavepoint();
                sender.table("t").longColumn("v", 2L).atNow();
                sender.flush();                                        // ships both: the savepoint is now empty
                sender.table("t").longColumn("v", 3L).atNow();
                sender.resetToSavepoint();
                Assert.assertEquals("a flush inside the lease makes every pending row the lease's own", 0, sender.pendingRowCount());
                sender.checkError();
            }
        }
    }

    @Test
    public void testHealthySenderReportsBufferState() throws Exception {
        try (TestWebSocketServer server = new TestWebSocketServer(new AckAllHandler())) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(server.getPort()))) {
                sender.checkError();                       // healthy: no throw
                Assert.assertFalse(sender.hasRowInProgress());
                Assert.assertEquals(0, sender.pendingRowCount());

                sender.table("t").longColumn("v", 1L);     // row started, not committed
                Assert.assertTrue(sender.hasRowInProgress());
                Assert.assertEquals(0, sender.pendingRowCount());

                sender.cancelRow();
                Assert.assertFalse(sender.hasRowInProgress());

                sender.table("t").longColumn("v", 2L).atNow();
                Assert.assertFalse(sender.hasRowInProgress());
                Assert.assertEquals(1, sender.pendingRowCount());

                sender.flush();
                Assert.assertEquals(0, sender.pendingRowCount());
            }
        }
    }

    @Test
    public void testHeldCrossingRowWithANewSymbolKeepsTheDictionaryGapFree() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(configWithoutAutoFlushBytes(server.getPort()))) {
                Assert.assertTrue("memory mode selects delta mode", ((QwpWebSocketSender) sender).isDeltaDictEnabledForTest());
                String wide = QuestDBFlushOnCloseTest.wideValue(1000);
                for (int i = 0; i < 3; i++) {
                    sender.table("t").symbol("y", "a").stringColumn("s", wide).atNow();
                }
                // the crossing row registers "b", which the full record has no room
                // for: "b" goes into the next record's dictionary with the row
                sender.table("t").symbol("y", "b").stringColumn("s", QuestDBFlushOnCloseTest.wideValue(1500)).atNow();
                sender.flush();
                sender.table("t").symbol("y", "b").atNow();
                sender.flush();
                log.awaitDataFrames(3, 5_000);
                List<String> dict = new ArrayList<>();
                for (byte[] frame : log.frames()) {
                    QwpWireTestUtils.accumulateDeltaDictionary(frame, dict, false);
                }
                Assert.assertEquals(Arrays.asList("a", "b"), dict);
                sender.checkError();
            }
        }
    }

    @Test
    public void testOverCapRowsShipInRecordSizedFramesAndCheckErrorStaysSilent() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(server.getPort()))) {
                Assert.assertEquals("the cap is known once fromConfig returns", 4 * 1024, ((QwpWebSocketSender) sender).getServerMaxBatchSize());
                String wide = QuestDBFlushOnCloseTest.wideValue(1000);
                for (int i = 0; i < 6; i++) {
                    sender.table("t").stringColumn("s", wide).atNow();
                }
                // six 1 KB rows do not fit one 4 KiB record: the record that filled was
                // sealed, and flush() seals the rest
                sender.flush();
                Assert.assertEquals(0, sender.pendingRowCount());
                sender.checkError();                                   // nothing is retained
                log.awaitDataFrames(2, 5_000);
                for (byte[] frame : log.frames()) {
                    Assert.assertTrue("a record's frame fits the cap: " + frame.length, frame.length <= 4 * 1024);
                }
            }
        }
    }

    @Test
    public void testResetToSavepointAfterARecordFilledInTheLeaseDropsOnlyTheUnshippedRows() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(server.getPort()))) {
                sender.table("t").longColumn("v", 1L).atNow();
                sender.markSavepoint();
                String wide = QuestDBFlushOnCloseTest.wideValue(1000);
                for (int i = 0; i < 6; i++) {
                    sender.table("t").stringColumn("s", wide).atNow();
                }
                // the record that filled shipped the row before the mark with the
                // lease rows it held, like any flush inside the lease
                log.awaitDataFrames(1, 5_000);
                int pending = sender.pendingRowCount();
                Assert.assertTrue("rows after the sealed record are pending: " + pending, pending > 0 && pending < 6);
                sender.checkError();                                   // nothing is retained
                sender.resetToSavepoint();
                Assert.assertEquals("the rows the seal did not ship are the lease's own and go", 0, sender.pendingRowCount());
                sender.flush();
                Thread.sleep(100);
                Assert.assertEquals("nothing is left to ship", 1, log.dataFrames());
            }
        }
    }

    @Test
    public void testResetToSavepointKeepsRowsBufferedBeforeTheMark() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(server.getPort()))) {
                sender.table("t1").longColumn("v", 1L).atNow();
                sender.table("t1").longColumn("v", 2L).atNow();
                sender.markSavepoint();
                sender.table("t1").longColumn("v", 3L).stringColumn("extra", "x").atNow();
                sender.table("t2").longColumn("v", 1L).atNow();
                Assert.assertEquals(4, sender.pendingRowCount());

                sender.resetToSavepoint();

                Assert.assertEquals("the two rows before the mark stay", 2, sender.pendingRowCount());
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals("t2 was created after the mark and is gone", 1, log.tableCount(0));
                // the buffer is still usable, including the column the rollback removed
                sender.table("t1").longColumn("v", 4L).stringColumn("extra", "y").atNow();
                sender.flush();
                log.awaitDataFrames(2, 5_000);
            }
        }
    }

    @Test
    public void testResetToSavepointReclaimsTheSymbolIdsOfTheDroppedRows() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(server.getPort()))) {
                sender.table("t").symbol("s", "a").atNow();
                sender.markSavepoint();
                for (int i = 0; i < 6; i++) {
                    sender.table("t").symbol("s", "dropped" + i).atNow();
                }
                Assert.assertEquals(7, sender.pendingRowCount());
                sender.resetToSavepoint();
                Assert.assertEquals("the row before the mark stays", 1, sender.pendingRowCount());
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                // without the reclaim "b" would take id 7 and drag the dropped rows'
                // ids 1..6 into its frame
                sender.table("t").symbol("s", "b").atNow();
                sender.flush();
                log.awaitDataFrames(2, 5_000);
                List<byte[]> frames = log.frames();
                List<String> dict = new ArrayList<>();
                for (byte[] frame : frames) {
                    QwpWireTestUtils.accumulateDeltaDictionary(frame, dict, false);
                }
                Assert.assertEquals(Arrays.asList("a", "b"), dict);
                sender.checkError();
            }
        }
    }

    @Test
    public void testResetToSavepointWithoutMarkIsReset() throws Exception {
        try (TestWebSocketServer server = new TestWebSocketServer(new AckAllHandler())) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(server.getPort()))) {
                sender.table("t").longColumn("v", 1L).atNow();
                sender.table("t").longColumn("v", 2L).atNow();
                sender.resetToSavepoint();
                Assert.assertEquals(0, sender.pendingRowCount());
            }
        }
    }

    @Test
    public void testRowOverTheFrameBoundFailsOnItsAtAfterTheRowsBeforeItShip() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // no auto_flush_bytes key: the byte trigger sits at 90% of the 4 KiB cap (3686 bytes)
            try (Sender sender = Sender.fromConfig(configWithoutAutoFlushBytes(server.getPort()))) {
                String wide = QuestDBFlushOnCloseTest.wideValue(1000);
                for (int i = 0; i < 3; i++) {
                    sender.table("t").stringColumn("s", wide).atNow();
                }
                // 4070 bytes and the record's own overhead exceed a 4 KiB record: the
                // sender seals the rows before it, a fresh record cannot take the row
                // either, and the row fails on its own at()
                assertRowTooLargeForTheFrameBound(sender, "u", "z", 4070);
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals("the three t rows ship as one frame with table t", 1, log.tableCount(0));
                Assert.assertEquals("the failed row is not buffered", 0, sender.pendingRowCount());
                sender.checkError();                                   // nothing is retained
                sender.table("u").longColumn("v", 1L).atNow();
                sender.flush();
                log.awaitDataFrames(2, 5_000);
            }
        }
    }

    @Test
    public void testRowOverTheFrameBoundFailsOnItsAtWithTheByteTriggerOff() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                    + ";auto_flush_bytes=off;auto_flush_rows=4;auto_flush_interval=60000;close_flush_timeout_millis=0;")) {
                String wide = QuestDBFlushOnCloseTest.wideValue(1000);
                for (int i = 0; i < 3; i++) {
                    sender.table("t").stringColumn("s", wide).atNow();
                }
                // the bound is the record's, with or without a byte trigger: the fourth
                // row neither reaches the row trigger nor stays behind
                assertRowTooLargeForTheFrameBound(sender, "u", "z", 4070);
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals("the failed row is not buffered", 0, sender.pendingRowCount());
                sender.checkError();                                   // the pool keeps the slot
            }
        }
    }

    @Test
    public void testRowsOverTheCapWithTheByteTriggerOffShipInRecordSizedFrames() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // auto_flush_bytes=off and a row trigger the rows never reach: only a full
            // record seals, and it does so before the rows exceed the cap
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                    + ";auto_flush_bytes=off;auto_flush_rows=6;auto_flush_interval=60000;close_flush_timeout_millis=0;")) {
                String wide = QuestDBFlushOnCloseTest.wideValue(1000);
                for (int i = 0; i < 6; i++) {
                    sender.table("t").stringColumn("s", wide).atNow();
                }
                log.awaitDataFrames(1, 5_000);
                int pending = sender.pendingRowCount();
                Assert.assertTrue("the record that filled shipped, the rows after it are pending: " + pending,
                        pending > 0 && pending < 6);
                sender.checkError();
                sender.flush();
                log.awaitDataFrames(2, 5_000);
                Assert.assertEquals(0, sender.pendingRowCount());
                for (byte[] frame : log.frames()) {
                    Assert.assertTrue("a record's frame fits the cap: " + frame.length, frame.length <= 4 * 1024);
                }
            }
        }
    }

    @Test
    public void testSingleRowOverTheFrameBoundFailsOnItsAtAndLeavesNothingBuffered() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(configWithoutAutoFlushBytes(server.getPort()))) {
                // the only row: no record of the cap's size can hold it, so it fails
                // on its own at() and nothing of it, its column included, stays behind
                assertRowTooLargeForTheFrameBound(sender, "u", "z", 4070);
                Assert.assertEquals("the row is not buffered", 0, sender.pendingRowCount());
                Assert.assertFalse(sender.hasRowInProgress());
                sender.checkError();                                   // nothing is retained
                sender.table("u").longColumn("v", 1L).atNow();
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                Assert.assertTrue("the next row ships without the failed row's 4 KB column",
                        log.frames().get(0).length < 200);
                sender.checkError();
            }
        }
    }

    @Test
    public void testTryFlushOnAFullRingReturnsFalseAndKeepsTheRows() throws Exception {
        try (TestWebSocketServer server = new TestWebSocketServer(new SilentHandler())) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            String sfDir = temp.newFolder("sf").getAbsolutePath();
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                    + ";sf_dir=" + sfDir + ";sf_max_segment_bytes=1024;sf_max_total_bytes=8192"
                    + ";sf_append_deadline_millis=2000;auto_flush_rows=100000;auto_flush_interval=3600000"
                    + ";auto_flush_bytes=off;close_flush_timeout_millis=0;")) {
                String pad = QuestDBFlushOnCloseTest.wideValue(128);
                boolean full = false;
                for (int i = 0; i < 200 && !full; i++) {
                    sender.table("t").stringColumn("p", pad).longColumn("v", i).atNow();
                    try {
                        sender.flush();
                    } catch (LineSenderException e) {
                        Assert.assertNotNull("backpressure must carry the engine's cause", e.getCause());
                        TestUtils.assertContains(e.getCause().getMessage(), "backpressured");
                        full = true;
                    }
                }
                Assert.assertTrue("setup: the ring must fill", full);
                int pending = sender.pendingRowCount();
                Assert.assertTrue(pending > 0);
                long start = System.nanoTime();
                Assert.assertFalse(sender.tryFlush());
                Assert.assertTrue("tryFlush must not wait out the append deadline",
                        System.nanoTime() - start < TimeUnit.MILLISECONDS.toNanos(1_000));
                Assert.assertEquals(pending, sender.pendingRowCount());
                sender.checkError(); // a busy ring is not an error
                sender.reset();      // so close() has nothing left to publish
            }
        }
    }

    @Test
    public void testTryFlushShipsLikeFlushWhenTheRingHasRoom() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(server.getPort()))) {
                sender.table("t").longColumn("v", 1L).atNow();
                sender.table("t").longColumn("v", 2L).atNow();
                Assert.assertTrue(sender.tryFlush());
                Assert.assertEquals(0, sender.pendingRowCount());
                Assert.assertFalse(sender.hasDeferredFrames());
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals(1, log.dataFrames());
            }
        }
    }

    @Test
    public void testUnsetAutoFlushBytesShipsAtTheAdvertisedCap() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(16 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // no auto_flush_bytes key: the sender binds its byte trigger to 90% of the 16 KiB cap
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                    + ";auto_flush_rows=1000000;auto_flush_interval=60000;close_flush_timeout_millis=0;")) {
                String value = QuestDBFlushOnCloseTest.wideValue(1000);
                for (int i = 0; i < 20; i++) {
                    sender.table("t").stringColumn("s", value).atNow();
                }
                // 20 KB of rows against a 14.7 KB budget in 16 KiB records: the byte
                // trigger or a full record shipped at least once
                log.awaitDataFrames(1, 5_000);
                Assert.assertTrue("rows left after the byte flush", sender.pendingRowCount() > 0 && sender.pendingRowCount() < 20);
            }
        }
    }

    private static void assertRowTooLargeForTheFrameBound(Sender sender, String table, String column, int length) {
        sender.table(table).stringColumn(column, QuestDBFlushOnCloseTest.wideValue(length));
        try {
            sender.atNow();
            Assert.fail("a row no record can take must fail on its own at()");
        } catch (LineSenderException expected) {
            TestUtils.assertContains(expected.getMessage(), "row too large for the frame bound");
            TestUtils.assertContains(expected.getMessage(), "serverMaxBatchSize=4096");
            TestUtils.assertContains(expected.getMessage(), "http.recv.buffer.size");
        }
    }

    private static String config(int port) {
        return "ws::addr=localhost:" + port + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;close_flush_timeout_millis=0;";
    }

    private static String configWithoutAutoFlushBytes(int port) {
        return "ws::addr=localhost:" + port + ";auto_flush_rows=1000000;auto_flush_interval=60000;close_flush_timeout_millis=0;";
    }

    private static final class AckAllHandler implements TestWebSocketServer.WebSocketServerHandler {
        private TestWebSocketServer.ClientHandler currentClient;
        private long nextSeq;

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                nextSeq = 0;
            }
            try {
                client.sendBinary(QwpWireTestUtils.buildAck(nextSeq++));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final class SilentHandler implements TestWebSocketServer.WebSocketServerHandler {
        @Override
        public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            // no ack: the frame stays unacked on disk
        }
    }
}
