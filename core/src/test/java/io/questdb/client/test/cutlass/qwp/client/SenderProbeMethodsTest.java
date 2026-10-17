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
import io.questdb.client.cutlass.qwp.client.BatchTooLargeForCapException;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.test.QuestDBFlushOnCloseTest;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

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
 * flush of the commit window, the cap-bound byte trigger and the crossing row
 * the sender holds back when the cap rejects the auto-flushed batch.
 */
public class SenderProbeMethodsTest {

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
    public void testCheckErrorReportsARetainedOverCapBatchUntilReset() throws Exception {
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
                try {
                    sender.flush();
                    Assert.fail("six 1 KB rows do not fit a 4 KiB cap");
                } catch (BatchTooLargeForCapException expected) {
                    // retained for retry, as the message says
                    Assert.assertTrue(expected.getMessage(), expected.getMessage().contains("call reset()"));
                    Assert.assertFalse("the detail leaves out the standalone remedy", expected.getDetail().contains("reset()"));
                    Assert.assertTrue(expected.getDetail(), expected.getDetail().endsWith("serverMaxBatchSize=4096]"));
                }
                try {
                    sender.checkError();
                    Assert.fail("the probe must report the retained batch");
                } catch (BatchTooLargeForCapException expected) {
                    // the pool retires a slot on this
                }
                sender.reset();
                sender.checkError();                                   // cleared with the batch
                sender.table("t").longColumn("v", 1L).atNow();
                sender.flush();
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
            // transaction=on, which a commit window injects: the rows before the crossing
            // row ship inside the open transaction, and only flush() commits it
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
                Assert.assertFalse("flush() commits the transaction with the held row", log.deferFlag(1));
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
                // the auto-flushed batch would be 4.5 KB, over the cap: the sender ships
                // the three rows before the crossing row and keeps that row
                sender.table("t").stringColumn("s", QuestDBFlushOnCloseTest.wideValue(1500)).atNow();
                Assert.assertEquals("only the crossing row stays buffered", 1, sender.pendingRowCount());
                log.awaitDataFrames(1, 5_000);
                sender.flush();
                log.awaitDataFrames(2, 5_000);
            }
        }
    }

    @Test
    public void testCrossingRowWhoseEarlierRowsStillExceedTheCapThrowsAndKeepsEveryRow() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            // auto_flush_bytes=off: only the row trigger fires, at the sixth row, when the
            // five rows before it already exceed the cap
            try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                    + ";auto_flush_bytes=off;auto_flush_rows=6;auto_flush_interval=60000;close_flush_timeout_millis=0;")) {
                String wide = QuestDBFlushOnCloseTest.wideValue(1000);
                for (int i = 0; i < 5; i++) {
                    sender.table("t").stringColumn("s", wide).atNow();
                }
                BatchTooLargeForCapException rejection = null;
                try {
                    sender.table("t").stringColumn("s", wide).atNow();
                    Assert.fail("the five rows before the crossing row still exceed the 4 KiB cap");
                } catch (BatchTooLargeForCapException expected) {
                    rejection = expected;
                }
                Assert.assertEquals("the crossing row goes back into the batch", 6, sender.pendingRowCount());
                Thread.sleep(300);
                Assert.assertEquals("nothing ships", 0, log.dataFrames());
                try {
                    sender.checkError();
                    Assert.fail("the probe must report the retained batch");
                } catch (BatchTooLargeForCapException expected) {
                    Assert.assertSame("the probe reports the rejection of the whole batch", rejection, expected);
                }
                try {
                    sender.flush();
                    Assert.fail("the retained batch still exceeds the cap");
                } catch (BatchTooLargeForCapException expected) {
                    Assert.assertEquals("flush() encodes the same six rows", rejection.getMessage(), expected.getMessage());
                }
                sender.reset();
            }
        }
    }

    @Test
    public void testCrossingRowWhoseOwnFrameExceedsTheCapThrowsAfterTheRowsBeforeItShip() throws Exception {
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
                // 4070 bytes pass the per-row guard, but the table name and schema take the
                // row's own frame over the cap: the rows before it ship, and the row, alone
                // over the byte trigger, fails its own flush
                BatchTooLargeForCapException rejection = null;
                try {
                    sender.table("u").stringColumn("z", QuestDBFlushOnCloseTest.wideValue(4070)).atNow();
                    Assert.fail("the row's own frame exceeds the 4 KiB cap");
                } catch (BatchTooLargeForCapException expected) {
                    rejection = expected;
                }
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals("the three t rows ship as one frame with table t", 1, log.tableCount(0));
                try {
                    sender.checkError();
                    Assert.fail("the probe must report the retained row");
                } catch (BatchTooLargeForCapException expected) {
                    Assert.assertSame(rejection, expected);
                }
                Assert.assertEquals("only the crossing row stays buffered", 1, sender.pendingRowCount());
                sender.reset();
                Assert.assertEquals(0, sender.pendingRowCount());
                sender.checkError();
            }
        }
    }

    @Test
    public void testCrossingRowWhoseOwnFrameExceedsTheCapFlushesAtOnceWithTheByteTriggerOff() throws Exception {
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
                try {
                    sender.table("u").stringColumn("z", QuestDBFlushOnCloseTest.wideValue(4070)).atNow();
                    Assert.fail("the row's own frame exceeds the 4 KiB cap");
                } catch (BatchTooLargeForCapException expected) {
                    // the held row flushed at once and its own rejection reached this call
                }
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals("only the crossing row stays buffered", 1, sender.pendingRowCount());
                try {
                    sender.checkError();
                    Assert.fail("the probe must report the retained row");
                } catch (BatchTooLargeForCapException expected) {
                    // the pool retires a slot on this
                }
                sender.reset();
                Assert.assertEquals(0, sender.pendingRowCount());
                sender.checkError();
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
                // the held crossing row registers "b" before the frame of the three
                // rows ahead of it ships
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
    public void testResetToSavepointAfterAnOverCapFlushClearsTheProbeAndShipsTheEarlierRows() throws Exception {
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
                try {
                    sender.flush();
                    Assert.fail("the lease's rows do not fit a 4 KiB cap");
                } catch (BatchTooLargeForCapException expected) {
                    // retained for retry
                }
                try {
                    sender.checkError();
                    Assert.fail("the probe must report the retained batch");
                } catch (BatchTooLargeForCapException expected) {
                    // the pool retires a slot on this
                }
                sender.resetToSavepoint();
                sender.checkError();
                Assert.assertEquals(1, sender.pendingRowCount());
                sender.flush();
                log.awaitDataFrames(1, 5_000);
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
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(config(server.getPort()))) {
                sender.table("t").symbol("s", "a").atNow();
                sender.markSavepoint();
                String wide = QuestDBFlushOnCloseTest.wideValue(1000);
                for (int i = 0; i < 6; i++) {
                    sender.table("t").symbol("s", wide + i).atNow();
                }
                try {
                    sender.flush();
                    Assert.fail("six wide symbols do not fit a 4 KiB cap");
                } catch (BatchTooLargeForCapException expected) {
                    // retained for retry
                }
                sender.resetToSavepoint();
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                sender.table("t").symbol("s", "b").atNow();
                sender.flush();
                log.awaitDataFrames(2, 5_000);
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
    public void testSingleRowWhoseOwnFrameExceedsTheCapThrowsAndStaysBuffered() throws Exception {
        QuestDBFlushOnCloseTest.FrameLog log = new QuestDBFlushOnCloseTest.FrameLog();
        try (TestWebSocketServer server = new TestWebSocketServer(log)) {
            server.setAdvertisedMaxBatchSize(4 * 1024);
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            try (Sender sender = Sender.fromConfig(configWithoutAutoFlushBytes(server.getPort()))) {
                // the crossing row is the only pending row: no rows before it can ship,
                // so the sender holds nothing back and the rejection reaches this call
                BatchTooLargeForCapException rejection = null;
                try {
                    sender.table("u").stringColumn("z", QuestDBFlushOnCloseTest.wideValue(4070)).atNow();
                    Assert.fail("the only pending row's own frame exceeds the 4 KiB cap");
                } catch (BatchTooLargeForCapException expected) {
                    rejection = expected;
                }
                Assert.assertEquals("the row stays buffered", 1, sender.pendingRowCount());
                try {
                    sender.checkError();
                    Assert.fail("the probe must report the retained row");
                } catch (BatchTooLargeForCapException expected) {
                    Assert.assertSame(rejection, expected);
                }
                sender.reset();
                sender.table("u").longColumn("v", 1L).atNow();
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                sender.checkError();
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
                // 20 KB of rows against a 14.7 KB budget: the byte trigger shipped at least once
                log.awaitDataFrames(1, 5_000);
                Assert.assertTrue("rows left after the byte flush", sender.pendingRowCount() > 0 && sender.pendingRowCount() < 20);
            }
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
}
