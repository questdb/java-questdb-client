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
import io.questdb.client.cutlass.qwp.client.GlobalSymbolDictionary;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketEncoder;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.WebSocketResponse;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.client.sf.cursor.SegmentRing;
import io.questdb.client.cutlass.qwp.client.sf.record.QwoFormat;
import io.questdb.client.cutlass.qwp.protocol.QwpConstants;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.QuietCloseable;
import io.questdb.client.std.Unsafe;
import io.questdb.client.test.cutlass.qwp.client.QwpSfTestUtils.FrameLog;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

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
            FrameLog log = new FrameLog();
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
            FrameLog log = new FrameLog();
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
    public void testBlockRowLimitSealsAtOneMillionRows() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle first = new Oracle();
                 Oracle second = new Oracle();
                 Oracle third = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                Assert.assertEquals(1_000_000, QwoFormat.MAX_ROWS_PER_BLOCK);
                QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                ws.setMaxRowsPerBlockForTest(1000);
                final Oracle[] oracles = {first, second, third};
                final QwpTableBuffer[] blocks = {first.block("t"), second.block("t"), third.block("t")};
                for (int i = 0; i < 2500; i++) {
                    longRow(sender, blocks[i / 1000], "v", i);
                }
                Assert.assertEquals("the full blocks sealed inside at()", 500, sender.pendingRowCount());
                Assert.assertEquals(3, ws.recordOpensForTest());
                sender.flush();
                log.awaitDataFrames(3, 5_000);
                Assert.assertEquals(3, log.dataFrames());
                for (int f = 0; f < 3; f++) {
                    Assert.assertArrayEquals("frame " + f, oracles[f].frame(-1, false), dataFrame(log, f));
                }
            }
        });
    }

    @Test
    public void testCancelRowKeepsEarlierRows() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
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
    public void testCancelledRowKeepsTheGeohashPrecisionAndDecimalScale() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                QwpTableBuffer t = oracle.block("t");
                // row 1 sets the precision to 20 bits and the scale to 2
                sender.table("t").geoHashColumn("g", 0x12345, 20).decimalColumn("d", Decimal64.fromLong(125, 2))
                        .at(TS_BASE + 1, ChronoUnit.MICROS);
                t.getOrCreateColumn("g", QwpConstants.TYPE_GEOHASH, true).addGeoHash(0x12345, 20);
                t.getOrCreateColumn("d", QwpConstants.TYPE_DECIMAL64, true).addDecimal64(Decimal64.fromLong(125, 2));
                t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + 1);
                t.nextRow();
                // row 2 sets both again and is cancelled
                sender.table("t").geoHashColumn("g", 0x54321, 20).decimalColumn("d", Decimal64.fromLong(7, 2));
                sender.cancelRow();
                // the staging buffer kept the precision across the cancel, as it does across an appended row
                try {
                    sender.table("t").geoHashColumn("g", 1, 30);
                    Assert.fail("another precision must be refused");
                } catch (LineSenderException expected) {
                    TestUtils.assertContains(expected.getMessage(), "precision mismatch");
                }
                // and the scale: a value at scale 3 is rescaled to 2, so the row joins row 1's block
                sender.table("t").geoHashColumn("g", 0x11111, 20).decimalColumn("d", Decimal64.fromLong(1500, 3))
                        .at(TS_BASE + 3, ChronoUnit.MICROS);
                t.getOrCreateColumn("g", QwpConstants.TYPE_GEOHASH, true).addGeoHash(0x11111, 20);
                t.getOrCreateColumn("d", QwpConstants.TYPE_DECIMAL64, true).addDecimal64(Decimal64.fromLong(1500, 3));
                t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + 3);
                t.nextRow();
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals("one block: the cancel did not change the column's prefix", 1, log.tableCount(0));
                Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));
            }
        });
    }

    @Test
    public void testCancelledRowSymbolIsReclaimedAndRegisteredOnce() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                QwpTableBuffer t = oracle.block("t");
                // the cancelled row mints id 0 for NEW and hands it back
                sender.table("t").symbol("s", "NEW");
                sender.cancelRow();
                Assert.assertEquals(0, ((QwpWebSocketSender) sender).getGlobalSymbolDictionaryForTest().size());
                // so the next new symbol takes id 0 and NEW, used for real, comes after it
                symbolRow(sender, oracle, t, "A", 1);
                symbolRow(sender, oracle, t, "NEW", 2);
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                byte[] frame = dataFrame(log, 0);
                List<String> dictionary = new ArrayList<>();
                QwpWireTestUtils.accumulateDeltaDictionary(frame, dictionary);
                Assert.assertEquals("NEW is registered once, by the row that used it", Arrays.asList("A", "NEW"), dictionary);
                // the frame equals the oracle's, whose second row references id 1: NEW
                Assert.assertEquals(1, oracle.dict.getOrAddSymbol("NEW"));
                Assert.assertArrayEquals(oracle.frame(-1, false), frame);
            }
        });
    }

    @Test
    public void testCloseSealsAndDrains() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
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
    public void testDecimalScaleBindsPerRecord() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle first = new Oracle();
                 Oracle second = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                // the first record binds the column's scale to its first value's, 1
                decimalRow(sender, first.block("t"), Decimal64.fromLong(105, 1), 1);
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                Assert.assertArrayEquals(first.frame(-1, false), dataFrame(log, 0));
                // the next record binds its own: 10.25 keeps scale 2, as in the next batch on the frame path
                decimalRow(sender, second.block("t"), Decimal64.fromLong(1025, 2), 2);
                sender.flush();
                log.awaitDataFrames(2, 5_000);
                Assert.assertArrayEquals(second.frame(-1, false), dataFrame(log, 1));

                // within one record the first scale binds, and a publish does not unbind it
                sender.table("t").decimalColumn("d", Decimal64.fromLong(105, 1)).at(TS_BASE + 3, ChronoUnit.MICROS);
                sender.publishPending();
                try {
                    sender.table("t").decimalColumn("d", Decimal64.fromLong(1025, 2)).at(TS_BASE + 4, ChronoUnit.MICROS);
                    Assert.fail("a finer scale within one record cannot be rescaled without precision loss");
                } catch (LineSenderException expected) {
                    TestUtils.assertContains(expected.getMessage(), "cannot rescale");
                }
                Assert.assertEquals("the earlier row stays", 1, sender.pendingRowCount());
            }
        });
    }

    @Test
    public void testFlushDeferredCarriesTheFlagAndFlushCommits() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
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
    public void testOversizeSymbolFailsOnItsWriter() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            try (TestWebSocketServer server = startServer(log, 64 * 1024);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                final String huge = TestUtils.repeat("h", 100 * 1024);
                try {
                    sender.table("t").symbol("s", huge).longColumn("v", 1).at(TS_BASE + 1, ChronoUnit.MICROS);
                    Assert.fail("a symbol larger than any record's dictionary must fail its row");
                } catch (LineSenderException expected) {
                    TestUtils.assertContains(expected.getMessage(),
                            "a single symbol value is too large for the frame bound; use a varchar column or shorten it");
                }
                Assert.assertEquals(0, sender.pendingRowCount());
                Assert.assertEquals("the failed row handed its symbol id back",
                        0, ((QwpWebSocketSender) sender).getGlobalSymbolDictionaryForTest().size());

                // the next row ships, and without the oversize text
                QwpTableBuffer t = oracle.block("t");
                sender.table("t").symbol("s", "small").longColumn("v", 2).at(TS_BASE + 2, ChronoUnit.MICROS);
                t.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true).addSymbolWithGlobalId("small", oracle.dict.getOrAddSymbol("small"));
                t.getOrCreateColumn("v", QwpConstants.TYPE_LONG, true).addLong(2);
                t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + 2);
                t.nextRow();
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals(1, log.dataFrames());
                byte[] frame = dataFrame(log, 0);
                Assert.assertTrue("frame of " + frame.length + " bytes", frame.length < 1024);
                Assert.assertArrayEquals(oracle.frame(-1, false), frame);
            }
        });
    }

    @Test
    public void testPendingRowCountCountsRecordRows() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
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
            FrameLog log = new FrameLog();
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
    public void testRecordSealFailureLatchesTheSenderTerminal() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle()) {
                Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;close_flush_timeout_millis=5000;");
                try {
                    QwpTableBuffer t = oracle.block("t");
                    longRow(sender, t, "v", 1);
                    sender.flush();
                    log.awaitDataFrames(1, 5_000);
                    Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, 0));

                    // an unpublished row whose reservation the ring then gives up: the seal that
                    // follows closes the writer and fails in the engine
                    sender.table("t").longColumn("v", 2).at(TS_BASE + 2, ChronoUnit.MICROS);
                    CursorSendEngine engine = ((QwpWebSocketSender) sender).getCursorEngineForTesting();
                    engine.abandonRecord(engine.getRingForTesting().getActive().reservedOffset(), engine.reservedRecordCapacity());
                    LineSenderException sealFailure = null;
                    try {
                        sender.flush();
                        Assert.fail("the seal must fail");
                    } catch (LineSenderException expected) {
                        sealFailure = expected;
                        TestUtils.assertContains(expected.getMessage(), "seal");
                        Assert.assertTrue(String.valueOf(expected.getCause()), expected.getCause() instanceof IllegalStateException);
                    }
                    // every later call reports that failure, not the closed writer's "record is not open"
                    for (int i = 0; i < 2; i++) {
                        try {
                            sender.table("t").longColumn("v", 3).at(TS_BASE + 3, ChronoUnit.MICROS);
                            Assert.fail("the sender is terminal");
                        } catch (LineSenderException expected) {
                            TestUtils.assertContains(expected.getMessage(), "terminal");
                            Assert.assertSame(sealFailure, expected.getCause());
                        }
                    }
                } finally {
                    // close() drains what shipped and does not trip over the closed writer
                    sender.close();
                }
                Assert.assertEquals(1, log.dataFrames());
            }
        });
    }

    @Test
    public void testRecordSealFailureOnAnIllegalArgumentSkipsTheCommitAtClose() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle()) {
                QwpWebSocketSender sender = (QwpWebSocketSender) Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                        + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;close_flush_timeout_millis=5000;");
                try {
                    // a transaction in progress: the first record is sealed, its commit deferred
                    sender.setDeferCommit(true);
                    QwpTableBuffer t = oracle.block("t");
                    longRow(sender, t, "v", 1);
                    sender.flush();
                    log.awaitDataFrames(1, 5_000);
                    Assert.assertArrayEquals(oracle.frame(-1, true), dataFrame(log, 0));
                    Assert.assertTrue(sender.hasDeferredMessagesForTesting());
                    Assert.assertTrue("the server acknowledged the first record", sender.awaitAckedFsn(0, 5_000));

                    // swap the second record's reservation for one smaller than its rows: the engine's
                    // seal throws an IllegalArgumentException before it clears the reservation, which
                    // the ring then keeps
                    sender.table("t").longColumn("v", 2).at(TS_BASE + 2, ChronoUnit.MICROS);
                    CursorSendEngine engine = sender.getCursorEngineForTesting();
                    long reserved = engine.getRingForTesting().getActive().reservedOffset();
                    engine.abandonRecord(reserved, engine.reservedRecordCapacity());
                    Assert.assertEquals(reserved, engine.getRingForTesting().reserveRecord(QwoFormat.HEADER_SIZE));
                    LineSenderException sealFailure = null;
                    try {
                        sender.flush();
                        Assert.fail("the seal must fail");
                    } catch (LineSenderException expected) {
                        sealFailure = expected;
                        TestUtils.assertContains(expected.getMessage(), "seal");
                        Assert.assertTrue(String.valueOf(expected.getCause()), expected.getCause() instanceof IllegalArgumentException);
                    }
                    try {
                        sender.table("t").longColumn("v", 3).at(TS_BASE + 3, ChronoUnit.MICROS);
                        Assert.fail("the sender is terminal");
                    } catch (LineSenderException expected) {
                        TestUtils.assertContains(expected.getMessage(), "terminal");
                        Assert.assertSame(sealFailure, expected.getCause());
                    }
                } finally {
                    // the transaction ends: close() would send the commit message, which the ring would
                    // refuse ("record reserved") and which would commit the first record of a transaction
                    // whose last record was lost; close() skips it and drains
                    sender.setDeferCommit(false);
                    sender.close();
                }
                Assert.assertEquals("only the first record shipped, and no commit message followed", 1, log.frames().size());
            }
        });
    }

    @Test
    public void testReservationTimeoutWithADeadLoopReportsTheLoopsError() throws Exception {
        assertMemoryLeak(() -> {
            NackOnDemandHandler handler = new NackOnDemandHandler();
            try (TestWebSocketServer server = startServer(handler)) {
                // two 128 KiB segments and no acks: the ring fills after a few records
                CursorSendEngine engine = new CursorSendEngine(null, 128 * 1024, 256L * 1024, TimeUnit.SECONDS.toNanos(5));
                QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", server.getPort(), null,
                        1_000_000, 0, 0L, null, false, engine, 0L);
                AtomicReference<Throwable> killerFailure = new AtomicReference<>();
                Thread killer = new Thread(() -> {
                    // once the producer waits for ring space, the server rejects the first frame and the I/O loop dies
                    // (nack() does nothing before a frame reached the server, so wait for that too)
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                    while ((engine.getCountersForTesting().backpressureStalls.get() == 0 || !handler.hasClient())
                            && System.nanoTime() < deadline) {
                        LockSupport.parkNanos(1_000_000);
                    }
                    if (engine.getCountersForTesting().backpressureStalls.get() == 0 || !handler.hasClient()) {
                        killerFailure.set(new AssertionError("no stall or no frame at the server within 10 s [stalls="
                                + engine.getCountersForTesting().backpressureStalls.get() + ", client=" + handler.hasClient() + ']'));
                        return;
                    }
                    try {
                        handler.nack();
                    } catch (Throwable t) {
                        killerFailure.set(t);
                    }
                });
                killer.start();
                LineSenderException failure = null;
                try {
                    // three 30 KiB rows fill a segment, leaving less than the default minimum
                    // reservation (a quarter segment); each frame fits the test server's 64 KiB buffer
                    final String value = TestUtils.repeat("r", 30 * 1024);
                    for (int i = 0; i < 100 && failure == null; i++) {
                        try {
                            sender.table("t").stringColumn("s", value).at(TS_BASE + i, ChronoUnit.MICROS);
                            sender.flush();
                        } catch (LineSenderException e) {
                            failure = e;
                        }
                    }
                    killer.join(30_000);
                    Throwable killerError = killerFailure.get();
                    if (killerError != null) {
                        throw new AssertionError(killerError.getMessage(), killerError);
                    }
                    Assert.assertNotNull("the ring never filled", failure);
                    Assert.assertTrue("the producer waited for ring space", engine.getCountersForTesting().backpressureStalls.get() > 0);
                    for (Throwable t = failure; t != null; t = t.getCause()) {
                        Assert.assertFalse("the loop's error, not the deadline: " + t.getMessage(),
                                t.getMessage() != null && t.getMessage().contains("backpressured"));
                    }
                    try {
                        sender.flush();
                        Assert.fail("the sender is terminal");
                    } catch (LineSenderException expected) {
                        Assert.assertSame("the reservation reported the I/O loop's own terminal error", failure, expected);
                    }
                } finally {
                    try {
                        sender.close();
                    } catch (LineSenderException ignored) {
                        // close() may surface the loop's terminal error again
                    }
                }
            }
        });
    }

    @Test
    public void testResetDropsOnlyUnpublishedRows() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
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
            FrameLog log = new FrameLog();
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
    public void testRowLargerThanAnyRecordFailsOnItsOwnAt() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            try (TestWebSocketServer server = startServer(log, 64 * 1024);
                 Oracle first = new Oracle();
                 Oracle second = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                final String wide = TestUtils.repeat("w", 100 * 1024);
                assertRowTooLarge(sender, wide);
                Assert.assertEquals(0, sender.pendingRowCount());

                // a following 10-byte row flushes fine
                longRow(sender, first.block("t"), "v", 1);
                sender.flush();
                log.awaitDataFrames(1, 5_000);
                Assert.assertArrayEquals(first.frame(-1, false), dataFrame(log, 0));

                // after earlier rows: the refusal seals them into their own frame before the row fails,
                // so nothing stays pending
                longRow(sender, second.block("t"), "v", 2);
                assertRowTooLarge(sender, wide);
                Assert.assertEquals(0, sender.pendingRowCount());
                log.awaitDataFrames(2, 5_000);
                Assert.assertEquals(2, log.dataFrames());
                Assert.assertArrayEquals(second.frame(-1, false), dataFrame(log, 1));
            }
        });
    }

    @Test
    public void testRowLargerThanRemainingReservationSealsAndReopensOnce() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            // a 48 KiB cap keeps every frame within the test server's 64 KiB receive buffer
            try (TestWebSocketServer server = startServer(log, 48 * 1024);
                 Oracle first = new Oracle();
                 Oracle second = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                // 70 rows of 1 KiB: about 45 fill the first record (its chunks double up to 16 rows,
                // then shrink to what is left), the rest go to the second
                final int rows = 70;
                final String value = TestUtils.repeat("v", 1024);
                for (int i = 0; i < rows; i++) {
                    sender.table("t").stringColumn("s", value).at(TS_BASE + i, ChronoUnit.MICROS);
                }
                QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                Assert.assertEquals(2, ws.recordOpensForTest());
                int inSecond = sender.pendingRowCount();
                Assert.assertTrue("rows in the second record: " + inSecond, inSecond > 0 && inSecond < rows);
                sender.flush();
                log.awaitDataFrames(2, 5_000);
                Assert.assertEquals(2, log.dataFrames());
                // every row arrives, in order, split where the first record filled
                QwpTableBuffer a = first.block("t");
                QwpTableBuffer b = second.block("t");
                for (int i = 0; i < rows; i++) {
                    QwpTableBuffer target = i < rows - inSecond ? a : b;
                    target.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(value);
                    target.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + i);
                    target.nextRow();
                }
                Assert.assertArrayEquals(first.frame(-1, false), dataFrame(log, 0));
                Assert.assertArrayEquals(second.frame(-1, false), dataFrame(log, 1));
                Assert.assertEquals(2, ws.recordOpensForTest());
            }
        });
    }

    @Test
    public void testRowLargerThanRoomLeftAfterASealedRecordShipsThroughAFullCapacityRecord() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            // a 60 KiB cap keeps every frame within the test server's 64 KiB receive buffer
            try (TestWebSocketServer server = startServer(log, 60 * 1024);
                 Oracle small = new Oracle();
                 Oracle big = new Oracle()) {
                // 128 KiB segments: the default minimum reservation is a quarter of one, below the cap
                CursorSendEngine engine = new CursorSendEngine(null, 128 * 1024, 64L * 1024 * 1024,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS);
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", server.getPort(), null,
                        1_000_000, 0, 0L, null, false, engine, 5_000L)) {
                    SegmentRing ring = engine.getRingForTesting();
                    final long defaultMinimum = Math.min(QwoFormat.MIN_RECORD_RESERVATION, engine.segmentPayloadCapacity() / 4);
                    final int rowBytes = 50 * 1024;
                    // fill the active segment with sealed records until the room left is below the row
                    final String filler = TestUtils.repeat("f", 20 * 1024);
                    int fillers = 0;
                    while (ring.getActive().capacityRemaining() >= rowBytes) {
                        sender.table("fill").stringColumn("s", filler).at(TS_BASE + fillers, ChronoUnit.MICROS);
                        sender.flush();
                        fillers++;
                    }
                    // the first record takes a small row, so the room it leaves once sealed is
                    // still at least the default minimum but below the big row
                    final String little = TestUtils.repeat("l", 4 * 1024);
                    long room = ring.getActive().capacityRemaining();
                    Assert.assertTrue("room left " + room, room >= defaultMinimum + 5 * 1024 && room < rowBytes);
                    sender.table("small").stringColumn("s", little).at(TS_BASE, ChronoUnit.MICROS);
                    Assert.assertEquals(1, sender.pendingRowCount());
                    MmapSegment filled = ring.getActive();
                    int opens = sender.recordOpensForTest();

                    // the big row is under the cap but neither the first record nor the room left once it
                    // is sealed holds it. Trace: the first record refuses and, holding rows, is sealed
                    // (frame 1, no open); the short retry record opens (+1) and refuses, and is given
                    // back; the full-capacity record opens (+1) in a fresh segment and takes the row
                    final String large = TestUtils.repeat("b", rowBytes);
                    sender.table("big").stringColumn("s", large).at(TS_BASE + 1, ChronoUnit.MICROS);
                    Assert.assertEquals("the short retry record, then a full-capacity one",
                            opens + 2, sender.recordOpensForTest());
                    Assert.assertNotSame("the full-capacity reservation rotated the active segment", filled, ring.getActive());
                    Assert.assertEquals("the small row's record was sealed", 1, sender.pendingRowCount());
                    sender.flush();
                    log.awaitDataFrames(fillers + 2, 5_000);

                    QwpTableBuffer s = small.block("small");
                    s.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(little);
                    s.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE);
                    s.nextRow();
                    QwpTableBuffer b = big.block("big");
                    b.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(large);
                    b.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + 1);
                    b.nextRow();
                    Assert.assertArrayEquals(small.frame(-1, false), dataFrame(log, fillers));
                    Assert.assertArrayEquals(big.frame(-1, false), dataFrame(log, fillers + 1));
                    Assert.assertEquals(fillers + 2, log.dataFrames());
                }
            }
        });
    }

    @Test
    public void testRowLargerThanTheRoomLeftShipsInAFreshSegment() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            // a 60 KiB cap keeps every frame within the test server's 64 KiB receive buffer
            try (TestWebSocketServer server = startServer(log, 60 * 1024);
                 Oracle oracle = new Oracle()) {
                // 128 KiB segments: the default minimum reservation is a quarter of one, below the cap
                CursorSendEngine engine = new CursorSendEngine(null, 128 * 1024, 64L * 1024 * 1024,
                        CursorSendEngine.DEFAULT_APPEND_DEADLINE_NANOS);
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect("localhost", server.getPort(), null,
                        1_000_000, 0, 0L, null, false, engine, 5_000L)) {
                    Assert.assertTrue(sender.isDeltaDictEnabledForTest());
                    SegmentRing ring = engine.getRingForTesting();
                    final long defaultMinimum = Math.min(QwoFormat.MIN_RECORD_RESERVATION, engine.segmentPayloadCapacity() / 4);
                    final int rowBytes = 50 * 1024;
                    // fill the active segment with sealed records until the room left is below the row
                    final String filler = TestUtils.repeat("f", 20 * 1024);
                    int fillers = 0;
                    while (ring.getActive().capacityRemaining() >= rowBytes) {
                        sender.table("fill").stringColumn("s", filler).at(TS_BASE + fillers, ChronoUnit.MICROS);
                        sender.flush();
                        fillers++;
                    }
                    long room = ring.getActive().capacityRemaining();
                    Assert.assertTrue("room left " + room, room >= defaultMinimum && room < rowBytes);
                    MmapSegment filled = ring.getActive();
                    int opens = sender.recordOpensForTest();

                    // the row is under the cap, but neither the room left nor a retry there holds it
                    final String big = TestUtils.repeat("b", rowBytes);
                    sender.table("big").stringColumn("s", big).at(TS_BASE, ChronoUnit.MICROS);
                    Assert.assertEquals("the room left, the retry there, then a full-capacity record",
                            opens + 3, sender.recordOpensForTest());
                    Assert.assertNotSame("the full-capacity reservation rotated the active segment", filled, ring.getActive());
                    Assert.assertEquals(1, sender.pendingRowCount());
                    sender.flush();
                    log.awaitDataFrames(fillers + 1, 5_000);
                    QwpTableBuffer t = oracle.block("big");
                    t.getOrCreateColumn("s", QwpConstants.TYPE_VARCHAR, true).addString(big);
                    t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE);
                    t.nextRow();
                    Assert.assertArrayEquals(oracle.frame(-1, false), dataFrame(log, fillers));
                }
            }
        });
    }

    @Test
    public void testRowRefusedByAFullRecordKeepsItsDecimalScale() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            String pad = TestUtils.repeat("x", 1200);
            try (TestWebSocketServer server = startServer(log, 4096);
                 Oracle first = new Oracle();
                 Oracle second = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                // one table per row: two rows fill the 4 KiB record, the third is refused, the
                // record seals and the row is retried in the next record with the scale it has
                paddedDecimalRow(sender, first.block("t1"), pad, 1);
                paddedDecimalRow(sender, first.block("t2"), pad, 2);
                Assert.assertEquals(1, ws.recordOpensForTest());
                paddedDecimalRow(sender, second.block("t3"), pad, 3);
                Assert.assertEquals("the refused row opened the next record", 2, ws.recordOpensForTest());
                sender.flush();
                log.awaitDataFrames(2, 5_000);
                Assert.assertArrayEquals(first.frame(-1, false), dataFrame(log, 0));
                Assert.assertArrayEquals("the refused row keeps its scale 2", second.frame(-1, false), dataFrame(log, 1));
            }
        });
    }

    @Test
    public void testRowWithColumnsInDifferentOrderLandsByName() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
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
            FrameLog log = new FrameLog();
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
    public void testSealedRecordAboveADeltaStartReplaysByteIdenticalAfterACatchUp() throws Exception {
        // The first record registers "a" and is acknowledged; the second starts its dictionary
        // at id 1 and is not. The reconnect re-registers the shipped dictionary with a rowless
        // catch-up frame, which covers the ids below the record's delta start, and then replays
        // the second record, byte for byte.
        assertMemoryLeak(() -> {
            ReplayLog log = new ReplayLog(1);
            try (TestWebSocketServer server = startServer(log);
                 Oracle first = new Oracle();
                 Oracle second = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                symbolRow(sender, first, first.block("t"), "a", 1);
                Assert.assertTrue("the first record is acknowledged", sender.awaitAckedFsn(sender.flushAndGetSequence(), 5_000));
                Assert.assertArrayEquals(first.frame(-1, false), log.awaitDataMessage(0, 0, 5_000));

                second.dict.getOrAddSymbol("a");
                QwpTableBuffer t = second.block("t");
                symbolRow(sender, second, t, "b", 2);
                symbolRow(sender, second, t, "a", 3);
                sender.flush();
                byte[] sent = log.awaitDataMessage(0, 1, 5_000);
                int[] position = {QwpConstants.HEADER_SIZE};
                Assert.assertEquals("delta start", 1, QwpWireTestUtils.readVarint(sent, position));
                Assert.assertArrayEquals(second.frame(0, false), sent);

                server.dropAllConnections();
                byte[] replayed = log.awaitDataMessage(1, 0, 10_000);
                Assert.assertArrayEquals("the replay is the frame first sent, byte for byte", sent, replayed);
                List<byte[]> reconnected = log.messages(1);
                Assert.assertEquals("the catch-up comes first and carries no rows", 0, QwpWireTestUtils.tableCount(reconnected.get(0)));
                List<String> dictionary = new ArrayList<>();
                QwpWireTestUtils.accumulateDeltaDictionary(reconnected.get(0), dictionary);
                Assert.assertEquals("every id shipped so far", Arrays.asList("a", "b"), dictionary);
            }
        });
    }

    @Test
    public void testSealedRecordReplaysByteIdenticalAfterAReconnect() throws Exception {
        assertMemoryLeak(() -> {
            ReplayLog log = new ReplayLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                QwpTableBuffer t = oracle.block("t");
                symbolRow(sender, oracle, t, "a", 1);
                symbolRow(sender, oracle, t, "b", 2);
                symbolRow(sender, oracle, t, "a", 3);
                sender.flush();
                byte[] sent = log.awaitDataMessage(0, 5_000);
                Assert.assertArrayEquals(oracle.frame(-1, false), sent);

                // the first connection acknowledges nothing, so the reconnect replays the record
                server.dropAllConnections();
                byte[] replayed = log.awaitDataMessage(1, 10_000);
                Assert.assertArrayEquals("the replay is the frame first sent, byte for byte", sent, replayed);
            }
        });
    }

    @Test
    public void testSymbolsRegisteredOnceAcrossReleases() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
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
    public void testThousandTablesRoundRobinFitInOneRecord() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
            try (TestWebSocketServer server = startServer(log);
                 Oracle oracle = new Oracle();
                 Sender sender = Sender.fromConfig(config(server.getPort()))) {
                final int tables = 1000;
                for (int i = 0; i < tables; i++) {
                    String table = "t" + i;
                    String symbol = "s" + i % 10;
                    sender.table(table).symbol("sym", symbol).doubleColumn("price", i * 0.5).longColumn("qty", i)
                            .at(TS_BASE + i, ChronoUnit.MICROS);
                    QwpTableBuffer t = oracle.block(table);
                    t.getOrCreateColumn("sym", QwpConstants.TYPE_SYMBOL, true).addSymbolWithGlobalId(symbol, oracle.dict.getOrAddSymbol(symbol));
                    t.getOrCreateColumn("price", QwpConstants.TYPE_DOUBLE, true).addDouble(i * 0.5);
                    t.getOrCreateColumn("qty", QwpConstants.TYPE_LONG, true).addLong(i);
                    t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + i);
                    t.nextRow();
                }
                QwpWebSocketSender ws = (QwpWebSocketSender) sender;
                Assert.assertEquals(tables, sender.pendingRowCount());
                Assert.assertEquals(1, ws.recordOpensForTest());
                int used = ws.recordUsedBytesForTest();
                sender.flush();
                Assert.assertEquals(0, ws.recordUsedBytesForTest());
                log.awaitDataFrames(1, 5_000);
                Assert.assertEquals(1, log.dataFrames());
                Assert.assertEquals(tables, log.tableCount(0));
                byte[] frame = dataFrame(log, 0);
                Assert.assertArrayEquals(oracle.frame(-1, false), frame);
                // A one-row block spends per column a 9-byte def tail and a 12-byte chunk header plus its
                // bitmap byte where the wire spends 2 bytes, and 20 bytes of descriptor header per block:
                // with these narrow rows the record runs at about 2.9 times the frame (161 KB vs 56 KB).
                Assert.assertTrue("record bytes " + used + ", frame bytes " + frame.length, used < 3 * frame.length);
            }
        });
    }

    @Test
    public void testTwoTablesAndASchemaChangeProduceThreeBlocks() throws Exception {
        assertMemoryLeak(() -> {
            FrameLog log = new FrameLog();
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

    private static void assertRowTooLarge(Sender sender, String value) {
        try {
            sender.table("t").stringColumn("s", value).at(TS_BASE, ChronoUnit.MICROS);
            Assert.fail("a row larger than any record must fail on its own at()");
        } catch (LineSenderException expected) {
            TestUtils.assertContains(expected.getMessage(), "row too large for the frame bound");
            // the row adds no symbol
            TestUtils.assertContains(expected.getMessage(), ", rowDictBytes=0, ");
            TestUtils.assertContains(expected.getMessage(), "http.recv.buffer.size");
        }
    }

    private static String config(int port) {
        return "ws::addr=localhost:" + port + ";auto_flush_bytes=off;auto_flush_rows=1000000;auto_flush_interval=60000;close_flush_timeout_millis=0;";
    }

    static byte[] dataFrame(FrameLog log, int index) {
        int seen = -1;
        for (byte[] frame : log.frames()) {
            if (QwpWireTestUtils.tableCount(frame) > 0 && ++seen == index) {
                return frame;
            }
        }
        throw new AssertionError("no data frame at index " + index);
    }

    /**
     * Appends {@code table(t).decimalColumn("d", value).at(TS_BASE + ts)} to the
     * sender and the same row to the oracle's block {@code t}.
     */
    private static void decimalRow(Sender sender, QwpTableBuffer t, Decimal64 value, long ts) {
        sender.table(t.getTableName()).decimalColumn("d", value).at(TS_BASE + ts, ChronoUnit.MICROS);
        t.getOrCreateColumn("d", QwpConstants.TYPE_DECIMAL64, true).addDecimal64(value);
        t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + ts);
        t.nextRow();
    }

    /**
     * Appends {@code table(name).longColumn(column, value).at(TS_BASE + value)} to
     * the sender and the same row to the oracle's block {@code t}.
     */
    static void longRow(Sender sender, QwpTableBuffer t, String column, long value) {
        sender.table(t.getTableName()).longColumn(column, value).at(TS_BASE + value, ChronoUnit.MICROS);
        t.getOrCreateColumn(column, QwpConstants.TYPE_LONG, true).addLong(value);
        t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + value);
        t.nextRow();
    }

    /**
     * Appends {@code table(t).decimalColumn("d", 1.25).stringColumn("p", pad).at(TS_BASE + ts)}
     * to the sender and the same row to the oracle's block {@code t}.
     */
    private static void paddedDecimalRow(Sender sender, QwpTableBuffer t, String pad, long ts) {
        Decimal64 value = Decimal64.fromLong(125, 2);
        sender.table(t.getTableName()).decimalColumn("d", value).stringColumn("p", pad).at(TS_BASE + ts, ChronoUnit.MICROS);
        t.getOrCreateColumn("d", QwpConstants.TYPE_DECIMAL64, true).addDecimal64(value);
        t.getOrCreateColumn("p", QwpConstants.TYPE_VARCHAR, true).addString(pad);
        t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + ts);
        t.nextRow();
    }

    private static TestWebSocketServer startServer(TestWebSocketServer.WebSocketServerHandler handler) throws Exception {
        return startServer(handler, 0);
    }

    /**
     * Starts a server advertising {@code maxBatchSize} as its batch cap; 0
     * advertises none.
     */
    private static TestWebSocketServer startServer(TestWebSocketServer.WebSocketServerHandler handler, int maxBatchSize) throws Exception {
        TestWebSocketServer server = new TestWebSocketServer(handler);
        server.setAdvertisedMaxBatchSize(maxBatchSize);
        server.start();
        Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
        return server;
    }

    /**
     * Appends {@code table(t).symbol("s", symbol).at(TS_BASE + value)} to the
     * sender and the same row to the oracle's block {@code t}.
     */
    static void symbolRow(Sender sender, Oracle oracle, QwpTableBuffer t, String symbol, long value) {
        sender.table(t.getTableName()).symbol("s", symbol).at(TS_BASE + value, ChronoUnit.MICROS);
        t.getOrCreateColumn("s", QwpConstants.TYPE_SYMBOL, true).addSymbolWithGlobalId(symbol, oracle.dict.getOrAddSymbol(symbol));
        t.getOrCreateDesignatedTimestampColumn(QwpConstants.TYPE_TIMESTAMP).addLong(TS_BASE + value);
        t.nextRow();
    }

    /**
     * Logs nothing and acks nothing; {@link #nack()} rejects the first frame of
     * the connection with a terminal PARSE_ERROR.
     */
    private static final class NackOnDemandHandler implements TestWebSocketServer.WebSocketServerHandler {
        private TestWebSocketServer.ClientHandler client;

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            this.client = client;
        }

        synchronized boolean hasClient() {
            return client != null;
        }

        synchronized void nack() {
            try {
                if (client != null) {
                    client.sendBinary(QwpWireTestUtils.buildNack(0, WebSocketResponse.STATUS_PARSE_ERROR));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * The expected frame: one legacy table buffer per frame block, encoded by the
     * production {@link QwpWebSocketEncoder} against a dictionary that mints the
     * same ids as the sender's.
     */
    static final class Oracle implements QuietCloseable {
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

    /**
     * Keeps every message per connection. The first connection acknowledges its
     * first {@code acksOnFirstConnection} messages and no more, so the frames after
     * them stay unacknowledged and a reconnect replays them; later connections
     * acknowledge everything.
     */
    private static final class ReplayLog implements TestWebSocketServer.WebSocketServerHandler {
        private final int acksOnFirstConnection;
        private final List<List<byte[]>> messages = new ArrayList<>();
        private TestWebSocketServer.ClientHandler currentClient;
        private long nextSeq;

        ReplayLog() {
            this(0);
        }

        ReplayLog(int acksOnFirstConnection) {
            this.acksOnFirstConnection = acksOnFirstConnection;
        }

        /**
         * Waits for the first data message (one with a table) the
         * {@code connection}-th connection, counted from 0, received.
         */
        byte[] awaitDataMessage(int connection, long timeoutMillis) throws InterruptedException {
            return awaitDataMessage(connection, 0, timeoutMillis);
        }

        /**
         * Waits for the {@code index}-th data message (one with a table), counted
         * from 0, the {@code connection}-th connection received.
         */
        byte[] awaitDataMessage(int connection, int index, long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            while (System.currentTimeMillis() < deadline) {
                byte[] message = dataMessage(connection, index);
                if (message != null) {
                    return message;
                }
                Thread.sleep(10);
            }
            throw new AssertionError("no data message " + index + " on connection " + connection + " within " + timeoutMillis + " ms");
        }

        synchronized List<byte[]> messages(int connection) {
            return new ArrayList<>(messages.get(connection));
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            if (currentClient != client) {
                currentClient = client;
                messages.add(new ArrayList<>());
                nextSeq = 0;
            }
            messages.get(messages.size() - 1).add(data.clone());
            if (messages.size() > 1 || nextSeq < acksOnFirstConnection) {
                try {
                    client.sendBinary(QwpWireTestUtils.buildAck(nextSeq++));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private synchronized byte[] dataMessage(int connection, int index) {
            if (connection < messages.size()) {
                int seen = -1;
                for (byte[] message : messages.get(connection)) {
                    if (QwpWireTestUtils.tableCount(message) > 0 && ++seen == index) {
                        return message;
                    }
                }
            }
            return null;
        }
    }
}
