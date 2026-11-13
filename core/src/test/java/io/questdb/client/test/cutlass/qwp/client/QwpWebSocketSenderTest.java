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
import io.questdb.client.cutlass.qwp.client.DurableAckTiers;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.cutlass.qwp.client.BatchTooLargeForCapException;
import io.questdb.client.cutlass.qwp.client.MicrobatchBuffer;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.client.sf.cursor.MmapSegment;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.bytes.DirectByteSlice;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Unit tests for QwpWebSocketSender.
 * These tests focus on state management and API validation without requiring a live server.
 */
public class QwpWebSocketSenderTest {

    @Test
    public void testApplyServerBatchSizeLimit_advertisedClampsLargerConfigured() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 9000,
                    /*autoFlushRows*/ 1000,
                    /*autoFlushBytes*/ 32 * 1024 * 1024,
                    /*autoFlushIntervalNanos*/ 0L)) {
                // Server advertises 16 MB. Configured 32 MB is over the cap,
                // so the effective budget should drop to 90% of 16 MB.
                sender.applyServerBatchSizeLimit(16 * 1024 * 1024);
                int effective = sender.getEffectiveAutoFlushBytes();
                Assert.assertEquals((long) (16 * 1024 * 1024) * 9 / 10, effective);
                Assert.assertEquals(16 * 1024 * 1024, sender.getServerMaxBatchSize());
            }
        });
    }

    @Test
    public void testApplyServerBatchSizeLimit_advertisedZeroKeepsConfigured() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 9000,
                    /*autoFlushRows*/ 1000,
                    /*autoFlushBytes*/ 2 * 1024 * 1024,
                    /*autoFlushIntervalNanos*/ 0L)) {
                // 0 advertisement = older server. Effective budget must equal
                // the configured value verbatim so the sender keeps working.
                sender.applyServerBatchSizeLimit(0);
                Assert.assertEquals(2 * 1024 * 1024, sender.getEffectiveAutoFlushBytes());
                Assert.assertEquals(0, sender.getServerMaxBatchSize());
            }
        });
    }

    @Test
    public void testApplyServerBatchSizeLimit_configuredSmallerThanAdvertisedWins() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 9000,
                    /*autoFlushRows*/ 1000,
                    /*autoFlushBytes*/ 2 * 1024 * 1024,
                    /*autoFlushIntervalNanos*/ 0L)) {
                // Server advertises 16 MB; configured 2 MB is well below.
                // Keep the user's tighter budget rather than overriding it.
                sender.applyServerBatchSizeLimit(16 * 1024 * 1024);
                Assert.assertEquals(2 * 1024 * 1024, sender.getEffectiveAutoFlushBytes());
            }
        });
    }

    @Test
    public void testApplyServerBatchSizeLimit_optOutPreservedDespiteAdvertisement() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 9000,
                    /*autoFlushRows*/ 1000,
                    /*autoFlushBytes*/ 0,
                    /*autoFlushIntervalNanos*/ 0L)) {
                // User explicitly disabled the byte trigger. The server's
                // advertised cap must update serverMaxBatchSize (for the
                // single-row guard) but must not re-enable byte flushing.
                sender.applyServerBatchSizeLimit(16 * 1024 * 1024);
                Assert.assertEquals(0, sender.getEffectiveAutoFlushBytes());
                Assert.assertEquals(16 * 1024 * 1024, sender.getServerMaxBatchSize());
            }
        });
    }

    @Test
    public void testAtAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.at(1000L, ChronoUnit.MICROS);
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testAtInstantAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.at(Instant.now());
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testAtNowAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.atNow();
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testBinaryColumnAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.binaryColumn("x", new byte[]{1, 2, 3});
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testBinaryColumnRejectsNullArray() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startLegacyServer();
                 QwpWebSocketSender sender = connectLegacySender(server, Integer.MAX_VALUE, 0, 0L)) {
                sender.table("t");
                try {
                    sender.binaryColumn("x", (byte[]) null);
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage().contains("BINARY value cannot be null"));
                }
            }
        });
    }

    @Test
    public void testBinaryColumnRejectsNullDirectByteSlice() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startLegacyServer();
                 QwpWebSocketSender sender = connectLegacySender(server, Integer.MAX_VALUE, 0, 0L)) {
                sender.table("t");
                try {
                    sender.binaryColumn("x", (io.questdb.client.std.bytes.DirectByteSlice) null);
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage().contains("BINARY slice cannot be null"));
                }
            }
        });
    }

    @Test
    public void testBoolColumnAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.boolColumn("x", true);
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testBufferViewNotSupported() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
                sender.bufferView();
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("not supported"));
            }
        });
    }

    @Test
    public void testCancelRowAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.cancelRow();
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testCancelRowDiscardsPartialRow() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startLegacyServer();
                 QwpWebSocketSender sender = connectLegacySender(server, Integer.MAX_VALUE, 0, 0L)) {
                sender.table("test");
                sender.longColumn("x", 1);
                sender.boolColumn("y", true);

                // Row is not yet committed (no at/atNow call), cancel it
                sender.cancelRow();

                // Buffer should have no committed rows
                QwpTableBuffer buf = sender.getTableBuffer("test");
                Assert.assertEquals(0, buf.getRowCount());
            }
        });
    }

    @Test
    public void testCancelRowNoOpWithoutTable() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
                // cancelRow without table() should be a no-op (no NPE)
                sender.cancelRow();
            }
        });
    }

    @Test
    public void testSelectedTableSupportsEmptyFlushAndImplicitRows() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startLegacyServer();
                 QwpWebSocketSender sender = connectLegacySender(server, Integer.MAX_VALUE, 0, 0L)) {
                sender.table("t");
                sender.flush();

                sender.longColumn("v", 1).atNow();
                sender.longColumn("discarded", 2);
                sender.cancelRow();
                sender.longColumn("v", 3).atNow();

                sender.longColumn("v", 4);
                sender.table("t"); // same-table selection does not disturb a partial row
                sender.boolColumn("flag", true).atNow();

                Assert.assertEquals(3, sender.getTableBuffer("t").getRowCount());
            }
        });
    }

    @Test
    public void testCancelledRowDoesNotShipItsTable() throws Exception {
        assertMemoryLeak(() -> {
            FrameCapture capture = new FrameCapture();
            try (TestWebSocketServer server = new TestWebSocketServer(capture)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                        + ";auto_flush_interval=60000;")) {
                    sender.table("alpha").longColumn("v", 1).atNow();
                    sender.table("bravo").longColumn("v", 2);
                    sender.cancelRow();
                    sender.flush();
                    capture.awaitFrames(1, 5_000);
                    Assert.assertEquals(1, capture.tableCount(0));
                    byte[] frame0 = capture.frame(0);
                    Assert.assertTrue("frame 0 carries alpha", indexOf(frame0, "alpha") >= 0);
                    Assert.assertEquals("frame 0 carries no bravo", -1, indexOf(frame0, "bravo"));
                }
            }
        });
    }

    @Test
    public void testCapRejectedFlushThenResetNeverShipsTheDroppedTable() throws Exception {
        assertMemoryLeak(() -> {
            FrameCapture capture = new FrameCapture();
            try (TestWebSocketServer server = new TestWebSocketServer(capture)) {
                server.setAdvertisedMaxBatchSize(4 * 1024);
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                // no row or byte trigger: the whole batch reaches the explicit flush(), whose split
                // pre-flight rejects it before publishing anything
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                        + ";auto_flush_rows=off;auto_flush_bytes=off;auto_flush_interval=60000;")) {
                    // warm-up, so the cap is known
                    sender.table("w").longColumn("v", 1).atNow();
                    sender.flush();
                    capture.awaitFrames(1, 5_000);

                    sender.table("alpha").longColumn("v", 2).atNow();
                    sender.table("bravo").longColumn("v", 3).atNow();
                    // three 1.5 KB rows take charlie_dropped's block over the 4 KiB cap
                    String wide = wideValue(1500);
                    for (int i = 0; i < 3; i++) {
                        sender.table("charlie_dropped").stringColumn("s", wide).atNow();
                    }
                    try {
                        sender.flush();
                        Assert.fail("the batch exceeds the 4 KiB cap");
                    } catch (BatchTooLargeForCapException expected) {
                        // thrown before anything was published
                    }

                    // the three tables stay listed after reset(); only the ones that get rows again ship
                    sender.reset();
                    sender.table("alpha").longColumn("v", 4).atNow();
                    sender.table("bravo").longColumn("v", 5).atNow();
                    sender.flush();
                    capture.awaitFrames(2, 5_000);
                    Assert.assertEquals(2, capture.tableCount(1));
                    byte[] frame1 = capture.frame(1);
                    int alphaAt = indexOf(frame1, "alpha");
                    int bravoAt = indexOf(frame1, "bravo");
                    Assert.assertTrue("frame 1 carries alpha", alphaAt >= 0);
                    Assert.assertTrue("frame 1 carries bravo", bravoAt >= 0);
                    Assert.assertTrue("tables ship in first-row order", alphaAt < bravoAt);
                    for (int i = 0, n = capture.frameCount(); i < n; i++) {
                        Assert.assertEquals("frame " + i + " must not carry the dropped table",
                                -1, indexOf(capture.frame(i), "charlie_dropped"));
                    }
                }
            }
        });
    }

    @Test
    public void testCloseIdemponent() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();
            sender.close(); // Should not throw
        });
    }

    @Test
    public void testConnectToClosedPort() throws Exception {
        assertMemoryLeak(() -> {
            try (AutoCloseable ignored = QwpWebSocketSender.connect("127.0.0.1", 1)) {
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("Failed to connect"));
            }
        });
    }

    @Test
    public void testDoubleArrayAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.doubleArray("x", new double[]{1.0, 2.0});
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testDoubleColumnAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.doubleColumn("x", 1.0);
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testFlushAfterResetShipsOnlyTablesWithNewRows() throws Exception {
        assertMemoryLeak(() -> {
            FrameCapture capture = new FrameCapture();
            try (TestWebSocketServer server = new TestWebSocketServer(capture)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                        + ";auto_flush_interval=60000;")) {
                    sender.table("alpha").longColumn("v", 1).atNow();
                    sender.table("bravo").longColumn("v", 2).atNow();
                    // both tables stay listed after reset(); the flush skips the one without rows
                    sender.reset();
                    sender.table("bravo").longColumn("v", 3).atNow();
                    sender.flush();
                    capture.awaitFrames(1, 5_000);
                    Assert.assertEquals("only bravo has rows", 1, capture.tableCount(0));
                    byte[] frame0 = capture.frame(0);
                    Assert.assertTrue("frame 0 carries bravo", indexOf(frame0, "bravo") >= 0);
                    Assert.assertEquals("frame 0 carries no alpha", -1, indexOf(frame0, "alpha"));

                    sender.table("alpha").longColumn("v", 4).atNow();
                    sender.flush();
                    capture.awaitFrames(2, 5_000);
                    Assert.assertEquals("only alpha has rows", 1, capture.tableCount(1));
                    byte[] frame1 = capture.frame(1);
                    Assert.assertTrue("frame 1 carries alpha", indexOf(frame1, "alpha") >= 0);
                    Assert.assertEquals("frame 1 carries no bravo", -1, indexOf(frame1, "bravo"));
                }
            }
        });
    }

    @Test
    public void testFlushAppendFailureDoesNotLeaveMicrobatchBufferInUse() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            })) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // Memory-only engine with the smallest viable segment budget (just
                // enough for the header plus one minimal frame) and a 1 ns append
                // deadline guarantees every appendBlocking() call trips the
                // backpressure deadline and throws.
                long minSegmentBytes = MmapSegment.HEADER_SIZE + MmapSegment.FRAME_HEADER_SIZE + 1;
                CursorSendEngine engine = new CursorSendEngine(null, minSegmentBytes, minSegmentBytes, 1L);
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, Integer.MAX_VALUE, 0, 0L, null,
                        DurableAckTiers.NONE, engine, 0L)) {
                    sender.table("t").longColumn("v", 1L).atNow();

                    try {
                        sender.flushAndGetSequence();
                        Assert.fail("Expected LineSenderException");
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage().contains("cursor SF append failed"));
                    }

                    MicrobatchBuffer buffer0 = getMicrobatchBuffer(sender, "buffer0");
                    MicrobatchBuffer buffer1 = getMicrobatchBuffer(sender, "buffer1");
                    Assert.assertFalse(
                            "failed append must not leave any buffer in use [buffer0="
                                    + MicrobatchBuffer.stateName(buffer0.getState())
                                    + ", buffer1=" + MicrobatchBuffer.stateName(buffer1.getState()) + "]",
                            buffer0.isInUse() || buffer1.isInUse());
                } catch (LineSenderException ignored) {
                }
                // close() drains pending rows, which appendBlocking still
                // rejects because the engine is permanently wedged in this
                // test. The bug under test is about microbatch buffer
                // state, not about close() being lenient toward residual
                // unflushed rows — swallow the predictable rethrow here.
            }
        });
    }

    @Test
    public void testFlushShipsOnlyTablesWithPendingRows() throws Exception {
        assertMemoryLeak(() -> {
            FrameCapture capture = new FrameCapture();
            try (TestWebSocketServer server = new TestWebSocketServer(capture)) {
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
                try (Sender sender = Sender.fromConfig("ws::addr=localhost:" + server.getPort()
                        + ";auto_flush_interval=60000;")) {
                    sender.table("alpha").longColumn("v", 1).atNow();
                    sender.table("bravo").longColumn("v", 2).atNow();
                    sender.table("alpha").longColumn("v", 3).atNow();
                    sender.flush();
                    capture.awaitFrames(1, 5_000);
                    Assert.assertEquals(2, capture.tableCount(0));
                    byte[] frame0 = capture.frame(0);
                    int alphaAt = indexOf(frame0, "alpha");
                    int bravoAt = indexOf(frame0, "bravo");
                    Assert.assertTrue("frame 0 carries alpha", alphaAt >= 0);
                    Assert.assertTrue("frame 0 carries bravo", bravoAt >= 0);
                    Assert.assertTrue("tables ship in first-row order", alphaAt < bravoAt);

                    sender.table("charlie").longColumn("v", 4).atNow();
                    sender.flush();
                    capture.awaitFrames(2, 5_000);
                    Assert.assertEquals("only charlie has rows", 1, capture.tableCount(1));

                    sender.table("bravo").longColumn("v", 5).atNow();
                    sender.flush();
                    capture.awaitFrames(3, 5_000);
                    Assert.assertEquals("only bravo has rows", 1, capture.tableCount(2));
                    byte[] frame2 = capture.frame(2);
                    Assert.assertTrue("frame 2 carries bravo", indexOf(frame2, "bravo") >= 0);
                    Assert.assertEquals("frame 2 carries no alpha", -1, indexOf(frame2, "alpha"));
                    Assert.assertEquals("frame 2 carries no charlie", -1, indexOf(frame2, "charlie"));

                    // bravo was created after alpha but gets its row first in this batch
                    sender.table("bravo").longColumn("v", 6).atNow();
                    sender.table("alpha").longColumn("v", 7).atNow();
                    sender.flush();
                    capture.awaitFrames(4, 5_000);
                    Assert.assertEquals(2, capture.tableCount(3));
                    byte[] frame3 = capture.frame(3);
                    Assert.assertTrue("tables ship in first-row order, not creation order",
                            indexOf(frame3, "bravo") < indexOf(frame3, "alpha"));
                }
            }
        });
    }

    @Test
    public void testGeoHashColumnLongAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.geoHashColumn("g", 0xFL, 5);
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testGeoHashColumnStringAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.geoHashColumn("g", "u33d");
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testIpv4ColumnStringNullReturnsThis() throws Exception {
        // A null reference is the explicit "skip the setter for this row"
        // signal -- the column gets null-padded by nextRow() if it already
        // exists in the table buffer, or stays undeclared otherwise. Either
        // way it round-trips as SQL NULL. The call must not throw and must
        // return the sender so chained builders keep working.
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
                Assert.assertSame(sender, sender.ipv4Column("addr", null));
            }
        });
    }

    @Test
    public void testIpv4ColumnStringRejectsNullLiteral() throws Exception {
        // The literal string "null" (case-insensitive) parses to the IPv4
        // NULL sentinel via Numbers.parseIPv4. Accepting it silently would
        // sneak a NULL through an API the user expects to mean "give me a
        // real IPv4". Reject it as an invalid IPv4 address; the user can
        // pass a null reference (or omit the setter) to mark the row null.
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startLegacyServer();
                 QwpWebSocketSender sender = connectLegacySender(server, Integer.MAX_VALUE, 0, 0L)) {
                sender.table("t");
                for (String input : new String[]{"null", "NULL", "Null", "nUlL"}) {
                    try {
                        sender.ipv4Column("addr", input);
                        Assert.fail("Expected LineSenderException for input: " + input);
                    } catch (LineSenderException e) {
                        Assert.assertTrue(
                                "expected error to mention 'invalid IPv4' or 'NULL sentinel'"
                                        + " for input " + input + ", got: " + e.getMessage(),
                                e.getMessage().contains("invalid IPv4")
                                        || e.getMessage().contains("NULL sentinel"));
                    }
                }
            }
        });
    }

    @Test
    public void testIpv4ColumnStringRejectsZeroAddress() throws Exception {
        // "0.0.0.0" parses to the bit pattern 0, which the storage layer
        // treats as the IPv4 NULL sentinel. Accepting it would silently
        // round-trip as SQL NULL even though the user wrote a syntactically
        // valid dotted-quad. Reject it on the client so the user gets a
        // clear error instead of a value that disappears on read.
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startLegacyServer();
                 QwpWebSocketSender sender = connectLegacySender(server, Integer.MAX_VALUE, 0, 0L)) {
                sender.table("t");
                try {
                    sender.ipv4Column("addr", "0.0.0.0");
                    Assert.fail("Expected LineSenderException for 0.0.0.0");
                } catch (LineSenderException e) {
                    Assert.assertTrue(
                            "expected error to mention 'invalid IPv4' or 'NULL sentinel', got: "
                                    + e.getMessage(),
                            e.getMessage().contains("invalid IPv4")
                                    || e.getMessage().contains("NULL sentinel"));
                }
            }
        });
    }

    @Test
    public void testLongArrayAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.longArray("x", new long[]{1L, 2L});
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testLongColumnAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.longColumn("x", 1);
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testNullArgsAfterCloseAllThrowSenderClosed() throws Exception {
        // Several column setters short-circuit (return this or throw "cannot
        // be null") before delegating to an overload that calls
        // checkNotClosed(). On a closed sender that means the user gets a
        // null-flavoured error (or worse, a silent no-op) instead of the
        // canonical "Sender is closed". This test pins the closed-sender
        // precedence across the entire affected method surface.
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();
            assertClosed(() -> sender.binaryColumn("x", (DirectByteSlice) null));
            assertClosed(() -> sender.decimalColumn("x", (Decimal64) null));
            assertClosed(() -> sender.decimalColumn("x", (Decimal128) null));
            assertClosed(() -> sender.decimalColumn("x", (Decimal256) null));
            assertClosed(() -> sender.decimalColumn("x", (CharSequence) null));
            assertClosed(() -> sender.doubleArray("x", (double[]) null));
            assertClosed(() -> sender.doubleArray("x", (double[][]) null));
            assertClosed(() -> sender.doubleArray("x", (double[][][]) null));
            assertClosed(() -> sender.doubleArray("x", (DoubleArray) null));
            assertClosed(() -> sender.geoHashColumn("x", null));
            assertClosed(() -> sender.ipv4Column("x", null));
            assertClosed(() -> sender.longArray("x", (long[]) null));
            assertClosed(() -> sender.longArray("x", (long[][]) null));
            assertClosed(() -> sender.longArray("x", (long[][][]) null));
            assertClosed(() -> sender.longArray("x", (LongArray) null));
        });
    }

    @Test
    public void testNullArrayReturnsThis() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
                // Null arrays should be no-ops and return sender
                Assert.assertSame(sender, sender.doubleArray("x", (double[]) null));
                Assert.assertSame(sender, sender.longArray("x", (long[]) null));
            }
        });
    }

    @Test
    public void testOperationsAfterCloseThrow() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.table("test");
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testPendingBytesMatchesGroundTruthAcrossTableSwitches() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startLegacyServer();
                 QwpWebSocketSender sender = connectLegacySender(server, Integer.MAX_VALUE, 0, 0L)) {

                // Round-robin three tables to exercise the per-row delta
                // logic across switches. The running pendingBytes must
                // always equal what a full re-walk would return.
                for (int i = 0; i < 30; i++) {
                    sender.table("t" + (i % 3));
                    sender.longColumn("a", i);
                    sender.longColumn("b", i * 7L);
                    sender.atNow();
                    Assert.assertEquals(
                            "row " + i + ": pendingBytes diverged from ground truth",
                            sender.totalBufferedBytes(),
                            sender.getPendingBytes());
                }
            }
        });
    }

    @Test
    public void testPendingBytesUnchangedByCancelRow() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startLegacyServer();
                 QwpWebSocketSender sender = connectLegacySender(server, Integer.MAX_VALUE, 0, 0L)) {

                sender.table("t0").longColumn("a", 1).longColumn("b", 2).atNow();
                long committed = sender.getPendingBytes();
                Assert.assertEquals(sender.totalBufferedBytes(), committed);

                // Begin a row, then cancel. The committed bytes must not
                // change and pendingBytes must still match ground truth.
                sender.table("t0").longColumn("a", 99);
                sender.cancelRow();

                Assert.assertEquals(committed, sender.getPendingBytes());
                Assert.assertEquals(sender.totalBufferedBytes(), sender.getPendingBytes());
            }
        });
    }

    @Test
    public void testResetAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.reset();
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testRowAcceptedWhenServerAdvertisesNoCap() throws Exception {
        // serverMaxBatchSize == 0 means "no cap" (an older server, or a failover to a
        // node that advertises none). The per-row guard in sendRow must be skipped, so
        // even a large row is accepted rather than rejected against a "cap" of 0. This
        // is the invariant the sendRow cap snapshot protects: the I/O thread can lower
        // the volatile serverMaxBatchSize to 0 mid-row, and a torn read that observed
        // the drop between the guard and the throw must not spuriously reject the row.
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startLegacyServer();
                 QwpWebSocketSender sender = connectLegacySender(server, Integer.MAX_VALUE, 0, 0L)) {
                sender.applyServerBatchSizeLimit(0);
                Assert.assertEquals(0, sender.getServerMaxBatchSize());

                StringBuilder big = new StringBuilder();
                for (int i = 0; i < 256; i++) {
                    big.append('x');
                }
                // A row far larger than any small cap still commits when the server
                // advertises no cap; the guard leaves it alone.
                sender.table("t").stringColumn("s", big.toString()).atNow();

                QwpTableBuffer buf = sender.getTableBuffer("t");
                Assert.assertEquals(1, buf.getRowCount());
            }
        });
    }

    @Test
    public void testRowExceedingServerBatchCapThrows() throws Exception {
        // The per-row guard in sendRow rejects a single row whose encoded bytes
        // already exceed the server's advertised cap, before nextRow() commits it, so
        // the flush cannot build an oversize WS frame the server closes with 1009.
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startLegacyServer();
                 QwpWebSocketSender sender = connectLegacySender(server, Integer.MAX_VALUE, 0, 0L)) {
                sender.applyServerBatchSizeLimit(64);
                Assert.assertEquals(64, sender.getServerMaxBatchSize());

                StringBuilder big = new StringBuilder();
                for (int i = 0; i < 256; i++) {
                    big.append('x');
                }
                sender.table("t").stringColumn("s", big.toString());
                try {
                    sender.atNow();
                    Assert.fail("Expected LineSenderException");
                } catch (LineSenderException e) {
                    Assert.assertTrue(
                            "expected 'row too large for server batch cap', got: " + e.getMessage(),
                            e.getMessage().contains("row too large for server batch cap"));
                    // The message reports the single snapshotted cap, not a torn value.
                    Assert.assertTrue(
                            "expected the snapshotted cap in the message, got: " + e.getMessage(),
                            e.getMessage().contains("serverMaxBatchSize=64"));
                }
                // The rejected row was rolled back before commit, leaving no row behind.
                QwpTableBuffer buf = sender.getTableBuffer("t");
                Assert.assertEquals(0, buf.getRowCount());
            }
        });
    }

    @Test
    public void testCumulativeBufferedBytesDoNotTripPerRowCap() throws Exception {
        // The guard budget is nextRow(currentTableBufferSnapshotBytes, cap): each
        // row is measured against the snapshot taken at the previous commit, not
        // against offset zero. A mutation to nextRow(0, cap) would reject every
        // row once the table's CUMULATIVE buffered bytes crossed the cap; this
        // pins the wiring the single-row tests cannot distinguish.
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = startLegacyServer();
                 QwpWebSocketSender sender = connectLegacySender(server, Integer.MAX_VALUE, 0, 0L)) {
                sender.applyServerBatchSizeLimit(64);

                // Three rows of 22-26 bytes each: every row fits the 64-byte cap,
                // but the running total (70 after row 3) exceeds it.
                for (int i = 0; i < 3; i++) {
                    sender.table("t").stringColumn("s", "abcdefghijklmnopqr").atNow();
                }
                QwpTableBuffer buf = sender.getTableBuffer("t");
                Assert.assertEquals(3, buf.getRowCount());
                // The accumulated frame is intentionally larger than the cap;
                // this test covers the per-row guard, not close-time flushing.
                sender.reset();
            }
        });
    }

    @Test
    public void testStringColumnAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.stringColumn("x", "test");
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testSymbolAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.symbol("x", "test");
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testTableBeforeAtNowRequired() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
                sender.atNow();
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("table()"));
            }
        });
    }

    @Test
    public void testTableBeforeAtRequired() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
                sender.at(1000L, ChronoUnit.MICROS);
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("table()"));
            }
        });
    }

    @Test
    public void testTableBeforeColumnsRequired() throws Exception {
        assertMemoryLeak(() -> {
            // Create sender without connecting (we'll catch the error earlier)
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
                sender.longColumn("x", 1);
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("table()"));
            }
        });
    }

    @Test
    public void testTimestampColumnAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.timestampColumn("x", 1000L, ChronoUnit.MICROS);
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testTimestampColumnInstantAfterCloseThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = createUnconnectedSender();
            sender.close();

            try {
                sender.timestampColumn("x", Instant.now());
                Assert.fail("Expected LineSenderException");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    private static void assertClosed(Runnable r) {
        try {
            r.run();
            Assert.fail("Expected LineSenderException with 'closed' message");
        } catch (LineSenderException e) {
            Assert.assertTrue(
                    "expected message to mention 'closed', got: " + e.getMessage(),
                    e.getMessage() != null && e.getMessage().contains("closed"));
        }
    }

    @Test
    public void testIllegalTableNameIsEscapedInTheMessage() throws Exception {
        assertMemoryLeak(() -> {
            // A rejected table name is attacker-influenced text on its way to a log line or a terminal, so
            // it is escaped rather than concatenated. Three of the four callsites that do this are covered -
            // QwpUdpSender and QwpTableBuffer by QwpUdpSenderTest, the ILP names by AbstractLineSender's
            // tests - and this one, the WebSocket sender's own check, was not: its message could regress to
            // a raw concatenation with the suite still green.
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
                try {
                    sender.table("bad" + (char) 0x01 + "name");
                    Assert.fail("an illegal table name must be rejected");
                } catch (LineSenderException e) {
                    final String message = e.getMessage();
                    Assert.assertTrue(message, message.contains("table name contains illegal characters"));
                    Assert.assertTrue("the offending char must be escaped: " + message,
                            message.contains("\\u0001"));
                    Assert.assertTrue("the raw control char must not reach the message",
                            message.indexOf(0x01) < 0);
                }
            }
        });
    }

    @Test
    public void testUnpairedSurrogateTableNameIsRejectedBeforeConnect() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
                for (char invalidChar : new char[]{'\uD800', '\uDB80', '\uDFFF'}) {
                    try {
                        sender.table("bad" + invalidChar + "name");
                        Assert.fail("an unpaired surrogate must be rejected");
                    } catch (LineSenderException e) {
                        Assert.assertTrue(e.getMessage().contains("table name contains illegal characters"));
                    }
                }
            }
        });
    }

    private static MicrobatchBuffer getMicrobatchBuffer(QwpWebSocketSender sender, String fieldName) throws Exception {
        Field field = QwpWebSocketSender.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (MicrobatchBuffer) field.get(sender);
    }

    private static QwpWebSocketSender connectLegacySender(
            TestWebSocketServer server,
            int autoFlushRows,
            int autoFlushBytes,
            long autoFlushIntervalNanos
    ) {
        CursorSendEngine engine = new CursorSendEngine(null, 4L * 1024 * 1024);
        try {
            return QwpWebSocketSender.connect(
                    "localhost", server.getPort(), null,
                    autoFlushRows, autoFlushBytes, autoFlushIntervalNanos,
                    null, DurableAckTiers.NONE, engine, 0L);
        } catch (Throwable t) {
            engine.close();
            throw t;
        }
    }

    private static TestWebSocketServer startLegacyServer() throws Exception {
        AtomicLong nextSequence = new AtomicLong();
        TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            @Override
            public void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
                try {
                    client.sendBinary(QwpWireTestUtils.buildAck(nextSequence.getAndIncrement()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        boolean started = false;
        try {
            server.start();
            Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));
            started = true;
            return server;
        } finally {
            if (!started) {
                server.close();
            }
        }
    }

    private static int indexOf(byte[] frame, String ascii) {
        int n = ascii.length();
        for (int i = 0, last = frame.length - n; i <= last; i++) {
            int j = 0;
            while (j < n && frame[i + j] == (byte) ascii.charAt(j)) {
                j++;
            }
            if (j == n) {
                return i;
            }
        }
        return -1;
    }

    private static String wideValue(int length) {
        char[] chars = new char[length];
        Arrays.fill(chars, 'x');
        return new String(chars);
    }

    /**
     * Creates a sender without connecting.
     * For unit tests that don't need actual connectivity.
     */
    private QwpWebSocketSender createUnconnectedSender() {
        return QwpWebSocketSender.createForTesting("localhost", 9000);
    }

    /**
     * Records every QWP frame the server receives, in arrival order, and acks each
     * binary message up to the last frame it carried.
     */
    private static final class FrameCapture implements TestWebSocketServer.WebSocketServerHandler {
        private static final int HEADER_SIZE = 12;
        private static final int OFFSET_PAYLOAD_LENGTH = 8;
        private static final int OFFSET_TABLE_COUNT = 6;
        private final List<byte[]> frames = new ArrayList<>();
        private long nextSeq;

        void awaitFrames(int atLeast, long timeoutMillis) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMillis;
            while (frameCount() < atLeast && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            Assert.assertTrue("expected at least " + atLeast + " frame(s) within " + timeoutMillis
                    + " ms, got " + frameCount(), frameCount() >= atLeast);
        }

        synchronized byte[] frame(int index) {
            return frames.get(index);
        }

        synchronized int frameCount() {
            return frames.size();
        }

        @Override
        public synchronized void onBinaryMessage(TestWebSocketServer.ClientHandler client, byte[] data) {
            int pos = 0;
            int framesInMessage = 0;
            while (pos + HEADER_SIZE <= data.length) {
                int payloadLength = (data[pos + OFFSET_PAYLOAD_LENGTH] & 0xFF)
                        | ((data[pos + OFFSET_PAYLOAD_LENGTH + 1] & 0xFF) << 8)
                        | ((data[pos + OFFSET_PAYLOAD_LENGTH + 2] & 0xFF) << 16)
                        | ((data[pos + OFFSET_PAYLOAD_LENGTH + 3] & 0xFF) << 24);
                if (payloadLength < 0 || pos + HEADER_SIZE + payloadLength > data.length) {
                    break;
                }
                frames.add(Arrays.copyOfRange(data, pos, pos + HEADER_SIZE + payloadLength));
                framesInMessage++;
                pos += HEADER_SIZE + payloadLength;
            }
            nextSeq += Math.max(1, framesInMessage);
            try {
                client.sendBinary(CloseDrainTest.buildAck(nextSeq - 1));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        synchronized int tableCount(int index) {
            byte[] frame = frames.get(index);
            return (frame[OFFSET_TABLE_COUNT] & 0xFF) | ((frame[OFFSET_TABLE_COUNT + 1] & 0xFF) << 8);
        }
    }
}
