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

import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.array.LongArray;
import io.questdb.client.cutlass.qwp.client.MicrobatchBuffer;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.client.sf.cursor.CursorSendEngine;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.std.Decimal128;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.Decimal64;
import io.questdb.client.std.bytes.DirectByteSlice;
import io.questdb.client.test.cutlass.qwp.websocket.TestWebSocketServer;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

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
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
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
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
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
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
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
    public void testFlushAppendFailureDoesNotLeaveMicrobatchBufferInUse() throws Exception {
        assertMemoryLeak(() -> {
            try (TestWebSocketServer server = new TestWebSocketServer(new TestWebSocketServer.WebSocketServerHandler() {
            })) {
                int port = server.getPort();
                server.start();
                Assert.assertTrue(server.awaitStart(5, TimeUnit.SECONDS));

                // Memory-only engine with a 33-byte budget and a 1 ns append
                // deadline guarantees every appendBlocking() call trips the
                // backpressure deadline and throws.
                CursorSendEngine engine = new CursorSendEngine(null, 33, 33, 1L);
                try (QwpWebSocketSender sender = QwpWebSocketSender.connect(
                        "localhost", port, null, Integer.MAX_VALUE, 0, 0L, null,
                        false, engine, 0L)) {
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
                sender.table("t");
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
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
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
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
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
            try (QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 9000,
                    /*autoFlushRows*/ Integer.MAX_VALUE,
                    /*autoFlushBytes*/ 0,
                    /*autoFlushIntervalNanos*/ 0L)) {
                // Bypass ensureConnected: sendRow only short-circuits on
                // connected==true and we don't drive any path that touches
                // the cursor engine in this test.
                sender.setConnectedForTest(true);

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
            try (QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 9000,
                    /*autoFlushRows*/ Integer.MAX_VALUE,
                    /*autoFlushBytes*/ 0,
                    /*autoFlushIntervalNanos*/ 0L)) {
                sender.setConnectedForTest(true);

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

    private static MicrobatchBuffer getMicrobatchBuffer(QwpWebSocketSender sender, String fieldName) throws Exception {
        Field field = QwpWebSocketSender.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (MicrobatchBuffer) field.get(sender);
    }

    /**
     * Creates a sender without connecting.
     * For unit tests that don't need actual connectivity.
     */
    private QwpWebSocketSender createUnconnectedSender() {
        return QwpWebSocketSender.createForTesting("localhost", 9000);
    }

}
