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
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Unit tests for QwpWebSocketSender.
 * These tests focus on state management and API validation without requiring a live server.
 */
public class QwpWebSocketSenderTest {

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
    public void testGorillaEnabledByDefault() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
                Assert.assertTrue(sender.isGorillaEnabled());
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
    public void testSetGorillaEnabled() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = createUnconnectedSender()) {
                sender.setGorillaEnabled(false);
                Assert.assertFalse(sender.isGorillaEnabled());
                sender.setGorillaEnabled(true);
                Assert.assertTrue(sender.isGorillaEnabled());
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
                invokeApplyServerBatchSizeLimit(sender, 16 * 1024 * 1024);
                Assert.assertEquals(0, getEffectiveAutoFlushBytes(sender));
                Assert.assertEquals(16 * 1024 * 1024, getServerMaxBatchSize(sender));
            }
        });
    }

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
                invokeApplyServerBatchSizeLimit(sender, 16 * 1024 * 1024);
                int effective = getEffectiveAutoFlushBytes(sender);
                Assert.assertEquals((long) (16 * 1024 * 1024) * 9 / 10, effective);
                Assert.assertEquals(16 * 1024 * 1024, getServerMaxBatchSize(sender));
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
                invokeApplyServerBatchSizeLimit(sender, 0);
                Assert.assertEquals(2 * 1024 * 1024, getEffectiveAutoFlushBytes(sender));
                Assert.assertEquals(0, getServerMaxBatchSize(sender));
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
                invokeApplyServerBatchSizeLimit(sender, 16 * 1024 * 1024);
                Assert.assertEquals(2 * 1024 * 1024, getEffectiveAutoFlushBytes(sender));
            }
        });
    }

    private static int getEffectiveAutoFlushBytes(QwpWebSocketSender sender) throws Exception {
        Field field = QwpWebSocketSender.class.getDeclaredField("effectiveAutoFlushBytes");
        field.setAccessible(true);
        return field.getInt(sender);
    }

    private static int getServerMaxBatchSize(QwpWebSocketSender sender) throws Exception {
        Field field = QwpWebSocketSender.class.getDeclaredField("serverMaxBatchSize");
        field.setAccessible(true);
        return field.getInt(sender);
    }

    private static void invokeApplyServerBatchSizeLimit(QwpWebSocketSender sender, int advertised) throws Exception {
        Method m = QwpWebSocketSender.class.getDeclaredMethod("applyServerBatchSizeLimit", int.class);
        m.setAccessible(true);
        try {
            m.invoke(sender, advertised);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    /**
     * Creates a sender without connecting.
     * For unit tests that don't need actual connectivity.
     */
    private QwpWebSocketSender createUnconnectedSender() {
        return QwpWebSocketSender.createForTesting("localhost", 9000);
    }

}
