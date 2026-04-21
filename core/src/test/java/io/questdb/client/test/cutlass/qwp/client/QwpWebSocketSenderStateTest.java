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
import io.questdb.client.cutlass.qwp.client.InFlightWindow;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.test.AbstractTest;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.temporal.ChronoUnit;

import static io.questdb.client.test.tools.TestUtils.assertMemoryLeak;

/**
 * Verifies {@link QwpWebSocketSender} internal state management:
 * <ul>
 *   <li>{@code reset()} discards all pending state, not just the current table buffer.</li>
 *   <li>Cached timestamp column references are invalidated during flush operations,
 *       preventing stale writes through freed {@code ColumnBuffer} instances.</li>
 *   <li>Auto-flush accumulates rows globally across all tables rather than flushing
 *       per-table on each table switch.</li>
 * </ul>
 */
public class QwpWebSocketSenderStateTest extends AbstractTest {

    @Test
    public void testGetHighestDurableSequenceDefaultsToMinusOne() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 0, 1)) {
                Assert.assertEquals(-1L, sender.getHighestDurableSequence());
            }
        });
    }

    @Test
    public void testGetHighestAckedSequenceDefaultsToMinusOne() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 0, 1)) {
                Assert.assertEquals(-1L, sender.getHighestAckedSequence());
            }
        });
    }

    @Test
    public void testSetRequestDurableAckBeforeConnect() throws Exception {
        assertMemoryLeak(() -> {
            try (QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 0, 1)) {
                // Must not throw before connection is established
                sender.setRequestDurableAck(true);
                sender.setRequestDurableAck(false);
            }
        });
    }

    @Test
    public void testSetRequestDurableAckAfterConnectThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 0, 1);
            try {
                setField(sender, "connected", true);
                try {
                    sender.setRequestDurableAck(true);
                    Assert.fail("Expected exception for setRequestDurableAck after connect");
                } catch (LineSenderException e) {
                    Assert.assertTrue(e.getMessage().contains("before the first send"));
                }
            } finally {
                setField(sender, "connected", false);
                sender.close();
            }
        });
    }

    @Test
    public void testSetRequestDurableAckOnClosedSenderThrows() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting("localhost", 0, 1);
            sender.close();
            try {
                sender.setRequestDurableAck(true);
                Assert.fail("Expected exception for setRequestDurableAck on closed sender");
            } catch (LineSenderException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }
        });
    }

    @Test
    public void testAutoFlushAccumulatesRowsAcrossAllTables() throws Exception {
        assertMemoryLeak(() -> {
            // autoFlushRows=5; bytes and interval are disabled to isolate the row-count check.
            // The test verifies that switching tables does NOT trigger a flush — flush fires
            // only when the TOTAL pending-row count reaches the configured threshold.
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 0, 5, 0, 0L, 1
            );
            try {
                setField(sender, "connected", true);
                setField(sender, "inFlightWindow", new InFlightWindow(1, InFlightWindow.DEFAULT_TIMEOUT_MS));

                // Write 4 rows interleaved between t1 and t2.
                // None of these should trigger auto-flush (4 < 5 = autoFlushRows).
                sender.table("t1").longColumn("x", 1).at(1, ChronoUnit.MICROS);
                sender.table("t2").longColumn("y", 1).at(1, ChronoUnit.MICROS);
                sender.table("t1").longColumn("x", 2).at(2, ChronoUnit.MICROS);
                sender.table("t2").longColumn("y", 2).at(2, ChronoUnit.MICROS);

                // All 4 rows must still be buffered — switching tables must not flush.
                QwpTableBuffer t1 = sender.getTableBuffer("t1");
                QwpTableBuffer t2 = sender.getTableBuffer("t2");
                Assert.assertEquals("t1 should have 2 buffered rows (no premature flush)",
                        2, t1.getRowCount());
                Assert.assertEquals("t2 should have 2 buffered rows (no premature flush)",
                        2, t2.getRowCount());
                Assert.assertEquals("pendingRowCount must reflect all 4 rows across both tables",
                        4, sender.getPendingRowCount());

                // The 5th row hits the global threshold and triggers auto-flush.
                // The flush fails because client is null, confirming that flush
                // was triggered by the row-count threshold, not by the table switch.
                boolean flushTriggered = false;
                try {
                    sender.table("t1").longColumn("x", 3).at(3, ChronoUnit.MICROS);
                } catch (Exception expected) {
                    flushTriggered = true;
                }
                Assert.assertTrue("auto-flush must be triggered on the 5th row", flushTriggered);
            } finally {
                setField(sender, "connected", false);
                sender.close();
            }
        });
    }

    @Test
    public void testCachedTimestampColumnInvalidatedDuringFlush() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 0, 1, 10_000_000, 0, 1
            );
            try {
                setField(sender, "connected", true);

                // Row 1: caches cachedTimestampColumn, then auto-flush
                // triggers and fails (no real connection).
                try {
                    sender.table("t")
                            .longColumn("x", 1)
                            .at(1, ChronoUnit.MICROS);
                } catch (Exception ignored) {
                }

                // Clear the table buffer so a stale cached reference now
                // points to a freed ColumnBuffer.
                QwpTableBuffer tb = sender.getTableBuffer("t");
                tb.clear();

                // Row 2: with the fix, atMicros() creates a fresh column
                // and the row is buffered. Without, addLong() NPEs before
                // sendRow()/nextRow() and the row is never counted.
                try {
                    sender.table("t")
                            .longColumn("x", 2)
                            .at(2, ChronoUnit.MICROS);
                } catch (Exception ignored) {
                }

                Assert.assertEquals("row must be buffered when cache is properly invalidated",
                        1, tb.getRowCount());
            } finally {
                setField(sender, "connected", false);
                sender.close();
            }
        });
    }

    @Test
    public void testCachedTimestampNanosColumnInvalidatedDuringFlush() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 0, 1, 10_000_000, 0, 1
            );
            try {
                setField(sender, "connected", true);

                try {
                    sender.table("t")
                            .longColumn("x", 1)
                            .at(1, ChronoUnit.NANOS);
                } catch (Exception ignored) {
                }

                QwpTableBuffer tb = sender.getTableBuffer("t");
                tb.clear();

                try {
                    sender.table("t")
                            .longColumn("x", 2)
                            .at(2, ChronoUnit.NANOS);
                } catch (Exception ignored) {
                }

                Assert.assertEquals("row must be buffered when cache is properly invalidated",
                        1, tb.getRowCount());
            } finally {
                setField(sender, "connected", false);
                sender.close();
            }
        });
    }

    @Test
    public void testReconnectResetsRetainedSchemaIds() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 0, 10_000, 0, 0L, 1
            );
            try {
                setField(sender, "connected", true);
                setField(sender, "inFlightWindow", new InFlightWindow(1, InFlightWindow.DEFAULT_TIMEOUT_MS));

                sender.table("t1").longColumn("x", 1).at(1, ChronoUnit.MICROS);
                sender.table("t2").longColumn("y", 2).at(2, ChronoUnit.MICROS);

                QwpTableBuffer t1 = sender.getTableBuffer("t1");
                QwpTableBuffer t2 = sender.getTableBuffer("t2");
                t1.setSchemaId(3);
                t2.setSchemaId(7);
                setField(sender, "maxSentSchemaId", 7);
                setField(sender, "nextSchemaId", 8);

                invokeResetSchemaStateForNewConnection(sender);

                Assert.assertEquals(-1, t1.getSchemaId());
                Assert.assertEquals(-1, t2.getSchemaId());
                Assert.assertEquals(-1, getIntField(sender, "maxSentSchemaId"));
                Assert.assertEquals(0, getIntField(sender, "nextSchemaId"));
            } finally {
                setField(sender, "connected", false);
                sender.close();
            }
        });
    }

    @Test
    public void testResetClearsAllTableBuffersAndPendingRowCount() throws Exception {
        assertMemoryLeak(() -> {
            // Use high autoFlushRows to prevent auto-flush during the test
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 0, 10_000, 10_000_000, 0, 1
            );
            try {
                // Bypass ensureConnected() — mark as connected, leave client null
                setField(sender, "connected", true);
                setField(sender, "inFlightWindow", new InFlightWindow(1, InFlightWindow.DEFAULT_TIMEOUT_MS));

                // Buffer rows into two different tables via the fluent API
                sender.table("t1")
                        .longColumn("x", 1)
                        .at(1, ChronoUnit.MICROS);
                sender.table("t2")
                        .longColumn("y", 2)
                        .at(2, ChronoUnit.MICROS);

                // Verify data is buffered
                QwpTableBuffer t1 = sender.getTableBuffer("t1");
                QwpTableBuffer t2 = sender.getTableBuffer("t2");
                Assert.assertEquals("t1 should have 1 row before reset", 1, t1.getRowCount());
                Assert.assertEquals("t2 should have 1 row before reset", 1, t2.getRowCount());
                Assert.assertEquals("pendingRowCount should be 2 before reset", 2, sender.getPendingRowCount());

                // Select t1 as the current table
                sender.table("t1");

                // Call reset — per the Sender contract this should discard
                // ALL pending state, not just the current table
                sender.reset();

                // Both table buffers should be cleared
                Assert.assertEquals("t1 row count should be 0 after reset", 0, t1.getRowCount());
                Assert.assertEquals("t2 row count should be 0 after reset", 0, t2.getRowCount());

                // Pending row count should be zeroed
                Assert.assertEquals("pendingRowCount should be 0 after reset", 0, sender.getPendingRowCount());
            } finally {
                setField(sender, "connected", false);
                sender.close();
            }
        });
    }

    @Test
    public void testSchemaLimitExceededFailsBeforeSend() throws Exception {
        assertMemoryLeak(() -> {
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 0, 3, 0, 0L, 1, 2
            );
            try {
                setField(sender, "connected", true);
                setField(sender, "inFlightWindow", new InFlightWindow(1, InFlightWindow.DEFAULT_TIMEOUT_MS));

                sender.table("t1").longColumn("x", 1).at(1, ChronoUnit.MICROS);
                sender.table("t2").longColumn("x", 2).at(2, ChronoUnit.MICROS);

                try {
                    sender.table("t3").longColumn("x", 3).at(3, ChronoUnit.MICROS);
                    Assert.fail("Expected schema limit failure");
                } catch (Exception e) {
                    Assert.assertTrue(e.getMessage().contains("maximum schemas per connection exceeded"));
                }
            } finally {
                setField(sender, "connected", false);
                sender.close();
            }
        });
    }

    @Test
    public void testTimestampOnlyRows() throws Exception {
        assertMemoryLeak(() -> {
            // autoFlushRows=10_000 prevents auto-flush; bytes and interval disabled
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 0, 10_000, 0, 0L, 1
            );
            try {
                setField(sender, "connected", true);
                setField(sender, "inFlightWindow", new InFlightWindow(1, InFlightWindow.DEFAULT_TIMEOUT_MS));

                // at(micros) with no other columns
                sender.table("t").at(1_000L, ChronoUnit.MICROS);
                // atNow() with no other columns
                sender.table("t").atNow();

                QwpTableBuffer tb = sender.getTableBuffer("t");
                Assert.assertEquals(
                        "at() and atNow() with no other columns must each buffer a row",
                        2, tb.getRowCount()
                );
            } finally {
                setField(sender, "connected", false);
                sender.close();
            }
        });
    }

    private static int getIntField(Object target, String fieldName) throws Exception {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        return f.getInt(target);
    }

    private static void invokeResetSchemaStateForNewConnection(Object target) throws Exception {
        Method method = target.getClass().getDeclaredMethod("resetSchemaStateForNewConnection");
        method.setAccessible(true);
        method.invoke(target);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(target, value);
    }
}
