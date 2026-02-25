/*******************************************************************************
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

import io.questdb.client.cutlass.qwp.client.InFlightWindow;
import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.test.AbstractTest;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.temporal.ChronoUnit;

/**
 * Verifies {@link QwpWebSocketSender} internal state management:
 * <ul>
 *   <li>{@code reset()} discards all pending state, not just the current table buffer.</li>
 *   <li>Cached timestamp column references are invalidated during flush operations,
 *       preventing stale writes through freed {@code ColumnBuffer} instances.</li>
 * </ul>
 */
public class QwpWebSocketSenderStateTest extends AbstractTest {

    @Test
    public void testCachedTimestampColumnInvalidatedDuringFlush() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 0, 1, 10_000_000, 0, 1, 16
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
        TestUtils.assertMemoryLeak(() -> {
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 0, 1, 10_000_000, 0, 1, 16
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
    public void testResetClearsAllTableBuffersAndPendingRowCount() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            // Use high autoFlushRows to prevent auto-flush during the test
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 0, 10_000, 10_000_000, 0, 1, 16
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

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(target, value);
    }
}
