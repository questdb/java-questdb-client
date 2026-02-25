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

import io.questdb.client.cutlass.qwp.client.QwpWebSocketSender;
import io.questdb.client.cutlass.qwp.protocol.QwpTableBuffer;
import io.questdb.client.test.AbstractTest;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.time.temporal.ChronoUnit;

/**
 * Verifies that {@link QwpWebSocketSender} invalidates its cached timestamp
 * column references ({@code cachedTimestampColumn} and
 * {@code cachedTimestampNanosColumn}) during flush operations.
 * <p>
 * These cached references point into a {@code QwpTableBuffer} whose columns
 * are reset by {@code flushSync()} / {@code flushPendingRows()}. If the cache
 * is not cleared, subsequent rows may write through a stale reference.
 * <p>
 * The test uses {@code autoFlushRows=1} so that every row triggers a flush
 * inside {@code sendRow()}. The flush itself fails (no real connection), but
 * the cache must be invalidated <em>before</em> the send is attempted.
 * After the failed flush the test clears the table buffer, making any
 * surviving stale reference point to a freed {@code ColumnBuffer}. A second
 * row is then sent: if the cache was properly invalidated, a fresh column is
 * created and the row is buffered normally; if stale, {@code addLong()} hits
 * an NPE before {@code sendRow()} / {@code nextRow()}, so the row is never
 * counted.
 */
public class QwpWebSocketSenderFlushCacheTest extends AbstractTest {

    @Test
    public void testCachedTimestampColumnInvalidatedDuringFlush() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            QwpWebSocketSender sender = QwpWebSocketSender.createForTesting(
                    "localhost", 0, 1, 10_000_000, 0, 1, 16
            );
            try {
                setConnected(sender, true);

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
                setConnected(sender, false);
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
                setConnected(sender, true);

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
                setConnected(sender, false);
                sender.close();
            }
        });
    }

    private static void setConnected(QwpWebSocketSender sender, boolean value) throws Exception {
        Field f = QwpWebSocketSender.class.getDeclaredField("connected");
        f.setAccessible(true);
        f.set(sender, value);
    }
}
