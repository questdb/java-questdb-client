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

package io.questdb.test.cutlass.line.udp;

import io.questdb.cutlass.line.AbstractLineSender;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Integration tests for UDP line sender.
 * <p>
 * Note: UDP is a fire-and-forget protocol, so tests need extra delays
 * to account for network latency and server processing time.
 * These tests require an external QuestDB instance.
 */
public class LineUdpSenderTest extends AbstractLineUdpSenderTest {

    @Test
    public void testAllColumnTypes() throws Exception {
        String tableName = createTrackedTable("udp_types");
        try (AbstractLineSender sender = createUdpSender()) {
            sender.table(tableName)
                    .symbol("sym", "abc")
                    .longColumn("long_col", 42)
                    .doubleColumn("double_col", 3.14)
                    .stringColumn("string_col", "hello")
                    .boolColumn("bool_col", true)
                    .timestampColumn("ts_col", 1234567890L, ChronoUnit.MICROS)
                    .atNow();
            sender.flush();
        }
        Thread.sleep(UDP_SETTLE_DELAY_MS);
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testCloseAndAssertHelper() throws Exception {
        String tableName = createTrackedTable("udp_close");
        try (AbstractLineSender sender = createUdpSender()) {
            sender.table(tableName)
                    .symbol("device", "dev1")
                    .longColumn("reading", 100)
                    .atNow();
            closeAndAssertRowCount(sender, tableName, 1);
        }
    }

    @Test
    public void testExplicitTimestamp() throws Exception {
        String tableName = createTrackedTable("udp_ts");
        long ts = Instant.now().toEpochMilli() * 1000;
        try (AbstractLineSender sender = createUdpSender()) {
            sender.table(tableName)
                    .symbol("city", "paris")
                    .longColumn("temp", 15)
                    .at(ts, ChronoUnit.MICROS);
            sender.flush();
        }
        Thread.sleep(UDP_SETTLE_DELAY_MS);
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testFlushAndAssertHelper() throws Exception {
        String tableName = createTrackedTable("udp_helper");
        try (AbstractLineSender sender = createUdpSender()) {
            sender.table(tableName)
                    .symbol("sensor", "s1")
                    .doubleColumn("value", 123.456)
                    .atNow();
            flushAndAssertRowCount(sender, tableName, 1);

            sender.table(tableName)
                    .symbol("sensor", "s2")
                    .doubleColumn("value", 789.012)
                    .atNow();
            flushAndAssertRowCount(sender, tableName, 2);
        }
    }

    @Test
    public void testInstantTimestamp() throws Exception {
        String tableName = createTrackedTable("udp_instant");
        Instant now = Instant.now();
        try (AbstractLineSender sender = createUdpSender()) {
            sender.table(tableName)
                    .symbol("city", "berlin")
                    .longColumn("temp", 20)
                    .at(now);
            sender.flush();
        }
        Thread.sleep(UDP_SETTLE_DELAY_MS);
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testLargeBuffer() throws Exception {
        String tableName = createTrackedTable("udp_large");
        // Use larger buffer for more data
        try (AbstractLineSender sender = createUdpSender(8192)) {
            for (int i = 0; i < 50; i++) {
                sender.table(tableName)
                        .symbol("sensor", "s" + i)
                        .doubleColumn("value", i * 1.5)
                        .longColumn("count", i)
                        .atNow();
            }
            sender.flush();
        }
        Thread.sleep(UDP_SETTLE_DELAY_MS * 2); // Extra time for more data
        assertTableRowCount(tableName, 50);
    }

    @Test
    public void testMultipleFlushes() throws Exception {
        String tableName = createTrackedTable("udp_multiflush");
        try (AbstractLineSender sender = createUdpSender()) {
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 10; i++) {
                    sender.table(tableName)
                            .symbol("batch", String.valueOf(batch))
                            .longColumn("idx", i)
                            .atNow();
                }
                sender.flush();
                Thread.sleep(UDP_SETTLE_DELAY_MS); // Wait between batches
            }
        }
        assertTableRowCount(tableName, 50);
    }

    @Test
    public void testMultipleRows() throws Exception {
        String tableName = createTrackedTable("udp_multi");
        try (AbstractLineSender sender = createUdpSender()) {
            for (int i = 0; i < 10; i++) {
                sender.table(tableName)
                        .symbol("city", "city_" + i)
                        .longColumn("temp", i * 10)
                        .atNow();
            }
            sender.flush();
        }
        Thread.sleep(UDP_SETTLE_DELAY_MS);
        assertTableRowCount(tableName, 10);
    }

    @Test
    public void testNullStringValue() throws Exception {
        String tableName = createTrackedTable("udp_nullstr");
        try (AbstractLineSender sender = createUdpSender()) {
            sender.table(tableName)
                    .symbol("id", "1")
                    .stringColumn("data", null)
                    .atNow();
            sender.flush();
        }
        Thread.sleep(UDP_SETTLE_DELAY_MS);
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testNullSymbolValue() throws Exception {
        String tableName = createTrackedTable("udp_nullsym");
        try (AbstractLineSender sender = createUdpSender()) {
            sender.table(tableName)
                    .symbol("city", null)
                    .longColumn("temp", 25)
                    .atNow();
            sender.flush();
        }
        Thread.sleep(UDP_SETTLE_DELAY_MS);
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testSimpleInsert() throws Exception {
        String tableName = createTrackedTable("udp_simple");
        try (AbstractLineSender sender = createUdpSender()) {
            sender.table(tableName)
                    .symbol("city", "london")
                    .longColumn("temp", 42)
                    .atNow();
            sender.flush();
        }
        Thread.sleep(UDP_SETTLE_DELAY_MS);
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testSpecialCharactersInSymbol() throws Exception {
        String tableName = createTrackedTable("udp_special");
        try (AbstractLineSender sender = createUdpSender()) {
            sender.table(tableName)
                    .symbol("name", "hello world")
                    .symbol("path", "/path/to/file")
                    .longColumn("count", 1)
                    .atNow();
            sender.flush();
        }
        Thread.sleep(UDP_SETTLE_DELAY_MS);
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testUnicodeInString() throws Exception {
        String tableName = createTrackedTable("udp_unicode");
        try (AbstractLineSender sender = createUdpSender()) {
            sender.table(tableName)
                    .symbol("lang", "ja")
                    .stringColumn("text", "こんにちは世界")
                    .atNow();
            sender.flush();
        }
        Thread.sleep(UDP_SETTLE_DELAY_MS);
        assertTableRowCount(tableName, 1);
    }
}
