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

package io.questdb.test.cutlass.line.http;

import io.questdb.client.Sender;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static io.questdb.client.Sender.PROTOCOL_VERSION_V1;
import static io.questdb.client.Sender.PROTOCOL_VERSION_V2;

/**
 * Integration tests for HTTP line sender.
 * <p>
 * HTTP transport provides stronger transactional guarantees than TCP and
 * better feedback in case of errors.
 * These tests require an external QuestDB instance.
 */
public class LineHttpSenderTest extends AbstractLineHttpSenderTest {

    @Test
    public void testAllColumnTypes() throws Exception {
        String tableName = createTrackedTable("http_types");
        try (Sender sender = createHttpSender()) {
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
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testAutoFlushByRows() throws Exception {
        String tableName = createTrackedTable("http_autorow");
        // Auto-flush after 10 rows
        try (Sender sender = createHttpSenderWithAutoFlush(PROTOCOL_VERSION_V1, 10, 0)) {
            for (int i = 0; i < 25; i++) {
                sender.table(tableName)
                        .symbol("id", String.valueOf(i))
                        .longColumn("value", i)
                        .atNow();
            }
            // Should have auto-flushed twice (at 10 and 20 rows)
            // Final 5 rows need explicit flush
            sender.flush();
        }
        assertTableRowCount(tableName, 25);
    }

    @Test
    public void testCloseAndAssertHelper() throws Exception {
        String tableName = createTrackedTable("http_close");
        try (Sender sender = createHttpSender()) {
            sender.table(tableName)
                    .symbol("device", "dev1")
                    .longColumn("reading", 100)
                    .atNow();
            closeAndAssertRowCount(sender, tableName, 1);
        }
    }

    @Test
    public void testExplicitTimestamp() throws Exception {
        String tableName = createTrackedTable("http_ts");
        long ts = MicrosFormatUtils.parseTimestamp("2025-01-15T12:30:00.000000Z");
        try (Sender sender = createHttpSender()) {
            sender.table(tableName)
                    .symbol("city", "paris")
                    .longColumn("temp", 15)
                    .at(ts, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testFlushAndAssertHelper() throws Exception {
        String tableName = createTrackedTable("http_helper");
        try (Sender sender = createHttpSender()) {
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
        String tableName = createTrackedTable("http_instant");
        Instant now = Instant.now();
        try (Sender sender = createHttpSender()) {
            sender.table(tableName)
                    .symbol("city", "berlin")
                    .longColumn("temp", 20)
                    .at(now);
            sender.flush();
        }
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testLargeBatch() throws Exception {
        String tableName = createTrackedTable("http_batch");
        try (Sender sender = createHttpSender()) {
            for (int i = 0; i < 100; i++) {
                sender.table(tableName)
                        .symbol("sensor", "s" + (i % 10))
                        .longColumn("seq", i)
                        .doubleColumn("value", i * 1.5)
                        .atNow();
            }
            sender.flush();
        }
        assertTableRowCount(tableName, 100);
    }

    @Test
    public void testLargeStringValue() throws Exception {
        String tableName = createTrackedTable("http_largestr");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("x");
        }
        String largeString = sb.toString();

        try (Sender sender = createHttpSender()) {
            sender.table(tableName)
                    .symbol("id", "1")
                    .stringColumn("data", largeString)
                    .atNow();
            sender.flush();
        }
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testMultipleFlushes() throws Exception {
        String tableName = createTrackedTable("http_multiflush");
        try (Sender sender = createHttpSender()) {
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 10; i++) {
                    sender.table(tableName)
                            .symbol("batch", String.valueOf(batch))
                            .longColumn("idx", i)
                            .atNow();
                }
                sender.flush();
            }
        }
        assertTableRowCount(tableName, 50);
    }

    @Test
    public void testMultipleRows() throws Exception {
        String tableName = createTrackedTable("http_multi");
        try (Sender sender = createHttpSender()) {
            for (int i = 0; i < 10; i++) {
                sender.table(tableName)
                        .symbol("city", "city_" + i)
                        .longColumn("temp", i * 10)
                        .atNow();
            }
            sender.flush();
        }
        assertTableRowCount(tableName, 10);
    }

    @Test
    public void testNullStringValue() throws Exception {
        String tableName = createTrackedTable("http_nullstr");
        try (Sender sender = createHttpSender()) {
            sender.table(tableName)
                    .symbol("id", "1")
                    .stringColumn("data", null)
                    .atNow();
            sender.flush();
        }
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testNullSymbolValue() throws Exception {
        String tableName = createTrackedTable("http_nullsym");
        try (Sender sender = createHttpSender()) {
            sender.table(tableName)
                    .symbol("city", null)
                    .longColumn("temp", 25)
                    .atNow();
            sender.flush();
        }
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testProtocolVersionV1() throws Exception {
        String tableName = createTrackedTable("http_v1");
        try (Sender sender = createHttpSender(PROTOCOL_VERSION_V1)) {
            sender.table(tableName)
                    .symbol("city", "tokyo")
                    .longColumn("temp", 30)
                    .atNow();
            sender.flush();
        }
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testProtocolVersionV2() throws Exception {
        String tableName = createTrackedTable("http_v2");
        try (Sender sender = createHttpSender(PROTOCOL_VERSION_V2)) {
            sender.table(tableName)
                    .symbol("city", "seoul")
                    .longColumn("temp", 28)
                    .atNow();
            sender.flush();
        }
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testSimpleInsert() throws Exception {
        String tableName = createTrackedTable("http_simple");
        try (Sender sender = createHttpSender()) {
            sender.table(tableName)
                    .symbol("city", "london")
                    .longColumn("temp", 42)
                    .atNow();
            sender.flush();
        }
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testSpecialCharactersInSymbol() throws Exception {
        String tableName = createTrackedTable("http_special");
        try (Sender sender = createHttpSender()) {
            sender.table(tableName)
                    .symbol("name", "hello world")
                    .symbol("path", "/path/to/file")
                    .longColumn("count", 1)
                    .atNow();
            sender.flush();
        }
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testUnicodeInString() throws Exception {
        String tableName = createTrackedTable("http_unicode");
        try (Sender sender = createHttpSender()) {
            sender.table(tableName)
                    .symbol("lang", "ja")
                    .stringColumn("text", "こんにちは世界")
                    .atNow();
            sender.flush();
        }
        assertTableRowCount(tableName, 1);
    }
}
