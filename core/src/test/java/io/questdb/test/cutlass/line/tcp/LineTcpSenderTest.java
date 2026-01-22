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

package io.questdb.test.cutlass.line.tcp;

import io.questdb.client.Sender;
import io.questdb.cutlass.line.LineChannel;
import io.questdb.cutlass.line.LineSenderException;
import io.questdb.cutlass.line.LineTcpSenderV2;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.function.Consumer;

import static io.questdb.client.Sender.PROTOCOL_VERSION_V1;
import static io.questdb.client.Sender.PROTOCOL_VERSION_V2;
import static io.questdb.test.tools.TestUtils.assertContains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Tests for LineTcpSender.
 * <p>
 * Unit tests use DummyLineChannel/ByteChannel (no server needed).
 * Integration tests use external QuestDB via AbstractLineTcpSenderTest infrastructure.
 */
public class LineTcpSenderTest extends AbstractLineTcpSenderTest {

    private static final Consumer<Sender> SET_TABLE_NAME_ACTION = s -> s.table("mytable");

    // ==================== Unit Tests (no server needed) ====================

    @Test
    public void testAllColumnTypes() throws Exception {
        String tableName = createTrackedTable("all_types");
        try (Sender sender = createTcpSender()) {
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
    public void testAtAfterAtIsNotAllowed() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("mytable").longColumn("a", 1).atNow();
            try {
                sender.atNow();
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "no symbols or columns were provided");
            }
        }
    }

    @Test
    public void testAtNowMustBeLast() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("mytable").longColumn("a", 1).atNow();
            try {
                sender.longColumn("b", 2);
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "table expected");
            }
        }
    }

    @Test
    public void testBoolColumnNameCannotBeEmpty() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("mytable");
            try {
                sender.boolColumn("", true);
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "column name contains an illegal char");
            }
        }
    }

    @Test
    public void testCannotStartNewRowBeforeClosingTheExistingAfterValidationError() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("mytable");
            try {
                sender.boolColumn("col\n", true);
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "name contains an illegal char");
            }
            try {
                sender.table("mytable");
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "duplicated table");
            }
        }
    }

    @Test
    public void testControlCharacterException() {
        DummyLineChannel channel = new DummyLineChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("mytable");
            sender.boolColumn("col\u0001", true);
            fail("control character in column or table name must throw exception");
        } catch (LineSenderException e) {
            String m = e.getMessage();
            assertContains(m, "name contains an illegal char");
            assertNoControlCharacter(m);
        }
    }

    @Test
    public void testDoubleColumnNameCannotBeEmpty() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("mytable");
            try {
                sender.doubleColumn("", 1.5);
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "column name contains an illegal char");
            }
        }
    }

    @Test
    public void testExceptionOnClosedSender() {
        assertExceptionOnClosedSender(s -> {
        }, SET_TABLE_NAME_ACTION);
    }

    @Test
    public void testExceptionOnClosedSender_afterBoolColumn() {
        assertExceptionOnClosedSender(s -> s.table("mytable").boolColumn("d", true), s -> s.timestampColumn("e", 123456, ChronoUnit.MICROS));
    }

    @Test
    public void testExceptionOnClosedSender_afterDoubleColumn() {
        assertExceptionOnClosedSender(s -> s.table("mytable").doubleColumn("b", 1.5), s -> s.stringColumn("c", "foo"));
    }

    @Test
    public void testExceptionOnClosedSender_afterLongColumn() {
        assertExceptionOnClosedSender(s -> s.table("mytable").longColumn("a", 42), s -> s.doubleColumn("b", 1.5));
    }

    @Test
    public void testExceptionOnClosedSender_afterStringColumn() {
        assertExceptionOnClosedSender(s -> s.table("mytable").stringColumn("c", "foo"), s -> s.boolColumn("d", true));
    }

    @Test
    public void testExceptionOnClosedSender_afterSymbol() {
        assertExceptionOnClosedSender(s -> s.table("mytable").symbol("s", "val"), s -> s.longColumn("a", 42));
    }

    @Test
    public void testExceptionOnClosedSender_afterTable() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.symbol("s", "val"));
    }

    @Test
    public void testExceptionOnClosedSender_afterTimestampColumn() {
        assertExceptionOnClosedSender(s -> s.table("mytable").timestampColumn("e", 123456, ChronoUnit.MICROS), s -> s.atNow());
    }

    @Test
    public void testExplicitTimestamp() throws Exception {
        String tableName = createTrackedTable("explicit_ts");
        long ts = MicrosFormatUtils.parseTimestamp("2025-01-15T12:30:00.000000Z");
        try (Sender sender = createTcpSender()) {
            sender.table(tableName)
                    .symbol("city", "paris")
                    .longColumn("temp", 15)
                    .at(ts, ChronoUnit.MICROS);
            sender.flush();
        }
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testFlushAndAssert() throws Exception {
        String tableName = createTrackedTable("flush_test");
        try (Sender sender = createTcpSender()) {
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
        String tableName = createTrackedTable("instant_ts");
        Instant now = Instant.now();
        try (Sender sender = createTcpSender()) {
            sender.table(tableName)
                    .symbol("city", "berlin")
                    .longColumn("temp", 20)
                    .at(now);
            sender.flush();
        }
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testLargeStringValue() throws Exception {
        String tableName = createTrackedTable("large_string");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("x");
        }
        String largeString = sb.toString();

        try (Sender sender = createTcpSender()) {
            sender.table(tableName)
                    .symbol("id", "1")
                    .stringColumn("data", largeString)
                    .atNow();
            sender.flush();
        }
        assertTableRowCount(tableName, 1);
    }

    @Test
    public void testLongColumnNameCannotBeEmpty() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("mytable");
            try {
                sender.longColumn("", 42);
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "column name contains an illegal char");
            }
        }
    }

    // ==================== Integration Tests (require external QuestDB) ====================

    @Test
    public void testMultipleFlushes() throws Exception {
        String tableName = createTrackedTable("multi_flush");
        try (Sender sender = createTcpSender()) {
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
        String tableName = createTrackedTable("multi_rows");
        try (Sender sender = createTcpSender()) {
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
    public void testNoTableNameSet() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            try {
                sender.boolColumn("col", true);
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "table expected");
            }
        }
    }

    @Test
    public void testNullStringValue() throws Exception {
        String tableName = createTrackedTable("null_string");
        try (Sender sender = createTcpSender()) {
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
        String tableName = createTrackedTable("null_symbol");
        try (Sender sender = createTcpSender()) {
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
        String tableName = createTrackedTable("v1_test");
        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V1)) {
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
        String tableName = createTrackedTable("v2_test");
        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
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
        String tableName = createTrackedTable("simple_insert");
        try (Sender sender = createTcpSender()) {
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
        String tableName = createTrackedTable("special_chars");
        try (Sender sender = createTcpSender()) {
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
    public void testStringColumnNameCannotBeEmpty() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("mytable");
            try {
                sender.stringColumn("", "value");
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "column name contains an illegal char");
            }
        }
    }

    @Test
    public void testSymbolColumnNameCannotBeEmpty() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("mytable");
            try {
                sender.symbol("", "value");
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "column name contains an illegal char");
            }
        }
    }

    @Test
    public void testTableNameCannotBeEmpty() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            try {
                sender.table("");
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "table name contains an illegal char");
            }
        }
    }

    @Test
    public void testTimestampColumnNameCannotBeEmpty() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("mytable");
            try {
                sender.timestampColumn("", 123456, ChronoUnit.MICROS);
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "column name contains an illegal char");
            }
        }
    }

    @Test
    public void testUnicodeInString() throws Exception {
        String tableName = createTrackedTable("unicode_test");
        try (Sender sender = createTcpSender()) {
            sender.table(tableName)
                    .symbol("lang", "ja")
                    .stringColumn("text", "こんにちは世界")
                    .atNow();
            sender.flush();
        }
        assertTableRowCount(tableName, 1);
    }

    // ==================== Helper Methods ====================

    private static void assertExceptionOnClosedSender(Consumer<Sender> beforeCloseAction, Consumer<Sender> afterCloseAction) {
        DummyLineChannel channel = new DummyLineChannel();
        Sender sender = new LineTcpSenderV2(channel, 1000, 127);
        beforeCloseAction.accept(sender);
        sender.close();
        try {
            afterCloseAction.accept(sender);
            fail("use-after-close must throw exception");
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "sender already closed");
        }
    }

    private static void assertNoControlCharacter(CharSequence m) {
        for (int i = 0, n = m.length(); i < n; i++) {
            assertFalse(Character.isISOControl(m.charAt(i)));
        }
    }

    // ==================== Mock Channel Classes ====================

    private static class ByteChannel implements LineChannel {
        @Override
        public void close() {
        }

        @Override
        public int errno() {
            return 0;
        }

        @Override
        public int receive(long ptr, int len) {
            return 0;
        }

        @Override
        public void send(long ptr, int len) {
        }
    }

    private static class DummyLineChannel implements LineChannel {
        @Override
        public void close() {
        }

        @Override
        public int errno() {
            return 0;
        }

        @Override
        public int receive(long ptr, int len) {
            return 0;
        }

        @Override
        public void send(long ptr, int len) {
        }
    }
}
