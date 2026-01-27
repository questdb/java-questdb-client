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

package io.questdb.client.test.cutlass.line.tcp;

import io.questdb.client.Sender;
import io.questdb.client.cairo.ColumnType;
import io.questdb.client.cutlass.line.AbstractLineTcpSender;
import io.questdb.client.cutlass.line.LineChannel;
import io.questdb.client.cutlass.line.LineSenderException;
import io.questdb.client.cutlass.line.LineTcpSenderV2;
import io.questdb.client.cutlass.line.array.DoubleArray;
import io.questdb.client.cutlass.line.tcp.PlainTcpLineChannel;
import io.questdb.client.network.NetworkFacadeImpl;
import io.questdb.client.std.Decimal256;
import io.questdb.client.std.datetime.microtime.Micros;
import io.questdb.client.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.client.std.datetime.nanotime.Nanos;
import io.questdb.client.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.function.Consumer;

import static io.questdb.client.Sender.*;
import static io.questdb.client.test.tools.TestUtils.assertContains;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

/**
 * Tests for LineTcpSender.
 * <p>
 * Unit tests use DummyLineChannel/ByteChannel (no server needed).
 * Integration tests use external QuestDB via AbstractLineTcpSenderTest
 * infrastructure.
 */
public class LineTcpSenderTest extends AbstractLineTcpSenderTest {
    @Test
    public void testArrayAtNow() throws Exception {
        String table = "test_array_at_now";
        useTable(table);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2);
                DoubleArray a1 = new DoubleArray(1, 1, 2, 1).setAll(1)) {
            sender.table(table)
                    .symbol("x", "42i")
                    .symbol("y", "[6f1.0,2.5,3.0,4.5,5.0]") // ensuring no array parsing for symbol
                    .longColumn("l1", 23452345)
                    .doubleArray("a1", a1)
                    .atNow();
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
    }

    @Test
    public void testArrayDouble() throws Exception {
        String table = "test_array_double";
        useTable(table);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2);
                DoubleArray a4 = new DoubleArray(1, 1, 2, 1).setAll(4);
                DoubleArray a5 = new DoubleArray(3, 2, 1, 4, 1).setAll(5);
                DoubleArray a6 = new DoubleArray(1, 3, 4, 2, 1, 1).setAll(6)) {
            long ts = Micros.floor("2025-02-22T00:00:00.000000000Z");
            double[] arr1d = createDoubleArray(5);
            double[][] arr2d = createDoubleArray(2, 3);
            double[][][] arr3d = createDoubleArray(1, 2, 3);
            sender.table(table)
                    .symbol("x", "42i")
                    .symbol("y", "[6f1.0,2.5,3.0,4.5,5.0]") // ensuring no array parsing for symbol
                    .longColumn("l1", 23452345)
                    .doubleArray("a1", arr1d)
                    .doubleArray("a2", arr2d)
                    .doubleArray("a3", arr3d)
                    .doubleArray("a4", a4)
                    .doubleArray("a5", a5)
                    .doubleArray("a6", a6)
                    .at(ts, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
    }

    @Test
    public void testAuthSuccess() throws Exception {
        Assume.assumeTrue(getIlpTcpAuthEnabled());
        useTable("test_auth_success");

        try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(HOST, getIlpTcpPort(), 256 * 1024)) {
            sender.authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1);
            sender.metric("test_auth_success").field("my int field", 42).$();
            sender.flush();
        }

        assertTableExistsEventually("test_auth_success");
    }

    @Test
    public void testAuthWrongKey() throws Exception {
        try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(HOST, getIlpTcpPort(), 2048)) {
            sender.authenticate(AUTH_KEY_ID2_INVALID, AUTH_PRIVATE_KEY1);
            // 30 seconds should be enough even on a slow CI server
            long deadline = System.nanoTime() + SECONDS.toNanos(30);
            while (System.nanoTime() < deadline) {
                sender.metric("test_auth_wrong_key").field("my int field", 42).$();
                sender.flush();
            }
            fail("Client fail to detected qdb server closed a connection due to wrong credentials");
        } catch (LineSenderException expected) {
            // ignored
        }
    }

    @Test
    public void testBuilderAuthSuccess() throws Exception {
        Assume.assumeTrue(getIlpTcpAuthEnabled());
        useTable("test_builder_auth_success");

        try (Sender sender = Sender.builder(Sender.Transport.TCP)
                .address(HOST + ":" + getIlpTcpPort())
                .enableAuth(AUTH_KEY_ID1).authToken(TOKEN)
                .protocolVersion(PROTOCOL_VERSION_V2)
                .build()) {
            sender.table("test_builder_auth_success").longColumn("my int field", 42).atNow();
            sender.flush();
        }

        assertTableExistsEventually("test_builder_auth_success");
    }

    @Test
    public void testBuilderAuthSuccess_confString() throws Exception {
        Assume.assumeTrue(getIlpTcpAuthEnabled());
        useTable("test_builder_auth_success_conf_string");

        try (Sender sender = Sender.fromConfig("tcp::addr=" + HOST + ":" + getIlpTcpPort() + ";user=" + AUTH_KEY_ID1
                + ";token=" + TOKEN + ";protocol_version=2;")) {
            sender.table("test_builder_auth_success_conf_string").longColumn("my int field", 42).atNow();
            sender.flush();
        }

        assertTableExistsEventually("test_builder_auth_success_conf_string");
    }

    @Test
    public void testBuilderPlainText_addressWithExplicitIpAndPort() throws Exception {
        useTable("test_builder_plain_text_explicit_ip_port");

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            sender.table("test_builder_plain_text_explicit_ip_port").longColumn("my int field", 42).atNow();
            sender.flush();
        }

        assertTableExistsEventually("test_builder_plain_text_explicit_ip_port");
    }

    @Test
    public void testBuilderPlainText_addressWithHostnameAndPort() throws Exception {
        useTable("test_builder_plain_text_hostname_port");

        try (Sender sender = Sender.builder(Sender.Transport.TCP)
                .address("localhost:" + getIlpTcpPort())
                .protocolVersion(PROTOCOL_VERSION_V2)
                .build()) {
            sender.table("test_builder_plain_text_hostname_port").longColumn("my int field", 42).atNow();
            sender.flush();
        }

        assertTableExistsEventually("test_builder_plain_text_hostname_port");
    }

    @Test
    public void testBuilderPlainText_addressWithIpAndPort() throws Exception {
        useTable("test_builder_plain_text_ip_port");

        String address = "127.0.0.1:" + getIlpTcpPort();
        try (Sender sender = Sender.builder(Sender.Transport.TCP)
                .address(address)
                .protocolVersion(PROTOCOL_VERSION_V2)
                .build()) {
            sender.table("test_builder_plain_text_ip_port").longColumn("my int field", 42).atNow();
            sender.flush();
        }

        assertTableExistsEventually("test_builder_plain_text_ip_port");
    }

    @Test
    public void testCannotStartNewRowBeforeClosingTheExistingAfterValidationError() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("test_mytable");
            try {
                sender.boolColumn("col\n", true);
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "name contains an illegal char");
            }
            try {
                sender.table("test_mytable");
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(), "duplicated table");
            }
        }
        assertFalse(channel.contain(new byte[] { (byte) '\n' }));
    }

    @Test
    public void testCloseIdempotent() {
        DummyLineChannel channel = new DummyLineChannel();
        AbstractLineTcpSender sender = new LineTcpSenderV2(channel, 1000, 127);
        sender.close();
        sender.close();
        assertEquals(1, channel.closeCounter);
    }

    @Test
    public void testCloseImpliesFlush() throws Exception {
        useTable("test_close_implies_flush");

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            sender.table("test_close_implies_flush").longColumn("my int field", 42).atNow();
        }

        assertTableExistsEventually("test_close_implies_flush");
    }

    @Test
    public void testConfString() throws Exception {
        Assume.assumeTrue(getIlpTcpAuthEnabled());
        useTable("test_conf_string");

        String confString = "tcp::addr=" + HOST + ":" + getIlpTcpPort() + ";user=" + AUTH_KEY_ID1 + ";token=" + TOKEN
                + ";protocol_version=2;";
        try (Sender sender = Sender.fromConfig(confString)) {
            long tsMicros = Micros.floor("2022-02-25");
            sender.table("test_conf_string")
                    .longColumn("int_field", 42)
                    .boolColumn("bool_field", true)
                    .stringColumn("string_field", "foo")
                    .doubleColumn("double_field", 42.0)
                    .timestampColumn("ts_field", tsMicros, ChronoUnit.MICROS)
                    .at(tsMicros, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually("test_conf_string", 1);
        assertSqlEventually(
                "int_field\tbool_field\tstring_field\tdouble_field\tts_field\ttimestamp\n" +
                        "42\ttrue\tfoo\t42.0\t2022-02-25T00:00:00.000000000Z\t2022-02-25T00:00:00.000000000Z\n",
                "select int_field, bool_field, string_field, double_field, ts_field, timestamp from test_conf_string");
    }

    @Test
    public void testConfString_autoFlushBytes() throws Exception {
        useTable("test_conf_string_auto_flush_bytes");

        String confString = "tcp::addr=localhost:" + getIlpTcpPort() + ";auto_flush_bytes=1;protocol_version=2;"; // the
                                                                                                                  // minimal
                                                                                                                  // allowed
                                                                                                                  // buffer
                                                                                                                  // size
        try (Sender sender = Sender.fromConfig(confString)) {
            // just 2 rows must be enough to trigger flush
            // why not 1? the first byte of the 2nd row will flush the last byte of the 1st
            // row
            sender.table("test_conf_string_auto_flush_bytes").longColumn("my int field", 42).atNow();
            sender.table("test_conf_string_auto_flush_bytes").longColumn("my int field", 42).atNow();

            // make sure to assert before closing the Sender
            // since the Sender will always flush on close
            assertTableExistsEventually("test_conf_string_auto_flush_bytes");
        }
    }

    @Test
    public void testControlCharInColumnName() {
        assertControlCharacterException();
    }

    @Test
    public void testControlCharInTableName() {
        assertControlCharacterException();
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedInstantV1() throws Exception {
        testCreateTimestampColumns(Nanos.floor("2025-11-20T10:55:24.123123123Z"), null, PROTOCOL_VERSION_V1,
                new int[] { ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP },
                "1.111\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123000000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.123123000Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedInstantV2() throws Exception {
        testCreateTimestampColumns(Nanos.floor("2025-11-20T10:55:24.123123123Z"), null, PROTOCOL_VERSION_V2,
                new int[] { ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO },
                "1.111\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123000000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.123123000Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMicrosV1() throws Exception {
        testCreateTimestampColumns(Micros.floor("2025-11-20T10:55:24.123456000Z"), ChronoUnit.MICROS,
                PROTOCOL_VERSION_V1,
                new int[] { ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP },
                "1.111\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123000000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.123456000Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMicrosV2() throws Exception {
        testCreateTimestampColumns(Micros.floor("2025-11-20T10:55:24.123456000Z"), ChronoUnit.MICROS,
                PROTOCOL_VERSION_V2,
                new int[] { ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP },
                "1.111\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123000000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.123456000Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMillisV1() throws Exception {
        testCreateTimestampColumns(Micros.floor("2025-11-20T10:55:24.123456000Z") / 1000, ChronoUnit.MILLIS,
                PROTOCOL_VERSION_V1,
                new int[] { ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP },
                "1.111\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123000000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.123000000Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedMillisV2() throws Exception {
        testCreateTimestampColumns(Micros.floor("2025-11-20T10:55:24.123456000Z") / 1000, ChronoUnit.MILLIS,
                PROTOCOL_VERSION_V2,
                new int[] { ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP },
                "1.111\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123000000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.123000000Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedNanosV1() throws Exception {
        testCreateTimestampColumns(Nanos.floor("2025-11-20T10:55:24.123456789Z"), ChronoUnit.NANOS, PROTOCOL_VERSION_V1,
                new int[] { ColumnType.TIMESTAMP, ColumnType.TIMESTAMP, ColumnType.TIMESTAMP },
                "1.111\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123000000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.123456000Z");
    }

    @Test
    public void testCreateTimestampColumnsWithDesignatedNanosV2() throws Exception {
        testCreateTimestampColumns(Nanos.floor("2025-11-20T10:55:24.123456789Z"), ChronoUnit.NANOS, PROTOCOL_VERSION_V2,
                new int[] { ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO, ColumnType.TIMESTAMP_NANO },
                "1.111\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-19T10:55:24.123000000Z\t2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.123456000Z");
    }

    @Test
    public void testDecimalDefaultValuesWithoutWal() throws Exception {
        useTable("test_decimal_default_values_without_wal");
        execute(
                "CREATE TABLE test_decimal_default_values_without_wal (\n" +
                        "    dec8 DECIMAL(2, 0),\n" +
                        "    dec16 DECIMAL(4, 1),\n" +
                        "    dec32 DECIMAL(8, 2),\n" +
                        "    dec64 DECIMAL(16, 4),\n" +
                        "    dec128 DECIMAL(34, 8),\n" +
                        "    dec256 DECIMAL(64, 16),\n" +
                        "    value INT,\n" +
                        "    ts TIMESTAMP\n" +
                        ") TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL\n");

        assertTableExistsEventually("test_decimal_default_values_without_wal");

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V3)) {
            sender.table("test_decimal_default_values_without_wal")
                    .longColumn("value", 1)
                    .at(100_000, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually("test_decimal_default_values_without_wal", 1);
        assertSqlEventually(
                "dec8\tdec16\tdec32\tdec64\tdec128\tdec256\tvalue\tts\n" +
                        "null\tnull\tnull\tnull\tnull\tnull\t1\t1970-01-01T00:00:00.100000000Z\n",
                "select dec8, dec16, dec32, dec64, dec128, dec256, value, ts from test_decimal_default_values_without_wal");
    }

    @Test
    public void testDouble_edgeValues() throws Exception {
        useTable("test_double_edge_values");

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            long ts = Micros.floor("2022-02-25");
            sender.table("test_double_edge_values")
                    .doubleColumn("negative_inf", Double.NEGATIVE_INFINITY)
                    .doubleColumn("positive_inf", Double.POSITIVE_INFINITY)
                    .doubleColumn("nan", Double.NaN)
                    .doubleColumn("max_value", Double.MAX_VALUE)
                    .doubleColumn("min_value", Double.MIN_VALUE)
                    .at(ts, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually("test_double_edge_values", 1);
        assertSqlEventually(
                "negative_inf\tpositive_inf\tnan\tmax_value\tmin_value\ttimestamp\n" +
                        "null\tnull\tnull\t1.7976931348623157E308\t4.9E-324\t2022-02-25T00:00:00.000000000Z\n",
                "select negative_inf, positive_inf, nan, max_value, min_value, timestamp from test_double_edge_values");
    }

    @Test
    public void testExplicitTimestampColumnIndexIsCleared() throws Exception {
        useTable("test_explicit_ts_col_idx_cleared_poison");
        useTable("test_explicit_ts_col_idx_cleared_victim");

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            long ts = Micros.floor("2022-02-25");
            // the poison table sets the timestamp column index explicitly
            sender.table("test_explicit_ts_col_idx_cleared_poison")
                    .stringColumn("str_col1", "str_col1")
                    .stringColumn("str_col2", "str_col2")
                    .stringColumn("str_col3", "str_col3")
                    .stringColumn("str_col4", "str_col4")
                    .timestampColumn("timestamp", ts, ChronoUnit.MICROS)
                    .at(ts, ChronoUnit.MICROS);
            sender.flush();
            assertTableSizeEventually("test_explicit_ts_col_idx_cleared_poison", 1);

            // the victim table does not set the timestamp column index explicitly
            sender.table("test_explicit_ts_col_idx_cleared_victim")
                    .stringColumn("str_col1", "str_col1")
                    .at(ts, ChronoUnit.MICROS);
            sender.flush();
            assertTableSizeEventually("test_explicit_ts_col_idx_cleared_victim", 1);
        }
    }

    @Test
    public void testInsertBadStringIntoUuidColumn() throws Exception {
        testValueCannotBeInsertedToUuidColumn("test_insert_bad_string_into_uuid_column", "totally not a uuid");
    }

    @Test
    public void testInsertBinaryToOtherColumns() throws Exception {
        useTable("test_insert_binary_to_other_columns");
        execute(
                "CREATE TABLE test_insert_binary_to_other_columns (\n" +
                        "    x SYMBOL,\n" +
                        "    y VARCHAR,\n" +
                        "    a1 DOUBLE,\n" +
                        "    timestamp TIMESTAMP\n" +
                        ") TIMESTAMP(timestamp) PARTITION BY YEAR BYPASS WAL\n");

        assertTableExistsEventually("test_insert_binary_to_other_columns");

        // send text double to symbol column
        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V1)) {
            sender.table("test_insert_binary_to_other_columns")
                    .doubleColumn("x", 9999.0)
                    .stringColumn("y", "ystr")
                    .doubleColumn("a1", 1)
                    .at(100000000000L, ChronoUnit.MICROS);
            sender.flush();
        }
        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            // insert binary double to symbol column
            sender.table("test_insert_binary_to_other_columns")
                    .doubleColumn("x", 10000.0)
                    .stringColumn("y", "ystr")
                    .doubleColumn("a1", 1)
                    .at(100000000001L, ChronoUnit.MICROS);
            sender.flush();

            // insert binary double to string column (should be rejected)
            sender.table("test_insert_binary_to_other_columns")
                    .symbol("x", "x1")
                    .doubleColumn("y", 9999.0)
                    .doubleColumn("a1", 1)
                    .at(100000000000L, ChronoUnit.MICROS);
            sender.flush();
            // insert string to double column (should be rejected)
            sender.table("test_insert_binary_to_other_columns")
                    .symbol("x", "x1")
                    .stringColumn("y", "ystr")
                    .stringColumn("a1", "11.u")
                    .at(100000000000L, ChronoUnit.MICROS);
            sender.flush();
            // insert array column to double (should be rejected)
            sender.table("test_insert_binary_to_other_columns")
                    .symbol("x", "x1")
                    .stringColumn("y", "ystr")
                    .doubleArray("a1", new double[] { 1.0, 2.0 })
                    .at(100000000000L, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually("test_insert_binary_to_other_columns", 2);
        assertSqlEventually(
                "x\ty\ta1\ttimestamp\n" +
                        "9999.0\tystr\t1.0\t1970-01-02T03:46:40.000000000Z\n" +
                        "10000.0\tystr\t1.0\t1970-01-02T03:46:40.000001000Z\n",
                "select x, y, a1, timestamp from test_insert_binary_to_other_columns order by timestamp");
    }

    @Test
    public void testInsertDecimalTextFormatBasic() throws Exception {
        String tableName = "test_decimal_text_format_basic";
        useTable(tableName);
        execute(
                "CREATE TABLE test_decimal_text_format_basic (\n" +
                        "    price DECIMAL(10, 2),\n" +
                        "    quantity DECIMAL(15, 4),\n" +
                        "    rate DECIMAL(8, 5),\n" +
                        "    timestamp TIMESTAMP\n" +
                        ") TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL\n");

        assertTableExistsEventually(tableName);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V3)) {
            // Basic positive decimal
            sender.table(tableName)
                    .decimalColumn("price", "123.45")
                    .decimalColumn("quantity", "100.0000")
                    .decimalColumn("rate", "0.12345")
                    .at(100000000000L, ChronoUnit.MICROS);

            // Negative decimal
            sender.table(tableName)
                    .decimalColumn("price", "-45.67")
                    .decimalColumn("quantity", "-10.5000")
                    .decimalColumn("rate", "-0.00001")
                    .at(100000000001L, ChronoUnit.MICROS);

            // Small values
            sender.table(tableName)
                    .decimalColumn("price", "0.01")
                    .decimalColumn("quantity", "0.0001")
                    .decimalColumn("rate", "0.00000")
                    .at(100000000002L, ChronoUnit.MICROS);

            // Integer strings (no decimal point)
            sender.table(tableName)
                    .decimalColumn("price", "999")
                    .decimalColumn("quantity", "42")
                    .decimalColumn("rate", "1")
                    .at(100000000003L, ChronoUnit.MICROS);

            sender.flush();
        }

        assertTableSizeEventually(tableName, 4);
        assertSqlEventually(
                "price\tquantity\trate\ttimestamp\n" +
                        "123.45\t100.0000\t0.12345\t1970-01-02T03:46:40.000000000Z\n" +
                        "-45.67\t-10.5000\t-0.00001\t1970-01-02T03:46:40.000001000Z\n" +
                        "0.01\t0.0001\t0.00000\t1970-01-02T03:46:40.000002000Z\n" +
                        "999.00\t42.0000\t1.00000\t1970-01-02T03:46:40.000003000Z\n",
                "select price, quantity, rate, timestamp from " + tableName + " order by timestamp");
    }

    @Test
    public void testInsertDecimalTextFormatEdgeCases() throws Exception {
        String tableName = "test_decimal_text_format_edge_cases";
        useTable(tableName);
        execute(
                "CREATE TABLE test_decimal_text_format_edge_cases (\n" +
                        "    value DECIMAL(20, 10),\n" +
                        "    timestamp TIMESTAMP\n" +
                        ") TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL\n");

        assertTableExistsEventually(tableName);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V3)) {
            // Explicit positive sign
            sender.table(tableName)
                    .decimalColumn("value", "+123.456")
                    .at(100000000000L, ChronoUnit.MICROS);

            // Leading zeros
            sender.table(tableName)
                    .decimalColumn("value", "000123.450000")
                    .at(100000000001L, ChronoUnit.MICROS);

            // Very small value
            sender.table(tableName)
                    .decimalColumn("value", "0.0000000001")
                    .at(100000000002L, ChronoUnit.MICROS);

            // Zero with decimal point
            sender.table(tableName)
                    .decimalColumn("value", "0.0")
                    .at(100000000003L, ChronoUnit.MICROS);

            // Just zero
            sender.table(tableName)
                    .decimalColumn("value", "0")
                    .at(100000000004L, ChronoUnit.MICROS);

            sender.flush();
        }

        assertTableSizeEventually(tableName, 5);
        assertSqlEventually(
                "value\ttimestamp\n" +
                        "123.4560000000\t1970-01-02T03:46:40.000000000Z\n" +
                        "123.4500000000\t1970-01-02T03:46:40.000001000Z\n" +
                        "0.0000000001\t1970-01-02T03:46:40.000002000Z\n" +
                        "0.0000000000\t1970-01-02T03:46:40.000003000Z\n" +
                        "0.0000000000\t1970-01-02T03:46:40.000004000Z\n",
                "select value, timestamp from " + tableName + " order by timestamp");
    }

    @Test
    public void testInsertDecimalTextFormatEquivalence() throws Exception {
        String tableName = "test_decimal_text_format_equivalence";
        useTable(tableName);
        execute(
                "CREATE TABLE test_decimal_text_format_equivalence (\n" +
                        "    text_format DECIMAL(10, 3),\n" +
                        "    binary_format DECIMAL(10, 3),\n" +
                        "    timestamp TIMESTAMP\n" +
                        ") TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL\n");

        assertTableExistsEventually(tableName);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V3)) {
            // Test various values sent via both text and binary formats
            sender.table(tableName)
                    .decimalColumn("text_format", "123.450")
                    .decimalColumn("binary_format", Decimal256.fromLong(123450, 3))
                    .at(100000000000L, ChronoUnit.MICROS);

            sender.table(tableName)
                    .decimalColumn("text_format", "-45.670")
                    .decimalColumn("binary_format", Decimal256.fromLong(-45670, 3))
                    .at(100000000001L, ChronoUnit.MICROS);

            sender.table(tableName)
                    .decimalColumn("text_format", "0.001")
                    .decimalColumn("binary_format", Decimal256.fromLong(1, 3))
                    .at(100000000002L, ChronoUnit.MICROS);

            sender.flush();
        }

        assertTableSizeEventually(tableName, 3);
        assertSqlEventually(
                "text_format\tbinary_format\ttimestamp\n" +
                        "123.450\t123.450\t1970-01-02T03:46:40.000000000Z\n" +
                        "-45.670\t-45.670\t1970-01-02T03:46:40.000001000Z\n" +
                        "0.001\t0.001\t1970-01-02T03:46:40.000002000Z\n",
                "select text_format, binary_format, timestamp from " + tableName + " order by timestamp");
    }

    @Test
    public void testInsertDecimalTextFormatInvalid() throws Exception {
        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V3)) {
            sender.table("test");
            // Test invalid characters
            try {
                sender.decimalColumn("value", "abc");
                Assert.fail("Letters should throw exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "Failed to parse sent decimal value");
            }

            // Test multiple dots
            try {
                sender.decimalColumn("value", "12.34.56");
                Assert.fail("Multiple dots should throw exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "Failed to parse sent decimal value");
            }

            // Test multiple signs
            try {
                sender.decimalColumn("value", "+-123");
                Assert.fail("Multiple signs should throw exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "Failed to parse sent decimal value");
            }

            // Test special characters
            try {
                sender.decimalColumn("value", "12$34");
                Assert.fail("Special characters should throw exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "Failed to parse sent decimal value");
            }

            // Test empty decimal
            try {
                sender.decimalColumn("value", "");
                Assert.fail("Empty string should throw exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "Failed to parse sent decimal value");
            }

            // Test invalid exponent
            try {
                sender.decimalColumn("value", "1.23eABC");
                Assert.fail("Invalid exponent should throw exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "Failed to parse sent decimal value");
            }

            // Test incomplete exponent
            try {
                sender.decimalColumn("value", "1.23e");
                Assert.fail("Incomplete exponent should throw exception");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "Failed to parse sent decimal value");
            }
        }
    }

    @Test
    public void testInsertDecimalTextFormatPrecisionOverflow() throws Exception {
        String tableName = "test_decimal_text_format_precision_overflow";
        useTable(tableName);
        execute(
                "CREATE TABLE test_decimal_text_format_precision_overflow (\n" +
                        "    x DECIMAL(6, 3),\n" +
                        "    timestamp TIMESTAMP\n" +
                        ") TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL\n");

        assertTableExistsEventually(tableName);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V3)) {
            // Value that exceeds column precision (6 digits total, 3 after decimal)
            // 1000.000 has 7 digits precision, should be rejected
            sender.table(tableName)
                    .decimalColumn("x", "1000.000")
                    .at(100000000000L, ChronoUnit.MICROS);

            // Another value that exceeds precision
            sender.table(tableName)
                    .decimalColumn("x", "12345.678")
                    .at(100000000001L, ChronoUnit.MICROS);

            sender.flush();
        }

        assertSqlEventually(
                "x\ttimestamp\n",
                "select x, timestamp from " + tableName);
    }

    @Test
    public void testInsertDecimalTextFormatScientificNotation() throws Exception {
        String tableName = "test_decimal_text_format_scientific_notation";
        useTable(tableName);
        execute(
                "CREATE TABLE test_decimal_text_format_scientific_notation (\n" +
                        "    large DECIMAL(15, 2),\n" +
                        "    small DECIMAL(20, 15),\n" +
                        "    timestamp TIMESTAMP\n" +
                        ") TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL\n");

        assertTableExistsEventually(tableName);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V3)) {
            // Scientific notation with positive exponent
            sender.table(tableName)
                    .decimalColumn("large", "1.23e5")
                    .decimalColumn("small", "1.23e-10")
                    .at(100000000000L, ChronoUnit.MICROS);

            // Scientific notation with uppercase E
            sender.table(tableName)
                    .decimalColumn("large", "4.56E3")
                    .decimalColumn("small", "4.56E-8")
                    .at(100000000001L, ChronoUnit.MICROS);

            // Negative value with scientific notation
            sender.table(tableName)
                    .decimalColumn("large", "-9.99e2")
                    .decimalColumn("small", "-1.5e-12")
                    .at(100000000002L, ChronoUnit.MICROS);

            sender.flush();
        }

        assertTableSizeEventually(tableName, 3);
        assertSqlEventually(
                "large\tsmall\ttimestamp\n" +
                        "123000.00\t0.000000000123000\t1970-01-02T03:46:40.000000000Z\n" +
                        "4560.00\t0.000000045600000\t1970-01-02T03:46:40.000001000Z\n" +
                        "-999.00\t-0.000000000001500\t1970-01-02T03:46:40.000002000Z\n",
                "select large, small, timestamp from " + tableName + " order by timestamp");
    }

    @Test
    public void testInsertDecimalTextFormatTrailingZeros() throws Exception {
        String tableName = "test_decimal_text_format_trailing_zeros";
        useTable(tableName);
        execute(
                "CREATE TABLE test_decimal_text_format_trailing_zeros (\n" +
                        "    value1 DECIMAL(10, 3),\n" +
                        "    value2 DECIMAL(12, 5),\n" +
                        "    timestamp TIMESTAMP\n" +
                        ") TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL\n");

        assertTableExistsEventually(tableName);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V3)) {
            // Trailing zeros should be preserved in scale
            sender.table(tableName)
                    .decimalColumn("value1", "100.000")
                    .decimalColumn("value2", "50.00000")
                    .at(100000000000L, ChronoUnit.MICROS);

            sender.table(tableName)
                    .decimalColumn("value1", "1.200")
                    .decimalColumn("value2", "0.12300")
                    .at(100000000001L, ChronoUnit.MICROS);

            sender.table(tableName)
                    .decimalColumn("value1", "0.100")
                    .decimalColumn("value2", "0.00100")
                    .at(100000000002L, ChronoUnit.MICROS);

            sender.flush();
        }

        assertTableSizeEventually(tableName, 3);
        assertSqlEventually(
                "value1\tvalue2\ttimestamp\n" +
                        "100.000\t50.00000\t1970-01-02T03:46:40.000000000Z\n" +
                        "1.200\t0.12300\t1970-01-02T03:46:40.000001000Z\n" +
                        "0.100\t0.00100\t1970-01-02T03:46:40.000002000Z\n",
                "select value1, value2, timestamp from " + tableName + " order by timestamp");
    }

    @Test
    public void testInsertDecimals() throws Exception {
        String tableName = "test_insert_decimals";
        useTable(tableName);
        execute(
                "CREATE TABLE test_insert_decimals (\n" +
                        "    a DECIMAL(9, 0),\n" +
                        "    b DECIMAL(9, 3),\n" +
                        "    timestamp TIMESTAMP\n" +
                        ") TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL\n");

        assertTableExistsEventually(tableName);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V3)) {
            sender.table(tableName)
                    .decimalColumn("a", Decimal256.fromLong(12345, 0))
                    .decimalColumn("b", Decimal256.fromLong(12345, 2))
                    .at(100000000000L, ChronoUnit.MICROS);

            // Decimal without rescale
            sender.table(tableName)
                    .decimalColumn("a", Decimal256.NULL_VALUE)
                    .decimalColumn("b", Decimal256.fromLong(123456, 3))
                    .at(100000000001L, ChronoUnit.MICROS);

            // Integers -> Decimal
            sender.table(tableName)
                    .longColumn("a", 42)
                    .longColumn("b", 42)
                    .at(100000000002L, ChronoUnit.MICROS);

            // Strings -> Decimal without rescale
            sender.table(tableName)
                    .stringColumn("a", "42")
                    .stringColumn("b", "42.123")
                    .at(100000000003L, ChronoUnit.MICROS);

            // Strings -> Decimal with rescale
            sender.table(tableName)
                    .stringColumn("a", "42.0")
                    .stringColumn("b", "42.1")
                    .at(100000000004L, ChronoUnit.MICROS);

            // Doubles -> Decimal
            sender.table(tableName)
                    .doubleColumn("a", 42d)
                    .doubleColumn("b", 42.1d)
                    .at(100000000005L, ChronoUnit.MICROS);

            // NaN/Inf Doubles -> Decimal
            sender.table(tableName)
                    .doubleColumn("a", Double.NaN)
                    .doubleColumn("b", Double.POSITIVE_INFINITY)
                    .at(100000000006L, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(tableName, 7);
        assertSqlEventually(
                "a\tb\ttimestamp\n" +
                        "12345\t123.450\t1970-01-02T03:46:40.000000000Z\n" +
                        "null\t123.456\t1970-01-02T03:46:40.000001000Z\n" +
                        "42\t42.000\t1970-01-02T03:46:40.000002000Z\n" +
                        "42\t42.123\t1970-01-02T03:46:40.000003000Z\n" +
                        "42\t42.100\t1970-01-02T03:46:40.000004000Z\n" +
                        "42\t42.100\t1970-01-02T03:46:40.000005000Z\n" +
                        "null\tnull\t1970-01-02T03:46:40.000006000Z\n",
                "select a, b, timestamp from " + tableName + " order by timestamp");
    }

    @Test
    public void testInsertInvalidDecimals() throws Exception {
        String tableName = "test_invalid_decimal_test";
        useTable(tableName);
        execute(
                "CREATE TABLE test_invalid_decimal_test (\n" +
                        "    x DECIMAL(6, 3),\n" +
                        "    y DECIMAL(76, 73),\n" +
                        "    timestamp TIMESTAMP\n" +
                        ") TIMESTAMP(timestamp) PARTITION BY NONE BYPASS WAL\n");

        assertTableExistsEventually(tableName);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V3)) {
            // Integers out of bound (with scaling, 1234 becomes 1234.000 which have a
            // precision of 7).
            sender.table(tableName)
                    .longColumn("x", 1234)
                    .at(100000000000L, ChronoUnit.MICROS);

            // Integers overbound during the rescale process.
            sender.table(tableName)
                    .longColumn("y", 12345)
                    .at(100000000001L, ChronoUnit.MICROS);

            // Floating points with a scale greater than expected.
            sender.table(tableName)
                    .doubleColumn("x", 1.2345d)
                    .at(100000000002L, ChronoUnit.MICROS);

            // Floating points with a precision greater than expected.
            sender.table(tableName)
                    .doubleColumn("x", 12345.678d)
                    .at(100000000003L, ChronoUnit.MICROS);

            // String that is not a valid decimal.
            sender.table(tableName)
                    .stringColumn("x", "abc")
                    .at(100000000004L, ChronoUnit.MICROS);

            // String that has a too big precision.
            sender.table(tableName)
                    .stringColumn("x", "1E8")
                    .at(100000000005L, ChronoUnit.MICROS);

            // Decimal with a too big precision.
            sender.table(tableName)
                    .decimalColumn("x", Decimal256.fromLong(12345678, 3))
                    .at(100000000006L, ChronoUnit.MICROS);

            // Decimal with a too big precision when scaled.
            sender.table(tableName)
                    .decimalColumn("y", Decimal256.fromLong(12345, 0))
                    .at(100000000007L, ChronoUnit.MICROS);
            sender.flush();

            // Decimal loosing precision
            sender.table(tableName)
                    .decimalColumn("x", Decimal256.fromLong(123456, 4))
                    .at(100000000007L, ChronoUnit.MICROS);
            sender.flush();
        }

        assertSqlEventually(
                "x\ty\ttimestamp\n",
                "select x, y, timestamp from " + tableName);
    }

    @Test
    public void testInsertLargeArray() throws Exception {
        String tableName = "test_arr_large_test";
        useTable(tableName);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            double[] arr = createDoubleArray(100_000_000);
            sender.table(tableName)
                    .doubleArray("arr", arr)
                    .at(100000000000L, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(tableName, 1);
    }

    @Test
    public void testInsertNonAsciiStringAndUuid() throws Exception {
        // this is to check that a non-ASCII string will not prevent
        // parsing a subsequent UUID
        useTable("test_insert_non_ascii_string_and_uuid");
        execute(
                "CREATE TABLE test_insert_non_ascii_string_and_uuid (\n" +
                        "    s STRING,\n" +
                        "    u UUID,\n" +
                        "    ts TIMESTAMP\n" +
                        ") TIMESTAMP(ts) PARTITION BY NONE BYPASS WAL\n");

        assertTableExistsEventually("test_insert_non_ascii_string_and_uuid");

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            long tsMicros = Micros.floor("2022-02-25");
            sender.table("test_insert_non_ascii_string_and_uuid")
                    .stringColumn("s", "non-ascii äöü")
                    .stringColumn("u", "11111111-2222-3333-4444-555555555555")
                    .at(tsMicros, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually("test_insert_non_ascii_string_and_uuid", 1);
        assertSqlEventually(
                "s\tu\tts\n" +
                        "non-ascii äöü\t11111111-2222-3333-4444-555555555555\t2022-02-25T00:00:00.000000000Z\n",
                "select s, u, ts from test_insert_non_ascii_string_and_uuid");
    }

    @Test
    public void testInsertNonAsciiStringIntoUuidColumn() throws Exception {
        // carefully crafted value so when encoded as UTF-8 it has the same byte length
        // as a proper UUID
        testValueCannotBeInsertedToUuidColumn("test_insert_non_ascii_string_into_uuid_column",
                "11111111-1111-1111-1111-1111111111ü");
    }

    @Test
    public void testInsertStringIntoUuidColumn() throws Exception {
        useTable("test_insert_string_into_uuid_column");
        execute(
                "CREATE TABLE test_insert_string_into_uuid_column (\n" +
                        "    u1 UUID,\n" +
                        "    u2 UUID,\n" +
                        "    u3 UUID,\n" +
                        "    ts TIMESTAMP\n" +
                        ") TIMESTAMP(ts) PARTITION BY NONE BYPASS WAL\n");

        assertTableExistsEventually("test_insert_string_into_uuid_column");

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            long tsMicros = Micros.floor("2022-02-25");
            sender.table("test_insert_string_into_uuid_column")
                    .stringColumn("u1", "11111111-1111-1111-1111-111111111111")
                    // u2 empty -> insert as null
                    .stringColumn("u3", "33333333-3333-3333-3333-333333333333")
                    .at(tsMicros, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually("test_insert_string_into_uuid_column", 1);
        assertSqlEventually(
                "u1\tu3\tts\n" +
                        "11111111-1111-1111-1111-111111111111\t33333333-3333-3333-3333-333333333333\t2022-02-25T00:00:00.000000000Z\n",
                "select u1, u3, ts from test_insert_string_into_uuid_column");
    }

    @Test
    public void testInsertTimestampAsInstant() throws Exception {
        useTable("test_insert_timestamp_as_instant");
        execute(
                "CREATE TABLE test_insert_timestamp_as_instant (\n" +
                        "    ts_col TIMESTAMP,\n" +
                        "    timestamp TIMESTAMP\n" +
                        ") TIMESTAMP(timestamp) PARTITION BY YEAR BYPASS WAL\n");

        assertTableExistsEventually("test_insert_timestamp_as_instant");

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            sender.table("test_insert_timestamp_as_instant")
                    .timestampColumn("ts_col", Instant.parse("2023-02-11T12:30:11.35Z"))
                    .at(Instant.parse("2022-01-10T20:40:22.54Z"));
            sender.flush();
        }

        assertTableSizeEventually("test_insert_timestamp_as_instant", 1);
        assertSqlEventually(
                "ts_col\ttimestamp\n" +
                        "2023-02-11T12:30:11.350000000Z\t2022-01-10T20:40:22.540000000Z\n",
                "select ts_col, timestamp from test_insert_timestamp_as_instant");
    }

    @Test
    public void testInsertTimestampMiscUnits() throws Exception {
        useTable("test_insert_timestamp_misc_units");
        execute(
                "CREATE TABLE test_insert_timestamp_misc_units (\n" +
                        "    unit STRING,\n" +
                        "    ts TIMESTAMP,\n" +
                        "    timestamp TIMESTAMP\n" +
                        ") TIMESTAMP(timestamp) PARTITION BY YEAR BYPASS WAL\n");

        assertTableExistsEventually("test_insert_timestamp_misc_units");

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            long tsMicros = Micros.floor("2023-09-18T12:01:01.01Z");
            sender.table("test_insert_timestamp_misc_units")
                    .stringColumn("unit", "ns")
                    .timestampColumn("ts", tsMicros * 1000, ChronoUnit.NANOS)
                    .at(tsMicros * 1000, ChronoUnit.NANOS);
            sender.table("test_insert_timestamp_misc_units")
                    .stringColumn("unit", "us")
                    .timestampColumn("ts", tsMicros, ChronoUnit.MICROS)
                    .at(tsMicros, ChronoUnit.MICROS);
            sender.table("test_insert_timestamp_misc_units")
                    .stringColumn("unit", "ms")
                    .timestampColumn("ts", tsMicros / 1000, ChronoUnit.MILLIS)
                    .at(tsMicros / 1000, ChronoUnit.MILLIS);
            sender.table("test_insert_timestamp_misc_units")
                    .stringColumn("unit", "s")
                    .timestampColumn("ts", tsMicros / Micros.SECOND_MICROS, ChronoUnit.SECONDS)
                    .at(tsMicros / Micros.SECOND_MICROS, ChronoUnit.SECONDS);
            sender.table("test_insert_timestamp_misc_units")
                    .stringColumn("unit", "m")
                    .timestampColumn("ts", tsMicros / Micros.MINUTE_MICROS, ChronoUnit.MINUTES)
                    .at(tsMicros / Micros.MINUTE_MICROS, ChronoUnit.MINUTES);
            sender.flush();
        }

        assertTableSizeEventually("test_insert_timestamp_misc_units", 5);
        assertSqlEventually(
                "unit\tts\ttimestamp\n" +
                        "m\t2023-09-18T12:01:00.000000000Z\t2023-09-18T12:01:00.000000000Z\n" +
                        "s\t2023-09-18T12:01:01.000000000Z\t2023-09-18T12:01:01.000000000Z\n" +
                        "ns\t2023-09-18T12:01:01.010000000Z\t2023-09-18T12:01:01.010000000Z\n" +
                        "us\t2023-09-18T12:01:01.010000000Z\t2023-09-18T12:01:01.010000000Z\n" +
                        "ms\t2023-09-18T12:01:01.010000000Z\t2023-09-18T12:01:01.010000000Z\n",
                "select unit, ts, timestamp from test_insert_timestamp_misc_units order by timestamp");
    }

    @Test
    public void testInsertTimestampNanoOverflow() throws Exception {
        useTable("test_insert_timestamp_nano_overflow");
        execute(
                "CREATE TABLE test_insert_timestamp_nano_overflow (\n" +
                        "    ts TIMESTAMP,\n" +
                        "    timestamp TIMESTAMP\n" +
                        ") TIMESTAMP(timestamp) PARTITION BY YEAR BYPASS WAL\n");

        assertTableExistsEventually("test_insert_timestamp_nano_overflow");

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            long tsMicros = Micros.floor("2323-09-18T12:01:01.011568901Z");
            sender.table("test_insert_timestamp_nano_overflow")
                    .timestampColumn("ts", tsMicros, ChronoUnit.MICROS)
                    .at(tsMicros, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually("test_insert_timestamp_nano_overflow", 1);
        assertSqlEventually(
                "ts\ttimestamp\n" +
                        "2323-09-18T12:01:01.011568000Z\t2323-09-18T12:01:01.011568000Z\n",
                "select ts, timestamp from test_insert_timestamp_nano_overflow");
    }

    @Test
    public void testInsertTimestampNanoUnits() throws Exception {
        useTable("test_insert_timestamp_nano_units");
        execute(
                "CREATE TABLE test_insert_timestamp_nano_units (\n" +
                        "    unit STRING,\n" +
                        "    ts TIMESTAMP,\n" +
                        "    timestamp TIMESTAMP\n" +
                        ") TIMESTAMP(timestamp) PARTITION BY YEAR BYPASS WAL\n");

        assertTableExistsEventually("test_insert_timestamp_nano_units");

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            long tsNanos = Micros.floor("2023-09-18T12:01:01.011568901Z") * 1000;
            sender.table("test_insert_timestamp_nano_units")
                    .stringColumn("unit", "ns")
                    .timestampColumn("ts", tsNanos, ChronoUnit.NANOS)
                    .at(tsNanos, ChronoUnit.NANOS);
            sender.flush();
        }

        assertTableSizeEventually("test_insert_timestamp_nano_units", 1);
        assertSqlEventually(
                "unit\tts\ttimestamp\n" +
                        "ns\t2023-09-18T12:01:01.011568000Z\t2023-09-18T12:01:01.011568000Z\n",
                "select unit, ts, timestamp from test_insert_timestamp_nano_units");
    }

    @Test
    public void testMaxNameLength() throws Exception {
        PlainTcpLineChannel channel = new PlainTcpLineChannel(NetworkFacadeImpl.INSTANCE, HOST, getIlpTcpPort(), 1024);
        try (AbstractLineTcpSender sender = new LineTcpSenderV2(channel, 1024, 20)) {
            try {
                sender.table("table_with_long______________________name");
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(),
                        "table name is too long: [name = table_with_long______________________name, maxNameLength=20]");
            }

            try {
                sender.table("tab")
                        .doubleColumn("column_with_long______________________name", 1.0);
                fail();
            } catch (LineSenderException e) {
                assertContains(e.getMessage(),
                        "column name is too long: [name = column_with_long______________________name, maxNameLength=20]");
            }
        }
    }

    @Test
    public void testMinBufferSizeWhenAuth() throws Exception {
        Assume.assumeTrue(getIlpTcpAuthEnabled());
        int tinyCapacity = 42;
        try (AbstractLineTcpSender sender = LineTcpSenderV2.newSender(HOST, getIlpTcpPort(), tinyCapacity)) {
            sender.authenticate(AUTH_KEY_ID1, AUTH_PRIVATE_KEY1);
            fail();
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "challenge did not fit into buffer");
        }
    }

    @Test
    public void testMultipleVarcharCols() throws Exception {
        String table = "test_string_table";
        useTable(table);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            long ts = Micros.floor("2024-02-27");
            sender.table(table)
                    .stringColumn("string1", "some string")
                    .stringColumn("string2", "another string")
                    .stringColumn("string3", "yet another string")
                    .at(ts, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "string1\tstring2\tstring3\ttimestamp\n" +
                        "some string\tanother string\tyet another string\t2024-02-27T00:00:00.000000000Z\n",
                "select string1, string2, string3, timestamp from " + table);
    }

    @Test
    public void testServerIgnoresUnfinishedRows() throws Exception {
        String tableName = "test_server_ignores_unfinished_rows";
        useTable(tableName);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            // well-formed row first
            sender.table(tableName).longColumn("field0", 42)
                    .longColumn("field1", 42)
                    .atNow();

            // failed validation
            sender.table(tableName)
                    .longColumn("field0", 42)
                    .longColumn("field1\n", 42);
            fail("validation should have failed");
        } catch (LineSenderException e) {
            // ignored
        }

        // make sure the 2nd unfinished row was not inserted by the server
        assertTableSizeEventually(tableName, 1);
    }

    @Test
    public void testSymbolCapacityReload() throws Exception {
        // Tests that the client can send many rows with symbols
        String tableName = "test_symbol_capacity_table";
        useTable(tableName);
        final int N = 1000;

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            Random rnd = new Random(42);
            for (int i = 0; i < N; i++) {
                sender.table(tableName)
                        .symbol("sym1", "sym_" + rnd.nextInt(100))
                        .symbol("sym2", "s" + rnd.nextInt(10))
                        .doubleColumn("dd", rnd.nextDouble())
                        .atNow();
            }
            sender.flush();
        }

        assertTableSizeEventually(tableName, N);
    }

    @Test
    public void testSymbolsCannotBeWrittenAfterBool() throws Exception {
        assertSymbolsCannotBeWrittenAfterOtherType(s -> s.boolColumn("columnName", false));
    }

    @Test
    public void testSymbolsCannotBeWrittenAfterDouble() throws Exception {
        assertSymbolsCannotBeWrittenAfterOtherType(s -> s.doubleColumn("columnName", 42.0));
    }

    @Test
    public void testSymbolsCannotBeWrittenAfterLong() throws Exception {
        assertSymbolsCannotBeWrittenAfterOtherType(s -> s.longColumn("columnName", 42));
    }

    @Test
    public void testSymbolsCannotBeWrittenAfterString() throws Exception {
        assertSymbolsCannotBeWrittenAfterOtherType(s -> s.stringColumn("columnName", "42"));
    }

    @Test
    public void testTimestampIngestV1() throws Exception {
        testTimestampIngest("test_timestamp_ingest_v1", "TIMESTAMP", PROTOCOL_VERSION_V1,
                "ts\tdts\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834000000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834000000Z\n" +
                        "2025-11-19T10:55:24.123000000Z\t2025-11-20T10:55:24.834000000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123000000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123000000Z\t2025-11-20T10:55:24.834129000Z\n",
                null);
    }

    @Test
    public void testTimestampIngestV2() throws Exception {
        testTimestampIngest("test_timestamp_ingest_v2", "TIMESTAMP", PROTOCOL_VERSION_V2,
                "ts\tdts\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834000000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834000000Z\n" +
                        "2025-11-19T10:55:24.123000000Z\t2025-11-20T10:55:24.834000000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123000000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123000000Z\t2025-11-20T10:55:24.834129000Z\n",
                "ts\tdts\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834000000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834000000Z\n" +
                        "2025-11-19T10:55:24.123000000Z\t2025-11-20T10:55:24.834000000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123000000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123456000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2025-11-19T10:55:24.123000000Z\t2025-11-20T10:55:24.834129000Z\n" +
                        "2300-11-19T10:55:24.123456000Z\t2300-11-20T10:55:24.834129000Z\n");
    }

    @Test
    public void testUnfinishedRowDoesNotContainNewLine() {
        ByteChannel channel = new ByteChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("test_mytable");
            sender.boolColumn("col\n", true);
        } catch (LineSenderException e) {
            assertContains(e.getMessage(), "name contains an illegal char");
        }
        assertFalse(channel.contain(new byte[] { (byte) '\n' }));
    }

    @Test
    public void testUseAfterClose_atMicros() {
        assertExceptionOnClosedSender(s -> {
            s.table("test_mytable");
            s.longColumn("col", 42);
        }, s -> s.at(MicrosecondClockImpl.INSTANCE.getTicks(), ChronoUnit.MICROS));
    }

    @Test
    public void testUseAfterClose_atNow() {
        assertExceptionOnClosedSender(s -> {
            s.table("test_mytable");
            s.longColumn("col", 42);
        }, Sender::atNow);
    }

    @Test
    public void testUseAfterClose_boolColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.boolColumn("col", true));
    }

    @Test
    public void testUseAfterClose_doubleColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.doubleColumn("col", 42.42));
    }

    @Test
    public void testUseAfterClose_flush() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, Sender::flush);
    }

    @Test
    public void testUseAfterClose_longColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.longColumn("col", 42));
    }

    @Test
    public void testUseAfterClose_stringColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.stringColumn("col", "val"));
    }

    @Test
    public void testUseAfterClose_symbol() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.symbol("sym", "val"));
    }

    @Test
    public void testUseAfterClose_table() {
        assertExceptionOnClosedSender();
    }

    @Test
    public void testUseAfterClose_tsColumn() {
        assertExceptionOnClosedSender(SET_TABLE_NAME_ACTION, s -> s.timestampColumn("col", 0, ChronoUnit.MICROS));
    }

    @Test
    public void testUseVarcharAsString() throws Exception {
        String table = "test_varchar_string_table";
        useTable(table);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            long ts = Micros.floor("2024-02-27");
            String expectedValue = "čćžšđçğéíáýůř";
            sender.table(table)
                    .stringColumn("string1", expectedValue)
                    .at(ts, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "string1\ttimestamp\n" +
                        "čćžšđçğéíáýůř\t2024-02-27T00:00:00.000000000Z\n",
                "select string1, timestamp from " + table);
    }

    @Test
    public void testWriteAllTypes() throws Exception {
        useTable("test_write_all_types");

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            long ts = Micros.floor("2022-02-25");
            sender.table("test_write_all_types")
                    .longColumn("int_field", 42)
                    .boolColumn("bool_field", true)
                    .stringColumn("string_field", "foo")
                    .doubleColumn("double_field", 42.0)
                    .timestampColumn("ts_field", ts, ChronoUnit.MICROS)
                    .at(ts, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually("test_write_all_types", 1);
        assertSqlEventually(
                "int_field\tbool_field\tstring_field\tdouble_field\tts_field\ttimestamp\n" +
                        "42\ttrue\tfoo\t42.0\t2022-02-25T00:00:00.000000000Z\t2022-02-25T00:00:00.000000000Z\n",
                "select int_field, bool_field, string_field, double_field, ts_field, timestamp from test_write_all_types");
    }

    @Test
    public void testWriteLongMinMax() throws Exception {
        String table = "test_long_min_max_table";
        useTable(table);

        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            long ts = Micros.floor("2023-02-22");
            sender.table(table)
                    .longColumn("max", Long.MAX_VALUE)
                    .longColumn("min", Long.MIN_VALUE)
                    .at(ts, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(table, 1);
        assertSqlEventually(
                "max\tmin\ttimestamp\n" +
                        "9223372036854775807\tnull\t2023-02-22T00:00:00.000000000Z\n",
                "select max, min, timestamp from " + table);
    }

    private static void assertControlCharacterException() {
        DummyLineChannel channel = new DummyLineChannel();
        try (Sender sender = new LineTcpSenderV2(channel, 1000, 127)) {
            sender.table("test_mytable");
            sender.boolColumn("col\u0001", true);
            fail("control character in column or table name must throw exception");
        } catch (LineSenderException e) {
            String m = e.getMessage();
            assertContains(m, "name contains an illegal char");
            assertNoControlCharacter(m);
        }
    }

    private static void assertExceptionOnClosedSender(Consumer<Sender> beforeCloseAction,
            Consumer<Sender> afterCloseAction) {
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

    private static void assertExceptionOnClosedSender() {
        assertExceptionOnClosedSender(s -> {
        }, LineTcpSenderTest.SET_TABLE_NAME_ACTION);
    }

    private static void assertNoControlCharacter(CharSequence m) {
        for (int i = 0, n = m.length(); i < n; i++) {
            assertFalse(Character.isISOControl(m.charAt(i)));
        }
    }

    private void assertSymbolsCannotBeWrittenAfterOtherType(Consumer<Sender> otherTypeWriter) throws Exception {
        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            sender.table("test_symbols_cannot_be_written_after_other_type");
            otherTypeWriter.accept(sender);
            try {
                sender.symbol("name", "value");
                fail("symbols cannot be written after any other column type");
            } catch (LineSenderException e) {
                TestUtils.assertContains(e.getMessage(), "before any other column types");
                sender.atNow();
            }
        }
    }

    private void testCreateTimestampColumns(long timestamp, ChronoUnit unit, int protocolVersion,
            int[] expectedColumnTypes, String expected) throws Exception {
        useTable("test_tab1");

        try (Sender sender = createTcpSender(protocolVersion)) {
            long ts_ns = Micros.floor("2025-11-19T10:55:24.123456000Z") * 1000;
            long ts_us = Micros.floor("2025-11-19T10:55:24.123456000Z");
            long ts_ms = Micros.floor("2025-11-19T10:55:24.123Z") / 1000;
            Instant ts_instant = Instant.ofEpochSecond(ts_ns / 1_000_000_000, ts_ns % 1_000_000_000 + 10);

            if (unit != null) {
                sender.table("test_tab1")
                        .doubleColumn("col1", 1.111)
                        .timestampColumn("ts_ns", ts_ns, ChronoUnit.NANOS)
                        .timestampColumn("ts_us", ts_us, ChronoUnit.MICROS)
                        .timestampColumn("ts_ms", ts_ms, ChronoUnit.MILLIS)
                        .timestampColumn("ts_instant", ts_instant)
                        .at(timestamp, unit);
            } else {
                sender.table("test_tab1")
                        .doubleColumn("col1", 1.111)
                        .timestampColumn("ts_ns", ts_ns, ChronoUnit.NANOS)
                        .timestampColumn("ts_us", ts_us, ChronoUnit.MICROS)
                        .timestampColumn("ts_ms", ts_ms, ChronoUnit.MILLIS)
                        .timestampColumn("ts_instant", ts_instant)
                        .at(Instant.ofEpochSecond(timestamp / 1_000_000_000, timestamp % 1_000_000_000));
            }

            sender.flush();
        }

        assertTableSizeEventually("test_tab1", 1);
        assertSqlEventually("column\ttype\n" +
                "col1\tDOUBLE\n" +
                "timestamp\t" + ColumnType.nameOf(expectedColumnTypes[2]) + "\n" +
                "ts_instant\t" + ColumnType.nameOf(expectedColumnTypes[1]) + "\n" +
                "ts_ms\tTIMESTAMP\n" +
                "ts_ns\t" + ColumnType.nameOf(expectedColumnTypes[0]) + "\n" +
                "ts_us\tTIMESTAMP\n",
                "select \"column\", \"type\" from table_columns('test_tab1') order by \"column\"");
        assertSqlEventually("col1\tts_ns\tts_us\tts_ms\tts_instant\ttimestamp\n" + expected + "\n",
                "test_tab1");
    }

    private void testTimestampIngest(String tableName, String timestampType, int protocolVersion, String expected1,
            String expected2) throws Exception {
        useTable(tableName);
        execute("create table " + tableName + " (ts " + timestampType + ", dts " + timestampType
                + ") timestamp(dts) partition by DAY BYPASS WAL");
        assertTableExistsEventually(tableName);

        try (Sender sender = createTcpSender(protocolVersion)) {
            long ts_ns = Micros.floor("2025-11-19T10:55:24.123456000Z") * 1000;
            long dts_ns = Micros.floor("2025-11-20T10:55:24.834129000Z") * 1000;
            long ts_us = Micros.floor("2025-11-19T10:55:24.123456000Z");
            long dts_us = Micros.floor("2025-11-20T10:55:24.834129000Z");
            long ts_ms = Micros.floor("2025-11-19T10:55:24.123Z") / 1000;
            long dts_ms = Micros.floor("2025-11-20T10:55:24.834Z") / 1000;
            Instant tsInstant_ns = Instant.ofEpochSecond(ts_ns / 1_000_000_000, ts_ns % 1_000_000_000 + 10);
            Instant dtsInstant_ns = Instant.ofEpochSecond(dts_ns / 1_000_000_000, dts_ns % 1_000_000_000 + 10);

            sender.table(tableName)
                    .timestampColumn("ts", ts_ns, ChronoUnit.NANOS)
                    .at(dts_ns, ChronoUnit.NANOS);
            sender.table(tableName)
                    .timestampColumn("ts", ts_us, ChronoUnit.MICROS)
                    .at(dts_ns, ChronoUnit.NANOS);
            sender.table(tableName)
                    .timestampColumn("ts", ts_ms, ChronoUnit.MILLIS)
                    .at(dts_ns, ChronoUnit.NANOS);

            sender.table(tableName)
                    .timestampColumn("ts", ts_ns, ChronoUnit.NANOS)
                    .at(dts_us, ChronoUnit.MICROS);
            sender.table(tableName)
                    .timestampColumn("ts", ts_us, ChronoUnit.MICROS)
                    .at(dts_us, ChronoUnit.MICROS);
            sender.table(tableName)
                    .timestampColumn("ts", ts_ms, ChronoUnit.MILLIS)
                    .at(dts_us, ChronoUnit.MICROS);

            sender.table(tableName)
                    .timestampColumn("ts", ts_ns, ChronoUnit.NANOS)
                    .at(dts_ms, ChronoUnit.MILLIS);
            sender.table(tableName)
                    .timestampColumn("ts", ts_us, ChronoUnit.MICROS)
                    .at(dts_ms, ChronoUnit.MILLIS);
            sender.table(tableName)
                    .timestampColumn("ts", ts_ms, ChronoUnit.MILLIS)
                    .at(dts_ms, ChronoUnit.MILLIS);

            sender.table(tableName)
                    .timestampColumn("ts", tsInstant_ns)
                    .at(dtsInstant_ns);

            sender.flush();

            assertTableSizeEventually(tableName, 10);
            assertSqlEventually(expected1, "select ts, dts from " + tableName);

            try {
                // fails for nanos, long overflow
                long ts_tooLargeForNanos_us = Micros.floor("2300-11-19T10:55:24.123456000Z");
                long dts_tooLargeForNanos_us = Micros.floor("2300-11-20T10:55:24.834129000Z");
                sender.table(tableName)
                        .timestampColumn("ts", ts_tooLargeForNanos_us, ChronoUnit.MICROS)
                        .at(dts_tooLargeForNanos_us, ChronoUnit.MICROS);
                sender.flush();

                if (expected2 == null && protocolVersion == PROTOCOL_VERSION_V1) {
                    Assert.fail("Exception expected");
                }
            } catch (ArithmeticException e) {
                if (expected2 == null && protocolVersion == PROTOCOL_VERSION_V1) {
                    TestUtils.assertContains(e.getMessage(), "long overflow");
                } else {
                    throw e;
                }
            }

            assertTableSizeEventually(tableName, expected2 == null ? 10 : 11);
            assertSqlEventually(expected2 == null ? expected1 : expected2, "select ts, dts from " + tableName);
        }
    }

    private void testValueCannotBeInsertedToUuidColumn(String tableName, String value) throws Exception {
        useTable(tableName);
        execute("CREATE TABLE " + tableName + " (" +
                "u1 UUID," +
                "ts TIMESTAMP" +
                ") TIMESTAMP(ts) PARTITION BY NONE BYPASS WAL");

        assertTableExistsEventually(tableName);

        // this sender fails as the string is not UUID
        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            long tsMicros = Micros.floor("2022-02-25");
            sender.table(tableName)
                    .stringColumn("u1", value)
                    .at(tsMicros, ChronoUnit.MICROS);
            sender.flush();
        }

        // this sender succeeds as the string is in the UUID format
        try (Sender sender = createTcpSender(PROTOCOL_VERSION_V2)) {
            long tsMicros = Micros.floor("2022-02-25");
            sender.table(tableName)
                    .stringColumn("u1", "11111111-1111-1111-1111-111111111111")
                    .at(tsMicros, ChronoUnit.MICROS);
            sender.flush();
        }

        assertTableSizeEventually(tableName, 1);
        assertSqlEventually(
                "u1\tts\n" +
                        "11111111-1111-1111-1111-111111111111\t2022-02-25T00:00:00.000000000Z\n",
                "select u1, ts from " + tableName);
    }

    private static class DummyLineChannel implements LineChannel {
        private int closeCounter;

        @Override
        public void close() {
            closeCounter++;
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
